"""LLM 因子挖掘：让大模型提出候选因子表达式，本地用数据严格验证。

流程：
1. 把可用字段、算子、已有因子与其表现发给 LLM，请它提出 N 个新表达式；
2. 每个表达式经 ExpressionFactor 的 AST 白名单校验（LLM 输出不可信，
   非法表达式直接丢弃，不存在代码注入风险）；
3. 本地计算 RankIC 验证，只保留达标的因子；
4. 可多轮迭代：把上一轮结果反馈给 LLM 继续改进。
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from quantmaster.ai.llm import LLMClient, LLMError
from quantmaster.factors.analysis import forward_returns, information_coefficient
from quantmaster.factors.base import ExpressionFactor, PanelDict
from quantmaster.factors.ops import OPERATORS

SYSTEM_PROMPT = """你是一位 A 股量化研究员，擅长构造量价因子表达式。
表达式规则：
- 可用字段: open, high, low, close, volume, amount, turnover, vwap, returns
- 可用算子: {operators}
- 支持 + - * / ** 与负号，数字常量；不支持其他任何语法
- ts_ 开头的算子沿时间轴滚动计算，rank/zscore 是当日截面运算
- 约定因子值越大越看好该股票
示例: rank(-delta(close, 5)) 、 -ts_corr(rank(volume), rank(close), 10)"""


@dataclass
class LLMMinedFactor:
    expression: str
    rationale: str
    ic_mean: float
    icir: float
    valid: bool
    error: str = ""


@dataclass
class LLMMiningReport:
    factors: list[LLMMinedFactor]
    rounds_requested: int
    rounds_completed: int
    attempts: int
    warnings: list[dict[str, Any]]


class LLMFactorMiner:
    def __init__(self, client: LLMClient | None = None, ic_threshold: float = 0.02):
        self.client = client or LLMClient()
        self.ic_threshold = ic_threshold

    def _prompt(self, n: int, feedback: list[LLMMinedFactor] | None) -> str:
        prompt = (
            f"请提出 {n} 个逻辑各异的 A 股量价因子表达式，追求与常见动量/反转因子的差异性。\n"
            '输出 JSON 数组: [{"expression": "...", "rationale": "一句话逻辑"}]'
        )
        if feedback:
            lines = [
                f"- {f.expression}  RankIC={f.ic_mean:.4f} ICIR={f.icir:.2f} "
                f"{'通过' if f.valid else '未达标'}"
                for f in feedback
            ]
            prompt += "\n\n上一轮候选与真实数据验证结果如下，请分析规律后提出更好的表达式：\n"
            prompt += "\n".join(lines)
        return prompt

    def _validate(self, expression: str, rationale: str,
                  panel: PanelDict, fwd) -> LLMMinedFactor:
        try:
            factor = ExpressionFactor(expression)          # AST 白名单校验
            values = factor.compute(panel)
            ic = information_coefficient(values, fwd)
            ic_mean = float(ic.mean()) if len(ic) else 0.0
            ic_std = float(ic.std()) if len(ic) else 0.0
            icir = ic_mean / ic_std if ic_std > 0 else 0.0
            valid = abs(ic_mean) >= self.ic_threshold
            return LLMMinedFactor(expression, rationale, ic_mean, icir, valid)
        except Exception as e:
            message = str(e)
            return LLMMinedFactor(
                expression, f"{rationale}（无效: {message}）", 0.0, 0.0, False, message,
            )

    @staticmethod
    def _emit(callback: Callable[[dict[str, Any]], None] | None, event: dict[str, Any]) -> None:
        if callback:
            callback(event)

    @staticmethod
    def _wait(delay: float, cancelled: Callable[[], bool] | None) -> None:
        deadline = time.monotonic() + max(0.0, delay)
        while True:
            if cancelled and cancelled():
                raise InterruptedError("研究任务已取消")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(min(0.25, remaining))

    def mine_report(
        self,
        panel: PanelDict,
        n: int = 8,
        rounds: int = 2,
        periods: int = 1,
        *,
        max_retries: int = 3,
        on_event: Callable[[dict[str, Any]], None] | None = None,
        cancelled: Callable[[], bool] | None = None,
        retry_backoff: tuple[float, ...] = (2.0, 4.0, 8.0),
    ) -> LLMMiningReport:
        """执行可恢复的多轮 LLM 发现，并保留已完成轮次的可用结果。"""
        system = SYSTEM_PROMPT.format(operators=", ".join(sorted(OPERATORS)))
        fwd = forward_returns(panel["close"], periods=periods)
        all_results: list[LLMMinedFactor] = []
        feedback: list[LLMMinedFactor] | None = None
        warnings: list[dict[str, Any]] = []
        rounds_completed = 0
        attempts_total = 0
        rounds = max(1, int(rounds))
        max_attempts = max(1, int(max_retries) + 1)
        configured_timeout = float(getattr(getattr(self.client, "config", None), "timeout", 60.0))
        base_timeout = min(600.0, max(180.0, configured_timeout))
        timeout_offsets = (0.0, 60.0, 180.0, 300.0)

        for round_number in range(1, rounds + 1):
            prompt = self._prompt(n, feedback)
            candidates: list[dict] = []
            final_error: LLMError | None = None
            for attempt in range(1, max_attempts + 1):
                if cancelled and cancelled():
                    raise InterruptedError("研究任务已取消")
                offset = timeout_offsets[min(attempt - 1, len(timeout_offsets) - 1)]
                timeout = min(600.0, base_timeout + offset)
                attempts_total += 1
                self._emit(on_event, {
                    "type": "llm_attempt_started",
                    "round": round_number,
                    "rounds": rounds,
                    "attempt": attempt,
                    "max_attempts": max_attempts,
                    "timeout_seconds": int(timeout),
                    "provider": str(getattr(getattr(self.client, "config", None), "provider", "")),
                    "model": str(getattr(getattr(self.client, "config", None), "model", "")),
                })
                try:
                    reply = self.client.chat_json(prompt, system=system, timeout=timeout)
                    value = (
                        reply if isinstance(reply, list)
                        else reply.get("factors", []) if isinstance(reply, dict)
                        else []
                    )
                    if not isinstance(value, list) or not value:
                        raise LLMError(
                            "模型没有返回候选因子数组",
                            code="empty_candidates",
                            retryable=True,
                        )
                    candidates = value
                    final_error = None
                    self._emit(on_event, {
                        "type": "llm_response_received",
                        "round": round_number,
                        "rounds": rounds,
                        "attempt": attempt,
                        "candidate_count": len(candidates),
                    })
                    break
                except LLMError as exc:
                    final_error = exc
                    can_retry = exc.retryable and attempt < max_attempts
                    self._emit(on_event, {
                        "type": "llm_attempt_failed",
                        "round": round_number,
                        "rounds": rounds,
                        "attempt": attempt,
                        "max_attempts": max_attempts,
                        "timeout_seconds": int(timeout),
                        "error_code": exc.code,
                        "message": str(exc),
                        "retryable": bool(can_retry),
                    })
                    if not can_retry:
                        break
                    fallback = retry_backoff[min(attempt - 1, len(retry_backoff) - 1)]
                    delay = min(60.0, max(float(exc.retry_after or 0.0), float(fallback)))
                    self._emit(on_event, {
                        "type": "llm_retry_scheduled",
                        "round": round_number,
                        "rounds": rounds,
                        "attempt": attempt,
                        "next_attempt": attempt + 1,
                        "retry_in_seconds": delay,
                        "error_code": exc.code,
                        "message": str(exc),
                    })
                    self._wait(delay, cancelled)

            if final_error is not None:
                usable = [item for item in all_results if not item.error]
                if not usable:
                    raise final_error
                warnings.append({
                    "code": "llm_round_incomplete",
                    "round": round_number,
                    "rounds": rounds,
                    "attempts": max_attempts,
                    "message": (
                        f"第 {round_number}/{rounds} 轮在 {max_attempts} 次尝试后仍未完成："
                        f"{final_error}"
                    ),
                    "error_code": final_error.code,
                })
                break

            round_results = []
            for index, item in enumerate(candidates, start=1):
                if not isinstance(item, dict) or "expression" not in item:
                    continue
                result = self._validate(
                    str(item["expression"]), str(item.get("rationale", "")), panel, fwd,
                )
                round_results.append(result)
                self._emit(on_event, {
                    "type": "llm_candidate_checked",
                    "round": round_number,
                    "rounds": rounds,
                    "candidate": index,
                    "candidate_count": len(candidates),
                    "dsl_valid": not bool(result.error),
                    "threshold_passed": bool(result.valid),
                })
            all_results.extend(round_results)
            feedback = round_results
            rounds_completed += 1
            self._emit(on_event, {
                "type": "llm_round_completed",
                "round": round_number,
                "rounds": rounds,
                "received": len(candidates),
                "dsl_valid": sum(1 for item in round_results if not item.error),
                "threshold_passed": sum(1 for item in round_results if item.valid),
            })

        seen: dict[str, LLMMinedFactor] = {}
        for result in all_results:
            seen.setdefault(result.expression, result)
        factors = sorted(seen.values(), key=lambda item: abs(item.ic_mean), reverse=True)
        if not any(not item.error for item in factors):
            raise LLMError(
                "AI 已响应，但没有返回可用的安全 DSL 因子",
                code="no_usable_factors",
            )
        return LLMMiningReport(
            factors=factors,
            rounds_requested=rounds,
            rounds_completed=rounds_completed,
            attempts=attempts_total,
            warnings=warnings,
        )

    def mine(self, panel: PanelDict, n: int = 8, rounds: int = 2,
             periods: int = 1) -> list[LLMMinedFactor]:
        return self.mine_report(panel, n=n, rounds=rounds, periods=periods).factors
