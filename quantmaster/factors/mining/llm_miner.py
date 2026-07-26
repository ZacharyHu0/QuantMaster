"""LLM 因子挖掘：让大模型提出候选因子表达式，本地用数据严格验证。

流程：
1. 把可用字段、算子、已有因子与其表现发给 LLM，请它提出 N 个新表达式；
2. 每个表达式经 ExpressionFactor 的 AST 白名单校验（LLM 输出不可信，
   非法表达式直接丢弃，不存在代码注入风险）；
3. 本地计算 RankIC 验证，只保留达标的因子；
4. 可多轮迭代：把上一轮结果反馈给 LLM 继续改进。
"""

from __future__ import annotations

from dataclasses import dataclass

from quantmaster.ai.llm import LLMClient
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
            return LLMMinedFactor(expression, f"{rationale}（无效: {e}）", 0.0, 0.0, False)

    def mine(self, panel: PanelDict, n: int = 8, rounds: int = 2,
             periods: int = 1) -> list[LLMMinedFactor]:
        system = SYSTEM_PROMPT.format(operators=", ".join(sorted(OPERATORS)))
        fwd = forward_returns(panel["close"], periods=periods)
        all_results: list[LLMMinedFactor] = []
        feedback: list[LLMMinedFactor] | None = None

        for _ in range(rounds):
            reply = self.client.chat_json(self._prompt(n, feedback), system=system)
            candidates = reply if isinstance(reply, list) else reply.get("factors", [])
            round_results = []
            for item in candidates:
                if not isinstance(item, dict) or "expression" not in item:
                    continue
                result = self._validate(
                    str(item["expression"]), str(item.get("rationale", "")), panel, fwd
                )
                round_results.append(result)
            all_results.extend(round_results)
            feedback = round_results

        seen: dict[str, LLMMinedFactor] = {}
        for r in all_results:
            seen.setdefault(r.expression, r)
        return sorted(seen.values(), key=lambda r: abs(r.ic_mean), reverse=True)
