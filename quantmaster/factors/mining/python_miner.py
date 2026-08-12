"""由 LLM 提案、完全由本地密封数据筛选的受限 Python 因子矿工。"""

from __future__ import annotations

import json
import math
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from typing import Any

import pandas as pd

from quantmaster.factors.analysis import (
    forward_returns,
    information_coefficient,
    top_quantile_turnover,
)
from quantmaster.factors.python_artifact import (
    PythonFactorPolicyError,
    RestrictedPythonRunner,
    validate_python_factor,
)
from quantmaster.lab.research import (
    benjamini_hochberg_family,
    compare_prefixes,
    recursive_stability,
    sealed_three_way_split,
)

AUTOMINER_SYSTEM_PROMPT = """你是 A 股横截面量化因子研究助手。你只能提出受限 Python 因子，
不能访问文件、网络、环境变量或原始样本。候选必须定义 compute(features, params)，仅使用
pandas/numpy 向量运算并返回与 features['close'] 完全对齐的 DataFrame。禁止负向 shift、
全样本统计、未来信息、循环、导入和任何 I/O。优化只能使用 TRAIN，筛选只能使用 VALID，
TEST 永远不可见。优先选择经济含义清楚、参数处于稳定平台、成本后仍有边际的候选。"""


@dataclass
class PythonMiningCandidate:
    id: str
    name: str
    hypothesis: str
    objective: str
    required_features: list[str]
    warmup: int
    parameters: list[dict[str, Any]]
    code: str
    status: str = "proposed"
    error: str = ""
    selected_params: dict[str, Any] = field(default_factory=dict)
    audit: dict[str, Any] = field(default_factory=dict)
    train_metrics: dict[str, Any] = field(default_factory=dict)
    valid_metrics: dict[str, Any] = field(default_factory=dict)
    test_metrics: dict[str, Any] = field(default_factory=dict)
    pareto_rank: int | None = None
    factor_version_id: str = ""
    artifact: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class PythonMiningReport:
    split: dict[str, Any]
    candidates: list[PythonMiningCandidate]
    finalists: list[PythonMiningCandidate]
    rounds_requested: int
    rounds_completed: int
    llm_calls: int
    warnings: list[dict[str, str]] = field(default_factory=list)


def _candidate_metrics(
    values: pd.DataFrame, close: pd.DataFrame, horizon: int,
) -> dict[str, Any]:
    values, close = values.align(close, join="inner")
    denominator = max(1, int(close.notna().sum().sum()))
    coverage = float((values.notna() & close.notna()).sum().sum()) / denominator
    daily_ic = information_coefficient(values, forward_returns(close, periods=horizon)).dropna()
    mean = float(daily_ic.mean()) if len(daily_ic) else 0.0
    std = float(daily_ic.std()) if len(daily_ic) > 1 else 0.0
    icir = mean / std if std > 0 else 0.0
    z_score = abs(mean) / (std / math.sqrt(len(daily_ic))) if std > 0 and len(daily_ic) > 1 else 0.0
    p_value = min(1.0, math.erfc(z_score / math.sqrt(2))) if z_score else 1.0
    raw_turnover = top_quantile_turnover(values * (1 if mean >= 0 else -1))
    turnover = float(raw_turnover.fillna(0).mean()) if isinstance(raw_turnover, pd.Series) \
        else float(raw_turnover or 0.0)
    return {
        "days": len(daily_ic), "coverage": round(coverage, 6),
        "rank_ic": round(mean, 6), "icir": round(icir, 6),
        "positive_ratio": round(float((daily_ic * (1 if mean >= 0 else -1) > 0).mean()), 6)
        if len(daily_ic) else 0.0,
        "turnover_daily": round(turnover / max(1, horizon), 6),
        "p_value": round(p_value, 6), "q_value": round(p_value, 6),
    }


def _slice(frame: pd.DataFrame, split: dict[str, Any], name: str) -> pd.DataFrame:
    item = split[name]
    return frame.loc[str(item["start"]):str(item["end"])]


def _feature_slice(
    features: dict[str, pd.DataFrame], split: dict[str, Any], name: str,
) -> dict[str, pd.DataFrame]:
    return {key: _slice(value, split, name) for key, value in features.items()}


def _normalize_parameters(raw: Any) -> list[dict[str, Any]]:
    result = []
    for item in raw if isinstance(raw, list) else []:
        if not isinstance(item, dict) or not str(item.get("name", "")).isidentifier():
            continue
        name = str(item["name"])
        default = item.get("default")
        low, high = item.get("min", default), item.get("max", default)
        if not all(isinstance(value, (int, float, bool)) for value in (default, low, high)):
            continue
        if float(low) > float(high) or not float(low) <= float(default) <= float(high):
            continue
        result.append({"name": name, "default": default, "min": low, "max": high})
    return result[:8]


def _parameter_variants(parameters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    base = {item["name"]: item["default"] for item in parameters}
    result = [base]
    for item in parameters:
        for value in (item["min"], item["max"]):
            variant = dict(base)
            variant[item["name"]] = value
            if variant not in result:
                result.append(variant)
            if len(result) >= 12:
                return result
    return result


def _quality(metrics: dict[str, Any]) -> float:
    return (
        0.45 * min(1.0, abs(float(metrics["rank_ic"])) / 0.05)
        + 0.20 * min(1.0, abs(float(metrics["icir"])) / 0.5)
        + 0.15 * float(metrics["positive_ratio"])
        + 0.10 * float(metrics["coverage"])
        + 0.10 * max(0.0, 1.0 - float(metrics["turnover_daily"]))
    )


def _pareto(candidates: list[PythonMiningCandidate]) -> list[PythonMiningCandidate]:
    def dimensions(item: PythonMiningCandidate) -> tuple[float, ...]:
        train, valid = item.train_metrics, item.valid_metrics
        retention = min(1.0, abs(valid["rank_ic"]) / max(1e-9, abs(train["rank_ic"])))
        sign = 1.0 if train["rank_ic"] * valid["rank_ic"] > 0 else 0.0
        return (_quality(valid), retention * sign, 1.0 - valid["turnover_daily"], valid["coverage"])

    remaining, ordered, rank = list(candidates), [], 1
    while remaining:
        front = []
        for candidate in remaining:
            point = dimensions(candidate)
            dominated = any(
                all(a >= b for a, b in zip(dimensions(other), point, strict=True))
                and any(a > b for a, b in zip(dimensions(other), point, strict=True))
                for other in remaining if other is not candidate
            )
            if not dominated:
                candidate.pareto_rank = rank
                front.append(candidate)
        front.sort(key=lambda item: (
            -_quality(item.valid_metrics), item.valid_metrics["turnover_daily"], item.id,
        ))
        ordered.extend(front)
        remaining = [item for item in remaining if item not in front]
        rank += 1
    return ordered


class PythonFactorMiner:
    def __init__(self, client=None, runner: RestrictedPythonRunner | None = None):
        self.client = client
        self.runner = runner or RestrictedPythonRunner()

    def _client(self):
        if self.client is None:
            from quantmaster.ai.llm import LLMClient
            self.client = LLMClient()
        return self.client

    def _ask(
        self, *, count: int, round_number: int, features: list[dict[str, Any]],
        feedback: list[dict[str, Any]], horizon: int,
    ) -> list[dict[str, Any]]:
        visible = [{key: item[key] for key in (
            "name", "group", "description", "pit_grade", "coverage",
        ) if key in item} for item in features if item.get("available")]
        prompt = {
            "task": f"提出最多 {count} 个互不重复的 {horizon} 日横截面因子",
            "round": round_number,
            "feature_registry": visible,
            "previous_local_metrics": feedback[-12:],
            "response_schema": {"candidates": [{
                "name": "中文短名", "hypothesis": "经济假设", "objective": "优化目标",
                "required_features": ["close"], "warmup": 60,
                "parameters": [{"name": "window", "default": 20, "min": 10, "max": 40}],
                "code": "def compute(features, params):\\n    return ...",
            }]},
        }
        response = self._client().chat_json(
            json.dumps(prompt, ensure_ascii=False), system=AUTOMINER_SYSTEM_PROMPT, timeout=240,
        )
        items = response.get("candidates", []) if isinstance(response, dict) else response
        return [item for item in items if isinstance(item, dict)][:count]

    def mine_report(
        self, features: dict[str, pd.DataFrame], feature_catalog: list[dict[str, Any]], *,
        horizon: int = 3, rounds: int = 3, candidate_limit: int = 24, finalists: int = 3,
        progress: Callable[..., None] | None = None,
        cancelled: Callable[[], bool] | None = None,
        on_candidate: Callable[[PythonMiningCandidate], None] | None = None,
    ) -> PythonMiningReport:
        rounds = min(3, max(1, int(rounds)))
        candidate_limit = min(24, max(1, int(candidate_limit)))
        finalists = min(3, max(1, int(finalists)))
        close = features["close"]
        split = sealed_three_way_split(close.index, purge_gap=max(7, horizon))
        known = {item["name"] for item in feature_catalog if item.get("available")}
        candidates: list[PythonMiningCandidate] = []
        feedback: list[dict[str, Any]] = []
        seen: set[str] = set()
        completed = 0
        llm_calls = 0
        warnings: list[dict[str, str]] = []
        per_round = math.ceil(candidate_limit / rounds)

        for round_number in range(1, rounds + 1):
            if cancelled and cancelled():
                raise InterruptedError("研究任务已请求取消")
            remaining = candidate_limit - len(candidates)
            if remaining <= 0:
                break
            if progress:
                progress(55 + int(18 * (round_number - 1) / rounds),
                         f"Python AutoMiner · 第 {round_number}/{rounds} 轮提案")
            try:
                llm_calls += 1
                proposals = self._ask(
                    count=min(per_round, remaining), round_number=round_number,
                    features=feature_catalog, feedback=feedback, horizon=horizon,
                )
                completed += 1
            except InterruptedError:
                raise
            except Exception as exc:
                warnings.append({"code": "llm_round_failed", "message": str(exc)[:500]})
                continue
            for raw in proposals:
                if cancelled and cancelled():
                    raise InterruptedError("研究任务已请求取消")
                code = str(raw.get("code") or "").strip()
                try:
                    candidate_id = validate_python_factor(code)["sha256"][:16]
                except Exception:
                    candidate_id = f"invalid-{round_number}-{len(candidates)}"
                if candidate_id in seen:
                    continue
                seen.add(candidate_id)
                candidate = PythonMiningCandidate(
                    id=candidate_id, name=str(raw.get("name") or f"候选 {len(candidates)+1}")[:120],
                    hypothesis=str(raw.get("hypothesis") or "")[:2000],
                    objective=str(raw.get("objective") or "")[:1000],
                    required_features=[str(item) for item in raw.get("required_features", [])][:24],
                    warmup=min(504, max(1, int(raw.get("warmup") or 60))),
                    parameters=_normalize_parameters(raw.get("parameters")), code=code,
                )
                candidates.append(candidate)
                try:
                    policy = validate_python_factor(code)
                    referenced = set(policy.get("features") or [])
                    undeclared = sorted(referenced - set(candidate.required_features))
                    if undeclared:
                        raise PythonFactorPolicyError(
                            f"代码引用了未声明特征: {', '.join(undeclared)}"
                        )
                    missing = sorted(set(candidate.required_features) - known)
                    if missing:
                        raise PythonFactorPolicyError(f"未注册或不可用特征: {', '.join(missing)}")
                    train_features = _feature_slice(features, split, "train")
                    valid_features = _feature_slice(features, split, "valid")
                    plateau = []
                    for params in _parameter_variants(candidate.parameters):
                        if cancelled and cancelled():
                            raise InterruptedError("研究任务已请求取消")
                        train_values = self.runner.execute(code, train_features, params)
                        train_metrics = _candidate_metrics(train_values, train_features["close"], horizon)
                        valid_values = self.runner.execute(code, valid_features, params)
                        valid_metrics = _candidate_metrics(valid_values, valid_features["close"], horizon)
                        same_sign = train_metrics["rank_ic"] * valid_metrics["rank_ic"] > 0
                        value = _quality(valid_metrics) + (0.15 if same_sign else -0.25)
                        plateau.append({"params": params, "train": train_metrics,
                                        "valid": valid_metrics, "score": round(value, 6)})
                    if not plateau:
                        raise PythonFactorPolicyError("没有可执行参数组合")
                    best_score = max(item["score"] for item in plateau)
                    stable = [item for item in plateau if item["score"] >= best_score - 0.12]
                    if candidate.parameters and len(stable) < min(2, len(plateau)):
                        raise PythonFactorPolicyError("参数最优点是孤立尖峰，未形成稳定平台")
                    default_params = {
                        item["name"]: item["default"] for item in candidate.parameters
                    }
                    chosen = next(
                        (item for item in stable if item["params"] == default_params), stable[0],
                    )
                    candidate.selected_params = chosen["params"]
                    candidate.train_metrics = chosen["train"]
                    candidate.valid_metrics = chosen["valid"]
                    # 防前视：在 TRAIN 内重复截断；防递归：不同 warm-up 重算同一末端。
                    full = self.runner.execute(code, train_features, candidate.selected_params)
                    prefixes = []
                    for ratio in (0.55, 0.70, 0.85):
                        length = max(candidate.warmup + 20, int(len(full) * ratio))
                        if length >= len(full):
                            continue
                        truncated = {key: value.iloc[:length] for key, value in train_features.items()}
                        prefixes.append(compare_prefixes(
                            full.iloc[:length], self.runner.execute(code, truncated,
                                                                   candidate.selected_params),
                        ))
                    warmups: dict[int, pd.Series] = {}
                    for length in sorted(set((candidate.warmup + 20, candidate.warmup * 2,
                                              candidate.warmup * 4))):
                        length = min(len(full), max(40, length))
                        subset = {key: value.iloc[-length:] for key, value in train_features.items()}
                        warmups[length] = self.runner.execute(
                            code, subset, candidate.selected_params,
                        ).iloc[-1]
                    recursive = recursive_stability(warmups) if len(warmups) >= 2 else {
                        "passed": True, "comparisons": [],
                    }
                    candidate.audit = {
                        "static": validate_python_factor(code),
                        "lookahead": {"passed": bool(prefixes) and all(x["passed"] for x in prefixes),
                                      "prefixes": prefixes},
                        "recursive": recursive,
                        "parameter_plateau": {
                            "passed": True, "stable_variants": len(stable),
                            "tested_variants": len(plateau), "selected": candidate.selected_params,
                            "variants": plateau,
                        },
                    }
                    if not candidate.audit["lookahead"]["passed"] or not recursive["passed"]:
                        raise PythonFactorPolicyError("前视或递归稳定性审计未通过")
                    candidate.status = "validated"
                    feedback.append({"name": candidate.name, "status": "validated",
                                     "valid_metrics": candidate.valid_metrics})
                except InterruptedError:
                    raise
                except Exception as exc:
                    candidate.status, candidate.error = "rejected", str(exc)[:1000]
                    feedback.append({"name": candidate.name, "status": "rejected",
                                     "reason": candidate.error})
                if on_candidate:
                    on_candidate(candidate)
                if progress:
                    progress(
                        min(88, 60 + int(28 * len(candidates) / candidate_limit)),
                        f"本地策略审计 · {len(candidates)}/{candidate_limit}",
                        f"{candidate.name} · {candidate.status}",
                    )

        eligible = [item for item in candidates if item.status == "validated"]
        q_values = benjamini_hochberg_family([item.valid_metrics["p_value"] for item in eligible])
        for candidate, q_value in zip(eligible, q_values, strict=True):
            candidate.valid_metrics["q_value"] = round(float(q_value), 6)
        ordered = _pareto(eligible)
        selected = ordered[:finalists]
        if not selected:
            warnings.append({
                "code": "no_finalists",
                "message": "没有候选同时通过代码策略、参数平台与防前视审计",
            })
        # 排序在这里冻结；下面才首次打开 TEST，且测试结果不参与重排。
        if progress:
            progress(90, f"Pareto 顺序已冻结 · 开启 {len(selected)} 个密封 TEST")
        test_features = _feature_slice(features, split, "test")
        for candidate in selected:
            if cancelled and cancelled():
                raise InterruptedError("研究任务已请求取消")
            try:
                values = self.runner.execute(candidate.code, test_features, candidate.selected_params)
                candidate.test_metrics = _candidate_metrics(values, test_features["close"], horizon)
                candidate.status = "finalist"
            except Exception as exc:
                candidate.status, candidate.error = "test_failed", str(exc)[:1000]
            if on_candidate:
                on_candidate(candidate)
        return PythonMiningReport(
            split=split, candidates=candidates, finalists=selected,
            rounds_requested=rounds, rounds_completed=completed, llm_calls=llm_calls,
            warnings=warnings,
        )
