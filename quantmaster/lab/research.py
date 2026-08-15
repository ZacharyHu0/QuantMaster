"""可复现的滚动研究协议、假设族校正与防泄漏审计。"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal

import numpy as np
import pandas as pd

from quantmaster.horizons import MAX_HORIZON, SUPPORTED_HORIZONS
from quantmaster.lab.errors import LabError
from quantmaster.lab.models import content_hash

HORIZONS = SUPPORTED_HORIZONS
DEEP_MODELS = ("multi-transformer", "multi-tcn", "multi-gru")
MODEL_FAMILIES = (*DEEP_MODELS, "ridge")


@dataclass(frozen=True)
class WalkForwardSpec:
    """研究、选参与密封评估共用的不可变时间协议。"""

    train_window: int = 756
    test_window: int = 244
    step_days: int = 244
    purge_gap: int = MAX_HORIZON
    development_folds: int = 3
    horizons: tuple[int, ...] = HORIZONS
    seed: int = 42

    def __post_init__(self) -> None:
        if self.train_window < 120:
            raise ValueError("train_window 至少为 120 个交易日")
        if self.test_window < 20 or self.step_days < 1:
            raise ValueError("test_window 至少为 20，step_days 必须为正")
        if self.purge_gap < max(self.horizons, default=0):
            raise ValueError("purge_gap 不能短于最长预测周期")
        if self.development_folds < 3:
            raise ValueError("开发期至少需要 3 个样本外窗口")
        if not self.horizons or any(value not in HORIZONS for value in self.horizons):
            raise ValueError("horizons 只支持 1/3/5/7/10/20/30 日")

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["horizons"] = list(self.horizons)
        return value

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> WalkForwardSpec:
        payload = dict(value or {})
        if "horizons" in payload:
            payload["horizons"] = tuple(int(item) for item in payload["horizons"])
        return cls(**payload)

    @classmethod
    def from_lab_config(
        cls, config: Any, *, horizons: tuple[int, ...] | None = None,
    ) -> WalkForwardSpec:
        """把持久化 Lab 设置投影为一次运行使用的不可变协议。"""
        selected = tuple(horizons or config.horizons)
        return cls(
            train_window=int(config.walk_forward_train_days),
            test_window=int(config.walk_forward_test_days),
            step_days=int(config.walk_forward_step_days),
            purge_gap=int(config.walk_forward_purge_days),
            development_folds=int(config.walk_forward_folds),
            horizons=selected,
        )

    @property
    def required_days(self) -> int:
        """开发期 OOS 窗口加一个末尾密封窗口所需的最少交易日。"""
        return (
            self.train_window + 2 * self.purge_gap + 2 * self.test_window
            + (self.development_folds - 1) * self.step_days
        )


@dataclass(frozen=True)
class FeatureSetSpec:
    groups: tuple[str, ...] = (
        "price_volume_v2", "market_context_v1", "pit_fundamental_v1",
    )
    include_news: bool = False
    minimum_coverage: float = 0.80

    def __post_init__(self) -> None:
        allowed = {
            "price_volume_v2", "market_context_v1", "pit_fundamental_v1", "news_v2",
        }
        if not self.groups or set(self.groups) - allowed:
            raise ValueError("特征组包含未知版本")
        if not 0.5 <= self.minimum_coverage <= 1:
            raise ValueError("minimum_coverage 必须在 0.5–1.0 之间")

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["groups"] = list(self.groups)
        return value

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> FeatureSetSpec:
        payload = dict(value or {})
        if "groups" in payload:
            payload["groups"] = tuple(str(item) for item in payload["groups"])
        return cls(**payload)


@dataclass(frozen=True)
class OptimizationSpec:
    universe: str = "csi800"
    start: str = "2015-01-01"
    end: str = ""
    models: tuple[str, ...] = MODEL_FAMILIES
    budget_hours: float = 10.0
    max_trials: int = 40
    top_n: int = 20
    sequence_length: int = 20
    research_tier: Literal["production", "sandbox"] = "production"
    protocol: WalkForwardSpec = field(default_factory=WalkForwardSpec)
    features: FeatureSetSpec = field(default_factory=FeatureSetSpec)
    minimum_coverage: float = 0.70
    minimum_fold_sign_ratio: float = 0.75
    maximum_fdr_q: float = 0.10
    minimum_edge_cost_ratio: float = 2.0
    seed: int = 42

    def __post_init__(self) -> None:
        if not self.universe or not self.start:
            raise ValueError("优化必须提供 universe 和 start")
        if not self.models or set(self.models) - set(MODEL_FAMILIES):
            raise ValueError(f"models 只支持 {', '.join(MODEL_FAMILIES)}")
        if not 0 < self.budget_hours <= 10:
            raise ValueError("单次优化预算必须在 0–10 小时之间")
        if not 1 <= self.max_trials <= 500:
            raise ValueError("max_trials 必须在 1–500 之间")
        if not 1 <= self.top_n <= 200 or self.sequence_length < 1:
            raise ValueError("top_n 或 sequence_length 无效")
        if self.research_tier == "production" and self.universe.lower() != "csi800":
            raise ValueError("首版 production 研究只支持 PIT CSI800")

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["models"] = list(self.models)
        value["protocol"] = self.protocol.to_dict()
        value["features"] = self.features.to_dict()
        return value

    @property
    def config_hash(self) -> str:
        return content_hash(self.to_dict())

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> OptimizationSpec:
        payload = dict(value)
        payload["models"] = tuple(payload.get("models") or MODEL_FAMILIES)
        payload["protocol"] = WalkForwardSpec.from_dict(payload.get("protocol"))
        payload["features"] = FeatureSetSpec.from_dict(payload.get("features"))
        return cls(**payload)


@dataclass(frozen=True)
class TimeFold:
    name: str
    train_start: str
    train_end: str
    test_start: str
    test_end: str
    sealed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def sealed_three_way_split(
    dates: pd.DatetimeIndex, *, purge_gap: int = MAX_HORIZON,
    minimum_train: int = 504, minimum_holdout: int = 252,
) -> dict[str, dict[str, str | int]]:
    """生成 TRAIN/VALID/TEST 三段；TEST 不参与候选排序或参数选择。"""
    index = pd.DatetimeIndex(dates).normalize().unique().sort_values()
    holdout = max(minimum_holdout, int(len(index) * 0.15))
    required = minimum_train + 2 * holdout + 2 * purge_gap
    if len(index) < required:
        raise ValueError(f"三段研究至少需要 {required} 个交易日，当前只有 {len(index)}")
    test_start = len(index) - holdout
    valid_end = test_start - purge_gap
    valid_start = valid_end - holdout
    train_end = valid_start - purge_gap

    def part(start: int, stop: int) -> dict[str, str | int]:
        return {
            "start": index[start].strftime("%Y-%m-%d"),
            "end": index[stop - 1].strftime("%Y-%m-%d"),
            "days": stop - start,
        }

    return {
        "train": part(0, train_end), "valid": part(valid_start, valid_end),
        "test": part(test_start, len(index)),
        "purge_gap": {"days": purge_gap},
    }


def walk_forward_folds(dates: pd.DatetimeIndex, spec: WalkForwardSpec) -> tuple[list[TimeFold], TimeFold]:
    """生成可配置的滚动 OOS 窗口与一个永不参与选参的末尾密封区间。"""
    index = pd.DatetimeIndex(dates).normalize().unique().sort_values()
    required = spec.required_days
    if len(index) < required:
        raise LabError(
            "WALK_FORWARD_EVIDENCE_INSUFFICIENT",
            f"证据不足：滚动研究至少需要 {required} 个交易日，当前只有 {len(index)}",
            action="在设置中缩短训练、测试或步长周期，或补充更早的本地历史数据",
            retryable=True,
            context={
                "required_days": required,
                "available_days": len(index),
                "missing_days": required - len(index),
                "configurable_fields": [
                    "walk_forward_train_days", "walk_forward_test_days",
                    "walk_forward_step_days", "walk_forward_purge_days",
                ],
                "protocol": spec.to_dict(),
            },
            status_code=422,
        )
    sealed_start_pos = len(index) - spec.test_window
    development_end = sealed_start_pos - spec.purge_gap
    folds: list[TimeFold] = []
    last_test_start = development_end - spec.test_window
    first_test = last_test_start - (spec.development_folds - 1) * spec.step_days
    for number in range(spec.development_folds):
        test_start_pos = first_test + number * spec.step_days
        test_end_pos = test_start_pos + spec.test_window - 1
        train_end_pos = test_start_pos - spec.purge_gap - 1
        train_start_pos = train_end_pos - spec.train_window + 1
        folds.append(TimeFold(
            name=f"development-{number + 1}",
            train_start=index[train_start_pos].strftime("%Y-%m-%d"),
            train_end=index[train_end_pos].strftime("%Y-%m-%d"),
            test_start=index[test_start_pos].strftime("%Y-%m-%d"),
            test_end=index[test_end_pos].strftime("%Y-%m-%d"),
        ))
    sealed = TimeFold(
        name="sealed-holdout",
        train_start=index[
            sealed_start_pos - spec.purge_gap - spec.train_window
        ].strftime("%Y-%m-%d"),
        train_end=index[sealed_start_pos - spec.purge_gap - 1].strftime("%Y-%m-%d"),
        test_start=index[sealed_start_pos].strftime("%Y-%m-%d"),
        test_end=index[-1].strftime("%Y-%m-%d"),
        sealed=True,
    )
    return folds, sealed


def benjamini_hochberg_family(p_values: list[float]) -> list[float]:
    """对一次研究批次中的全部候选/周期统一执行 BH-FDR。"""
    if not p_values:
        return []
    values: Any = np.asarray(p_values, dtype=float)
    values = np.where(np.isfinite(values), np.clip(values, 0, 1), 1.0)
    order = np.argsort(values)
    ranked = values[order]
    adjusted = ranked * len(values) / np.arange(1, len(values) + 1)
    adjusted = np.minimum.accumulate(adjusted[::-1])[::-1].clip(0, 1)
    result = np.empty_like(adjusted)
    result[order] = adjusted
    return result.tolist()


def compare_prefixes(full: pd.DataFrame, prefix: pd.DataFrame, *, atol: float = 1e-10) -> dict[str, Any]:
    """比较完整计算和截断计算的共同历史，用于发现未来数据污染。"""
    left, right = full.align(prefix, join="inner")
    common = left.notna() & right.notna()
    difference = (left - right).abs().where(common)
    maximum = float(difference.max().max()) if difference.notna().any().any() else 0.0
    changed_mask = difference > atol
    changed = int(changed_mask.sum().sum())
    compared = int(common.sum().sum())
    changed_rows, changed_columns = np.where(changed_mask.to_numpy())
    first_changed_at = None
    first_changed_symbol = None
    if len(changed_rows):
        first = int(np.argmin(changed_rows))
        first_changed_at = pd.Timestamp(changed_mask.index[changed_rows[first]]).isoformat()
        first_changed_symbol = str(changed_mask.columns[changed_columns[first]])
    return {
        "passed": changed == 0,
        "maximum_difference": maximum,
        "changed_values": changed,
        "compared_values": compared,
        "first_changed_at": first_changed_at,
        "first_changed_symbol": first_changed_symbol,
    }


def recursive_stability(values: dict[int, pd.Series], *, tolerance: float = 1e-6) -> dict[str, Any]:
    """比较不同 warm-up 长度末端输出；调用方负责分别重算。"""
    ordered = sorted(values)
    if len(ordered) < 2:
        raise ValueError("递归稳定性至少需要两个 warm-up 长度")
    reference = values[ordered[-1]].astype(float)
    comparisons = []
    passed = True
    for length in ordered[:-1]:
        current, target = values[length].align(reference, join="inner")
        delta = (current - target).abs().replace([np.inf, -np.inf], np.nan)
        maximum = float(delta.max()) if delta.notna().any() else 0.0
        item_passed = maximum <= tolerance
        passed = passed and item_passed
        comparisons.append({"warmup": length, "maximum_difference": maximum, "passed": item_passed})
    return {"passed": passed, "reference_warmup": ordered[-1], "comparisons": comparisons}
