"""回测与模拟盘共享的不可变策略配置。"""

from __future__ import annotations

import hashlib
from datetime import date
from typing import Annotated, Any, Literal

from pydantic import ConfigDict, Field, model_validator

from quantmaster.runtime.contracts import ContractModel
from quantmaster.runtime.json import strict_json_dumps


def canonical_json(value: object) -> str:
    return strict_json_dumps(value, sort_keys=True)


def content_hash(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


class FactorStrategySpec(ContractModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["factor"] = "factor"
    factor: str = Field("mom_20d", min_length=1, max_length=500)
    top_n: int = Field(5, ge=1, le=200)
    rebalance: Literal["D", "W", "M"] = "W"
    weighting: Literal["equal", "ic"] = "equal"
    cap_weight: float = Field(0.35, gt=0, le=1)

    @model_validator(mode="after")
    def validate_factors(self):
        names = [item.strip() for item in self.factor.split(",") if item.strip()]
        if not names:
            raise ValueError("因子策略至少需要一个因子")
        if len(names) > 20:
            raise ValueError("一次最多组合 20 个因子")
        if len(names) == 1 and self.weighting != "equal":
            raise ValueError("单因子策略不需要合成方式，请使用 equal")
        return self


class SwingStrategySpec(ContractModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["swing"] = "swing"
    top_n: int = Field(5, ge=1, le=50)
    holding_days: int = Field(3, ge=1, le=7)
    cap_weight: float = Field(0.25, gt=0, le=1)


class DecisionStrategySpec(ContractModel):
    """Hybrid v2 决策策略；policy_snapshot 在进入任务账本前由服务端固化。"""

    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["decision"] = "decision"
    profile: Literal["risk_adjusted", "short_term", "stable"] = "risk_adjusted"
    top_n: int = Field(5, ge=1, le=50)
    holding_days: Literal[1, 3, 5, 7] = 3
    cap_weight: float = Field(0.25, gt=0, le=1)
    policy_snapshot: dict[str, Any] = Field(default_factory=dict)


class LabVersionStrategySpec(ContractModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["lab_version"] = "lab_version"
    version_id: str = Field(min_length=1, max_length=64)
    horizon: Literal[1, 3, 5, 7] = 3
    top_n: int = Field(20, ge=1, le=200)
    rebalance_days: int = Field(3, ge=1, le=20)
    cap_weight: float = Field(0.10, gt=0, le=1)


StrategySpec = Annotated[
    FactorStrategySpec | SwingStrategySpec | DecisionStrategySpec | LabVersionStrategySpec,
    Field(discriminator="kind"),
]


class BacktestSpec(ContractModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field("", max_length=80)
    strategy: StrategySpec
    universe: str = Field("demo", min_length=1, max_length=40)
    start: str = "2022-01-01"
    end: str | None = None
    benchmark: str | None = Field("000300.SH", max_length=40)
    initial_capital: float = Field(1_000_000.0, ge=10_000, le=10_000_000_000)
    stop_loss: float | None = Field(None, gt=0, le=0.5)
    take_profit: float | None = Field(None, gt=0, le=2)
    allow_partial: bool = False
    research_tier: Literal["auto", "production", "sandbox"] = "auto"

    @model_validator(mode="after")
    def validate_dates(self):
        try:
            start = date.fromisoformat(self.start)
            end = date.fromisoformat(self.end) if self.end else date.today()
        except ValueError as exc:
            raise ValueError("开始和结束日期必须使用 YYYY-MM-DD 格式") from exc
        if start >= end:
            raise ValueError("结束日期必须晚于开始日期")
        return self

    @property
    def snapshot_hash(self) -> str:
        return content_hash(self.model_dump(mode="json"))


class PaperAccountSpec(ContractModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., min_length=1, max_length=40)
    strategy: StrategySpec
    universe: str = Field("demo", min_length=1, max_length=40)
    initial_capital: float = Field(1_000_000.0, ge=10_000, le=10_000_000_000)
    mode: Literal["manual", "auto"] = "manual"
    source_backtest_id: str = Field("", max_length=64)

    @property
    def strategy_hash(self) -> str:
        return content_hash({
            "strategy": self.strategy.model_dump(mode="json"),
            "universe": self.universe,
        })


def pin_decision_strategy(
    spec: FactorStrategySpec | SwingStrategySpec | DecisionStrategySpec | LabVersionStrategySpec,
    universe: str,
    *,
    symbols: list[str] | None = None,
) -> FactorStrategySpec | SwingStrategySpec | DecisionStrategySpec | LabVersionStrategySpec:
    """Resolve or verify the immutable Hybrid policy used by a stored experiment."""
    if not isinstance(spec, DecisionStrategySpec):
        return spec
    if not spec.policy_snapshot:
        from quantmaster.decision import resolve_policy

        if symbols is None:
            try:
                from quantmaster.data.universe import load_universe

                symbols = load_universe(universe)
            except Exception:
                symbols = None
        snapshot = resolve_policy(
            universe, spec.holding_days, spec.profile, symbols=symbols,
        )
        return spec.model_copy(update={"policy_snapshot": snapshot})
    snapshot = dict(spec.policy_snapshot)
    if snapshot.get("profile") != spec.profile:
        raise ValueError("决策策略画像与模型快照不一致")
    if int(snapshot.get("horizon", 0)) != spec.holding_days:
        raise ValueError("决策持有期与模型快照不一致")
    if snapshot.get("universe") != universe:
        raise ValueError("决策候选与模型快照不一致")
    supplied_hash = str(snapshot.get("policy_hash") or "")
    payload = dict(snapshot)
    payload.pop("policy_hash", None)
    payload.pop("model_version", None)
    if not supplied_hash or content_hash(payload) != supplied_hash:
        raise ValueError("决策模型快照完整性校验失败")
    return spec


def build_strategy(
    spec: FactorStrategySpec | SwingStrategySpec | DecisionStrategySpec | LabVersionStrategySpec,
    symbols: list[str],
    start: str,
    end: str,
    *,
    universe: str = "demo",
):
    """把稳定快照解析成当前运行时策略对象。"""
    from quantmaster.backtest.strategy import (
        FactorStrategy,
        LabVersionStrategy,
        MultiFactorStrategy,
        SwingStrategy,
    )

    if isinstance(spec, LabVersionStrategySpec):
        from quantmaster.lab.store import LabStore

        version = LabStore().version(spec.version_id)
        if version is None:
            raise KeyError("Quant Lab 版本不存在")
        if spec.horizon not in tuple((version.get("spec") or {}).get("horizons") or ()):
            raise ValueError(f"Quant Lab 版本没有 {spec.horizon} 日预测头")
        return LabVersionStrategy(
            version, horizon=spec.horizon, top_n=spec.top_n,
            rebalance_days=spec.rebalance_days, cap_weight=spec.cap_weight,
        )

    if isinstance(spec, SwingStrategySpec):
        return SwingStrategy(
            top_n=spec.top_n, holding_days=spec.holding_days, cap_weight=spec.cap_weight,
        )
    if isinstance(spec, DecisionStrategySpec):
        from quantmaster.decision import HybridDecisionStrategy

        pinned = pin_decision_strategy(spec, universe)
        return HybridDecisionStrategy(
            top_n=pinned.top_n,
            holding_days=pinned.holding_days,
            profile=pinned.profile,
            universe=universe,
            policy_snapshot=pinned.policy_snapshot,
            cap_weight=pinned.cap_weight,
        )
    from quantmaster.factors.fundamental import resolve_factor

    names = [item.strip() for item in spec.factor.split(",") if item.strip()]
    factors = [resolve_factor(name, symbols, start, end) for name in names]
    if len(factors) == 1:
        return FactorStrategy(
            factors[0], top_n=spec.top_n, rebalance=spec.rebalance,
            cap_weight=spec.cap_weight,
        )
    return MultiFactorStrategy(
        factors, top_n=spec.top_n, rebalance=spec.rebalance,
        cap_weight=spec.cap_weight, weighting=spec.weighting,
    )


def signal_is_due(
    spec: FactorStrategySpec | SwingStrategySpec | DecisionStrategySpec | LabVersionStrategySpec,
    dates,
    position: int,
) -> bool:
    """判断某个已完成交易日是否应生成新信号，不把滚动窗口末行误当调仓日。"""
    import pandas as pd

    index = pd.DatetimeIndex(dates)
    if position < 0 or position >= len(index):
        return False
    current = index[position]
    if isinstance(spec, (SwingStrategySpec, DecisionStrategySpec, LabVersionStrategySpec)):
        if isinstance(spec, LabVersionStrategySpec):
            return position % spec.rebalance_days == 0
        return position % spec.holding_days == 0
    if spec.rebalance == "D":
        return True
    if spec.rebalance == "W":
        return current.weekday() == 4
    return (current + pd.offsets.BDay(1)).month != current.month
