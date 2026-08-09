"""回测与模拟盘共享的不可变策略配置。"""

from __future__ import annotations

import hashlib
from datetime import date
from typing import Annotated, Any, Literal

from pydantic import ConfigDict, Field, model_validator

from quantmaster.runtime.contracts import ContractModel
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.trading_sessions import market_date


def canonical_json(value: object) -> str:
    return strict_json_dumps(value, sort_keys=True)


def content_hash(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def split_factor_references(value: str) -> list[str]:
    """Split a factor list only on top-level commas inside expression-safe syntax."""
    result: list[str] = []
    current: list[str] = []
    depth = 0
    quote = ""
    escaped = False
    for character in str(value):
        if escaped:
            current.append(character)
            escaped = False
            continue
        if quote:
            current.append(character)
            if character == "\\":
                escaped = True
            elif character == quote:
                quote = ""
            continue
        if character in {"'", '"'}:
            quote = character
            current.append(character)
        elif character in "([{":
            depth += 1
            current.append(character)
        elif character in ")]}":
            depth -= 1
            if depth < 0:
                raise ValueError("因子表达式括号不匹配")
            current.append(character)
        elif character == "," and depth == 0:
            item = "".join(current).strip()
            if item:
                result.append(item)
            current = []
        else:
            current.append(character)
    if quote or depth:
        raise ValueError("因子表达式引号或括号不完整")
    item = "".join(current).strip()
    if item:
        result.append(item)
    return result


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
        names = split_factor_references(self.factor)
        if not names:
            raise ValueError("因子策略至少需要一个因子")
        if len(names) > 20:
            raise ValueError("一次最多组合 20 个因子")
        if len(names) == 1 and self.weighting != "equal":
            raise ValueError("单因子策略不需要合成方式，请使用 equal")
        return self


class DecisionStrategySpec(ContractModel):
    """Hybrid 决策策略；policy_snapshot 在进入任务账本前由服务端固化。"""

    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["decision"] = "decision"
    profile: Literal["risk_adjusted", "short_term", "stable"] = "risk_adjusted"
    top_n: int = Field(5, ge=1, le=50)
    holding_days: Literal[1, 3, 5, 7, 10, 20, 30] = 3
    cap_weight: float = Field(0.25, gt=0, le=1)
    policy_snapshot: dict[str, Any] = Field(default_factory=dict)


class LabVersionStrategySpec(ContractModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["lab_version"] = "lab_version"
    version_id: str = Field(min_length=1, max_length=64)
    horizon: Literal[1, 3, 5, 7, 10, 20, 30] = 3
    top_n: int = Field(20, ge=1, le=200)
    rebalance_days: int = Field(3, ge=1, le=20)
    cap_weight: float = Field(0.10, gt=0, le=1)


StrategySpec = Annotated[
    FactorStrategySpec | DecisionStrategySpec | LabVersionStrategySpec,
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
            end = date.fromisoformat(self.end) if self.end else market_date()
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
    spec: FactorStrategySpec | DecisionStrategySpec | LabVersionStrategySpec,
    universe: str,
    *,
    symbols: list[str] | None = None,
) -> FactorStrategySpec | DecisionStrategySpec | LabVersionStrategySpec:
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
            mode="retrospective",
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
    spec: FactorStrategySpec | DecisionStrategySpec | LabVersionStrategySpec,
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

    names = split_factor_references(spec.factor)
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


def preflight_strategy(spec: BacktestSpec) -> None:
    """Resolve strategy references locally before a durable run is accepted."""
    strategy = spec.strategy
    if isinstance(strategy, LabVersionStrategySpec):
        from quantmaster.lab.store import LabStore

        version = LabStore().version(strategy.version_id)
        if version is None:
            raise ValueError("Quant Lab 版本不存在")
        if strategy.horizon not in tuple((version.get("spec") or {}).get("horizons") or ()):
            raise ValueError(f"Quant Lab 版本没有 {strategy.horizon} 日预测头")
        return
    if not isinstance(strategy, FactorStrategySpec):
        return

    from quantmaster.factors.base import ExpressionFactor
    from quantmaster.factors.fundamental import FUNDAMENTAL_FIELD_BY_FACTOR
    from quantmaster.factors.library import BUILTIN_FACTORS
    from quantmaster.lab.store import LabStore

    for reference in split_factor_references(strategy.factor):
        if reference in BUILTIN_FACTORS or reference in FUNDAMENTAL_FIELD_BY_FACTOR:
            continue
        stored = LabStore().factor_reference(reference)
        if stored is not None:
            if stored.get("kind") != "expression":
                raise ValueError(
                    f"Quant Lab 因子“{reference}”不可直接执行；请改用 Lab 版本策略"
                )
            expression = str((stored.get("spec") or {}).get("expression") or "").strip()
            if not expression:
                raise ValueError(f"Quant Lab 因子“{reference}”没有可执行表达式")
            ExpressionFactor(expression)
            continue
        ExpressionFactor(reference)


def signal_is_due(
    spec: FactorStrategySpec | DecisionStrategySpec | LabVersionStrategySpec,
    dates,
    position: int,
) -> bool:
    """判断某个已完成交易日是否应生成新信号，不把滚动窗口末行误当调仓日。"""
    import pandas as pd

    index = pd.DatetimeIndex(dates)
    if position < 0 or position >= len(index):
        return False
    current = index[position]
    if isinstance(spec, (DecisionStrategySpec, LabVersionStrategySpec)):
        if isinstance(spec, LabVersionStrategySpec):
            return position % spec.rebalance_days == 0
        return position % spec.holding_days == 0
    if spec.rebalance == "D":
        return True
    if spec.rebalance == "W":
        return current.weekday() == 4
    return (current + pd.offsets.BDay(1)).month != current.month
