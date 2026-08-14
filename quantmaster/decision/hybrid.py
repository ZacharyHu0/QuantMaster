"""Hybrid v2 decision engine.

The engine keeps the explainable swing rules as a mandatory baseline and can
blend approved Quant Lab expression and learned-model champions.  Every score
used on date T is built from information available no later than T close; the
holding-period outcome used to estimate feature reliability is delayed by the
same horizon before it becomes eligible.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

import numpy as np
import pandas as pd

from quantmaster.config import get_config
from quantmaster.decision.schema import validate_current_policy
from quantmaster.trading_sessions import daily_signal_cutoff

logger = logging.getLogger(__name__)
DecisionProfile = Literal["risk_adjusted", "short_term", "stable"]
PolicyMode = Literal["live", "historical_replay", "retrospective"]
EPS = 1e-12


@dataclass(frozen=True)
class ProfileDefinition:
    key: DecisionProfile
    label: str
    description: str
    rule_weight: float
    factor_weight: float
    ml_weight: float
    rule_minimum: float
    ml_maximum: float
    target_volatility: float
    max_exposure: float
    buy_probability: float


PROFILE_DEFINITIONS: dict[DecisionProfile, ProfileDefinition] = {
    "risk_adjusted": ProfileDefinition(
        "risk_adjusted", "扣费风险收益", "优先平衡净收益、回撤与换手",
        0.45, 0.35, 0.20, 0.35, 0.30, 0.12, 1.00, 0.55,
    ),
    "short_term": ProfileDefinition(
        "short_term", "短期命中收益", "优先提高持有期内 Top-N 命中与净收益",
        0.30, 0.25, 0.45, 0.25, 0.45, 0.16, 1.00, 0.52,
    ),
    "stable": ProfileDefinition(
        "stable", "稳定可解释", "限制模型占比、换手与整体风险暴露",
        0.60, 0.30, 0.10, 0.55, 0.15, 0.08, 0.65, 0.58,
    ),
}

RULE_PRIORS = {
    "momentum_5": 0.18,
    "momentum_20": 0.17,
    "trend": 0.20,
    "macd": 0.18,
    "price_position": 0.12,
    "money": 0.10,
    "low_volatility": 0.05,
}


def _round_trip_cost() -> float:
    costs = get_config().trade
    return float(
        2 * (costs.commission_rate + costs.transfer_fee_rate + costs.slippage)
        + costs.stamp_tax_rate
    )


def _position_control_policy() -> dict[str, Any]:
    round_trip_cost = _round_trip_cost()
    return {
        "version": "hybrid-position-control-v1",
        "round_trip_cost": round(round_trip_cost, 8),
        "market_flat_below": 0.20,
        "volatility_window": 20,
        "winsor_limits": [0.10, 0.90],
        "stable_min_agreement": round(2 / 3, 8),
        "strength_formula": (
            "net_expected_return * (probability_up - 0.50) * confidence / volatility_20d"
        ),
        "opportunity_formula": "min(qualified_count / top_n, 1)",
        "rebalance_band": round(max(0.01, 2 * round_trip_cost), 8),
        "cash_policy": "unallocated_cap_capacity_remains_cash",
        "calibration": "expanding_matured_horizon_outcomes_only",
    }


def profile_definition(profile: str) -> ProfileDefinition:
    try:
        return PROFILE_DEFINITIONS[profile]  # type: ignore[index]
    except KeyError as exc:
        raise ValueError("profile 只支持 risk_adjusted/short_term/stable") from exc


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _content_hash(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _rank(values: pd.DataFrame) -> pd.DataFrame:
    return values.rank(axis=1, pct=True, method="average")


def _rule_features(panel: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    required = {"close", "high", "low"}
    missing = required - panel.keys()
    if missing:
        raise ValueError(f"选股行情缺少字段: {sorted(missing)}")
    close = panel["close"].sort_index().astype(float)
    high = panel["high"].reindex_like(close).astype(float)
    low = panel["low"].reindex_like(close).astype(float)
    returns = close.pct_change(fill_method=None)
    ema10 = close.ewm(span=10, adjust=False).mean()
    ema30 = close.ewm(span=30, adjust=False).mean()
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    low20 = low.rolling(20, min_periods=10).min()
    high20 = high.rolling(20, min_periods=10).max()
    money = panel.get("amount", panel.get("volume"))
    if money is None:
        money_ratio = pd.DataFrame(1.0, index=close.index, columns=close.columns)
    else:
        money = money.reindex_like(close).astype(float)
        money_ratio = (
            money.rolling(5, min_periods=3).mean()
            / (money.rolling(20, min_periods=10).mean() + EPS)
        ).clip(upper=3.0)
    return {
        "momentum_5": close.pct_change(5, fill_method=None),
        "momentum_20": close.pct_change(20, fill_method=None),
        "trend": ema10 / (ema30 + EPS) - 1.0,
        "macd": (macd - macd.ewm(span=9, adjust=False).mean()) / (close + EPS),
        "price_position": (close - low20) / (high20 - low20 + EPS),
        "money": money_ratio,
        "low_volatility": -returns.rolling(20, min_periods=10).std(),
    }


def _capped_weights(values: pd.Series, cap: float = 0.35) -> pd.Series:
    """Normalize non-negative weights while respecting a per-component cap."""
    result = values.clip(lower=0).astype(float)
    if not math.isfinite(float(result.sum())) or result.sum() <= 0:
        result = pd.Series(RULE_PRIORS, dtype=float).reindex(result.index).fillna(0.0)
    result /= result.sum()
    for _ in range(len(result) + 1):
        over = result > cap + 1e-12
        if not over.any():
            break
        overflow = float((result[over] - cap).sum())
        result[over] = cap
        under = ~over
        available = (cap - result[under]).clip(lower=0)
        if overflow <= 0 or available.sum() <= 0:
            break
        result.loc[under] += overflow * available / available.sum()
    return result / result.sum()


def rule_signal_bundle(
    panel: dict[str, pd.DataFrame],
    horizon: int = 3,
    *,
    lookback: int = 120,
    min_periods: int = 60,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, pd.DataFrame]]:
    """Return adaptive rule score, known-at-date weights, and ranked features."""
    if horizon not in {1, 3, 5, 7, 10, 20, 30}:
        raise ValueError("horizon 只支持 1/3/5/7/10/20/30 日")
    close = panel["close"].sort_index().astype(float)
    features = _rule_features(panel)
    ranked = {name: _rank(values.reindex_like(close)) for name, values in features.items()}
    forward = close.shift(-horizon) / close - 1.0
    forward_rank = _rank(forward)
    ic = pd.DataFrame(index=close.index)
    for name, values in ranked.items():
        ic[name] = values.corrwith(forward_rank, axis=1)

    # An IC observed at date s uses the close at s+h.  It therefore only becomes
    # legal input on s+h; shifting by the full horizon enforces that maturity.
    known_ic = ic.shift(horizon)
    mean = known_ic.rolling(lookback, min_periods=min_periods).mean()
    std = known_ic.rolling(lookback, min_periods=min_periods).std(ddof=0)
    reliability = (mean / (std + EPS)).clip(lower=0, upper=2)
    reliability = reliability.div(reliability.sum(axis=1).replace(0, np.nan), axis=0)

    prior = pd.Series(RULE_PRIORS, dtype=float).reindex(list(ranked))
    weight_rows: list[pd.Series] = []
    for date in close.index:
        adaptive = reliability.loc[date]
        raw = prior if adaptive.isna().all() else 0.50 * prior + 0.50 * adaptive.fillna(0.0)
        weight_rows.append(_capped_weights(raw).rename(date))
    weights = pd.DataFrame(weight_rows, index=close.index)

    combined = pd.DataFrame(0.0, index=close.index, columns=close.columns)
    coverage = pd.DataFrame(0.0, index=close.index, columns=close.columns)
    for name, values in ranked.items():
        weight = weights[name]
        combined = combined.add(values.fillna(0.0).mul(weight, axis=0), fill_value=0.0)
        coverage = coverage.add(values.notna().astype(float).mul(weight, axis=0), fill_value=0.0)
    score = 100 * combined.div(coverage.where(coverage > 0))

    tradable = close.gt(1.0) & close.notna()
    if "amount" in panel:
        amount = panel["amount"].reindex_like(close).astype(float)
        tradable &= amount.rolling(20, min_periods=10).mean() >= 1e7
        tradable &= amount.gt(0)
    elif "volume" in panel:
        tradable &= panel["volume"].reindex_like(close).astype(float).gt(0)
    return score.where(tradable).clip(0, 100), weights, ranked


def adaptive_rule_score_panel(
    panel: dict[str, pd.DataFrame], horizon: int = 3,
) -> pd.DataFrame:
    return rule_signal_bundle(panel, horizon)[0]


def _deployment_matches(
    deployment: dict[str, Any], *, universe: str, horizon: int, profile: str,
    a_share_compatible: bool,
) -> tuple[bool, int]:
    if int(deployment.get("horizon", 0)) != horizon:
        return False, -1
    deployed_profile = str(deployment.get("profile") or "all")
    if deployed_profile not in {profile, "all"}:
        return False, -1
    deployed_universe = str(deployment.get("universe") or "")
    scope = str(deployment.get("scope") or "exact")
    if deployed_universe == universe:
        universe_rank = 3
    elif scope == "a_share" and a_share_compatible:
        universe_rank = 1
    else:
        return False, -1
    profile_rank = 2 if deployed_profile == profile else 1
    return True, universe_rank * 10 + profile_rank


def _is_a_share_symbols(symbols: list[str] | None) -> bool:
    if not symbols:
        return False
    try:
        from quantmaster.data.instruments import InstrumentStore

        store = InstrumentStore()
        for symbol in symbols:
            instrument = store.get(symbol)
            if instrument is None:
                return False
            if instrument.market != "CN" or instrument.asset_type != "stock":
                return False
            if instrument.status not in {"listed", "active", "l"}:
                return False
        return True
    except Exception:
        return all(
            symbol.endswith((".SH", ".SZ", ".BJ")) and symbol[:6].isdigit()
            for symbol in symbols
        )


def _component_summary(deployment: dict[str, Any], version: dict[str, Any]) -> dict[str, Any]:
    spec = version.get("spec") or {}
    validation = version.get("validation") or {}
    return {
        "role": str(deployment.get("role") or ("ml" if spec.get("kind") == "learned" else "factor")),
        "deployment_id": deployment.get("id", ""),
        "version_id": version.get("id", ""),
        "content_hash": version.get("content_hash", ""),
        "name": version.get("name") or spec.get("name") or version.get("slug") or "Quant Lab Champion",
        "kind": spec.get("kind", version.get("kind", "expression")),
        "status": version.get("status", "unknown"),
        "profile": deployment.get("profile", "all"),
        "horizon": deployment.get("horizon"),
        "scope": deployment.get("scope", "exact"),
        "universe": deployment.get("universe", ""),
        "deployed_at": deployment.get("created_at", ""),
        "validation": {
            "candidate_score": validation.get("candidate_score"),
            "best_horizon": validation.get("best_horizon"),
            "coverage": validation.get("coverage"),
            "gates": validation.get("gates", {}),
        },
        "spec": spec,
    }


def resolve_policy(
    universe: str,
    horizon: int,
    profile: DecisionProfile = "risk_adjusted",
    *,
    symbols: list[str] | None = None,
    store=None,
    as_of: str = "",
    mode: PolicyMode = "live",
) -> dict[str, Any]:
    """Resolve active Lab champions into an immutable runtime snapshot."""
    if mode not in {"live", "historical_replay", "retrospective"}:
        raise ValueError("模型模式只支持 live/historical_replay/retrospective")
    if mode == "historical_replay" and not as_of:
        raise ValueError("历史重放必须提供 as_of")
    if as_of:
        try:
            pd.Timestamp(as_of)
        except (TypeError, ValueError) as exc:
            raise ValueError("模型查看日期需要使用 YYYY-MM-DD 格式") from exc
    definition = profile_definition(profile)
    components: list[dict[str, Any]] = [{
        "role": "rule", "name": "自适应规则基线", "kind": "rule",
        "status": "active", "version_id": "swing-adaptive-v2",
        "content_hash": _content_hash({"priors": RULE_PRIORS, "horizon": horizon}),
        "scope": "builtin", "universe": universe, "validation": {},
    }]
    warnings: list[str] = []
    a_share_compatible = _is_a_share_symbols(symbols)
    try:
        if store is None:
            from quantmaster.lab.store import LabStore

            store = LabStore()
        if mode == "historical_replay":
            if not hasattr(store, "deployments_as_of") or not hasattr(
                store, "champion_strategies_as_of"
            ):
                raise RuntimeError("Lab 账本不支持冻结部署/Champion 历史证据")
            deployments = store.deployments_as_of(as_of)
        else:
            deployments = store.active_deployments()
        selected: dict[str, tuple[int, dict, dict]] = {}
        for deployment in deployments:
            if mode == "historical_replay":
                deployed_at = str(deployment.get("created_at") or "")
                cutoff = pd.Timestamp(daily_signal_cutoff(as_of))
                deployed = pd.to_datetime(deployed_at, errors="coerce", utc=True)
                if pd.isna(deployed) or pd.Timestamp(deployed) > cutoff:
                    continue
            matches, rank = _deployment_matches(
                deployment, universe=universe, horizon=horizon, profile=profile,
                a_share_compatible=a_share_compatible,
            )
            if not matches:
                continue
            if mode == "historical_replay":
                version = deployment.get("version_snapshot")
                if deployment.get("evidence_status") != "verified" or not isinstance(
                    version, dict
                ):
                    raise RuntimeError(
                        f"部署 {deployment.get('id') or ''} 缺少可验证的冻结版本/验证快照"
                    )
            else:
                version = store.version(deployment["version_id"])
            if not version or version.get("status") not in {"production", "approved"}:
                continue
            component = _component_summary(deployment, version)
            role = component["role"]
            if role not in {"factor", "ml"}:
                continue
            if role not in selected or rank > selected[role][0]:
                selected[role] = (rank, deployment, component)
        champions = (
            store.champion_strategies_as_of(as_of, horizon=horizon)
            if mode == "historical_replay"
            else store.strategies(horizon=horizon, status="champion", limit=1)
            if hasattr(store, "strategies")
            else []
        )
        if champions:
            champion = champions[0]
            nested = []
            frozen_versions = champion.get("component_versions") or {}
            for item in champion.get("components") or []:
                version_id = str(item.get("version_id") or "")
                version = (
                    frozen_versions.get(version_id)
                    if mode == "historical_replay"
                    else store.version(version_id)
                )
                if version is None:
                    if mode == "historical_replay":
                        raise RuntimeError(
                            f"Champion {champion.get('id') or ''} 缺少成分 {version_id} 的冻结证据"
                        )
                    continue
                nested.append({
                    "version_id": version["id"], "weight": float(item.get("weight") or 0),
                    "spec": version.get("spec") or {}, "name": version.get("name") or "因子",
                })
            if nested:
                champion_events = champion.get("promotion_events") or []
                champion_event = next(
                    (
                        event for event in reversed(champion_events)
                        if str(event.get("to_status") or "") == "champion"
                    ),
                    {},
                )
                component = {
                    "role": "factor", "name": champion["name"], "kind": "composite",
                    "status": "active", "strategy_id": champion["id"],
                    "version_id": champion["id"], "content_hash": _content_hash(nested),
                    "scope": "a_share", "universe": "csi800", "horizon": horizon,
                    "spec": {"kind": "composite", "components": nested},
                    "validation": champion.get("sealed_evidence") or {},
                    "deployed_at": str(champion_event.get("created_at") or ""),
                }
                selected["factor"] = (99, {}, component)
        if mode == "historical_replay" and not selected:
            raise RuntimeError(
                f"{as_of} 的模型部署/Champion 事件证据不可用；"
                "拒绝把证据缺失静默解释为当时仅使用规则基线"
            )
        components.extend(value[2] for value in selected.values())
    except Exception as exc:
        if mode == "historical_replay":
            raise RuntimeError(f"历史模型策略无法重建：{exc}") from exc
        logger.warning("Quant Lab Champion 解析失败，已使用规则基线", exc_info=True)
        warnings.append("Quant Lab Champion 暂不可用，已使用规则基线")

    available = {item["role"] for item in components}
    requested = {
        "rule": definition.rule_weight,
        "factor": definition.factor_weight,
        "ml": min(definition.ml_weight, definition.ml_maximum),
    }
    total = sum(weight for role, weight in requested.items() if role in available)
    weights = {
        role: (weight / total if total else 1.0)
        for role, weight in requested.items() if role in available
    }
    if weights.get("ml", 0.0) > definition.ml_maximum:
        overflow = weights["ml"] - definition.ml_maximum
        weights["ml"] = definition.ml_maximum
        recipients = {key: value for key, value in weights.items() if key != "ml"}
        recipient_total = sum(recipients.values())
        if recipient_total > 0:
            for key, value in recipients.items():
                weights[key] += overflow * value / recipient_total
    if "rule" in weights and len(weights) > 1 and weights["rule"] < definition.rule_minimum:
        remainder = 1.0 - definition.rule_minimum
        other_total = sum(value for key, value in weights.items() if key != "rule")
        weights = {
            key: (definition.rule_minimum if key == "rule" else value / other_total * remainder)
            for key, value in weights.items()
        }
    weights = {key: round(value, 6) for key, value in weights.items()}
    for component in components:
        component["weight"] = round(weights.get(component["role"], 0.0), 6)
        if component.get("scope") == "a_share" and component.get("universe") != universe:
            warnings.append(
                f"{component['name']} 在 {component['universe']} 验证，本次跨候选应用。"
            )
    payload = {
        "schema_version": 3,
        "engine_version": "hybrid-v3-position-control",
        "profile": profile,
        "profile_label": definition.label,
        "universe": universe,
        "horizon": horizon,
        "policy_mode": mode,
        "policy_effective_as_of": as_of if mode == "historical_replay" else "",
        "components": components,
        "warnings": warnings,
        "risk": {
            "target_volatility": definition.target_volatility,
            "max_exposure": definition.max_exposure,
            "buy_probability": definition.buy_probability,
        },
        "position_control": _position_control_policy(),
    }
    payload["policy_hash"] = _content_hash(payload)
    payload["model_version"] = f"hybrid-v3:{profile}:{payload['policy_hash'][:12]}"
    return payload


def _expression_component(
    panel: dict[str, pd.DataFrame], component: dict[str, Any],
) -> pd.DataFrame:
    from quantmaster.factors import compute_factor
    from quantmaster.factors.fundamental import resolve_factor

    spec = component.get("spec") or {}
    expression = spec.get("expression") or spec.get("slug")
    if not expression:
        raise ValueError("生产因子缺少表达式")
    close = panel["close"]
    factor = resolve_factor(
        expression, list(close.columns), str(close.index.min().date()),
        str(close.index.max().date()),
    )
    values = compute_factor(factor, panel)
    direction = int(spec.get("direction", 1) or 1)
    return 100 * _rank(values * direction)


def _learned_component(
    panel: dict[str, pd.DataFrame], component: dict[str, Any],
) -> pd.DataFrame:
    from quantmaster.lab.ml import predict_panel

    spec = component.get("spec") or {}
    values = predict_panel(
        panel, spec.get("model") or {}, horizon=int(component.get("horizon") or 3),
    )
    return 100 * _rank(values * int(spec.get("direction", 1) or 1))


def _python_component(
    panel: dict[str, pd.DataFrame],
    component: dict[str, Any],
    evidence_sink: dict[str, pd.DataFrame] | None = None,
) -> pd.DataFrame:
    from quantmaster.config import get_config
    from quantmaster.data.research import ResearchDataBundle
    from quantmaster.data.research_features import registered_features
    from quantmaster.factors.python_artifact import execute_python_factor_artifact

    spec = component.get("spec") or {}
    bundle = ResearchDataBundle.from_legacy_panel(panel)
    bundle.signal.setdefault("returns", panel["close"].pct_change(fill_method=None))
    required = set(spec.get("required_features") or [])
    fundamental_fields = {"pe_ttm", "pb", "dv_ratio", "total_mv", "roe"}
    if required & fundamental_fields:
        from quantmaster.data.fundamentals import fundamental_panel

        close = panel["close"]
        bundle.fundamentals = fundamental_panel(
            list(close.columns), str(close.index.min().date()), str(close.index.max().date()),
            fields=sorted(required & fundamental_fields),
        )
    if "news_sentiment" in required:
        from quantmaster.ai.sentiment import quality_sentiment_panel

        close = panel["close"]
        bundle.signal["news_sentiment"] = quality_sentiment_panel(
            close.index, list(close.columns),
        ).reindex_like(close)
    if "membership" in required:
        close = panel["close"]
        membership = panel.get("membership")
        if not isinstance(membership, pd.DataFrame) or membership.empty:
            raise ValueError("正式决策缺少 PIT membership，拒绝用全量成分替代")
        membership = membership.reindex(index=close.index, columns=close.columns)
        if membership.isna().any().any():
            raise ValueError("正式决策 membership 覆盖不足，拒绝静默补全")
        bundle.membership = membership.astype(bool)
    features, _catalog = registered_features(bundle)
    if evidence_sink is not None:
        for name in sorted(required):
            value = features.get(name)
            if value is None:
                continue
            key = f"feature::{name}"
            existing = evidence_sink.get(key)
            if existing is not None and not existing.equals(value):
                raise RuntimeError(f"同一决策内特征 {name} 出现不一致的输入版本")
            evidence_sink[key] = value.copy()
    values = execute_python_factor_artifact(
        get_config().data_root, spec.get("artifact") or {}, features,
    )
    return 100 * _rank(values * int(spec.get("direction", 1) or 1))


def _composite_component(
    panel: dict[str, pd.DataFrame],
    component: dict[str, Any],
    evidence_sink: dict[str, pd.DataFrame] | None = None,
) -> pd.DataFrame:
    spec = component.get("spec") or {}
    combined: pd.DataFrame | None = None
    total = 0.0
    seen: set[str] = set()
    for nested in spec.get("components") or []:
        version_id = str(nested.get("version_id") or "")
        if not version_id or version_id in seen:
            continue
        seen.add(version_id)
        nested_component = {"spec": nested.get("spec") or {}, "horizon": component.get("horizon")}
        kind = str(nested_component["spec"].get("kind") or "expression")
        values = (
            _learned_component(panel, nested_component) if kind == "learned"
            else _python_component(
                panel, nested_component, evidence_sink=evidence_sink,
            ) if kind == "python"
            else _expression_component(panel, nested_component)
        )
        weight = float(nested.get("weight") or 0)
        combined = values * weight if combined is None else combined.add(values * weight, fill_value=0)
        total += weight
    if combined is None or total <= 0:
        raise ValueError("Champion 组合缺少可执行成分")
    return 100 * _rank(combined / total)


def hybrid_score_bundle(
    panel: dict[str, pd.DataFrame],
    *,
    horizon: int = 3,
    profile: DecisionProfile = "risk_adjusted",
    universe: str = "demo",
    policy_snapshot: dict[str, Any] | None = None,
    evidence_sink: dict[str, pd.DataFrame] | None = None,
) -> dict[str, Any]:
    """Compute official component scores and deterministic fallback metadata."""
    close = panel["close"].sort_index()
    snapshot = json.loads(json.dumps(policy_snapshot or resolve_policy(
        universe, horizon, profile, symbols=list(close.columns)
    ), ensure_ascii=False))
    validate_current_policy(snapshot)
    rule_score, rule_weights, _ranked = rule_signal_bundle(panel, horizon)
    scores: dict[str, pd.DataFrame] = {"rule": rule_score}
    warnings = list(snapshot.get("warnings") or [])
    active_components: list[dict[str, Any]] = []
    shadow: dict[str, Any] | None = None
    for component in snapshot.get("components", []):
        role = component.get("role")
        if role == "rule":
            active_components.append(component)
            continue
        try:
            spec_kind = str((component.get("spec") or {}).get("kind") or "expression")
            values = (
                _learned_component(panel, component) if role == "ml"
                else _python_component(
                    panel, component, evidence_sink=evidence_sink,
                ) if spec_kind == "python"
                else _composite_component(
                    panel, component, evidence_sink=evidence_sink,
                ) if spec_kind == "composite"
                else _expression_component(panel, component)
            ).reindex_like(close)
            if values.dropna(how="all").empty:
                raise ValueError("没有满足覆盖率的有效预测")
            if evidence_sink is not None:
                component_id = str(
                    component.get("version_id")
                    or component.get("content_hash")
                    or component.get("name")
                    or role
                )
                evidence_sink[f"component_score::{role}::{component_id}"] = values.copy()
            scores[role] = values
            active_components.append(component)
        except Exception:
            logger.warning(
                "决策组件执行失败 role=%s name=%s",
                role, component.get("name", ""), exc_info=True,
            )
            component["status"] = "fallback"
            component["fallback_reason"] = "组件执行失败，请查看本机日志"
            warnings.append(f"{component.get('name', role)} 未参与正式评分")
            if role == "ml":
                shadow = {
                    "name": component.get("name", "学习模型"),
                    "status": "failed", "reason": "组件执行失败，请查看本机日志",
                }

    total_weight = sum(float(item.get("weight", 0)) for item in active_components)
    if total_weight <= 0:
        total_weight = 1.0
    combined = pd.DataFrame(0.0, index=close.index, columns=close.columns)
    coverage = pd.DataFrame(0.0, index=close.index, columns=close.columns)
    normalized_weights: dict[str, float] = {}
    for component in active_components:
        role = str(component["role"])
        weight = float(component.get("weight", 0)) / total_weight
        component["effective_weight"] = round(weight, 6)
        normalized_weights[role] = weight
        values = scores[role]
        combined = combined.add(values.fillna(0.0) * weight, fill_value=0.0)
        coverage = coverage.add(values.notna().astype(float) * abs(weight), fill_value=0.0)
    combined = combined.div(coverage.where(coverage > 0)).clip(0, 100)
    snapshot["components"] = snapshot.get("components", [])
    snapshot["warnings"] = warnings
    snapshot["fallback_active"] = any(item.get("status") == "fallback" for item in snapshot["components"])
    snapshot["effective_weights"] = {key: round(value, 6) for key, value in normalized_weights.items()}
    return {
        "score": combined,
        "components": scores,
        "rule_weights": rule_weights,
        "model_snapshot": snapshot,
        "warnings": warnings,
        "shadow_model": shadow,
    }


def _calibration_bins(
    scores: pd.DataFrame, close: pd.DataFrame, horizon: int,
) -> tuple[list[dict[str, float]], float | None]:
    forward = close.shift(-horizon) / close - 1.0
    left, right = scores.align(forward, join="inner")
    values = pd.DataFrame({
        "score": left.stack(future_stack=True),
        "return": right.stack(future_stack=True),
    }).dropna()
    if values.empty:
        return [], None
    values["bin"] = np.minimum(9, np.maximum(0, np.floor(values["score"] / 10))).astype(int)
    global_probability = float((values["return"] > 0).mean())
    global_mean = float(values["return"].mean())
    bins: list[dict[str, float]] = []
    predicted = pd.Series(index=values.index, dtype=float)
    for number in range(10):
        group = values[values["bin"] == number]
        count = len(group)
        probability = (
            (float((group["return"] > 0).sum()) + 4 * global_probability) / (count + 4)
            if count else global_probability
        )
        shrink = count / (count + 50)
        expected = shrink * (float(group["return"].mean()) if count else 0.0) + (1 - shrink) * global_mean
        downside = float(group["return"].quantile(0.10)) if count >= 10 else float("nan")
        upside = float(group["return"].quantile(0.75)) if count >= 10 else float("nan")
        bins.append({
            "bin": float(number), "samples": float(count), "probability_up": probability,
            "expected_return": expected, "downside_q10": downside, "upside_q75": upside,
        })
        predicted.loc[group.index] = probability
    actual = (values["return"] > 0).astype(float)
    brier = float(((predicted - actual) ** 2).mean()) if predicted.notna().any() else None
    return bins, brier


def calibrate_latest(
    scores: pd.DataFrame, close: pd.DataFrame, horizon: int,
) -> tuple[dict[str, dict[str, float]], dict[str, Any]]:
    bins, brier = _calibration_bins(scores, close, horizon)
    latest = scores.dropna(how="all")
    if latest.empty:
        return {}, {"samples": 0, "brier_score": None, "method": "empirical-decile"}
    rows: dict[str, dict[str, float]] = {}
    total_samples = int(sum(item["samples"] for item in bins))
    for symbol, raw_score in latest.iloc[-1].dropna().items():
        number = int(min(9, max(0, math.floor(float(raw_score) / 10))))
        item = bins[number] if bins else {
            "samples": 0.0, "probability_up": 0.5, "expected_return": 0.0,
            "downside_q10": float("nan"), "upside_q75": float("nan"),
        }
        rows[str(symbol)] = dict(item)
    return rows, {
        "method": "empirical-decile",
        "samples": total_samples,
        "brier_score": round(brier, 6) if brier is not None and math.isfinite(brier) else None,
    }


def continuous_market_exposure(
    panel: dict[str, pd.DataFrame], profile: DecisionProfile = "risk_adjusted",
) -> pd.Series:
    definition = profile_definition(profile)
    close = panel["close"].sort_index().astype(float)
    returns = close.pct_change(fill_method=None)
    advance = (returns > 0).sum(axis=1) / returns.notna().sum(axis=1).replace(0, np.nan)
    ma20 = close.rolling(20, min_periods=10).mean()
    above = (close > ma20).sum(axis=1) / ma20.notna().sum(axis=1).replace(0, np.nan)
    state = ((advance - 0.5) + (above - 0.5)).clip(-1, 1)
    regime_multiplier = 0.20 + 0.80 * ((state + 1.0) / 2.0)
    market_return = returns.mean(axis=1, skipna=True)
    annual_vol = market_return.rolling(20, min_periods=10).std(ddof=0) * math.sqrt(252)
    volatility_multiplier = (definition.target_volatility / annual_vol.replace(0, np.nan)).clip(0.30, 1.0)
    exposure = definition.max_exposure * regime_multiplier * volatility_multiplier
    return exposure.clip(0.05, definition.max_exposure).where(state.notna(), 0.05)


def _select_diversified(
    ranked: pd.Series, top_n: int, industry_map: dict[str, str],
) -> tuple[pd.Series, bool]:
    if not industry_map or top_n <= 2:
        return ranked.head(top_n), False
    cap = max(1, math.ceil(top_n * 0.30))
    selected: list[str] = []
    counts: dict[str, int] = {}
    for symbol in ranked.index:
        industry = industry_map.get(str(symbol), "未知")
        if industry != "未知" and counts.get(industry, 0) >= cap:
            continue
        selected.append(str(symbol))
        counts[industry] = counts.get(industry, 0) + 1
        if len(selected) == top_n:
            break
    relaxed = False
    if len(selected) < top_n:
        relaxed = True
        for symbol in ranked.index:
            value = str(symbol)
            if value not in selected:
                selected.append(value)
            if len(selected) == top_n:
                break
    return ranked.loc[selected], relaxed


def _safe_float(value: Any, digits: int = 4) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return round(number, digits) if math.isfinite(number) else None


def hybrid_daily_selection(
    panel: dict[str, pd.DataFrame],
    *,
    top_n: int = 10,
    horizon: int = 3,
    profile: DecisionProfile = "risk_adjusted",
    universe: str = "demo",
    industry_map: dict[str, str] | None = None,
    name_map: dict[str, str] | None = None,
    policy_snapshot: dict[str, Any] | None = None,
    cap_weight: float = 0.25,
    policy_mode: PolicyMode = "live",
    evidence_sink: dict[str, pd.DataFrame] | None = None,
) -> dict[str, Any]:
    """Generate a calibrated Hybrid decision with an executable position plan."""
    if top_n < 1:
        raise ValueError("top_n 必须为正整数")
    if not 0 < cap_weight <= 1:
        raise ValueError("cap_weight 必须在 (0, 1] 范围内")
    definition = profile_definition(profile)
    industries = industry_map or {}
    names = name_map or {}
    close_input = panel.get("close")
    if close_input is None or close_input.empty:
        raise ValueError("行情面板缺少 close，无法生成 Hybrid 决策")
    signal_dates = close_input.dropna(how="all").index
    if signal_dates.empty:
        raise ValueError("有效历史不足，无法生成 Hybrid 决策")
    signal_as_of = pd.Timestamp(signal_dates[-1]).strftime("%Y-%m-%d")
    if policy_snapshot is None:
        policy_snapshot = resolve_policy(
            universe,
            horizon,
            profile,
            symbols=list(close_input.columns),
            as_of=signal_as_of if policy_mode == "historical_replay" else "",
            mode=policy_mode,
        )
    elif str(policy_snapshot.get("policy_mode") or "live") != policy_mode:
        raise ValueError("决策模式与模型快照模式不一致")
    bundle = hybrid_score_bundle(
        panel, horizon=horizon, profile=profile, universe=universe,
        policy_snapshot=policy_snapshot,
        evidence_sink=evidence_sink,
    )
    scores = bundle["score"]
    valid = scores.dropna(how="all")
    if valid.empty:
        raise ValueError("有效历史不足，无法生成 Hybrid 决策")
    date = valid.index[-1]
    ranked = valid.loc[date].dropna().sort_values(ascending=False)
    close = panel["close"].reindex(scores.index).astype(float)
    calibration, calibration_summary = calibrate_latest(scores, close, horizon)
    snapshot = bundle["model_snapshot"]
    from quantmaster.decision.position_control import build_position_plan

    # Horizon is the forecast label; target weights still roll every session.
    rebalance = pd.Series(True, index=scores.index)
    plan = build_position_plan(
        panel,
        bundle,
        top_n=top_n,
        horizon=horizon,
        profile=profile,
        cap_weight=cap_weight,
        policy_snapshot=snapshot,
        rebalance_mask=rebalance,
    )
    target_row = plan.weights.loc[date]
    positive = target_row[target_row.gt(0)].sort_values(ascending=False)
    display_symbols = list(positive.index)
    display_symbols.extend(
        str(symbol) for symbol in ranked.index
        if str(symbol) not in display_symbols
    )
    display_symbols = display_symbols[:top_n]
    round_trip_cost = float(
        (snapshot.get("position_control") or {}).get("round_trip_cost", _round_trip_cost())
    )
    component_latest = {
        role: values.reindex(scores.index).loc[date]
        for role, values in bundle["components"].items()
        if date in values.index
    }
    picks: list[dict[str, Any]] = []
    for symbol in display_symbols:
        score = float(ranked.loc[symbol])
        calibrated = calibration.get(str(symbol), {})
        probability = float(plan.probability_up.at[date, symbol])
        expected = float(plan.expected_return.at[date, symbol])
        expected_net = float(plan.expected_return_net.at[date, symbol])
        daily_vol = float(plan.volatility.at[date, symbol])
        if not math.isfinite(daily_vol):
            daily_vol = 0.025
        fallback_stop = daily_vol * math.sqrt(horizon) * 1.5
        downside = float(calibrated.get("downside_q10", np.nan))
        upside = float(calibrated.get("upside_q75", np.nan))
        stop_reference = (
            abs(downside)
            if math.isfinite(downside) and downside < 0
            else fallback_stop
        )
        profit_reference = (
            upside
            if math.isfinite(upside) and upside > 0
            else expected * 1.8
        )
        stop_loss = min(0.10, max(0.03, stop_reference))
        take_profit = min(0.20, max(stop_loss * 1.6, profit_reference))
        component_scores = {
            role: _safe_float(values.get(symbol), 2)
            for role, values in component_latest.items()
        }
        agreement = float(plan.agreement.at[date, symbol])
        confidence = float(plan.confidence.at[date, symbol])
        target_weight = float(target_row.get(symbol, 0.0))
        strength = float(plan.allocation_strength.at[date, symbol])
        action = "buy" if target_weight > EPS else "watch"
        strongest = sorted(
            ((role, value) for role, value in component_scores.items() if value is not None),
            key=lambda item: item[1], reverse=True,
        )
        reasons = [f"{role.upper()} 贡献 {value:.1f}" for role, value in strongest[:2]]
        reasons.append(f"历史校准上涨概率 {probability:.1%}")
        if bundle["model_snapshot"].get("fallback_active"):
            reasons.append("部分模型不可用，已按可用组件重算")
        picks.append({
            "rank": len(picks) + 1,
            "symbol": str(symbol),
            "name": names.get(str(symbol), "名称待同步"),
            "industry": industries.get(str(symbol), "未知"),
            "score": round(score, 2),
            "action": action,
            "position_role": "holding" if target_weight > EPS else "watchlist",
            "target_weight": round(target_weight, 6),
            "allocation_strength": round(strength, 6) if math.isfinite(strength) else None,
            "allocation_components": {
                "expected_return_gross": _safe_float(expected, 6),
                "expected_return_net": _safe_float(expected_net, 6),
                "probability_up": _safe_float(probability, 6),
                "probability_edge": _safe_float(probability - 0.5, 6),
                "confidence": _safe_float(confidence, 6),
                "agreement": _safe_float(agreement, 6),
                "volatility_20d": _safe_float(daily_vol, 6),
                "round_trip_cost": round(round_trip_cost, 8),
            },
            "holding_days": horizon,
            "confidence": round(confidence, 4),
            "probability_up": round(probability, 4),
            "expected_return": round(expected_net, 4),
            "expected_return_gross": round(expected, 4),
            "expected_return_net": round(expected_net, 4),
            "last_close": _safe_float(close.at[date, symbol]),
            "stop_loss": round(stop_loss, 4),
            "take_profit": round(take_profit, 4),
            "component_scores": component_scores,
            "model_agreement": round(agreement, 4),
            "reasons": reasons,
        })
    warnings = list(bundle["warnings"])
    state = str(plan.position_state.loc[date])
    state_reasons = list(plan.reasons.get(pd.Timestamp(date), ()))
    if state == "degraded":
        warnings.append("评分或市场输入不足，本期不发新仓位信号，既有持仓保持不变。")
    elif state == "flat":
        warnings.append("仓位计划主动空仓；本期没有需要持有的标的。")
    market_exposure = _safe_float(plan.market_exposure.loc[date], 6)
    actual_exposure = _safe_float(plan.actual_exposure.loc[date], 6)
    opportunity_scale = _safe_float(plan.opportunity_scale.loc[date], 6)
    regime = (
        "unknown" if market_exposure is None else
        "bull" if market_exposure >= 0.75 else
        "range" if market_exposure >= 0.30 else "bear"
    )
    rule_weights = bundle["rule_weights"].loc[date].to_dict()
    return {
        "model_version": snapshot["model_version"],
        "profile": profile,
        "profile_label": definition.label,
        "policy_hash": snapshot["policy_hash"],
        "policy_mode": policy_mode,
        "policy_effective_at": str(snapshot.get("policy_effective_as_of") or ""),
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "signal_date": str(date.date()) if hasattr(date, "date") else str(date),
        "holding_horizon_days": horizon,
        "market_regime": regime,
        "market_base_exposure": market_exposure,
        "opportunity_scale": opportunity_scale,
        "recommended_exposure": actual_exposure,
        "cash_weight": (
            round(max(0.0, 1.0 - actual_exposure), 6)
            if actual_exposure is not None else None
        ),
        "qualified_count": int(plan.eligible.loc[date].sum()),
        "position_state": state,
        "position_reasons": state_reasons,
        "model_snapshot": snapshot,
        "validation_summary": calibration_summary,
        "shadow_model": bundle["shadow_model"],
        "data_quality": {
            "requested_symbols": int(close.shape[1]),
            "scored_symbols": int(valid.loc[date].notna().sum()),
            "status": "complete" if valid.loc[date].notna().all() else "partial",
            "position_signal": "withheld" if state == "degraded" else "ready",
        },
        "rule_weights": {key: round(float(value), 6) for key, value in rule_weights.items()},
        "warnings": warnings,
        "picks": picks,
        "risk_note": (
            "信号于收盘后生成，按 T+1 开盘执行；概率与收益来自已揭晓样本的历史校准，"
            "模型异常时自动回退，不构成投资建议。"
        ),
    }


class HybridDecisionStrategy:
    """Backtest/paper adapter sharing the exact Hybrid position-control path."""

    def __init__(
        self,
        *,
        top_n: int = 5,
        holding_days: int = 3,
        profile: DecisionProfile = "risk_adjusted",
        universe: str = "demo",
        policy_snapshot: dict[str, Any] | None = None,
        cap_weight: float = 0.25,
    ):
        if top_n < 1 or holding_days not in {1, 3, 5, 7, 10, 20, 30}:
            raise ValueError("top_n 必须为正数，holding_days 只支持 1/3/5/7/10/20/30")
        profile_definition(profile)
        self.top_n = top_n
        self.holding_days = holding_days
        self.profile = profile
        self.universe = universe
        self.policy_snapshot = policy_snapshot
        self.cap_weight = cap_weight
        self.name = f"decision_{profile}_top{top_n}_hold{holding_days}d"

    def signal_bundle(
        self,
        panel: dict[str, pd.DataFrame],
        *,
        eligibility_mask: pd.DataFrame | None = None,
        force_latest: bool = False,
    ):
        from quantmaster.backtest.strategy import SignalBundle

        bundle = hybrid_score_bundle(
            panel, horizon=self.holding_days, profile=self.profile,
            universe=self.universe, policy_snapshot=self.policy_snapshot,
        )
        snapshot = bundle["model_snapshot"]
        from quantmaster.decision.position_control import build_position_plan

        rebalance = pd.Series(True, index=bundle["score"].index)
        plan = build_position_plan(
            panel,
            bundle,
            top_n=self.top_n,
            horizon=self.holding_days,
            profile=self.profile,
            cap_weight=self.cap_weight,
            policy_snapshot=snapshot,
            rebalance_mask=rebalance,
            eligibility_mask=eligibility_mask,
        )
        return SignalBundle(
            weights=plan.weights,
            scores=bundle["score"],
            confidence=plan.confidence,
            degraded=plan.degraded.to_frame("position_control"),
            contributions={
                **bundle["components"],
                "probability_up": plan.probability_up,
                "expected_return_net": plan.expected_return_net,
                "volatility_20d": plan.volatility,
                "allocation_strength": plan.allocation_strength,
            },
            intentional_flat=plan.intentional_flat,
            target_exposure=plan.actual_exposure,
            metadata={
                "policy_snapshot": snapshot,
                "position_control": "hybrid-position-control-v1",
                "position_state": {
                    str(date): str(value)
                    for date, value in plan.position_state.items()
                    if value != "not_due"
                },
                "position_reasons": {
                    str(date): list(values) for date, values in plan.reasons.items()
                },
            },
        )

    def target_weights(self, panel: dict[str, pd.DataFrame]) -> pd.DataFrame:
        return self.signal_bundle(panel).weights


def policy_public_summary(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Return the stable subset safe to expose through APIs and stored specs."""
    keys = {
        "schema_version", "engine_version", "profile", "profile_label", "universe",
        "horizon", "components", "warnings", "risk", "policy_hash", "model_version",
        "fallback_active", "effective_weights", "position_control",
    }
    return {key: value for key, value in snapshot.items() if key in keys}


__all__ = [
    "PROFILE_DEFINITIONS",
    "DecisionProfile",
    "HybridDecisionStrategy",
    "adaptive_rule_score_panel",
    "calibrate_latest",
    "continuous_market_exposure",
    "hybrid_daily_selection",
    "hybrid_score_bundle",
    "policy_public_summary",
    "profile_definition",
    "resolve_policy",
    "rule_signal_bundle",
]
