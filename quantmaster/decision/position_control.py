"""Hybrid position control with matured calibration and risk-aware sizing."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

EPS = 1e-12


@dataclass
class PositionPlan:
    """Complete date-by-date Hybrid allocation plan."""

    weights: pd.DataFrame
    raw_weights: pd.DataFrame
    probability_up: pd.DataFrame
    expected_return: pd.DataFrame
    expected_return_net: pd.DataFrame
    confidence: pd.DataFrame
    agreement: pd.DataFrame
    volatility: pd.DataFrame
    allocation_strength: pd.DataFrame
    eligible: pd.DataFrame
    market_exposure: pd.Series
    opportunity_scale: pd.Series
    target_exposure: pd.Series
    actual_exposure: pd.Series
    position_state: pd.Series
    intentional_flat: pd.Series
    degraded: pd.Series
    calibration_samples: pd.DataFrame
    reasons: dict[pd.Timestamp, tuple[str, ...]]


def _as_frame(values: np.ndarray, like: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(values, index=like.index, columns=like.columns)


def expanding_calibration(
    scores: pd.DataFrame,
    close: pd.DataFrame,
    horizon: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Map scores to probability/return using only outcomes revealed by each date."""
    scores, close = scores.align(close, join="inner", axis=0)
    close = close.reindex(columns=scores.columns)
    forward = close.shift(-horizon) / close - 1.0
    score_values = scores.to_numpy(dtype=float)
    return_values = forward.to_numpy(dtype=float)
    finite = np.isfinite(score_values) & np.isfinite(return_values)
    bins = np.floor(score_values / 10.0)
    bins = np.minimum(9, np.maximum(0, bins))

    daily_count = pd.Series(finite.sum(axis=1), index=scores.index, dtype=float)
    daily_positive = pd.Series(
        (finite & (return_values > 0)).sum(axis=1), index=scores.index, dtype=float,
    )
    daily_return = pd.Series(
        np.where(finite, return_values, 0.0).sum(axis=1), index=scores.index, dtype=float,
    )
    known_count = daily_count.shift(horizon, fill_value=0.0).cumsum()
    known_positive = daily_positive.shift(horizon, fill_value=0.0).cumsum()
    known_return = daily_return.shift(horizon, fill_value=0.0).cumsum()
    global_probability = (known_positive / known_count.replace(0, np.nan)).fillna(0.5)
    global_mean = (known_return / known_count.replace(0, np.nan)).fillna(0.0)

    probability = np.full(scores.shape, np.nan, dtype=float)
    expected = np.full(scores.shape, np.nan, dtype=float)
    samples = np.full(scores.shape, np.nan, dtype=float)
    for number in range(10):
        observations = finite & (bins == number)
        bin_count = pd.Series(observations.sum(axis=1), index=scores.index, dtype=float)
        bin_positive = pd.Series(
            (observations & (return_values > 0)).sum(axis=1),
            index=scores.index,
            dtype=float,
        )
        bin_return = pd.Series(
            np.where(observations, return_values, 0.0).sum(axis=1),
            index=scores.index,
            dtype=float,
        )
        known_bin_count = bin_count.shift(horizon, fill_value=0.0).cumsum()
        known_bin_positive = bin_positive.shift(horizon, fill_value=0.0).cumsum()
        known_bin_return = bin_return.shift(horizon, fill_value=0.0).cumsum()
        bin_probability = (
            known_bin_positive + 4.0 * global_probability
        ) / (known_bin_count + 4.0)
        bin_mean = known_bin_return / known_bin_count.replace(0, np.nan)
        shrink = known_bin_count / (known_bin_count + 50.0)
        bin_expected = (
            shrink * bin_mean.fillna(0.0) + (1.0 - shrink) * global_mean
        )
        current = np.isfinite(score_values) & (bins == number)
        broadcast_probability = np.broadcast_to(
            bin_probability.to_numpy()[:, None], scores.shape,
        )
        broadcast_expected = np.broadcast_to(
            bin_expected.to_numpy()[:, None], scores.shape,
        )
        broadcast_samples = np.broadcast_to(
            known_bin_count.to_numpy()[:, None], scores.shape,
        )
        probability[current] = broadcast_probability[current]
        expected[current] = broadcast_expected[current]
        samples[current] = broadcast_samples[current]
    return (
        _as_frame(probability, scores),
        _as_frame(expected, scores),
        _as_frame(samples, scores),
    )


def _component_agreement(
    components: dict[str, pd.DataFrame], scores: pd.DataFrame,
) -> pd.DataFrame:
    positive = pd.DataFrame(0.0, index=scores.index, columns=scores.columns)
    available = pd.DataFrame(0.0, index=scores.index, columns=scores.columns)
    for values in components.values():
        aligned = values.reindex_like(scores)
        positive = positive.add(aligned.ge(50).astype(float), fill_value=0.0)
        available = available.add(aligned.notna().astype(float), fill_value=0.0)
    return positive.div(available.where(available > 0)).fillna(0.5).clip(0, 1)


def _market_budget(
    close: pd.DataFrame,
    *,
    target_volatility: float,
    max_exposure: float,
) -> tuple[pd.Series, pd.Series]:
    returns = close.pct_change(fill_method=None)
    denominator = returns.notna().sum(axis=1).replace(0, np.nan)
    advance = (returns > 0).sum(axis=1) / denominator
    ma20 = close.rolling(20, min_periods=10).mean()
    above = (close > ma20).sum(axis=1) / ma20.notna().sum(axis=1).replace(0, np.nan)
    state = ((advance - 0.5) + (above - 0.5)).clip(-1, 1)
    regime_multiplier = 0.20 + 0.80 * ((state + 1.0) / 2.0)
    annual_vol = returns.mean(axis=1, skipna=True).rolling(
        20, min_periods=10,
    ).std(ddof=0) * math.sqrt(252)
    safe_vol = annual_vol.where(annual_vol > EPS, EPS)
    volatility_multiplier = (target_volatility / safe_vol).clip(0.30, 1.0)
    exposure = (
        max_exposure * regime_multiplier * volatility_multiplier
    ).clip(0.05, max_exposure)
    valid = state.notna() & annual_vol.notna() & np.isfinite(exposure)
    return exposure.where(valid), valid


def _winsorize(
    values: pd.DataFrame,
    eligible: pd.DataFrame,
    lower: float,
    upper: float,
) -> pd.DataFrame:
    usable = values.where(eligible)
    low = usable.quantile(lower, axis=1)
    high = usable.quantile(upper, axis=1)
    return usable.clip(lower=low, upper=high, axis=0)


def _allocate_capped(strength: pd.Series, target: float, cap: float) -> pd.Series:
    allocation = pd.Series(0.0, index=strength.index, dtype=float)
    active = strength[strength.gt(0) & np.isfinite(strength)].copy()
    remaining = max(0.0, float(target))
    while not active.empty and remaining > EPS:
        total_strength = float(active.sum())
        if total_strength <= 0:
            break
        proposed = remaining * active / total_strength
        capacity = (cap - allocation.loc[active.index]).clip(lower=0.0)
        capped = proposed >= capacity - EPS
        if not capped.any():
            allocation.loc[active.index] += proposed
            remaining = 0.0
            break
        capped_index = active.index[capped]
        assigned = capacity.loc[capped_index]
        allocation.loc[capped_index] += assigned
        remaining -= float(assigned.sum())
        active = active.drop(capped_index)
    return allocation.clip(lower=0.0, upper=cap)


def _reason_codes(
    *, valid: bool, market_exposure: float, market_floor: float,
    qualified: int, top_n: int, actual_exposure: float,
) -> tuple[str, ...]:
    if not valid:
        return ("insufficient_signal_data",)
    if market_exposure < market_floor:
        return ("market_risk_off",)
    if qualified <= 0:
        return ("no_qualified_candidates",)
    values = []
    if qualified < top_n:
        values.append("opportunity_limited")
    if actual_exposure <= EPS:
        values.append("rebalance_buffer")
    return tuple(values or ("allocated",))


def build_position_plan(
    panel: dict[str, pd.DataFrame],
    score_bundle: dict[str, Any],
    *,
    top_n: int,
    horizon: int,
    profile: str,
    cap_weight: float,
    policy_snapshot: dict[str, Any],
    rebalance_mask: pd.Series,
    eligibility_mask: pd.DataFrame | None = None,
) -> PositionPlan:
    """Create non-equal Hybrid weights under market, opportunity and cost gates."""
    scores = score_bundle["score"].astype(float)
    close = panel["close"].reindex_like(scores).astype(float)
    risk = policy_snapshot.get("risk") or {}
    control = policy_snapshot.get("position_control") or {}
    probability, expected, samples = expanding_calibration(scores, close, horizon)
    round_trip_cost = float(control.get("round_trip_cost", 0.0))
    expected_net = expected - round_trip_cost
    agreement = _component_agreement(score_bundle.get("components") or {}, scores)
    sample_confidence = samples.div(250.0).clip(upper=1.0).fillna(0.0)
    confidence = (
        0.35
        + 0.30 * (probability.sub(0.5).abs() * 2.0)
        + 0.15 * agreement
        + 0.10 * sample_confidence
    ).clip(upper=0.90)
    volatility_window = int(control.get("volatility_window", 20))
    volatility = close.pct_change(fill_method=None).rolling(
        volatility_window, min_periods=volatility_window,
    ).std(ddof=0)
    market_exposure, market_valid = _market_budget(
        close,
        target_volatility=float(risk.get("target_volatility", 0.12)),
        max_exposure=float(risk.get("max_exposure", 1.0)),
    )

    eligible = (
        scores.notna()
        & probability.ge(float(risk.get("buy_probability", 0.55)))
        & expected_net.gt(0)
        & volatility.gt(0)
        & _as_frame(np.isfinite(volatility.to_numpy(dtype=float)), volatility)
    )
    if profile == "stable":
        eligible &= agreement.ge(float(control.get("stable_min_agreement", 2 / 3)))
    if eligibility_mask is not None:
        eligible &= eligibility_mask.reindex_like(scores).fillna(False).astype(bool)

    winsor = control.get("winsor_limits") or [0.10, 0.90]
    lower, upper = float(winsor[0]), float(winsor[1])
    clipped_edge = _winsorize(expected_net, eligible, lower, upper).clip(lower=0.0)
    clipped_volatility = _winsorize(volatility, eligible, lower, upper).clip(lower=EPS)
    strength = (
        clipped_edge
        * probability.sub(0.5).clip(lower=0.0)
        * confidence
        / clipped_volatility
    ).where(eligible)

    weights = pd.DataFrame(np.nan, index=scores.index, columns=scores.columns)
    raw_weights = weights.copy()
    opportunity_scale = pd.Series(np.nan, index=scores.index, dtype=float)
    target_exposure = pd.Series(np.nan, index=scores.index, dtype=float)
    actual_exposure = pd.Series(np.nan, index=scores.index, dtype=float)
    position_state = pd.Series("not_due", index=scores.index, dtype=object)
    intentional_flat = pd.Series(False, index=scores.index, dtype=bool)
    degraded = pd.Series(False, index=scores.index, dtype=bool)
    reasons: dict[pd.Timestamp, tuple[str, ...]] = {}
    previous = pd.Series(0.0, index=scores.columns, dtype=float)
    market_floor = float(control.get("market_flat_below", 0.20))
    rebalance_band = float(control.get("rebalance_band", 0.01))
    sizing_inputs = (
        scores.notna()
        & probability.notna()
        & expected_net.notna()
        & volatility.gt(0)
        & _as_frame(np.isfinite(volatility.to_numpy(dtype=float)), volatility)
    )
    row_valid = sizing_inputs.any(axis=1) & market_valid
    due = rebalance_mask.reindex(scores.index).fillna(False).astype(bool)

    for date in scores.index[due]:
        valid = bool(row_valid.loc[date])
        qualified = int(eligible.loc[date].sum()) if valid else 0
        market = float(market_exposure.loc[date]) if valid else float("nan")
        if not valid:
            degraded.loc[date] = True
            position_state.loc[date] = "degraded"
            reasons[pd.Timestamp(date)] = ("insufficient_signal_data",)
            continue
        opportunity = min(1.0, qualified / max(1, top_n))
        opportunity_scale.loc[date] = opportunity
        target = market * opportunity if market >= market_floor and qualified else 0.0
        target_exposure.loc[date] = target
        row = pd.Series(0.0, index=scores.columns, dtype=float)
        raw = row.copy()
        if target > EPS:
            ranked = scores.loc[date].where(eligible.loc[date]).dropna().nlargest(top_n)
            selected_strength = strength.loc[date, ranked.index].dropna()
            raw = _allocate_capped(selected_strength, target, cap_weight).reindex(
                scores.columns, fill_value=0.0,
            )
            row = raw.copy()
            continuing = row.gt(0) & previous.gt(0)
            small_change = row.sub(previous).abs().lt(rebalance_band)
            row.loc[continuing & small_change] = previous.loc[continuing & small_change]
            new_too_small = row.gt(0) & previous.le(0) & row.lt(rebalance_band)
            row.loc[new_too_small] = 0.0
            total = float(row.sum())
            if total > target + EPS:
                adjustable = row.gt(0) & ~(continuing & small_change)
                adjustable_total = float(row.loc[adjustable].sum())
                reduction = min(total - target, adjustable_total)
                if reduction > EPS and adjustable_total > EPS:
                    row.loc[adjustable] *= 1.0 - reduction / adjustable_total
        else:
            intentional_flat.loc[date] = True
        actual = float(row.sum())
        if actual <= EPS:
            intentional_flat.loc[date] = True
            position_state.loc[date] = "flat"
        elif actual < float(risk.get("max_exposure", 1.0)) - EPS:
            position_state.loc[date] = "reduced"
        else:
            position_state.loc[date] = "invested"
        reason = _reason_codes(
            valid=valid,
            market_exposure=market,
            market_floor=market_floor,
            qualified=qualified,
            top_n=top_n,
            actual_exposure=actual,
        )
        reasons[pd.Timestamp(date)] = reason
        weights.loc[date] = row
        raw_weights.loc[date] = raw
        actual_exposure.loc[date] = actual
        previous = row

    return PositionPlan(
        weights=weights,
        raw_weights=raw_weights,
        probability_up=probability,
        expected_return=expected,
        expected_return_net=expected_net,
        confidence=confidence,
        agreement=agreement,
        volatility=volatility,
        allocation_strength=strength,
        eligible=eligible,
        market_exposure=market_exposure,
        opportunity_scale=opportunity_scale,
        target_exposure=target_exposure,
        actual_exposure=actual_exposure,
        position_state=position_state,
        intentional_flat=intentional_flat,
        degraded=degraded,
        calibration_samples=samples,
        reasons=reasons,
    )


__all__ = ["PositionPlan", "build_position_plan", "expanding_calibration"]
