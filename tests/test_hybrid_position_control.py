"""Hybrid 自动仓位控制的确定性单元测试。"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from quantmaster.decision.position_control import (
    _allocate_capped,
    build_position_plan,
    expanding_calibration,
)


def _panel(symbols: tuple[str, ...] = ("A", "B", "C", "D")) -> dict[str, pd.DataFrame]:
    dates = pd.bdate_range("2024-01-02", periods=90)
    returns = 0.001 + 0.004 * np.sin(np.arange(len(dates)) / 3)
    base = 100 * np.cumprod(1 + returns)
    close = pd.DataFrame(
        {symbol: base * (1 + index / 10) for index, symbol in enumerate(symbols)},
        index=dates,
    )
    return {
        "open": close.copy(),
        "high": close * 1.01,
        "low": close * 0.99,
        "close": close,
        "volume": close * 10_000,
    }


def _policy(*, band: float = 0.01) -> dict:
    return {
        "schema_version": 3,
        "risk": {
            "target_volatility": 0.12,
            "max_exposure": 1.0,
            "buy_probability": 0.55,
        },
        "position_control": {
            "round_trip_cost": 0.002,
            "market_flat_below": 0.20,
            "volatility_window": 20,
            "winsor_limits": [0.10, 0.90],
            "stable_min_agreement": 2 / 3,
            "rebalance_band": band,
        },
    }


def _bundle(panel: dict[str, pd.DataFrame]) -> dict:
    close = panel["close"]
    scores = pd.DataFrame(
        np.tile([90.0, 80.0, 70.0, 60.0], (len(close), 1)),
        index=close.index,
        columns=close.columns,
    )
    return {
        "score": scores,
        "components": {"rule": scores},
        "model_snapshot": _policy(),
    }


def _stub_inputs(monkeypatch, bundle, *, market: float = 0.80, valid: bool = True):
    import quantmaster.decision.position_control as control

    scores = bundle["score"]
    probability = pd.DataFrame(
        np.tile([0.72, 0.66, 0.62, 0.58], (len(scores), 1)),
        index=scores.index,
        columns=scores.columns,
    )
    expected = pd.DataFrame(
        np.tile([0.080, 0.045, 0.025, 0.014], (len(scores), 1)),
        index=scores.index,
        columns=scores.columns,
    )
    samples = pd.DataFrame(250.0, index=scores.index, columns=scores.columns)
    monkeypatch.setattr(
        control,
        "expanding_calibration",
        lambda scores, close, horizon: (probability, expected, samples),
    )
    monkeypatch.setattr(
        control,
        "_market_budget",
        lambda close, **kwargs: (
            pd.Series(market, index=close.index),
            pd.Series(valid, index=close.index),
        ),
    )
    return probability, expected


def _plan(monkeypatch, *, cap: float = 0.30, market: float = 0.80, valid: bool = True,
          eligibility_mask: pd.DataFrame | None = None):
    panel = _panel()
    bundle = _bundle(panel)
    _stub_inputs(monkeypatch, bundle, market=market, valid=valid)
    due = pd.Series(False, index=panel["close"].index)
    due.iloc[-1] = True
    plan = build_position_plan(
        panel,
        bundle,
        top_n=4,
        horizon=3,
        profile="risk_adjusted",
        cap_weight=cap,
        policy_snapshot=_policy(),
        rebalance_mask=due,
        eligibility_mask=eligibility_mask,
    )
    return plan


def test_non_equal_sizing_cap_redistribution_and_conservation(monkeypatch):
    plan = _plan(monkeypatch)
    weights = plan.weights.iloc[-1]

    assert weights.sum() == pytest.approx(0.80)
    assert weights.max() <= 0.30 + 1e-12
    assert weights["A"] >= weights["B"] > weights["C"] > weights["D"]
    assert weights.nunique() > 2
    assert plan.target_exposure.iloc[-1] == pytest.approx(0.80)
    assert 1 - weights.sum() == pytest.approx(0.20)


def test_unallocatable_cap_capacity_remains_cash(monkeypatch):
    plan = _plan(monkeypatch, cap=0.15)
    weights = plan.weights.iloc[-1]

    assert weights.sum() == pytest.approx(0.60)
    assert (weights <= 0.15 + 1e-12).all()
    assert plan.target_exposure.iloc[-1] == pytest.approx(0.80)


def test_opportunity_scaling_and_membership_do_not_reinflate(monkeypatch):
    panel = _panel()
    mask = pd.DataFrame(False, index=panel["close"].index, columns=panel["close"].columns)
    mask.loc[:, ["A", "B"]] = True
    plan = _plan(monkeypatch, eligibility_mask=mask)

    assert plan.opportunity_scale.iloc[-1] == pytest.approx(0.50)
    assert plan.weights.iloc[-1].sum() == pytest.approx(0.40)
    assert plan.weights.iloc[-1, 2:].eq(0).all()


def test_market_floor_flat_and_invalid_input_withholds_signal(monkeypatch):
    flat = _plan(monkeypatch, market=0.19)
    assert flat.weights.iloc[-1].eq(0).all()
    assert bool(flat.intentional_flat.iloc[-1])
    assert flat.position_state.iloc[-1] == "flat"
    assert flat.reasons[flat.weights.index[-1]] == ("market_risk_off",)

    degraded = _plan(monkeypatch, valid=False)
    assert degraded.weights.iloc[-1].isna().all()
    assert bool(degraded.degraded.iloc[-1])
    assert not bool(degraded.intentional_flat.iloc[-1])

    short_panel = {key: value.iloc[:15] for key, value in _panel().items()}
    short_bundle = _bundle(short_panel)
    _stub_inputs(monkeypatch, short_bundle)
    due = pd.Series(False, index=short_panel["close"].index)
    due.iloc[-1] = True
    insufficient = build_position_plan(
        short_panel,
        short_bundle,
        top_n=4,
        horizon=3,
        profile="risk_adjusted",
        cap_weight=0.30,
        policy_snapshot=_policy(),
        rebalance_mask=due,
    )
    assert insufficient.weights.iloc[-1].isna().all()
    assert insufficient.position_state.iloc[-1] == "degraded"


def test_rebalance_buffer_and_hard_exit_bypass(monkeypatch):
    panel = _panel()
    bundle = _bundle(panel)
    probability, expected = _stub_inputs(monkeypatch, bundle)
    expected.iloc[-1, 1] += 0.0001
    due = pd.Series(False, index=panel["close"].index)
    due.iloc[-2:] = True
    plan = build_position_plan(
        panel,
        bundle,
        top_n=4,
        horizon=3,
        profile="risk_adjusted",
        cap_weight=0.30,
        policy_snapshot=_policy(band=0.01),
        rebalance_mask=due,
    )
    assert not plan.raw_weights.iloc[-1].equals(plan.weights.iloc[-2])
    pd.testing.assert_series_equal(
        plan.weights.iloc[-1], plan.weights.iloc[-2], check_names=False,
    )

    probability.iloc[-1] = 0.40
    exit_plan = build_position_plan(
        panel,
        bundle,
        top_n=4,
        horizon=3,
        profile="risk_adjusted",
        cap_weight=0.30,
        policy_snapshot=_policy(band=1.0),
        rebalance_mask=due,
    )
    assert exit_plan.weights.iloc[-1].eq(0).all()
    assert bool(exit_plan.intentional_flat.iloc[-1])


def test_capped_allocator_is_non_equal_and_never_overallocates():
    strength = pd.Series({"A": 10.0, "B": 3.0, "C": 1.0})
    weights = _allocate_capped(strength, target=0.90, cap=0.25)

    assert weights.sum() == pytest.approx(0.75)
    assert weights.max() == pytest.approx(0.25)


def test_expanding_calibration_uses_only_matured_outcomes():
    panel = _panel()
    scores = _bundle(panel)["score"]
    cutoff = scores.index[60]
    original = expanding_calibration(scores, panel["close"], 5)
    changed_close = panel["close"].copy()
    changed_close.loc[changed_close.index > cutoff] *= 50
    mutated = expanding_calibration(scores, changed_close, 5)

    for before, after in zip(original, mutated, strict=True):
        pd.testing.assert_series_equal(before.loc[cutoff], after.loc[cutoff])
