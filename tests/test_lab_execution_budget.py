"""Cash conservation across blocked rotations and changing open prices."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from quantmaster.config import get_config
from quantmaster.lab.models import canonical_json
from quantmaster.lab.service import LabService
from quantmaster.lab.store import LabStore
from quantmaster.lab.strategy import EXECUTION_CONTRACT, execute_daily_targets, strategy_sealed_gate


def _rotation():
    dates = pd.bdate_range("2026-01-05", periods=5)
    columns = [f"S{i:02}" for i in range(20)]
    scores = pd.DataFrame(0.0, index=dates, columns=columns)
    scores.iloc[0, :10] = 1.0
    scores.iloc[1:, 10:] = 1.0
    opens = pd.DataFrame(10.0, index=dates, columns=columns)
    return scores, {"open": opens}


def _rates():
    trade = get_config().trade
    buy = trade.commission_rate + trade.transfer_fee_rate + trade.slippage
    return buy, buy + trade.stamp_tax_rate


@pytest.mark.parametrize("blocked_count", [5, 10])
@pytest.mark.parametrize("block", ["suspended", "down_limit"])
def test_blocked_rotation_spends_only_net_sale_proceeds(blocked_count, block):
    scores, panel = _rotation()
    panel[block] = pd.DataFrame(
        False if block == "suspended" else 9.0,
        index=scores.index, columns=scores.columns,
    )
    panel[block].iloc[2:, :blocked_count] = True if block == "suspended" else 10.0
    result = execute_daily_targets(scores, panel, horizon=3, top_n=10)
    weights = result["weights"]
    buy_rate, sell_rate = _rates()
    # Initial fees are reserved. Flat prices then leave 10% of NAV in each old name.
    assert weights.iloc[1].sum() == pytest.approx(1 / (1 + buy_rate))
    retained = blocked_count / 10
    sold = 1 - retained
    bought = sold * (1 - sell_rate) / (1 + buy_rate)
    assert weights.iloc[2, :blocked_count].tolist() == pytest.approx([0.1] * blocked_count)
    assert weights.iloc[2, 10:].sum() == pytest.approx(bought)
    assert result["costs"].iloc[2] == pytest.approx(sold * sell_rate + bought * buy_rate)
    assert result["turnover_series"].iloc[2] == pytest.approx((sold + bought) / 2)
    assert (weights.sum(axis=1) <= 1 + 1e-12).all()


def test_price_drift_is_rebalanced_and_cash_reconciles():
    dates = pd.bdate_range("2026-01-05", periods=4)
    scores = pd.DataFrame({"A": 1.0, "B": 0.0}, index=dates)
    opens = pd.DataFrame({"A": [10, 10, 20, 20], "B": [10, 10, 10, 10]}, index=dates)
    result = execute_daily_targets(scores, {"open": opens}, horizon=3, top_n=10)
    buy_rate, sell_rate = _rates()
    previous_nav = 1.1 - 0.2 * buy_rate
    sale = 0.2 / previous_nav - 0.1
    purchase = 0.1 - 0.1 / previous_nav
    assert result["costs"].iloc[2] == pytest.approx(sale * sell_rate + purchase * buy_rate)
    assert result["daily_net"].iloc[1] == pytest.approx(0.1 - 0.2 * buy_rate)
    assert result["weights"].iloc[2].to_dict() == pytest.approx({"A": 0.1, "B": 0.1})
    np.testing.assert_allclose(
        result["weights"].sum(axis=1) + result["cash_weights"] + result["costs"], 1.0,
    )


def test_blocked_holdings_recover_without_fictional_sales():
    scores, panel = _rotation()
    panel["suspended"] = pd.DataFrame(False, index=scores.index, columns=scores.columns)
    panel["suspended"].iloc[2:4, :10] = True
    result = execute_daily_targets(scores, panel, horizon=3, top_n=10)
    assert result["weights"].iloc[2:4, 10:].to_numpy().sum() == pytest.approx(0)
    assert result["costs"].iloc[2:4].sum() == pytest.approx(0)
    assert result["weights"].iloc[4, :10].sum() == pytest.approx(0)
    assert result["weights"].iloc[4, 10:].sum() > 0.99


@pytest.mark.parametrize("price", [np.nan, np.inf, -np.inf, 0, -1])
def test_invalid_buy_price_keeps_cash(price):
    scores, panel = _rotation()
    panel["open"].iloc[1, :10] = price
    result = execute_daily_targets(scores, panel, horizon=3, top_n=10)
    assert result["weights"].iloc[1].sum() == 0
    assert result["costs"].iloc[1] == 0
    assert np.isfinite(result["daily_net"]).all()


@pytest.mark.parametrize("price", [np.nan, np.inf, 0, -1])
def test_missing_held_valuation_requires_new_evidence(price):
    scores, panel = _rotation()
    panel["open"].iloc[2, 0] = price
    with pytest.raises(ValueError, match="LAB_EXECUTION_PRICE_INVALID"):
        execute_daily_targets(scores, panel, horizon=3, top_n=10)


def test_limit_up_buy_is_not_charged_or_redistributed():
    scores, panel = _rotation()
    panel["up_limit"] = pd.DataFrame(11.0, index=scores.index, columns=scores.columns)
    panel["up_limit"].iloc[1, :5] = 10.0
    result = execute_daily_targets(scores, panel, horizon=3, top_n=10)
    assert result["weights"].iloc[1, :5].sum() == 0
    assert result["weights"].iloc[1, 5:10].tolist() == pytest.approx([0.1] * 5)
    assert result["costs"].iloc[1] == pytest.approx(0.5 * _rates()[0])


@pytest.mark.parametrize("cap", [0, -0.1, 1.1, np.inf, np.nan])
def test_invalid_weight_cap_fails_explicitly(cap):
    scores, panel = _rotation()
    with pytest.raises(ValueError, match="cap_weight"):
        execute_daily_targets(scores, panel, horizon=3, top_n=10, cap_weight=cap)


def test_cash_and_holdings_follow_an_independent_share_ledger():
    scores, panel = _rotation()
    panel["open"] *= np.random.default_rng(23).uniform(0.9, 1.1, scores.shape).cumprod(axis=0)
    panel["suspended"] = pd.DataFrame(False, index=scores.index, columns=scores.columns)
    panel["suspended"].iloc[2:4, :6] = True
    result = execute_daily_targets(scores, panel, horizon=3, top_n=10)
    cash = 1.0
    shares = pd.Series(0.0, index=scores.columns)
    buy_rate, sell_rate = _rates()
    for i, date in enumerate(scores.index):
        prices = panel["open"].loc[date]
        nav = cash + float((shares * prices).sum())
        values = result["weights"].loc[date] * nav
        trades = values - shares * prices
        fees = float(trades.clip(lower=0).sum()) * buy_rate
        fees -= float(trades.clip(upper=0).sum()) * sell_rate
        cash -= float(trades.sum()) + fees
        assert cash >= -1e-12
        assert result["cash_weights"].loc[date] == pytest.approx(cash / nav)
        blocked = panel["suspended"].loc[date]
        assert trades[blocked].to_numpy() == pytest.approx(np.zeros(int(blocked.sum())))
        shares = values / prices
        next_prices = panel["open"].iloc[min(i + 1, len(scores) - 1)]
        next_nav = cash + float((shares * next_prices).sum())
        assert result["daily_net"].loc[date] == pytest.approx(next_nav / nav - 1)
    assert result["metrics"]["execution_contract"] == EXECUTION_CONTRACT


@pytest.mark.parametrize("contract", [None, "obsolete_v0", EXECUTION_CONTRACT])
def test_execution_contract_is_required_by_sealed_gate(contract):
    metrics = {
        "execution_contract": contract, "net_information_ratio": 1.0,
        "net_annual_excess_return": 0.2, "positive_folds": 4,
        "max_drawdown": 0.1, "calmar": 2.0,
    }
    gate = strategy_sealed_gate(metrics, {"probability_positive": 0.9})
    assert gate["passed"] is (contract == EXECUTION_CONTRACT)
    assert gate["override_allowed"] is False


def test_old_strategy_evidence_stays_readable_but_cannot_authorize_actions(tmp_path):
    store = LabStore(tmp_path / "lab.sqlite")
    cycle = store.create_research_cycle(snapshot_id="synthetic", protocol={})
    evidence = {"metrics": {"execution_contract": EXECUTION_CONTRACT}, "gates": {"passed": True}}
    candidate = store.save_strategy_candidate(
        cycle_id=cycle["id"], horizon=3, name="budget",
        components=[{"version_id": str(i), "weight": w} for i, w in enumerate([0.34, 0.33, 0.33])],
        development={}, sealed_evidence=evidence,
    )
    store.update_strategy_tracking(candidate["id"], shadow={
        "matured_signal_days": 20, "net_excess_return": 0.1,
        "drawdown_within_stress": True, "coverage_degraded": False,
    })
    assert LabService._shadow_candidates(store, candidate["id"])
    # Simulate an already-saved legacy strategy, including its stale passed gate.
    evidence["metrics"] = {"net_information_ratio": 2.0}
    original = canonical_json(evidence)
    with store._conn() as conn:
        conn.execute("UPDATE strategy_candidates SET sealed_json=? WHERE id=?", (original, candidate["id"]))
    old = store.strategy(candidate["id"])
    assert old["sealed_evidence"]["metrics"]["net_information_ratio"] == 2.0
    assert old["sealed_evidence"]["gates"]["passed"] is False
    assert "LAB_EXECUTION_REVALIDATION_REQUIRED" in old["sealed_evidence"]["gates"]["failures"][0]
    assert LabService._shadow_candidates(store, candidate["id"]) == []
    assert LabService._shadow_candidates(store, "") == []
    with pytest.raises(ValueError, match="密封集硬门槛"):
        store.promote_strategy(candidate["id"], target="paper", actor="test", reason="override")
    with pytest.raises(ValueError, match="LAB_EXECUTION_REVALIDATION_REQUIRED"):
        store.save_shadow_signal(
            candidate["id"], signal_date="2026-01-05", mature_date="2026-01-08", payload={},
        )
    with store._conn() as conn:
        saved = conn.execute(
            "SELECT sealed_json FROM strategy_candidates WHERE id=?", (candidate["id"],),
        ).fetchone()[0]
        assert saved == original
    revised = store.save_strategy_candidate(
        cycle_id=cycle["id"], horizon=3, name="old revision", components=candidate["components"],
        development={}, sealed_evidence=evidence,
    )
    assert revised["status"] == "historical_candidate"
    assert revised["sealed_evidence"]["gates"]["passed"] is False
