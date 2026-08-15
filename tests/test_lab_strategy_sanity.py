"""Only the numerical sanity checks required by the 1–30 day Lab protocol."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from quantmaster.config import get_config
from quantmaster.factors.analysis import forward_returns
from quantmaster.lab.models import FactorSpec
from quantmaster.lab.research import WalkForwardSpec, walk_forward_folds
from quantmaster.lab.service import _ledger_weight_context
from quantmaster.lab.store import LabStore
from quantmaster.lab.strategy import (
    ensemble_weights,
    execute_daily_targets,
    holding_actions,
    return_curve_points,
)
from quantmaster.portfolio import Ledger, TradeRecord


def test_two_stock_t_plus_one_cost_turnover_and_return() -> None:
    dates = pd.bdate_range("2026-01-05", periods=4)
    scores = pd.DataFrame({"A": [2, 2, 2, 2], "B": [1, 1, 1, 1]}, index=dates)
    opens = pd.DataFrame({"A": [9, 10, 11, 11], "B": [21, 20, 18, 18]}, index=dates)
    result = execute_daily_targets(scores, {"open": opens}, horizon=3, top_n=10)
    buy_rate = (
        get_config().trade.commission_rate
        + get_config().trade.transfer_fee_rate
        + get_config().trade.slippage
    )

    # T close cannot earn T→T+1.  Both names execute at 10% at T+1 open.
    assert result["weights"].iloc[0].sum() == 0
    assert result["weights"].iloc[1].to_dict() == pytest.approx({"A": 0.1, "B": 0.1})
    assert result["turnover_series"].iloc[1] == pytest.approx(0.1)
    assert result["daily_gross"].iloc[1] == pytest.approx(0.1 * 0.10 + 0.1 * -0.10)
    assert result["daily_net"].iloc[1] == pytest.approx(
        result["daily_gross"].iloc[1] - 0.2 * buy_rate
    )


def test_longest_label_and_split_are_purged_for_30_sessions() -> None:
    dates = pd.bdate_range("2018-01-01", periods=2200)
    close = pd.DataFrame({"A": np.arange(1, len(dates) + 1, dtype=float)}, index=dates)
    label = forward_returns(close, periods=30)
    assert label.iloc[0, 0] == pytest.approx(close.iloc[30, 0] / close.iloc[0, 0] - 1)
    folds, sealed = walk_forward_folds(dates, WalkForwardSpec())
    positions = {date.strftime("%Y-%m-%d"): number for number, date in enumerate(dates)}
    for fold in folds:
        assert positions[fold.test_start] - positions[fold.train_end] - 1 >= 30
    assert positions[sealed.test_start] - positions[sealed.train_end] - 1 >= 30


def test_ensemble_rejects_duplicates_and_bounds_weights() -> None:
    rng = np.random.default_rng(42)
    dates = pd.bdate_range("2024-01-01", periods=30)
    symbols = list("ABCDEFGH")
    values = {
        "a": pd.DataFrame(rng.normal(size=(30, 8)), index=dates, columns=symbols),
        "c": pd.DataFrame(rng.normal(size=(30, 8)), index=dates, columns=symbols),
        "d": pd.DataFrame(rng.normal(size=(30, 8)), index=dates, columns=symbols),
        "e": pd.DataFrame(rng.normal(size=(30, 8)), index=dates, columns=symbols),
    }
    values["b"] = values["a"].copy()
    candidates = [
        {"version_id": name, "development_score": 5 - number}
        for number, name in enumerate(("a", "b", "c", "d", "e"))
    ]
    result = ensemble_weights(candidates, values, horizon=5)
    ids = {item["version_id"] for item in result}
    assert "a" in ids and "b" not in ids
    assert 3 <= len(result) <= 8
    assert sum(item["weight"] for item in result) == pytest.approx(1)
    assert all(0.05 <= item["weight"] <= 0.35 for item in result)


def test_only_the_exact_passed_horizon_can_deploy(tmp_path) -> None:
    store = LabStore(tmp_path / "lab.sqlite")
    _factor, version, _created = store.create_factor(FactorSpec(
        slug="sanity_factor", name="Sanity factor", expression="close",
    ))
    report = {
        "best_horizon": 3,
        "horizons": {
            "3": {"gates": {"passed": True}},
            "5": {"gates": {"passed": False, "soft_failures": ["RankIC"]}},
        },
        "gates": {"passed": True, "hard_failures": [], "soft_failures": []},
    }
    store.save_validation(version["id"], "dataset", report)
    store.approve(version["id"], actor="sanity")
    with pytest.raises(ValueError, match="5 日门槛未通过"):
        store.deploy(version["id"], universe="csi800", horizon=5, actor="sanity")
    assert store.deploy(
        version["id"], universe="csi800", horizon=3, actor="sanity",
    )["deployment_id"]


def test_return_curve_point_is_the_saved_sealed_evidence() -> None:
    evidence = {
        "metrics": {
            "net_annual_excess_return": 0.123,
            "net_information_ratio": 0.71,
            "max_drawdown": 0.08,
            "turnover": 0.12,
            "cost_annual": 0.02,
            "samples": 252,
        },
        "bootstrap": {"ci_95": [0.03, 0.20]},
    }
    points = return_curve_points([{
        "id": "strategy", "horizon": 7, "status": "shadow_challenger",
        "sealed_evidence": evidence,
    }])
    point = next(item for item in points if item["horizon"] == 7)
    assert point["annual_net_excess_return"] == evidence["metrics"]["net_annual_excess_return"]
    assert point["ci_95"] == evidence["bootstrap"]["ci_95"]


def test_holding_actions_and_missing_evidence_do_not_force_exit() -> None:
    actions = holding_actions(
        {"ADD": 0.08, "HOLD": 0.05, "REDUCE": 0.02, "BUY": 0.05},
        {"ADD": 0.05, "HOLD": 0.05, "REDUCE": 0.07, "EXIT": 0.04,
         "REVIEW": 0.04},
        confidence={"REVIEW": 0.2},
    )
    by_symbol = {item["symbol"]: item for item in actions}
    assert {symbol: by_symbol[symbol]["action"] for symbol in by_symbol} == {
        "ADD": "add", "BUY": "buy", "EXIT": "exit", "HOLD": "hold",
        "REDUCE": "reduce", "REVIEW": "review",
    }
    assert by_symbol["REVIEW"]["target_weight"] == 0


def test_real_ledger_weights_use_signal_date_local_closes(tmp_path) -> None:
    ledger = Ledger(tmp_path / "ledger.sqlite")
    ledger.add_cashflow("2026-01-02", 100_000)
    ledger.add_trade(TradeRecord("2026-01-05", "000001.SZ", "buy", 10, 1_000))
    ledger.add_trade(TradeRecord("2026-01-05", "600000.SH", "buy", 20, 2_000))
    dates = pd.to_datetime(["2026-01-05", "2026-01-06"])
    panel = {"close": pd.DataFrame({
        "000001.SZ": [10.0, 12.0], "600000.SH": [20.0, 18.0],
    }, index=dates)}

    current, summary, priced = _ledger_weight_context(
        ledger, panel, pd.Timestamp("2026-01-06"),
    )

    assert summary["total_assets"] == pytest.approx(98_000)
    assert summary["cash"] == pytest.approx(50_000)
    assert current == pytest.approx({
        "000001.SZ": 12_000 / 98_000,
        "600000.SH": 36_000 / 98_000,
    })
    assert priced == {"000001.SZ", "600000.SH"}
    assert summary["reliable"] is True
