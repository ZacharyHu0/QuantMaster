"""统一问题协议与回测数据质量门禁。"""

import numpy as np
import pandas as pd
import pytest

from quantmaster.server.problems import (
    OperationProblem,
    assess_panel_quality,
    assess_signal_quality,
    make_problem,
)


def price_panel(symbols=("A", "B"), periods=25):
    dates = pd.bdate_range("2026-06-01", periods=periods)
    values = np.arange(periods, dtype=float) + 10
    return {
        "open": pd.DataFrame({symbol: values for symbol in symbols}, index=dates),
        "close": pd.DataFrame({symbol: values + 0.5 for symbol in symbols}, index=dates),
    }


def test_missing_required_price_field_blocks_backtest():
    panel = price_panel()
    panel.pop("open")

    with pytest.raises(OperationProblem) as caught:
        assess_panel_quality(panel, ["A", "B"], minimum_symbols=1, allow_partial=False)

    assert caught.value.status_code == 422
    assert caught.value.problem["code"] == "missing_price_fields"
    assert caught.value.problem["blocking"] is True
    assert caught.value.problem["can_continue"] is False


def test_partial_universe_requires_confirmation_then_returns_warning():
    panel = price_panel(("A",))

    with pytest.raises(OperationProblem) as caught:
        assess_panel_quality(panel, ["A", "B"], minimum_symbols=1, allow_partial=False)

    assert caught.value.status_code == 409
    assert caught.value.problem["code"] == "partial_market_data"
    assert caught.value.problem["can_continue"] is True
    assert caught.value.data_quality["missing_symbols"] == ["B"]

    quality, warnings = assess_panel_quality(
        panel, ["A", "B"], minimum_symbols=1, allow_partial=True,
    )
    assert quality["status"] == "partial"
    assert quality["usable_symbol_count"] == 1
    assert [item["code"] for item in warnings] == ["partial_market_data"]


def test_signal_without_next_day_price_is_blocked():
    panel = price_panel(("A",))
    quality, _ = assess_panel_quality(
        panel, ["A"], minimum_symbols=1, allow_partial=False,
    )
    weights = pd.DataFrame(np.nan, index=panel["close"].index, columns=["A"])
    weights.iloc[-1, 0] = 1.0

    with pytest.raises(OperationProblem) as caught:
        assess_signal_quality(panel, weights, quality, allow_partial=False)

    assert caught.value.status_code == 422
    assert caught.value.problem["code"] == "no_executable_signal"


def test_partial_execution_prices_require_confirmation():
    panel = price_panel(("A", "B"))
    quality, _ = assess_panel_quality(
        panel, ["A", "B"], minimum_symbols=2, allow_partial=False,
    )
    weights = pd.DataFrame(np.nan, index=panel["close"].index, columns=["A", "B"])
    weights.iloc[3] = [0.5, 0.5]
    panel["open"].iloc[4, 1] = np.nan

    with pytest.raises(OperationProblem) as caught:
        assess_signal_quality(panel, weights, quality, allow_partial=False)

    assert caught.value.status_code == 409
    assert caught.value.problem["code"] == "partial_execution_prices"
    assert caught.value.problem["can_continue"] is True

    warnings = assess_signal_quality(panel, weights, quality, allow_partial=True)
    assert quality["executable_signals"] == 1
    assert [item["code"] for item in warnings] == ["partial_execution_prices"]


def test_async_backtest_worker_persists_structured_problem(tmp_path, monkeypatch):
    from quantmaster.backtest.spec import BacktestSpec
    from quantmaster.backtest.workbench import BacktestService, BacktestStore, BacktestWorker

    store = BacktestStore(tmp_path / "runs.sqlite", tmp_path / "artifacts")
    service = BacktestService(store)
    spec = BacktestSpec.model_validate({
        "strategy": {"kind": "swing", "top_n": 1, "holding_days": 3},
        "universe": "demo", "start": "2026-01-01", "end": "2026-07-01",
        "benchmark": None,
    })
    queued = store.create(spec)
    run = store.claim_next("test-worker")
    problem = make_problem(
        "partial_market_data",
        severity="warning",
        source="策略回测",
        title="回测数据不完整",
        message="一只候选缺少行情。",
        action="补齐数据，或确认后仅用可用数据继续。",
        blocking=True,
        can_continue=True,
    )

    def blocked(*args, **kwargs):
        raise OperationProblem(409, problem, data_quality={"status": "needs_confirmation"})

    monkeypatch.setattr(service, "run", blocked)
    BacktestWorker(service).run_one(run)

    failed = store.get(queued["id"])
    assert failed["status"] == "failed"
    assert failed["result"]["problem"]["can_continue"] is True
    assert failed["result"]["data_quality"]["status"] == "needs_confirmation"
