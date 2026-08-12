"""统一问题协议与回测数据质量门禁。"""

import numpy as np
import pandas as pd
import pytest

from quantmaster.server.problems import (
    OperationProblem,
    assess_panel_quality,
    assess_signal_quality,
    collect_health_report,
    make_problem,
)


def price_panel(symbols=("A", "B"), periods=25):
    dates = pd.bdate_range("2026-06-01", periods=periods)
    values = np.arange(periods, dtype=float) + 10
    return {
        "open": pd.DataFrame({symbol: values for symbol in symbols}, index=dates),
        "close": pd.DataFrame({symbol: values + 0.5 for symbol in symbols}, index=dates),
    }


def test_provider_health_problem_reports_remote_failures_and_local_blocks(monkeypatch):
    from quantmaster.data.resilience import PROVIDER_HEALTH

    monkeypatch.setattr(
        PROVIDER_HEALTH,
        "status",
        lambda _lane=None: {
            "tushare:etf_basic": {
                "state": "disabled",
                "open_until": 0,
                "failure_class": "permission",
                "last_error": "permission denied",
                "last_success": 0,
                "failures": 2,
                "suppressed": 7,
            }
        },
    )

    report = collect_health_report()
    problem = next(
        item for item in report["issues"] if item["id"] == "provider:tushare:etf_basic"
    )

    assert problem["remote_failures"] == 2
    assert problem["local_blocks"] == 7
    assert problem["title"] == "Tushare（基金目录）已停止自动请求"
    assert problem["message"] == "当前账号没有读取这项数据的权限。"
    assert problem["severity"] == "info"
    assert problem["provider_status"] == "permission_missing"
    assert problem["capability"] == "etf_basic"
    assert problem["diagnostic_id"].startswith("provider:tushare:etf_basic:")
    assert "permission denied" not in str(problem)


def test_llm_health_problem_keeps_safe_diagnostic_context(monkeypatch):
    monkeypatch.setattr(
        "quantmaster.ai.llm.llm_provider_health",
        lambda: [{
            "status": "degraded", "provider": "openai-compatible",
            "endpoint": "https://gateway.example/v1", "model": "test-model",
            "error_code": "http_429", "error_category": "rate_limit",
            "message": "触发限流或额度限制", "response_summary": "上游返回了错误响应（内容已隐藏）",
            "last_request_id": "llm-diagnostic-42", "http_status": 429,
            "retry_after_seconds": 15, "retry_status": "等待退避重试",
            "next_retry_at": "2026-08-13T00:00:15+00:00", "occurred_at": "2026-08-13T00:00:00+00:00",
            "last_success_at": "2026-08-12T23:59:00+00:00",
        }],
    )

    report = collect_health_report()
    problem = next(item for item in report["issues"] if item["id"] == "llm:openai-compatible:test-model")

    assert problem["code"] == "http_429"
    assert problem["diagnostic_id"] == "llm-diagnostic-42"
    assert problem["endpoint"] == "https://gateway.example/v1"
    assert problem["http_status"] == 429
    assert problem["retry_status"] == "等待退避重试"
    assert problem["can_continue"] is True


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


def test_intentional_flat_signal_is_valid_cash_period():
    panel = price_panel(("A",))
    quality, _ = assess_panel_quality(
        panel, ["A"], minimum_symbols=1, allow_partial=False,
    )
    weights = pd.DataFrame(np.nan, index=panel["close"].index, columns=["A"])
    weights.iloc[3, 0] = 0.0
    intentional_flat = pd.Series(False, index=weights.index)
    intentional_flat.iloc[3] = True

    warnings = assess_signal_quality(
        panel,
        weights,
        quality,
        allow_partial=False,
        intentional_flat=intentional_flat,
    )

    assert [item["code"] for item in warnings] == ["intentional_cash_backtest"]
    assert quality["intentional_flat_signal_dates"] == 1
    assert quality["selected_signals"] == 0


def test_async_backtest_worker_persists_structured_problem(tmp_path, monkeypatch):
    from quantmaster.backtest.spec import BacktestSpec
    from quantmaster.backtest.workbench import BacktestService, BacktestStore, BacktestWorker

    store = BacktestStore(tmp_path / "runs.sqlite", tmp_path / "artifacts")
    service = BacktestService(store)
    spec = BacktestSpec.model_validate({
        "strategy": {"kind": "factor", "factor": "mom_20d", "top_n": 1},
        "universe": "demo", "start": "2026-01-01", "end": "2026-07-01",
        "benchmark": None,
    })
    queued = store.create(spec)
    worker = BacktestWorker(service)
    run = store.claim_next(worker.worker_id)
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
    worker.run_one(run)

    failed = store.get(queued["id"])
    assert failed["status"] == "needs_confirmation"
    assert failed["result"]["problem"]["can_continue"] is True
    assert failed["result"]["data_quality"]["status"] == "needs_confirmation"
