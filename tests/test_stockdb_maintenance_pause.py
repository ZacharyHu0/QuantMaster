"""Planned StockDB downtime must preserve refresh work without supplier storms."""

from __future__ import annotations

import json
import threading
import time

import pandas as pd
import pytest

from quantmaster.data.maintenance import DATA_REFRESH_TASK_TYPE, DataRefreshManager
from quantmaster.runtime.jobs import JobOutcome, UnifiedJobRuntime, UnifiedJobStore
from quantmaster.runtime.sqlite import connect_sqlite


@pytest.fixture
def accepted_bars(isolated_config, monkeypatch):
    from quantmaster.data import registry
    from quantmaster.data.free_stockdb_source import FreeStockDBSource
    from quantmaster.data.storage import BarStore

    root = isolated_config.data_root / "stockdb"
    root.mkdir()
    isolated_config.data.free_stockdb_root = str(root)
    isolated_config.data.primary_provider = "free-stockdb"
    dates = pd.bdate_range("2026-07-01", "2026-08-07")
    frame = FreeStockDBSource._frame([
        {"date": day.strftime("%Y%m%d"), "open": 10, "high": 11, "low": 9,
         "close": 10, "volume": 100, "amount": 1000} for day in dates
    ], intraday=False)
    calls = []

    def daily(source, symbol, start, end):
        calls.append((symbol, start, end))
        return source._bind_session_acceptance(frame.loc[start:end].copy(), end)

    monkeypatch.setattr(FreeStockDBSource, "daily", daily)
    monkeypatch.setattr(registry, "_local_sessions", lambda *args: (pd.DatetimeIndex([]), "unavailable"))
    monkeypatch.setattr(registry, "_request_factories", lambda **kwargs: {
        registry.Market.CN: [FreeStockDBSource],
    })
    store = BarStore()
    quality = registry._assess_daily_frame(
        frame, "2026-07-01", "2026-08-07", symbol="600000.SH", source="free-stockdb",
    )
    store.put("600000.SH", frame, replace=True, replace_coverage=True,
              request_start="2026-07-01", request_end="2026-08-07",
              source="free-stockdb", quality=quality.to_dict())
    store.mark_status("600000.SH", "stale")

    def accept(stamp="2026-08-07T18:00:00+08:00"):
        (root / ".quantmaster-update.json").write_text(json.dumps({
            "schema_version": 2, "validated_session": "2026-08-07",
            "target_session": "2026-08-07", "updated_at": stamp,
            "validation": {"accepted": True, "complete": False,
                           "actual_session": "2026-08-07", "target_session": "2026-08-07"},
        }), encoding="utf-8")

    return store, frame, calls, accept


def test_accepted_generation_replaces_old_local_evidence_once(accepted_bars):
    from quantmaster.data import registry

    store, _, calls, accept = accepted_bars
    accept()
    first = registry.refresh_history("600000.SH", "2026-07-01", "2026-08-07", store=store)
    assert calls == [("600000.SH", "2026-07-01", "2026-08-07")]
    assert first.quality.analysis_eligible and not first.quality.stale
    assert not first.quality.formal_eligible
    assert first.quality.semantic_diagnostic_code == "factor_contract_incomplete"
    assert len(json.loads(store.metadata("600000.SH")["source_chain_json"])) == 1
    registry.refresh_history("600000.SH", "2026-07-01", "2026-08-07", store=store)
    assert len(calls) == 1
    accept("2026-08-07T19:00:00+08:00")
    registry.refresh_history("600000.SH", "2026-07-01", "2026-08-07", store=store)
    assert len(calls) == 2


def test_accepted_resume_refreshes_real_cache_and_projects_formal_warnings(
    manager, owner, accepted_bars, monkeypatch,
):
    monkeypatch.setattr(manager, "_fingerprint", DataRefreshManager._fingerprint)
    store, _, calls, accept = accepted_bars
    owner("validating", "validating")
    job = manager._submit("watchlist", "", "2026-07-01", "2026-08-07", ["600000.SH"])
    assert run_due(manager, job["id"])["waiting_on"] == "stockdb_update"
    assert not calls
    accept()
    owner("completed", "success")
    runtime = manager._ensure_runtime()
    with runtime.store._conn() as db:
        db.execute("UPDATE runtime_jobs SET next_retry_at=0 WHERE id=?", (job["id"],))
    runtime.start()
    runtime.wait(job["id"], 10)
    result = manager.get(job["id"])
    assert result["id"] == job["id"] and result["attempt"] == 1
    assert result["status"] == "completed" and not result["waiting_on"]
    assert result["succeeded"] == 1 and result["failed"] == 0
    assert result["outcome"] == "completed_with_warnings" and result["warning_count"] == 1
    assert result["warnings"][0]["formal_eligible"] is False
    assert "factor_contract" in result["warnings"][0]["warning"]
    assert len(calls) == 1
    assert store.get("600000.SH").attrs["factor_coverage"] == "unconfirmed"
    for _ in range(3):
        reused = manager._submit("watchlist", "", "2026-07-01", "2026-08-07", ["600000.SH"])
        assert reused["id"] == job["id"] and reused["reused"]
    assert len(calls) == 1 and len(manager.list()) == 1
    accept("2026-08-07T19:00:00+08:00")
    changed = manager._submit("watchlist", "", "2026-07-01", "2026-08-07", ["600000.SH"])
    assert changed["id"] != job["id"] and changed["created"]

    from quantmaster.server.jobs import _apply_domain_projection

    public = {}
    _apply_domain_projection(public, "data", result)
    assert public["warnings"] == result["warnings"] and public["warning_count"] == 1
    assert public["outcome"] == "completed_with_warnings"


@pytest.mark.parametrize("missing", ["receipt", "latest", "units", "numeric", "history"])
def test_daily_preparation_does_not_hide_real_gaps(accepted_bars, missing):
    store, frame, _, accept = accepted_bars
    if missing != "receipt":
        accept()
    if missing == "latest":
        frame.drop(frame.index[-1], inplace=True)
    elif missing == "units":
        frame.attrs["unit_status"] = "unverified"
    elif missing == "numeric":
        frame.loc[frame.index[-1], "high"] = -1
    elif missing == "history":
        frame.drop(frame.index[10], inplace=True)
    outcome = DataRefreshManager._refresh_one(store, "600000.SH", "2026-07-01", "2026-08-07")
    assert outcome and "error" in outcome and "warning" not in outcome


@pytest.fixture
def owner(isolated_config, tmp_path, monkeypatch):
    path = tmp_path / "owner.sqlite"
    monkeypatch.setenv("QM_FREE_STOCKDB_CONTROL_PATH", str(path))
    isolated_config.data.free_stockdb_managed = True
    with connect_sqlite(path) as db:
        db.execute("CREATE TABLE runtime_state(singleton INTEGER, payload_json TEXT, updated_at REAL)")
        db.execute("INSERT INTO runtime_state VALUES(1, '{}', 0)")

    def publish(phase, result="running", *, age=0, state=""):
        phases = {"stopping", "syncing", "validating", "restarting", "closing"}
        payload = {"state": state or ("updating" if phase in phases
                   else "running"), "phase": phase, "update_result": result,
                   "managed": True, "message": f"owner:{phase}:{result}"}
        with connect_sqlite(path) as db:
            db.execute("UPDATE runtime_state SET payload_json=?,updated_at=?",
                       (json.dumps(payload), time.time() - age))
    publish("completed", "success")
    return publish


@pytest.fixture
def manager(isolated_config, monkeypatch, owner):
    value = DataRefreshManager()
    value.initialize()
    monkeypatch.setattr(value, "_start", lambda _: None)
    monkeypatch.setattr(value, "_publish_market_snapshot", lambda: None)
    monkeypatch.setattr(value, "_fingerprint", lambda symbols: "accepted-generation")
    yield value
    value.shutdown()


def run_due(manager, job_id):
    runtime = manager._ensure_runtime()
    # Advance only the persisted admission timestamp, not runtime lease clocks.
    with runtime.store._conn() as db:
        db.execute("UPDATE runtime_jobs SET next_retry_at=0 WHERE id=?", (job_id,))
    manager._run(job_id)
    return manager.get(job_id)


@pytest.mark.parametrize("phase,result", [
    ("stopping", "running"), ("syncing", "running"), ("validating", "validating"),
    ("closing", "running"), ("restarting", "running"), ("retry_wait", "retry_wait"),
    ("completed", "failed"), ("completed", "manual_required"),
])
def test_pause_coalesces_and_recovers_without_attempt_exhaustion(manager, owner, monkeypatch, phase, result):
    calls = []
    monkeypatch.setattr(manager, "_refresh_one", lambda store, symbol, *args: calls.append(symbol))
    owner(phase, result)
    def submit():
        return manager._submit("watchlist", "", "2026-01-01", "2026-09-17", ["600000.SH"])
    job = submit()
    for _ in range(10):
        waiting = run_due(manager, job["id"])
        assert waiting["status"] == "interrupted"
        assert waiting["waiting_on"] == "stockdb_update"
        assert waiting["attempt"] == 1
        assert waiting["failed"] == waiting["succeeded"] == 0
        assert waiting["can_cancel"] and not waiting["can_retry"]
        assert submit()["id"] == job["id"]
    assert not calls
    assert len(manager.list()) == 1
    assert manager.active  # Deferred business work is never declared complete.
    owner("completed", "success")
    completed = run_due(manager, job["id"])
    assert completed["status"] == "completed"
    assert completed["succeeded"] == 1 and completed["failed"] == 0
    assert calls == ["600000.SH"]
    assert submit()["id"] == job["id"]


def test_mixed_sources_continue_and_wait_is_public(manager, owner, monkeypatch):
    from quantmaster.server.jobs import _public_job

    calls = []
    monkeypatch.setattr(manager, "_refresh_one", lambda store, symbol, *args: calls.append(symbol))
    owner("syncing")
    job = manager._submit("watchlist", "", "2026-01-01", "2026-09-17", ["600000.SH", "AAPL.US"])
    waiting = run_due(manager, job["id"])
    assert calls == ["AAPL.US"]
    assert waiting["succeeded"] == 1
    public = _public_job("data", waiting)
    assert public["waiting_on"] == "stockdb_update"
    assert public["next_retry_at"] > time.time()
    owner("completed", "success")
    assert run_due(manager, job["id"])["succeeded"] == 2
    assert calls == ["AAPL.US", "600000.SH"]


@pytest.mark.parametrize("parallelism,total", [(1, 300), (8, 800)])
def test_inflight_stops_at_unit_boundary_and_cancellation_survives(
    manager, owner, monkeypatch, parallelism, total,
):
    started, release = threading.Event(), threading.Event()
    calls = []
    manager.MAX_PARALLEL_SYMBOLS = parallelism

    def refresh(store, symbol, *args):
        calls.append(symbol)
        started.set()
        assert release.wait(5)
        return {"error": "planned shutdown", "code": "transient_network", "retryable": True}

    monkeypatch.setattr(manager, "_refresh_one", refresh)
    job = manager._submit("watchlist", "", "2026-01-01", "2026-09-17",
                          [f"{600000 + i}.SH" for i in range(total)])
    runtime = manager._ensure_runtime()
    runtime.dispatch_job(job["id"])
    try:
        assert started.wait(5)
        owner("stopping")
    finally:
        release.set()
    runtime.wait(job["id"], 10)
    assert 1 <= len(calls) <= parallelism
    before_cancel = list(calls)
    assert manager.get(job["id"])["failed"] == 0
    manager.cancel(job["id"])
    owner("completed", "success")
    assert run_due(manager, job["id"])["status"] == "cancelled"
    assert calls == before_cancel


def test_csi800_planning_wait_and_automatic_dispatch_resume(manager, owner, monkeypatch):
    calls = []
    monkeypatch.setattr(manager, "_resolve_symbols", lambda *args: calls.append("plan") or ["600000.SH"])
    monkeypatch.setattr(manager, "_refresh_one", lambda *args: calls.append("refresh"))
    owner("validating", "validating")
    job = manager.create("universe", "csi800")
    waiting = run_due(manager, job["id"])
    assert waiting["status"] == "interrupted" and not calls
    owner("completed", "success")
    runtime = manager._ensure_runtime()
    with runtime.store._conn() as db:
        db.execute("UPDATE runtime_jobs SET next_retry_at=0 WHERE id=?", (job["id"],))
    runtime.start()  # Ordinary dispatcher resumes persisted work; no manual refresh.
    runtime.wait(job["id"], 10)
    assert manager.get(job["id"])["status"] == "completed"
    assert calls == ["plan", "refresh"]


def test_disabled_auto_and_stale_owner_are_explicit(manager, owner, isolated_config):
    owner("syncing", age=90)
    assert "失联" in manager._stockdb_wait_reason()
    isolated_config.data.repair_enabled = False
    assert manager.maintain_consumed() == []
    assert not manager.list()
    isolated_config.data.free_stockdb_managed = False
    assert not manager._stockdb_wait_reason()


@pytest.mark.parametrize("state,age", [("running", 90), ("stopped", 0), ("degraded", 0)])
def test_completed_receipt_does_not_hide_offline_owner(manager, owner, state, age):
    owner("completed", "success", age=age, state=state)
    assert manager._stockdb_wait_reason()
    owner("completed", "success")
    assert not manager._stockdb_wait_reason()


def test_dependency_wait_cancel_fence(tmp_path):
    store = UnifiedJobStore(tmp_path / "jobs.sqlite")
    first = UnifiedJobRuntime(store, dispatch=False)
    job, _ = store.submit(DATA_REFRESH_TASK_TYPE, {})
    assert store.claim(job["id"], first.identity.value)
    token = store.get(job["id"])["lease_token"]
    store.cancel(job["id"])
    result = store.finish(
        job["id"], first.identity.value,
        JobOutcome("interrupted", "waiting", waiting_on="stockdb_update"), lease_token=token,
    )
    assert result["status"] == "cancelled"
    assert not result["waiting_on"]
    first.stop()


def test_wait_survives_manager_restart_and_rechecks_previous_success(manager, owner, monkeypatch):
    calls = []
    manager.MAX_PARALLEL_SYMBOLS = 1

    def refresh(store, symbol, *args):
        calls.append(symbol)
        owner("stopping")

    monkeypatch.setattr(manager, "_refresh_one", refresh)
    job = manager._submit("watchlist", "", "2026-01-01", "2026-09-17", ["600000.SH", "600001.SH"])
    assert run_due(manager, job["id"])["status"] == "interrupted"
    assert calls == ["600000.SH"]
    manager.shutdown()
    second = DataRefreshManager()
    second.initialize()
    monkeypatch.setattr(second, "_publish_market_snapshot", lambda: None)
    monkeypatch.setattr(second, "_refresh_one", lambda store, symbol, *args: calls.append(symbol))
    try:
        assert second.get(job["id"])["waiting_on"] == "stockdb_update"
        owner("completed", "success")
        assert run_due(second, job["id"])["status"] == "completed"
        assert calls[0] == "600000.SH"
        assert sorted(calls[1:]) == ["600000.SH", "600001.SH"]
    finally:
        second.shutdown()


def test_repeated_automatic_ticks_only_queue_one_job_per_scope(manager, owner, isolated_config, monkeypatch):
    owner("syncing")
    isolated_config.automation.watchlist = ["600000.SH", "AAPL.US"]
    isolated_config.automation.primary_universe = "csi800"
    isolated_config.lab.enabled = False
    monkeypatch.setattr("quantmaster.data.maintenance.market_symbols", lambda: ["AAPL.US"])
    monkeypatch.setattr(manager, "_refresh_one", lambda *args: None)
    monkeypatch.setattr(manager, "_resolve_symbols",
                        lambda scope, *args: ["AAPL.US"] if scope == "market" else ["600000.SH"])
    for _ in range(3):
        for job in manager.maintain_consumed():
            run_due(manager, job["id"])
    assert len(manager.list()) == 3
    assert len([job for job in manager.list() if job["waiting_on"]]) == 2
    owner("completed", "success")
    for job in manager.maintain_consumed():
        run_due(manager, job["id"])


def test_independent_primary_continues_without_offline_stockdb_fallback(
    manager, owner, isolated_config, monkeypatch,
):
    from quantmaster.data.resilience import LocalOnlyDataAccessError, data_priority, provider_call

    isolated_config.data.primary_provider = "tushare"
    isolated_config.data.tushare_enabled = True
    owner("syncing")
    calls = []
    monkeypatch.setattr(manager, "_refresh_one", lambda store, symbol, *args: calls.append(symbol))
    job = manager._submit("watchlist", "", "2026-01-01", "2026-09-17", ["600000.SH"])
    assert run_due(manager, job["id"])["status"] == "completed"
    assert calls == ["600000.SH"]
    with data_priority("maintenance"), pytest.raises(LocalOnlyDataAccessError, match="StockDB"):
        provider_call("free-stockdb", "fallback", lambda: calls.append("offline"))
    assert calls == ["600000.SH"]
    # The owner must still be able to validate the restored database.
    provider_call("free-stockdb", "owner-validation", lambda: calls.append("validate"))
    assert calls == ["600000.SH", "validate"]


def test_planned_failures_do_not_poison_or_reset_existing_circuits(owner):
    from quantmaster.data.resilience import ProviderHealthStore, ProviderTimeoutError

    health = ProviderHealthStore()
    health.failure("free-stockdb", ConnectionError("real earlier fault"), immediate=True)
    before = health.status()["free-stockdb"]
    owner("stopping")
    for _ in range(8):
        health.failure("free-stockdb", ProviderTimeoutError("free-stockdb", 30, first=True), immediate=True)
    assert health.status()["free-stockdb"] == before
    health.failure("tushare", ConnectionError("independent fault"), immediate=True)
    assert health.status()["tushare"]["failures"] == 1
    owner("completed", "success")
    health.failure("free-stockdb", ConnectionError("real new fault"), immediate=True)
    assert health.status()["free-stockdb"]["failures"] > before["failures"]


def test_recovery_waits_out_existing_cooldown_without_reset(manager, owner, monkeypatch):
    from quantmaster.data.resilience import PROVIDER_HEALTH

    calls = []
    monkeypatch.setattr(manager, "_refresh_one", lambda store, symbol, *args: calls.append(symbol))
    PROVIDER_HEALTH.failure("free-stockdb", ConnectionError("earlier outage"), immediate=True)
    before = PROVIDER_HEALTH.status("free-stockdb")["free-stockdb"]
    owner("syncing")
    job = manager._submit("watchlist", "", "2026-01-01", "2026-09-17", ["600000.SH"])
    assert run_due(manager, job["id"])["waiting_on"] == "stockdb_update"
    owner("completed", "success")
    waiting = run_due(manager, job["id"])
    assert "冷却" in waiting["detail"] and waiting["failed"] == 0
    assert not calls
    assert PROVIDER_HEALTH.status("free-stockdb")["free-stockdb"] == before
    # Advance the clock past the actual recorded deadline, preserving the row.
    monkeypatch.setattr("quantmaster.data.maintenance.time.time", lambda: before["open_until"] + 1)
    owner("completed", "success")  # The live owner continues heartbeating during cooldown.
    assert run_due(manager, job["id"])["status"] == "completed"
    assert calls == ["600000.SH"]
