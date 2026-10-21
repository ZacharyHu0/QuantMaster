"""Durable update orchestration; providers are replaced, never called."""
from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

import pytest

from quantmaster.data.maintenance import DataRefreshManager
from quantmaster.server.data_update import run_update


class Context:
    spec_hash = "plan"
    deadline_seconds = 3600

    def __init__(self):
        self.checkpoint = None

    def load_checkpoint(self, *args):
        return deepcopy(self.checkpoint)

    def write_checkpoint(self, key, spec_hash, state):
        self.checkpoint = deepcopy(state)

    def cancelled(self):
        return False

    def progress(self, *args):
        pass

    def write_artifact(self, *args):
        return {"id": "result"}


@pytest.mark.parametrize("source_ready,warning", [(True, False), (False, False), (True, True)])
def test_update_yields_for_dependencies_and_reports_partial(
    isolated_config, monkeypatch, source_ready, warning,
):
    from quantmaster.after_close import jobs as after_close
    from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime
    from quantmaster.rotation import service

    isolated_config.data.after_close_enabled = True
    status = {"phase": "syncing", "update_result": "running"}
    requests = []
    monkeypatch.setattr(free_stockdb_runtime, "request_update", lambda: requests.append("source"))
    monkeypatch.setattr(free_stockdb_runtime, "status", lambda: status)
    children = {"cache": {"id": "cache", "status": "running"}}
    calls = []
    rotation = SimpleNamespace(
        start=lambda: None,
        submit=lambda spec: calls.append("analysis") or {"id": "analysis"},
    )
    monkeypatch.setattr(service, "get_rotation_worker", lambda: rotation)
    monkeypatch.setattr(after_close, "get_after_close_jobs", lambda: SimpleNamespace(
        submit=lambda **kw: ({"id": "scan"}, True),
    ))
    store = SimpleNamespace(get=lambda key: children[key])
    manager = SimpleNamespace(
        maintain_consumed=lambda **kw: calls.append(kw) or [{"id": "cache"}],
        _read_store=lambda: store,
        get=lambda key: {**children[key], "warning_count": int(warning)},
    )
    ctx = Context()
    spec = {"target_session": "2026-10-20"}
    assert run_update(ctx, spec, manager).waiting_on == "data_dependencies"
    assert run_update(ctx, spec, manager).waiting_on == "data_dependencies"
    assert calls == []  # A running source must not trigger analysis prematurely.
    assert requests == ["source"]
    status.update(phase="idle", update_result="success" if source_ready else "failed",
                  validated_session="2026-10-20" if source_ready else "2026-10-19")
    next_step = run_update(ctx, spec, manager)
    if not source_ready:
        assert next_step.status == "failed"
        assert ctx.checkpoint["steps"]["analysis"]["status"] == "blocked"
        assert calls == []
        return
    assert next_step.waiting_on == "data_dependencies"
    assert calls == [{"manual": True, "end": "2026-10-20"}]
    # Reloaded observers resume from the persisted checkpoint without resubmission.
    restored = Context()
    restored.checkpoint = deepcopy(ctx.checkpoint)
    assert run_update(restored, spec, manager).waiting_on == "data_dependencies"
    assert calls == [{"manual": True, "end": "2026-10-20"}]
    children["cache"]["status"] = "completed"
    assert run_update(restored, spec, manager).waiting_on == "data_dependencies"
    children.update(analysis={"status": "completed"}, scan={"status": "completed"})
    result = run_update(restored, spec, manager)
    assert result.status == ("completed" if source_ready and not warning else "failed")
    assert restored.checkpoint["outcome"] == ("completed" if result.status == "completed" else "partial")
    assert requests == ["source"]
    assert calls.count("analysis") == 1


def test_update_duplicate_admission_reuses_durable_job(isolated_config, monkeypatch):
    from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime

    monkeypatch.setattr(DataRefreshManager, "_owns_runtime", staticmethod(lambda: False))
    monkeypatch.setattr(free_stockdb_runtime, "update_target", lambda: "2026-10-20")
    first = DataRefreshManager()
    second = DataRefreshManager()
    job = first.create_update()
    assert job["target_session"] == "2026-10-20"
    assert job["id"] == second.create_update()["id"]


def test_automatic_intake_includes_ui_lists_and_only_open_positions(isolated_config, monkeypatch):
    from quantmaster.portfolio.ledger import Ledger
    from quantmaster.portfolio.watchlist import AssetListStore

    isolated_config.automation.watchlist = ["A"]
    monkeypatch.setattr(AssetListStore, "all", lambda self: {
        "favorites": [{"symbol": "B"}], "following": [{"symbol": "C"}],
    })
    monkeypatch.setattr(Ledger, "positions", lambda self: [
        SimpleNamespace(symbol="D", shares=100), SimpleNamespace(symbol="E", shares=0),
    ])
    from quantmaster.server.data_update import consumed_symbols

    manager = DataRefreshManager()
    manager.consumed_symbols = consumed_symbols
    calls = []
    monkeypatch.setattr(manager, "create", lambda *a, **kw: {})
    monkeypatch.setattr(manager, "_submit", lambda *args: calls.append(args) or {})
    manager.maintain_consumed(end="2026-10-20")
    assert calls[0][-1] == ["A", "B", "C", "D"]
    isolated_config.data.repair_enabled = False
    calls.clear()
    assert manager.maintain_consumed(end="2026-10-20") == []
    manager.maintain_consumed(manual=True, end="2026-10-20")
    assert calls[0][-1] == ["A", "B", "C", "D"]


def test_fresh_source_is_reused_without_remote_update(monkeypatch):
    from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime
    from quantmaster.server.data_update import _source_step

    monkeypatch.setattr(free_stockdb_runtime, "status", lambda: {"validated_session": "2026-10-20"})
    monkeypatch.setattr(
        free_stockdb_runtime, "request_update", lambda: pytest.fail("fresh data must be reused"),
    )
    assert _source_step({}, "2026-10-20")["status"] == "completed"


def test_unified_job_projection_preserves_progress_and_dependency_wait():
    from quantmaster.server.jobs import _public_job

    public = _public_job("data", {
        "id": "update", "type": "data.update", "status": "interrupted",
        "waiting_on": "data_dependencies", "steps": {"stockdb": {"status": "completed"}},
        "target_session": "2026-10-20", "can_retry": False,
    })
    assert public["waiting_on"] == "data_dependencies"
    assert public["steps"]["stockdb"]["status"] == "completed"
    assert public["can_retry"] is False


def test_update_admission_uses_updater_calendar_not_old_reader_fallback(isolated_config, monkeypatch):
    from quantmaster import trading_sessions
    from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime

    monkeypatch.setattr(DataRefreshManager, "_owns_runtime", staticmethod(lambda: False))
    monkeypatch.setattr(free_stockdb_runtime, "check_vendor_notice", lambda **kw: {})
    monkeypatch.setattr(trading_sessions, "resolve_session_target", lambda: SimpleNamespace(
        ready=True, session="2026-09-30", source="stockdb:validated",
        completion="previous_session_complete",
        coverage={"official_dates": ["2026-09-30", "2026-10-20"], "official_source": "tushare:SSE"},
    ))
    manager = DataRefreshManager()
    job = manager.create_update()
    assert manager._read_store().get(job["id"])["spec"]["target_session"] == "2026-10-20"
    monkeypatch.setattr(free_stockdb_runtime, "_target_session", lambda: ("", "unavailable"))
    with pytest.raises(ValueError, match="旧验收日期不代表最新"):
        manager.create_update()


def test_plan_pins_every_consumed_scope_to_its_target(isolated_config, monkeypatch):
    isolated_config.automation.watchlist = ["600000.SH"]
    isolated_config.automation.primary_universe = "selected"
    isolated_config.lab.enabled = False
    monkeypatch.setattr("quantmaster.data.universe.load_universe", lambda name: ["600001.SH"])
    monkeypatch.setattr("quantmaster.data.universe.load_universe_snapshot", lambda name: SimpleNamespace(
        content_hash="revision", symbols=["600001.SH"],
    ))
    monkeypatch.setattr(DataRefreshManager, "_owns_runtime", staticmethod(lambda: False))
    manager = DataRefreshManager()
    jobs = manager.maintain_consumed(manual=True, end="2026-10-20")
    assert len(jobs) == 3
    assert {manager._read_store().get(job["id"])["spec"]["end"] for job in jobs} == {"2026-10-20"}


def test_dependency_wait_has_durable_deadline_and_keeps_results(monkeypatch):
    from quantmaster.server import data_update

    ctx = Context()
    ctx.checkpoint = {
        "target_session": "2026-10-20", "plan_started_at": 100,
        "steps": {"stockdb": {"status": "completed"}, "consumers": {"status": "running"}},
        "children": [{"id": "shared", "kind": "行情缓存"}],
        "results": [{"id": "done", "status": "completed"}],
    }
    monkeypatch.setattr(data_update.time, "time", lambda: 3701)
    result = run_update(ctx, {"target_session": "2026-10-20"}, None)
    assert result.status == "failed"
    assert ctx.checkpoint["outcome"] == "partial"
    assert ctx.checkpoint["steps"]["consumers"]["status"] == "blocked"
    assert ctx.checkpoint["results"] == [{"id": "done", "status": "completed"}]
    assert ctx.checkpoint["children"][0]["id"] == "shared"


def test_source_coverage_warning_is_not_projected_as_full_success(monkeypatch):
    from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime
    from quantmaster.server.data_update import _source_step

    monkeypatch.setattr(free_stockdb_runtime, "status", lambda: {
        "validated_session": "2026-10-20", "validation": {"warnings": ["46 只证券存在缺口"]},
    })
    step = _source_step({}, "2026-10-20")
    assert step["status"] == "partial"
    assert "46" in step["detail"]
