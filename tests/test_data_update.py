"""Durable update orchestration; providers are replaced, never called."""
from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

import pytest

from quantmaster.data.maintenance import DataRefreshManager
from quantmaster.server.data_update import run_update


class Context:
    spec_hash = "plan"

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
    assert calls == [{"manual": True}]
    # Reloaded observers resume from the persisted checkpoint without resubmission.
    restored = Context()
    restored.checkpoint = deepcopy(ctx.checkpoint)
    assert run_update(restored, spec, manager).waiting_on == "data_dependencies"
    assert calls == [{"manual": True}]
    children["cache"]["status"] = "completed"
    assert run_update(restored, spec, manager).waiting_on == "data_dependencies"
    children.update(analysis={"status": "completed"}, scan={"status": "completed"})
    result = run_update(restored, spec, manager)
    assert result.status == ("completed" if source_ready and not warning else "failed")
    assert restored.checkpoint["outcome"] == ("completed" if result.status == "completed" else "partial")
    assert requests == ["source"]
    assert calls.count("analysis") == 1


def test_update_duplicate_admission_reuses_durable_job(isolated_config, monkeypatch):
    import quantmaster.data.maintenance as maintenance

    monkeypatch.setattr(DataRefreshManager, "_owns_runtime", staticmethod(lambda: False))
    monkeypatch.setattr(maintenance, "resolve_session_target", lambda *args: SimpleNamespace(
        ready=True, session="2026-10-20",
    ))
    first = DataRefreshManager()
    second = DataRefreshManager()
    assert first.create_update()["id"] == second.create_update()["id"]


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
    manager.maintain_consumed()
    assert calls[0][-1] == ["A", "B", "C", "D"]
    isolated_config.data.repair_enabled = False
    calls.clear()
    assert manager.maintain_consumed() == []
    manager.maintain_consumed(manual=True)
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
