from __future__ import annotations

import pytest
import yaml

from quantmaster.config import get_config
from quantmaster.server import settings_control
from quantmaster.server.settings_apply import initialize_worker_settings
from quantmaster.server.settings_jobs import SettingsJobs
from quantmaster.server.settings_runtime import begin_apply, public_state
from quantmaster.settings import ConfigManager


@pytest.fixture
def settings_worker(tmp_path, monkeypatch):
    monkeypatch.delenv("QM_WEB_PROCESS", raising=False)
    monkeypatch.setattr(settings_control, "_settings_manager", None)
    monkeypatch.setattr(settings_control, "_apply_runtime", None)
    path = tmp_path / "settings.yaml"
    path.write_text(yaml.safe_dump({
        "_revision": 2,
        "data": {"root": str(get_config().data_root), "repair_enabled": False},
    }), encoding="utf-8")
    manager = ConfigManager(path)
    initialize_worker_settings(manager)
    jobs = SettingsJobs()
    saved = {
        "config_revision": 2, "generation": begin_apply(path, 2),
        "changed_fields": ["data.repair_enabled", "data.free_stockdb_auto_update"],
    }
    yield manager, jobs, saved
    jobs.stop()


def test_owner_unconfirmed_fails_and_retry_confirms(settings_worker, monkeypatch):
    from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime

    manager, jobs, saved = settings_worker
    monkeypatch.setattr(free_stockdb_runtime, "request_apply_config", lambda changed, **kwargs: {
        "status": "degraded",
    })
    task, _ = jobs.submit_apply(saved)
    result = jobs.apply_runtime.wait(task["id"], timeout=10)
    assert result["status"] == "failed"
    stock = public_state(manager.path)["components"]["free-stockdb"]
    assert stock["effective_revision"] == 0
    assert not stock["confirmed"]
    assert not get_config().data.repair_enabled

    monkeypatch.setattr(free_stockdb_runtime, "request_apply_config", lambda changed, **kwargs: {
        "status": "applied",
    })
    jobs.apply_runtime.retry(task["id"])
    result = jobs.apply_runtime.wait(task["id"], timeout=10)
    assert result["status"] == "completed"
    assert public_state(manager.path)["components"]["free-stockdb"]["effective_revision"] == 2
    # Reapplying an acknowledged revision is safe and remains confirmed.
    settings_control.apply_runtime(dict(saved))
    assert public_state(manager.path)["components"]["runtime-worker"]["effective_revision"] == 2


def test_stale_job_does_not_apply_components(settings_worker, monkeypatch):
    manager, jobs, saved = settings_worker
    monkeypatch.setattr(settings_control, "_apply_runtime", lambda value: pytest.fail("stale apply"))
    task, _ = jobs.submit_apply({**saved, "config_revision": 1})
    assert jobs.apply_runtime.wait(task["id"], timeout=10)["status"] == "completed"
    assert public_state(manager.path)["components"]["runtime-worker"]["effective_revision"] == 0


def test_pending_owner_is_retried_automatically(settings_worker, monkeypatch):
    import time

    from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime

    manager, jobs, saved = settings_worker
    observations = []

    def acknowledge(changed, **kwargs):
        observations.append(public_state(manager.path)["components"]["free-stockdb"])
        return {"status": "queued" if len(observations) == 1 else "applied"}

    monkeypatch.setattr(free_stockdb_runtime, "request_apply_config", acknowledge)
    task, _ = jobs.submit_apply(saved)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        result = jobs.get(task["id"])
        if result["status"] == "completed":
            break
        time.sleep(.02)
    assert result["status"] == "completed"
    assert result["attempt"] == 2
    assert observations[1]["status"] == "pending"
    assert observations[1]["effective_revision"] == 0
    assert public_state(manager.path)["components"]["free-stockdb"]["effective_revision"] == 2


def test_load_failure_does_not_confirm_worker(settings_worker, monkeypatch):
    manager, jobs, saved = settings_worker

    def fail_load():
        raise RuntimeError("isolated load failure")

    monkeypatch.setattr(manager, "load", fail_load)
    task, _ = jobs.submit_apply(saved)
    assert jobs.apply_runtime.wait(task["id"], timeout=10)["status"] == "failed"
    assert public_state(manager.path)["components"]["runtime-worker"]["effective_revision"] == 0
