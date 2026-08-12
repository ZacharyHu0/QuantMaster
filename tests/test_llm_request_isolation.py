"""Regression coverage for durable LLM request isolation (no real provider)."""

from __future__ import annotations

import threading
import time

import pytest

from quantmaster.ai.llm import LLMClient
from quantmaster.config import Config, LLMConfig, set_config
from quantmaster.credentials import CredentialError, CredentialStore
from quantmaster.lab.store import LabStore
from quantmaster.runtime.jobs import JobOutcome, UnifiedJobRuntime, UnifiedJobStore
from quantmaster.runtime.llm import (
    DirectLLMRequestError,
    LLMExecutionCoordinator,
    enter_http_request,
    get_llm_execution_coordinator,
    leave_http_request,
    reject_http_llm_transport,
)
from quantmaster.settings import ConfigManager


def _config(tmp_path) -> Config:
    cfg = Config()
    cfg.data.root = str(tmp_path / "data")
    return cfg


def _wait(store: UnifiedJobStore, job_id: str, statuses: set[str], timeout: float = 4) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        current = store.get(job_id)
        if current["status"] in statuses:
            return current
        time.sleep(0.01)
    raise AssertionError(store.get(job_id))


def test_revision_rotation_cancels_only_matching_scope(tmp_path):
    try:
        set_config(_config(tmp_path))
        coordinator = LLMExecutionCoordinator(tmp_path / "revisions.sqlite")
        store = UnifiedJobStore(tmp_path / "jobs.sqlite")
        coordinator.register_store(store)
        global_job, _ = store.submit(
            "settings.diagnostic", {"kind": "llm-models"},
            llm_scope="global", llm_revision=coordinator.revision("global"),
        )
        news_job, _ = store.submit(
            "news.reanalyze", {"mode": "pending"},
            llm_scope="news", llm_revision=coordinator.revision("news"),
        )

        updated = coordinator.rotate(global_scope=False, news_scope=True, reason="annotation")

        assert updated["queued_cancelled"] == 1
        assert store.get(news_job["id"])["status"] == "cancelled"
        assert store.get(global_job["id"])["status"] == "queued"
    finally:
        set_config(None)


def test_rotation_discards_late_llm_result_and_does_not_publish_artifact(tmp_path):
    set_config(_config(tmp_path))
    store = UnifiedJobStore(tmp_path / "jobs.sqlite")
    runtime = UnifiedJobRuntime(store, max_workers=1)
    started = threading.Event()
    release = threading.Event()

    def delayed_provider(context, _spec):
        started.set()
        release.wait(2)
        # This deliberately does not call ensure_active; ``finish`` remains
        # the last atomic ledger fence for an upstream response that arrives
        # during a configuration rotation.
        return JobOutcome("completed", "late provider response")

    runtime.register("test.llm", delayed_provider)
    job, _ = runtime.submit("test.llm", {"value": 1}, llm_scope="global")
    assert started.wait(2)
    coordinator = LLMExecutionCoordinator()
    coordinator.register_store(store)
    coordinator.rotate(global_scope=True, reason="settings_saved")
    assert _wait(store, job["id"], {"cancelling", "cancelled"})["cancel_requested"] is True
    release.set()
    final = _wait(store, job["id"], {"cancelled"})
    assert final["result_artifact_id"] == ""
    runtime.stop()
    set_config(None)


def test_lab_revision_fences_late_result_and_stale_startup_work(tmp_path):
    try:
        set_config(_config(tmp_path))
        store = LabStore(tmp_path / "data" / "lab.sqlite")
        coordinator = get_llm_execution_coordinator()

        running = store.enqueue("discover_llm", {"universe": "fixture"})
        assert store.claim_next("fixture-worker")["id"] == running["id"]
        coordinator.rotate(global_scope=True, news_scope=False, reason="settings_saved")
        assert store.job(running["id"])["status"] == "cancelling"
        assert store.finish_job(
            running["id"], result={"candidate": "late-result"},
            expected_worker="fixture-worker",
        )
        finished = store.job(running["id"])
        assert finished["status"] == "cancelled"
        assert finished["result"] == {}

        stale = store.enqueue("discover_python", {"universe": "fixture"})
        with store._conn() as connection:
            connection.execute(
                "UPDATE lab_jobs SET llm_revision='expired-revision' WHERE id=?",
                (stale["id"],),
            )
        assert store.interrupt_stale_llm() == 1
        expired = store.job(stale["id"])
        assert expired["status"] == "interrupted"
        assert expired["phase"] == "需要手动重试"
    finally:
        set_config(None)


def test_http_request_guard_rejects_direct_llm_transport():
    token = enter_http_request()
    try:
        with pytest.raises(DirectLLMRequestError):
            reject_http_llm_transport()
        with pytest.raises(DirectLLMRequestError):
            LLMClient(LLMConfig(
                provider="openai-compatible", model="fixture", api_key="",
                base_url="https://example.invalid",
            ))._post("https://example.invalid")
    finally:
        leave_http_request(token)


def test_public_settings_avoids_keyring_and_winerror_is_normalized(tmp_path, monkeypatch):
    class ForbiddenCredentials:
        def get(self, _target):
            raise AssertionError("GET /settings must not read Credential Manager")

        def set(self, _target, _value):
            raise AssertionError("GET /settings must not write Credential Manager")

        def delete(self, _target):
            raise AssertionError("GET /settings must not delete Credential Manager")

    manager = ConfigManager(tmp_path / "config.yaml", tmp_path / "backups", ForbiddenCredentials())
    assert manager.public()["checks"] == {}

    class WinErrorBackend:
        priority = 1

    class WinErrorKeyring:
        @staticmethod
        def get_keyring():
            return WinErrorBackend()

        @staticmethod
        def get_password(_service, _target):
            raise OSError(1312, "A specified logon session does not exist")

    import sys
    import types

    errors = types.SimpleNamespace(PasswordDeleteError=type("DeleteError", (Exception,), {}))
    monkeypatch.setitem(sys.modules, "keyring", WinErrorKeyring())
    monkeypatch.setitem(sys.modules, "keyring.errors", errors)
    with pytest.raises(CredentialError):
        CredentialStore().get("test")


def test_settings_diagnostic_uses_process_local_one_shot_credentials(tmp_path, monkeypatch):
    """A WinError 1312 keyring must not prevent an explicit model check."""
    from quantmaster.server import settings_jobs
    from quantmaster.settings import SettingsDocument

    set_config(_config(tmp_path))
    jobs = settings_jobs.SettingsJobs()
    captured = {}

    def submit(_job_type, spec, **_kwargs):
        captured.update(spec)
        return ({"id": "diagnostic", "type": "settings.diagnostic"}, True)

    monkeypatch.setattr(jobs.diagnostic_runtime, "submit", submit)
    monkeypatch.setattr(
        CredentialStore,
        "set",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("diagnostic must not write Credential Manager")
        ),
    )
    document = SettingsDocument()

    task, created = jobs.submit_diagnostic("llm-models", document, api_key="draft-secret")

    assert created is True
    assert task["id"] == "diagnostic"
    assert set(captured) == {"kind", "credential_reference"}
    secret = settings_jobs._DIAGNOSTIC_CREDENTIALS.pop(captured["credential_reference"])
    assert secret is not None
    restored, api_key = secret
    assert restored == document
    assert api_key == "draft-secret"
    jobs.stop()
    set_config(None)


def test_web_settings_diagnostic_transfers_secret_to_worker_without_ledger_persistence(
    tmp_path, monkeypatch,
):
    from quantmaster.server import settings_jobs
    from quantmaster.settings import SettingsDocument

    set_config(_config(tmp_path))
    jobs = settings_jobs.SettingsJobs()
    jobs.diagnostic_runtime._dispatch_enabled = False
    document = SettingsDocument()
    captured = {}

    def worker_command(operation, payload, **kwargs):
        captured.update({"operation": operation, "payload": payload, "kwargs": kwargs})
        return {
            "task": {
                "id": "worker-diagnostic",
                "type": "settings.diagnostic",
                "spec": {"kind": payload["kind"], "credential_reference": "opaque"},
            },
            "created": True,
        }

    monkeypatch.setattr(
        "quantmaster.runtime.worker_ipc.call_worker_command", worker_command,
    )

    task, created = jobs.submit_diagnostic(
        "llm-web-search", document, api_key="draft-secret",
    )

    assert created is True
    assert task["id"] == "worker-diagnostic"
    assert captured["operation"] == "settings.diagnostic.create"
    assert captured["payload"]["api_key"] == "draft-secret"
    assert captured["kwargs"]["timeout"] == 2.0
    assert "draft-secret" not in repr(task)
    jobs.stop()
    set_config(None)
