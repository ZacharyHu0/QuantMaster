"""Extensible unified runtime job kernel contracts."""

from __future__ import annotations

import threading
import time

import pytest

from quantmaster.runtime.jobs import (
    JobOutcome,
    UnifiedJobRuntime,
    UnifiedJobStore,
)


def _wait(store: UnifiedJobStore, job_id: str, statuses: set[str], timeout: float = 5) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = store.get(job_id)
        if job["status"] in statuses:
            return job
        time.sleep(0.01)
    raise AssertionError(f"job did not reach {statuses}: {store.get(job_id)}")


def test_unified_runtime_idempotency_events_artifacts_and_retry(tmp_path):
    store = UnifiedJobStore(tmp_path / "jobs.sqlite")
    runtime = UnifiedJobRuntime(store, max_workers=1)

    def handler(context, spec):
        context.progress(20, "取数", "读取证据")
        context.emit("dimension_started", {"dimension": "fundamental"})
        if context.attempt == 1:
            context.write_checkpoint(
                "fundamental",
                context.spec_hash,
                {"schema_version": "1.0", "dimension": {"score": 60}},
            )
            raise RuntimeError("transient reviewer failure")
        checkpoint = context.load_checkpoint("fundamental", context.spec_hash)
        assert checkpoint["dimension"]["score"] == 60
        artifact = context.write_artifact(
            "test.report",
            {"schema_version": "1.0", "query": spec["query"], "attempt": context.attempt},
            {"schema_version": "1.0", "lineage": {"spec_hash": context.spec_hash}},
        )
        context.progress(98, "终审", "完成")
        return JobOutcome("completed_with_errors", "recovered", artifact["id"])

    runtime.register("test.analysis", handler)
    first, created = runtime.submit(
        "test.analysis",
        {"query": "600519", "mode": "deep"},
        idempotency_key="request-1",
    )
    duplicate, duplicate_created = runtime.submit(
        "test.analysis",
        {"query": "600519", "mode": "deep"},
        idempotency_key="request-1",
    )
    assert created is True and duplicate_created is False
    assert duplicate["id"] == first["id"]
    with pytest.raises(ValueError, match="不同任务规格"):
        runtime.submit(
            "test.analysis",
            {"query": "000001", "mode": "deep"},
            idempotency_key="request-1",
        )

    failed = _wait(store, first["id"], {"failed"})
    assert failed["attempt"] == 1
    runtime.retry(first["id"])
    completed = _wait(store, first["id"], {"completed_with_errors"})
    assert completed["attempt"] == 2
    artifact = store.artifact(completed["result_artifact_id"])
    assert artifact["payload"]["attempt"] == 2
    events = store.events(first["id"])
    assert [event["seq"] for event in events] == list(range(1, len(events) + 1))
    assert {event["attempt"] for event in events} == {1, 2}
    assert runtime.public(completed)["estimated_remaining_seconds"] == 0
    runtime.stop()


def test_unified_runtime_cancel_and_expired_lease_recovery(tmp_path):
    store = UnifiedJobStore(tmp_path / "jobs.sqlite")
    runtime = UnifiedJobRuntime(store, max_workers=1)

    def cancellable(context, spec):
        while not context.cancelled():
            time.sleep(0.01)
        context.ensure_active()
        raise AssertionError("unreachable")

    runtime.register("test.cancel", cancellable)
    job, _ = runtime.submit("test.cancel", {"value": 1})
    _wait(store, job["id"], {"running"})
    store.cancel(job["id"])
    cancelled = _wait(store, job["id"], {"cancelled"})
    assert cancelled["cancel_requested"] is True
    runtime.stop()

    recovery_store = UnifiedJobStore(tmp_path / "recovery.sqlite")
    recovery_job, _ = recovery_store.submit("test.recover", {"value": 2})
    assert recovery_store.claim(recovery_job["id"], "dead-worker")
    with recovery_store._conn() as connection:
        connection.execute(
            "UPDATE runtime_jobs SET lease_expires=0 WHERE id=?",
            (recovery_job["id"],),
        )
    recovered_runtime = UnifiedJobRuntime(recovery_store, max_workers=1)
    recovered_runtime.register(
        "test.recover",
        lambda context, spec: JobOutcome("completed", str(spec["value"])),
    )
    recovered_runtime.start()
    recovered = _wait(recovery_store, recovery_job["id"], {"completed"})
    assert recovered["attempt"] == 1
    assert any(event["type"] == "job_interrupted" for event in recovery_store.events(recovery_job["id"]))
    recovered_runtime.stop()


def test_corrupt_checkpoint_is_rejected_and_queued_for_repair(tmp_path):
    store = UnifiedJobStore(tmp_path / "jobs.sqlite")
    job, _ = store.submit("test.integrity", {"query": "600519"})
    artifact = store.write_artifact(
        job["id"],
        "checkpoint.news",
        {"schema_version": "1.0", "dimension": {"score": 50}},
        {"schema_version": "1.0", "lineage": {"spec_hash": job["spec_hash"]}},
        checkpoint_key="news",
    )
    with store._conn() as connection:
        connection.execute(
            "UPDATE runtime_job_artifacts SET payload_json='{}' WHERE id=?",
            (artifact["id"],),
        )

    assert store.checkpoint(job["id"], "news", job["spec_hash"]) is None
    repairs = store.repairs()
    assert repairs[0]["artifact_id"] == artifact["id"]
    assert repairs[0]["status"] == "queued"

    lineage_artifact = store.write_artifact(
        job["id"],
        "test.lineage",
        {"schema_version": "1.0", "value": 1},
        {"schema_version": "1.0", "lineage": {"spec_hash": job["spec_hash"]}},
    )
    with store._conn() as connection:
        connection.execute(
            "UPDATE runtime_job_artifacts SET lineage_json='[]' WHERE id=?",
            (lineage_artifact["id"],),
        )

    assert store.latest_artifact(job["id"], "test.lineage") is None
    assert any(item["artifact_id"] == lineage_artifact["id"] for item in store.repairs())


def test_runtime_pause_drains_and_resume_recovers_interrupted_job(tmp_path):
    store = UnifiedJobStore(tmp_path / "jobs.sqlite")
    runtime = UnifiedJobRuntime(store, max_workers=1)
    attempts = 0
    first_started = threading.Event()

    def handler(context, _spec):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            first_started.set()
            while not context.cancelled():
                time.sleep(0.01)
            raise InterruptedError("maintenance")
        return JobOutcome("completed", "resumed")

    runtime.register("test.pause", handler)
    job, _ = runtime.submit("test.pause", {"value": 1})
    assert first_started.wait(2)

    runtime.pause()
    deadline = time.monotonic() + 2
    while not runtime.idle and time.monotonic() < deadline:
        time.sleep(0.01)
    assert runtime.idle
    assert store.get(job["id"])["status"] == "interrupted"
    with pytest.raises(RuntimeError, match="维护"):
        runtime.submit("test.pause", {"value": 2})

    runtime.resume()
    assert _wait(store, job["id"], {"completed"})["detail"] == "resumed"
    assert attempts == 2
    runtime.stop()


def test_retry_queued_before_previous_worker_cleanup_is_rescheduled(tmp_path):
    store = UnifiedJobStore(tmp_path / "jobs.sqlite")
    runtime = UnifiedJobRuntime(store, max_workers=1)
    failed_persisted = threading.Event()
    allow_cleanup = threading.Event()
    original_finish = store.finish

    def finish_then_pause(job_id, owner, outcome):
        finished = original_finish(job_id, owner, outcome)
        if outcome.status == "failed":
            failed_persisted.set()
            assert allow_cleanup.wait(2)
        return finished

    store.finish = finish_then_pause

    def fail_once(context, spec):
        if context.attempt == 1:
            raise RuntimeError("review failed")
        return JobOutcome("completed", str(spec["value"]))

    runtime.register("test.retry-race", fail_once)
    job, _ = runtime.submit("test.retry-race", {"value": 7})
    assert failed_persisted.wait(2)
    runtime.retry(job["id"])
    allow_cleanup.set()

    completed = _wait(store, job["id"], {"completed"})
    assert completed["attempt"] == 2
    runtime.stop()


def test_unified_runtime_converts_unexpected_value_error_to_terminal_failure(tmp_path):
    store = UnifiedJobStore(tmp_path / "jobs.sqlite")
    runtime = UnifiedJobRuntime(store, max_workers=1)

    def invalid(_context, _spec):
        raise ValueError("invalid immutable specification")

    runtime.register("test.invalid", invalid)
    job, _ = runtime.submit("test.invalid", {"value": 1}, max_attempts=1)
    failed = _wait(store, job["id"], {"failed"})

    assert "invalid immutable specification" in failed["detail"]
    assert failed["owner"] == ""
    runtime.stop()
