"""Extensible unified runtime job kernel contracts."""

from __future__ import annotations

import os
import threading
import time

import pytest

from quantmaster.runtime.jobs import (
    JobLeaseLost,
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


def test_business_key_reuses_active_completed_and_resumes_failed_job(tmp_path):
    store = UnifiedJobStore(tmp_path / "jobs.sqlite")
    first, created = store.submit(
        "automation.daily_close_pipeline", {"name": "daily_close_pipeline", "as_of": "2026-08-12"},
        business_key="daily_close_pipeline:date:2026-08-12", trigger_actor="scheduler",
    )
    active, active_created = store.submit(
        first["type"], first["spec"], business_key=first["business_key"], trigger_actor="web",
    )
    assert created is True
    assert active_created is False
    assert active["id"] == first["id"]
    assert active["coalesced_count"] == 1

    assert store.claim(first["id"], "worker") is True
    running = store.get(first["id"])
    store.finish(
        first["id"], "worker", JobOutcome("completed", "done"),
        lease_token=running["lease_token"],
    )
    completed, completed_created = store.submit(
        first["type"], first["spec"], business_key=first["business_key"],
        trigger_actor="startup_recovery",
    )
    assert completed_created is False
    assert completed["id"] == first["id"]
    assert completed["status"] == "completed"
    assert completed["reused"] is True

    second, _ = store.submit(
        "automation.fast_news_scan", {"name": "fast_news_scan", "as_of": ""},
        business_key="fast_news_scan:window:10:00:10:20",
    )
    assert store.claim(second["id"], "worker") is True
    failed = store.get(second["id"])
    store.finish(
        second["id"], "worker", JobOutcome("failed", "network timeout"),
        lease_token=failed["lease_token"],
    )
    resumed, resumed_created = store.submit(
        second["type"], second["spec"], business_key=second["business_key"],
        trigger_actor="scheduler",
    )
    assert resumed_created is False
    assert resumed["id"] == second["id"]
    assert resumed["status"] == "queued"


def test_business_key_rejects_different_parameters(tmp_path):
    store = UnifiedJobStore(tmp_path / "jobs.sqlite")
    store.submit("demo", {"range": "a"}, business_key="symbol:000001.SZ:1d:a")
    with pytest.raises(ValueError, match="业务幂等键"):
        store.submit("demo", {"range": "b"}, business_key="symbol:000001.SZ:1d:a")


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
    assert recovered["attempt"] == 2
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

    def finish_then_pause(job_id, owner, outcome, *, lease_token):
        finished = original_finish(job_id, owner, outcome, lease_token=lease_token)
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


def test_runtime_stop_drains_current_atomic_unit_and_rejects_new_work(tmp_path):
    store = UnifiedJobStore(tmp_path / "jobs.sqlite")
    runtime = UnifiedJobRuntime(store, max_workers=1)
    started = threading.Event()
    release_atomic_unit = threading.Event()

    def handler(context, _spec):
        started.set()
        assert release_atomic_unit.wait(2)
        context.write_checkpoint(
            "article-1",
            context.spec_hash,
            {"schema_version": "1.0", "article_id": 1, "committed": True},
        )
        return JobOutcome("completed", "article committed")

    runtime.register("test.atomic-drain", handler)
    job, _ = runtime.submit("test.atomic-drain", {"article_id": 1})
    assert started.wait(2)

    result: dict = {}

    def stop_runtime():
        result.update(runtime.stop(deadline_seconds=2))

    stopper = threading.Thread(target=stop_runtime)
    stopper.start()
    deadline = time.monotonic() + 1
    while runtime.snapshot()["status"] != "draining" and time.monotonic() < deadline:
        time.sleep(0.01)
    assert runtime.snapshot()["accepting"] is False
    with pytest.raises(RuntimeError, match=r"维护|停止"):
        runtime.submit("test.atomic-drain", {"article_id": 2})

    release_atomic_unit.set()
    stopper.join(timeout=3)
    assert not stopper.is_alive()
    assert result["status"] == "stopped"
    assert result["timeout_issues"] == []
    completed = store.get(job["id"])
    assert completed["status"] == "completed"
    assert store.checkpoint(job["id"], "article-1", job["spec_hash"])["committed"] is True


def test_runtime_stop_deadline_fences_late_provider_result_and_preserves_queue(tmp_path):
    store = UnifiedJobStore(tmp_path / "jobs.sqlite")
    runtime = UnifiedJobRuntime(store, max_workers=1)
    provider_started = threading.Event()
    provider_returned = threading.Event()

    def handler(context, _spec):
        provider_started.set()
        assert provider_returned.wait(2)
        context.write_artifact(
            "late.provider.result",
            {"schema_version": "1.0", "value": "late"},
            {"schema_version": "1.0", "lineage": {}},
        )
        return JobOutcome("completed", "late")

    runtime.register("test.provider-deadline", handler)
    job, _ = runtime.submit("test.provider-deadline", {"value": 1})
    assert provider_started.wait(2)

    started_at = time.monotonic()
    stopped = runtime.stop(deadline_seconds=0.05)
    elapsed = time.monotonic() - started_at
    assert elapsed < 0.5
    assert stopped["status"] == "stopped"
    assert stopped["timeout_issues"][0]["phase"] == "draining_provider_or_atomic_unit"
    assert stopped["timeout_issues"][0]["task_count"] == 1
    assert store.get(job["id"])["status"] == "interrupted"

    provider_returned.set()
    deadline = time.monotonic() + 2
    while not runtime.idle and time.monotonic() < deadline:
        time.sleep(0.01)
    assert runtime.idle
    assert store.get(job["id"])["status"] == "interrupted"
    assert store.latest_artifact(job["id"], "late.provider.result") is None


def test_runtime_shutdown_cancels_owned_retry_timer_and_submit_race(tmp_path, monkeypatch):
    store = UnifiedJobStore(tmp_path / "jobs.sqlite")
    runtime = UnifiedJobRuntime(store, max_workers=1)
    runtime.register("test.retry-owner", lambda _context, _spec: JobOutcome())
    job, _ = store.submit("test.retry-owner", {"value": 1})
    generation = runtime.generation
    runtime._start_retry_timer(job["id"], generation, 60)
    assert runtime.snapshot()["retry_timers"] == 1

    runtime.stop(deadline_seconds=0)
    assert runtime.snapshot()["retry_timers"] == 0

    raced = UnifiedJobRuntime(UnifiedJobStore(tmp_path / "race.sqlite"), max_workers=1)
    raced.register("test.submit-race", lambda _context, _spec: JobOutcome())
    race_job, _ = raced.store.submit("test.submit-race", {"value": 1})

    def rejected_submit(*_args, **_kwargs):
        raise RuntimeError("cannot schedule new futures after shutdown")

    monkeypatch.setattr(raced._executor, "submit", rejected_submit)
    raced._schedule(race_job["id"])
    assert raced.idle
    assert raced.snapshot()["active_tasks"] == 0
    raced.stop(deadline_seconds=0)


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


def test_unified_store_singleflight_lease_token_and_external_artifact(tmp_path):
    store = UnifiedJobStore(tmp_path / "jobs.sqlite")
    first, created = store.submit(
        "rotation.refresh",
        {"scope": "themes", "mode": "incremental"},
        input_fingerprint="input-v1",
        algorithm_version="algo-v2",
    )
    duplicate, duplicate_created = store.submit(
        "rotation.refresh",
        {"scope": "themes", "mode": "incremental"},
        input_fingerprint="input-v1",
        algorithm_version="algo-v2",
    )
    changed, changed_created = store.submit(
        "rotation.refresh",
        {"scope": "themes", "mode": "incremental"},
        input_fingerprint="input-v2",
        algorithm_version="algo-v2",
    )
    assert created and not duplicate_created and changed_created
    assert duplicate["id"] == first["id"]
    assert duplicate["coalesced"] is True
    assert changed["id"] != first["id"]

    assert store.claim(first["id"], "worker-old", lease_seconds=5)
    old = store.get(first["id"])
    old_token = old["lease_token"]
    with store._conn() as connection:
        connection.execute(
            "UPDATE runtime_jobs SET lease_expires=0 WHERE id=?", (first["id"],),
        )
    store.recover_expired()
    assert store.claim(first["id"], "worker-new", lease_seconds=5)
    new = store.get(first["id"])
    assert new["lease_token"] != old_token
    with pytest.raises(JobLeaseLost):
        store.progress(first["id"], "worker-old", old_token, 50, "old", "late")
    with pytest.raises(JobLeaseLost):
        store.write_artifact(
            first["id"], "late", {"value": "old"},
            owner="worker-old", lease_token=old_token,
        )
    with pytest.raises(JobLeaseLost):
        store.finish(
            first["id"], "worker-old", JobOutcome("completed"), lease_token=old_token,
        )

    payload = {"schema_version": "1.0", "blob": "x" * (129 * 1024)}
    artifact = store.write_artifact(
        first["id"], "large.result", payload,
        owner="worker-new", lease_token=new["lease_token"],
    )
    assert artifact["external"] is True
    restored = store.artifact(artifact["id"])
    assert restored["payload"] == payload
    assert restored["payload_json"] == ""


def test_unified_store_reuses_completed_artifact_for_identical_versioned_input(tmp_path):
    store = UnifiedJobStore(tmp_path / "jobs.sqlite")
    first, created = store.submit(
        "rotation.refresh",
        {"scope": "etf", "mode": "incremental"},
        input_fingerprint="etf-generation-v7",
        algorithm_version="QM_ETF_V3",
    )
    assert created
    assert store.claim(first["id"], "worker", lease_seconds=30)
    active = store.get(first["id"])
    artifact = store.write_artifact(
        first["id"], "rotation.etf.snapshot", {"snapshot_id": "etf_immutable"},
        owner="worker", lease_token=active["lease_token"],
    )
    store.finish(
        first["id"], "worker", JobOutcome("completed", "published", artifact["id"]),
        lease_token=active["lease_token"],
    )

    reused, reused_created = store.submit(
        "rotation.refresh",
        {"scope": "etf", "mode": "incremental"},
        input_fingerprint="etf-generation-v7",
        algorithm_version="QM_ETF_V3",
    )

    assert reused_created is False
    assert reused["id"] == first["id"]
    assert reused["status"] == "completed"
    assert reused["coalesced"] is True
    assert reused["reused"] is True
    assert reused["outcome"] == "unchanged"


def test_unified_runtime_runs_registered_process_handler_outside_supervisor(tmp_path):
    store = UnifiedJobStore(tmp_path / "jobs.sqlite")
    runtime = UnifiedJobRuntime(store, max_workers=1)
    runtime.register(
        "test.process",
        lambda _context, _spec: (_ for _ in ()).throw(AssertionError("must spawn")),
        process_entrypoint="tests.process_handler:write_artifact",
    )

    job, created = runtime.submit("test.process", {"value": "fixture"})
    assert created is True
    completed = _wait(store, job["id"], {"completed", "failed", "cancelled"}, timeout=15)
    assert completed["status"] == "completed"
    artifact = store.artifact(completed["result_artifact_id"])
    assert artifact["payload"]["value"] == "fixture"
    assert artifact["payload"]["pid"] != os.getpid()
    runtime.stop()
