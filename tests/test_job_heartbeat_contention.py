"""Lease renewal and maintenance sweeps under real competing SQLite writers."""

from __future__ import annotations

import logging
import threading
import time
from types import SimpleNamespace

import pytest

import quantmaster.runtime.jobs as jobs
from quantmaster.runtime.jobs import JobLeaseLost, JobOutcome, UnifiedJobRuntime, UnifiedJobStore
from quantmaster.runtime.sqlite import connect_sqlite


@pytest.mark.parametrize("expire", [False, True])
def test_heartbeat_survives_busy_timeout_and_fences_expired_owner(tmp_path, monkeypatch, caplog, expire):
    store = UnifiedJobStore(tmp_path / "jobs.sqlite")
    runtime = UnifiedJobRuntime(store, dispatch=False)
    job, _ = store.submit("test.lock", {})
    assert store.claim(job["id"], runtime.identity.value)
    active = store.get(job["id"])
    expires_at = time.time() + (0.3 if expire else 10)
    with store._conn() as connection:
        connection.execute(
            "UPDATE runtime_jobs SET lease_expires=? WHERE id=?", (expires_at, job["id"]),
        )
    monkeypatch.setattr(jobs, "HEARTBEAT_INTERVAL_SECONDS", 0.01)
    monkeypatch.setattr(store, "_conn", lambda: connect_sqlite(store.path, timeout=0.05, row_factory=True))
    stopped, alive = threading.Event(), threading.Event()
    alive.set()
    caplog.set_level(logging.INFO, logger=jobs.__name__)
    thread = threading.Thread(
        target=runtime._heartbeat,
        args=(job["id"], active["lease_token"], stopped, alive, expires_at),
    )
    try:
        with store._conn() as writer:
            writer.execute("BEGIN IMMEDIATE")
            thread.start()
            deadline = time.monotonic() + 2
            while "JOB_LEDGER_BUSY" not in caplog.text and time.monotonic() < deadline:
                stopped.wait(0.01)
            assert "JOB_LEDGER_BUSY" in caplog.text
            assert thread.is_alive() and alive.is_set()
            if expire:
                thread.join(timeout=2)
                assert not thread.is_alive() and not alive.is_set()
        if expire:
            assert store.recover_expired() == [job["id"]]
            assert store.claim(job["id"], "replacement")
            assert not store.heartbeat(job["id"], runtime.identity.value, active["lease_token"])
            with pytest.raises(JobLeaseLost):
                store.write_artifact(
                    job["id"], "late", {"late": True},
                    owner=runtime.identity.value, lease_token=active["lease_token"],
                )
            with pytest.raises(JobLeaseLost):
                store.finish(
                    job["id"], runtime.identity.value, JobOutcome(), lease_token=active["lease_token"],
                )
        else:
            deadline = time.monotonic() + 2
            while (
                store.get(job["id"])["heartbeat_at"] <= active["heartbeat_at"]
                and time.monotonic() < deadline
            ):
                stopped.wait(0.01)
            renewed = store.get(job["id"])
            assert renewed["heartbeat_at"] > active["heartbeat_at"]
            assert renewed["attempt"] == active["attempt"] and alive.is_set()
            assert renewed["lease_token"] == active["lease_token"]
            assert store.recover_expired() == []
    finally:
        stopped.set()
        thread.join(timeout=2)
        runtime.stop()


def test_heartbeat_cannot_revive_lease_that_expired_during_busy_wait(tmp_path, monkeypatch):
    store = UnifiedJobStore(tmp_path / "jobs.sqlite")
    job, _ = store.submit("test.expiry", {})
    assert store.claim(job["id"], "old")
    active = store.get(job["id"])
    expires_at = time.time() + 0.15
    with store._conn() as connection:
        connection.execute("UPDATE runtime_jobs SET lease_expires=? WHERE id=?", (expires_at, job["id"]))
    waiting = threading.Event()
    original = store._conn

    def connection():
        value = original()
        value.set_trace_callback(lambda sql: waiting.set() if sql == "BEGIN IMMEDIATE" else None)
        return value

    monkeypatch.setattr(store, "_conn", connection)
    result = []
    with original() as writer:
        writer.execute("BEGIN IMMEDIATE")
        thread = threading.Thread(
            target=lambda: result.append(store.heartbeat(job["id"], "old", active["lease_token"])),
        )
        thread.start()
        assert waiting.wait(1)
        # Keep the real write lock until the existing lease has expired.
        threading.Event().wait(max(0, expires_at - time.time()) + 0.05)
    thread.join(timeout=2)
    assert result == [False]
    assert store.get(job["id"])["lease_expires"] == expires_at


def test_empty_maintenance_sweeps_do_not_request_write_lock(tmp_path, monkeypatch):
    store = UnifiedJobStore(tmp_path / "jobs.sqlite")
    store.submit("data.refresh", {"scope": "market"})
    monkeypatch.setattr(store, "_conn", lambda: connect_sqlite(store.path, timeout=0.05, row_factory=True))
    with store._conn() as writer:
        writer.execute("BEGIN IMMEDIATE")
        assert store.recover_expired() == []
        assert store.interrupt_legacy_llm() == 0
        assert store.interrupt_stale_llm() == 0


def test_revision_check_releases_write_lock_and_revalidates_job(tmp_path, monkeypatch):
    import quantmaster.runtime.llm as llm

    store = UnifiedJobStore(tmp_path / "jobs.sqlite")
    job, _ = store.submit("test.llm", {}, llm_scope="global", llm_revision="old")
    monkeypatch.setattr(store, "_conn", lambda: connect_sqlite(store.path, timeout=0.05, row_factory=True))

    def current(scope, revision):
        # A second writer can claim while revision lookup is slow; the sweep
        # must neither block it nor interrupt its newly acquired lease.
        assert store.claim(job["id"], "worker")
        return False

    monkeypatch.setattr(llm, "get_llm_execution_coordinator", lambda: SimpleNamespace(current=current))
    assert store.interrupt_stale_llm() == 0
    assert store.get(job["id"])["status"] == "running"
