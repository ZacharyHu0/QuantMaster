from __future__ import annotations

import threading

import pytest

from quantmaster.runtime.lifecycle_state import RuntimeLifecycle


def test_owner_registry_fences_generation_and_converges_only_registered_task():
    lifecycle = RuntimeLifecycle("runtime-worker", "generation-7")
    stopped = threading.Event()
    started = threading.Event()

    def run() -> None:
        started.set()
        stopped.wait()

    thread = lifecycle.start_thread(
        name="owned-heartbeat",
        target=run,
        phase="heartbeat",
        diagnostic_id="QM-LC-TEST-001",
        shutdown_policy="signal_then_join",
        deadline_seconds=1.0,
        stop=stopped.set,
    )
    assert started.wait(1)
    snapshot = lifecycle.snapshot()
    assert snapshot["state"] == "running"
    assert snapshot["generation"] == "generation-7"
    assert snapshot["tasks"][0]["component_owner"] == "runtime-worker"

    lifecycle.begin_shutdown(reloading=True)
    with pytest.raises(RuntimeError, match="draining"):
        lifecycle.start_thread(
            name="late-work",
            target=lambda: None,
            phase="producer",
            diagnostic_id="QM-LC-TEST-002",
            shutdown_policy="cancel",
            deadline_seconds=0.1,
        )
    lifecycle.converge_owned()
    assert not thread.is_alive()
    assert lifecycle.snapshot()["task_counts"]["active"] == 0


def test_owner_registry_reports_deadline_with_diagnostic_id():
    lifecycle = RuntimeLifecycle("runtime-worker", "generation-8")
    release = threading.Event()
    started = threading.Event()

    def stuck() -> None:
        started.set()
        release.wait()

    thread = lifecycle.start_thread(
        name="stuck-atomic-unit",
        target=stuck,
        phase="drain_atomic",
        diagnostic_id="QM-LC-TEST-TIMEOUT",
        shutdown_policy="finish_atomic_unit",
        deadline_seconds=0.01,
    )
    assert started.wait(1)
    lifecycle.begin_shutdown()
    lifecycle.converge_owned()
    issue = lifecycle.snapshot()["timeout_issues"][0]
    assert issue["diagnostic_id"] == "QM-LC-TEST-TIMEOUT"
    assert issue["phase"] == "drain_atomic"
    release.set()
    thread.join(1)


def test_lifecycle_public_counts_and_deadline_are_non_secret():
    lifecycle = RuntimeLifecycle("web", "12")
    lifecycle.set_durable_counts(pending=4, handoff=2)
    lifecycle.begin_shutdown()
    lifecycle.enter_phase("handoff", 3.0)

    snapshot = lifecycle.snapshot()

    assert snapshot["task_counts"] == {"active": 0, "converging": 0, "handoff": 2}
    assert snapshot["durable_queue"] == {"pending": 4}
    assert 0 <= snapshot["deadline"]["remaining_seconds"] <= 3.0
