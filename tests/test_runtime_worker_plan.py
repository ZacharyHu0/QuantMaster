from __future__ import annotations

from typing import Any

import pytest


class _CommandServer:
    def __init__(self, handler):
        self.handler = handler
        self.running = False
        self.endpoint = "memory://worker-plan"

    def start(self) -> None:
        self.running = True

    def stop(self) -> None:
        self.running = False


class _Plan:
    def __init__(self, *, fail_start: bool = False, fail_stop: bool = False) -> None:
        self.events: list[Any] = []
        self.fail_start = fail_start
        self.fail_stop = fail_stop

    def settings_projection(self) -> tuple[int, int]:
        return 4, 7

    def start(self, *, bootstrap_rotation: bool) -> None:
        self.events.append(("start", bootstrap_rotation))
        if self.fail_start:
            raise RuntimeError("plan startup failed")

    def drain(self) -> None:
        self.events.append("drain")

    def resume(self) -> None:
        self.events.append("resume")

    def idle(self) -> bool:
        return True

    def handle_command(
        self, operation: str, payload: dict[str, Any],
    ) -> dict[str, Any]:
        if operation == "unknown":
            from quantmaster.runtime.worker_ipc import WorkerCommandError

            raise WorkerCommandError("unknown_command", "unsupported")
        self.events.append((operation, payload))
        return {"operation": operation}

    def stop(self, enter_phase) -> None:
        self.events.append("stop")
        if self.fail_stop:
            raise RuntimeError("plan cleanup failed")
        enter_phase("fake-stop", 1.0)


def _worker(monkeypatch, plan: _Plan):
    from quantmaster.runtime import worker as worker_module

    monkeypatch.setattr(worker_module, "RuntimeCommandServer", _CommandServer)
    worker = worker_module.RuntimeWorker(lambda: plan)
    monkeypatch.setattr(worker, "_start_heartbeat", lambda: None)
    monkeypatch.setattr(worker, "_stop_heartbeat", lambda: None)
    return worker


def test_runtime_worker_executes_injected_plan_lifecycle_and_commands(
    isolated_config, monkeypatch,
):
    plan = _Plan()
    worker = _worker(monkeypatch, plan)

    assert worker.start(bootstrap_rotation=False) is True
    assert worker._handle_command("probe", {"value": 1}) == {"operation": "probe"}
    lease = worker._handle_command("maintenance.enter", {"reason": "test"})
    worker._handle_command("maintenance.exit", {"token": lease["token"]})
    worker.stop()

    assert plan.events == [
        ("start", False),
        ("probe", {"value": 1}),
        "drain",
        "resume",
        "stop",
    ]
    assert worker.status()["in_process_started"] is False


def test_runtime_worker_preserves_plan_command_error_code(isolated_config, monkeypatch):
    from quantmaster.runtime.worker_ipc import WorkerCommandError

    plan = _Plan()
    worker = _worker(monkeypatch, plan)
    worker.start(bootstrap_rotation=False)

    with pytest.raises(WorkerCommandError) as caught:
        worker._handle_command("unknown", {})

    assert caught.value.code == "unknown_command"
    worker.stop()


def test_runtime_worker_cleans_plan_after_partial_start_failure(
    isolated_config, monkeypatch,
):
    plan = _Plan(fail_start=True)
    worker = _worker(monkeypatch, plan)

    with pytest.raises(RuntimeError, match="plan startup failed"):
        worker.start(bootstrap_rotation=True)

    assert plan.events == [("start", True), "stop"]
    assert worker._plan is None
    assert worker._unregister_maintenance is None


def test_runtime_worker_preserves_startup_error_when_partial_cleanup_also_fails(
    isolated_config, monkeypatch,
):
    plan = _Plan(fail_start=True, fail_stop=True)
    worker = _worker(monkeypatch, plan)

    with pytest.raises(RuntimeError, match="plan startup failed"):
        worker.start(bootstrap_rotation=True)

    assert plan.events == [("start", True), "stop"]
    assert worker._plan is None
