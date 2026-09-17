from __future__ import annotations

from typing import Any

import pytest


def test_spawn_entry_initializes_runtime_logging_before_consumers(monkeypatch):
    import os
    from types import SimpleNamespace

    from quantmaster.server import bootstrap

    events = []
    monkeypatch.setenv("QM_WEB_PROCESS", "1")
    monkeypatch.setenv("QM_WORKER_SUPERVISOR", "0")
    monkeypatch.setattr("quantmaster.runtime.windows_app.initialize_windows_app_process", lambda: None)
    monkeypatch.setattr("quantmaster.logging_config.configure_logging", lambda **kwargs: events.append((
        "logging", os.environ.get("QM_WEB_PROCESS"), os.environ.get("QM_WORKER_SUPERVISOR"),
    )))
    worker = SimpleNamespace(
        start=lambda **kwargs: events.append("start"), stop=lambda: events.append("stop"),
    )
    monkeypatch.setattr(bootstrap, "get_runtime_worker", lambda: worker)
    monkeypatch.setattr(bootstrap, "publish_worker_supervisor_status", lambda *a, **k: None)
    bootstrap.run_runtime_worker(SimpleNamespace(wait=lambda seconds: True), False)
    assert events == [("logging", None, "1"), "start", "stop"]


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


def test_worker_heartbeat_follows_confirmed_plan_settings(isolated_config, monkeypatch):
    from quantmaster.runtime.worker import runtime_worker_status

    plan = _Plan()
    worker = _worker(monkeypatch, plan)
    worker.start(bootstrap_rotation=False)
    worker._write_heartbeat()
    assert runtime_worker_status()["effective_revision"] == 4
    monkeypatch.setattr(plan, "settings_projection", lambda: (5, 8))
    worker._write_heartbeat()
    assert runtime_worker_status()["effective_revision"] == 5
    assert runtime_worker_status()["config_generation"] == 8
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


@pytest.mark.parametrize("release_before_deadline", [True, False])
def test_default_plan_maintenance_preserves_executor_and_recovers_busy_work(
    isolated_config, tmp_path, monkeypatch, release_before_deadline,
):
    import threading
    import time
    from types import SimpleNamespace

    from quantmaster.backtest.jobs import BacktestJobManager
    from quantmaster.data.maintenance import DataRefreshManager
    from quantmaster.data.repair import DataRepairManager
    from quantmaster.research.jobs import ResearchJobManager
    from quantmaster.runtime.jobs import JobOutcome, UnifiedJobRuntime, UnifiedJobStore
    from quantmaster.runtime.maintenance import MaintenanceBarrier, MaintenanceParticipant
    from quantmaster.server.bootstrap import _DefaultWorkerPlan

    monkeypatch.delenv("QM_WEB_PROCESS", raising=False)
    monkeypatch.delenv("QM_WORKER_SUPERVISOR", raising=False)
    runtime = UnifiedJobRuntime(UnifiedJobStore(tmp_path / "jobs.sqlite"), max_workers=1)
    entered, release = threading.Event(), threading.Event()
    attempts = []

    def handler(context, spec):
        attempts.append(context.attempt)
        entered.set()
        assert release.wait(3)
        return JobOutcome("completed", "resumed")

    runtime.register("test.drain", handler)
    plan = _DefaultWorkerPlan.__new__(_DefaultWorkerPlan)
    def noop(*a, **k):
        return None
    other = SimpleNamespace(idle=True, start=noop, stop=noop, pause=noop, resume=noop)
    for name in ("cnn_fear_greed_refresher", "ashare_fear_greed_refresher",
                 "paper_automation_worker", "rotation_worker", "stock_analysis_worker",
                 "after_close_worker", "etf_research_worker", "news_worker", "settings_worker"):
        setattr(plan, name, other)
    plan.runtime = SimpleNamespace(start=noop, stop=noop, service=SimpleNamespace(jobs=other))
    plan.lab_llm_worker = SimpleNamespace(runtime=other)
    plan.lab_worker = None
    plan.research_worker = ResearchJobManager(runtime=runtime)
    plan.data_refresh_manager = DataRefreshManager(runtime=runtime)
    plan.repair_worker = DataRepairManager(runtime=runtime, read_only=True)
    plan.backtest_jobs = BacktestJobManager(runtime=runtime)
    plan._publish_async = noop
    plan._publish_market_overview = noop
    barrier = MaintenanceBarrier()
    def drain():
        plan.drain()
        if release_before_deadline:
            release.set()

    barrier.register(MaintenanceParticipant("plan", drain, plan.resume, plan.idle))
    job, _ = runtime.submit("test.drain", {})
    assert entered.wait(2)
    executor = runtime._executor
    try:
        if release_before_deadline:
            lease = barrier.enter("application activation", timeout=1)
            assert plan.idle()
            assert runtime.store.get(job["id"])["status"] == "interrupted"
            barrier.exit(lease)
        else:
            with pytest.raises(TimeoutError):
                barrier.enter("application activation", timeout=0.1)
            assert not barrier.active
            release.set()
        deadline = time.monotonic() + 3
        while not runtime.idle and time.monotonic() < deadline:
            time.sleep(0.01)
        next_job, _ = runtime.submit("test.drain", {"next": True})
        assert runtime.wait(next_job["id"], timeout=2)["status"] == "completed"
        assert runtime._executor is executor
        assert not runtime.stopping
    finally:
        release.set()
        runtime.stop()



def test_recovery_failure_is_visible_in_heartbeat(isolated_config, monkeypatch):
    import json

    from quantmaster.runtime import worker as worker_module
    from quantmaster.runtime.maintenance import MaintenanceBarrier, MaintenanceParticipant

    barrier = MaintenanceBarrier()

    def fail_resume():
        raise RuntimeError("partial recovery failed")

    barrier.register(MaintenanceParticipant("broken", lambda: None, fail_resume, lambda: True))
    lease = barrier.enter("application activation")
    with pytest.raises(RuntimeError):
        barrier.exit(lease)
    monkeypatch.setattr(worker_module, "maintenance_barrier", barrier)
    worker = _worker(monkeypatch, _Plan())
    worker._command_server = _CommandServer(worker._handle_command)
    worker._command_server.start()
    worker._write_heartbeat()
    value = json.loads(worker_module._heartbeat_path().read_text(encoding="utf-8"))
    assert value["commands_available"] is False
    assert value["maintenance"]["state"] == "recovery_failed"


def test_worker_activation_token_can_be_recovered_after_reply_loss(isolated_config, monkeypatch):
    worker = _worker(monkeypatch, _Plan())
    worker.start(bootstrap_rotation=False)
    try:
        worker._handle_command("maintenance.enter", {"reason": "application activation"})
        status = worker._handle_command("maintenance.status", {"token": ""})
        assert status["state"] == "frozen"
        assert status["token"]
        worker._handle_command("maintenance.exit", {"token": status["token"]})
        assert worker._handle_command("maintenance.status", {})["state"] == "open"
    finally:
        worker.stop()

