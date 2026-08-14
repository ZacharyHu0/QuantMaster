from __future__ import annotations

import os
import time

from quantmaster.runtime.supervisor import WorkerSupervisor
from tests.process_handler import supervisor_crash_once, supervisor_probe


def test_worker_supervisor_is_single_instance_and_runs_outside_web_process(tmp_path, monkeypatch):
    marker = tmp_path / "worker.pid"
    monkeypatch.delenv("QM_DISABLE_WORKER_SUPERVISOR", raising=False)
    monkeypatch.setenv("QM_TEST_SUPERVISOR_MARKER", str(marker))
    primary = WorkerSupervisor(
        tmp_path,
        target=supervisor_probe,
    )
    secondary = WorkerSupervisor(tmp_path, target=supervisor_probe)
    try:
        assert primary.start(bootstrap_rotation=True) == "started"
        deadline = time.monotonic() + 10
        while not marker.is_file() and time.monotonic() < deadline:
            time.sleep(0.05)
        assert marker.is_file()
        pid, bootstrap = marker.read_text(encoding="utf-8").split("|")
        assert int(pid) != os.getpid()
        assert bootstrap == "1"
        assert secondary.start() == "attached"
    finally:
        primary.stop()
        secondary.stop()


def test_worker_supervisor_restarts_a_child_that_exits_during_bootstrap(tmp_path, monkeypatch):
    marker = tmp_path / "restarted.pid"
    monkeypatch.delenv("QM_DISABLE_WORKER_SUPERVISOR", raising=False)
    monkeypatch.setenv("QM_TEST_SUPERVISOR_MARKER", str(marker))
    supervisor = WorkerSupervisor(tmp_path, target=supervisor_crash_once)
    try:
        assert supervisor.start(bootstrap_rotation=False) == "started"
        deadline = time.monotonic() + 10
        while not marker.is_file() and time.monotonic() < deadline:
            time.sleep(0.05)
        assert marker.is_file()
        assert supervisor.ensure_running(bootstrap_rotation=False) == "running"
        assert marker.read_text(encoding="utf-8").endswith("|0")
    finally:
        supervisor.stop()
