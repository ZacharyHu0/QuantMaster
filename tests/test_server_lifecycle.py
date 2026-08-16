"""前台服务器父子进程生命周期测试。"""

from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace

import pytest

from quantmaster.server import lifecycle


def test_app_lifespan_forwards_rotation_bootstrap_to_supervisor(monkeypatch):
    from quantmaster.bootstrap import get_worker_supervisor
    from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime
    from quantmaster.server import app as server_app

    calls = []
    supervisor = get_worker_supervisor()
    monkeypatch.setattr(server_app, "_stream_runtime", lambda: None)
    monkeypatch.setattr(server_app, "_shutdown_web_stream_executor", lambda: None)
    monkeypatch.setattr("quantmaster.server.management.capture_runtime_baseline", lambda: None)
    monkeypatch.setattr("quantmaster.logging_config.current_log_path", lambda: None)
    monkeypatch.setattr("quantmaster.ai.llm.close_llm_http_clients", lambda: None)
    monkeypatch.setattr(free_stockdb_runtime, "start", lambda: calls.append("stockdb-start"))
    monkeypatch.setattr(free_stockdb_runtime, "stop", lambda: calls.append("stockdb-stop"))
    monkeypatch.setattr(
        supervisor, "start",
        lambda *, bootstrap_rotation: calls.append(("supervisor", bootstrap_rotation)) or "started",
    )
    monkeypatch.setattr(supervisor, "stop", lambda: calls.append("supervisor-stop"))

    async def run() -> None:
        async with server_app.create_lifespan(bootstrap_rotation=False)(server_app.app):
            assert "splash-close" not in calls

    asyncio.run(run())

    assert calls[0] == "stockdb-start"
    assert ("supervisor", False) in calls
    assert "supervisor-stop" in calls
    assert calls[-1] == "stockdb-stop"


def test_splash_waits_for_listener_and_core_readiness_without_sleep() -> None:
    server = SimpleNamespace(started=False)
    readiness = {"core_ready": False}
    transitions = iter(((False, True), (True, False), (True, True)))
    closed = []

    class ControlledEvent:
        def wait(self, _timeout: float) -> bool:
            server.started, readiness["core_ready"] = next(transitions)
            return False

    lifecycle._wait_for_splash_readiness(
        server,
        ControlledEvent(),
        lambda **_kwargs: readiness,
        lambda: closed.append((server.started, readiness["core_ready"])),
    )

    assert closed == [(True, True)]


def test_inactive_splash_does_not_start_readiness_watcher(monkeypatch) -> None:
    monkeypatch.setattr(lifecycle, "splash_active", lambda: False)
    monkeypatch.setattr(
        lifecycle.threading,
        "Thread",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not start watcher")),
    )

    assert lifecycle._start_splash_readiness_watcher(
        SimpleNamespace(started=False),
        threading.Event(),
    ) is None


def test_splash_stays_open_when_startup_stops_before_listener() -> None:
    server = SimpleNamespace(started=False)
    closed = []

    class StartupFailureEvent:
        def __init__(self) -> None:
            self.calls = 0

        def wait(self, _timeout: float) -> bool:
            self.calls += 1
            return self.calls > 1

    lifecycle._wait_for_splash_readiness(
        server,
        StartupFailureEvent(),
        lambda **_kwargs: {"core_ready": True},
        lambda: closed.append(True),
    )

    assert closed == []


def test_app_lifespan_forwards_rotation_bootstrap_to_disabled_fallback(monkeypatch):
    from quantmaster.bootstrap import get_runtime_worker, get_worker_supervisor
    from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime
    from quantmaster.server import app as server_app

    calls = []
    supervisor = get_worker_supervisor()
    worker = get_runtime_worker()
    monkeypatch.setattr(server_app, "_stream_runtime", lambda: None)
    monkeypatch.setattr(server_app, "_shutdown_web_stream_executor", lambda: None)
    monkeypatch.setattr("quantmaster.server.management.capture_runtime_baseline", lambda: None)
    monkeypatch.setattr("quantmaster.logging_config.current_log_path", lambda: None)
    monkeypatch.setattr("quantmaster.ai.llm.close_llm_http_clients", lambda: None)
    monkeypatch.setattr(free_stockdb_runtime, "start", lambda: None)
    monkeypatch.setattr(free_stockdb_runtime, "stop", lambda: None)
    monkeypatch.setattr(supervisor, "start", lambda **_kwargs: "disabled")
    monkeypatch.setattr(
        worker, "start",
        lambda *, bootstrap_rotation: calls.append(("worker", bootstrap_rotation)),
    )
    monkeypatch.setattr(worker, "stop", lambda: calls.append("worker-stop"))

    async def run() -> None:
        async with server_app.create_lifespan(bootstrap_rotation=False)(server_app.app):
            pass

    asyncio.run(run())

    assert calls == [("worker", False), "worker-stop"]


def test_parent_exit_requests_graceful_shutdown(monkeypatch):
    states = iter((True, False))
    monkeypatch.setattr(lifecycle, "_process_is_alive", lambda _pid: next(states))
    requested = []

    lifecycle.watch_parent_exit(
        12345, lambda: requested.append(True), threading.Event(), poll_interval=0,
    )

    assert requested == [True]


def test_parent_watcher_stops_without_requesting_shutdown(monkeypatch):
    stop_event = threading.Event()
    stop_event.set()
    monkeypatch.setattr(
        lifecycle, "_process_is_alive",
        lambda _pid: (_ for _ in ()).throw(AssertionError("不应检查父进程")),
    )
    requested = []

    lifecycle.watch_parent_exit(12345, lambda: requested.append(True), stop_event)

    assert requested == []


def test_explicit_launcher_pid_overrides_onefile_bootloader_parent(monkeypatch):
    monkeypatch.setenv("QM_LAUNCHER_PID", "54321")
    monkeypatch.setattr(
        lifecycle.os,
        "getppid",
        lambda: (_ for _ in ()).throw(AssertionError("must not watch the bootloader")),
    )

    assert lifecycle.server_parent_pid() == 54321


def test_windows_console_handler_routes_ctrl_c_to_coordinated_shutdown(monkeypatch):
    import ctypes

    callbacks = {}

    class Function:
        argtypes = None
        restype = None

        def __call__(self, handler, enabled):
            callbacks["handler"] = handler if enabled else None
            return True

    class Kernel:
        SetConsoleCtrlHandler = Function()

    monkeypatch.setattr(lifecycle.os, "name", "nt")
    monkeypatch.setattr(
        ctypes, "WinDLL", lambda *_args, **_kwargs: Kernel(), raising=False,
    )
    monkeypatch.setattr(
        ctypes, "WINFUNCTYPE", ctypes.CFUNCTYPE, raising=False,
    )
    requested = []
    complete = threading.Event()
    complete.set()

    unregister = lifecycle.install_windows_console_handler(
        lambda: requested.append(True), complete,
    )

    assert callbacks["handler"](0) == 1
    assert requested == [True]
    unregister()


def test_run_uvicorn_foreground_releases_handlers(monkeypatch):
    calls = []

    class FakeConfig:
        def __init__(self, app, **kwargs):
            calls.append(("config", app, kwargs))

    class FakeServer:
        def __init__(self, _config):
            self.should_exit = False

        def run(self):
            calls.append(("run",))

    class FakeUvicorn:
        Config = FakeConfig
        Server = FakeServer

    monkeypatch.setitem(__import__("sys").modules, "uvicorn", FakeUvicorn())
    monkeypatch.setattr(
        lifecycle,
        "inspect_startup_address",
        lambda host, port, *, version: lifecycle.StartupPreflight(host, port, True, "start"),
    )
    monkeypatch.setattr(lifecycle.os, "getppid", lambda: 12345)
    monkeypatch.setattr(lifecycle, "_process_is_alive", lambda _pid: True)
    monkeypatch.setattr(
        lifecycle,
        "install_windows_console_handler",
        lambda _request, _complete: lambda: calls.append(("unregister",)),
    )

    lifecycle.run_uvicorn_foreground("app", "127.0.0.1", 8686)

    assert calls == [
        ("config", "app", {
            "host": "127.0.0.1", "port": 8686, "log_level": "warning",
            "log_config": None, "access_log": False,
        }),
        ("run",),
        ("unregister",),
    ]


def test_startup_preflight_reuses_healthy_same_version_quantmaster(monkeypatch):
    monkeypatch.setattr(lifecycle, "_port_is_available", lambda _host, _port: False)
    monkeypatch.setattr(
        lifecycle,
        "_http_json",
        lambda _host, _port, path: (
            {"status": "ok", "version": "9.9.9", "generation": "4"}
                if path.endswith("/health") else {"status": "ready", "generation": "4"}
        ),
    )
    monkeypatch.setattr(
        lifecycle, "_listener_process",
        lambda _host, _port: lifecycle.ListenProcess(
            pid=4321, name="QuantMaster Web Worker.exe", executable="C:/qm.exe",
        ),
    )

    result = lifecycle.inspect_startup_address("127.0.0.1", 8686, version="9.9.9")

    assert result.action == "reuse"
    assert result.process and result.process.pid == 4321
    assert result.process and result.process.quantmaster_role == "web"
    assert result.health and result.health["health"]["generation"] == "4"
    assert "复用" in result.message


def test_startup_preflight_reports_unknown_listener_without_touching_it(monkeypatch):
    monkeypatch.setattr(lifecycle, "_port_is_available", lambda _host, _port: False)
    monkeypatch.setattr(lifecycle, "_http_json", lambda *_args: None)
    process = lifecycle.ListenProcess(pid=9876, name="nginx.exe", executable="C:/nginx/nginx.exe")
    monkeypatch.setattr(lifecycle, "_listener_process", lambda _host, _port: process)

    result = lifecycle.inspect_startup_address("127.0.0.1", 8686, version="9.9.9")

    assert result.action == "blocked"
    assert result.process == process
    assert "PID 9876" in result.message
    assert "不会自动结束" in result.message


def test_run_uvicorn_foreground_returns_without_starting_duplicate_same_version(monkeypatch):
    monkeypatch.setitem(__import__("sys").modules, "uvicorn", object())
    monkeypatch.setattr(
        lifecycle,
        "inspect_startup_address",
        lambda host, port, *, version: lifecycle.StartupPreflight(
            host, port, False, "reuse", "复用现有实例",
        ),
    )

    lifecycle.run_uvicorn_foreground("app", "127.0.0.1", 8686)


def test_run_uvicorn_foreground_raises_diagnostic_port_conflict(monkeypatch):
    conflict = lifecycle.StartupPreflight(
        "127.0.0.1", 8686, False, "blocked", "PID 9876 已占用",
        lifecycle.ListenProcess(pid=9876, name="nginx.exe"),
    )
    monkeypatch.setitem(__import__("sys").modules, "uvicorn", object())
    monkeypatch.setattr(
        lifecycle, "inspect_startup_address", lambda *_args, **_kwargs: conflict,
    )
    monkeypatch.setattr(
        lifecycle,
        "_start_splash_readiness_watcher",
        lambda *_args: (_ for _ in ()).throw(AssertionError("must not start before preflight")),
    )

    with pytest.raises(lifecycle.StartupPortConflictError, match="PID 9876"):
        lifecycle.run_uvicorn_foreground("app", "127.0.0.1", 8686)


def test_runtime_worker_heartbeat_is_a_fast_local_lease(isolated_config):
    from quantmaster.runtime.identity import get_application_identity
    from quantmaster.runtime.worker import RuntimeWorker, runtime_worker_status

    worker = RuntimeWorker()
    worker._started = True
    worker._write_heartbeat()

    status = runtime_worker_status()
    assert status["available"] is True
    assert status["worker_id"] == worker._worker_id
    assert status["pid"] > 0
    assert {
        key: status[key]
        for key in ("build_sha", "slot_id", "runtime_generation")
    } == {
        key: getattr(get_application_identity(), key)
        for key in ("build_sha", "slot_id", "runtime_generation")
    }

    worker._stop_heartbeat()
    assert runtime_worker_status()["available"] is False


def test_runtime_worker_status_rejects_another_application_generation(
    isolated_config, monkeypatch,
):
    from quantmaster.runtime.identity import RUNTIME_GENERATION_ENV
    from quantmaster.runtime.worker import RuntimeWorker, runtime_worker_status

    monkeypatch.setenv(RUNTIME_GENERATION_ENV, "a" * 32)
    worker = RuntimeWorker()
    worker._started = True
    worker._write_heartbeat()
    monkeypatch.setenv(RUNTIME_GENERATION_ENV, "b" * 32)

    status = runtime_worker_status()

    assert status["available"] is False
    assert status["status"] == "unavailable"
    assert status["reason"] == "runtime_identity_mismatch"


def test_runtime_worker_status_reports_a_persisted_bootstrap_failure(isolated_config):
    from quantmaster.runtime.worker import runtime_worker_status

    (isolated_config.data_root / "runtime-worker-supervisor.json").write_text(
        '{"status":"failed","detail":"RuntimeError: startup failed"}',
        encoding="utf-8",
    )

    status = runtime_worker_status()

    assert status["available"] is False
    assert status["supervisor"]["status"] == "failed"
    assert "startup failed" in status["reason"]


def test_runtime_worker_status_exposes_schema_migration_block(isolated_config):
    from quantmaster.runtime.worker import RuntimeWorker, runtime_worker_status

    worker = RuntimeWorker()
    worker._plan = type(
        "SchemaBlockedPlan",
        (),
        {
            "schema_migration": "schema-migration-blocked",
            "schema_migration_detail": "lab schema 需显式迁移",
        },
    )()
    worker._started = True
    worker._write_heartbeat()

    status = runtime_worker_status()

    assert status["status"] == "running"
    assert status["available"] is True
    assert status["schema_migration_blocked"] is True
    assert status["worker_state"] == "schema-migration-blocked"
    assert status["schema_migration_detail"] == "lab schema 需显式迁移"
