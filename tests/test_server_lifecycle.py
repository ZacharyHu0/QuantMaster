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
    monkeypatch.delenv("QM_SERVER_RELOAD_WORKER", raising=False)
    monkeypatch.setattr(server_app, "_stream_runtime", lambda: None)
    monkeypatch.setattr(server_app, "_shutdown_web_stream_executor", lambda: None)
    monkeypatch.setattr(server_app, "_configure_reload_worker_logging", lambda: False)
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

    assert ("supervisor", False) in calls
    assert "supervisor-stop" in calls


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
    monkeypatch.delenv("QM_SERVER_RELOAD_WORKER", raising=False)
    monkeypatch.setattr(server_app, "_stream_runtime", lambda: None)
    monkeypatch.setattr(server_app, "_shutdown_web_stream_executor", lambda: None)
    monkeypatch.setattr(server_app, "_configure_reload_worker_logging", lambda: False)
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


def test_reload_supervisor_owns_free_stockdb_across_worker_reloads(monkeypatch, tmp_path):
    from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime

    calls = []

    class FakeUvicorn:
        pass

    def run_reload(_uvicorn, **kwargs):
        calls.append(("run", kwargs))
        assert lifecycle.os.environ["QM_SERVER_RELOAD_WORKER"] == "1"
        assert lifecycle.os.environ["QM_SERVER_RELOAD_VERBOSE"] == "0"

    monkeypatch.setitem(__import__("sys").modules, "uvicorn", FakeUvicorn())
    monkeypatch.setattr(
        lifecycle,
        "inspect_startup_address",
        lambda host, port, *, version: lifecycle.StartupPreflight(host, port, True, "start"),
    )
    monkeypatch.setattr(free_stockdb_runtime, "start", lambda: calls.append(("stockdb-start",)))
    monkeypatch.setattr(free_stockdb_runtime, "stop", lambda: calls.append(("stockdb-stop",)))
    monkeypatch.setattr("quantmaster.logging_config.is_verbose_logging", lambda: False)
    monkeypatch.setattr(lifecycle, "_run_manual_uvicorn_reload", run_reload)
    monkeypatch.delenv("QM_SERVER_RELOAD_WORKER", raising=False)
    monkeypatch.delenv("QM_SERVER_RELOAD_VERBOSE", raising=False)
    monkeypatch.setenv("QM_FREE_STOCKDB_CONTROL_PATH", str(tmp_path / "control.sqlite"))

    lifecycle.run_uvicorn_foreground(
        "ignored-app", "127.0.0.1", 8686, reload=True,
    )

    assert calls[0] == ("stockdb-start",)
    assert calls[1][0] == "run"
    assert calls[1][1]["host"] == "127.0.0.1"
    assert calls[1][1]["port"] == 8686
    assert calls[1][1]["trigger_path"] == tmp_path / ".quantmaster-reload-trigger"
    assert calls[2] == ("stockdb-stop",)
    assert "QM_SERVER_RELOAD_WORKER" not in lifecycle.os.environ


def test_reload_console_close_stops_owned_stockdb_once(monkeypatch):
    from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime

    calls = []
    callbacks = {}

    class FakeUvicorn:
        pass

    def install(request_shutdown, _shutdown_complete):
        callbacks["close"] = request_shutdown
        return lambda: calls.append("unregister")

    monkeypatch.setitem(__import__("sys").modules, "uvicorn", FakeUvicorn())
    monkeypatch.setattr(
        lifecycle,
        "inspect_startup_address",
        lambda host, port, *, version: lifecycle.StartupPreflight(host, port, True, "start"),
    )
    monkeypatch.setattr(free_stockdb_runtime, "start", lambda: calls.append("start"))
    monkeypatch.setattr(free_stockdb_runtime, "stop", lambda: calls.append("stop"))
    monkeypatch.setattr(lifecycle, "install_windows_console_handler", install)
    monkeypatch.setattr(lifecycle, "_process_is_alive", lambda _pid: True)
    monkeypatch.setattr(
        lifecycle, "_run_manual_uvicorn_reload",
        lambda *_args, **_kwargs: callbacks["close"](),
    )

    lifecycle.run_uvicorn_foreground(
        "ignored-app", "127.0.0.1", 8686, reload=True,
    )

    assert calls == ["start", "stop", "unregister"]
    assert "QM_SERVER_RELOAD_VERBOSE" not in lifecycle.os.environ


def test_reload_lifecycle_deadlines_are_bounded(monkeypatch):
    assert lifecycle._reload_lifecycle_seconds() == (20.0, 10.0, 5.0)
    monkeypatch.setenv("QM_RELOAD_READY_SECONDS", "invalid")
    monkeypatch.setenv("QM_RELOAD_DRAIN_SECONDS", "0")
    monkeypatch.setenv("QM_RELOAD_FORCE_KILL_SECONDS", "999")
    assert lifecycle._reload_lifecycle_seconds() == (20.0, 1.0, 120.0)


def test_windows_reload_listener_uses_exclusive_address(monkeypatch):
    calls = []

    class FakeListener:
        def setsockopt(self, *args):
            calls.append(("setsockopt", args))

        def bind(self, address):
            calls.append(("bind", address))

        def set_inheritable(self, value):
            calls.append(("inheritable", value))

        def close(self):
            calls.append(("close",))

    listener = FakeListener()
    monkeypatch.setattr(lifecycle.os, "name", "nt")
    monkeypatch.setattr(
        lifecycle.socket_module,
        "socket",
        lambda *, family, type: listener,
    )

    class Config:
        def bind_socket(self):
            raise AssertionError("Windows must not use Uvicorn's reusable socket")

    assert lifecycle._bind_reload_socket(Config(), "127.0.0.1", 8686) is listener
    assert calls == [
        (
            "setsockopt",
            (
                lifecycle.socket_module.SOL_SOCKET,
                lifecycle._SO_EXCLUSIVEADDRUSE,
                1,
            ),
        ),
        ("bind", ("127.0.0.1", 8686)),
        ("inheritable", True),
    ]


def test_windows_reload_listener_reports_port_conflict(monkeypatch):
    calls = []

    class ConflictingListener:
        def setsockopt(self, *_args):
            pass

        def bind(self, _address):
            raise OSError("address already in use")

        def set_inheritable(self, _value):
            raise AssertionError("a failed listener must not be inherited")

        def close(self):
            calls.append("closed")

    monkeypatch.setattr(lifecycle.os, "name", "nt")
    monkeypatch.setattr(
        lifecycle.socket_module,
        "socket",
        lambda **_kwargs: ConflictingListener(),
    )

    with pytest.raises(RuntimeError, match=r"127\.0\.0\.1:8686.*端口已被"):
        lifecycle._bind_reload_socket(object(), "127.0.0.1", 8686)

    assert calls == ["closed"]


def test_reload_stop_never_joins_a_wedged_worker_without_a_deadline(monkeypatch):
    class FakeProcess:
        pid = 42

        def __init__(self):
            self.alive = True
            self.terminations = 0
            self.joins = []

        def is_alive(self):
            return self.alive

        def join(self, timeout):
            self.joins.append(timeout)

        def terminate(self):
            self.terminations += 1
            if self.terminations >= 2:
                self.alive = False

    # Avoid emitting a real Ctrl+C event in this unit test; Unix semantics
    # still exercise the bounded drain and forced-stop path.
    monkeypatch.setattr(lifecycle.os, "name", "posix")
    process = FakeProcess()

    assert lifecycle._stop_reload_process(
        process, drain_seconds=10.0, force_seconds=5.0,
    ) is True
    assert process.terminations == 2
    assert process.joins == [10.0, 5.0]


def test_reload_stop_uses_private_drain_not_windows_console_broadcast(monkeypatch):
    """A Web replacement must never Ctrl+C the independent worker process."""

    class DrainEvent:
        def __init__(self):
            self.set_calls = 0

        def set(self):
            self.set_calls += 1

    class FakeProcess:
        pid = 42

        def __init__(self, event):
            self.alive = True
            self.event = event
            self.terminations = 0
            self.joins = []

        def is_alive(self):
            return self.alive

        def join(self, timeout):
            self.joins.append(timeout)
            if self.event.set_calls:
                self.alive = False

        def terminate(self):
            self.terminations += 1

    event = DrainEvent()
    process = FakeProcess(event)
    monkeypatch.setattr(lifecycle.os, "name", "nt")
    monkeypatch.setattr(
        lifecycle.os,
        "kill",
        lambda *_args: (_ for _ in ()).throw(AssertionError("不得发送 CTRL_C_EVENT")),
    )

    assert lifecycle._stop_reload_process(
        process,
        drain_seconds=10.0,
        force_seconds=5.0,
        drain_event=event,
    ) is True
    assert event.set_calls == 1
    assert process.terminations == 0
    assert process.joins == [10.0]


def test_manual_reload_trigger_is_only_available_to_reload_worker(tmp_path, monkeypatch):
    trigger = tmp_path / "reload.trigger"
    monkeypatch.delenv("QM_SERVER_RELOAD_WORKER", raising=False)
    monkeypatch.setenv(lifecycle.RELOAD_TRIGGER_PATH_ENV, str(trigger))
    assert lifecycle.manual_reload_trigger_path() is None

    monkeypatch.setenv("QM_SERVER_RELOAD_WORKER", "1")
    assert lifecycle.manual_reload_trigger_path() == trigger.resolve()
    lifecycle.request_manual_reload(trigger)
    assert trigger.read_text(encoding="ascii").isdigit()


def test_only_manual_trigger_requests_web_worker_reload(tmp_path):
    trigger = tmp_path / ".quantmaster-reload-trigger"
    source = tmp_path / "quantmaster" / "server" / "app.py"

    assert lifecycle._manual_reload_changes({source}, trigger) is None
    assert lifecycle._manual_reload_changes({source, trigger}, trigger) == [trigger.resolve()]


def test_reload_worker_restores_cli_logging(monkeypatch):
    from quantmaster.server import app as server_app

    calls = []
    monkeypatch.setenv("QM_SERVER_RELOAD_WORKER", "1")
    monkeypatch.setenv("QM_SERVER_RELOAD_VERBOSE", "1")
    monkeypatch.setattr(
        "quantmaster.logging_config.configure_logging",
        lambda *, verbose: calls.append(verbose),
    )

    assert server_app._configure_reload_worker_logging() is True
    assert calls == [True]


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
