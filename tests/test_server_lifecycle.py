"""前台服务器父子进程生命周期测试。"""

from __future__ import annotations

import threading

from quantmaster.server import lifecycle


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
    monkeypatch.setattr(free_stockdb_runtime, "start", lambda: calls.append(("stockdb-start",)))
    monkeypatch.setattr(free_stockdb_runtime, "stop", lambda: calls.append(("stockdb-stop",)))
    monkeypatch.setattr("quantmaster.logging_config.is_verbose_logging", lambda: False)
    monkeypatch.setattr(lifecycle, "_run_quiet_uvicorn_reload", run_reload)
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
    monkeypatch.setattr(free_stockdb_runtime, "start", lambda: calls.append("start"))
    monkeypatch.setattr(free_stockdb_runtime, "stop", lambda: calls.append("stop"))
    monkeypatch.setattr(lifecycle, "install_windows_console_handler", install)
    monkeypatch.setattr(lifecycle, "_process_is_alive", lambda _pid: True)
    monkeypatch.setattr(
        lifecycle, "_run_quiet_uvicorn_reload",
        lambda *_args, **_kwargs: callbacks["close"](),
    )

    lifecycle.run_uvicorn_foreground(
        "ignored-app", "127.0.0.1", 8686, reload=True,
    )

    assert calls == ["start", "stop", "unregister"]
    assert "QM_SERVER_RELOAD_VERBOSE" not in lifecycle.os.environ


def test_reload_ignores_release_bookkeeping_until_backend_changes(tmp_path, monkeypatch):
    package_dir = tmp_path / "quantmaster"
    package_dir.mkdir()
    release = package_dir / "release.py"
    config = package_dir / "config.py"

    assert lifecycle._meaningful_reload_paths({release}, package_dir) == []
    assert lifecycle._meaningful_reload_paths(
        {release, config}, package_dir,
    ) == [config]
    assert lifecycle._reload_timing_ms() == (30_000, 300_000, 300_000)
    monkeypatch.setenv("QM_RELOAD_QUIET_SECONDS", "45")
    monkeypatch.setenv("QM_RELOAD_MAX_BATCH_SECONDS", "600")
    monkeypatch.setenv("QM_RELOAD_MIN_INTERVAL_SECONDS", "900")
    assert lifecycle._reload_timing_ms() == (45_000, 600_000, 900_000)


def test_reload_timing_is_bounded_and_invalid_values_use_defaults(monkeypatch):
    monkeypatch.setenv("QM_RELOAD_QUIET_SECONDS", "invalid")
    monkeypatch.setenv("QM_RELOAD_MAX_BATCH_SECONDS", "1")
    monkeypatch.setenv("QM_RELOAD_MIN_INTERVAL_SECONDS", "invalid")
    assert lifecycle._reload_timing_ms() == (30_000, 30_000, 300_000)

    monkeypatch.setenv("QM_RELOAD_QUIET_SECONDS", "9999")
    monkeypatch.setenv("QM_RELOAD_MAX_BATCH_SECONDS", "9999")
    monkeypatch.setenv("QM_RELOAD_MIN_INTERVAL_SECONDS", "9999")
    assert lifecycle._reload_timing_ms() == (300_000, 1_800_000, 1_800_000)


def test_reload_gate_accumulates_changes_during_five_minute_cooldown(tmp_path):
    package_dir = tmp_path / "quantmaster"
    package_dir.mkdir()
    first = package_dir / "config.py"
    second = package_dir / "server.py"
    release = package_dir / "release.py"
    now = [100.0]
    gate = lifecycle._ReloadChangeGate(package_dir, 300.0, lambda: now[0])

    assert gate.offer({first}) == [first]
    now[0] = 200.0
    assert gate.offer({second, release}) is None
    now[0] = 399.9
    assert gate.offer(set()) is None
    now[0] = 400.0
    assert gate.offer(set()) == [second]


def test_manual_reload_trigger_is_only_available_to_reload_worker(tmp_path, monkeypatch):
    trigger = tmp_path / "reload.trigger"
    monkeypatch.delenv("QM_SERVER_RELOAD_WORKER", raising=False)
    monkeypatch.setenv(lifecycle.RELOAD_TRIGGER_PATH_ENV, str(trigger))
    assert lifecycle.manual_reload_trigger_path() is None

    monkeypatch.setenv("QM_SERVER_RELOAD_WORKER", "1")
    assert lifecycle.manual_reload_trigger_path() == trigger.resolve()
    lifecycle.request_manual_reload(trigger)
    assert trigger.read_text(encoding="ascii").isdigit()


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
