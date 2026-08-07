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


def test_reload_supervisor_owns_free_stockdb_across_worker_reloads(monkeypatch):
    from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime

    calls = []

    class FakeUvicorn:
        @staticmethod
        def run(app, **kwargs):
            calls.append(("run", app, kwargs))
            assert lifecycle.os.environ["QM_SERVER_RELOAD_WORKER"] == "1"

    monkeypatch.setitem(__import__("sys").modules, "uvicorn", FakeUvicorn())
    monkeypatch.setattr(free_stockdb_runtime, "start", lambda: calls.append(("stockdb-start",)))
    monkeypatch.setattr(free_stockdb_runtime, "stop", lambda: calls.append(("stockdb-stop",)))
    monkeypatch.delenv("QM_SERVER_RELOAD_WORKER", raising=False)

    lifecycle.run_uvicorn_foreground(
        "ignored-app", "127.0.0.1", 8686, reload=True,
    )

    assert calls[0] == ("stockdb-start",)
    assert calls[1][0:2] == ("run", "quantmaster.server.app:app")
    assert calls[1][2]["reload"] is True
    assert calls[1][2]["reload_includes"] == ["*.py"]
    assert calls[2] == ("stockdb-stop",)
    assert "QM_SERVER_RELOAD_WORKER" not in lifecycle.os.environ
