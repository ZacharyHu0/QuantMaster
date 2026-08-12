from __future__ import annotations


def test_role_process_uses_normal_start_outside_windows(monkeypatch):
    from quantmaster.runtime import windows_app

    calls: list[str] = []

    class Process:
        def start(self) -> None:
            calls.append("start")

    monkeypatch.setattr(windows_app.os, "name", "posix")

    windows_app.start_windows_role_process(Process(), "Compute Worker")

    assert calls == ["start"]


def test_role_process_uses_temporary_spawn_executable_on_windows(monkeypatch):
    from quantmaster.runtime import windows_app

    calls: list[object] = []

    class Process:
        def start(self) -> None:
            calls.append("start")

    monkeypatch.setattr(windows_app.os, "name", "nt")
    monkeypatch.setenv(windows_app.APP_JOB_ENV, "123")
    monkeypatch.setattr(windows_app, "_role_executable", lambda role: f"C:/tmp/{role}.exe")
    monkeypatch.setattr(windows_app.spawn, "get_executable", lambda: b"C:/python.exe")
    monkeypatch.setattr(windows_app.multiprocessing, "set_executable", calls.append)

    windows_app.start_windows_role_process(Process(), "Runtime Worker")

    assert calls == ["C:/tmp/Runtime Worker.exe", "start", "C:/python.exe"]
