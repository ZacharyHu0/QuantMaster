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


def test_base_interpreter_falls_back_when_renamed_base_path_does_not_exist(
    tmp_path, monkeypatch,
):
    from quantmaster.runtime import windows_app

    base = tmp_path / "base"
    base.mkdir()
    canonical = base / "python.exe"
    canonical.write_bytes(b"python")
    venv = tmp_path / "venv"
    venv.mkdir()
    renamed = venv / "QuantMaster Runtime Worker.exe"
    renamed.write_bytes(b"python")
    monkeypatch.setattr(windows_app.sys, "base_prefix", str(base))
    monkeypatch.setattr(windows_app.sys, "executable", str(renamed))
    monkeypatch.setattr(
        windows_app.sys,
        "_base_executable",
        str(base / "QuantMaster Runtime Worker.exe"),
        raising=False,
    )

    assert windows_app._base_interpreter() == canonical.resolve()


def test_frozen_role_process_reuses_onefile_archive_without_copying(tmp_path, monkeypatch):
    from quantmaster.runtime import windows_app

    executable = tmp_path / "QuantMaster.exe"
    executable.write_bytes(b"onefile archive")
    monkeypatch.setattr(windows_app.os, "name", "nt")
    monkeypatch.setattr(windows_app.sys, "frozen", True, raising=False)
    monkeypatch.setattr(windows_app.sys, "executable", str(executable))
    monkeypatch.setattr(
        windows_app.shutil,
        "copy2",
        lambda *_args: (_ for _ in ()).throw(AssertionError("frozen EXE must not be copied")),
    )

    assert windows_app._role_executable("Compute Worker") == str(executable.resolve())
    assert not (tmp_path / "QuantMaster Compute Worker.exe").exists()


def test_windows_role_sanitizes_filename_without_losing_job_identity():
    from quantmaster.runtime.windows_app import _safe_role

    assert _safe_role("Worker - after_close.scan") == "Worker - after_close.scan"
    assert _safe_role("Worker: news/crawl*") == "Worker news crawl"


def test_missing_role_marker_reads_as_empty(tmp_path):
    from quantmaster.runtime.windows_app import _read_text

    assert _read_text(tmp_path / "missing.role") == ""


def test_role_version_resource_exposes_human_description():
    from quantmaster.runtime.windows_executable import build_version_resource

    payload = build_version_resource(
        "1.2.3",
        description="QuantMaster Worker - News Crawl",
        internal_name="QuantMaster Worker - News Crawl",
        original_filename="QuantMaster Worker - News Crawl.exe",
    )

    assert "QuantMaster Worker - News Crawl".encode("utf-16le") in payload
    assert "QuantMaster Worker - News Crawl.exe".encode("utf-16le") in payload
