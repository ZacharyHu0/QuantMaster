import io
import os
from pathlib import Path

import pytest

from scripts.release import smoke_frozen_runtime
from scripts.release.smoke_frozen_runtime import (
    _assert_same_identity,
    _pid_alive,
)


def test_frozen_runtime_smoke_requires_one_exact_application_identity():
    identity = {
        "build_sha": "a" * 40,
        "slot_id": "slot-a",
        "runtime_generation": "b" * 32,
    }

    _assert_same_identity(identity, {**identity, "pid": 2}, {**identity, "pid": 3})

    with pytest.raises(RuntimeError, match="runtime_generation"):
        _assert_same_identity(
            identity,
            {**identity, "runtime_generation": "c" * 32},
            identity,
        )


@pytest.mark.skipif(os.name != "nt", reason="Windows process contract")
def test_frozen_runtime_smoke_observes_windows_process_liveness():
    assert _pid_alive(os.getpid())
    assert not _pid_alive(0xFFFFFFFF)


def test_internal_launcher_exits_on_eof_without_signaling_child(tmp_path, monkeypatch):
    calls = []

    class FrozenProcess:
        pid = 4321

        def send_signal(self, _signal):
            raise AssertionError("launcher exit must be the shutdown signal")

        def kill(self):
            raise AssertionError("successful launcher exit must not kill the child")

    def popen(command, **kwargs):
        calls.append((command, kwargs))
        return FrozenProcess()

    monkeypatch.setattr(smoke_frozen_runtime.subprocess, "Popen", popen)
    monkeypatch.setattr(smoke_frozen_runtime.sys, "stdin", io.StringIO(""))
    monkeypatch.setattr(smoke_frozen_runtime.os, "getpid", lambda: 1234)
    pid_path = tmp_path / "serve.pid"

    assert smoke_frozen_runtime._run_launcher(
        tmp_path / "QuantMaster.exe",
        tmp_path / "serve.stdout.log",
        tmp_path / "serve.stderr.log",
        pid_path,
    ) == 0

    assert pid_path.read_text(encoding="ascii") == "4321"
    assert calls[0][1]["env"]["QM_LAUNCHER_PID"] == "1234"
    assert calls[0][1]["stdin"] is smoke_frozen_runtime.subprocess.DEVNULL


def test_frozen_teardown_rejects_web_process_that_keeps_log_open(monkeypatch):
    alive = {11: False, 22: True, 33: False}
    monkeypatch.setattr(smoke_frozen_runtime, "_pid_alive", alive.__getitem__)

    with pytest.raises(RuntimeError, match="web 22"):
        smoke_frozen_runtime._wait_stopped(
            {"bootloader": 11, "web": 22, "runtime-worker": 33}, timeout=0,
        )


def test_windows_package_and_release_workflows_run_the_frozen_smoke():
    root = Path(__file__).parents[1]
    ci = (root / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    release = (root / ".github/workflows/release.yml").read_text(encoding="utf-8")

    assert "windows-package:" in ci
    assert "scripts/release/smoke_frozen_runtime.py" in ci
    assert "scripts/release/smoke_frozen_runtime.py" in release
    assert "if: runner.os == 'Windows'" in release
