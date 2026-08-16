import io
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.release import smoke_frozen_runtime
from scripts.release.smoke_frozen_runtime import (
    _ONEDIR_CORE_READY_BUDGET_SECONDS,
    _assert_same_identity,
    _median_of,
    _run_deep_doctor,
    _run_help,
    _run_help_measure,
    _sample_summary,
    _wait_stopped,
    measure_help,
    smoke_onedir,
)


def test_frozen_runtime_smoke_requires_one_exact_application_identity():
    identity = {
        "build_sha": "a" * 40,
        "slot_id": "a" * 40,
        "runtime_generation": "b" * 32,
    }

    _assert_same_identity(identity, {**identity, "pid": 2}, {**identity, "pid": 3})

    with pytest.raises(RuntimeError, match="runtime_generation"):
        _assert_same_identity(
            identity,
            {**identity, "runtime_generation": "c" * 32},
            identity,
        )

    with pytest.raises(RuntimeError, match="build_sha"):
        _assert_same_identity({**identity, "build_sha": "source"}, identity)

    with pytest.raises(RuntimeError, match="slot_id"):
        _assert_same_identity({**identity, "slot_id": "slot-a"}, identity)


def test_frozen_smoke_uses_installed_default_paths_without_runtime_overrides(
    tmp_path, monkeypatch,
):
    for name in ("QM_CONFIG_PATH", "QM_DATA_ROOT", "QM_FREE_STOCKDB_ROOT"):
        monkeypatch.setenv(name, "polluted-by-caller")

    environment, instance = smoke_frozen_runtime._isolated_environment(tmp_path, 18686)

    assert instance == tmp_path / "instance"
    assert environment["APPDATA"] == str(tmp_path / "appdata")
    assert environment["LOCALAPPDATA"] == str(tmp_path / "localappdata")
    assert all(
        name not in environment
        for name in ("QM_CONFIG_PATH", "QM_DATA_ROOT", "QM_FREE_STOCKDB_ROOT")
    )
    config_path = tmp_path / "appdata" / "QuantMaster" / "config.yaml"
    document = json.loads(config_path.read_text(encoding="utf-8"))
    assert "root" not in document["data"]
    assert "free_stockdb_root" not in document["data"]


def test_internal_launcher_exits_on_eof_without_signaling_child(tmp_path, monkeypatch):
    calls = []

    class FrozenProcess:
        pid = 4321

        def send_signal(self, _signal):
            raise AssertionError("launcher exit must be the shutdown signal")

        def kill(self):
            raise AssertionError("successful launcher exit must not kill the child")

    monkeypatch.setattr(
        smoke_frozen_runtime.subprocess,
        "Popen",
        lambda command, **kwargs: calls.append((command, kwargs)) or FrozenProcess(),
    )
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


def test_frozen_teardown_rejects_a_surviving_process(monkeypatch):
    alive = {11: False, 22: True, 33: False}
    monkeypatch.setattr(smoke_frozen_runtime, "_pid_alive", alive.__getitem__)

    with pytest.raises(RuntimeError, match="web 22"):
        _wait_stopped({"bootloader": 11, "web": 22, "runtime-worker": 33}, timeout=0)


def test_frozen_onefile_help_allows_twenty_seconds_and_reports_latency(tmp_path, monkeypatch):
    monkeypatch.setattr(
        smoke_frozen_runtime.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=0,
            stdout="usage: qm [-h]",
            stderr="",
        ),
    )
    ticks = iter([0.0, 19.2])
    monkeypatch.setattr(smoke_frozen_runtime.time, "monotonic", lambda: next(ticks))

    assert _run_help(tmp_path / "QuantMaster.exe", {}, layout="onefile") == 19.2


@pytest.mark.parametrize(
    ("layout", "elapsed", "budget"),
    (("onefile", 20.1, 20.0), ("onedir", 1.6, 1.5)),
)
def test_frozen_help_hard_fails_above_layout_budget(
    tmp_path, monkeypatch, layout, elapsed, budget,
):
    monkeypatch.setattr(
        smoke_frozen_runtime.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=0, stdout="usage: qm [-h]", stderr="",
        ),
    )
    ticks = iter([0.0, elapsed])
    monkeypatch.setattr(smoke_frozen_runtime.time, "monotonic", lambda: next(ticks))

    with pytest.raises(
        RuntimeError,
        match=rf"{layout} help took {elapsed:.3f}s; {budget:.1f} second budget",
    ):
        _run_help(tmp_path / "QuantMaster.exe", {}, layout=layout)


def test_frozen_help_measurement_reports_layout_latency_and_budget(tmp_path, monkeypatch):
    executable = tmp_path / "QuantMaster.exe"
    executable.write_bytes(b"frozen")
    monkeypatch.setattr(smoke_frozen_runtime, "_free_port", lambda: 18686)
    monkeypatch.setattr(
        smoke_frozen_runtime, "_isolated_environment", lambda root, _port: ({}, root),
    )
    monkeypatch.setattr(
        smoke_frozen_runtime, "_run_help",
        lambda _executable, _environment, *, layout: 1.2,
    )

    assert measure_help(executable, layout="onedir") == {
        "layout": "onedir",
        "help_seconds": 1.2,
        "help_budget_seconds": 1.5,
    }


def test_frozen_doctor_uses_utf8_wire_encoding(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(
        smoke_frozen_runtime.subprocess,
        "run",
        lambda command, **kwargs: (
            calls.append((command, kwargs))
            or SimpleNamespace(returncode=0, stdout="{}", stderr="")
        ),
    )
    environment = {"PYTHONIOENCODING": "cp1252"}

    _run_deep_doctor(tmp_path / "QuantMaster.exe", environment)

    assert environment == {"PYTHONIOENCODING": "cp1252"}
    assert calls[0][1]["env"]["PYTHONIOENCODING"] == "utf-8"
    assert calls[0][1]["encoding"] == "utf-8"
    assert calls[0][1]["errors"] == "replace"


def test_frozen_smoke_reads_core_readiness_from_health_not_diagnostics(
    tmp_path, monkeypatch,
):
    identity = {
        "build_sha": "a" * 40,
        "slot_id": "a" * 40,
        "runtime_generation": "b" * 32,
    }
    executable = tmp_path / "QuantMaster.exe"
    executable.write_bytes(b"frozen")
    events = []
    urls = []

    class Launcher:
        def __init__(self):
            self.stdin = io.StringIO()
            self.returncode = None

        def wait(self, timeout):
            self.returncode = 0

        def poll(self):
            return self.returncode

        def kill(self):
            self.returncode = -1

    def isolated(root, _port):
        instance = root / "instance"
        instance.mkdir()
        return {}, instance

    def start(_executable, _environment, stdout_path, stderr_path, _pid_path):
        stdout_path.write_text("", encoding="utf-8")
        stderr_path.write_text("", encoding="utf-8")
        return Launcher(), 10

    def wait_json(url, _ready, **_kwargs):
        events.append(f"request:{url.rsplit('/', 1)[-1]}")
        urls.append(url)
        if url.endswith("/health"):
            return {"status": "ok", "core_ready": True, "process_pid": 20, **identity}
        if url.endswith("/settings/runtime"):
            return {"worker": {"available": True, "pid": 30, **identity}}
        raise AssertionError("frozen smoke must not wait for full diagnostics")

    report = json.dumps({"metrics": {"application_identity_probe": identity}})
    monkeypatch.setattr(smoke_frozen_runtime, "os", SimpleNamespace(name="nt"))
    monkeypatch.setattr(smoke_frozen_runtime, "_free_port", lambda: 18686)
    monkeypatch.setattr(smoke_frozen_runtime, "_isolated_environment", isolated)
    monkeypatch.setattr(
        smoke_frozen_runtime, "_run_deep_doctor",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=0, stdout=report, stderr=""),
    )
    monkeypatch.setattr(
        smoke_frozen_runtime, "_run_help", lambda *_args, **_kwargs: 4.2,
    )
    monkeypatch.setattr(smoke_frozen_runtime, "_start_launcher", start)
    monkeypatch.setattr(
        smoke_frozen_runtime,
        "_wait_splash_window",
        lambda pid: events.append(f"splash-visible:{pid}") or 99,
    )
    monkeypatch.setattr(
        smoke_frozen_runtime,
        "_wait_splash_closed",
        lambda handle: events.append(f"splash-closed:{handle}"),
    )
    monkeypatch.setattr(smoke_frozen_runtime, "_wait_json", wait_json)
    monkeypatch.setattr(smoke_frozen_runtime, "_wait_stopped", lambda *_args: None)
    monkeypatch.setattr(
        smoke_frozen_runtime.socket, "create_connection",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError()),
    )

    evidence = smoke_frozen_runtime.smoke(executable)

    assert evidence["layout"] == "onefile"
    assert evidence["help_seconds"] == 4.2
    assert evidence["help_budget_seconds"] == 20.0
    assert evidence["core_ready_seconds"] <= 20.0
    assert evidence["splash_visible_before_core_ready"] is True
    assert evidence["splash_closed_after_listener_and_core_ready"] is True
    assert events[:3] == [
        "splash-visible:10",
        "request:health",
        "splash-closed:99",
    ]
    assert urls == [
        "http://127.0.0.1:18686/api/v1/health",
        "http://127.0.0.1:18686/api/v1/settings/runtime",
    ]


def test_frozen_onedir_smoke_skips_splash_and_preserves_the_slot(
    tmp_path, monkeypatch,
):
    identity = {
        "build_sha": "a" * 40,
        "slot_id": "a" * 40,
        "runtime_generation": "b" * 32,
    }
    application = tmp_path / "QuantMaster"
    application.mkdir()
    executable = application / "QuantMaster.exe"
    executable.write_bytes(b"frozen")

    class Launcher:
        def __init__(self):
            self.stdin = io.StringIO()
            self.returncode = None

        def wait(self, timeout):
            self.returncode = 0

        def poll(self):
            return self.returncode

        def kill(self):
            self.returncode = -1

    def isolated(root, _port):
        instance = root / "instance"
        instance.mkdir()
        return {}, instance

    def start(_executable, _environment, stdout_path, stderr_path, _pid_path):
        stdout_path.write_text("", encoding="utf-8")
        stderr_path.write_text("", encoding="utf-8")
        return Launcher(), 10

    def wait_json(url, _ready, **_kwargs):
        if url.endswith("/health"):
            return {"status": "ok", "core_ready": True, "process_pid": 20, **identity}
        if url.endswith("/settings/runtime"):
            return {"worker": {"available": True, "pid": 30, **identity}}
        raise AssertionError(url)

    report = json.dumps({"metrics": {"application_identity_probe": identity}})
    monkeypatch.setattr(smoke_frozen_runtime, "os", SimpleNamespace(name="nt"))
    monkeypatch.setattr(smoke_frozen_runtime, "_free_port", lambda: 18686)
    monkeypatch.setattr(smoke_frozen_runtime, "_isolated_environment", isolated)
    monkeypatch.setattr(
        smoke_frozen_runtime, "_run_deep_doctor",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=0, stdout=report, stderr=""),
    )
    monkeypatch.setattr(smoke_frozen_runtime, "_run_help", lambda *_args, **_kwargs: 0.2)
    monkeypatch.setattr(smoke_frozen_runtime, "_start_launcher", start)
    monkeypatch.setattr(
        smoke_frozen_runtime, "_wait_splash_window",
        lambda *_args, **_kwargs: pytest.fail("onedir must not wait for a splash"),
    )
    monkeypatch.setattr(smoke_frozen_runtime, "_wait_json", wait_json)
    monkeypatch.setattr(smoke_frozen_runtime, "_wait_stopped", lambda *_args: None)
    monkeypatch.setattr(
        smoke_frozen_runtime.socket, "create_connection",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError()),
    )

    evidence = smoke_frozen_runtime.smoke(executable, layout="onedir")

    assert evidence["layout"] == "onedir"
    assert evidence["help_budget_seconds"] == 1.5
    assert evidence["splash_visible_before_core_ready"] is False
    assert evidence["splash_closed_after_listener_and_core_ready"] is False
    assert evidence["executable_unchanged"] is True


def test_splash_window_waits_for_one_visible_handle_then_for_that_handle_to_close(
    monkeypatch,
):
    visible = iter(([], [], [77]))
    alive = iter((True, False))
    monkeypatch.setattr(
        smoke_frozen_runtime, "_visible_process_windows", lambda _pid: next(visible),
    )
    monkeypatch.setattr(smoke_frozen_runtime, "_window_visible", lambda _handle: next(alive))
    monkeypatch.setattr(smoke_frozen_runtime.time, "sleep", lambda _seconds: None)

    assert smoke_frozen_runtime._wait_splash_window(12, timeout=1.0) == 77
    smoke_frozen_runtime._wait_splash_closed(77, timeout=1.0)


def test_onedir_help_measure_times_one_run_without_enforcing_the_budget(
    tmp_path, monkeypatch,
):
    monkeypatch.setattr(
        smoke_frozen_runtime.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(
            returncode=0, stdout="usage: qm [-h]", stderr="",
        ),
    )
    ticks = iter([0.0, 1.8])
    monkeypatch.setattr(smoke_frozen_runtime.time, "monotonic", lambda: next(ticks))

    # 1.8s exceeds the onedir 1.5s budget, but the raw measurer must not gate.
    assert _run_help_measure(tmp_path / "QuantMaster.exe", {}, layout="onedir") == 1.8


def test_onedir_sample_summary_reports_cold_and_median():
    assert _median_of([4.2, 4.0, 4.1]) == 4.1

    assert _sample_summary([1.6, 1.0, 1.0]) == {
        "samples": [1.6, 1.0, 1.0],
        "cold_seconds": 1.6,
        "median_seconds": 1.0,
    }


def _fake_onedir_evidence(help_vals, core_vals):
    return {
        "mode": "onedir-measurement",
        "layout": "onedir",
        "build_sha": "a" * 40,
        "help": {
            "budget_seconds": 1.5,
            **_sample_summary(help_vals),
            "within_budget": _sample_summary(help_vals)["cold_seconds"] <= 1.5,
            "median_within_budget": _sample_summary(help_vals)["median_seconds"] <= 1.5,
        },
        "core_ready": {
            "budget_seconds": _ONEDIR_CORE_READY_BUDGET_SECONDS,
            **_sample_summary(core_vals),
            "within_budget": _sample_summary(core_vals)["cold_seconds"] <= _ONEDIR_CORE_READY_BUDGET_SECONDS,
            "median_within_budget": (
                _sample_summary(core_vals)["median_seconds"]
                <= _ONEDIR_CORE_READY_BUDGET_SECONDS,
            ),
        },
        "within_budgets": bool(
            _sample_summary(help_vals)["cold_seconds"] <= 1.5
            and _sample_summary(core_vals)["cold_seconds"] <= _ONEDIR_CORE_READY_BUDGET_SECONDS
        ),
        "limit_failures": [],
        "errors": [],
    }


def _doctor_stdout():
    return json.dumps({"metrics": {"application_identity_probe":
        {"build_sha": "a" * 40, "slot_id": "a" * 40,
         "runtime_generation": "b"},}})


def test_onedir_smoke_reports_help_and_core_ready_within_budgets(
    tmp_path, monkeypatch,
):
    executable = tmp_path / "QuantMaster.exe"
    executable.write_bytes(b"frozen")
    help_vals = [1.2, 1.0, 0.95]
    core_vals = [4.2, 4.0, 4.1]

    def help_m(_exe, _env, *, layout):
        assert layout == "onedir"
        return help_vals.pop(0)

    def core_m(_exe, _env, _inst, _port):
        return core_vals.pop(0)

    monkeypatch.setattr(smoke_frozen_runtime, "os", SimpleNamespace(name="nt"))
    monkeypatch.setattr(smoke_frozen_runtime, "_free_port", lambda: 18686)
    monkeypatch.setattr(
        smoke_frozen_runtime, "_isolated_environment",
        lambda root, _port: ({}, root / "instance"),
    )
    monkeypatch.setattr(
        smoke_frozen_runtime, "_run_deep_doctor",
        lambda *_a, **_kw: SimpleNamespace(returncode=0, stdout=_doctor_stdout(), stderr=""),
    )
    monkeypatch.setattr(smoke_frozen_runtime, "_run_help_measure", help_m)
    monkeypatch.setattr(smoke_frozen_runtime, "_measure_core_ready_once", core_m)

    evidence = smoke_onedir(executable)

    assert evidence["layout"] == "onedir"
    assert evidence["build_sha"] == "a" * 40
    assert evidence["help"]["samples"] == [1.2, 1.0, 0.95]
    assert evidence["help"]["cold_seconds"] == 1.2
    assert evidence["help"]["median_seconds"] == 1.0
    assert evidence["help"]["within_budget"] is True
    assert evidence["help"]["median_within_budget"] is True
    assert evidence["core_ready"]["samples"] == [4.2, 4.0, 4.1]
    assert evidence["core_ready"]["cold_seconds"] == 4.2
    assert evidence["core_ready"]["median_seconds"] == 4.1
    assert evidence["core_ready"]["within_budget"] is True
    assert evidence["core_ready"]["median_within_budget"] is True
    assert evidence["within_budgets"] is True
    assert evidence["limit_failures"] == []


def test_onedir_smoke_gates_only_cold_help_and_records_partial_samples(
    tmp_path, monkeypatch,
):
    executable = tmp_path / "QuantMaster.exe"
    executable.write_bytes(b"frozen")
    help_vals = [1.6, 1.0, 1.0]
    core_vals = [4.2, 4.0, 4.0]

    monkeypatch.setattr(smoke_frozen_runtime, "os", SimpleNamespace(name="nt"))
    monkeypatch.setattr(smoke_frozen_runtime, "_free_port", lambda: 18686)
    monkeypatch.setattr(
        smoke_frozen_runtime, "_isolated_environment",
        lambda root, _port: ({}, root / "instance"),
    )
    monkeypatch.setattr(
        smoke_frozen_runtime, "_run_deep_doctor",
        lambda *_a, **_kw: SimpleNamespace(returncode=0, stdout=_doctor_stdout(), stderr=""),
    )
    monkeypatch.setattr(
        smoke_frozen_runtime, "_run_help_measure",
        lambda _exe, _env, *, layout: help_vals.pop(0),
    )
    monkeypatch.setattr(
        smoke_frozen_runtime, "_measure_core_ready_once",
        lambda _exe, _env, _inst, _port: core_vals.pop(0),
    )

    evidence = smoke_onedir(executable)

    assert evidence["help"]["cold_seconds"] == 1.6
    assert evidence["help"]["median_seconds"] == 1.0
    assert evidence["help"]["within_budget"] is False
    assert evidence["help"]["median_within_budget"] is True
    assert evidence["within_budgets"] is False
    assert any("help cold 1.600s" in f for f in evidence["limit_failures"])


def test_onedir_smoke_hard_fails_when_cold_core_ready_exceeds_five_seconds(
    tmp_path, monkeypatch,
):
    executable = tmp_path / "QuantMaster.exe"
    executable.write_bytes(b"frozen")

    monkeypatch.setattr(smoke_frozen_runtime, "os", SimpleNamespace(name="nt"))
    monkeypatch.setattr(smoke_frozen_runtime, "_free_port", lambda: 18686)
    monkeypatch.setattr(
        smoke_frozen_runtime, "_isolated_environment",
        lambda root, _port: ({}, root / "instance"),
    )
    monkeypatch.setattr(
        smoke_frozen_runtime, "_run_deep_doctor",
        lambda *_a, **_kw: SimpleNamespace(returncode=0, stdout=_doctor_stdout(), stderr=""),
    )
    monkeypatch.setattr(
        smoke_frozen_runtime, "_run_help_measure",
        lambda _exe, _env, *, layout: 1.2,
    )
    monkeypatch.setattr(
        smoke_frozen_runtime, "_measure_core_ready_once",
        lambda _exe, _env, _inst, _port: 5.3,
    )

    evidence = smoke_onedir(executable)

    assert evidence["core_ready"]["cold_seconds"] == 5.3
    assert evidence["core_ready"]["within_budget"] is False
    assert evidence["within_budgets"] is False
    assert any("core_ready cold 5.300s" in f for f in evidence["limit_failures"])


def test_onedir_smoke_keeps_writable_state_under_the_given_instance_root(
    tmp_path, monkeypatch,
):
    executable = tmp_path / "QuantMaster.exe"
    executable.write_bytes(b"frozen")
    inst_root = tmp_path / "instance-root"

    seen_roots: list[Path] = []

    def capture_isolated(root, _port):
        seen_roots.append(root)
        instance = root / "instance"
        instance.mkdir()
        return {}, instance

    monkeypatch.setattr(smoke_frozen_runtime, "os", SimpleNamespace(name="nt"))
    monkeypatch.setattr(smoke_frozen_runtime, "_free_port", lambda: 18686)
    monkeypatch.setattr(smoke_frozen_runtime, "_isolated_environment", capture_isolated)
    monkeypatch.setattr(
        smoke_frozen_runtime, "_run_deep_doctor",
        lambda *_a, **_kw: SimpleNamespace(returncode=0, stdout=_doctor_stdout(), stderr=""),
    )
    monkeypatch.setattr(
        smoke_frozen_runtime, "_run_help_measure",
        lambda _exe, _env, *, layout: 1.2,
    )
    monkeypatch.setattr(
        smoke_frozen_runtime, "_measure_core_ready_once",
        lambda _exe, _env, _inst, _port: 4.0,
    )

    smoke_onedir(executable, instance_root=inst_root)

    assert all(root == inst_root for root in seen_roots)


def test_onedir_smoke_cli_writes_evidence_and_exits_one_on_budget_failure(
    tmp_path, monkeypatch,
):
    executable = tmp_path / "QuantMaster.exe"
    executable.write_bytes(b"frozen")
    evidence_path = tmp_path / "startup-budgets.json"

    monkeypatch.setattr(
        smoke_frozen_runtime, "smoke_onedir",
        lambda exe, *, instance_root=None: _fake_onedir_evidence(
            [1.6, 1.0, 1.0], [4.2, 4.0, 4.1],
        ),
    )
    monkeypatch.setattr(
        smoke_frozen_runtime.sys,
        "argv",
        [
            "smoke_frozen_runtime.py", str(executable),
            "--onedir-smoke", "--evidence", str(evidence_path),
        ],
    )

    assert smoke_frozen_runtime.main() == 1
    payload = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert payload["within_budgets"] is False
    assert payload["help"]["cold_seconds"] == 1.6
    assert payload["core_ready"]["within_budget"] is True
