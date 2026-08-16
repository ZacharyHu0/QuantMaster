"""Smoke the default frozen Windows onefile executable as an isolated instance."""

from __future__ import annotations

import argparse
import ctypes
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from ctypes import wintypes
from pathlib import Path
from typing import Any

from quantmaster.logging_config import redact_public_text

_IDENTITY_FIELDS = ("build_sha", "slot_id", "runtime_generation")
_HELP_MAX_SECONDS = {"onefile": 20.0, "onedir": 1.5}
_CORE_READY_MAX_SECONDS = 20.0
_ONEDIR_CORE_READY_BUDGET_SECONDS = 5.0
# Documented sampling: one true-cold sample (the gated value) plus a fixed
# number of warm repeats whose median is reported for trend evidence.
_WARM_HELP_SAMPLES = 2
_WARM_CORE_READY_SAMPLES = 2


def _assert_same_identity(*members: dict[str, Any]) -> None:
    expected = {name: str(members[0].get(name) or "") for name in _IDENTITY_FIELDS}
    build_sha = expected["build_sha"]
    if len(build_sha) != 40 or any(
        character not in "0123456789abcdef" for character in build_sha
    ):
        raise RuntimeError("frozen runtime identity mismatch: build_sha")
    if expected["slot_id"] != build_sha:
        raise RuntimeError("frozen runtime identity mismatch: slot_id")
    if not expected["runtime_generation"]:
        raise RuntimeError("frozen runtime omitted runtime_generation")
    for member in members[1:]:
        for name, value in expected.items():
            if str(member.get(name) or "") != value:
                raise RuntimeError(f"frozen runtime identity mismatch: {name}")


def _run_help(
    executable: Path, environment: dict[str, str], *, layout: str,
) -> float:
    max_seconds = _HELP_MAX_SECONDS[layout]
    started = time.monotonic()
    result = subprocess.run(
        [str(executable), "--help"],
        cwd=executable.parent,
        env={**environment, "PYTHONIOENCODING": "utf-8"},
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=25.0,
        check=False,
    )
    elapsed = time.monotonic() - started
    if result.returncode:
        raise RuntimeError(f"frozen help failed ({result.returncode}): {result.stderr[-2000:]}")
    if "usage: qm" not in result.stdout:
        raise RuntimeError("frozen help omitted the QuantMaster usage marker")
    if elapsed > max_seconds:
        raise RuntimeError(
            f"frozen {layout} help took {elapsed:.3f}s; {max_seconds:.1f} second budget"
        )
    return elapsed


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _get_json(url: str) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=1.0) as response:
        value = json.loads(response.read().decode("utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError(f"frozen runtime returned non-object JSON: {url}")
    return value


def _wait_json(
    url: str,
    ready: Callable[[dict[str, Any]], bool],
    *,
    timeout: float = 60.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_error = "no response"
    while time.monotonic() < deadline:
        try:
            value = _get_json(url)
            if ready(value):
                return value
            last_error = json.dumps(value, ensure_ascii=False)[:1000]
        except (OSError, RuntimeError, ValueError, urllib.error.URLError) as exc:
            last_error = str(exc)
        time.sleep(0.2)
    raise RuntimeError(f"frozen runtime did not become ready: {last_error}")


def _visible_process_windows(pid: int) -> list[int]:
    """Return visible top-level windows owned by one exact Windows process."""
    if os.name != "nt":
        return []
    user32 = ctypes.WinDLL("user32", use_last_error=True)  # type: ignore[attr-defined]
    user32.IsWindowVisible.argtypes = [wintypes.HWND]
    user32.IsWindowVisible.restype = wintypes.BOOL
    user32.GetWindowThreadProcessId.argtypes = [wintypes.HWND, ctypes.POINTER(wintypes.DWORD)]
    user32.GetWindowThreadProcessId.restype = wintypes.DWORD
    handles: list[int] = []
    callback_type = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)
    user32.EnumWindows.argtypes = [callback_type, wintypes.LPARAM]
    user32.EnumWindows.restype = wintypes.BOOL

    def collect(handle, _parameter):
        owner = wintypes.DWORD()
        user32.GetWindowThreadProcessId(handle, ctypes.byref(owner))
        if int(owner.value) == int(pid) and user32.IsWindowVisible(handle):
            handles.append(int(handle))
        return True

    callback = callback_type(collect)
    if not user32.EnumWindows(callback, 0):
        raise OSError(ctypes.get_last_error(), "EnumWindows failed")  # type: ignore[attr-defined]
    return handles


def _window_visible(handle: int) -> bool:
    if os.name != "nt":
        return False
    user32 = ctypes.WinDLL("user32", use_last_error=True)  # type: ignore[attr-defined]
    user32.IsWindow.argtypes = [wintypes.HWND]
    user32.IsWindow.restype = wintypes.BOOL
    user32.IsWindowVisible.argtypes = [wintypes.HWND]
    user32.IsWindowVisible.restype = wintypes.BOOL
    return bool(user32.IsWindow(handle) and user32.IsWindowVisible(handle))


def _wait_splash_window(pid: int, *, timeout: float = 10.0) -> int:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        handles = _visible_process_windows(pid)
        if handles:
            return handles[0]
        time.sleep(0.05)
    raise RuntimeError(f"frozen splash was not visible for bootloader {pid}")


def _wait_splash_closed(handle: int, *, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _window_visible(handle):
            return
        time.sleep(0.05)
    raise RuntimeError(f"frozen splash window {handle} remained visible after core_ready")


def _pid_alive(pid: int) -> bool:
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)  # type: ignore[attr-defined]
    kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel32.WaitForSingleObject.restype = wintypes.DWORD
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    handle = kernel32.OpenProcess(0x00100000, False, int(pid))
    if not handle:
        return False
    try:
        return int(kernel32.WaitForSingleObject(handle, 0)) == 0x00000102
    finally:
        kernel32.CloseHandle(handle)


def _wait_stopped(pids: dict[str, int], *, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while True:
        survivors = [f"{role} {pid}" for role, pid in pids.items() if _pid_alive(pid)]
        if not survivors:
            return
        if time.monotonic() >= deadline:
            raise RuntimeError(
                "frozen application processes outlived launcher: " + ", ".join(survivors)
            )
        time.sleep(0.1)


def _terminate_exact_process(pid: int, executable: Path) -> None:
    """Best-effort failure cleanup, guarded by the exact packaged image path."""
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)  # type: ignore[attr-defined]
    kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.QueryFullProcessImageNameW.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        wintypes.LPWSTR,
        ctypes.POINTER(wintypes.DWORD),
    ]
    kernel32.QueryFullProcessImageNameW.restype = wintypes.BOOL
    kernel32.TerminateProcess.argtypes = [wintypes.HANDLE, wintypes.UINT]
    kernel32.TerminateProcess.restype = wintypes.BOOL
    kernel32.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel32.WaitForSingleObject.restype = wintypes.DWORD
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    handle = kernel32.OpenProcess(0x101001, False, int(pid))
    if not handle:
        return
    try:
        size = wintypes.DWORD(32768)
        buffer = ctypes.create_unicode_buffer(size.value)
        if not kernel32.QueryFullProcessImageNameW(handle, 0, buffer, ctypes.byref(size)):
            raise RuntimeError(f"cannot verify frozen cleanup process {pid}")
        if os.path.normcase(buffer.value) != os.path.normcase(str(executable)):
            raise RuntimeError(f"refusing to terminate non-package process {pid}: {buffer.value}")
        if not kernel32.TerminateProcess(handle, 1):
            raise RuntimeError(f"cannot terminate frozen cleanup process {pid}")
        if kernel32.WaitForSingleObject(handle, 5000) != 0:
            raise RuntimeError(f"frozen cleanup process {pid} did not stop")
    finally:
        kernel32.CloseHandle(handle)


def _run_launcher(
    executable: Path,
    stdout_path: Path,
    stderr_path: Path,
    pid_path: Path,
) -> int:
    """Start the frozen application and exit when the smoke parent closes stdin."""
    environment = os.environ.copy()
    environment["QM_LAUNCHER_PID"] = str(os.getpid())
    with stdout_path.open("w", encoding="utf-8") as stdout, stderr_path.open(
        "w", encoding="utf-8"
    ) as stderr:
        server = subprocess.Popen(
            [str(executable), "serve"],
            cwd=executable.parent,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
        )
        pid_path.write_text(str(server.pid), encoding="ascii")
        sys.stdin.read()
    return 0


def _start_launcher(
    executable: Path,
    environment: dict[str, str],
    stdout_path: Path,
    stderr_path: Path,
    pid_path: Path,
) -> tuple[subprocess.Popen[Any], int]:
    launcher = subprocess.Popen(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--internal-launcher",
            str(executable),
            str(stdout_path),
            str(stderr_path),
            str(pid_path),
        ],
        env=environment,
        stdin=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            try:
                return launcher, int(pid_path.read_text(encoding="ascii"))
            except (OSError, ValueError):
                if launcher.poll() is not None:
                    raise RuntimeError(
                        f"frozen launcher exited early ({launcher.returncode})"
                    ) from None
                time.sleep(0.1)
        raise RuntimeError("frozen launcher did not publish its child PID")
    except BaseException:
        if launcher.stdin is not None:
            launcher.stdin.close()
        if launcher.poll() is None:
            launcher.kill()
            launcher.wait(timeout=5.0)
        raise


def _run_deep_doctor(
    executable: Path,
    environment: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(executable), "doctor", "--deep"],
        cwd=executable.parent,
        env={**environment, "PYTHONIOENCODING": "utf-8"},
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=90.0,
        check=False,
    )


def _isolated_environment(root: Path, port: int) -> tuple[dict[str, str], Path]:
    instance = root / "instance"
    instance.mkdir()
    appdata = root / "appdata"
    localappdata = root / "localappdata"
    config_path = appdata / "QuantMaster" / "config.yaml"
    config_path.parent.mkdir(parents=True)
    config_path.write_text(
        json.dumps(
            {
                "server": {"host": "127.0.0.1", "port": port},
                "data": {
                    "free_stockdb_managed": False,
                    "free_stockdb_auto_update": False,
                    "free_stockdb_online_enabled": False,
                    "akshare_enabled": False,
                    "tushare_enabled": False,
                    "yfinance_enabled": False,
                    "after_close_enabled": False,
                    "after_close_auto_run": False,
                    "repair_enabled": False,
                },
                "automation": {"enabled": False},
                "lab": {"enabled": False},
            }
        ),
        encoding="utf-8",
    )
    environment = os.environ.copy()
    environment.pop("PYINSTALLER_SUPPRESS_SPLASH_SCREEN", None)
    for name in ("QM_CONFIG_PATH", "QM_DATA_ROOT", "QM_FREE_STOCKDB_ROOT"):
        environment.pop(name, None)
    environment.update(
        {
            "APPDATA": str(appdata),
            "LOCALAPPDATA": str(localappdata),
            "QM_FREE_STOCKDB_MANAGED": "false",
            "QM_FREE_STOCKDB_AUTO_UPDATE": "false",
            "QM_FREE_STOCKDB_ONLINE_ENABLED": "false",
            "QM_AKSHARE_ENABLED": "false",
            "QM_TUSHARE_ENABLED": "false",
            "QM_YFINANCE_ENABLED": "false",
            "QM_AUTOMATION_ENABLED": "false",
            "QM_LAB_ENABLED": "false",
        }
    )
    for name in ("QM_BUILD_SHA", "QM_SLOT_ID", "QM_RUNTIME_GENERATION"):
        environment.pop(name, None)
    environment.pop("PYTHONPATH", None)
    environment.pop("PYTHONHOME", None)
    return environment, instance


def measure_help(executable: Path, *, layout: str) -> dict[str, Any]:
    executable = executable.resolve()
    if not executable.is_file():
        raise FileNotFoundError(executable)
    with tempfile.TemporaryDirectory(prefix="quantmaster-frozen-help-") as raw_temp:
        environment, _instance = _isolated_environment(Path(raw_temp), _free_port())
        elapsed = _run_help(executable, environment, layout=layout)
    return {
        "layout": layout,
        "help_seconds": round(elapsed, 3),
        "help_budget_seconds": _HELP_MAX_SECONDS[layout],
    }


def _application_state(executable: Path, *, layout: str) -> object:
    if layout == "onefile":
        stat = executable.stat()
        return stat.st_size, stat.st_mtime_ns
    entries = []
    for path in executable.parent.rglob("*"):
        stat = path.lstat()
        entries.append(
            (
                path.relative_to(executable.parent).as_posix(),
                stat.st_mode,
                stat.st_size,
                stat.st_mtime_ns,
            )
        )
    return tuple(sorted(entries))


def _validate_layout(layout: str) -> None:
    if layout not in _HELP_MAX_SECONDS:
        raise RuntimeError(f"unsupported frozen layout: {layout}")


def _wait_splash_for_layout(pid: int, *, layout: str) -> int | None:
    if layout == "onefile":
        return _wait_splash_window(pid)
    return None


def _close_splash_for_layout(handle: int | None) -> None:
    if handle is not None:
        _wait_splash_closed(handle)


def _assert_application_unchanged(
    executable: Path, *, layout: str, initial_state: object,
) -> None:
    if _application_state(executable, layout=layout) != initial_state:
        messages = {
            "onefile": "frozen runtime modified its onefile executable",
            "onedir": "frozen runtime modified its onedir application",
        }
        raise RuntimeError(messages[layout])


def smoke(executable: Path, *, layout: str = "onefile") -> dict[str, Any]:
    if os.name != "nt":
        raise RuntimeError("frozen runtime smoke requires Windows")
    _validate_layout(layout)
    executable = executable.resolve()
    if not executable.is_file():
        raise FileNotFoundError(executable)
    initial_state = _application_state(executable, layout=layout)

    with tempfile.TemporaryDirectory(prefix="quantmaster-frozen-smoke-") as raw_temp:
        root = Path(raw_temp)
        port = _free_port()
        environment, instance = _isolated_environment(root, port)
        bootstrap = _run_deep_doctor(executable, environment)
        if bootstrap.returncode:
            raise RuntimeError(
                f"frozen schema bootstrap failed ({bootstrap.returncode}): "
                f"{bootstrap.stderr[-2000:]}"
            )
        help_seconds = _run_help(executable, environment, layout=layout)
        stdout_path = instance / "serve.stdout.log"
        stderr_path = instance / "serve.stderr.log"
        pid_path = instance / "serve.pid"
        pids: dict[str, int] = {}
        launcher: subprocess.Popen[Any] | None = None
        try:
            server_started = time.monotonic()
            launcher, pids["bootloader"] = _start_launcher(
                executable, environment, stdout_path, stderr_path, pid_path
            )
            try:
                splash_window = _wait_splash_for_layout(pids["bootloader"], layout=layout)
                base_url = f"http://127.0.0.1:{port}/api/v1"
                health = _wait_json(
                    f"{base_url}/health",
                    lambda value: value.get("status") == "ok" and value.get("core_ready") is True,
                )
                pids["web"] = int(health["process_pid"])
                _close_splash_for_layout(splash_window)
                core_ready_seconds = time.monotonic() - server_started
                if core_ready_seconds > _CORE_READY_MAX_SECONDS:
                    raise RuntimeError(
                        f"frozen core_ready took {core_ready_seconds:.3f}s; "
                        f"{_CORE_READY_MAX_SECONDS:.1f} second budget"
                    )
                runtime = _wait_json(
                    f"{base_url}/settings/runtime",
                    lambda value: bool((value.get("worker") or {}).get("available")),
                )
                worker = dict(runtime["worker"])
                pids["runtime-worker"] = int(worker["pid"])
                if pids["runtime-worker"] == pids["web"]:
                    raise RuntimeError("runtime-worker did not start in a distinct process")

                doctor_env = dict(environment)
                for name in _IDENTITY_FIELDS:
                    doctor_env[f"QM_{name.upper()}"] = str(health[name])
                doctor = _run_deep_doctor(executable, doctor_env)
                if doctor.returncode:
                    raise RuntimeError(
                        f"frozen deep doctor failed ({doctor.returncode}): "
                        f"{doctor.stderr[-2000:]}"
                    )
                report = json.loads(doctor.stdout)
                compute = dict(report["metrics"]["application_identity_probe"])
                _assert_same_identity(health, worker, compute)

                if launcher.stdin is None:
                    raise RuntimeError("frozen launcher stdin is unavailable")
                launcher.stdin.close()
                launcher.wait(timeout=5.0)
                if launcher.returncode:
                    raise RuntimeError(f"frozen launcher failed ({launcher.returncode})")
                _wait_stopped(pids)
                try:
                    socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
                except OSError:
                    pass
                else:
                    raise RuntimeError("frozen runtime port remained bound after shutdown")
                _assert_application_unchanged(
                    executable, layout=layout, initial_state=initial_state,
                )
                return {
                    "layout": layout,
                    "build_sha": str(health["build_sha"]),
                    "slot_id": str(health["slot_id"]),
                    "runtime_generation": str(health["runtime_generation"]),
                    "help_seconds": round(help_seconds, 3),
                    "help_budget_seconds": _HELP_MAX_SECONDS[layout],
                    "core_ready_seconds": round(core_ready_seconds, 3),
                    "splash_visible_before_core_ready": layout == "onefile",
                    "splash_closed_after_listener_and_core_ready": layout == "onefile",
                    "processes_stopped": True,
                    "port_released": True,
                    "executable_unchanged": True,
                }
            except BaseException as exc:
                if launcher.stdin is not None and not launcher.stdin.closed:
                    launcher.stdin.close()
                if launcher.poll() is None:
                    launcher.kill()
                    launcher.wait(timeout=5.0)
                cleanup_errors = []
                for pid in dict.fromkeys(reversed(tuple(pids.values()))):
                    try:
                        _terminate_exact_process(pid, executable)
                    except RuntimeError as cleanup_exc:
                        cleanup_errors.append(str(cleanup_exc))
                detail = redact_public_text(
                    stderr_path.read_text(encoding="utf-8", errors="replace")[-4000:]
                )
                cleanup = redact_public_text("\n".join(cleanup_errors))
                raise RuntimeError(
                    f"{exc}\n--- frozen server stderr ---\n{detail}"
                    + (f"\n--- cleanup errors ---\n{cleanup}" if cleanup else "")
                ) from exc
        finally:
            if launcher is not None and launcher.poll() is None:
                launcher.kill()
                launcher.wait(timeout=5.0)

def _run_help_measure(
    executable: Path,
    environment: dict[str, str],
    *,
    layout: str,
) -> float:
    """Time one frozen ``--help`` run without enforcing the layout budget."""

    started = time.monotonic()
    result = subprocess.run(
        [str(executable), "--help"],
        cwd=executable.parent,
        env={**environment, "PYTHONIOENCODING": "utf-8"},
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=25.0,
        check=False,
    )
    elapsed = time.monotonic() - started
    if result.returncode:
        raise RuntimeError(f"frozen help failed ({result.returncode}): {result.stderr[-2000:]}")
    if "usage: qm" not in result.stdout:
        raise RuntimeError("frozen help omitted the QuantMaster usage marker")
    return elapsed


def _median_of(values: list[float]) -> float:
    ordered = sorted(values)
    n = len(ordered)
    mid = n // 2
    return ordered[mid] if n % 2 else (ordered[mid - 1] + ordered[mid]) / 2.0


def _sample_summary(samples: list[float]) -> dict[str, object]:
    return {
        "samples": [round(value, 3) for value in samples],
        "cold_seconds": round(samples[0], 3),
        "median_seconds": round(_median_of(samples), 3),
    }


def _core_ready_config(port: int, data_root: Path, stockdb_root: Path) -> str:
    return json.dumps(
        {
            "server": {"host": "127.0.0.1", "port": port},
            "data": {
                "root": str(data_root),
                "free_stockdb_root": str(stockdb_root),
                "free_stockdb_managed": False,
                "free_stockdb_auto_update": False,
                "free_stockdb_online_enabled": False,
                "akshare_enabled": False,
                "tushare_enabled": False,
                "yfinance_enabled": False,
                "after_close_enabled": False,
                "after_close_auto_run": False,
                "repair_enabled": False,
            },
            "automation": {"enabled": False},
            "lab": {"enabled": False},
        }
    )


def _measure_core_ready_once(
    executable: Path,
    environment: dict[str, str],
    instance: Path,
    port: int,
) -> float:
    """Start the frozen server once and return seconds until lightweight core_ready."""

    data_root = instance / "data"
    stockdb_root = instance / "stockdb"
    config_path = instance / "config.yaml"
    config_path.write_text(
        _core_ready_config(port, data_root, stockdb_root), encoding="utf-8"
    )
    env = dict(environment)
    env["QM_CONFIG_PATH"] = str(config_path)
    env["QM_DATA_ROOT"] = str(data_root)
    env["QM_FREE_STOCKDB_ROOT"] = str(stockdb_root)
    stdout_path = instance / "serve.stdout.log"
    stderr_path = instance / "serve.stderr.log"
    pid_path = instance / "serve.pid"
    pids: dict[str, int] = {}
    launcher: subprocess.Popen[Any] | None = None
    try:
        started = time.monotonic()
        launcher, pids["bootloader"] = _start_launcher(
            executable, env, stdout_path, stderr_path, pid_path,
        )
        base_url = f"http://127.0.0.1:{port}/api/v1"
        _wait_json(
            f"{base_url}/health",
            lambda value: value.get("status") == "ok"
            and value.get("core_ready") is True,
            timeout=_ONEDIR_CORE_READY_BUDGET_SECONDS + 5.0,
        )
        elapsed = time.monotonic() - started
        if launcher.stdin is not None:
            launcher.stdin.close()
        launcher.wait(timeout=5.0)
        if launcher.returncode:
            raise RuntimeError(
                f"onedir core_ready launcher failed ({launcher.returncode})"
            )
        _wait_stopped(pids)
        return elapsed
    finally:
        if launcher is not None and launcher.poll() is None:
            launcher.kill()
            launcher.wait(timeout=5.0)
            for pid in dict.fromkeys(reversed(tuple(pids.values()))):
                try:
                    _terminate_exact_process(pid, executable)
                except RuntimeError:
                    pass


def smoke_onedir(
    executable: Path,
    *,
    instance_root: Path | None = None,
) -> dict[str, object]:
    """Measure installed onedir help and lightweight core_ready startup budgets.

    Sampling policy: one true-cold sample (the gated value) plus a fixed number
    of warm repeats. The warm-sample median is reported alongside the cold value
    for trend evidence. The hard gate is the cold sample. All writable state
    (config, data, StockDB, logs) stays under ``instance_root`` when provided,
    otherwise under a temporary directory.
    """

    if os.name != "nt":
        raise RuntimeError("frozen onedir runtime smoke requires Windows")
    executable = executable.resolve()
    if not executable.is_file():
        raise FileNotFoundError(executable)

    help_budget = _HELP_MAX_SECONDS["onedir"]
    core_budget = _ONEDIR_CORE_READY_BUDGET_SECONDS

    with tempfile.TemporaryDirectory(prefix="quantmaster-frozen-onedir-") as raw_root:
        root = instance_root if instance_root is not None else Path(raw_root)
        root.mkdir(parents=True, exist_ok=True)
        port = _free_port()
        environment, instance = _isolated_environment(root, port)
        instance.mkdir(exist_ok=True)

        bootstrap = _run_deep_doctor(executable, environment)
        if bootstrap.returncode:
            raise RuntimeError(
                "frozen onedir schema bootstrap failed "
                f"({bootstrap.returncode}): {bootstrap.stderr[-2000:]}"
            )
        identity = json.loads(bootstrap.stdout)
        build_sha = str(
            identity["metrics"]["application_identity_probe"]["build_sha"]
        )

        help_samples: list[float] = []
        for _ in range(1 + _WARM_HELP_SAMPLES):
            try:
                help_samples.append(
                    _run_help_measure(executable, environment, layout="onedir")
                )
            except RuntimeError:
                help_samples.append(help_budget + 1.0)
                break

        core_samples: list[float] = []
        for _ in range(1 + _WARM_CORE_READY_SAMPLES):
            core_port = _free_port()
            try:
                core_samples.append(
                    _measure_core_ready_once(
                        executable, environment, instance, core_port,
                    )
                )
            except RuntimeError:
                core_samples.append(core_budget + 1.0)
                break

        help_summary = _sample_summary(help_samples)
        core_summary = _sample_summary(core_samples)
        help_within = help_summary["cold_seconds"] <= help_budget
        core_within = core_summary["cold_seconds"] <= core_budget
        failures: list[str] = []
        if not help_within:
            failures.append(
                f"onedir help cold {help_summary['cold_seconds']:.3f}s; "
                f"{help_budget:.1f}s budget"
            )
        if not core_within:
            failures.append(
                f"onedir core_ready cold {core_summary['cold_seconds']:.3f}s; "
                f"{core_budget:.1f}s budget"
            )

        return {
            "mode": "onedir-measurement",
            "layout": "onedir",
            "build_sha": build_sha,
            "help": {
                "budget_seconds": help_budget,
                **help_summary,
                "within_budget": bool(help_within),
                "median_within_budget": bool(
                    help_summary["median_seconds"] <= help_budget
                ),
            },
            "core_ready": {
                "budget_seconds": core_budget,
                **core_summary,
                "within_budget": bool(core_within),
                "median_within_budget": bool(
                    core_summary["median_seconds"] <= core_budget
                ),
            },
            "within_budgets": bool(not failures),
            "limit_failures": failures,
            "errors": [],
        }

def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "--internal-launcher":
        if len(sys.argv) != 6:
            raise RuntimeError("internal frozen launcher requires four paths")
        return _run_launcher(*(Path(value) for value in sys.argv[2:]))
    parser = argparse.ArgumentParser()
    parser.add_argument("executable", type=Path)
    parser.add_argument("--layout", choices=sorted(_HELP_MAX_SECONDS), default="onefile")
    parser.add_argument("--help-layout", choices=sorted(_HELP_MAX_SECONDS))
    parser.add_argument(
        "--onedir-smoke",
        action="store_true",
        help="measure installed onedir help and core_ready startup budgets",
    )
    parser.add_argument(
        "--evidence", type=Path,
        help="write onedir smoke JSON evidence to this path",
    )
    parser.add_argument(
        "--instance-root", type=Path,
        help="keep onedir writable state under this artifact root",
    )
    args = parser.parse_args()
    if args.onedir_smoke and args.help_layout:
        parser.error("--onedir-smoke and --help-layout are mutually exclusive")
    if args.evidence and not args.onedir_smoke:
        parser.error("--evidence requires --onedir-smoke")
    if args.instance_root and not args.onedir_smoke:
        parser.error("--instance-root requires --onedir-smoke")
    if args.onedir_smoke:
        evidence = smoke_onedir(
            args.executable, instance_root=args.instance_root,
        )
        if args.evidence is not None:
            args.evidence.write_text(
                json.dumps(evidence, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
        if not evidence["within_budgets"]:
            print(
                "Frozen Windows onedir smoke budgets exceeded: "
                + json.dumps(evidence, sort_keys=True),
                file=sys.stderr,
            )
            return 1
        print(
            "Frozen Windows onedir smoke passed: "
            + json.dumps(evidence, sort_keys=True)
        )
        return 0
    if args.help_layout:
        evidence = measure_help(args.executable, layout=args.help_layout)
        print("Frozen Windows help passed: " + json.dumps(evidence, sort_keys=True))
        return 0
    evidence = smoke(args.executable, layout=args.layout)
    print(f"Frozen Windows {args.layout} smoke passed: " + json.dumps(evidence, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
