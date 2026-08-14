"""Windows smoke for one frozen QuantMaster application process tree."""

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

_IDENTITY_FIELDS = ("build_sha", "slot_id", "runtime_generation")


def _assert_same_identity(*members: dict[str, Any]) -> None:
    expected = {name: str(members[0].get(name) or "") for name in _IDENTITY_FIELDS}
    for name, value in expected.items():
        if not value:
            raise RuntimeError(f"frozen runtime omitted {name}")
    for member in members[1:]:
        for name, value in expected.items():
            if str(member.get(name) or "") != value:
                raise RuntimeError(f"frozen runtime identity mismatch: {name}")


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
        wintypes.HANDLE, wintypes.DWORD, wintypes.LPWSTR, ctypes.POINTER(wintypes.DWORD),
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
    """Start the frozen tree and stay alive until the smoke parent closes stdin."""
    environment = os.environ.copy()
    environment["QM_LAUNCHER_PID"] = str(os.getpid())
    with stdout_path.open("w", encoding="utf-8") as stdout, stderr_path.open(
        "w", encoding="utf-8",
    ) as stderr:
        server = subprocess.Popen(
            [str(executable), "serve", "--no-reload"],
            env=environment,
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


def smoke(executable: Path) -> None:
    if os.name != "nt":
        raise RuntimeError("frozen runtime smoke requires Windows")
    executable = executable.resolve()
    if not executable.is_file():
        raise FileNotFoundError(executable)

    with tempfile.TemporaryDirectory(prefix="quantmaster-frozen-smoke-") as raw_temp:
        root = Path(raw_temp)
        port = _free_port()
        config_path = root / "config.yaml"
        data_root = root / "data"
        stockdb_root = root / "runtime" / "free-stockdb"
        config_path.write_text(json.dumps({
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
        }), encoding="utf-8")
        environment = os.environ.copy()
        environment.update({
            "APPDATA": str(root / "appdata"),
            "LOCALAPPDATA": str(root / "localappdata"),
            "QM_CONFIG_PATH": str(config_path),
            "QM_DATA_ROOT": str(data_root),
            "QM_FREE_STOCKDB_ROOT": str(stockdb_root),
            "QM_FREE_STOCKDB_MANAGED": "false",
            "QM_FREE_STOCKDB_AUTO_UPDATE": "false",
            "QM_FREE_STOCKDB_ONLINE_ENABLED": "false",
            "QM_AKSHARE_ENABLED": "false",
            "QM_TUSHARE_ENABLED": "false",
            "QM_YFINANCE_ENABLED": "false",
            "QM_AUTOMATION_ENABLED": "false",
            "QM_LAB_ENABLED": "false",
        })
        for name in ("QM_BUILD_SHA", "QM_SLOT_ID", "QM_RUNTIME_GENERATION"):
            environment.pop(name, None)

        stdout_path = root / "serve.stdout.log"
        stderr_path = root / "serve.stderr.log"
        pid_path = root / "serve.pid"
        pids: dict[str, int] = {}
        launcher: subprocess.Popen[Any] | None = None
        try:
            launcher, pids["bootloader"] = _start_launcher(
                executable, environment, stdout_path, stderr_path, pid_path,
            )
            try:
                base_url = f"http://127.0.0.1:{port}/api/v1"
                health = _wait_json(
                    f"{base_url}/health",
                    lambda value: value.get("status") == "ok",
                )
                pids["web"] = int(health["process_pid"])
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
                doctor = subprocess.run(
                    [str(executable), "doctor", "--deep"],
                    env=doctor_env,
                    capture_output=True,
                    text=True,
                    timeout=90.0,
                    check=False,
                )
                if doctor.returncode:
                    raise RuntimeError(
                        f"frozen deep doctor failed ({doctor.returncode}): {doctor.stderr[-2000:]}"
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
                detail = stderr_path.read_text(encoding="utf-8", errors="replace")[-4000:]
                cleanup = "\n".join(cleanup_errors)
                raise RuntimeError(
                    f"{exc}\n--- frozen server stderr ---\n{detail}"
                    + (f"\n--- cleanup errors ---\n{cleanup}" if cleanup else "")
                ) from exc
        finally:
            if launcher is not None and launcher.poll() is None:
                launcher.kill()
                launcher.wait(timeout=5.0)


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "--internal-launcher":
        if len(sys.argv) != 6:
            raise RuntimeError("internal frozen launcher requires four paths")
        return _run_launcher(*(Path(value) for value in sys.argv[2:]))
    parser = argparse.ArgumentParser()
    parser.add_argument("executable", type=Path)
    args = parser.parse_args()
    smoke(args.executable)
    print("Frozen Windows runtime identity smoke passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
