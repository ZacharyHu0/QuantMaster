"""Windows smoke for one frozen QuantMaster application process tree."""

from __future__ import annotations

import argparse
import ctypes
import json
import os
import signal
import socket
import subprocess
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


def _wait_stopped(server: subprocess.Popen[Any], worker_pid: int) -> None:
    deadline = time.monotonic() + 15.0
    server.wait(timeout=max(0.1, deadline - time.monotonic()))
    while time.monotonic() < deadline:
        if not _pid_alive(worker_pid):
            return
        time.sleep(0.1)
    raise RuntimeError(f"runtime-worker {worker_pid} outlived the frozen application")


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
        worker_pid = 0
        with stdout_path.open("w", encoding="utf-8") as stdout, stderr_path.open(
            "w", encoding="utf-8",
        ) as stderr:
            server = subprocess.Popen(
                [str(executable), "serve", "--no-reload"],
                env=environment,
                stdout=stdout,
                stderr=stderr,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
            )
            try:
                base_url = f"http://127.0.0.1:{port}/api/v1"
                health = _wait_json(
                    f"{base_url}/health",
                    lambda value: value.get("status") == "ok",
                )
                runtime = _wait_json(
                    f"{base_url}/settings/runtime",
                    lambda value: bool((value.get("worker") or {}).get("available")),
                )
                worker = dict(runtime["worker"])
                worker_pid = int(worker["pid"])
                if worker_pid == int(health["process_pid"]):
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

                server.send_signal(signal.CTRL_BREAK_EVENT)
                _wait_stopped(server, worker_pid)
                try:
                    socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
                except OSError:
                    pass
                else:
                    raise RuntimeError("frozen runtime port remained bound after shutdown")
            except BaseException as exc:
                if server.poll() is None:
                    server.kill()
                    server.wait(timeout=5.0)
                stderr.flush()
                detail = stderr_path.read_text(encoding="utf-8", errors="replace")[-4000:]
                raise RuntimeError(f"{exc}\n--- frozen server stderr ---\n{detail}") from exc


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("executable", type=Path)
    args = parser.parse_args()
    smoke(args.executable)
    print("Frozen Windows runtime identity smoke passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
