"""前台服务进程的安全退出与父进程守护。"""

from __future__ import annotations

import http.client
import json
import logging
import os
import socket as socket_module
import subprocess
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from quantmaster.runtime.activation import DETACHED_ACTIVATION_ENV
from quantmaster.runtime.splash import close_splash, splash_active

logger = logging.getLogger(__name__)


def _wait_for_splash_readiness(
    server: Any,
    stop_event: threading.Event,
    readiness_probe: Callable[..., dict[str, Any]],
    close: Callable[[], None],
) -> None:
    """Close only after Uvicorn listens and the lightweight core is ready."""
    while not stop_event.wait(0.05):
        try:
            core_ready = bool(
                readiness_probe(include_optional_services=False).get("core_ready")
            )
        except (OSError, RuntimeError, TypeError, ValueError):
            core_ready = False
        if bool(getattr(server, "started", False)) and core_ready:
            close()
            return


def _start_splash_readiness_watcher(
    server: Any,
    stop_event: threading.Event,
) -> threading.Thread | None:
    if not splash_active():
        return None
    from quantmaster.server.readiness import readiness_status

    watcher = threading.Thread(
        target=_wait_for_splash_readiness,
        args=(server, stop_event, readiness_status, close_splash),
        name="qm-splash-readiness",
        daemon=True,
    )
    watcher.start()
    return watcher


@dataclass(frozen=True)
class ListenProcess:
    """Best-effort, non-invasive description of the owner of a TCP listener."""

    pid: int | None = None
    name: str = ""
    executable: str = ""
    command: str = ""

    @property
    def quantmaster_role(self) -> str:
        text = " ".join((self.name, self.executable, self.command)).lower()
        if "quantmaster" not in text and "qm-web" not in text:
            return ""
        if "web" in text:
            return "web"
        if "supervisor" in text:
            return "supervisor"
        if "worker" in text:
            return "worker"
        return "web"


@dataclass(frozen=True)
class StartupPreflight:
    """The decision made before a server is allowed to bind its configured URL."""

    host: str
    port: int
    available: bool
    action: str
    message: str = ""
    process: ListenProcess | None = None
    health: dict[str, Any] | None = None


class StartupPortConflictError(RuntimeError):
    """A configured listen address belongs to another, non-reusable process."""

    def __init__(self, preflight: StartupPreflight) -> None:
        self.preflight = preflight
        super().__init__(preflight.message)


def _probe_host(host: str) -> str:
    """Choose a loopback address for a local probe of a wildcard listener."""

    value = str(host).strip()
    if value in {"", "0.0.0.0", "::", "[::]"}:
        return "127.0.0.1"
    return value


def _http_json(host: str, port: int, path: str) -> dict[str, Any] | None:
    connection = http.client.HTTPConnection(_probe_host(host), int(port), timeout=0.5)
    try:
        connection.request("GET", path)
        response = connection.getresponse()
        raw = response.read()
        if response.status != 200:
            return None
        value = json.loads(raw.decode("utf-8"))
        return value if isinstance(value, dict) else None
    except (OSError, ValueError, http.client.HTTPException):
        return None
    finally:
        connection.close()


def _listener_process(host: str, port: int) -> ListenProcess:
    """Identify a listener when the platform exposes it; never kill or alter it."""

    if os.name == "nt":
        # netstat is present on supported Windows versions and does not need
        # elevation.  Process metadata may be unavailable for protected PIDs;
        # the PID is still useful to the operator.
        try:
            output = subprocess.check_output(
                ["netstat", "-ano", "-p", "tcp"], text=True,
                encoding="utf-8", errors="replace", timeout=2,
            )
        except (OSError, subprocess.SubprocessError):
            return ListenProcess()
        target_port = str(int(port))
        pid: int | None = None
        for line in output.splitlines():
            fields = line.split()
            if len(fields) >= 5 and fields[3].upper() == "LISTENING":
                local = fields[1].rsplit(":", 1)
                if len(local) == 2 and local[1] == target_port:
                    try:
                        pid = int(fields[-1])
                    except ValueError:
                        continue
                    break
        if pid is None:
            return ListenProcess()
        try:
            query = (
                "Get-CimInstance Win32_Process -Filter 'ProcessId=" + str(pid) + "' | "
                "Select-Object ProcessId,Name,ExecutablePath,CommandLine | ConvertTo-Json -Compress"
            )
            detail = subprocess.check_output(
                ["powershell", "-NoProfile", "-Command", query], text=True,
                encoding="utf-8", errors="replace", timeout=2,
            )
            value = json.loads(detail)
            if isinstance(value, dict):
                return ListenProcess(
                    pid=pid, name=str(value.get("Name") or ""),
                    executable=str(value.get("ExecutablePath") or ""),
                    command=str(value.get("CommandLine") or ""),
                )
        except (OSError, ValueError, subprocess.SubprocessError):
            pass
        return ListenProcess(pid=pid)
    return ListenProcess()


def _port_is_available(host: str, port: int) -> bool:
    family = socket_module.AF_INET6 if ":" in str(host) else socket_module.AF_INET
    listener = socket_module.socket(family, socket_module.SOCK_STREAM)
    try:
        listener.bind((host, int(port)))
        return True
    except OSError:
        return False
    finally:
        listener.close()


def inspect_startup_address(host: str, port: int, *, version: str) -> StartupPreflight:
    """Safely classify a configured address before starting QuantMaster.

    A healthy same-version server is intentionally a reusable success.  Every
    other listener remains untouched and produces an actionable diagnostic;
    choosing another port must be an explicit operator configuration change.
    """

    if _port_is_available(host, port):
        return StartupPreflight(host, int(port), True, "start")
    live = _http_json(host, port, "/api/v1/health")
    process = _listener_process(host, port)
    health = {"health": live} if live else None
    if live and str(live.get("version") or "") == str(version):
        generation = str(live.get("generation") or "")
        message = (
            f"检测到已运行的 QuantMaster {version}（{host}:{port}）"
            f"；复用现有实例。"
        )
        if generation:
            message += f" Web 代次 {generation}。"
        return StartupPreflight(host, int(port), False, "reuse", message, process, health)
    identity = f"PID {process.pid}" if process.pid else "未能识别 PID"
    if process.name:
        identity += f"（{process.name}）"
    if process.executable:
        identity += f"，{process.executable}"
    message = (
        f"QuantMaster 无法监听 {host}:{port}：端口由 {identity} 占用。"
        "请停止或重新配置该程序后重试；QuantMaster 不会自动结束进程或改用随机端口。"
    )
    return StartupPreflight(host, int(port), False, "blocked", message, process, health)


def _process_is_alive(pid: int) -> bool:
    """不引入 psutil，跨平台检查父进程是否仍然存在。"""
    if pid <= 0:
        return False
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        synchronize = 0x00100000
        wait_timeout = 0x00000102
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.WaitForSingleObject.argtypes = (wintypes.HANDLE, wintypes.DWORD)
        kernel32.WaitForSingleObject.restype = wintypes.DWORD
        kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
        kernel32.CloseHandle.restype = wintypes.BOOL
        handle = kernel32.OpenProcess(synchronize, False, pid)
        if not handle:
            # 查询权限不足不代表进程已退出；宁可继续等待，也不要误停服务。
            return ctypes.get_last_error() == 5  # ERROR_ACCESS_DENIED
        try:
            return kernel32.WaitForSingleObject(handle, 0) == wait_timeout
        finally:
            kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def watch_parent_exit(
    parent_pid: int,
    request_shutdown: Callable[[], None],
    stop_event: threading.Event,
    poll_interval: float = 0.25,
) -> None:
    """父启动器消失时通知 Uvicorn 走正常停机流程。"""
    while not stop_event.wait(poll_interval):
        if _process_is_alive(parent_pid):
            continue
        logger.info("启动器进程 %s 已退出，正在安全停止 QuantMaster", parent_pid)
        request_shutdown()
        return


def server_parent_pid() -> int | None:
    """Return the stable-launcher PID, or no owner for an activation generation."""

    if os.environ.get(DETACHED_ACTIVATION_ENV) == "1":
        return None
    raw = os.environ.get("QM_LAUNCHER_PID")
    if raw is None:
        return os.getppid()
    try:
        pid = int(raw)
    except ValueError as exc:
        raise RuntimeError("QM_LAUNCHER_PID must be a positive integer") from exc
    if pid <= 0:
        raise RuntimeError("QM_LAUNCHER_PID must be a positive integer")
    return pid


def install_windows_console_handler(
    request_shutdown: Callable[[], None],
    shutdown_complete: threading.Event,
) -> Callable[[], None]:
    """处理关闭窗口、注销与关机事件；返回用于解除注册的函数。"""
    if os.name != "nt":
        return lambda: None

    import ctypes
    from ctypes import wintypes

    # Console close, logout, and shutdown all use the same bounded Uvicorn
    # shutdown path.
    handled_events = {0, 2, 5, 6}
    handler_type = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.DWORD)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)

    @handler_type
    def handler(control_type: int) -> bool:
        if control_type not in handled_events:
            return False
        request_shutdown()
        # Windows 关闭控制台时留给处理器的时间有限；尽量等待 lifespan 清理完成。
        shutdown_complete.wait(timeout=4.0)
        return True

    kernel32.SetConsoleCtrlHandler.argtypes = (handler_type, wintypes.BOOL)
    kernel32.SetConsoleCtrlHandler.restype = wintypes.BOOL
    if not kernel32.SetConsoleCtrlHandler(handler, True):
        logger.warning("无法注册 Windows 控制台关闭处理器")
        return lambda: None

    def unregister() -> None:
        kernel32.SetConsoleCtrlHandler(handler, False)

    return unregister


def run_uvicorn_foreground(
    app: Any,
    host: str,
    port: int,
    log_level: str = "warning",
) -> None:
    """运行 Uvicorn，并把终端、启动器与服务生命周期绑定在一起。"""
    import uvicorn

    from quantmaster import __version__
    from quantmaster.runtime.network import validate_listen_host

    host = validate_listen_host(host)
    preflight = inspect_startup_address(host, port, version=__version__)
    if preflight.action == "reuse":
        logger.info(preflight.message)
        return
    if preflight.action == "blocked":
        raise StartupPortConflictError(preflight)

    server = uvicorn.Server(uvicorn.Config(
        app, host=host, port=port, log_level=log_level,
        log_config=None, access_log=False,
    ))
    parent_pid = server_parent_pid()
    stop_watcher = threading.Event()
    shutdown_complete = threading.Event()

    def request_shutdown() -> None:
        server.should_exit = True

    watcher = None
    if parent_pid is not None:
        watcher = threading.Thread(
            target=watch_parent_exit,
            args=(parent_pid, request_shutdown, stop_watcher),
            name="qm-parent-watch",
            daemon=True,
        )
        watcher.start()
    splash_stop = threading.Event()
    splash_watcher = _start_splash_readiness_watcher(server, splash_stop)
    unregister_handler = install_windows_console_handler(request_shutdown, shutdown_complete)
    try:
        server.run()
    finally:
        splash_stop.set()
        if splash_watcher is not None:
            splash_watcher.join(timeout=1.0)
        stop_watcher.set()
        shutdown_complete.set()
        unregister_handler()
        if watcher is not None:
            watcher.join(timeout=1.0)
