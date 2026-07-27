"""前台服务进程的安全退出与父进程守护。"""

from __future__ import annotations

import logging
import os
import threading
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)


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


def install_windows_console_handler(
    request_shutdown: Callable[[], None],
    shutdown_complete: threading.Event,
) -> Callable[[], None]:
    """处理关闭窗口、注销与关机事件；返回用于解除注册的函数。"""
    if os.name != "nt":
        return lambda: None

    import ctypes
    from ctypes import wintypes

    handled_events = {2, 5, 6}  # CTRL_CLOSE_EVENT / CTRL_LOGOFF_EVENT / CTRL_SHUTDOWN_EVENT
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


def run_uvicorn_foreground(app: Any, host: str, port: int, log_level: str = "info") -> None:
    """运行 Uvicorn，并把终端、启动器与服务生命周期绑定在一起。"""
    import uvicorn

    server = uvicorn.Server(uvicorn.Config(
        app, host=host, port=port, log_level=log_level,
    ))
    parent_pid = os.getppid()
    stop_watcher = threading.Event()
    shutdown_complete = threading.Event()

    def request_shutdown() -> None:
        server.should_exit = True

    watcher = threading.Thread(
        target=watch_parent_exit,
        args=(parent_pid, request_shutdown, stop_watcher),
        name="qm-parent-watch",
        daemon=True,
    )
    watcher.start()
    unregister_handler = install_windows_console_handler(request_shutdown, shutdown_complete)
    try:
        server.run()
    finally:
        stop_watcher.set()
        shutdown_complete.set()
        unregister_handler()
        watcher.join(timeout=1.0)
