"""前台服务进程的安全退出与父进程守护。"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

RELOAD_QUIET_SECONDS = 30.0
RELOAD_MAX_BATCH_SECONDS = 300.0
RELOAD_MIN_INTERVAL_SECONDS = 300.0
RELOAD_TRIGGER_PATH_ENV = "QM_SERVER_RELOAD_TRIGGER_PATH"


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


def _meaningful_reload_paths(paths: set[Path], package_dir: Path) -> list[Path]:
    """Ignore release bookkeeping when it is the only backend source change."""
    release_path = (package_dir / "release.py").resolve()
    return sorted(
        (path for path in paths if path.resolve() != release_path),
        key=lambda path: str(path).casefold(),
    )


def _reload_timing_ms() -> tuple[int, int, int]:
    """Return bounded quiet, batching and minimum reload interval windows."""
    try:
        quiet = float(os.environ.get("QM_RELOAD_QUIET_SECONDS", RELOAD_QUIET_SECONDS))
    except ValueError:
        quiet = RELOAD_QUIET_SECONDS
    try:
        maximum = float(
            os.environ.get("QM_RELOAD_MAX_BATCH_SECONDS", RELOAD_MAX_BATCH_SECONDS),
        )
    except ValueError:
        maximum = RELOAD_MAX_BATCH_SECONDS
    try:
        interval = float(
            os.environ.get("QM_RELOAD_MIN_INTERVAL_SECONDS", RELOAD_MIN_INTERVAL_SECONDS),
        )
    except ValueError:
        interval = RELOAD_MIN_INTERVAL_SECONDS
    quiet = min(300.0, max(2.0, quiet))
    maximum = min(1800.0, max(quiet, maximum))
    interval = min(1800.0, max(quiet, interval))
    return round(quiet * 1000), round(maximum * 1000), round(interval * 1000)


class _ReloadChangeGate:
    """Accumulate backend changes and enforce a real minimum reload interval."""

    def __init__(
        self,
        package_dir: Path,
        minimum_interval: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.package_dir = package_dir
        self.minimum_interval = minimum_interval
        self.clock = clock
        self.pending: set[Path] = set()
        self.last_reload_at: float | None = None

    def offer(self, paths: set[Path]) -> list[Path] | None:
        self.pending.update(_meaningful_reload_paths(paths, self.package_dir))
        if not self.pending:
            return None
        now = self.clock()
        if (
            self.last_reload_at is not None
            and now - self.last_reload_at < self.minimum_interval
        ):
            return None
        ready = sorted(self.pending, key=lambda path: str(path).casefold())
        self.pending.clear()
        self.last_reload_at = now
        return ready

    def clear(self) -> None:
        """Drop accumulated automatic changes after a manual full worker reload."""
        self.pending.clear()


def manual_reload_trigger_path() -> Path | None:
    """Return the supervisor-owned trigger path when reload mode is active."""
    if os.environ.get("QM_SERVER_RELOAD_WORKER") != "1":
        return None
    configured = os.environ.get(RELOAD_TRIGGER_PATH_ENV, "").strip()
    return Path(configured).resolve() if configured else None


def request_manual_reload(path: Path) -> None:
    """Notify the reload supervisor after the HTTP response has been sent."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(time.time_ns()), encoding="ascii")


def _run_quiet_uvicorn_reload(
    uvicorn: Any,
    *,
    package_dir: Path,
    trigger_path: Path,
    host: str,
    port: int,
    log_level: str,
) -> None:
    """Run a reload supervisor that waits for a real editing quiet period."""
    from uvicorn.supervisors.basereload import BaseReload
    from uvicorn.supervisors.watchfilesreload import FileFilter
    from watchfiles import watch

    quiet_ms, maximum_ms, interval_ms = _reload_timing_ms()

    class QuietReload(BaseReload):
        def __init__(self, config, target, sockets):
            super().__init__(config, target, sockets)
            self.reloader_name = "QuantMaster watcher"
            self.watch_filter = FileFilter(config)
            self.change_gate = _ReloadChangeGate(
                package_dir,
                minimum_interval=interval_ms / 1000,
            )
            self.watcher = watch(
                package_dir,
                trigger_path.parent,
                watch_filter=None,
                stop_event=self.should_exit,
                debounce=maximum_ms,
                step=quiet_ms,
                yield_on_timeout=True,
                ignore_permission_denied=True,
            )

        def should_restart(self) -> list[Path] | None:
            changes = next(self.watcher)
            changed_paths = {Path(changed_path).resolve() for _change, changed_path in changes}
            if trigger_path.resolve() in changed_paths:
                self.change_gate.clear()
                return [trigger_path]
            candidates = {
                changed_path
                for changed_path in changed_paths
                if self.watch_filter(Path(changed_path))
            }
            return self.change_gate.offer(candidates)

    config = uvicorn.Config(
        "quantmaster.server.app:app",
        host=host,
        port=port,
        log_level=log_level,
        log_config=None,
        access_log=False,
        reload=True,
        reload_dirs=[str(package_dir)],
        reload_includes=["*.py"],
    )
    server = uvicorn.Server(config=config)
    socket = config.bind_socket()
    try:
        QuietReload(config, target=server.run, sockets=[socket]).run()
    except KeyboardInterrupt:  # pragma: no cover - interactive terminal path
        pass


def _run_uvicorn_reload(host: str, port: int, log_level: str) -> None:
    """在独立监督进程中热重载主站，同时保持 free-stockdb 持续运行。"""
    import uvicorn

    from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime

    package_dir = Path(__file__).resolve().parents[1]
    worker_flag = "QM_SERVER_RELOAD_WORKER"
    verbose_flag = "QM_SERVER_RELOAD_VERBOSE"
    control_flag = "QM_FREE_STOCKDB_CONTROL_PATH"
    trigger_flag = RELOAD_TRIGGER_PATH_ENV
    previous_flag = os.environ.get(worker_flag)
    previous_verbose = os.environ.get(verbose_flag)
    previous_control = os.environ.get(control_flag)
    previous_trigger = os.environ.get(trigger_flag)

    # Uvicorn 的 reload 监督进程不会加载 ASGI lifespan，正适合持有数据库
    # sidecar；每次替换的 Web worker 只连接它，不取得进程所有权。
    free_stockdb_runtime.start()
    control_path = free_stockdb_runtime._control_path()
    os.environ[control_flag] = str(control_path)
    trigger_path = control_path.with_name(".quantmaster-reload-trigger")
    os.environ[trigger_flag] = str(trigger_path)
    cleanup_lock = threading.Lock()
    cleanup_complete = threading.Event()

    def cleanup_sidecar() -> None:
        with cleanup_lock:
            if cleanup_complete.is_set():
                return
            try:
                free_stockdb_runtime.stop()
            finally:
                cleanup_complete.set()

    parent_stop = threading.Event()
    parent_watcher = threading.Thread(
        target=watch_parent_exit,
        args=(os.getppid(), cleanup_sidecar, parent_stop),
        name="qm-reload-parent-watch",
        daemon=True,
    )
    parent_watcher.start()
    unregister_handler = install_windows_console_handler(cleanup_sidecar, cleanup_complete)
    os.environ[worker_flag] = "1"
    from quantmaster.logging_config import is_verbose_logging

    os.environ[verbose_flag] = "1" if is_verbose_logging() else "0"
    try:
        _run_quiet_uvicorn_reload(
            uvicorn,
            package_dir=package_dir,
            trigger_path=trigger_path,
            host=host,
            port=port,
            log_level=log_level,
        )
    finally:
        # 只有整个启动器退出才停止数据库；Web worker 的多次热替换不会经过这里。
        parent_stop.set()
        cleanup_sidecar()
        unregister_handler()
        parent_watcher.join(timeout=1)
        if previous_flag is None:
            os.environ.pop(worker_flag, None)
        else:
            os.environ[worker_flag] = previous_flag
        if previous_verbose is None:
            os.environ.pop(verbose_flag, None)
        else:
            os.environ[verbose_flag] = previous_verbose
        if previous_control is None:
            os.environ.pop(control_flag, None)
        else:
            os.environ[control_flag] = previous_control
        if previous_trigger is None:
            os.environ.pop(trigger_flag, None)
        else:
            os.environ[trigger_flag] = previous_trigger


def run_uvicorn_foreground(
    app: Any,
    host: str,
    port: int,
    log_level: str = "warning",
    *,
    reload: bool = False,
) -> None:
    """运行 Uvicorn，并把终端、启动器与服务生命周期绑定在一起。"""
    import uvicorn

    from quantmaster.runtime.network import validate_listen_host

    host = validate_listen_host(host)

    if reload:
        _run_uvicorn_reload(host, port, log_level)
        return

    server = uvicorn.Server(uvicorn.Config(
        app, host=host, port=port, log_level=log_level,
        log_config=None, access_log=False,
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
