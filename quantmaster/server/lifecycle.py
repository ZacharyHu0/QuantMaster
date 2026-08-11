"""前台服务进程的安全退出与父进程守护。"""

from __future__ import annotations

import functools
import logging
import multiprocessing
import os
import http.client
import signal
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# A hot-reload change must become a replacement Web generation quickly.  The
# old 30s quiet window plus a five-minute cooldown made an accepted manual
# reload look stuck even though the blue/green state machine itself was sound.
# These values still coalesce editor write bursts, while keeping normal reloads
# comfortably inside the ten-second readiness target.
RELOAD_QUIET_SECONDS = 2.0
RELOAD_MAX_BATCH_SECONDS = 30.0
RELOAD_MIN_INTERVAL_SECONDS = 5.0
RELOAD_TRIGGER_PATH_ENV = "QM_SERVER_RELOAD_TRIGGER_PATH"
RELOAD_READY_SECONDS = 20.0
RELOAD_DRAIN_SECONDS = 10.0
RELOAD_FORCE_KILL_SECONDS = 5.0
WEB_GENERATION_ENV = "QM_WEB_GENERATION"
WEB_PROCESS_ENV = "QM_WEB_PROCESS"


def _run_web_generation(
    *,
    sockets: list[Any],
    config: Any,
    drain_event: Any,
) -> None:
    """Run one disposable ASGI generation with a private drain signal.

    Windows ``CTRL_C_EVENT`` is a console-group broadcast, not a reliable
    child-process control channel.  Sending it while blue/green reloading
    could therefore interrupt the independent runtime worker (and did so in
    production).  A spawn-safe ``multiprocessing.Event`` gives the reload
    parent a private, process-local way to ask this Uvicorn server to stop
    accepting new work and perform its normal graceful shutdown.
    """

    import uvicorn

    server = uvicorn.Server(config=config)
    watcher_stop = threading.Event()

    def request_drain() -> None:
        drain_event.wait()
        if not watcher_stop.is_set():
            server.should_exit = True

    watcher = threading.Thread(
        target=request_drain,
        name="qm-web-generation-drain",
        daemon=True,
    )
    watcher.start()
    try:
        server.run(sockets=sockets)
    finally:
        watcher_stop.set()


class _WindowsGenerationJob:
    """Kill one Windows Web generation and all of its descendants together."""

    def __init__(self, process: Any) -> None:
        if os.name != "nt":  # pragma: no cover - construction is Windows-only
            raise OSError("Windows Job Object is unavailable")
        import ctypes
        from ctypes import wintypes

        class BasicLimitInformation(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_longlong),
                ("PerJobUserTimeLimit", ctypes.c_longlong),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class IoCounters(ctypes.Structure):
            _fields_ = [
                ("ReadOperationCount", ctypes.c_ulonglong),
                ("WriteOperationCount", ctypes.c_ulonglong),
                ("OtherOperationCount", ctypes.c_ulonglong),
                ("ReadTransferCount", ctypes.c_ulonglong),
                ("WriteTransferCount", ctypes.c_ulonglong),
                ("OtherTransferCount", ctypes.c_ulonglong),
            ]

        class ExtendedLimitInformation(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", BasicLimitInformation),
                ("IoInfo", IoCounters),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
        kernel.CreateJobObjectW.restype = wintypes.HANDLE
        kernel.SetInformationJobObject.argtypes = [
            wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD,
        ]
        kernel.SetInformationJobObject.restype = wintypes.BOOL
        kernel.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
        kernel.AssignProcessToJobObject.restype = wintypes.BOOL
        kernel.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
        kernel.TerminateJobObject.restype = wintypes.BOOL
        kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel.CloseHandle.restype = wintypes.BOOL

        handle = kernel.CreateJobObjectW(None, None)
        if not handle:
            raise OSError(ctypes.get_last_error(), "CreateJobObjectW failed")
        self._kernel = kernel
        self._handle = handle
        try:
            info = ExtendedLimitInformation()
            # KILL_ON_JOB_CLOSE provides the missing process-tree boundary.
            # We intentionally do not set a memory/process-count limit here:
            # this is a Web generation, not an untrusted compute sandbox.
            info.BasicLimitInformation.LimitFlags = 0x00002000
            if not kernel.SetInformationJobObject(
                handle, 9, ctypes.byref(info), ctypes.sizeof(info),
            ):
                raise OSError(ctypes.get_last_error(), "SetInformationJobObject failed")
            process_handle = wintypes.HANDLE(int(process.sentinel))
            if not process_handle or not kernel.AssignProcessToJobObject(handle, process_handle):
                raise OSError(ctypes.get_last_error(), "AssignProcessToJobObject failed")
        except OSError:
            self.close()
            raise

    def terminate(self) -> None:
        if self._handle:
            self._kernel.TerminateJobObject(self._handle, 1)

    def close(self) -> None:
        if self._handle:
            self._kernel.CloseHandle(self._handle)
            self._handle = None


def _attach_generation_job(process: Any) -> _WindowsGenerationJob | None:
    if os.name != "nt":
        return None
    try:
        return _WindowsGenerationJob(process)
    except (AttributeError, OSError, ValueError):
        logger.warning("无法把 Web 代次放入 Windows Job Object", exc_info=True)
        return None


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


def _reload_lifecycle_seconds() -> tuple[float, float, float]:
    """Return bounded startup, drain and forced-stop deadlines.

    Uvicorn's stock reloader waits forever for a child that has entered a
    blocking provider call.  These limits are deliberately independent from
    the edit batching settings above: they protect availability, not reload
    frequency.
    """

    defaults = (
        RELOAD_READY_SECONDS,
        RELOAD_DRAIN_SECONDS,
        RELOAD_FORCE_KILL_SECONDS,
    )
    names = (
        "QM_RELOAD_READY_SECONDS",
        "QM_RELOAD_DRAIN_SECONDS",
        "QM_RELOAD_FORCE_KILL_SECONDS",
    )
    values: list[float] = []
    for name, default in zip(names, defaults, strict=True):
        try:
            value = float(os.environ.get(name, default))
        except ValueError:
            value = default
        values.append(min(120.0, max(1.0, value)))
    return tuple(values)  # type: ignore[return-value]


def _stop_reload_process(
    process: Any,
    *,
    drain_seconds: float,
    force_seconds: float,
    job: _WindowsGenerationJob | None = None,
    drain_event: Any | None = None,
) -> bool:
    """Stop one Web generation without ever blocking the supervisor forever.

    A private drain event gives ASGI lifespan cleanup a chance to finish.  A
    provider call or a wedged thread may prevent that cleanup from completing,
    in which case the process is force-stopped after the explicit drain budget.
    """

    if not process.is_alive():
        process.join(timeout=0)
        if job is not None:
            job.close()
        return True
    if drain_event is not None:
        try:
            drain_event.set()
        except (OSError, RuntimeError, ValueError):
            # A broken control primitive must not revive the historical
            # unbounded join black hole.  Fall through to bounded termination.
            logger.warning("无法向 Web 子进程发送私有排空信号", exc_info=True)
            drain_event = None
    if drain_event is None:
        # Never use CTRL_C_EVENT here.  On Windows it can broadcast to the
        # shared console and kill the independent runtime-worker; on every
        # platform ``terminate`` is the safe bounded fallback when a private
        # drain channel was unavailable during a failed early spawn.
        process.terminate()
    process.join(timeout=max(0.0, drain_seconds))
    if not process.is_alive():
        if job is not None:
            job.close()
        return True
    logger.warning(
        "Web 子进程 %s 在 %.1fs 排空预算内未退出，正在强制停止",
        getattr(process, "pid", "?"),
        drain_seconds,
    )
    try:
        if job is not None:
            job.terminate()
        else:
            process.terminate()
    except (OSError, ValueError):
        logger.warning("无法终止卡住的 Web 子进程", exc_info=True)
    process.join(timeout=max(0.0, force_seconds))
    if process.is_alive():
        kill = getattr(process, "kill", None)
        if callable(kill):
            try:
                kill()
            except (OSError, ValueError):
                logger.warning("无法杀死卡住的 Web 子进程", exc_info=True)
            process.join(timeout=max(0.0, force_seconds))
    stopped = not process.is_alive()
    if job is not None:
        # On a last-resort failure, closing a KILL_ON_JOB_CLOSE handle still
        # tears down descendants rather than leaking them into the next Web
        # generation.
        job.close()
    return stopped


def _reload_probe_host(host: str) -> str:
    """Use an address that can reach the stable listener from its supervisor."""

    value = str(host).strip()
    if value in {"", "0.0.0.0", "::", "[::]"}:
        return "127.0.0.1"
    return value


def _generation_is_ready(host: str, port: int, generation: int) -> bool:
    """Return true only when the requested Web generation answered itself."""

    connection = http.client.HTTPConnection(_reload_probe_host(host), int(port), timeout=0.35)
    try:
        connection.request("GET", "/api/v1/health/live")
        response = connection.getresponse()
        response.read()
        return (
            response.status == 200
            and response.getheader("X-QM-Worker-Generation") == str(generation)
        )
    except (OSError, http.client.HTTPException):
        return False
    finally:
        connection.close()


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
    worker_supervisor: Any | None = None,
) -> None:
    """Run a bounded blue/green reload supervisor.

    The listener remains owned by the parent process.  A replacement child is
    started on that listener and must prove its own generation through the
    liveness endpoint before the serving child is asked to drain.  This avoids
    the historical failure mode where ``BaseReload.restart()`` joined a wedged
    worker forever while the TCP port still accepted connections.
    """
    from uvicorn.supervisors.basereload import BaseReload
    from uvicorn.supervisors.watchfilesreload import FileFilter
    from uvicorn._subprocess import get_subprocess, spawn
    from watchfiles import watch

    quiet_ms, maximum_ms, interval_ms = _reload_timing_ms()
    ready_seconds, drain_seconds, force_seconds = _reload_lifecycle_seconds()

    class QuietReload(BaseReload):
        def __init__(self, config, target, sockets):
            super().__init__(config, target, sockets)
            self.reloader_name = "QuantMaster watcher"
            self.watch_filter = FileFilter(config)
            self.change_gate = _ReloadChangeGate(
                package_dir,
                minimum_interval=interval_ms / 1000,
            )
            self._generation_jobs: dict[int, _WindowsGenerationJob] = {}
            self._generation_drains: dict[int, Any] = {}
            self._last_watchdog_at = 0.0
            self._unhealthy_checks = 0
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

        def _spawn_generation(self, generation: int):
            """Spawn a child with an immutable, observable generation ID."""

            previous = os.environ.get(WEB_GENERATION_ENV)
            previous_web_process = os.environ.get(WEB_PROCESS_ENV)
            os.environ[WEB_GENERATION_ENV] = str(generation)
            os.environ[WEB_PROCESS_ENV] = "1"
            try:
                drain_event = spawn.Event()
                process = get_subprocess(
                    config=self.config,
                    target=functools.partial(
                        _run_web_generation,
                        config=self.config,
                        drain_event=drain_event,
                    ),
                    sockets=self.sockets,
                )
                process.start()
                self._generation_drains[id(process)] = drain_event
                job = _attach_generation_job(process)
                if job is not None:
                    self._generation_jobs[id(process)] = job
                return process
            finally:
                if previous is None:
                    os.environ.pop(WEB_GENERATION_ENV, None)
                else:
                    os.environ[WEB_GENERATION_ENV] = previous
                if previous_web_process is None:
                    os.environ.pop(WEB_PROCESS_ENV, None)
                else:
                    os.environ[WEB_PROCESS_ENV] = previous_web_process

        def _stop_generation(self, process: Any, *, drain_seconds: float) -> bool:
            job = self._generation_jobs.pop(id(process), None)
            drain_event = self._generation_drains.pop(id(process), None)
            return _stop_reload_process(
                process,
                drain_seconds=drain_seconds,
                force_seconds=force_seconds,
                job=job,
                drain_event=drain_event,
            )

        def _wait_for_generation(self, process: Any, generation: int) -> bool:
            deadline = time.monotonic() + ready_seconds
            while time.monotonic() < deadline:
                if not process.is_alive():
                    logger.error("Web 代次 %s 在就绪前退出", generation)
                    return False
                if _generation_is_ready(host, port, generation):
                    return True
                if self.should_exit.wait(0.1):
                    return False
            logger.error("Web 代次 %s 在 %.1fs 内未就绪", generation, ready_seconds)
            return False

        def startup(self) -> None:
            # This is BaseReload.startup with one important difference: the
            # child is given a generation identity before it is spawned.
            from uvicorn.supervisors.basereload import HANDLED_SIGNALS
            self.reloader_name = self.reloader_name or "QuantMaster watcher"
            logger.info("Started reloader process [%s] using %s", self.pid, self.reloader_name)
            for handled in HANDLED_SIGNALS:
                signal.signal(handled, self.signal_handler)
            self.generation = 1
            self.process = self._spawn_generation(self.generation)
            if not self._wait_for_generation(self.process, self.generation):
                # Initial startup must fail explicitly rather than leave a
                # parent owning an apparently healthy but handler-less port.
                self._stop_generation(
                    self.process,
                    drain_seconds=0,
                )
                raise RuntimeError("QuantMaster Web 初始代次未能就绪")

        def restart(self) -> None:
            old = self.process
            next_generation = int(getattr(self, "generation", 0)) + 1
            self.is_restarting = True
            replacement = self._spawn_generation(next_generation)
            try:
                if not self._wait_for_generation(replacement, next_generation):
                    self._stop_generation(
                        replacement,
                        drain_seconds=0,
                    )
                    logger.error(
                        "热重载回滚：代次 %s 保持服务，代次 %s 未上线",
                        getattr(self, "generation", 0),
                        next_generation,
                    )
                    return
                self.process = replacement
                self.generation = next_generation
                if not self._stop_generation(
                    old,
                    drain_seconds=drain_seconds,
                ):
                    logger.error("旧 Web 代次 %s 未能在强制停止后退出", getattr(old, "pid", "?"))
            finally:
                self.is_restarting = False

        def _watch_current_generation(self) -> bool:
            """Return whether the active child passed the bounded watchdog.

            This is intentionally a private, generation-specific liveness
            probe: a TCP accept alone was the source of the old black-hole
            behaviour.  Three misses mean the child is no longer serving its
            event loop, so the normal blue/green replacement path is used.
            """

            now = time.monotonic()
            if now - self._last_watchdog_at < 1.0:
                return True
            self._last_watchdog_at = now
            if worker_supervisor is not None:
                worker_state = worker_supervisor.ensure_running(bootstrap_rotation=True)
                if worker_state == "restarted":
                    logger.warning("runtime-worker 异常退出，已请求替代进程")
            generation = int(getattr(self, "generation", 0))
            process = getattr(self, "process", None)
            healthy = bool(
                process is not None
                and process.is_alive()
                and _generation_is_ready(host, port, generation)
            )
            if healthy:
                self._unhealthy_checks = 0
                return True
            self._unhealthy_checks += 1
            if self._unhealthy_checks < 3:
                return True
            logger.error(
                "Web 代次 %s 连续 %s 次心跳失败，准备蓝绿替换",
                generation,
                self._unhealthy_checks,
            )
            self._unhealthy_checks = 0
            return False

        def shutdown(self) -> None:
            self.should_exit.set()
            self._stop_generation(
                self.process,
                drain_seconds=drain_seconds,
            )
            for sock in self.sockets:
                sock.close()
            logger.info("Stopping reloader process [%s]", self.pid)

        def should_restart(self) -> list[Path] | None:
            changes = next(self.watcher)
            if not self._watch_current_generation():
                return [trigger_path]
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
    socket = config.bind_socket()
    try:
        QuietReload(config, target=_run_web_generation, sockets=[socket]).run()
    except KeyboardInterrupt:  # pragma: no cover - interactive terminal path
        pass


def _run_uvicorn_reload(host: str, port: int, log_level: str) -> None:
    """在独立监督进程中热重载主站，同时保持 free-stockdb 持续运行。"""
    import uvicorn

    from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime
    from quantmaster.data.maintenance import data_refresh_manager
    from quantmaster.runtime.supervisor import get_worker_supervisor
    from quantmaster.runtime.worker import get_runtime_worker

    package_dir = Path(__file__).resolve().parents[1]
    worker_flag = "QM_SERVER_RELOAD_WORKER"
    verbose_flag = "QM_SERVER_RELOAD_VERBOSE"
    control_flag = "QM_FREE_STOCKDB_CONTROL_PATH"
    trigger_flag = RELOAD_TRIGGER_PATH_ENV
    web_flag = "QM_WEB_PROCESS"
    previous_flag = os.environ.get(worker_flag)
    previous_verbose = os.environ.get(verbose_flag)
    previous_control = os.environ.get(control_flag)
    previous_trigger = os.environ.get(trigger_flag)
    previous_web = os.environ.get(web_flag)

    # Uvicorn 的 reload 监督进程不会加载 ASGI lifespan，正适合持有数据库
    # sidecar；每次替换的 Web worker 只连接它，不取得进程所有权。
    free_stockdb_runtime.start()
    # The durable refresh dispatcher belongs to a process separate from both
    # the reload parent and disposable ASGI children.  Jobs survive Web
    # generation changes and CPU work cannot stall request handling.
    worker_supervisor = get_worker_supervisor()
    supervisor_state = worker_supervisor.start(bootstrap_rotation=True)
    if supervisor_state == "disabled":
        data_refresh_manager.start()
        get_runtime_worker().start(bootstrap_rotation=True)
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
                if supervisor_state == "disabled":
                    get_runtime_worker().stop()
                else:
                    worker_supervisor.stop()
                free_stockdb_runtime.stop()
            finally:
                if supervisor_state == "disabled":
                    data_refresh_manager.shutdown(timeout=10.0)
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
    os.environ[web_flag] = "1"
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
            worker_supervisor=worker_supervisor,
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
        if previous_web is None:
            os.environ.pop(web_flag, None)
        else:
            os.environ[web_flag] = previous_web


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
