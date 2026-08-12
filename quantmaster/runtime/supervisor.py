"""Single-instance process supervisor for durable background workers.

The ASGI process is intentionally a submission/read process.  This module
starts one lightweight parent-owned process that runs :mod:`runtime.worker`;
individual CPU handlers are then spawned again by ``UnifiedJobRuntime``.  The
two levels keep FastAPI responsive without giving compute children authority to
renew or complete leases.
"""

from __future__ import annotations

import json
import logging
import multiprocessing
import os
import tempfile
import threading
import time
from collections.abc import Callable
from multiprocessing.process import BaseProcess
from pathlib import Path
from typing import Any, BinaryIO, Protocol

from quantmaster.config import get_config

logger = logging.getLogger(__name__)
WORKER_RESTART_MAX_DELAY_SECONDS = 30.0
WORKER_RESTART_STABLE_SECONDS = 10.0
WORKER_SUPERVISOR_MONITOR_SECONDS = 1.0


class _StopEvent(Protocol):
    def wait(self, timeout: float | None = None) -> bool: ...

    def set(self) -> None: ...


def _supervisor_status_path() -> Path:
    return get_config().data_root / "runtime-worker-supervisor.json"


def _publish_supervisor_status(status: str, *, detail: str = "") -> None:
    """Persist bootstrap state without depending on the SQLite task ledger."""

    path = _supervisor_status_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    value = {
        "status": status,
        "detail": detail[:800],
        "pid": os.getpid(),
        "updated_at": time.time(),
    }
    fd, raw_temp = tempfile.mkstemp(
        prefix=".runtime-worker-supervisor.", suffix=".tmp", dir=path.parent,
    )
    temp = Path(raw_temp)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def _supervisor_main(stop_event: _StopEvent, bootstrap_rotation: bool) -> None:
    """Spawn target kept importable for Windows ``spawn`` semantics."""

    from quantmaster.runtime.windows_app import initialize_windows_app_process

    initialize_windows_app_process()
    os.environ["QM_WORKER_SUPERVISOR"] = "1"
    os.environ.pop("QM_WEB_PROCESS", None)
    from quantmaster.runtime.worker import get_runtime_worker

    worker = get_runtime_worker()
    state, detail = "stopped", ""
    try:
        _publish_supervisor_status("starting")
        worker.start(bootstrap_rotation=bootstrap_rotation)
        state = "running"
        _publish_supervisor_status(state)
        while not stop_event.wait(0.5):
            pass
    except BaseException as exc:
        state = "failed"
        detail = f"{type(exc).__name__}: {exc}"
        try:
            _publish_supervisor_status(state, detail=detail)
        except OSError:
            logger.exception("runtime-worker 启动失败且无法写入诊断状态")
        logger.exception("runtime-worker 启动失败")
        raise
    finally:
        try:
            worker.stop()
        finally:
            try:
                _publish_supervisor_status(state, detail=detail)
            except OSError:
                logger.warning("runtime-worker 监督状态写入失败", exc_info=True)


class WorkerSupervisor:
    """Own one process and a cross-process lock for a data-root.

    A second Web process must never start a duplicate scheduler; it observes
    the existing ledger through its normal API clients and reports ``attached``
    to its caller instead.  The owner keeps the lock, so an orphaned child is
    never silently adopted after the Web process exits.
    """

    def __init__(
        self,
        root: Path | None = None,
        *,
        target: Callable[[_StopEvent, bool], None] | None = None,
    ) -> None:
        self.root = Path(root) if root is not None else get_config().data_root
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock_path = self.root / ".runtime-worker-supervisor.lock"
        self._context = multiprocessing.get_context("spawn")
        self._lock = threading.RLock()
        self._stream: BinaryIO | None = None
        self._process: BaseProcess | None = None
        self._stop_event: _StopEvent | None = None
        self._owned = False
        self._target = target or _supervisor_main
        self._restart_attempts = 0
        self._next_restart_at = 0.0
        self._last_spawn_at = 0.0
        self._monitor_stop = threading.Event()
        self._monitor_thread: threading.Thread | None = None

    @property
    def owned(self) -> bool:
        return self._owned

    @staticmethod
    def _disabled() -> bool:
        return os.environ.get("QM_DISABLE_WORKER_SUPERVISOR", "").strip().lower() in {
            "1", "true", "yes", "on",
        }

    def _acquire_lock(self) -> bool:
        if self._stream is not None:
            return True
        stream = self.lock_path.open("a+b")
        if self.lock_path.stat().st_size == 0:
            stream.write(b"0")
            stream.flush()
        try:
            stream.seek(0)
            if os.name == "nt":
                import msvcrt

                windows_lock: Any = msvcrt
                windows_lock.locking(stream.fileno(), windows_lock.LK_NBLCK, 1)
            else:
                import fcntl

                posix_lock: Any = fcntl
                posix_lock.flock(stream.fileno(), posix_lock.LOCK_EX | posix_lock.LOCK_NB)
        except (BlockingIOError, OSError):
            stream.close()
            return False
        self._stream = stream
        return True

    def _release_lock(self) -> None:
        stream, self._stream = self._stream, None
        if stream is None:
            return
        try:
            stream.seek(0)
            if os.name == "nt":
                import msvcrt

                windows_lock: Any = msvcrt
                windows_lock.locking(stream.fileno(), windows_lock.LK_UNLCK, 1)
            else:
                import fcntl

                posix_lock: Any = fcntl
                posix_lock.flock(stream.fileno(), posix_lock.LOCK_UN)
        finally:
            stream.close()

    def _spawn_locked(self, *, bootstrap_rotation: bool) -> None:
        """Start a child after this supervisor has acquired its instance lock."""

        stop_event = self._context.Event()
        process = self._context.Process(
            target=self._target,
            args=(stop_event, bool(bootstrap_rotation)),
            name="quantmaster-worker-supervisor",
            daemon=False,
        )
        try:
            process.start()
        except (OSError, RuntimeError):
            self._release_lock()
            self._owned = False
            raise
        self._stop_event = stop_event
        self._process = process
        self._last_spawn_at = time.monotonic()

    def _start_monitor_locked(self, *, bootstrap_rotation: bool) -> None:
        thread = self._monitor_thread
        if thread is not None and thread.is_alive():
            return
        self._monitor_stop.clear()

        def monitor() -> None:
            while not self._monitor_stop.wait(WORKER_SUPERVISOR_MONITOR_SECONDS):
                try:
                    self.ensure_running(bootstrap_rotation=bootstrap_rotation)
                except (OSError, RuntimeError):
                    logger.exception("runtime-worker 监督器无法启动替代进程")

        self._monitor_thread = threading.Thread(
            target=monitor,
            name="quantmaster-worker-supervisor-monitor",
            daemon=True,
        )
        self._monitor_thread.start()

    def start(self, *, bootstrap_rotation: bool = True) -> str:
        """Return ``started``, ``attached`` or ``disabled``.

        ``attached`` is still a success for the Web process: it should enqueue
        jobs passively because another owner holds the durable scheduler.
        """

        with self._lock:
            if self._disabled():
                return "disabled"
            if self._process is not None and self._process.is_alive():
                return "started"
            if self._owned:
                self._release_lock()
                self._owned = False
            if not self._acquire_lock():
                return "attached"
            self._owned = True
            self._restart_attempts = 0
            self._next_restart_at = 0.0
            self._spawn_locked(bootstrap_rotation=bootstrap_rotation)
            self._start_monitor_locked(bootstrap_rotation=bootstrap_rotation)
            return "started"

    def ensure_running(self, *, bootstrap_rotation: bool = True) -> str:
        """Recreate a failed child with a bounded exponential backoff.

        ``multiprocessing.Process.start`` only proves that a child was
        created.  If it then fails during imports or service bootstrap, the
        old parent kept its lock forever and every Web generation saw a cold
        worker.  The Web watchdog calls this cheap check once per second.
        """

        with self._lock:
            if self._disabled():
                return "disabled"
            process = self._process
            if process is not None and process.is_alive():
                if time.monotonic() - self._last_spawn_at >= WORKER_RESTART_STABLE_SECONDS:
                    self._restart_attempts = 0
                return "running"
            if process is not None:
                process.join(timeout=0)
                self._process = None
                self._stop_event = None
            now = time.monotonic()
            if now < self._next_restart_at:
                return "backoff"
            if not self._owned:
                if not self._acquire_lock():
                    return "attached"
                self._owned = True
            self._restart_attempts += 1
            delay = min(
                WORKER_RESTART_MAX_DELAY_SECONDS,
                float(2 ** min(self._restart_attempts - 1, 5)),
            )
            self._next_restart_at = now + delay
            self._spawn_locked(bootstrap_rotation=bootstrap_rotation)
            logger.warning(
                "runtime-worker 已退出，已启动第 %s 次替代进程（下次失败重试不早于 %.1fs）",
                self._restart_attempts,
                delay,
            )
            return "restarted"

    def stop(self, timeout: float = 12.0) -> None:
        with self._lock:
            self._monitor_stop.set()
            monitor, self._monitor_thread = self._monitor_thread, None
            process, stop_event = self._process, self._stop_event
            self._process = None
            self._stop_event = None
            owned, self._owned = self._owned, False
            self._restart_attempts = 0
            self._next_restart_at = 0.0
            if owned and stop_event is not None:
                stop_event.set()
        if monitor is not None and monitor is not threading.current_thread():
            monitor.join(timeout=1.5)
        if owned and process is not None:
            process.join(timeout=max(0.1, timeout))
            if process.is_alive():
                process.terminate()
                process.join(timeout=3.0)
        if owned:
            self._release_lock()


_SUPERVISOR: WorkerSupervisor | None = None
_SUPERVISOR_LOCK = threading.Lock()


def get_worker_supervisor() -> WorkerSupervisor:
    global _SUPERVISOR
    with _SUPERVISOR_LOCK:
        root = get_config().data_root
        if _SUPERVISOR is None or _SUPERVISOR.root != root:
            if _SUPERVISOR is not None:
                _SUPERVISOR.stop()
            _SUPERVISOR = WorkerSupervisor(root)
        return _SUPERVISOR


def reset_worker_supervisor_for_tests() -> None:
    global _SUPERVISOR
    with _SUPERVISOR_LOCK:
        value, _SUPERVISOR = _SUPERVISOR, None
    if value is not None:
        value.stop(2.0)
