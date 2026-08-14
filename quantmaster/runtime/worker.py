"""Supervisor-owned background runtime for reloadable QuantMaster Web workers.

The ASGI process is intentionally disposable.  Long-running refreshes,
schedulers and CPU/network workers live here so source reloads cannot inherit
their threads or wait for them during shutdown.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import threading
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

from quantmaster.config import get_config
from quantmaster.runtime.lifecycle_state import RuntimeLifecycle
from quantmaster.runtime.maintenance import MaintenanceParticipant, maintenance_barrier
from quantmaster.runtime.worker_ipc import RuntimeCommandServer, WorkerCommandError

logger = logging.getLogger(__name__)
WORKER_HEARTBEAT_SECONDS = 1.0
WORKER_HEARTBEAT_MAX_AGE_SECONDS = 5.0


def _heartbeat_path() -> Path:
    return get_config().data_root / "runtime-worker.json"


def _supervisor_status() -> dict[str, Any]:
    """Read a failed bootstrap record without constructing a supervisor."""

    path = get_config().data_root / "runtime-worker-supervisor.json"
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def runtime_worker_status() -> dict[str, Any]:
    """Read the supervisor-owned worker lease without opening SQLite.

    A Web generation can safely use this in a write endpoint: it is a tiny
    atomically replaced local file, so a dead worker produces a prompt,
    explicit ``worker_unavailable`` result instead of a queued task that no
    process will ever run.
    """

    path = _heartbeat_path()
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        updated_at = float(value.get("updated_at") or 0)
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        supervisor = _supervisor_status()
        supervisor_state = str(supervisor.get("status") or "")
        detail = str(supervisor.get("detail") or "")
        reason = "runtime-worker 未发布心跳"
        if supervisor_state == "starting":
            reason = "runtime-worker 正在启动"
        elif supervisor_state == "failed":
            reason = f"runtime-worker 启动失败：{detail or '请查看本地诊断'}"
        return {
            "status": "unavailable",
            "available": False,
            "reason": reason,
            "heartbeat_path": str(path),
            "supervisor": supervisor,
        }
    age_seconds = max(0.0, time.time() - updated_at)
    available = age_seconds <= WORKER_HEARTBEAT_MAX_AGE_SECONDS
    return {
        **value,
        "status": "running" if available else "unavailable",
        "available": available,
        "age_seconds": round(age_seconds, 3),
        "heartbeat_path": str(path),
        "reason": "" if available else "runtime-worker 心跳已过期",
    }


class WorkerPlan(Protocol):
    """Concrete worker wiring supplied by the application composition root."""

    def settings_projection(self) -> tuple[int, int]: ...

    def start(self, *, bootstrap_rotation: bool) -> None: ...

    def drain(self) -> None: ...

    def resume(self) -> None: ...

    def idle(self) -> bool: ...

    def handle_command(
        self, operation: str, payload: dict[str, Any],
    ) -> dict[str, Any]: ...

    def stop(self, enter_phase: Callable[[str, float], None]) -> None: ...


class RuntimeWorker:
    """Start and stop the persistent background services once per process."""

    def __init__(self, plan_factory: Callable[[], WorkerPlan] | None = None) -> None:
        self._lock = threading.RLock()
        self._started = False
        self._plan_factory = plan_factory
        self._plan: WorkerPlan | None = None
        self._maintenance_lease: Any = None
        self._unregister_maintenance: Callable[[], None] | None = None
        self._worker_id = uuid.uuid4().hex
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None
        self._command_server: RuntimeCommandServer | None = None
        self._command_error = ""
        self._generation = uuid.uuid4().hex[:12]
        self._lifecycle = RuntimeLifecycle("runtime-worker", self._generation)
        self._config_revision = 0
        self._config_generation = 0

    def _write_heartbeat(self) -> None:
        path = _heartbeat_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        value = {
            "worker_id": self._worker_id,
            "generation": self._generation,
            "pid": os.getpid(),
            "updated_at": time.time(),
            "started": self._started,
            "threads": threading.active_count(),
            "lifecycle": self._lifecycle.snapshot(),
            "effective_revision": self._config_revision,
            "config_generation": self._config_generation,
        }
        command_server = self._command_server
        value["commands_available"] = bool(
            command_server is not None and command_server.running,
        )
        if self._command_error:
            value["commands_error"] = self._command_error
        if command_server is not None:
            value["command_endpoint"] = str(command_server.endpoint)
        fd, raw_temp = tempfile.mkstemp(
            prefix=".runtime-worker.", suffix=".tmp", dir=path.parent,
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

    def _start_heartbeat(self) -> None:
        self._heartbeat_stop.clear()
        try:
            self._write_heartbeat()
        except OSError:
            # Runtime work is still usable if a removable data drive briefly
            # refuses the lease write; the next tick retries without blocking
            # the supervisor startup path.
            logger.warning("runtime-worker 初始心跳写入失败", exc_info=True)

        def tick() -> None:
            while not self._heartbeat_stop.wait(WORKER_HEARTBEAT_SECONDS):
                try:
                    self._write_heartbeat()
                except OSError:
                    logger.warning("runtime-worker 心跳写入失败", exc_info=True)

        self._heartbeat_thread = self._lifecycle.start_thread(
            target=tick, name="runtime-worker-heartbeat",
            phase="heartbeat", diagnostic_id="QM-LC-WORKER-HEARTBEAT",
            shutdown_policy="signal_then_join", deadline_seconds=1.5,
            stop=self._heartbeat_stop.set,
        )

    def _stop_heartbeat(self) -> None:
        self._heartbeat_stop.set()
        thread = self._heartbeat_thread
        if thread is not None:
            thread.join(timeout=1.5)
        self._heartbeat_thread = None
        path = _heartbeat_path()
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            if str(value.get("worker_id") or "") == self._worker_id:
                path.unlink(missing_ok=True)
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            pass

    def _handle_command(
        self, operation: str, payload: dict[str, Any],
    ) -> dict[str, Any]:
        try:
            if operation == "maintenance.enter":
                if self._maintenance_lease is not None:
                    raise ValueError("已有维护租约")
                self._maintenance_lease = maintenance_barrier.enter(
                    str(payload.get("reason") or "external maintenance"),
                    timeout=float(payload.get("timeout") or 30),
                )
                return {
                    "token": self._maintenance_lease.token,
                    "worker_id": self._worker_id,
                    "pid": os.getpid(),
                    **maintenance_barrier.status(),
                }
            if operation == "maintenance.status":
                token = str(payload.get("token") or "")
                return {
                    "valid": bool(
                        self._maintenance_lease is not None
                        and self._maintenance_lease.token == token
                        and maintenance_barrier.frozen
                    ),
                    "worker_id": self._worker_id,
                    "pid": os.getpid(),
                    **maintenance_barrier.status(),
                }
            if operation == "maintenance.exit":
                token = str(payload.get("token") or "")
                if self._maintenance_lease is None or self._maintenance_lease.token != token:
                    raise ValueError("维护租约 token 无效")
                lease, self._maintenance_lease = self._maintenance_lease, None
                maintenance_barrier.exit(lease)
                return {"released": True, **maintenance_barrier.status()}
            plan = self._plan
            if plan is None:
                raise RuntimeError("runtime-worker plan 尚未启动")
            result = plan.handle_command(operation, payload)
            if (
                operation == "settings.apply.latest"
                and result.get("status") == "effective"
            ):
                self._config_revision = int(
                    result.get("latest_revision") or result.get("revision") or 0,
                )
                self._config_generation = max(
                    self._config_generation, int(result.get("generation") or 0),
                )
            return result
        except KeyError as exc:
            raise WorkerCommandError("job_not_found", "数据刷新任务不存在") from exc
        except ValueError as exc:
            raise WorkerCommandError("command_conflict", str(exc)) from exc
        except WorkerCommandError:
            raise
        except (FileNotFoundError, RuntimeError) as exc:
            raise WorkerCommandError("worker_command_failed", str(exc)) from exc

    def start(self, *, bootstrap_rotation: bool) -> bool:
        with self._lock:
            if self._started:
                return False
            if self._lifecycle.snapshot()["state"] == "stopped":
                # A deliberate in-process restart is a new generation.  It
                # cannot reuse the stopped generation's task registry.
                self._generation = uuid.uuid4().hex[:12]
                self._lifecycle = RuntimeLifecycle("runtime-worker", self._generation)
            self._command_error = ""
            # Directory creation is a worker-startup responsibility.  Web
            # readers only receive the pure ``Config.data_root`` path and
            # must report a cold snapshot instead of creating it themselves.
            get_config().ensure_data_root()
            if self._plan_factory is None:
                raise RuntimeError("runtime-worker 需要 composition root 提供 worker plan")
            plan = self._plan_factory()
            self._plan = plan
            self._config_revision, self._config_generation = plan.settings_projection()
            self._unregister_maintenance = maintenance_barrier.register(
                MaintenanceParticipant(
                    name=f"runtime-worker:{uuid.uuid4().hex}",
                    drain=plan.drain,
                    resume=plan.resume,
                    idle=plan.idle,
                )
            )
            try:
                plan.start(bootstrap_rotation=bootstrap_rotation)
            except BaseException:
                try:
                    plan.stop(lambda _name, _deadline: None)
                except (OSError, RuntimeError, ValueError, TypeError):
                    logger.exception("runtime-worker plan 部分启动清理失败")
                finally:
                    self._unregister_maintenance()
                    self._unregister_maintenance = None
                    self._plan = None
                raise
            command_server = RuntimeCommandServer(self._handle_command)
            try:
                command_server.start()
            except OSError as exc:
                # The command channel is an explicit write-path dependency,
                # not a page-read dependency.  A stale/denied local named
                # pipe must leave the durable worker, heartbeats and published
                # snapshots available; Web mutations then fail promptly as
                # ``worker_unavailable`` instead of taking the whole service
                # down during bootstrap.
                self._command_error = f"{type(exc).__name__}: {exc}"
                logger.warning(
                    "runtime-worker 本机命令通道不可用；页面读取保持可用",
                    exc_info=True,
                )
            else:
                self._command_server = command_server
            self._started = True
            self._start_heartbeat()
            logger.info("QuantMaster runtime-worker 已启动（Web 代次可独立重载）")
            return True

    def stop(self) -> None:
        with self._lock:
            if not self._started:
                return
            # Phase 1: fence admission before touching any producer, lease or
            # client.  Every component below owns only its process generation.
            self._lifecycle.begin_shutdown()
            command_server, self._command_server = self._command_server, None
            if command_server is not None:
                command_server.stop()
            self._stop_heartbeat()
            plan, self._plan = self._plan, None
            if plan is not None:
                plan.stop(self._lifecycle.enter_phase)
            if self._unregister_maintenance is not None:
                self._unregister_maintenance()
                self._unregister_maintenance = None
            self._lifecycle.enter_phase("close_resources", 5.0)
            self._lifecycle.converge_owned()
            self._started = False
            self._lifecycle.finish()
            logger.info("QuantMaster runtime-worker 已停止")

    def status(self) -> dict[str, Any]:
        status = runtime_worker_status()
        status["in_process_started"] = self._started
        status["lifecycle"] = self._lifecycle.snapshot()
        return status
