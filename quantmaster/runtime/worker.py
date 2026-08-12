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
from typing import Any

from quantmaster.config import get_config
from quantmaster.runtime.worker_ipc import RuntimeCommandServer

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


class RuntimeWorker:
    """Start and stop the persistent background services once per process."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._started = False
        self._unregister_maintenance: Callable[[], None] | None = None
        self._worker_id = uuid.uuid4().hex
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None
        self._command_server: RuntimeCommandServer | None = None
        self._command_error = ""

    def _write_heartbeat(self) -> None:
        path = _heartbeat_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        value = {
            "worker_id": self._worker_id,
            "pid": os.getpid(),
            "updated_at": time.time(),
            "started": self._started,
            "threads": threading.active_count(),
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

        self._heartbeat_thread = threading.Thread(
            target=tick,
            name="runtime-worker-heartbeat",
            daemon=True,
        )
        self._heartbeat_thread.start()

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

    def start(self, *, bootstrap_rotation: bool) -> bool:
        with self._lock:
            if self._started:
                return False
            self._command_error = ""
            # Directory creation is a worker-startup responsibility.  Web
            # readers only receive the pure ``Config.data_root`` path and
            # must report a cold snapshot instead of creating it themselves.
            get_config().ensure_data_root()

            from quantmaster.after_close.jobs import get_after_close_jobs
            from quantmaster.ai.news_jobs import get_news_jobs
            from quantmaster.analysis.stock_jobs import get_stock_analysis_jobs
            from quantmaster.automation.runtime import get_runtime
            from quantmaster.backtest.paper_automation import get_paper_automation_worker
            from quantmaster.backtest.workbench import get_backtest_worker
            from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime
            from quantmaster.data.instruments import InstrumentStore
            from quantmaster.data.maintenance import data_refresh_manager
            from quantmaster.data.repair import get_data_repair_manager
            from quantmaster.lab.capabilities import publish_capabilities
            from quantmaster.lab.llm_jobs import get_lab_llm_jobs
            from quantmaster.lab.worker import get_worker
            from quantmaster.market.overview_snapshot import publish_market_overview_snapshot
            from quantmaster.research.jobs import get_research_job_manager
            from quantmaster.rotation.etf_jobs import get_etf_research_jobs
            from quantmaster.rotation.service import get_rotation_worker
            from quantmaster.runtime.maintenance import MaintenanceParticipant, maintenance_barrier
            from quantmaster.runtime.worker_ipc import RuntimeCommandServer, WorkerCommandError
            from quantmaster.server.diagnostics import start_diagnostics_sampler
            from quantmaster.server.settings_jobs import get_settings_jobs

            # This installs only the bundled offline catalogue.  It must not
            # trigger a remote catalogue refresh at worker startup.
            InstrumentStore()
            runtime = get_runtime()
            worker = get_worker()
            backtest_worker = get_backtest_worker()
            research_worker = get_research_job_manager()
            rotation_worker = get_rotation_worker()
            repair_worker = get_data_repair_manager()
            stock_analysis_worker = get_stock_analysis_jobs()
            after_close_worker = get_after_close_jobs()
            etf_research_worker = get_etf_research_jobs()
            news_worker = get_news_jobs()
            settings_worker = get_settings_jobs()
            lab_llm_worker = get_lab_llm_jobs()

            def publish_lab_capabilities() -> None:
                try:
                    publish_capabilities()
                except (OSError, RuntimeError, ValueError, TypeError):
                    logger.warning("Quant Lab 能力快照发布失败", exc_info=True)

            def publish_market_overview() -> None:
                try:
                    publish_market_overview_snapshot()
                except (OSError, RuntimeError, ValueError, TypeError):
                    logger.warning("市场总览快照发布失败", exc_info=True)

            def handle_command(operation: str, payload: dict[str, Any]) -> dict[str, Any]:
                """Perform Web-submitted data mutations in this worker only."""

                try:
                    if operation == "data.refresh.preview":
                        return data_refresh_manager.preview(
                            str(payload.get("scope") or "market"),
                            str(payload.get("universe") or ""),
                            str(payload.get("start") or ""),
                        )
                    if operation == "data.refresh.create":
                        return data_refresh_manager.create(
                            str(payload.get("scope") or "market"),
                            str(payload.get("universe") or ""),
                            str(payload.get("start") or ""),
                        )
                    if operation == "data.refresh.cancel":
                        return data_refresh_manager.cancel(str(payload.get("job_id") or ""))
                    if operation == "data.refresh.retry":
                        return data_refresh_manager.resume(str(payload.get("job_id") or ""))
                    if operation == "automation.apply_config":
                        from quantmaster.config import load_config, set_config

                        set_config(load_config())
                        changed = [str(value) for value in payload.get("changed_fields") or []]
                        return runtime.apply_config(changed)
                    if operation == "settings.diagnostic.create":
                        from quantmaster.settings import SettingsDocument

                        document = SettingsDocument.model_validate(payload.get("document") or {})
                        task, created = settings_worker._submit_diagnostic_local(
                            str(payload.get("kind") or ""),
                            document,
                            api_key=str(payload.get("api_key") or ""),
                        )
                        return {"task": task, "created": created}
                except KeyError as exc:
                    raise WorkerCommandError("job_not_found", "数据刷新任务不存在") from exc
                except ValueError as exc:
                    raise WorkerCommandError("command_conflict", str(exc)) from exc
                except (FileNotFoundError, RuntimeError) as exc:
                    raise WorkerCommandError("worker_command_failed", str(exc)) from exc
                raise WorkerCommandError("unknown_command", "后台执行器不支持该命令")

            def drain() -> None:
                get_paper_automation_worker().stop()
                rotation_worker.stop()
                repair_worker.shutdown()
                data_refresh_manager.shutdown()
                research_worker.shutdown()
                backtest_worker.stop()
                worker.stop()
                runtime.stop()
                stock_analysis_worker.pause()
                after_close_worker.pause()
                etf_research_worker.pause()
                news_worker.pause()
                settings_worker.pause()
                lab_llm_worker.runtime.pause()

            def resume() -> None:
                stock_analysis_worker.resume()
                after_close_worker.resume()
                etf_research_worker.resume()
                news_worker.resume()
                settings_worker.resume()
                lab_llm_worker.runtime.resume()
                runtime.start()
                research_worker.start()
                data_refresh_manager.start()
                repair_worker.start()
                backtest_worker.start()
                get_paper_automation_worker().start()
                rotation_worker.start()
                if get_config().lab.enabled:
                    worker.start()
                threading.Thread(
                    target=publish_market_overview,
                    name="quant-market-overview-publish",
                    daemon=True,
                ).start()

            self._unregister_maintenance = maintenance_barrier.register(
                MaintenanceParticipant(
                    name=f"runtime-worker:{uuid.uuid4().hex}",
                    drain=drain,
                    resume=resume,
                    idle=lambda: (
                        not data_refresh_manager.active
                        and rotation_worker.idle
                        and get_paper_automation_worker().idle
                        and stock_analysis_worker.idle
                        and after_close_worker.idle
                        and etf_research_worker.idle
                        and news_worker.idle
                        and settings_worker.idle
                        and lab_llm_worker.runtime.idle
                    ),
                )
            )
            runtime.start()
            research_worker.start()
            stock_analysis_worker.start()
            after_close_worker.start()
            etf_research_worker.start()
            news_worker.start()
            settings_worker.start()
            lab_llm_worker.runtime.start()
            start_diagnostics_sampler()
            free_stockdb_runtime.start_event_bridge()
            data_refresh_manager.start()
            repair_worker.start()
            backtest_worker.start()
            get_paper_automation_worker().start()
            rotation_worker.start(bootstrap_local=bootstrap_rotation)
            command_server = RuntimeCommandServer(handle_command)
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
            # Market cards are a precomputed local projection.  Do this on
            # the runtime worker so a browser's first GET can never scan the
            # BarStore or create a per-request executor.
            threading.Thread(
                target=publish_market_overview,
                name="quant-market-overview-publish",
                daemon=True,
            ).start()
            if get_config().lab.enabled:
                worker.start()
                # GPU/runtime inspection is intentionally outside the Web
                # process and detached from worker readiness.  Until it
                # completes, pages render a clear cold capability state
                # instead of blocking.
                threading.Thread(
                    target=publish_lab_capabilities,
                    name="quant-lab-capabilities-publish",
                    daemon=True,
                ).start()
            self._started = True
            self._start_heartbeat()
            logger.info("QuantMaster runtime-worker 已启动（Web 代次可独立重载）")
            return True

    def stop(self) -> None:
        with self._lock:
            if not self._started:
                return
            from quantmaster.after_close.jobs import shutdown_after_close_jobs
            from quantmaster.ai.news_jobs import shutdown_news_jobs
            from quantmaster.analysis.stock_jobs import shutdown_stock_analysis_jobs
            from quantmaster.automation.runtime import get_runtime
            from quantmaster.backtest.paper_automation import get_paper_automation_worker
            from quantmaster.backtest.workbench import get_backtest_worker
            from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime
            from quantmaster.data.maintenance import data_refresh_manager
            from quantmaster.data.repair import get_data_repair_manager
            from quantmaster.lab.llm_jobs import shutdown_lab_llm_jobs
            from quantmaster.lab.worker import get_worker
            from quantmaster.research.jobs import get_research_job_manager
            from quantmaster.rotation.etf_jobs import shutdown_etf_research_jobs
            from quantmaster.rotation.service import get_rotation_worker
            from quantmaster.server.diagnostics import stop_diagnostics_sampler
            from quantmaster.server.settings_jobs import shutdown_settings_jobs

            command_server, self._command_server = self._command_server, None
            if command_server is not None:
                command_server.stop()
            self._stop_heartbeat()
            stop_diagnostics_sampler()
            free_stockdb_runtime.stop_event_bridge()
            get_paper_automation_worker().stop()
            get_rotation_worker().shutdown()
            get_data_repair_manager().shutdown()
            data_refresh_manager.shutdown()
            get_research_job_manager().shutdown()
            get_backtest_worker().stop()
            get_worker().stop()
            get_runtime().stop()
            shutdown_stock_analysis_jobs()
            shutdown_after_close_jobs()
            shutdown_etf_research_jobs()
            shutdown_news_jobs()
            shutdown_settings_jobs()
            shutdown_lab_llm_jobs()
            if self._unregister_maintenance is not None:
                self._unregister_maintenance()
                self._unregister_maintenance = None
            self._started = False
            logger.info("QuantMaster runtime-worker 已停止")

    def status(self) -> dict[str, Any]:
        status = runtime_worker_status()
        status["in_process_started"] = self._started
        return status


_WORKER: RuntimeWorker | None = None
_WORKER_LOCK = threading.Lock()


def get_runtime_worker() -> RuntimeWorker:
    global _WORKER
    with _WORKER_LOCK:
        if _WORKER is None:
            _WORKER = RuntimeWorker()
        return _WORKER
