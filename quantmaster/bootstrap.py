"""Application composition root for durable QuantMaster worker processes."""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from quantmaster.bootstrap_hooks import server_worker_hooks
from quantmaster.config import get_config
from quantmaster.data.free_stockdb_runtime import StockDBUpdateEvent
from quantmaster.runtime.identity import get_application_identity
from quantmaster.runtime.supervisor import (
    WorkerSupervisor,
    publish_worker_supervisor_status,
)
from quantmaster.runtime.worker import RuntimeWorker, WorkerPlan
from quantmaster.runtime.worker_ipc import WorkerCommandError

logger = logging.getLogger(__name__)


class _StopEvent(Protocol):
    def wait(self, timeout: float | None = None) -> bool: ...


class StockDBEventSource(Protocol):
    def claim_update_event(self) -> StockDBUpdateEvent | None: ...

    def complete_update_event(self, event_key: str) -> None: ...


class StockDBEventDelivery:
    """Interpret durable StockDB evidence and dispatch its concrete consumers."""

    def __init__(
        self,
        source: StockDBEventSource,
        *,
        after_close_jobs: Any,
        rotation_worker: Any,
        automation_runtime: Any,
        paper_automation_worker: Any,
        reset_after_close: Callable[[], None],
        reset_etf_research: Callable[[], None],
    ) -> None:
        self.source = source
        self.after_close_jobs = after_close_jobs
        self.rotation_worker = rotation_worker
        self.automation_runtime = automation_runtime
        self.paper_automation_worker = paper_automation_worker
        self.reset_after_close = reset_after_close
        self.reset_etf_research = reset_etf_research
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def deliver(self, event: StockDBUpdateEvent) -> None:
        kind = event.kind
        payload = event.payload
        cfg = get_config()
        if kind in {
            "update_succeeded", "market_session_available", "market_session_partial",
        }:
            from quantmaster.rotation.contracts import RotationJobSpec

            target = str(payload.get("target_session") or "")
            if kind in {"update_succeeded", "market_session_partial"}:
                self.reset_after_close()
                self.reset_etf_research()
            if (
                kind == "update_succeeded"
                and cfg.data.after_close_enabled
                and cfg.data.after_close_auto_run
            ):
                self.after_close_jobs.submit(as_of=target, force=False)
                logger.info("free-stockdb 验收完成，已提交 %s 盘后研究扫描", target)
            self.rotation_worker.submit(
                RotationJobSpec(scope="all", source="auto", as_of=target),
            )
            if kind == "update_succeeded" and target and cfg.automation.enabled:
                self.automation_runtime.service.run_task(
                    "daily_close_pipeline",
                    actor="free-stockdb",
                    as_of=target,
                    business_key=f"daily_close_pipeline:date:{target}",
                )
                logger.info("free-stockdb 验收完成，已提交 %s 正式选股流水线", target)
            if kind == "update_succeeded" and target:
                requeued = self.paper_automation_worker.requeue_market_data(target)
                if requeued:
                    logger.info(
                        "free-stockdb 验收完成，已重新唤醒 %s 个因行情证据失败的模拟账户（%s）",
                        requeued,
                        target,
                    )
            logger.info(
                "free-stockdb %s，已提交 %s 观察刷新",
                "验收完成" if kind == "update_succeeded" else "目标交易日部分数据可用",
                target or "最近完成交易日",
            )
            return
        if kind != "update_failed" or not (
            cfg.data.after_close_notify and cfg.automation.enabled
        ):
            return
        from quantmaster.automation.models import AlertEvent, stable_hash

        target = str(payload.get("target_session") or "未知")
        validation = dict(payload.get("validation") or {})
        actual = str(validation.get("actual_session") or "未知")
        message = str(payload.get("message") or "真实交易日验收未通过")[:500]
        attempts = payload.get("attempt") or "未知"
        self.automation_runtime.service.process_event(AlertEvent(
            kind="task_failure", score=100, severity="warning",
            data_as_of=datetime.now(UTC).isoformat(),
            evidence=[
                f"目标交易日 {target}；本地实际 {actual}", message,
                f"自动更新已尝试 {attempts} 次",
            ],
            dedupe_key=stable_hash({"free_stockdb_update_failed": target}),
            payload={"title": "free-stockdb 自动更新未完成", "target_session": target},
        ))

    def poll_once(self) -> bool:
        event = self.source.claim_update_event()
        if event is None:
            return False
        self.deliver(event)
        self.source.complete_update_event(event.event_key)
        return True

    def _run(self) -> None:
        while not self._stop.wait(1.0):
            try:
                self.poll_once()
            except (ImportError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
                logger.warning("free-stockdb 更新事件消费失败", exc_info=True)
                self._stop.wait(2.0)

    def start(self) -> None:
        self._stop.clear()
        if self._thread is None or not self._thread.is_alive():
            self._thread = threading.Thread(
                target=self._run, name="free-stockdb-event-bridge", daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread and thread is not threading.current_thread():
            thread.join(timeout=2)
        self._thread = None


class _DefaultWorkerPlan:
    """Own the concrete services and ordering hidden from ``runtime.worker``."""

    def __init__(self) -> None:
        from quantmaster.after_close.jobs import (
            get_after_close_jobs,
            shutdown_after_close_jobs,
        )
        from quantmaster.after_close.service import reset_after_close_service
        from quantmaster.ai.news_jobs import get_news_jobs, shutdown_news_jobs
        from quantmaster.analysis.stock_jobs import (
            get_stock_analysis_jobs,
            shutdown_stock_analysis_jobs,
        )
        from quantmaster.automation.runtime import get_runtime
        from quantmaster.backtest.jobs import (
            get_backtest_job_manager,
            shutdown_backtest_job_managers,
        )
        from quantmaster.backtest.paper_automation import get_paper_automation_worker
        from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime
        from quantmaster.data.instruments import InstrumentStore
        from quantmaster.data.maintenance import data_refresh_manager
        from quantmaster.data.repair import get_data_repair_manager
        from quantmaster.lab.capabilities import publish_capabilities
        from quantmaster.lab.llm_jobs import get_lab_llm_jobs, shutdown_lab_llm_jobs
        from quantmaster.lab.worker import get_worker
        from quantmaster.market import get_cnn_fear_greed_refresher
        from quantmaster.market.overview_snapshot import publish_market_overview_snapshot
        from quantmaster.research.jobs import get_research_job_manager
        from quantmaster.rotation.etf_jobs import (
            get_etf_research_jobs,
            shutdown_etf_research_jobs,
        )
        from quantmaster.rotation.etf_research import reset_etf_research_service
        from quantmaster.rotation.service import get_rotation_worker
        from quantmaster.worker_components import register_worker_components

        register_worker_components()
        (
            settings_manager,
            get_settings_jobs,
            shutdown_settings_jobs,
            start_diagnostics_sampler,
            stop_diagnostics_sampler,
        ) = server_worker_hooks()

        # This installs only the bundled offline catalogue. It must not
        # trigger a remote catalogue refresh at worker startup.
        InstrumentStore()
        self.runtime = get_runtime()
        self.lab_worker = get_worker()
        self.backtest_jobs = get_backtest_job_manager()
        self.research_worker = get_research_job_manager()
        self.rotation_worker = get_rotation_worker()
        self.repair_worker = get_data_repair_manager()
        self.stock_analysis_worker = get_stock_analysis_jobs()
        self.after_close_worker = get_after_close_jobs()
        self.etf_research_worker = get_etf_research_jobs()
        self.news_worker = get_news_jobs()
        self.settings_worker = get_settings_jobs()
        self.lab_llm_worker = get_lab_llm_jobs()
        self.cnn_fear_greed_refresher = get_cnn_fear_greed_refresher()
        self.paper_automation_worker = get_paper_automation_worker()
        self.data_refresh_manager = data_refresh_manager
        self.free_stockdb_runtime = free_stockdb_runtime
        self.stockdb_event_delivery = StockDBEventDelivery(
            free_stockdb_runtime,
            after_close_jobs=self.after_close_worker,
            rotation_worker=self.rotation_worker,
            automation_runtime=self.runtime,
            paper_automation_worker=self.paper_automation_worker,
            reset_after_close=reset_after_close_service,
            reset_etf_research=reset_etf_research_service,
        )
        self.settings_manager = settings_manager
        self._publish_capabilities = publish_capabilities
        self._publish_market_overview_snapshot = publish_market_overview_snapshot
        self._start_diagnostics_sampler = start_diagnostics_sampler
        self._stop_diagnostics_sampler = stop_diagnostics_sampler
        self._shutdown_stock_analysis_jobs = shutdown_stock_analysis_jobs
        self._shutdown_after_close_jobs = shutdown_after_close_jobs
        self._shutdown_etf_research_jobs = shutdown_etf_research_jobs
        self._shutdown_news_jobs = shutdown_news_jobs
        self._shutdown_settings_jobs = shutdown_settings_jobs
        self._shutdown_lab_llm_jobs = shutdown_lab_llm_jobs
        self._shutdown_backtest_jobs = shutdown_backtest_job_managers

    def settings_projection(self) -> tuple[int, int]:
        from quantmaster.settings_runtime import public_state

        state = public_state(self.settings_manager.path)
        return int(state["persisted_revision"]), int(state["latest_generation"])

    def _publish_lab_capabilities(self) -> None:
        try:
            self._publish_capabilities()
        except (OSError, RuntimeError, ValueError, TypeError):
            logger.warning("Quant Lab 能力快照发布失败", exc_info=True)

    def _publish_market_overview(self) -> None:
        try:
            self._publish_market_overview_snapshot()
        except (OSError, RuntimeError, ValueError, TypeError):
            logger.warning("市场总览快照发布失败", exc_info=True)

    def _publish_async(self, target: Callable[[], None], name: str) -> None:
        threading.Thread(target=target, name=name, daemon=True).start()

    def start(self, *, bootstrap_rotation: bool) -> None:
        self.runtime.start()
        self.research_worker.start()
        self.stock_analysis_worker.start()
        self.after_close_worker.start()
        self.etf_research_worker.start()
        self.news_worker.start()
        self.settings_worker.start()
        self.lab_llm_worker.runtime.start()
        self._start_diagnostics_sampler()
        self.stockdb_event_delivery.start()
        self.data_refresh_manager.start()
        self.repair_worker.start()
        self.backtest_jobs.start()
        self.paper_automation_worker.start()
        self.rotation_worker.start(bootstrap_local=bootstrap_rotation)
        self.cnn_fear_greed_refresher.start()
        self._publish_async(
            self._publish_market_overview, "quant-market-overview-publish",
        )
        if get_config().lab.enabled:
            self.lab_worker.start()
            self._publish_async(
                self._publish_lab_capabilities, "quant-lab-capabilities-publish",
            )

    def drain(self) -> None:
        self.cnn_fear_greed_refresher.stop()
        self.paper_automation_worker.stop()
        self.rotation_worker.stop()
        self.repair_worker.shutdown()
        self.data_refresh_manager.shutdown()
        self.research_worker.shutdown()
        self.backtest_jobs.shutdown()
        self.lab_worker.stop()
        self.runtime.stop()
        self.stock_analysis_worker.pause()
        self.after_close_worker.pause()
        self.etf_research_worker.pause()
        self.news_worker.pause()
        self.settings_worker.pause()
        self.lab_llm_worker.runtime.pause()

    def resume(self) -> None:
        self.cnn_fear_greed_refresher.start()
        self.stock_analysis_worker.resume()
        self.after_close_worker.resume()
        self.etf_research_worker.resume()
        self.news_worker.resume()
        self.settings_worker.resume()
        self.lab_llm_worker.runtime.resume()
        self.runtime.start()
        self.research_worker.start()
        self.data_refresh_manager.start()
        self.repair_worker.start()
        self.backtest_jobs.start()
        self.paper_automation_worker.start()
        self.rotation_worker.start()
        if get_config().lab.enabled:
            self.lab_worker.start()
        self._publish_async(
            self._publish_market_overview, "quant-market-overview-publish",
        )

    def idle(self) -> bool:
        return bool(
            not self.data_refresh_manager.active
            and self.rotation_worker.idle
            and self.paper_automation_worker.idle
            and self.stock_analysis_worker.idle
            and self.after_close_worker.idle
            and self.etf_research_worker.idle
            and self.news_worker.idle
            and self.settings_worker.idle
            and self.lab_llm_worker.runtime.idle
            and self.backtest_jobs.idle
        )

    def _apply_latest_settings(self, payload: dict[str, Any]) -> dict[str, Any]:
        from quantmaster.config import load_config, set_config
        from quantmaster.settings_runtime import persisted_revision

        revision = int(payload.get("revision") or 0)
        generation = int(payload.get("generation") or 0)
        latest = persisted_revision(self.settings_manager.path)
        if revision < latest:
            return {
                "status": "superseded",
                "revision": revision,
                "latest_revision": latest,
                "generation": generation,
            }
        set_config(load_config())
        return {"status": "effective", "revision": latest, "generation": generation}

    def handle_command(
        self, operation: str, payload: dict[str, Any],
    ) -> dict[str, Any]:
        if operation == "data.refresh.preview":
            return self.data_refresh_manager.preview(
                str(payload.get("scope") or "market"),
                str(payload.get("universe") or ""),
                str(payload.get("start") or ""),
            )
        if operation == "data.refresh.create":
            return self.data_refresh_manager.create(
                str(payload.get("scope") or "market"),
                str(payload.get("universe") or ""),
                str(payload.get("start") or ""),
            )
        if operation == "data.refresh.cancel":
            return self.data_refresh_manager.cancel(str(payload.get("job_id") or ""))
        if operation == "data.refresh.retry":
            return self.data_refresh_manager.resume(str(payload.get("job_id") or ""))
        if operation == "automation.apply_config":
            from quantmaster.config import load_config, set_config

            set_config(load_config())
            changed = [str(value) for value in payload.get("changed_fields") or []]
            return self.runtime.apply_config(changed)
        if operation == "settings.apply.latest":
            return self._apply_latest_settings(payload)
        if operation == "settings.diagnostic.create":
            from quantmaster.settings import SettingsDocument

            document = SettingsDocument.model_validate(payload.get("document") or {})
            task, created = self.settings_worker._submit_diagnostic_local(
                str(payload.get("kind") or ""),
                document,
                api_key=str(payload.get("api_key") or ""),
            )
            return {"task": task, "created": created}
        raise WorkerCommandError("unknown_command", "后台执行器不支持该命令")

    def stop(self, enter_phase: Callable[[str, float], None]) -> None:
        enter_phase("stop_producers", 5.0)
        self._stop_diagnostics_sampler()
        self.stockdb_event_delivery.stop()
        self.cnn_fear_greed_refresher.stop()
        # Scheduler/channel owners stop before durable workers so no producer
        # can submit during the durable drain.
        self.runtime.close()
        self.paper_automation_worker.stop()
        enter_phase("drain_atomic", 10.0)
        self.rotation_worker.shutdown()
        self.repair_worker.shutdown()
        self.data_refresh_manager.shutdown()
        self.research_worker.shutdown()
        self._shutdown_backtest_jobs()
        self.lab_worker.stop()
        enter_phase("persist_and_release", 10.0)
        self._shutdown_stock_analysis_jobs()
        self._shutdown_after_close_jobs()
        self._shutdown_etf_research_jobs()
        self._shutdown_news_jobs()
        self._shutdown_settings_jobs()
        self._shutdown_lab_llm_jobs()


def build_worker_plan() -> WorkerPlan:
    return _DefaultWorkerPlan()


_WORKER: RuntimeWorker | None = None
_WORKER_LOCK = threading.Lock()


def get_runtime_worker() -> RuntimeWorker:
    global _WORKER
    with _WORKER_LOCK:
        if _WORKER is None:
            _WORKER = RuntimeWorker(build_worker_plan)
        return _WORKER


def run_runtime_worker(stop_event: _StopEvent, bootstrap_rotation: bool) -> None:
    """Spawn target kept in the composition root for Windows ``spawn``."""

    from quantmaster.runtime.windows_app import initialize_windows_app_process

    initialize_windows_app_process()
    os.environ["QM_WORKER_SUPERVISOR"] = "1"
    os.environ.pop("QM_WEB_PROCESS", None)
    worker = get_runtime_worker()
    state, detail = "stopped", ""
    try:
        publish_worker_supervisor_status("starting")
        worker.start(bootstrap_rotation=bootstrap_rotation)
        state = "running"
        publish_worker_supervisor_status(state)
        while not stop_event.wait(0.5):
            pass
    except BaseException as exc:
        state = "failed"
        detail = f"{type(exc).__name__}: {exc}"
        try:
            publish_worker_supervisor_status(state, detail=detail)
        except OSError:
            logger.exception("runtime-worker 启动失败且无法写入诊断状态")
        logger.exception("runtime-worker 启动失败")
        raise
    finally:
        try:
            worker.stop()
        finally:
            try:
                publish_worker_supervisor_status(state, detail=detail)
            except OSError:
                logger.warning("runtime-worker 监督状态写入失败", exc_info=True)


_SUPERVISOR: WorkerSupervisor | None = None
_SUPERVISOR_LOCK = threading.Lock()


def get_worker_supervisor() -> WorkerSupervisor:
    global _SUPERVISOR
    get_application_identity()
    with _SUPERVISOR_LOCK:
        root = get_config().data_root
        if _SUPERVISOR is None or _SUPERVISOR.root != Path(root):
            if _SUPERVISOR is not None:
                _SUPERVISOR.stop()
            _SUPERVISOR = WorkerSupervisor(root, target=run_runtime_worker)
        return _SUPERVISOR


def reset_worker_supervisor_for_tests() -> None:
    global _SUPERVISOR
    with _SUPERVISOR_LOCK:
        value, _SUPERVISOR = _SUPERVISOR, None
    if value is not None:
        value.stop(2.0)
