"""Quant Lab 的进程内/独立进程通用 Worker。"""

from __future__ import annotations

import logging
import socket
import sqlite3
import threading
import time
import uuid
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

from quantmaster.config import get_config
from quantmaster.lab.errors import LabError, classify_lab_error
from quantmaster.lab.preflight import require_runnable
from quantmaster.lab.service import LabService

logger = logging.getLogger(__name__)


class LabWorker:
    def __init__(self, service: LabService | None = None, poll_seconds: float = 1.0):
        self.service = service or LabService()
        self.poll_seconds = poll_seconds
        self.worker_id = f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
        self._stop = threading.Event()
        self._accepting = threading.Event()
        self._thread: threading.Thread | None = None
        self._scheduler: Any = None
        self._lock = threading.RLock()
        self._task_threads: dict[str, threading.Thread] = {}
        self._active_job_ids: set[str] = set()

    @staticmethod
    def _worker_limit() -> int:
        return min(4, max(1, int(get_config().lab.max_workers)))

    def start(self) -> None:
        # Discovery rows live in the Lab ledger rather than the generic jobs
        # ledger, so register it before any settings rotation can occur.
        from quantmaster.runtime.llm import get_llm_execution_coordinator

        get_llm_execution_coordinator().register_lab_store(self.service.store)
        with self._lock:
            if self._thread and self._thread.is_alive():
                self._accepting.set()
                if self._scheduler is None:
                    self._start_scheduler_locked()
                return
            self._stop.clear()
            self._accepting.set()
            # drain 后重新启用时，旧任务可能仍在正常收尾；此时不能把它们误判为
            # 上一进程遗留任务。只有当前实例确实没有活动任务时才恢复 stale 任务。
            if not self._active_job_ids:
                self.service.store.interrupt_legacy_llm()
                self.service.store.interrupt_stale_llm()
                self.service.store.interrupt_stale()
                recovered = self.service.store.recover_orphaned_records()
                if any(recovered.values()):
                    logger.info("Quant Lab recovered orphaned records: %s", recovered)
            self._thread = threading.Thread(
                target=self.run_forever, name="quant-lab-dispatcher", daemon=True)
            self._thread.start()
            self._start_scheduler_locked()

    def drain(self) -> None:
        """停止领取新任务并关闭调度器，但允许当前研究任务正常完成。"""
        with self._lock:
            self._accepting.clear()
            if self._scheduler:
                self._scheduler.shutdown(wait=False)
                self._scheduler = None

    def stop(self) -> None:
        self._stop.set()
        self._accepting.clear()
        if self._scheduler:
            self._scheduler.shutdown(wait=False)
            self._scheduler = None
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        deadline = time.monotonic() + 5
        with self._lock:
            task_threads = list(self._task_threads.values())
        for thread in task_threads:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            if thread.is_alive():
                thread.join(timeout=remaining)
        self.service.store.interrupt_stale(self.worker_id)
        self._thread = None

    def run_forever(self) -> None:
        while not self._stop.is_set():
            if not self._accepting.is_set():
                break
            recover_publications = getattr(self.service, "recover_publications", None)
            recovered = {"attempted": 0, "published": 0}
            if callable(recover_publications):
                try:
                    recovered = recover_publications(limit=1)
                except (OSError, sqlite3.Error, ValueError, TypeError, KeyError) as exc:
                    logger.error("Quant Lab model publication recovery deferred: %s", exc)
            if recovered["attempted"]:
                logger.info(
                    "Quant Lab model publication recovery attempted=%s published=%s",
                    recovered["attempted"], recovered["published"],
                )
            # 多进程 Worker 可以并存；只恢复心跳真正过期的任务，不能在第二个
            # Worker 启动时把第一个 Worker 的正常任务改成 interrupted。
            self.service.store.interrupt_stale()
            claimed = False
            while not self._stop.is_set() and self._accepting.is_set():
                limit = self._worker_limit()
                with self._lock:
                    capacity = limit - len(self._active_job_ids)
                if capacity <= 0:
                    break
                allow_scheduled = self._within_window() and self._budget_remaining() > 0
                job = self.service.store.claim_next(
                    self.worker_id,
                    allow_scheduled=allow_scheduled,
                    max_running=limit,
                    resource_limits={
                        "gpu": max(1, int(get_config().lab.gpu_max_concurrent_jobs)),
                        "external": 1,
                        "io": 1,
                    },
                )
                if job is None:
                    break
                self._launch(job)
                claimed = True
            if not claimed:
                self._stop.wait(self.poll_seconds)

    def _launch(self, job: dict) -> None:
        job_id = str(job.get("id") or "")
        thread = threading.Thread(
            target=self._run_claimed,
            args=(job,),
            name=f"quant-lab-job-{job_id[:8]}",
            daemon=True,
        )
        with self._lock:
            self._active_job_ids.add(job_id)
            self._task_threads[job_id] = thread
        thread.start()

    def _run_claimed(self, job: dict) -> None:
        job_id = str(job.get("id") or "")
        try:
            self.run_one(job)
        finally:
            with self._lock:
                self._active_job_ids.discard(job_id)
                self._task_threads.pop(job_id, None)

    def _run_claimed_job(self, job: dict, progress, cancelled, lease_alive) -> dict:
        job_id = job["id"]
        preflight = getattr(self.service, "preflight", None)
        if callable(preflight):
            admission = preflight(str(job["kind"]), dict(job.get("params") or {}))
            require_runnable(admission)
        scope = str(job.get("llm_scope") or "")
        revision = str(job.get("llm_revision") or "")
        if scope:
            if not revision:
                raise InterruptedError("旧 AI 发现任务缺少执行版本")
            from quantmaster.runtime.llm import get_llm_execution_coordinator

            with get_llm_execution_coordinator().lease(
                SimpleNamespace(job_id=job_id, cancelled=cancelled), scope, revision,
            ):
                result = self.service.run_job(job, progress=progress, cancelled=cancelled)
            if not self._llm_revision_current(job):
                raise InterruptedError("LLM 配置版本已更新")
        else:
            result = self.service.run_job(job, progress=progress, cancelled=cancelled)
        if lease_alive.is_set():
            self.service.store.finish_job(
                job_id, result=result,
                telemetry=(result.get("telemetry") if isinstance(result, dict) else None),
                expected_worker=self.worker_id,
            )
        return result

    def _llm_revision_current(self, job: dict) -> bool:
        scope = str(job.get("llm_scope") or "")
        if not scope:
            return True
        from quantmaster.runtime.llm import get_llm_execution_coordinator

        return get_llm_execution_coordinator().current(
            scope, str(job.get("llm_revision") or ""),
        )

    def _handle_interrupted_job(self, job_id: str, lease_alive) -> None:
        if not lease_alive.is_set():
            return
        if self._stop.is_set() and not self.service.store.is_cancel_requested(job_id):
            self.service.store.interrupt_stale(self.worker_id)
            return
        self.service.store.request_cancel(job_id)
        self.service.store.finish_job(job_id, expected_worker=self.worker_id)

    def _handle_failed_job(self, job: dict, exc: Exception) -> None:
        job_id = job["id"]
        logger.exception("Quant Lab 任务失败 job=%s kind=%s", job_id, job["kind"])
        failure = classify_lab_error(exc)
        self.service.store.finish_job(
            job_id, error=failure.message, error_info=failure.to_dict(),
            expected_worker=self.worker_id,
        )

    def run_one(self, job: dict) -> None:
        job_id = job["id"]

        def progress(
            value: int,
            phase: str,
            detail: str = "",
            *,
            event_type: str = "progress",
            metadata: dict | None = None,
        ) -> None:
            self.service.store.update_job(
                job_id, value, phase, detail,
                event_type=event_type, metadata=metadata,
                expected_worker=self.worker_id,
            )

        lease_alive = threading.Event()
        lease_alive.set()

        def cancelled() -> bool:
            return (
                self._stop.is_set() or not lease_alive.is_set()
                or self.service.store.is_cancel_requested(job_id)
                or not self._llm_revision_current(job)
            )

        heartbeat_stop = threading.Event()

        def heartbeat() -> None:
            if not self.service.store.heartbeat_job(job_id, self.worker_id):
                lease_alive.clear()
                return
            while not heartbeat_stop.wait(5.0):
                if self.service.store.heartbeat_job(job_id, self.worker_id):
                    continue
                lease_alive.clear()
                return

        heartbeat_thread = threading.Thread(
            target=heartbeat, name=f"lab-heartbeat-{job_id[:8]}", daemon=True,
        )
        heartbeat_thread.start()
        try:
            self._run_claimed_job(job, progress, cancelled, lease_alive)
        except InterruptedError:
            self._handle_interrupted_job(job_id, lease_alive)
        except Exception as exc:
            self._handle_failed_job(job, exc)
        finally:
            heartbeat_stop.set()
            heartbeat_thread.join(timeout=0.5)

    def _budget_remaining(self) -> float:
        return max(
            0.0,
            float(get_config().lab.daily_budget_hours)
            - self.service.store.scheduled_usage_hours(),
        )

    @staticmethod
    def _minutes(value: str) -> int:
        hours, minutes = value.split(":")
        return int(hours) * 60 + int(minutes)

    def _within_window(self) -> bool:
        cfg = get_config()
        now = datetime.now(ZoneInfo(cfg.automation.timezone))
        current = now.hour * 60 + now.minute
        start = self._minutes(cfg.lab.window_start)
        end = self._minutes(cfg.lab.window_end)
        return start <= current <= end if start <= end else current >= start or current <= end

    def _enqueue_daily(self) -> None:
        cfg = get_config().lab
        now = datetime.now(ZoneInfo(get_config().automation.timezone))
        day = now.date().isoformat()
        if now.isoweekday() > 5 or not self.service.store.reserve_schedule(f"daily:{day}"):
            return
        end = day
        base = {
            "universe": cfg.universe, "start": cfg.start, "end": end,
            "_scheduled": True,
        }
        try:
            self.service.enqueue("prepare_data", base)
        except (LabError, OSError, RuntimeError, ValueError, KeyError, sqlite3.Error) as exc:
            failure = classify_lab_error(exc)
            logger.info("Quant Lab daily prepare skipped code=%s", failure.code)
        for deployment in self.service.store.active_deployments():
            try:
                self.service.enqueue("validate", {
                    **base, "version_id": deployment["version_id"],
                })
            except (LabError, OSError, RuntimeError, ValueError, KeyError, sqlite3.Error) as exc:
                failure = classify_lab_error(exc)
                logger.info(
                    "Quant Lab scheduled validation skipped version=%s code=%s",
                    deployment["version_id"], failure.code,
                )

    def _enqueue_heavy_research(self) -> None:
        cfg = get_config().lab
        now = datetime.now(ZoneInfo(get_config().automation.timezone))
        day = now.date().isoformat()
        weekly_day = min(cfg.weekly_days) if cfg.weekly_days else 1
        if now.isoweekday() != weekly_day:
            return
        week = f"{now.isocalendar().year}-W{now.isocalendar().week:02d}"
        if self.service.store.reserve_schedule(f"optimize:{week}"):
            from quantmaster.lab.research import OptimizationSpec, WalkForwardSpec

            protocol = WalkForwardSpec.from_lab_config(cfg)
            spec = OptimizationSpec(
                universe=cfg.universe, start=cfg.start, end=day,
                budget_hours=min(10.0, max(0.1, self._budget_remaining())),
                protocol=protocol,
                research_tier="production" if cfg.universe.lower() == "csi800" else "sandbox",
            )
            try:
                self.service.create_study({**spec.to_dict(), "_scheduled": True})
            except (LabError, OSError, RuntimeError, ValueError, KeyError, sqlite3.Error) as exc:
                failure = classify_lab_error(exc)
                logger.info("Quant Lab scheduled optimization skipped code=%s", failure.code)
        python_slot = f"python:{week}"
        if cfg.ai_python_mining_enabled and self.service.store.reserve_schedule(python_slot):
            try:
                self.service.enqueue("discover_python", {
                    "universe": cfg.universe, "start": cfg.start, "end": day,
                    "horizon": 3 if 3 in cfg.horizons else cfg.horizons[0],
                    "rounds": 3, "candidate_limit": 24, "finalists": 3,
                    "_scheduled": True,
                })
            except (LabError, OSError, RuntimeError, ValueError, KeyError, sqlite3.Error) as exc:
                # A reservation is only a claim on an enqueue attempt.  Keep a
                # preflight/data/LLM failure retryable during the same week.
                try:
                    self.service.store.release_schedule(python_slot)
                except sqlite3.Error:
                    logger.exception("Quant Lab failed to release Python AutoMiner slot=%s", python_slot)
                failure = classify_lab_error(exc)
                logger.info("Quant Lab scheduled Python AutoMiner skipped code=%s", failure.code)

    def _start_scheduler_locked(self) -> None:
        try:
            from apscheduler.schedulers.background import BackgroundScheduler
            from apscheduler.triggers.cron import CronTrigger
        except ImportError:  # pragma: no cover - 核心依赖缺失
            logger.error("未安装 APScheduler，Quant Lab 自动研究未启动")
            return
        cfg = get_config()
        hour, minute = map(int, cfg.lab.window_start.split(":"))
        self._scheduler = BackgroundScheduler(timezone=ZoneInfo(cfg.automation.timezone))
        trigger = CronTrigger(hour=hour, minute=minute, timezone=self._scheduler.timezone)
        self._scheduler.add_job(
            self._enqueue_daily, trigger, id="lab-daily", replace_existing=True,
            coalesce=True, max_instances=1, misfire_grace_time=3600,
        )
        self._scheduler.add_job(
            self._enqueue_heavy_research, trigger, id="lab-heavy", replace_existing=True,
            coalesce=True, max_instances=1, misfire_grace_time=3600,
        )
        self._scheduler.start()

    def rebuild_scheduler(self) -> str:
        with self._lock:
            if not self._thread or not self._thread.is_alive() or not self._accepting.is_set():
                return "disabled"
            if self._scheduler:
                self._scheduler.shutdown(wait=False)
                self._scheduler = None
            self._start_scheduler_locked()
            return "applied" if self._scheduler else "degraded"

    def apply_config(self, changed_fields: list[str]) -> dict[str, str]:
        changed = set(changed_fields)
        if "lab.enabled" in changed:
            if get_config().lab.enabled:
                self.start()
                return {"status": "applied"}
            self.drain()
            return {"status": "draining" if self._active_job_ids else "disabled"}
        if changed & {"lab.window_start", "automation.timezone"}:
            return {"status": self.rebuild_scheduler()}
        lab_changed = any(field.startswith("lab.") for field in changed)
        return {"status": "applied" if lab_changed else "unchanged"}

    def status(self) -> dict:
        alive = bool(self._thread and self._thread.is_alive())
        with self._lock:
            active_job_ids = sorted(self._active_job_ids)
        if not get_config().lab.enabled and active_job_ids:
            state = "draining"
        elif alive and self._accepting.is_set():
            state = "running"
        elif alive or active_job_ids:
            state = "draining"
        else:
            state = "disabled"
        return {
            "status": state,
            "active_job_id": active_job_ids[0] if active_job_ids else "",
            "active_job_ids": active_job_ids,
            "max_workers": self._worker_limit(),
            "accepting": self._accepting.is_set(),
            "scheduler": bool(self._scheduler),
        }


_worker: LabWorker | None = None
_worker_root = ""


def get_worker() -> LabWorker:
    global _worker, _worker_root
    root = str(get_config().data_root.resolve())
    if _worker is None or _worker_root != root:
        if _worker is not None:
            _worker.stop()
        _worker = LabWorker()
        _worker_root = root
    return _worker


def run_standalone() -> None:
    from quantmaster.settings import ConfigManager

    manager = ConfigManager()
    worker = LabWorker()
    last_mtime = manager.path.stat().st_mtime_ns if manager.path.is_file() else 0
    try:
        if get_config().lab.enabled:
            worker.start()
        while True:
            time.sleep(1)
            mtime = manager.path.stat().st_mtime_ns if manager.path.is_file() else 0
            if mtime == last_mtime:
                continue
            last_mtime = mtime
            from quantmaster.config import set_config

            set_config(manager.load())
            worker.apply_config([
                "lab.enabled", "lab.window_start", "automation.timezone",
                "lab.universe", "lab.start", "lab.horizons", "lab.weekly_days",
                "lab.window_end", "lab.daily_budget_hours", "lab.max_workers", "lab.device",
                "lab.data_policy", "lab.panel_cache_mb", "lab.feature_cache_gb",
                "lab.gpu_memory_fraction", "lab.gpu_max_concurrent_jobs",
                "lab.allow_cloud_sample", "lab.ai_python_mining_enabled",
            ])
    except KeyboardInterrupt:
        worker.stop()
    finally:
        time.sleep(0)
