"""Quant Lab 的进程内/独立进程通用 Worker。"""

from __future__ import annotations

import logging
import socket
import threading
import time
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

from quantmaster.config import get_config
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
        self._scheduler = None
        self._lock = threading.RLock()
        self._active_job_id = ""

    def start(self) -> None:
        with self._lock:
            if self._thread and self._thread.is_alive():
                self._accepting.set()
                if self._scheduler is None:
                    self._start_scheduler_locked()
                return
            self._stop.clear()
            self._accepting.set()
            self.service.store.interrupt_stale()
            self._thread = threading.Thread(
                target=self.run_forever, name="quant-lab-worker", daemon=True)
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
        self.service.store.interrupt_stale(self.worker_id)
        self._thread = None

    def run_forever(self) -> None:
        while not self._stop.is_set():
            if not self._accepting.is_set():
                break
            allow_scheduled = self._within_window() and self._budget_remaining() > 0
            job = self.service.store.claim_next(
                self.worker_id, allow_scheduled=allow_scheduled)
            if job is None:
                self._stop.wait(self.poll_seconds)
                continue
            self._active_job_id = str(job.get("id") or "")
            try:
                self.run_one(job)
            finally:
                self._active_job_id = ""

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
            )

        def cancelled() -> bool:
            return self._stop.is_set() or self.service.store.is_cancel_requested(job_id)

        heartbeat_stop = threading.Event()

        def heartbeat() -> None:
            self.service.store.heartbeat_job(job_id)
            while not heartbeat_stop.wait(5.0):
                self.service.store.heartbeat_job(job_id)

        heartbeat_thread = threading.Thread(
            target=heartbeat, name=f"lab-heartbeat-{job_id[:8]}", daemon=True,
        )
        heartbeat_thread.start()
        try:
            result = self.service.run_job(job, progress=progress, cancelled=cancelled)
            self.service.store.finish_job(job_id, result=result)
        except InterruptedError:
            self.service.store.request_cancel(job_id)
            self.service.store.finish_job(job_id)
        except Exception as exc:
            logger.exception("Quant Lab 任务失败 job=%s kind=%s", job_id, job["kind"])
            self.service.store.finish_job(job_id, error=str(exc))
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
        self.service.store.enqueue("prepare_data", base)
        for deployment in self.service.store.active_deployments():
            self.service.store.enqueue("validate", {
                **base, "version_id": deployment["version_id"],
            })

    def _enqueue_heavy_research(self) -> None:
        cfg = get_config().lab
        now = datetime.now(ZoneInfo(get_config().automation.timezone))
        day = now.date().isoformat()
        if now.isoweekday() not in cfg.weekly_days:
            return
        if not self.service.store.reserve_schedule(f"heavy:{day}"):
            return
        base = {
            "universe": cfg.universe, "start": cfg.start, "end": day,
            "horizon": 3 if 3 in cfg.horizons else cfg.horizons[0], "_scheduled": True,
        }
        self.service.store.enqueue("discover_genetic", {
            **base, "population": 60, "generations": 8, "top_n": 10,
        })
        self.service.store.enqueue("train", {
            **base, "model": "ridge", "sequence_length": 20,
            "config": {"seed": 42},
        })

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
            return {"status": "draining" if self._active_job_id else "disabled"}
        if changed & {"lab.window_start", "automation.timezone"}:
            return {"status": self.rebuild_scheduler()}
        lab_changed = any(field.startswith("lab.") for field in changed)
        return {"status": "applied" if lab_changed else "unchanged"}

    def status(self) -> dict:
        alive = bool(self._thread and self._thread.is_alive())
        if not get_config().lab.enabled and alive and self._active_job_id:
            state = "draining"
        elif alive and self._accepting.is_set():
            state = "running"
        elif alive:
            state = "draining"
        else:
            state = "disabled"
        return {
            "status": state,
            "active_job_id": self._active_job_id,
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
                "lab.window_end", "lab.daily_budget_hours", "lab.device",
                "lab.allow_cloud_sample",
            ])
    except KeyboardInterrupt:
        worker.stop()
    finally:
        time.sleep(0)
