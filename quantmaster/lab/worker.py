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
        self._thread: threading.Thread | None = None
        self._scheduler = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self.service.store.interrupt_stale()
        self._thread = threading.Thread(target=self.run_forever, name="quant-lab-worker", daemon=True)
        self._thread.start()
        self._start_scheduler()

    def stop(self) -> None:
        self._stop.set()
        if self._scheduler:
            self._scheduler.shutdown(wait=False)
            self._scheduler = None
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        self.service.store.interrupt_stale(self.worker_id)
        self._thread = None

    def run_forever(self) -> None:
        while not self._stop.is_set():
            allow_scheduled = self._within_window() and self._budget_remaining() > 0
            job = self.service.store.claim_next(
                self.worker_id, allow_scheduled=allow_scheduled)
            if job is None:
                self._stop.wait(self.poll_seconds)
                continue
            self.run_one(job)

    def run_one(self, job: dict) -> None:
        job_id = job["id"]

        def progress(value: int, phase: str) -> None:
            self.service.store.update_job(job_id, value, phase)

        def cancelled() -> bool:
            return self._stop.is_set() or self.service.store.is_cancel_requested(job_id)

        try:
            result = self.service.run_job(job, progress=progress, cancelled=cancelled)
            self.service.store.finish_job(job_id, result=result)
        except InterruptedError:
            self.service.store.request_cancel(job_id)
            self.service.store.finish_job(job_id)
        except Exception as exc:
            logger.exception("Quant Lab 任务失败 job=%s kind=%s", job_id, job["kind"])
            self.service.store.finish_job(job_id, error=str(exc))

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
            "horizon": 3, "_scheduled": True,
        }
        self.service.store.enqueue("discover_genetic", {
            **base, "population": 60, "generations": 8, "top_n": 10,
        })
        self.service.store.enqueue("train", {
            **base, "model": "ridge", "sequence_length": 20,
            "config": {"seed": 42},
        })

    def _start_scheduler(self) -> None:
        try:
            from apscheduler.schedulers.background import BackgroundScheduler
            from apscheduler.triggers.cron import CronTrigger
        except ImportError:  # pragma: no cover - 核心依赖缺失
            logger.exception("未安装 APScheduler，Quant Lab 自动研究未启动")
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


_worker: LabWorker | None = None


def get_worker() -> LabWorker:
    global _worker
    if _worker is None:
        _worker = LabWorker()
    return _worker


def run_standalone() -> None:
    worker = LabWorker()
    try:
        worker.start()
        while worker._thread and worker._thread.is_alive():
            time.sleep(1)
    except KeyboardInterrupt:
        worker.stop()
    finally:
        time.sleep(0)
