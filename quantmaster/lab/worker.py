"""Quant Lab scheduling facade over the shared unified job runtime."""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from quantmaster.config import get_config
from quantmaster.lab.errors import LabError, classify_lab_error
from quantmaster.lab.jobs import LabJobManager, get_lab_job_manager
from quantmaster.lab.service import LabService

logger = logging.getLogger(__name__)


class LabWorker:
    """Keep Lab's calendar policy local; delegate every task lease to UnifiedJobRuntime."""

    def __init__(
        self,
        service: LabService | None = None,
        poll_seconds: float = 1.0,
        *,
        manager: LabJobManager | None = None,
    ) -> None:
        del poll_seconds
        if manager is not None:
            self.jobs = manager
            self.service = manager.service
        elif service is not None:
            self.service = service
            self.jobs = LabJobManager(service=service)
        else:
            self.jobs = get_lab_job_manager()
            self.service = self.jobs.service
        self._accepting = threading.Event()
        self._scheduler: Any = None
        self._lock = threading.RLock()

    @staticmethod
    def _worker_limit() -> int:
        return min(4, max(1, int(get_config().lab.max_workers)))

    def start(self) -> None:
        with self._lock:
            if self._accepting.is_set():
                return
            self._accepting.set()
            self.jobs.resume()
            recovered = self.service.store.recover_orphaned_records(
                self.jobs.live_job_ids(),
            )
            if any(recovered.values()):
                logger.info("Quant Lab recovered orphaned domain records: %s", recovered)
            try:
                self.service.recover_publications(limit=20)
            except (OSError, sqlite3.Error, TypeError, ValueError, KeyError):
                logger.warning("Quant Lab publication recovery deferred", exc_info=True)
            self._start_scheduler_locked()

    def drain(self) -> None:
        """Pause claims and fence in-flight attempts at their cooperative boundary."""
        with self._lock:
            self._accepting.clear()
            if self._scheduler:
                self._scheduler.shutdown(wait=False)
                self._scheduler = None
            self.jobs.pause()

    def stop(self) -> None:
        with self._lock:
            self._accepting.clear()
            if self._scheduler:
                self._scheduler.shutdown(wait=False)
                self._scheduler = None
        self.jobs.shutdown(timeout=5.0)

    def _budget_remaining(self) -> float:
        return max(
            0.0,
            float(get_config().lab.daily_budget_hours) - self.jobs.scheduled_usage_hours(),
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
        if now.isoweekday() > 5:
            return
        day = now.date().isoformat()
        base = {
            "universe": cfg.universe,
            "start": cfg.start,
            "end": day,
            "_scheduled": True,
        }
        try:
            self.service.enqueue(
                "prepare_data", {**base, "_schedule_key": f"lab:daily:{day}:prepare"},
            )
        except (LabError, OSError, RuntimeError, ValueError, KeyError, sqlite3.Error) as exc:
            failure = classify_lab_error(exc)
            logger.info("Quant Lab daily prepare skipped code=%s", failure.code)
        for deployment in self.service.store.active_deployments():
            version_id = str(deployment["version_id"])
            try:
                self.service.enqueue("validate", {
                    **base,
                    "version_id": version_id,
                    "_schedule_key": f"lab:daily:{day}:validate:{version_id}",
                })
            except (LabError, OSError, RuntimeError, ValueError, KeyError, sqlite3.Error) as exc:
                failure = classify_lab_error(exc)
                logger.info(
                    "Quant Lab scheduled validation skipped version=%s code=%s",
                    version_id,
                    failure.code,
                )

    def _enqueue_heavy_research(self) -> None:
        cfg = get_config().lab
        now = datetime.now(ZoneInfo(get_config().automation.timezone))
        day = now.date().isoformat()
        weekly_day = min(cfg.weekly_days) if cfg.weekly_days else 1
        if now.isoweekday() != weekly_day:
            return
        week = f"{now.isocalendar().year}-W{now.isocalendar().week:02d}"
        from quantmaster.lab.research import OptimizationSpec, WalkForwardSpec

        protocol = WalkForwardSpec.from_lab_config(cfg)
        spec = OptimizationSpec(
            universe=cfg.universe,
            start=cfg.start,
            end=day,
            budget_hours=min(10.0, max(0.1, self._budget_remaining())),
            protocol=protocol,
            research_tier="production" if cfg.universe.lower() == "csi800" else "sandbox",
        )
        try:
            self.service.create_study({
                **spec.to_dict(),
                "_scheduled": True,
                "_schedule_key": f"lab:weekly:{week}:optimize",
            })
        except (LabError, OSError, RuntimeError, ValueError, KeyError, sqlite3.Error) as exc:
            failure = classify_lab_error(exc)
            logger.info("Quant Lab scheduled optimization skipped code=%s", failure.code)
        if cfg.ai_python_mining_enabled:
            try:
                self.service.enqueue("discover_python", {
                    "universe": cfg.universe,
                    "start": cfg.start,
                    "end": day,
                    "horizon": 3 if 3 in cfg.horizons else cfg.horizons[0],
                    "rounds": 3,
                    "candidate_limit": 24,
                    "finalists": 3,
                    "_scheduled": True,
                    "_schedule_key": f"lab:weekly:{week}:python",
                })
            except (LabError, OSError, RuntimeError, ValueError, KeyError, sqlite3.Error) as exc:
                failure = classify_lab_error(exc)
                logger.info("Quant Lab scheduled Python AutoMiner skipped code=%s", failure.code)

    def _recover_publications(self) -> None:
        if not self._accepting.is_set():
            return
        try:
            result = self.service.recover_publications(limit=5)
            if result["attempted"]:
                logger.info("Quant Lab publication recovery: %s", result)
        except (OSError, sqlite3.Error, TypeError, ValueError, KeyError):
            logger.warning("Quant Lab publication recovery tick deferred", exc_info=True)

    def _start_scheduler_locked(self) -> None:
        if self._scheduler is not None or not self._accepting.is_set():
            return
        try:
            from apscheduler.schedulers.background import BackgroundScheduler
            from apscheduler.triggers.cron import CronTrigger
        except ImportError:  # pragma: no cover
            logger.error("未安装 APScheduler，Quant Lab 自动研究未启动")
            return
        cfg = get_config()
        hour, minute = map(int, cfg.lab.window_start.split(":"))
        self._scheduler = BackgroundScheduler(timezone=ZoneInfo(cfg.automation.timezone))
        trigger = CronTrigger(hour=hour, minute=minute, timezone=self._scheduler.timezone)
        self._scheduler.add_job(
            self._enqueue_daily,
            trigger,
            id="lab-daily",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=3600,
        )
        self._scheduler.add_job(
            self._enqueue_heavy_research,
            trigger,
            id="lab-heavy",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
            misfire_grace_time=3600,
        )
        self._scheduler.add_job(
            self._recover_publications,
            "interval",
            minutes=1,
            id="lab-publications",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
        )
        self._scheduler.start()

    def rebuild_scheduler(self) -> str:
        with self._lock:
            if not self._accepting.is_set():
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
            return {"status": "disabled"}
        if changed & {"lab.window_start", "automation.timezone"}:
            return {"status": self.rebuild_scheduler()}
        lab_changed = any(field.startswith("lab.") for field in changed)
        return {"status": "applied" if lab_changed else "unchanged"}

    def status(self) -> dict[str, Any]:
        active_job_ids = self.jobs.active_job_ids()
        snapshot = self.jobs.snapshot()
        if self._accepting.is_set():
            state = "running"
        elif active_job_ids:
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
            "runtime": snapshot,
        }


_worker: LabWorker | None = None
_worker_root = ""


def get_worker() -> LabWorker:
    global _worker, _worker_root
    root = str(get_config().data_root.resolve())
    if _worker is None or _worker_root != root:
        if _worker is not None:
            _worker.stop()
        _worker = LabWorker(manager=get_lab_job_manager())
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
