from __future__ import annotations

import logging
import os
import socket
import threading
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

from quantmaster.automation.service import AutomationService
from quantmaster.config import get_config

logger = logging.getLogger(__name__)
STANDBY_RETRY_SECONDS = 5.0


class AutomationRuntime:
    def __init__(self, service: AutomationService | None = None):
        self.service = service or AutomationService()
        self.scheduler = None
        self.owner = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        self.started = False
        self.leader = False
        self._lock = threading.RLock()
        self._channel_stop = threading.Event()
        self._channel_threads: dict[str, threading.Thread] = {}
        self._standby_stop = threading.Event()
        self._standby_thread: threading.Thread | None = None

    @property
    def timezone(self) -> ZoneInfo:
        return ZoneInfo(get_config().automation.timezone)

    def start(self) -> bool:
        with self._lock:
            if self.started:
                return self.leader
            self.started = True
            if not get_config().automation.enabled:
                return False
            if not self.service.store.acquire_lease("scheduler", self.owner):
                logger.info("自动化调度由另一进程持有，本进程进入备用态并等待接管")
                self._start_standby_monitor_locked()
                return False
            return self._activate_leader_locked()

    def _activate_leader_locked(self) -> bool:
        """在已经取得租约后启动调度器；调用方必须持有 ``_lock``。"""
        self.leader = True
        try:
            from apscheduler.schedulers.background import BackgroundScheduler
        except ImportError:
            logger.exception("未安装 APScheduler，自动化运行时未启动")
            self.service.store.release_lease("scheduler", self.owner)
            self.leader = False
            return False
        try:
            self.scheduler = BackgroundScheduler(timezone=self.timezone)
            self.scheduler.start()
            self.reload_jobs()
            self.scheduler.add_job(
                self._heartbeat, "interval", seconds=10, id="_lease", replace_existing=True,
                coalesce=True, max_instances=1, misfire_grace_time=10,
            )
            self.scheduler.add_job(
                self.service.dispatcher.dispatch, "interval", seconds=15, id="_outbox",
                replace_existing=True, coalesce=True, max_instances=1, misfire_grace_time=30,
            )
            self.scheduler.add_job(
                self._cleanup,
                "cron", hour=3, minute=15, id="_cleanup", replace_existing=True,
                coalesce=True, max_instances=1, misfire_grace_time=2700,
            )
            self.start_channels()
            return True
        except Exception:
            logger.exception("自动化调度器启动失败")
            if self.scheduler:
                self.scheduler.shutdown(wait=False)
            self.scheduler = None
            self.leader = False
            self.service.store.release_lease("scheduler", self.owner)
            return False

    def _start_standby_monitor_locked(self) -> None:
        if self._standby_thread and self._standby_thread.is_alive():
            return
        self._standby_stop.clear()
        self._standby_thread = threading.Thread(
            target=self._standby_worker,
            name="qm-automation-standby",
            daemon=True,
        )
        self._standby_thread.start()

    def _standby_worker(self) -> None:
        while not self._standby_stop.wait(STANDBY_RETRY_SECONDS):
            with self._lock:
                if not self.started or self.leader:
                    return
                if not self.service.store.acquire_lease("scheduler", self.owner):
                    continue
                if self._activate_leader_locked():
                    logger.info("旧调度租约已释放，本进程已自动接管自动化任务与 Bot 连接")
                return

    def _cleanup(self) -> None:
        from quantmaster.ai.news_sources import NewsSourceStore

        cfg = get_config()
        self.service.store.cleanup(cfg.automation.retention_days)
        NewsSourceStore().cleanup_raw(cfg.news.raw_cache_days)

    def _handle_message(self, actor, text: str) -> None:
        from quantmaster.automation.commands import BotCommandRouter

        router = BotCommandRouter(self.service, self.service.reply)
        self.service.executor.submit(router.handle, actor, text)

    def _channel_worker(self, channel: str) -> None:
        try:
            if channel == "weixin":
                self.service.weixin.poll_forever(self._handle_message, self._channel_stop)
            else:
                account = self.service.store.bot_account("feishu")
                if account:
                    self.service.store.set_bot_status("feishu", account["account_id"], "listening")
                self.service.feishu.listen_forever(self._handle_message, self._channel_stop)
        except Exception as exc:  # pragma: no cover - 外部 SDK/网络错误路径
            logger.exception("%s Bot 接收线程退出", channel)
            account = self.service.store.bot_account(channel)
            if account:
                self.service.store.set_bot_status(channel, account["account_id"], "degraded", str(exc))

    def start_channels(self) -> dict[str, bool]:
        """启动已配置的直连 Bot；重复调用不会创建重复监听线程。"""
        with self._lock:
            if not self.started:
                self.start()
            if not self.leader:
                return {"weixin": False, "feishu": False}
            result: dict[str, bool] = {}
            for channel in ("weixin", "feishu"):
                account = self.service.store.bot_account(channel)
                thread = self._channel_threads.get(channel)
                if not account:
                    result[channel] = False
                    continue
                if thread and thread.is_alive():
                    result[channel] = True
                    continue
                thread = threading.Thread(
                    target=self._channel_worker, args=(channel,),
                    name=f"qm-{channel}-bot", daemon=True,
                )
                self._channel_threads[channel] = thread
                thread.start()
                result[channel] = True
            return result

    def _heartbeat(self) -> None:
        if not self.service.store.acquire_lease("scheduler", self.owner):
            logger.error("自动化调度租约丢失，停止当前调度器")
            self.leader = False
            if self.scheduler:
                self.scheduler.shutdown(wait=False)

    @staticmethod
    def _minutes(value: str) -> int:
        hours, minutes = value.split(":")
        return int(hours) * 60 + int(minutes)

    def _within_schedule(self, schedule: dict) -> bool:
        now = datetime.now(self.timezone)
        if schedule.get("weekdays") and now.weekday() >= 5:
            return False
        current = now.hour * 60 + now.minute
        windows = schedule.get("windows") or ([schedule["window"]] if schedule.get("window") else [])
        return not windows or any(
            self._minutes(start) <= current <= self._minutes(end)
            for start, end in (window.split("-") for window in windows)
        )

    def _scheduled_job(self, name: str) -> None:
        job = self.service.store.job(name)
        if not job or not job["enabled"] or not self._within_schedule(job["schedule"]):
            return
        self.service.run_task(name)

    def reload_jobs(self) -> None:
        if not self.scheduler or not self.leader:
            return
        from apscheduler.triggers.cron import CronTrigger
        from apscheduler.triggers.interval import IntervalTrigger

        for existing in self.scheduler.get_jobs():
            if not existing.id.startswith("_"):
                self.scheduler.remove_job(existing.id)
        for item in self.service.store.jobs():
            if not item["enabled"]:
                self.service.store.set_next_run(item["name"], "")
                continue
            schedule = item["schedule"]
            common = {
                "coalesce": True, "max_instances": 1,
                "misfire_grace_time": 120 if schedule["type"] == "interval" else 2700,
            }
            if schedule["type"] == "interval":
                trigger = IntervalTrigger(
                    minutes=int(schedule["minutes"]), timezone=self.timezone,
                )
                self.scheduler.add_job(
                    self._scheduled_job, trigger, args=[item["name"]], id=item["name"],
                    replace_existing=True, **common,
                )
            else:
                for value in schedule["times"]:
                    hour, minute = map(int, value.split(":"))
                    trigger = CronTrigger(
                        hour=hour, minute=minute, timezone=self.timezone,
                        day_of_week="mon-fri" if schedule.get("weekdays") else None,
                    )
                    self.scheduler.add_job(
                        self._scheduled_job, trigger, args=[item["name"]],
                        id=f"{item['name']}:{value}", replace_existing=True, **common,
                    )
        next_by_name: dict[str, str] = {}
        for scheduled in self.scheduler.get_jobs():
            if scheduled.id.startswith("_"):
                continue
            name = scheduled.id.split(":", 1)[0]
            value = scheduled.next_run_time.isoformat() if scheduled.next_run_time else ""
            if name not in next_by_name or (value and value < next_by_name[name]):
                next_by_name[name] = value
        for name, value in next_by_name.items():
            self.service.store.set_next_run(name, value)

    def reconfigure(self) -> bool:
        """应用刚保存的自动化配置，并按需重建调度器与通道监听。"""
        self.stop()
        with self._lock:
            self.service.weixin.base_url = get_config().automation.weixin_api_base.rstrip("/")
        return self.start()

    def stop(self) -> None:
        threads: list[threading.Thread]
        standby_thread: threading.Thread | None
        with self._lock:
            self._channel_stop.set()
            self._standby_stop.set()
            threads = list(self._channel_threads.values())
            standby_thread = self._standby_thread
            if self.scheduler:
                self.scheduler.shutdown(wait=False)
            if self.leader:
                self.service.store.release_lease("scheduler", self.owner)
            self.scheduler, self.leader, self.started = None, False, False
        if standby_thread and standby_thread is not threading.current_thread():
            standby_thread.join(timeout=2)
        for thread in threads:
            thread.join(timeout=5)
        with self._lock:
            self._channel_threads = {}
            self._channel_stop = threading.Event()
            self._standby_thread = None
            self._standby_stop = threading.Event()


_runtime: AutomationRuntime | None = None
_runtime_root: str = ""


def get_runtime() -> AutomationRuntime:
    global _runtime, _runtime_root
    root = str(get_config().data_root.resolve())
    if _runtime is None or _runtime_root != root:
        if _runtime is not None:
            _runtime.stop()
        _runtime = AutomationRuntime()
        _runtime_root = root
    return _runtime
