from __future__ import annotations

import logging
import os
import socket
import threading
import uuid
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from quantmaster.automation.channels.feishu import feishu_connection_error
from quantmaster.automation.service import AutomationService
from quantmaster.config import get_config

logger = logging.getLogger(__name__)
STANDBY_RETRY_SECONDS = 5.0
FEISHU_RECONNECT_INITIAL_SECONDS = 2.0
FEISHU_RECONNECT_MAX_SECONDS = 60.0


class AutomationRuntime:
    def __init__(self, service: AutomationService | None = None):
        self.service = service or AutomationService()
        self.scheduler: Any | None = None
        self.owner = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        self.started = False
        self.leader = False
        self._lock = threading.RLock()
        self._channel_stops = {name: threading.Event() for name in ("weixin", "feishu")}
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
            if getattr(self.service, "_closed", False):
                raise RuntimeError("自动化运行时 generation 已关闭")
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
        if not self._start_scheduler_locked():
            self.service.store.release_lease("scheduler", self.owner)
            self.leader = False
            return False
        self.service.dispatcher.start()
        jobs = getattr(self.service, "jobs", None)
        if jobs is not None:
            jobs.start()
        self.start_channels()
        return True

    def _start_scheduler_locked(self) -> bool:
        """按当前配置创建调度器；调用方必须持有 ``_lock``。"""
        try:
            from apscheduler.schedulers.background import BackgroundScheduler
        except ImportError:
            logger.error("未安装 APScheduler，自动化运行时未启动")
            return False
        try:
            self.scheduler = BackgroundScheduler(timezone=self.timezone)
            self.scheduler.start()
            self.reload_jobs()
            self.catch_up_daily_jobs()
            self.scheduler.add_job(
                self._heartbeat, "interval", seconds=10, id="_lease", replace_existing=True,
                coalesce=True, max_instances=1, misfire_grace_time=10,
            )
            if self.service.dispatcher.enabled():
                self.scheduler.add_job(
                    self.service.dispatcher.wake, "interval", seconds=15, id="_outbox",
                    replace_existing=True, coalesce=True, max_instances=1,
                    misfire_grace_time=30,
                )
            self.scheduler.add_job(
                self._cleanup,
                "cron", hour=3, minute=15, id="_cleanup", replace_existing=True,
                coalesce=True, max_instances=1, misfire_grace_time=2700,
            )
            return True
        except Exception:
            logger.exception("自动化调度器启动失败")
            if self.scheduler:
                self.scheduler.shutdown(wait=False)
            self.scheduler = None
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

        if not self.started or getattr(self.service, "_closed", False):
            return
        router = BotCommandRouter(self.service, self.service.reply)
        try:
            self.service.executor.submit(router.handle, actor, text)
        except RuntimeError:
            # Shutdown won the admission race.  The message stays in the
            # channel/provider's durable delivery path; this generation must
            # not revive its closed executor.
            if self.started:
                raise

    def _channel_worker(self, channel: str) -> None:
        stop_event = self._channel_stops[channel]
        retry_delay = FEISHU_RECONNECT_INITIAL_SECONDS
        while not stop_event.is_set():
            try:
                if channel == "weixin":
                    self.service.weixin.poll_forever(self._handle_message, stop_event)
                else:
                    self.service.feishu.listen_forever(self._handle_message, stop_event)
                return
            except Exception as exc:  # pragma: no cover - 外部 SDK/网络错误路径
                normal_close = stop_event.is_set() or any(
                    token in str(exc).casefold()
                    for token in ("normal closure", "closed normally", "close code 1000")
                )
                if normal_close:
                    logger.info("%s Bot 接收线程正常退出", channel)
                    return

                diagnostic = (feishu_connection_error(exc) if channel == "feishu" else {
                    "kind": "unknown", "retryable": False, "summary": str(exc)[:500],
                })
                account = self.service.store.bot_account(channel)
                if account:
                    state = "degraded"
                    if channel == "feishu":
                        classify = getattr(self.service.feishu, "failure_state", None)
                        if callable(classify):
                            state = classify(exc)
                    self.service.store.set_bot_status(
                        channel, account["account_id"], state,
                        f"{diagnostic['kind']}: {diagnostic['summary']}",
                    )
                if channel != "feishu" or not diagnostic["retryable"]:
                    logger.exception(
                        "%s Bot 接收线程异常退出 kind=%s；详情已脱敏记录",
                        channel, diagnostic["kind"],
                    )
                    return

                logger.warning(
                    "飞书 Bot 连接中断 kind=%s；%.0f 秒后重连，TLS 校验未被绕过",
                    diagnostic["kind"], retry_delay, exc_info=True,
                )
                if stop_event.wait(retry_delay):
                    return
                retry_delay = min(retry_delay * 2, FEISHU_RECONNECT_MAX_SECONDS)

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
                if channel == "feishu" and not self.service.feishu.is_configured():
                    # Missing credentials are a terminal local condition, not
                    # a retryable transport failure.  Do not create a worker.
                    self.service.store.set_bot_status(
                        channel, account["account_id"], "not_configured",
                    )
                    result[channel] = False
                    continue
                if thread and thread.is_alive():
                    result[channel] = True
                    continue
                thread = threading.Thread(
                    target=self._channel_worker, args=(channel,),
                    name=f"qm-{channel}-bot", daemon=True,
                )
                self._channel_stops[channel].clear()
                self._channel_threads[channel] = thread
                thread.start()
                result[channel] = True
            return result

    def _heartbeat(self) -> None:
        if not self.service.store.acquire_lease("scheduler", self.owner):
            logger.error("自动化调度租约丢失，停止当前调度器")
            self.leader = False
            dispatcher = getattr(self.service, "dispatcher", None)
            if dispatcher is not None:
                dispatcher.stop(timeout=0)
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
        self.discover_job(name)

    def _business_request(
        self, name: str, now: datetime | None = None,
    ) -> tuple[str, str, str]:
        """Return business key, explicit as-of and durable interval boundary."""

        current = (now or datetime.now(self.timezone)).astimezone(self.timezone)
        job = self.service.store.job(name)
        if not job:
            raise KeyError(name)
        schedule = job["schedule"]
        if schedule["type"] == "interval":
            minutes = max(1, int(schedule["minutes"]))
            epoch = int(current.timestamp())
            window_end_epoch = epoch - epoch % (minutes * 60)
            window_end = datetime.fromtimestamp(window_end_epoch, self.timezone)
            window_start = window_end - timedelta(minutes=minutes)
            return (
                f"{name}:window:{window_start.isoformat()}:{window_end.isoformat()}",
                "",
                str(float(window_end_epoch)),
            )
        business_key, as_of = self.service.business_request(name, now=current)
        return business_key, as_of, ""

    def discover_job(self, name: str, *, now: datetime | None = None, actor: str = "scheduler") -> dict:
        """Fast APS callback: discover and wake one durable business task."""

        current = (now or datetime.now(self.timezone)).astimezone(self.timezone)
        business_key, as_of, boundary = self._business_request(name, current)
        if boundary:
            end_epoch = float(boundary)
            previous = self.service.store.scheduler_cursor(name)
            if previous and previous < end_epoch:
                start = datetime.fromtimestamp(previous, self.timezone)
                end = datetime.fromtimestamp(end_epoch, self.timezone)
                business_key = f"{name}:window:{start.isoformat()}:{end.isoformat()}"
            result = self.service.run_task(
                name, actor=actor, business_key=business_key, as_of=as_of,
            )
            self.service.store.advance_scheduler_cursor(name, end_epoch)
            return result
        return self.service.run_task(
            name, actor=actor, business_key=business_key, as_of=as_of,
        )

    def catch_up_daily_jobs(self, *, now: datetime | None = None) -> list[dict]:
        """On leader start, discover each due daily business date once."""

        current = (now or datetime.now(self.timezone)).astimezone(self.timezone)
        recovered: list[dict] = []
        for item in self.service.store.jobs():
            schedule = item["schedule"]
            if not item["enabled"] or schedule.get("type") != "daily":
                continue
            if schedule.get("weekdays") and current.weekday() >= 5:
                continue
            if not any(value <= current.strftime("%H:%M") for value in schedule.get("times") or ()):
                continue
            recovered.append(self.discover_job(item["name"], now=current, actor="startup_recovery"))
        return recovered

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

    def rebuild_scheduler(self) -> str:
        """热重建任务触发器，不断开已经运行的消息通道。"""
        with self._lock:
            if not self.started:
                return "disabled"
            if not self.leader:
                return "standby"
            if self.scheduler:
                self.scheduler.shutdown(wait=False)
                self.scheduler = None
            return "applied" if self._start_scheduler_locked() else "degraded"

    def restart_channel(self, channel: str) -> str:
        """只重启指定消息通道；凭据更新不会干扰另一通道或调度器。"""
        if channel not in self._channel_stops:
            raise ValueError(f"未知消息通道: {channel}")
        with self._lock:
            thread = self._channel_threads.pop(channel, None)
            stop_event = self._channel_stops[channel]
            stop_event.set()
        if thread and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=5)
        with self._lock:
            self._channel_stops[channel] = threading.Event()
            if not get_config().automation.enabled or not self.started:
                return "disabled"
            if not self.leader:
                return "standby"
            account = self.service.store.bot_account(channel)
            if not account:
                return "disabled"
            if channel == "feishu" and not self.service.feishu.is_configured():
                self.service.store.set_bot_status(
                    channel, account["account_id"], "not_configured",
                )
                return "not_configured"
            replacement = threading.Thread(
                target=self._channel_worker, args=(channel,),
                name=f"qm-{channel}-bot", daemon=True,
            )
            self._channel_threads[channel] = replacement
            replacement.start()
            return "applying"

    def stop_channel(self, channel: str) -> str:
        if channel not in self._channel_stops:
            raise ValueError(f"未知消息通道: {channel}")
        with self._lock:
            thread = self._channel_threads.pop(channel, None)
            self._channel_stops[channel].set()
        if thread and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=5)
        with self._lock:
            self._channel_stops[channel] = threading.Event()
        return "disabled"

    def apply_config(self, changed_fields: list[str]) -> dict[str, str]:
        """把已落盘配置安全应用到当前进程。"""
        changed = set(changed_fields)
        if "automation.weixin_api_base" in changed:
            self.service.weixin.base_url = get_config().automation.weixin_api_base.rstrip("/")
        if "automation.enabled" in changed:
            active = self.reconfigure()
            state = "applied" if active else (
                "disabled" if not get_config().automation.enabled else "standby"
            )
            return {"status": state}
        if "automation.timezone" in changed:
            return {"status": self.rebuild_scheduler()}
        news_intervals = {
            "automation.fast_news_interval_minutes",
            "automation.official_news_interval_minutes",
            "automation.periodic_news_interval_minutes",
        }
        if changed & news_intervals:
            self.service.store.sync_news_intervals()
            return {"status": self.rebuild_scheduler()}
        return {"status": "applied" if changed & {
            "automation.primary_universe", "automation.watchlist",
            "automation.sentinel_indices", "automation.retention_days",
            "automation.weixin_api_base",
        } else "unchanged"}

    def status(self) -> dict:
        if not get_config().automation.enabled:
            state = "disabled"
        elif self.leader and self.scheduler:
            state = "running"
        elif self.started:
            state = "standby" if not self.leader else "degraded"
        else:
            state = "degraded"
        return {
            "status": state,
            "started": self.started,
            "leader": self.leader,
            "channels": {
                name: bool(thread and thread.is_alive())
                for name, thread in self._channel_threads.items()
            },
        }

    def stop(self) -> None:
        threads: list[threading.Thread]
        standby_thread: threading.Thread | None
        with self._lock:
            for event in self._channel_stops.values():
                event.set()
            self._standby_stop.set()
            threads = list(self._channel_threads.values())
            standby_thread = self._standby_thread
            if self.scheduler:
                self.scheduler.shutdown(wait=False)
            if self.leader:
                self.service.store.release_lease("scheduler", self.owner)
            jobs = getattr(self.service, "jobs", None)
            if jobs is not None:
                jobs.pause()
            self.scheduler, self.leader, self.started = None, False, False
        dispatcher = getattr(self.service, "dispatcher", None)
        if dispatcher is not None:
            dispatcher.stop()
        if standby_thread and standby_thread is not threading.current_thread():
            standby_thread.join(timeout=2)
        for thread in threads:
            thread.join(timeout=5)
        with self._lock:
            self._channel_threads = {}
            self._channel_stops = {
                name: threading.Event() for name in ("weixin", "feishu")
            }
            self._standby_thread = None
            self._standby_stop = threading.Event()

    def close(self) -> None:
        """Permanently stop this owner generation and close its executor."""

        self.stop()
        self.service.close()


_runtime: AutomationRuntime | None = None
_runtime_root: str = ""


def get_runtime() -> AutomationRuntime:
    global _runtime, _runtime_root
    root = str(get_config().data_root.resolve())
    if (
        _runtime is None
        or _runtime_root != root
        or getattr(_runtime.service, "_closed", False)
    ):
        if _runtime is not None:
            _runtime.stop()
        _runtime = AutomationRuntime()
        _runtime_root = root
    return _runtime
