"""外部数据接口的重试、限速与原始响应缓存。

行情研究会反复请求相同区间。这里把网络层的防护放在数据源内部：

- AKShare 失败时指数退避重试，再由 registry 降级到其他数据源；
- Tushare 使用 SQLite 跨进程匀速限流，避免瞬时突发触发 2000 积分档流控；
- Tushare 原始 DataFrame 按 ``endpoint + params`` 缓存为 Parquet，相同请求
  即使跨进程、重启服务也不再次消耗接口次数。
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import queue
import re
import sqlite3
import tempfile
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from importlib import metadata as package_metadata
from itertools import count
from pathlib import Path
from typing import Any, ClassVar, TypeVar

import pandas as pd

from quantmaster.config import get_config
from quantmaster.runtime.sqlite import connect_sqlite

logger = logging.getLogger(__name__)
T = TypeVar("T")


class CircuitOpenError(RuntimeError):
    """数据源处于冷却期；调用方应立即尝试备用源或本地缓存。"""


class EmptyProviderResponse(RuntimeError):
    """提供商完成请求但没有返回任何可用数据。"""


class ProviderTimeoutError(TimeoutError):
    """One scheduled upstream call exceeded its shared hard deadline."""

    def __init__(self, lane: str, timeout: float, *, first: bool):
        self.lane = lane
        self.timeout = timeout
        self.first = first
        super().__init__(f"{lane} 在 {timeout:g} 秒内未返回，已暂停该数据源并尝试备用数据")


_PERMANENT_FAILURES = frozenset({"permission", "authentication", "capability_missing"})


def classify_provider_failure(exc: BaseException) -> str:
    """Classify provider errors without persisting volatile exception types."""
    name = type(exc).__name__.lower()
    text = str(exc).lower()
    if isinstance(exc, EmptyProviderResponse):
        return "empty_response"
    if isinstance(exc, AttributeError) or any(value in text for value in (
        "has no attribute", "接口不存在", "endpoint not found", "not implemented",
    )):
        return "capability_missing"
    if any(value in text for value in (
        "no permission", "permission denied", "无权限", "没有权限", "访问权限", "权限不足",
        "forbidden", "积分不足",
    )):
        return "permission"
    if any(value in text for value in (
        "unauthorized", "invalid token", "token invalid", "认证失败", "未配置 tushare_token",
    )):
        return "authentication"
    if "429" in text or "rate limit" in text or "too many requests" in text:
        return "rate_limit"
    if re.search(r"\b5\d\d\b", text):
        return "upstream_5xx"
    if any(value in name or value in text for value in (
        "proxy", "ssl", "timeout", "connection", "remote disconnected", "getaddrinfo",
        "name or service not known", "certificate verify failed",
    )):
        return "transient_network"
    return "transient_upstream"


def _provider_revision(lane: str) -> str:
    provider = lane.partition(":")[0]
    version = ""
    package = {"akshare": "akshare", "ths": "akshare", "yahoo": "yfinance"}.get(provider)
    if package:
        try:
            version = package_metadata.version(package)
        except package_metadata.PackageNotFoundError:
            version = "missing"
    credential = ""
    if provider == "tushare":
        credential = get_config().data.tushare_token
    payload = json.dumps({
        "lane": lane,
        "package_version": version,
        "credential_digest": hashlib.sha256(credential.encode("utf-8")).hexdigest() if credential else "",
    }, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]


_PRIORITIES = {"interactive": 0, "maintenance": 5, "normal": 7, "background": 10}
_REQUEST_PRIORITY: ContextVar[int] = ContextVar(
    "quantmaster_data_priority", default=_PRIORITIES["normal"])
_BYPASS_ENDPOINT_CACHE: ContextVar[bool] = ContextVar(
    "quantmaster_bypass_endpoint_cache", default=False)


@contextmanager
def data_priority(value: str):
    token = _REQUEST_PRIORITY.set(_PRIORITIES.get(value, _PRIORITIES["normal"]))
    try:
        yield
    finally:
        _REQUEST_PRIORITY.reset(token)


@contextmanager
def bypass_endpoint_cache(enabled: bool = True):
    """Force provider calls to ignore persisted endpoint responses for this request."""
    token = _BYPASS_ENDPOINT_CACHE.set(bool(enabled))
    try:
        yield
    finally:
        _BYPASS_ENDPOINT_CACHE.reset(token)


def endpoint_cache_bypassed() -> bool:
    return _BYPASS_ENDPOINT_CACHE.get()


@dataclass(order=True)
class _ScheduledCall:
    priority: int
    sequence: int
    key: str = field(compare=False)
    func: Callable[[], Any] = field(compare=False)
    future: Future = field(compare=False)
    deadline: float = field(compare=False)
    timeout_seconds: float = field(compare=False)
    expired: threading.Event = field(compare=False, default_factory=threading.Event)
    timeout_lock: threading.Lock = field(compare=False, default_factory=threading.Lock)
    timeout_reported: bool = field(compare=False, default=False)

    def timeout_error(self, lane: str) -> ProviderTimeoutError:
        with self.timeout_lock:
            first = not self.timeout_reported
            self.timeout_reported = True
            self.expired.set()
        return ProviderTimeoutError(lane, self.timeout_seconds, first=first)


class ProviderScheduler:
    """按真实上游隔离并发，同时让前台请求优先于批量研究。"""

    DEFAULT_WORKERS: ClassVar[dict[str, int]] = {
        "akshare:eastmoney": 1,
        "akshare:eastmoney-concept": 1,
        "akshare:eastmoney-reference": 1,
        "akshare:eastmoney-spot": 1,
        "akshare:sina": 1,
        "akshare:sina-reference": 1,
        "akshare:bond-reference": 1,
        "akshare:csindex": 1,
        "akshare:other": 1,
        "yahoo": 1,
        "tushare": 4,
        "tushare:fx-reference": 1,
        "ths:concept": 1,
    }

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._queues: dict[str, queue.PriorityQueue] = {}
        self._inflight: dict[tuple[str, str], _ScheduledCall] = {}
        self._timeout_counts: dict[str, int] = {}
        self._sequence = count()

    def _ensure_lane(self, lane: str) -> queue.PriorityQueue:
        with self._lock:
            existing = self._queues.get(lane)
            if existing is not None:
                return existing
            work: queue.PriorityQueue = queue.PriorityQueue()
            self._queues[lane] = work
            workers = self.DEFAULT_WORKERS.get(lane, 2)
            for index in range(workers):
                threading.Thread(
                    target=self._worker,
                    args=(lane, work),
                    name=f"data-{lane.replace(':', '-')}-{index + 1}",
                    daemon=True,
                ).start()
            return work

    def _worker(self, lane: str, work: queue.PriorityQueue) -> None:
        while True:
            item: _ScheduledCall = work.get()
            try:
                if not item.future.set_running_or_notify_cancel():
                    continue
                if item.expired.is_set() or time.monotonic() >= item.deadline:
                    item.future.set_exception(self._expire(lane, item))
                    continue
                try:
                    result = item.func()
                    if item.expired.is_set() or time.monotonic() >= item.deadline:
                        item.future.set_exception(self._expire(lane, item))
                    else:
                        item.future.set_result(result)
                except BaseException as exc:
                    item.future.set_exception(exc)
            finally:
                with self._lock:
                    if self._inflight.get((lane, item.key)) is item:
                        self._inflight.pop((lane, item.key), None)
                work.task_done()

    def _expire(self, lane: str, item: _ScheduledCall) -> ProviderTimeoutError:
        error = item.timeout_error(lane)
        if error.first:
            with self._lock:
                self._timeout_counts[lane] = self._timeout_counts.get(lane, 0) + 1
        return error

    def call(
        self, lane: str, key: str, func: Callable[[], T], *, timeout: float | None = None,
    ) -> T:
        timeout_seconds = min(300.0, max(0.01, float(
            get_config().data.provider_timeout if timeout is None else timeout
        )))
        work = self._ensure_lane(lane)
        with self._lock:
            item = self._inflight.get((lane, key))
            if item is None:
                item = _ScheduledCall(
                    _REQUEST_PRIORITY.get(), next(self._sequence), key, func, Future(),
                    time.monotonic() + timeout_seconds, timeout_seconds,
                )
                self._inflight[(lane, key)] = item
                work.put(item)
        if item.expired.is_set():
            raise self._expire(lane, item)
        remaining = item.deadline - time.monotonic()
        if remaining <= 0:
            raise self._expire(lane, item)
        try:
            return item.future.result(timeout=remaining)
        except ProviderTimeoutError:
            raise
        except FutureTimeoutError as exc:
            if item.future.done():
                raise
            raise self._expire(lane, item) from exc

    def status(self) -> dict[str, Any]:
        with self._lock:
            lanes = sorted(set(self._queues) | set(self._timeout_counts))
            inflight = list(self._inflight.items())
            lane_status = {}
            for lane in lanes:
                items = [item for (item_lane, _key), item in inflight if item_lane == lane]
                lane_status[lane] = {
                    "active": sum(item.future.running() and not item.future.done() for item in items),
                    "waiting": sum(not item.future.running() and not item.future.done() for item in items),
                    "expired": sum(item.expired.is_set() and not item.future.done() for item in items),
                    "timeout_count": self._timeout_counts.get(lane, 0),
                }
        return {
            "timeout_seconds": float(get_config().data.provider_timeout),
            "timeout_count": sum(item["timeout_count"] for item in lane_status.values()),
            "lanes": lane_status,
        }


PROVIDER_SCHEDULER = ProviderScheduler()


class ProviderHealthStore:
    """把上游熔断状态存到数据目录，避免重启后重新制造错误风暴。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()

    @staticmethod
    def _path() -> Path:
        return get_config().data_root / "source_health.sqlite"

    def _conn(self) -> sqlite3.Connection:
        path = self._path()
        conn = connect_sqlite(path, policy="cache")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS source_health ("
            "lane TEXT PRIMARY KEY,state TEXT NOT NULL DEFAULT 'closed',"
            "failures INTEGER NOT NULL DEFAULT 0,open_count INTEGER NOT NULL DEFAULT 0,"
            "open_until REAL NOT NULL DEFAULT 0,last_failure REAL NOT NULL DEFAULT 0,"
            "last_success REAL NOT NULL DEFAULT 0,last_error TEXT NOT NULL DEFAULT '',"
            "suppressed INTEGER NOT NULL DEFAULT 0,failure_class TEXT NOT NULL DEFAULT '',"
            "config_revision TEXT NOT NULL DEFAULT '',probe_started REAL NOT NULL DEFAULT 0)"
        )
        columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(source_health)")}
        for name, definition in (
            ("failure_class", "TEXT NOT NULL DEFAULT ''"),
            ("config_revision", "TEXT NOT NULL DEFAULT ''"),
            ("probe_started", "REAL NOT NULL DEFAULT 0"),
        ):
            if name not in columns:
                conn.execute(f"ALTER TABLE source_health ADD COLUMN {name} {definition}")
        schema_version = int(conn.execute("PRAGMA user_version").fetchone()[0])
        if schema_version < 3:
            # v3 corrects previously persisted Tushare wordings such as
            # “没有接口(dc_index)访问权限”.  Without this migration an upgraded
            # process would retain the old transient cooldown until another
            # wasteful upstream request happened to fail.
            rows = conn.execute(
                "SELECT lane,state,last_error,failure_class FROM source_health"
            ).fetchall()
            for lane, state, last_error, stored_class in rows:
                corrected = classify_provider_failure(RuntimeError(str(last_error or "")))
                if corrected in _PERMANENT_FAILURES and (
                    str(state) != "disabled" or str(stored_class) != corrected
                ):
                    conn.execute(
                        "UPDATE source_health SET state='disabled',open_until=0,"
                        "failure_class=?,config_revision=?,probe_started=0 WHERE lane=?",
                        (corrected, _provider_revision(str(lane)), lane),
                    )
            conn.execute("PRAGMA user_version=3")
            # Callers such as before_call() immediately open an explicit
            # transaction on this same connection.  End the migration
            # transaction first so a first request after upgrade is usable.
            conn.commit()
        return conn

    def status(self, lane: str | None = None) -> dict[str, dict]:
        now = time.time()
        with self._lock, self._conn() as conn:
            conn.row_factory = sqlite3.Row
            if lane:
                rows = conn.execute(
                    "SELECT * FROM source_health WHERE lane=?", (lane,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM source_health ORDER BY lane").fetchall()
            result: dict[str, dict] = {}
            for row in rows:
                value = dict(row)
                if value["state"] == "half_open" and float(value["open_until"] or 0) <= now:
                    value["state"] = "open"
                    value["open_until"] = 0.0
                    value["probe_started"] = 0.0
                    conn.execute(
                        "UPDATE source_health SET state='open',open_until=0,probe_started=0 "
                        "WHERE lane=? AND state='half_open' AND open_until<=?",
                        (value["lane"], now),
                    )
                value["permanent"] = value["state"] == "disabled"
                value["next_probe_at"] = float(value.get("open_until") or 0)
                result[str(value["lane"])] = value
        return result

    def before_call(self, lane: str, *, probe: bool = False) -> None:
        now = time.time()
        with self._lock, self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT state,open_until,suppressed,config_revision FROM source_health WHERE lane=?",
                (lane,),
            ).fetchone()
            if row is None or row[0] == "closed":
                return
            if row[0] == "disabled":
                revision = _provider_revision(lane)
                if revision != str(row[3] or ""):
                    conn.execute(
                        "UPDATE source_health SET state='closed',failures=0,open_count=0,"
                        "open_until=0,last_error='',failure_class='',config_revision=?,"
                        "probe_started=0 WHERE lane=?",
                        (revision, lane),
                    )
                    return
                if not probe:
                    conn.execute(
                        "UPDATE source_health SET suppressed=suppressed+1 WHERE lane=?", (lane,))
                    raise CircuitOpenError(f"{lane} 已因权限、认证或能力缺失停用；请更新配置后探测")
            if not probe and float(row[1]) > now:
                conn.execute(
                    "UPDATE source_health SET suppressed=suppressed+1 WHERE lane=?", (lane,))
                conn.commit()
                remaining = max(1, round(float(row[1]) - now))
                raise CircuitOpenError(f"{lane} 暂停请求，约 {remaining} 秒后探测")
            # 冷却结束或用户主动检测时，只放行一个半开探测窗口。
            if row[0] == "half_open" and not probe and float(row[1]) > now:
                raise CircuitOpenError(f"{lane} 正在探测恢复状态")
            conn.execute(
                "UPDATE source_health SET state='half_open',open_until=?,probe_started=? WHERE lane=?",
                (now + 60, now, lane),
            )

    def check_available(self, lane: str, *, probe: bool = False) -> None:
        """Fail fast for a known open circuit without reserving a half-open probe."""
        if probe:
            return
        now = time.time()
        with self._lock, self._conn() as conn:
            row = conn.execute(
                "SELECT state,open_until,config_revision FROM source_health WHERE lane=?",
                (lane,),
            ).fetchone()
            if row is None or row[0] == "closed":
                return
            if row[0] == "disabled" and _provider_revision(lane) != str(row[2] or ""):
                return
            if row[0] == "disabled" or float(row[1]) > now:
                conn.execute(
                    "UPDATE source_health SET suppressed=suppressed+1 WHERE lane=?", (lane,)
                )
                conn.commit()
                if row[0] == "disabled":
                    raise CircuitOpenError(
                        f"{lane} 已因权限、认证或能力缺失停用；请更新配置后探测"
                    )
                remaining = max(1, round(float(row[1]) - now))
                raise CircuitOpenError(f"{lane} 暂停请求，约 {remaining} 秒后探测")

    def success(self, lane: str) -> None:
        now = time.time()
        with self._lock, self._conn() as conn:
            row = conn.execute(
                "SELECT state,failures,suppressed FROM source_health WHERE lane=?", (lane,)
            ).fetchone()
            if row and (row[0] != "closed" or row[1] or row[2]):
                logger.info(
                    "数据源 %s 已恢复；冷却期间跳过 %s 次请求", lane, int(row[2] or 0))
            conn.execute(
                "INSERT INTO source_health "
                "(lane,state,failures,open_count,open_until,last_success,last_error,suppressed,"
                "failure_class,config_revision,probe_started) "
                "VALUES (?,'closed',0,0,0,?,'',0,'',?,0) "
                "ON CONFLICT(lane) DO UPDATE SET state='closed',failures=0,open_count=0,"
                "open_until=0,last_success=excluded.last_success,last_error='',suppressed=0,"
                "failure_class='',config_revision=excluded.config_revision,probe_started=0",
                (lane, now, _provider_revision(lane)),
            )

    def failure(self, lane: str, exc: BaseException, *, immediate: bool = False) -> None:
        from quantmaster.logging_config import redact_sensitive_text

        now = time.time()
        summary = redact_sensitive_text(exc).replace("\n", " ")
        summary = re.sub(r"(?i)([a-z][a-z0-9+.-]*://)[^/@\s]+@", r"\1***@", summary)[:300]
        failure_class = classify_provider_failure(exc)
        permanent = failure_class in _PERMANENT_FAILURES
        with self._lock, self._conn() as conn:
            row = conn.execute(
                "SELECT failures,open_count,state FROM source_health WHERE lane=?", (lane,)
            ).fetchone()
            failures = int(row[0] if row else 0) + 1
            open_count = int(row[1] if row else 0)
            half_open = bool(row and row[2] == "half_open")
            should_open = immediate or half_open or failures >= 2
            if permanent:
                conn.execute(
                    "INSERT INTO source_health "
                    "(lane,state,failures,open_count,open_until,last_failure,last_error,"
                    "failure_class,config_revision,probe_started) "
                    "VALUES (?,'disabled',?,?,0,?,?,?,?,0) "
                    "ON CONFLICT(lane) DO UPDATE SET state='disabled',failures=excluded.failures,"
                    "open_count=excluded.open_count,open_until=0,last_failure=excluded.last_failure,"
                    "last_error=excluded.last_error,failure_class=excluded.failure_class,"
                    "config_revision=excluded.config_revision,probe_started=0",
                    (
                        lane, failures, open_count, now, summary, failure_class,
                        _provider_revision(lane),
                    ),
                )
                if not row or str(row[2]) != "disabled":
                    logger.warning("数据源 %s 已停用（%s）：%s", lane, failure_class, summary)
                return
            if should_open:
                open_count += 1
                cooldown = (300, 900, 1800)[min(open_count - 1, 2)]
                conn.execute(
                    "INSERT INTO source_health "
                    "(lane,state,failures,open_count,open_until,last_failure,last_error,"
                    "failure_class,config_revision,probe_started) "
                    "VALUES (?,'open',?,?,?,?,?,?,?,0) "
                    "ON CONFLICT(lane) DO UPDATE SET state='open',failures=excluded.failures,"
                    "open_count=excluded.open_count,open_until=excluded.open_until,"
                    "last_failure=excluded.last_failure,last_error=excluded.last_error,"
                    "failure_class=excluded.failure_class,config_revision=excluded.config_revision,"
                    "probe_started=0",
                    (
                        lane, failures, open_count, now + cooldown, now, summary,
                        failure_class, _provider_revision(lane),
                    ),
                )
                logger.warning(
                    "数据源 %s 暂停 %s 分钟：%s", lane, cooldown // 60, summary)
            else:
                conn.execute(
                    "INSERT INTO source_health "
                    "(lane,state,failures,open_count,open_until,last_failure,last_error,"
                    "failure_class,config_revision,probe_started) "
                    "VALUES (?,'closed',?,0,0,?,?,?,?,0) "
                    "ON CONFLICT(lane) DO UPDATE SET failures=excluded.failures,"
                    "last_failure=excluded.last_failure,last_error=excluded.last_error,"
                    "failure_class=excluded.failure_class,config_revision=excluded.config_revision",
                    (lane, failures, now, summary, failure_class, _provider_revision(lane)),
                )

    def reset(self, lane: str) -> dict[str, dict]:
        """Allow one explicit recovery probe without deleting audit history."""
        with self._lock, self._conn() as conn:
            conn.execute(
                "UPDATE source_health SET state='open',open_until=0,probe_started=0,"
                "config_revision=? WHERE lane=?",
                (_provider_revision(lane), lane),
            )
        return self.status(lane)


PROVIDER_HEALTH = ProviderHealthStore()


def _hard_connectivity_error(exc: BaseException) -> bool:
    name = type(exc).__name__.lower()
    text = str(exc).lower()
    return (
        "proxy" in name or "ssl" in name or "proxyerror" in text
        or "name or service not known" in text or "getaddrinfo failed" in text
        or "certificate verify failed" in text
    )


def _rate_limited(exc: BaseException) -> bool:
    text = str(exc).lower()
    return "429" in text or "rate limit" in text or "too many requests" in text


def _permanent_failure(exc: BaseException) -> bool:
    return classify_provider_failure(exc) in _PERMANENT_FAILURES


def _run_scheduled_provider(lane: str, key: str, func: Callable[[], T]) -> T:
    try:
        result = PROVIDER_SCHEDULER.call(lane, key, func)
    except ProviderTimeoutError as exc:
        if exc.first:
            PROVIDER_HEALTH.failure(lane, exc, immediate=True)
        raise
    PROVIDER_HEALTH.success(lane)
    return result


def provider_call(
    lane: str,
    key: str,
    func: Callable[[], T],
    *,
    probe: bool = False,
    empty_opens: bool = False,
) -> T:
    """经过优先级队列、请求合并和持久化熔断执行一次上游调用。"""
    PROVIDER_HEALTH.check_available(lane, probe=probe)

    def scheduled() -> T:
        # 在真正轮到上游执行时再次看熔断状态；否则高并发会在首个失败
        # 打开熔断器前把大量请求预先排进队列。
        PROVIDER_HEALTH.before_call(lane, probe=probe)
        try:
            result = func()
            if empty_opens and (result is None or getattr(result, "empty", False)):
                raise EmptyProviderResponse(f"{lane} 返回空数据")
        except (CircuitOpenError, ProviderTimeoutError):
            raise
        except BaseException as exc:
            # 必须在上游 worker 取下一项前写入熔断状态，才能真正阻止
            # 已排队的错误风暴；同 key 合并时也只记录一次。
            PROVIDER_HEALTH.failure(
                lane, exc,
                immediate=(
                    _hard_connectivity_error(exc) or _rate_limited(exc)
                    or isinstance(exc, EmptyProviderResponse) or _permanent_failure(exc)
                ),
            )
            raise
        return result

    return _run_scheduled_provider(lane, key, scheduled)


def akshare_call(
    label: str,
    func: Callable[..., T],
    *args,
    lane: str = "akshare:eastmoney",
    probe: bool = False,
    **kwargs,
) -> T:
    """执行 AKShare 请求，失败时按配置做指数退避重试。"""
    cfg = get_config().data
    attempts = max(1, int(cfg.akshare_retries))
    backoff = max(0.0, float(cfg.akshare_retry_backoff))
    key = label + ":" + hashlib.sha256(
        json.dumps(
            {"args": args, "kwargs": kwargs}, sort_keys=True,
            ensure_ascii=False, default=str,
        ).encode("utf-8")
    ).hexdigest()[:16]
    for attempt in range(1, attempts + 1):
        try:
            PROVIDER_HEALTH.check_available(lane, probe=probe)

            def scheduled(attempt_number: int = attempt) -> T:
                PROVIDER_HEALTH.before_call(lane, probe=probe)
                try:
                    result = func(*args, **kwargs)
                except (CircuitOpenError, ProviderTimeoutError):
                    raise
                except Exception as exc:
                    immediate = (
                        _hard_connectivity_error(exc) or _rate_limited(exc)
                        or _permanent_failure(exc)
                    )
                    if immediate or attempt_number >= attempts:
                        PROVIDER_HEALTH.failure(lane, exc, immediate=immediate)
                    raise
                return result

            return _run_scheduled_provider(lane, key, scheduled)
        except CircuitOpenError:
            raise
        except (AttributeError, ImportError, OSError, RuntimeError, TypeError, ValueError) as exc:
            immediate = (
                _hard_connectivity_error(exc) or _rate_limited(exc)
                or _permanent_failure(exc)
            )
            if immediate or attempt >= attempts:
                raise
            delay = backoff * (2 ** (attempt - 1))
            logger.debug(
                "AKShare %s 失败（%s/%s），%.2f 秒后重试: %s",
                label, attempt, attempts, delay, exc,
            )
            if delay:
                time.sleep(delay)
    raise AssertionError("unreachable")


class TushareRateLimiter:
    """按稳定间隔放行请求；SQLite 让同一数据目录下的进程共享限流。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._next_call = 0.0

    def wait(self) -> None:
        calls = max(1, int(get_config().data.tushare_calls_per_minute))
        interval = 60.0 / calls
        with self._lock:
            path = get_config().data_root / "tushare_rate.sqlite"
            with connect_sqlite(path, policy="cache") as conn:
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS rate_state ("
                    "name TEXT PRIMARY KEY, next_call REAL NOT NULL)"
                )
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT next_call FROM rate_state WHERE name='global'").fetchone()
                now = time.time()
                next_call = float(row[0]) if row else self._next_call
                reserved = max(now, next_call)
                conn.execute(
                    "INSERT OR REPLACE INTO rate_state VALUES ('global', ?)",
                    (reserved + interval,),
                )
                conn.commit()
            self._next_call = reserved + interval
            delay = reserved - time.time()
        if delay > 0:
            time.sleep(delay)


TUSHARE_LIMITER = TushareRateLimiter()


class EndpointFrameCache:
    """磁盘持久化的 DataFrame 接口缓存，以文件 mtime 判断新鲜度。"""

    def __init__(self, provider: str = "tushare", root: Path | None = None):
        base = Path(root) if root else get_config().data_root / "api_cache"
        self.root = base / provider
        self.root.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _digest(endpoint: str, params: dict) -> str:
        payload = json.dumps(
            {"endpoint": endpoint, "params": params},
            sort_keys=True, ensure_ascii=False, default=str,
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()[:24]

    def path_for(self, endpoint: str, params: dict) -> Path:
        safe_endpoint = "".join(c if c.isalnum() or c in "-_" else "_" for c in endpoint)
        return self.root / f"{safe_endpoint}-{self._digest(endpoint, params)}.parquet"

    def get(
        self,
        endpoint: str,
        params: dict,
        ttl_days: float,
        *,
        min_mtime: float | None = None,
        required_nonempty: bool = False,
        required_columns: tuple[str, ...] = (),
    ) -> pd.DataFrame | None:
        path = self.path_for(endpoint, params)
        if not path.exists():
            return None
        mtime = path.stat().st_mtime
        if min_mtime is not None and mtime < min_mtime:
            return None
        age = time.time() - mtime
        if age > max(0.0, ttl_days) * 86400:
            return None
        try:
            frame = pd.read_parquet(path)
            missing = [column for column in required_columns if column not in frame.columns]
            if (required_nonempty and frame.empty) or missing:
                logger.warning(
                    "接口缓存语义无效，将重新拉取: %s%s",
                    path,
                    f"（缺少列 {', '.join(missing)}）" if missing else "（空响应）",
                )
                return None
            return frame
        except Exception as exc:
            logger.warning("接口缓存损坏，将隔离并重新拉取: %s", path)
            try:
                from quantmaster.data.repair import enqueue_repair, quarantine_file

                quarantine = quarantine_file(
                    path,
                    category="api-cache",
                    target=str(path.resolve()),
                    reason=f"{type(exc).__name__}: {exc}",
                )
                enqueue_repair(
                    "api_cache",
                    str(path.resolve()),
                    reason="接口缓存完整性校验失败",
                    spec={
                        "path": str(path.resolve()),
                        "root": str(self.root.parent.resolve()),
                        "provider": self.root.name,
                        "quarantine": quarantine,
                    },
                    source=f"api-cache:{self.root.name}",
                )
            except (ImportError, OSError, RuntimeError, sqlite3.Error, TypeError, ValueError):
                logger.exception("接口缓存隔离或修复入队失败: %s", path)
            return None

    def put(
        self,
        endpoint: str,
        params: dict,
        frame: pd.DataFrame,
        *,
        required_nonempty: bool = False,
        required_columns: tuple[str, ...] = (),
    ) -> None:
        if frame is None or (required_nonempty and frame.empty):
            return
        missing = [column for column in required_columns if column not in frame.columns]
        if missing:
            raise ValueError(f"接口响应缺少必需列: {', '.join(missing)}")
        target = self.path_for(endpoint, params)
        fd, temp_name = tempfile.mkstemp(
            prefix=f".{target.stem}.", suffix=".parquet.tmp", dir=self.root)
        os.close(fd)
        temp_path = Path(temp_name)
        try:
            frame.to_parquet(temp_path, index=False)
            os.replace(temp_path, target)
        finally:
            temp_path.unlink(missing_ok=True)
