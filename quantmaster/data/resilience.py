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
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from itertools import count
from pathlib import Path
from typing import Any, ClassVar, TypeVar

import pandas as pd

from quantmaster.config import get_config

logger = logging.getLogger(__name__)
T = TypeVar("T")


class CircuitOpenError(RuntimeError):
    """数据源处于冷却期；调用方应立即尝试备用源或本地缓存。"""


class EmptyProviderResponse(RuntimeError):
    """提供商完成请求但没有返回任何可用数据。"""


_PRIORITIES = {"interactive": 0, "maintenance": 5, "normal": 7, "background": 10}
_REQUEST_PRIORITY: ContextVar[int] = ContextVar(
    "quantmaster_data_priority", default=_PRIORITIES["normal"])


@contextmanager
def data_priority(value: str):
    token = _REQUEST_PRIORITY.set(_PRIORITIES.get(value, _PRIORITIES["normal"]))
    try:
        yield
    finally:
        _REQUEST_PRIORITY.reset(token)


@dataclass(order=True)
class _ScheduledCall:
    priority: int
    sequence: int
    key: str = field(compare=False)
    func: Callable[[], Any] = field(compare=False)
    future: Future = field(compare=False)


class ProviderScheduler:
    """按真实上游隔离并发，同时让前台请求优先于批量研究。"""

    DEFAULT_WORKERS: ClassVar[dict[str, int]] = {
        "akshare:eastmoney": 1,
        "akshare:sina": 1,
        "akshare:csindex": 1,
        "akshare:other": 1,
        "yahoo": 1,
        "tushare": 4,
    }

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._queues: dict[str, queue.PriorityQueue] = {}
        self._inflight: dict[tuple[str, str], Future] = {}
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
                try:
                    item.future.set_result(item.func())
                except BaseException as exc:
                    item.future.set_exception(exc)
            finally:
                with self._lock:
                    if self._inflight.get((lane, item.key)) is item.future:
                        self._inflight.pop((lane, item.key), None)
                work.task_done()

    def call(self, lane: str, key: str, func: Callable[[], T]) -> T:
        work = self._ensure_lane(lane)
        with self._lock:
            future = self._inflight.get((lane, key))
            if future is None:
                future = Future()
                self._inflight[(lane, key)] = future
                work.put(_ScheduledCall(
                    _REQUEST_PRIORITY.get(), next(self._sequence), key, func, future))
        return future.result()


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
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(path, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS source_health ("
            "lane TEXT PRIMARY KEY,state TEXT NOT NULL DEFAULT 'closed',"
            "failures INTEGER NOT NULL DEFAULT 0,open_count INTEGER NOT NULL DEFAULT 0,"
            "open_until REAL NOT NULL DEFAULT 0,last_failure REAL NOT NULL DEFAULT 0,"
            "last_success REAL NOT NULL DEFAULT 0,last_error TEXT NOT NULL DEFAULT '',"
            "suppressed INTEGER NOT NULL DEFAULT 0)"
        )
        return conn

    def status(self, lane: str | None = None) -> dict[str, dict]:
        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            if lane:
                rows = conn.execute(
                    "SELECT * FROM source_health WHERE lane=?", (lane,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM source_health ORDER BY lane").fetchall()
        return {str(row["lane"]): dict(row) for row in rows}

    def before_call(self, lane: str, *, probe: bool = False) -> None:
        now = time.time()
        with self._lock, self._conn() as conn:
            row = conn.execute(
                "SELECT state,open_until,suppressed FROM source_health WHERE lane=?", (lane,)
            ).fetchone()
            if row is None or row[0] == "closed":
                return
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
                "UPDATE source_health SET state='half_open',open_until=? WHERE lane=?",
                (now + 60, lane),
            )

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
                "(lane,state,failures,open_count,open_until,last_success,last_error,suppressed) "
                "VALUES (?,'closed',0,0,0,?,'',0) "
                "ON CONFLICT(lane) DO UPDATE SET state='closed',failures=0,open_count=0,"
                "open_until=0,last_success=excluded.last_success,last_error='',suppressed=0",
                (lane, now),
            )

    def failure(self, lane: str, exc: BaseException, *, immediate: bool = False) -> None:
        from quantmaster.logging_config import redact_sensitive_text

        now = time.time()
        summary = redact_sensitive_text(exc).replace("\n", " ")
        summary = re.sub(r"(?i)([a-z][a-z0-9+.-]*://)[^/@\s]+@", r"\1***@", summary)[:300]
        with self._lock, self._conn() as conn:
            row = conn.execute(
                "SELECT failures,open_count,state FROM source_health WHERE lane=?", (lane,)
            ).fetchone()
            failures = int(row[0] if row else 0) + 1
            open_count = int(row[1] if row else 0)
            half_open = bool(row and row[2] == "half_open")
            should_open = immediate or half_open or failures >= 2
            if should_open:
                open_count += 1
                cooldown = (300, 900, 1800)[min(open_count - 1, 2)]
                conn.execute(
                    "INSERT INTO source_health "
                    "(lane,state,failures,open_count,open_until,last_failure,last_error) "
                    "VALUES (?,'open',?,?,?,?,?) "
                    "ON CONFLICT(lane) DO UPDATE SET state='open',failures=excluded.failures,"
                    "open_count=excluded.open_count,open_until=excluded.open_until,"
                    "last_failure=excluded.last_failure,last_error=excluded.last_error",
                    (lane, failures, open_count, now + cooldown, now, summary),
                )
                logger.warning(
                    "数据源 %s 暂停 %s 分钟：%s", lane, cooldown // 60, summary)
            else:
                conn.execute(
                    "INSERT INTO source_health "
                    "(lane,state,failures,open_count,open_until,last_failure,last_error) "
                    "VALUES (?,'closed',?,0,0,?,?) "
                    "ON CONFLICT(lane) DO UPDATE SET failures=excluded.failures,"
                    "last_failure=excluded.last_failure,last_error=excluded.last_error",
                    (lane, failures, now, summary),
                )


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


def provider_call(
    lane: str,
    key: str,
    func: Callable[[], T],
    *,
    probe: bool = False,
    empty_opens: bool = False,
) -> T:
    """经过优先级队列、请求合并和持久化熔断执行一次上游调用。"""
    def scheduled() -> T:
        # 在真正轮到上游执行时再次看熔断状态；否则高并发会在首个失败
        # 打开熔断器前把大量请求预先排进队列。
        PROVIDER_HEALTH.before_call(lane, probe=probe)
        try:
            result = func()
            if empty_opens and (result is None or getattr(result, "empty", False)):
                raise EmptyProviderResponse(f"{lane} 返回空数据")
        except CircuitOpenError:
            raise
        except BaseException as exc:
            # 必须在上游 worker 取下一项前写入熔断状态，才能真正阻止
            # 已排队的错误风暴；同 key 合并时也只记录一次。
            PROVIDER_HEALTH.failure(
                lane, exc,
                immediate=(
                    _hard_connectivity_error(exc) or _rate_limited(exc)
                    or isinstance(exc, EmptyProviderResponse)
                ),
            )
            raise
        PROVIDER_HEALTH.success(lane)
        return result

    return PROVIDER_SCHEDULER.call(lane, key, scheduled)


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
            def scheduled(attempt_number: int = attempt) -> T:
                PROVIDER_HEALTH.before_call(lane, probe=probe)
                try:
                    result = func(*args, **kwargs)
                except CircuitOpenError:
                    raise
                except Exception as exc:
                    immediate = _hard_connectivity_error(exc) or _rate_limited(exc)
                    if immediate or attempt_number >= attempts:
                        PROVIDER_HEALTH.failure(lane, exc, immediate=immediate)
                    raise
                PROVIDER_HEALTH.success(lane)
                return result

            result = PROVIDER_SCHEDULER.call(lane, key, scheduled)
            return result
        except CircuitOpenError:
            raise
        except Exception as exc:
            immediate = _hard_connectivity_error(exc) or _rate_limited(exc)
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
            with sqlite3.connect(path, timeout=30.0) as conn:
                conn.execute("PRAGMA busy_timeout=30000")
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

    def get(self, endpoint: str, params: dict, ttl_days: float) -> pd.DataFrame | None:
        path = self.path_for(endpoint, params)
        if not path.exists():
            return None
        age = time.time() - path.stat().st_mtime
        if age > max(0.0, ttl_days) * 86400:
            return None
        try:
            return pd.read_parquet(path)
        except Exception:
            logger.warning("接口缓存损坏，将重新拉取: %s", path)
            return None

    def put(self, endpoint: str, params: dict, frame: pd.DataFrame) -> None:
        if frame is None:
            return
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
