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
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from importlib import metadata as package_metadata
from itertools import count
from pathlib import Path
from typing import Any, ClassVar, TypeVar

import pandas as pd

from quantmaster.config import get_config
from quantmaster.data.cache_contracts import CacheResultKind
from quantmaster.runtime.sqlite import connect_sqlite

logger = logging.getLogger(__name__)
T = TypeVar("T")


def _retry_sleep(seconds: float) -> None:
    time.sleep(seconds)


def _rate_limit_sleep(seconds: float) -> None:
    time.sleep(seconds)


def _wall_time() -> float:
    return time.time()


class CircuitOpenError(RuntimeError):
    """数据源处于冷却期；调用方应立即尝试备用源或本地缓存。"""


class EmptyProviderResponse(RuntimeError):
    """提供商完成请求但没有返回任何可用数据。"""


class ProviderContractChanged(RuntimeError):
    """The endpoint responded, but its documented schema is no longer usable."""


class ProviderCapabilityMissing(RuntimeError):
    """A deterministic capability gap isolated to one provider lane."""

    def __init__(self, message: str, *, reason: str = "provider_unsupported"):
        self.reason = reason
        super().__init__(message)


class ProviderTimeoutError(TimeoutError):
    """One scheduled upstream call exceeded its shared hard deadline."""

    def __init__(self, lane: str, timeout: float, *, first: bool):
        self.lane = lane
        self.timeout = timeout
        self.first = first
        super().__init__(f"{lane} 在 {timeout:g} 秒内未返回，已暂停该数据源并尝试备用数据")


_PERMANENT_FAILURES = frozenset({
    "permission", "authentication", "capability_missing",
    "http_401_authentication", "http_403_permission", "contract_changed",
})


def classify_provider_failure(exc: BaseException) -> str:
    """Classify provider errors without persisting volatile exception types."""
    name = type(exc).__name__.lower()
    text = str(exc).lower()
    if isinstance(exc, EmptyProviderResponse):
        return "empty_response"
    if isinstance(exc, ProviderContractChanged):
        return "contract_changed"
    if isinstance(exc, (ProviderCapabilityMissing, ModuleNotFoundError)):
        return "capability_missing"
    if (
        getattr(exc, "winerror", None) == 10013
        or "winerror 10013" in text
        or "socket access" in text
        or "套接字访问权限" in text
    ):
        return "transient_network"
    if isinstance(exc, AttributeError) or any(value in text for value in (
        "has no attribute", "接口不存在", "endpoint not found", "not implemented",
    )):
        return "capability_missing"
    status = _http_status(exc)
    if status in {404, 410}:
        return "capability_missing"
    if status == 401:
        return "http_401_authentication"
    if status == 403:
        return "http_403_permission"
    if status == 429:
        # Keep the public failure family stable for existing consumers; the
        # persisted diagnostic code records the exact HTTP cause.
        return "rate_limit"
    if status is not None and 500 <= status <= 599:
        return f"http_{status}_upstream"
    if any(value in text for value in (
        "no permission", "permission denied", "无权限", "没有权限", "访问权限", "权限不足",
        "forbidden", "积分不足",
    )):
        return "permission"
    if any(value in text for value in (
        "unauthorized", "invalid token", "token invalid", "token无效", "token 无效",
        "认证失败", "令牌无效", "未配置 tushare_token",
    )):
        return "authentication"
    if any(value in text for value in (
        "missing columns", "missing column", "缺少字段", "缺少代码或名称列",
        "schema changed", "contract changed", "响应结构", "页面结构",
    )):
        return "contract_changed"
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


def provider_capability_reason(exc: BaseException) -> str:
    """Describe deterministic capability absence without changing its state family."""

    if isinstance(exc, ProviderCapabilityMissing):
        return exc.reason
    if isinstance(exc, ModuleNotFoundError):
        return "dependency_missing"
    if isinstance(exc, AttributeError) or "has no attribute" in str(exc).lower():
        return "sdk_method_missing"
    if _http_status(exc) in {404, 410}:
        return "endpoint_removed"
    return "provider_unsupported"


@dataclass(frozen=True, slots=True)
class ProviderFailureResult:
    """Cache-facing failure semantics; provider failures are never valid empties."""

    kind: CacheResultKind
    diagnostic_code: str
    retryable: bool


def provider_failure_result(exc: BaseException) -> ProviderFailureResult:
    failure = classify_provider_failure(exc)
    if failure == "rate_limit":
        return ProviderFailureResult(CacheResultKind.RATE_LIMITED, failure, True)
    if failure in {"permission", "authentication", "http_401_authentication", "http_403_permission"}:
        return ProviderFailureResult(CacheResultKind.PERMISSION_DENIED, failure, False)
    if failure in {"contract_changed", "empty_response"}:
        return ProviderFailureResult(
            CacheResultKind.INVALID_RESPONSE, failure, failure == "empty_response"
        )
    if failure == "capability_missing":
        return ProviderFailureResult(
            CacheResultKind.INVALID_RESPONSE, provider_capability_reason(exc), False
        )
    return ProviderFailureResult(CacheResultKind.TEMPORARY_FAILURE, failure, True)


def _http_status(exc: BaseException) -> int | None:
    """Extract a status without depending on one HTTP client implementation."""
    response = getattr(exc, "response", None)
    value = getattr(response, "status_code", None)
    if value is None:
        value = getattr(exc, "status_code", None)
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _retry_after_seconds(exc: BaseException, *, now: float | None = None) -> float | None:
    """Parse HTTP Retry-After (delta or RFC date) without ever trusting a bad value."""
    response = getattr(exc, "response", None)
    headers = getattr(response, "headers", {}) or {}
    raw = str(headers.get("Retry-After") or headers.get("retry-after") or "").strip()
    if not raw:
        return None
    try:
        return max(0.0, min(float(raw), 7 * 86400.0))
    except ValueError:
        try:
            target = parsedate_to_datetime(raw)
            if target.tzinfo is None:
                target = target.replace(tzinfo=UTC)
            base = datetime.now(UTC).timestamp() if now is None else now
            return max(0.0, min(target.timestamp() - base, 7 * 86400.0))
        except (TypeError, ValueError, IndexError, OverflowError):
            return None


def _provider_revision(lane: str) -> str:
    provider = lane.partition(":")[0]
    version = ""
    package = {
        "akshare": "akshare", "ths": "akshare", "yahoo": "yfinance",
        "tushare": "tushare",
    }.get(provider)
    if package:
        try:
            version = package_metadata.version(package)
        except package_metadata.PackageNotFoundError:
            version = "missing"
    credential = ""
    if provider == "tushare":
        credential = get_config().data.tushare_token
    elif provider in {"free-stockdb", "free-stockdb-online"}:
        data = get_config().data
        url = (
            data.free_stockdb_online_url
            if provider == "free-stockdb-online" else data.free_stockdb_url
        )
        credential = f"{url}|{data.free_stockdb_sdk_path}"
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
_REMOTE_IO_ALLOWED: ContextVar[bool] = ContextVar(
    "quantmaster_remote_io_allowed", default=True,
)


class LocalOnlyDataAccessError(RuntimeError):
    """Raised when a page-read request attempts a provider operation.

    A stale local snapshot is an acceptable page result; turning that snapshot
    into a 45-second upstream wait is not.  This boundary lives below every
    provider adapter so an accidental direct Tushare/AkShare call cannot evade
    the registry's local-read policy.
    """


@contextmanager
def local_only_data_access():
    """Forbid provider I/O for the lifetime of a Web read request."""

    token = _REMOTE_IO_ALLOWED.set(False)
    try:
        yield
    finally:
        _REMOTE_IO_ALLOWED.reset(token)


def remote_io_allowed() -> bool:
    """Whether the current execution context may contact a data provider."""

    return _REMOTE_IO_ALLOWED.get()


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
    lane: str = field(compare=False)
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
    """Use fixed provider-family pools with one global network budget.

    A provider library can ignore Python-level timeouts (notably while stuck
    in a socket read).  We cannot safely kill that library thread in-process,
    so the scheduler puts a hard ceiling around the damage.  Crucially, lanes
    are accounting labels, not executor identities: creating a new endpoint
    must not create another permanent thread.  Production uses exactly eight
    worker threads across Tushare (2), other external market providers (2),
    and local StockDB access (4).
    """

    MAX_NETWORK_CONCURRENCY: ClassVar[int] = 8
    MAX_CALL_SECONDS: ClassVar[float] = 30.0

    FAMILY_WORKERS: ClassVar[dict[str, int]] = {
        "tushare": 2,
        "external": 2,
        "stockdb": 4,
    }

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._queues: dict[str, queue.PriorityQueue] = {}
        self._lanes: set[str] = set()
        self._active_lanes: set[str] = set()
        self._active_providers: dict[str, int] = {}
        self._inflight: dict[tuple[str, str], _ScheduledCall] = {}
        self._timeout_counts: dict[str, int] = {}
        self._sequence = count()
        self._network_slots = threading.BoundedSemaphore(self.MAX_NETWORK_CONCURRENCY)
        self._active_network = 0

    @staticmethod
    def _family(lane: str) -> str:
        value = str(lane).casefold()
        if value.startswith("tushare"):
            return "tushare"
        if value.startswith(("free-stockdb", "stockdb")):
            return "stockdb"
        # AkShare, THS and Yahoo are all external/interruptible sources.  A
        # conservative shared two-call pool keeps a newly added source from
        # bypassing the external provider budget.
        return "external"

    @staticmethod
    def _provider(lane: str) -> str:
        """Provider identity is the shared budget boundary, never an endpoint label."""
        return str(lane).partition(":")[0].casefold()

    def _ensure_family(self, family: str) -> queue.PriorityQueue:
        with self._lock:
            existing = self._queues.get(family)
            if existing is not None:
                return existing
            work: queue.PriorityQueue = queue.PriorityQueue()
            self._queues[family] = work
            workers = self.FAMILY_WORKERS[family]
            for index in range(workers):
                threading.Thread(
                    target=self._worker,
                    args=(family, work),
                    name=f"data-provider-{family}-{index + 1}",
                    daemon=True,
                ).start()
            return work

    def _worker(self, family: str, work: queue.PriorityQueue) -> None:
        while True:
            item: _ScheduledCall = work.get()
            owns_lane = False
            rescheduled = False
            try:
                if item.expired.is_set() or time.monotonic() >= item.deadline:
                    item.future.set_exception(self._expire(item.lane, item))
                    continue
                # A lane is a concrete upstream resource and therefore gets
                # at most one in-flight call.  The family workers remain
                # shared: a duplicate lane yields back to the priority queue
                # instead of holding an external-provider thread hostage.
                with self._lock:
                    provider = self._provider(item.lane)
                    # The provider's family budget applies to all of its lanes;
                    # a new endpoint cannot create an unbounded executor.
                    if (
                        item.lane in self._active_lanes
                        or self._active_providers.get(provider, 0) >= self.FAMILY_WORKERS[family]
                    ):
                        item.sequence = next(self._sequence)
                        work.put(item)
                        rescheduled = True
                    else:
                        self._active_lanes.add(item.lane)
                        self._active_providers[provider] = self._active_providers.get(provider, 0) + 1
                        owns_lane = True
                if rescheduled:
                    time.sleep(0.001)
                    continue
                if not item.future.set_running_or_notify_cancel():
                    continue
                remaining = item.deadline - time.monotonic()
                if remaining <= 0 or not self._network_slots.acquire(timeout=remaining):
                    item.future.set_exception(self._expire(item.lane, item))
                    continue
                with self._lock:
                    self._active_network += 1
                try:
                    try:
                        if item.expired.is_set() or time.monotonic() >= item.deadline:
                            item.future.set_exception(self._expire(item.lane, item))
                            continue
                        result = item.func()
                        if item.expired.is_set() or time.monotonic() >= item.deadline:
                            item.future.set_exception(self._expire(item.lane, item))
                        else:
                            item.future.set_result(result)
                    except BaseException as exc:
                        item.future.set_exception(exc)
                finally:
                    with self._lock:
                        self._active_network -= 1
                    self._network_slots.release()
            finally:
                with self._lock:
                    if owns_lane:
                        self._active_lanes.discard(item.lane)
                        provider = self._provider(item.lane)
                        remaining = self._active_providers.get(provider, 1) - 1
                        if remaining > 0:
                            self._active_providers[provider] = remaining
                        else:
                            self._active_providers.pop(provider, None)
                    if not rescheduled and self._inflight.get((item.lane, item.key)) is item:
                        self._inflight.pop((item.lane, item.key), None)
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
        timeout_seconds = min(self.MAX_CALL_SECONDS, max(0.01, float(
            get_config().data.provider_timeout if timeout is None else timeout
        )))
        lane = str(lane)
        work = self._ensure_family(self._family(lane))
        with self._lock:
            self._lanes.add(lane)
            item = self._inflight.get((lane, key))
            if item is None:
                item = _ScheduledCall(
                    _REQUEST_PRIORITY.get(), next(self._sequence), lane, key, func, Future(),
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
            lanes = sorted(self._lanes | set(self._timeout_counts))
            inflight = list(self._inflight.items())
            active_network = self._active_network
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
            "hard_timeout_seconds": self.MAX_CALL_SECONDS,
            "network_concurrency_limit": self.MAX_NETWORK_CONCURRENCY,
            "network_active": active_network,
            "provider_concurrency_limits": dict(self.FAMILY_WORKERS),
            "active_providers": dict(self._active_providers),
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
        database_exists = path.is_file()
        conn = connect_sqlite(path, policy="cache")
        if not database_exists:
            self._initialize_schema(conn)
        else:
            self._require_current(conn)
        return conn

    @staticmethod
    def _initialize_schema(conn: sqlite3.Connection) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS source_health ("
            "lane TEXT PRIMARY KEY,state TEXT NOT NULL DEFAULT 'closed',"
            "failures INTEGER NOT NULL DEFAULT 0,open_count INTEGER NOT NULL DEFAULT 0,"
            "open_until REAL NOT NULL DEFAULT 0,last_failure REAL NOT NULL DEFAULT 0,"
            "last_success REAL NOT NULL DEFAULT 0,last_error TEXT NOT NULL DEFAULT '',"
            "suppressed INTEGER NOT NULL DEFAULT 0,failure_class TEXT NOT NULL DEFAULT '',"
            "config_revision TEXT NOT NULL DEFAULT '',probe_started REAL NOT NULL DEFAULT 0,"
            "retry_after REAL NOT NULL DEFAULT 0,diagnostic_code TEXT NOT NULL DEFAULT '')"
        )
        columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(source_health)")}
        for name, definition in (
            ("failure_class", "TEXT NOT NULL DEFAULT ''"),
            ("config_revision", "TEXT NOT NULL DEFAULT ''"),
            ("probe_started", "REAL NOT NULL DEFAULT 0"),
            ("retry_after", "REAL NOT NULL DEFAULT 0"),
            ("diagnostic_code", "TEXT NOT NULL DEFAULT ''"),
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
        if schema_version < 4:
            # Older builds briefly used Tushare ``ths_index`` as a concept
            # fallback.  The current fallback reads the public THS catalog and
            # no code can probe this lane, so keeping its permanent failure
            # would leave a false, unactionable warning in diagnostics.
            conn.execute("DELETE FROM source_health WHERE lane='tushare:ths-concept'")
            conn.execute("PRAGMA user_version=4")
            conn.commit()

    @staticmethod
    def _require_current(conn: sqlite3.Connection) -> None:
        tables = {str(row[0]) for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(source_health)")}
        if (
            int(conn.execute("PRAGMA user_version").fetchone()[0]) != 4
            or "source_health" not in tables
            or {"failure_class", "config_revision", "probe_started", "retry_after", "diagnostic_code"}
            - columns
        ):
            conn.close()
            raise RuntimeError(
                "source health 不是当前 schema，需执行 remaining-schemas 一次性迁移"
            )

    @classmethod
    def migrate_legacy_database(cls, path: str | Path) -> None:
        with connect_sqlite(Path(path), policy="cache") as conn:
            cls._initialize_schema(conn)

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
                value["retry_after_at"] = float(value.get("retry_after") or 0)
                result[str(value["lane"])] = value
        return result

    def disabled_status(self, lane: str) -> dict[str, Any] | None:
        """Return a current permanent failure without recording a suppressed call.

        Optional task layers use this to select their documented fallback
        before entering the provider boundary.  A changed credential revision
        deliberately returns ``None`` so the new credential gets one normal
        capability attempt.
        """
        value = self.status(lane).get(lane)
        if not value or value.get("state") != "disabled":
            return None
        if str(value.get("config_revision") or "") != _provider_revision(lane):
            return None
        return value

    def before_call(self, lane: str, *, probe: bool = False) -> None:
        now = time.time()
        with self._lock, self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT state,open_until,suppressed,config_revision,failure_class "
                "FROM source_health WHERE lane=?",
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
                conn.execute(
                    "UPDATE source_health SET suppressed=suppressed+1 WHERE lane=?", (lane,))
                raise CircuitOpenError(_disabled_message(lane, str(row[4] or "")))
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
        now = time.time()
        with self._lock, self._conn() as conn:
            row = conn.execute(
                "SELECT state,open_until,config_revision,failure_class FROM source_health WHERE lane=?",
                (lane,),
            ).fetchone()
            if row is None or row[0] == "closed":
                return
            if row[0] == "disabled" and _provider_revision(lane) != str(row[2] or ""):
                return
            if row[0] == "disabled" or (not probe and float(row[1]) > now):
                conn.execute(
                    "UPDATE source_health SET suppressed=suppressed+1 WHERE lane=?", (lane,)
                )
                conn.commit()
                if row[0] == "disabled":
                    raise CircuitOpenError(_disabled_message(lane, str(row[3] or "")))
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
                "failure_class,config_revision,probe_started,retry_after,diagnostic_code) "
                "VALUES (?,'closed',0,0,0,?,'',0,'',?,0,0,'') "
                "ON CONFLICT(lane) DO UPDATE SET state='closed',failures=0,open_count=0,"
                "open_until=0,last_success=excluded.last_success,last_error='',suppressed=0,"
                "failure_class='',config_revision=excluded.config_revision,probe_started=0,"
                "retry_after=0,diagnostic_code=''",
                (lane, now, _provider_revision(lane)),
            )

    def failure(
        self, lane: str, exc: BaseException, *, immediate: bool = False,
        retry_after: float | None = None,
    ) -> None:
        from quantmaster.logging_config import redact_sensitive_text

        now = time.time()
        summary = redact_sensitive_text(exc).replace("\n", " ")
        summary = re.sub(r"(?i)([a-z][a-z0-9+.-]*://)[^/@\s]+@", r"\1***@", summary)[:300]
        failure_class = classify_provider_failure(exc)
        permanent = failure_class in _PERMANENT_FAILURES
        status = _http_status(exc)
        diagnostic = failure_class
        if failure_class == "capability_missing":
            diagnostic = f"capability_missing:{provider_capability_reason(exc)}"
        if status is not None:
            diagnostic = (
                f"capability_missing:{provider_capability_reason(exc)}"
                if failure_class == "capability_missing" else f"http_{status}"
            )
        retry_after = max(0.0, float(retry_after or 0.0))
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
                    "failure_class,config_revision,probe_started,retry_after,diagnostic_code) "
                    "VALUES (?,'disabled',?,?,0,?,?,?,?,0,0,?) "
                    "ON CONFLICT(lane) DO UPDATE SET state='disabled',failures=excluded.failures,"
                    "open_count=excluded.open_count,open_until=0,last_failure=excluded.last_failure,"
                    "last_error=excluded.last_error,failure_class=excluded.failure_class,"
                    "config_revision=excluded.config_revision,probe_started=0,retry_after=0,"
                    "diagnostic_code=excluded.diagnostic_code",
                    (
                        lane, failures, open_count, now, summary, failure_class,
                        _provider_revision(lane), diagnostic,
                    ),
                )
                if not row or str(row[2]) != "disabled":
                    logger.warning("数据源 %s 已停用（%s）：%s", lane, failure_class, summary)
                return
            if should_open:
                open_count += 1
                cooldown = max(
                    (300, 900, 1800)[min(open_count - 1, 2)], retry_after,
                )
                conn.execute(
                    "INSERT INTO source_health "
                    "(lane,state,failures,open_count,open_until,last_failure,last_error,"
                    "failure_class,config_revision,probe_started,retry_after,diagnostic_code) "
                    "VALUES (?,'open',?,?,?,?,?,?,?,0,?,?) "
                    "ON CONFLICT(lane) DO UPDATE SET state='open',failures=excluded.failures,"
                    "open_count=excluded.open_count,open_until=excluded.open_until,"
                    "last_failure=excluded.last_failure,last_error=excluded.last_error,"
                    "failure_class=excluded.failure_class,config_revision=excluded.config_revision,"
                    "probe_started=0,retry_after=excluded.retry_after,"
                    "diagnostic_code=excluded.diagnostic_code",
                    (
                        lane, failures, open_count, now + cooldown, now, summary,
                        failure_class, _provider_revision(lane), now + retry_after, diagnostic,
                    ),
                )
                logger.warning(
                    "数据源 %s 暂停 %s 分钟：%s", lane, cooldown // 60, summary)
            else:
                conn.execute(
                    "INSERT INTO source_health "
                    "(lane,state,failures,open_count,open_until,last_failure,last_error,"
                    "failure_class,config_revision,probe_started,retry_after,diagnostic_code) "
                    "VALUES (?,'closed',?,0,0,?,?,?,?,0,0,?) "
                    "ON CONFLICT(lane) DO UPDATE SET failures=excluded.failures,"
                    "last_failure=excluded.last_failure,last_error=excluded.last_error,"
                    "failure_class=excluded.failure_class,config_revision=excluded.config_revision,"
                    "retry_after=0,diagnostic_code=excluded.diagnostic_code",
                    (lane, failures, now, summary, failure_class, _provider_revision(lane), diagnostic),
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
    return _http_status(exc) == 429 or "429" in text or "rate limit" in text or "too many requests" in text


def _permanent_failure(exc: BaseException) -> bool:
    return classify_provider_failure(exc) in _PERMANENT_FAILURES


def _retryable_failure(exc: BaseException) -> bool:
    """Only network/DNS/TLS and upstream 5xx failures earn another request.

    Empty responses are also retried: free providers like AKShare frequently
    return zero-row DataFrames on transient 5xx or page-structure glitches.
    Without retrying empty responses, the circuit breaker opens immediately
    after one transient empty response, blocking all subsequent symbols.
    """
    value = classify_provider_failure(exc)
    return value in {
        "transient_network", "transient_upstream", "upstream_5xx", "empty_response",
    } or (value.startswith("http_") and value.endswith("_upstream"))


def _retry_delay(attempt: int) -> float:
    cfg = get_config().data
    initial = max(0.0, float(cfg.provider_retry_backoff))
    ceiling = max(initial, float(cfg.provider_retry_max_backoff))
    return min(ceiling, initial * (2 ** max(0, attempt - 1)))


def _disabled_message(lane: str, failure_class: str = "") -> str:
    if failure_class == "http_401_authentication":
        return f"{lane} 返回 HTTP 401，令牌无效或缺失；请在设置中更新凭据后探测"
    if failure_class == "http_403_permission":
        return f"{lane} 返回 HTTP 403，当前凭据没有该接口权限；请开通权限或改用本地数据"
    if failure_class == "contract_changed":
        return f"{lane} 响应合同已变化；请更新 SDK 或适配器后重新探测"
    return f"{lane} 已因权限、认证或能力缺失停用；请更新配置后探测"


def _run_scheduled_provider[T](lane: str, key: str, func: Callable[[], T]) -> T:
    try:
        result = PROVIDER_SCHEDULER.call(lane, key, func)
    except ProviderTimeoutError as exc:
        if exc.first:
            PROVIDER_HEALTH.failure(lane, exc, immediate=True)
        raise
    PROVIDER_HEALTH.success(lane)
    return result


def _require_remote_io(lane: str) -> None:
    if not remote_io_allowed():
        raise LocalOnlyDataAccessError(
            f"页面读取只能使用本地快照；{lane} 如需更新，请提交后台刷新任务"
        )


def _provider_enabled(lane: str) -> bool:
    """Return the user's online-request switch for a known provider lane."""
    cfg = get_config().data
    return {
        "akshare": cfg.akshare_enabled,
        "tushare": cfg.tushare_enabled,
        "yahoo": cfg.yfinance_enabled,
        "free-stockdb-online": cfg.free_stockdb_online_enabled,
    }.get(lane.partition(":")[0].casefold(), True)


def _require_provider_enabled(lane: str, *, probe: bool) -> None:
    if probe or _provider_enabled(lane):
        return
    provider = lane.partition(":")[0].casefold()
    raise LocalOnlyDataAccessError(f"数据源 {provider} 已在设置中关闭")


def provider_call[T](
    lane: str,
    key: str,
    func: Callable[[], T],
    *,
    probe: bool = False,
    empty_opens: bool = False,
    retry_attempts: int | None = None,
    retry_backoff: float | None = None,
) -> T:
    """经过优先级队列、请求合并和持久化熔断执行一次上游调用。"""
    _require_remote_io(lane)
    _require_provider_enabled(lane, probe=probe)
    PROVIDER_HEALTH.check_available(lane, probe=probe)
    # The scheduler owns daemon workers shared by the process.  Capture the
    # execution hook with this logical request so a call submitted by an
    # earlier test cannot observe a later test's temporary monkeypatch while
    # it is still queued or unwinding from a provider timeout.
    retry_sleep = _retry_sleep

    def scheduled() -> T:
        # 在真正轮到上游执行时再次看熔断状态；否则高并发会在首个失败
        # 打开熔断器前把大量请求预先排进队列。
        PROVIDER_HEALTH.before_call(lane, probe=probe)
        attempts = max(1, int(
            get_config().data.provider_retry_attempts if retry_attempts is None else retry_attempts
        ))
        for attempt in range(1, attempts + 1):
            try:
                result = func()
                if empty_opens and (result is None or getattr(result, "empty", False)):
                    raise EmptyProviderResponse(f"{lane} 返回空数据")
                return result
            except (CircuitOpenError, ProviderTimeoutError):
                raise
            except BaseException as exc:
                retry_after = _retry_after_seconds(exc)
                immediate = (
                    _hard_connectivity_error(exc) or _rate_limited(exc)
                    or _permanent_failure(exc)
                )
                # Empty responses from free providers (AKShare) are retried
                # as transient failures — they are common on transient 5xx
                # and should not immediately open the circuit breaker.
                if immediate or not _retryable_failure(exc) or attempt >= attempts:
                    PROVIDER_HEALTH.failure(
                        lane, exc, immediate=immediate, retry_after=retry_after,
                    )
                    raise
                delay = _retry_delay(attempt) if retry_backoff is None else min(
                    max(0.0, float(get_config().data.provider_retry_max_backoff)),
                    max(0.0, float(retry_backoff)) * (2 ** (attempt - 1)),
                )
                label = "空数据" if isinstance(exc, EmptyProviderResponse) else "瞬态失败"
                logger.debug(
                    "%s %s（%s/%s），%.2f 秒后重试: %s",
                    lane, label, attempt, attempts, delay, exc,
                )
                if delay:
                    retry_sleep(delay)
        raise AssertionError("unreachable")

    return _run_scheduled_provider(lane, key, scheduled)


def akshare_call[T](
    label: str,
    func: Callable[..., T],
    *args,
    lane: str = "akshare:eastmoney",
    probe: bool = False,
    empty_opens: bool = True,
    **kwargs,
) -> T:
    """AKShare adapter; retry/circuit/singleflight are owned by provider_call.

    ``empty_opens=True`` treats zero-row DataFrame responses as transient
    failures (AKShare frequently returns empty frames on transient 5xx).
    The provider will retry them before opening a circuit breaker.
    """
    key = label + ":" + hashlib.sha256(
        json.dumps(
            {"args": args, "kwargs": kwargs}, sort_keys=True,
            ensure_ascii=False, default=str,
        ).encode("utf-8")
    ).hexdigest()[:16]
    cfg = get_config().data
    return provider_call(
        lane, key, lambda: func(*args, **kwargs), probe=probe,
        empty_opens=empty_opens,
        retry_attempts=max(1, int(cfg.akshare_retries)),
        retry_backoff=max(0.0, float(cfg.akshare_retry_backoff)),
    )


class BatchProgressStore:
    """SQLite-backed per-symbol progress tracker for resumable batch refreshes.

    When refreshing hundreds of symbols, a single transient failure should not
    discard all previously-fetched data. Each successful symbol write is
    checkpointed here; on crash/restart, consumers can skip already-completed
    symbols and resume from the last checkpoint.
    """

    def __init__(self, batch_id: str, root: Path | None = None) -> None:
        self.batch_id = batch_id
        self.root = Path(root) if root else get_config().data_root
        self.path = self.root / "batch_progress.sqlite"

    def _conn(self) -> sqlite3.Connection:
        conn = connect_sqlite(self.path, policy="cache")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS batch_progress ("
            "batch_id TEXT NOT NULL, symbol TEXT NOT NULL, status TEXT NOT NULL,"
            "updated_at REAL NOT NULL, error TEXT NOT NULL DEFAULT '',"
            "PRIMARY KEY (batch_id, symbol))"
        )
        conn.execute("PRAGMA user_version=1")
        return conn

    def mark(self, symbol: str, status: str, *, error: str = "") -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO batch_progress (batch_id, symbol, status, updated_at, error)"
                " VALUES (?,?,?,strftime('%s','now'),?) "
                "ON CONFLICT(batch_id,symbol) DO UPDATE SET "
                "status=excluded.status, updated_at=excluded.updated_at, error=excluded.error",
                (self.batch_id, symbol, status, error),
            )

    def completed(self, symbol: str) -> None:
        self.mark(symbol, "completed")

    def failed(self, symbol: str, error: str = "") -> None:
        self.mark(symbol, "failed", error=str(error)[:200])

    def is_done(self, symbol: str) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT status FROM batch_progress WHERE batch_id=? AND symbol=?",
                (self.batch_id, symbol),
            ).fetchone()
            return row is not None and row[0] == "completed"

    def reset(self) -> None:
        with self._conn() as conn:
            conn.execute("DELETE FROM batch_progress WHERE batch_id=?", (self.batch_id,))

    def status(self) -> dict[str, str]:
        with self._conn() as conn:
            return dict(
                conn.execute(
                    "SELECT symbol, status FROM batch_progress WHERE batch_id=? ORDER BY symbol",
                    (self.batch_id,),
                ).fetchall()
            )


BATCH_PROGRESS = BatchProgressStore  # alias for import convenience


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
            database_exists = path.is_file()
            with connect_sqlite(path, policy="cache") as conn:
                if not database_exists:
                    self._initialize_schema(conn)
                else:
                    self._require_current(conn)
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT next_call FROM rate_state WHERE name='global'").fetchone()
                now = _wall_time()
                next_call = float(row[0]) if row else self._next_call
                reserved = max(now, next_call)
                conn.execute(
                    "INSERT OR REPLACE INTO rate_state VALUES ('global', ?)",
                    (reserved + interval,),
                )
                conn.commit()
            self._next_call = reserved + interval
            delay = reserved - _wall_time()
        if delay > 0:
            _rate_limit_sleep(delay)

    @staticmethod
    def _initialize_schema(conn: sqlite3.Connection) -> None:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS rate_state ("
            "name TEXT PRIMARY KEY, next_call REAL NOT NULL)"
        )
        conn.execute("PRAGMA user_version=1")

    @staticmethod
    def _require_current(conn: sqlite3.Connection) -> None:
        tables = {str(row[0]) for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(rate_state)")}
        if (
            int(conn.execute("PRAGMA user_version").fetchone()[0]) != 1
            or "rate_state" not in tables or {"name", "next_call"} - columns
        ):
            raise RuntimeError(
                "tushare rate 不是当前 schema，需执行 remaining-schemas 一次性迁移"
            )

    @classmethod
    def migrate_legacy_database(cls, path: str | Path) -> None:
        with connect_sqlite(Path(path), policy="cache") as conn:
            cls._initialize_schema(conn)


TUSHARE_LIMITER = TushareRateLimiter()


class EndpointFrameCache:
    """磁盘持久化的 DataFrame 接口缓存，以文件 mtime 判断新鲜度。"""

    def __init__(
        self,
        provider: str = "tushare",
        root: Path | None = None,
        *,
        config_revision: str | None = None,
    ):
        base = Path(root) if root else get_config().data_root / "api_cache"
        self.provider = provider
        self.config_revision = config_revision or _provider_revision(provider)
        self.root = base / provider
        self.root.mkdir(parents=True, exist_ok=True)

    def _digest(self, endpoint: str, params: dict) -> str:
        payload = json.dumps(
            {
                "provider": self.provider,
                "endpoint": endpoint,
                "params": params,
                "config_revision": self.config_revision,
            },
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
                from quantmaster.repair_access import enqueue_repair, quarantine_file

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
