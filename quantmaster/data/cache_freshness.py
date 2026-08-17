"""Purpose-aware freshness and durable partial-refresh bookkeeping.

The freshness helpers are deliberately pure: a page read may describe stale
local evidence, but it must never acquire a provider while doing so.  Batch
bookkeeping lives beside the bar namespace in its own SQLite database so a
partially successful panel can be resumed without changing the bar catalog's
schema or losing already-published symbol files.
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar

import pandas as pd

from quantmaster.data.base import Market
from quantmaster.data.cache_contracts import CacheResultKind, cache_registry
from quantmaster.market_capabilities import guess_market
from quantmaster.runtime.sqlite import connect_sqlite
from quantmaster.trading_sessions import SessionExpectation


class CachePurpose(StrEnum):
    DISPLAY = "display"
    CURRENT_ANALYSIS = "current_analysis"
    HISTORICAL = "historical"
    FORMAL_RESEARCH = "formal_research"


@dataclass(frozen=True)
class FreshnessAssessment:
    state: str
    age_seconds: float | None
    stale_while_revalidate: bool
    refresh_reason: str = ""
    expected_session: str = ""
    calendar_source: str = "unavailable"
    future_rows: int = 0

    @property
    def formal_eligible(self) -> bool:
        return self.state == "fresh" and self.future_rows == 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "age_seconds": self.age_seconds,
            "stale_while_revalidate": self.stale_while_revalidate,
            "refresh_reason": self.refresh_reason,
            "expected_session": self.expected_session,
            "calendar_source": self.calendar_source,
            "future_rows": self.future_rows,
            "formal_eligible": self.formal_eligible,
        }


def _purpose(value: CachePurpose | str) -> CachePurpose:
    aliases = {"historical_replay": CachePurpose.HISTORICAL}
    raw = str(value)
    return aliases.get(raw, CachePurpose(raw))


def _historical_freshness(
    *,
    usage: CachePurpose,
    observed_end: pd.Timestamp | None,
    requested: pd.Timestamp,
    expectation: SessionExpectation | None,
    age: float | None,
) -> FreshnessAssessment:
    if observed_end is None:
        return FreshnessAssessment(
            "missing", age, False, f"as_of {requested.date()} 没有本地数据",
        )
    expected = expectation or SessionExpectation()
    requested_date = requested.date().isoformat()
    trusted_target = bool(
        expected.ready and expected.session and expected.session <= requested_date
    )
    if not trusted_target:
        qualifier = "正式研究" if usage == CachePurpose.FORMAL_RESEARCH else "历史读取"
        return FreshnessAssessment(
            "incomplete", age, False,
            f"{qualifier}无法确认 as_of {requested_date} 的完整尾部："
            f"{expected.reason or '缺少 requested_end 之前的可信交易日证据'}",
            calendar_source=expected.source,
        )
    observed_session = observed_end.date().isoformat()
    if observed_session < expected.session:
        return FreshnessAssessment(
            "incomplete", age, False,
            f"本地行情截至 {observed_session}，as_of {requested_date} "
            f"之前应发布的最近交易日为 {expected.session}",
            expected.session, expected.source,
        )
    return FreshnessAssessment(
        "fresh", age, False, expected_session=expected.session,
        calendar_source=expected.source,
    )


def assess_daily_freshness(
    *,
    symbol: str,
    frame: pd.DataFrame,
    requested_end: str,
    checked_at: float,
    purpose: CachePurpose | str = CachePurpose.DISPLAY,
    now: datetime | pd.Timestamp | None = None,
    expectation: SessionExpectation | None = None,
    display_ttl_seconds: float = 86400.0,
) -> FreshnessAssessment:
    """Describe freshness without inventing a trading session or doing I/O."""
    usage = _purpose(purpose)
    current = pd.Timestamp(now) if now is not None else pd.Timestamp.now(tz="Asia/Shanghai")
    if current.tzinfo is None:
        current = current.tz_localize("Asia/Shanghai")
    else:
        current = current.tz_convert("Asia/Shanghai")
    raw_age = max(0.0, current.timestamp() - checked_at) if checked_at else None
    # A minute bucket is precise enough for UI age disclosure and keeps one
    # immutable snapshot's envelope stable across adjacent local-only reads.
    age = float(int(raw_age // 60) * 60) if raw_age is not None else None
    requested = pd.Timestamp(requested_end).normalize()
    normalized_index = (
        pd.DatetimeIndex(frame.index).tz_localize(None).normalize()
        if isinstance(frame.index, pd.DatetimeIndex) and frame.index.tz is not None
        else pd.DatetimeIndex(frame.index).normalize()
    )
    future_rows = int((normalized_index > requested).sum())
    if future_rows:
        return FreshnessAssessment(
            "invalid_future", age, False,
            f"本地快照含 {future_rows} 条晚于 as_of {requested.date()} 的数据",
            future_rows=future_rows,
        )

    historical = usage in {CachePurpose.HISTORICAL, CachePurpose.FORMAL_RESEARCH}
    if historical:
        # Historical bytes are immutable with respect to wall-clock TTL.  They
        # are admissible only when their requested as_of boundary is complete;
        # provider recency must not cause an unrelated re-download.
        observed_end = normalized_index.max() if len(normalized_index) else None
        return _historical_freshness(
            usage=usage,
            observed_end=observed_end,
            requested=requested,
            expectation=expectation,
            age=age,
        )

    expected = expectation or SessionExpectation()
    if (
        expected.ready
        and requested.date().isoformat() >= expected.session
        and guess_market(symbol) in {Market.CN, Market.INDEX}
    ):
        latest = normalized_index.max().date().isoformat() if len(normalized_index) else ""
        if latest < expected.session:
            reason = f"本地行情截至 {latest or '无'}，预期交易日为 {expected.session}"
            return FreshnessAssessment(
                "stale", age, usage == CachePurpose.DISPLAY, reason,
                expected.session, expected.source,
            )
    if age is None:
        return FreshnessAssessment(
            "unchecked", None, usage == CachePurpose.DISPLAY, "本地快照缺少检查时间",
            expected.session, expected.source,
        )
    if usage == CachePurpose.DISPLAY and age > max(0.0, display_ttl_seconds):
        return FreshnessAssessment(
            "stale", age, usage == CachePurpose.DISPLAY,
            f"距上次检查 {int(age)} 秒，超过 {int(display_ttl_seconds)} 秒新鲜度窗口",
            expected.session, expected.source,
        )
    return FreshnessAssessment(
        "fresh", age, False, expected_session=expected.session,
        calendar_source=expected.source,
    )


class BarRefreshBatchStore:
    """Durable exact-key state for resumable panel refreshes."""

    SCHEMA_VERSION = 1

    _REASON_BY_DIAGNOSTIC: ClassVar[dict[str, str]] = {
        "not_attempted": "partial_pending",
        "not_found": "not_published",
        "empty_response": "not_published",
        "parse_error": "parse_failed",
        "invalid_response": "parse_failed",
        "insufficient_history": "insufficient_history",
        "market_suspended": "market_suspended",
        "filtered_by_contract": "filtered_by_contract",
    }

    @classmethod
    def _missing_reason(cls, diagnostic_code: str) -> str:
        code = str(diagnostic_code or "").lower()
        if code in cls._REASON_BY_DIAGNOSTIC:
            return cls._REASON_BY_DIAGNOSTIC[code]
        if any(token in code for token in ("parse", "schema", "invalid")):
            return "parse_failed"
        if any(token in code for token in ("timeout", "tls", "network", "http", "upstream")):
            return "source_unavailable"
        return "source_unavailable"

    def __init__(self, bars_root: str | Path):
        self.path = Path(bars_root) / "refresh_pending.sqlite"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _conn(self) -> sqlite3.Connection:
        return connect_sqlite(self.path, policy="cache")

    def _initialize(self) -> None:
        with self._conn() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS refresh_batches ("
                "batch_id TEXT PRIMARY KEY,provider TEXT NOT NULL,frequency TEXT NOT NULL,"
                "request_start TEXT NOT NULL,request_end TEXT NOT NULL,symbols_json TEXT NOT NULL,"
                "status TEXT NOT NULL,requested_count INTEGER NOT NULL,succeeded_count INTEGER NOT NULL,"
                "pending_count INTEGER NOT NULL,created_at REAL NOT NULL,updated_at REAL NOT NULL)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS ix_refresh_batches_exact ON refresh_batches("
                "provider,frequency,request_start,request_end,symbols_json,updated_at DESC)"
            )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS refresh_pending ("
                "batch_id TEXT NOT NULL,symbol TEXT NOT NULL,reason TEXT NOT NULL,"
                "diagnostic_code TEXT NOT NULL,attempts INTEGER NOT NULL,last_attempt_at REAL NOT NULL,"
                "PRIMARY KEY(batch_id,symbol),"
                "FOREIGN KEY(batch_id) REFERENCES refresh_batches(batch_id) ON DELETE CASCADE)"
            )
            conn.execute(f"PRAGMA user_version={self.SCHEMA_VERSION}")

    @staticmethod
    def _symbols_json(symbols: Iterable[str]) -> str:
        return json.dumps(list(dict.fromkeys(symbols)), ensure_ascii=False, separators=(",", ":"))

    def begin_or_resume(
        self, symbols: Iterable[str], start: str, end: str, *, frequency: str, provider: str,
    ) -> tuple[str, tuple[str, ...], bool]:
        symbols_json = self._symbols_json(symbols)
        with self._conn() as conn:
            row = conn.execute(
                "SELECT batch_id FROM refresh_batches WHERE provider=? AND frequency=? "
                "AND request_start=? AND request_end=? AND symbols_json=? AND status='partial' "
                "ORDER BY updated_at DESC LIMIT 1",
                (provider, frequency, start, end, symbols_json),
            ).fetchone()
            if row:
                pending = tuple(str(item[0]) for item in conn.execute(
                    "SELECT symbol FROM refresh_pending WHERE batch_id=? ORDER BY rowid", (row[0],),
                ))
                return str(row[0]), pending, True
            batch_id = uuid.uuid4().hex
            requested = tuple(json.loads(symbols_json))
            now = time.time()
            conn.execute(
                "INSERT INTO refresh_batches VALUES (?,?,?,?,?,?,'partial',?,?,?,?,?)",
                (batch_id, provider, frequency, start, end, symbols_json,
                 len(requested), 0, len(requested), now, now),
            )
            conn.executemany(
                "INSERT INTO refresh_pending VALUES (?,?,?,'not_attempted',0,0)",
                ((batch_id, symbol, "尚未尝试") for symbol in requested),
            )
            return batch_id, requested, False

    def record_success(self, batch_id: str, symbol: str) -> None:
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "DELETE FROM refresh_pending WHERE batch_id=? AND symbol=?", (batch_id, symbol),
            )
            self._update_counts(conn, batch_id)

    def record_failure(
        self, batch_id: str, symbol: str, reason: str, diagnostic_code: str = "refresh_failed",
    ) -> None:
        now = time.time()
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT INTO refresh_pending VALUES (?,?,?,?,1,?) "
                "ON CONFLICT(batch_id,symbol) DO UPDATE SET reason=excluded.reason,"
                "diagnostic_code=excluded.diagnostic_code,attempts=refresh_pending.attempts+1,"
                "last_attempt_at=excluded.last_attempt_at",
                (batch_id, symbol, str(reason)[:1000], diagnostic_code, now),
            )
            self._update_counts(conn, batch_id)

    @staticmethod
    def _update_counts(conn: sqlite3.Connection, batch_id: str) -> None:
        row = conn.execute(
            "SELECT requested_count FROM refresh_batches WHERE batch_id=?", (batch_id,),
        ).fetchone()
        if row is None:
            return
        requested = int(row[0])
        pending = int(conn.execute(
            "SELECT COUNT(*) FROM refresh_pending WHERE batch_id=?", (batch_id,),
        ).fetchone()[0])
        status = (
            CacheResultKind.SUCCESS.value
            if pending == 0
            else CacheResultKind.PARTIAL.value
        )
        conn.execute(
            "UPDATE refresh_batches SET status=?,succeeded_count=?,pending_count=?,updated_at=? "
            "WHERE batch_id=?",
            (status, requested - pending, pending, time.time(), batch_id),
        )
        cache_registry.observe(
            "market.bars",
            state=status,
            pending_completed=requested - pending,
            pending_total=requested,
        )

    def summary(self, batch_id: str) -> dict[str, Any]:
        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM refresh_batches WHERE batch_id=?", (batch_id,),
            ).fetchone()
            pending = conn.execute(
                "SELECT symbol,reason,diagnostic_code,attempts,last_attempt_at "
                "FROM refresh_pending WHERE batch_id=? ORDER BY rowid", (batch_id,),
            ).fetchall()
        if row is None:
            return {}
        result = dict(row)
        result["symbols"] = json.loads(str(result.pop("symbols_json")))
        result["pending"] = [dict(item) for item in pending]
        missing = [{
            "item": item["symbol"],
            "reason": self._missing_reason(item["diagnostic_code"]),
            "detail": item["reason"],
            "diagnostic_code": item["diagnostic_code"],
            "attempts": item["attempts"],
        } for item in result["pending"]]
        missing_names = {item["item"] for item in missing}
        completed = [symbol for symbol in result["symbols"] if symbol not in missing_names]
        reason_counts = Counter(item["reason"] for item in missing)
        result.update({
            "requested": list(result["symbols"]),
            "completed": completed,
            "missing": missing,
            "complete": not missing,
            "reason_counts": dict(sorted(reason_counts.items())),
        })
        return result

    def latest_exact(
        self, symbols: Iterable[str], start: str, end: str, *, frequency: str, provider: str,
    ) -> dict[str, Any]:
        symbols_json = self._symbols_json(symbols)
        with self._conn() as conn:
            row = conn.execute(
                "SELECT batch_id FROM refresh_batches WHERE provider=? AND frequency=? "
                "AND request_start=? AND request_end=? AND symbols_json=? "
                "ORDER BY updated_at DESC LIMIT 1",
                (provider, frequency, start, end, symbols_json),
            ).fetchone()
        return self.summary(str(row[0])) if row else {}
