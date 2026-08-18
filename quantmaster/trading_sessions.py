"""Shared, evidence-based A-share session expectations.

Calendar consumers must never invent Chinese holidays from generic weekdays.
This resolver accepts either the official SSE calendar or dates evidenced by
validated local market data.  With neither source it returns a safe skip.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from datetime import time as wall_time
from typing import Any
from zoneinfo import ZoneInfo

from quantmaster.config import get_config
from quantmaster.runtime.trading_session_sources import official_calendar, research_calendar
from quantmaster.stockdb_acceptance import read_stockdb_session_acceptance

SHANGHAI = ZoneInfo("Asia/Shanghai")
DAILY_SIGNAL_CUTOFF = wall_time(15, 0)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SessionExpectation:
    session: str = ""
    source: str = "unavailable"
    ready: bool = False
    reason: str = ""
    completion: str = "calendar_unavailable"
    market_timezone: str = "Asia/Shanghai"
    cutoff_at: str = ""
    coverage: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "session": self.session,
            "source": self.source,
            "ready": self.ready,
            "reason": self.reason,
            "completion": self.completion,
            "market_timezone": self.market_timezone,
            "cutoff_at": self.cutoff_at,
            "coverage": dict(self.coverage or {}),
        }


class SessionTargetUnavailable(RuntimeError):
    """A close-data consumer cannot safely choose a business-date target."""

    def __init__(self, expectation: SessionExpectation) -> None:
        self.expectation = expectation
        super().__init__(expectation.reason or "无法确认最近完成交易日")


def _normalize_now(value: datetime | None) -> datetime:
    current = value or datetime.now(SHANGHAI)
    if current.tzinfo is None:
        return current.replace(tzinfo=SHANGHAI)
    return current.astimezone(SHANGHAI)


def market_now(value: datetime | None = None) -> datetime:
    """Return an aware wall clock in the China exchange timezone.

    UTC remains the storage clock, but every default trading date must be
    derived from this function so host timezone changes cannot move a run to a
    different market day.
    """
    return _normalize_now(value)


def market_date(value: datetime | None = None) -> date:
    """Return the Shanghai market calendar date for an optional instant."""
    return market_now(value).date()


def default_close_data_end(as_of: str | None = None) -> str:
    """Resolve a close-data endpoint default without using today's wall date."""
    if as_of:
        return str(as_of)
    expectation = resolve_session_target()
    if expectation.ready and expectation.session:
        return expectation.session
    logger.warning("交易日历目标不可用，阻断默认收盘数据请求：%s", expectation.reason)
    raise SessionTargetUnavailable(expectation)


def daily_signal_cutoff(value: date | str) -> datetime:
    """Return the contractual availability cutoff for a daily close signal.

    A date-only ``as_of`` means the information set available at the Shanghai
    close, not the end of that host-machine day.  Keeping that interpretation
    here prevents individual replay consumers from drifting to UTC midnight or
    a 23:59 wall-clock cutoff.
    """
    target = date.fromisoformat(value) if isinstance(value, str) else value
    return datetime.combine(target, DAILY_SIGNAL_CUTOFF, SHANGHAI)


def _latest_not_after(values: Iterable[object], cutoff: date) -> str:
    dates: set[date] = set()
    for raw in values:
        try:
            parsed = date.fromisoformat(str(raw)[:10])
        except ValueError:
            continue
        if parsed <= cutoff:
            dates.add(parsed)
    return max(dates).isoformat() if dates else ""


class SessionExpectationResolver:
    """Resolve the latest A-share session without equating clock time with data."""

    def _official_sessions(self, start: date, end: date) -> list[str]:
        if not get_config().data.tushare_token:
            return []
        return official_calendar(start, end)

    @staticmethod
    def _research_sessions(start: date, end: date) -> list[str]:
        return research_calendar(get_config().data_root, start, end)

    @staticmethod
    def _stockdb_evidence(start: date, end: date, cutoff_at: datetime) -> tuple[str, str]:
        """Read only a self-consistent v2 StockDB acceptance marker.

        Older markers recorded updater completion rather than accepted market
        data, so they must never be reinterpreted as session evidence.
        """

        acceptance = read_stockdb_session_acceptance(get_config().free_stockdb_root)
        if acceptance is None:
            return "", "unavailable"
        session = date.fromisoformat(acceptance.session)
        if (
            not start <= session <= end
            or acceptance.updated_at.astimezone(UTC) > cutoff_at.astimezone(UTC)
        ):
            return "", "unavailable"
        completion = (
            "current_session_complete"
            if acceptance.complete
            else "current_session_provider_published_waiting_ingest"
        )
        return session.isoformat(), completion

    @classmethod
    def _validated_stockdb_sessions(cls, start: date, end: date) -> list[str]:
        """Compatibility query returning only formally complete StockDB evidence."""
        session, completion = cls._stockdb_evidence(
            start, end, datetime.now(SHANGHAI),
        )
        return [session] if session and completion == "current_session_complete" else []

    @staticmethod
    def _closed_sessions(values: Iterable[object], current: datetime) -> list[str]:
        result: list[str] = []
        for raw in values:
            try:
                session = date.fromisoformat(str(raw)[:10])
            except ValueError:
                continue
            close_at = datetime.combine(session, DAILY_SIGNAL_CUTOFF, SHANGHAI)
            if close_at <= current:
                result.append(session.isoformat())
        return result

    def resolve(self, now: datetime | None = None) -> SessionExpectation:
        current = _normalize_now(now)
        cutoff = current.date()
        start = cutoff - timedelta(days=45)
        failures: list[str] = []
        official_sessions: list[str] = []
        research_sessions: list[str] = []
        try:
            official_sessions = self._closed_sessions(
                self._official_sessions(start, cutoff), current,
            )
        except (ImportError, OSError, RuntimeError, ValueError) as exc:
            failures.append(f"官方日历不可用：{str(exc)[:160]}")
        try:
            research_sessions = self._closed_sessions(
                self._research_sessions(start, cutoff), current,
            )
        except (ImportError, OSError, RuntimeError, ValueError) as exc:
            failures.append(f"研究湖日历不可用：{str(exc)[:160]}")
        try:
            stockdb_session, stockdb_completion = self._stockdb_evidence(
                start, cutoff, current,
            )
        except (ImportError, OSError, RuntimeError, ValueError) as exc:
            failures.append(f"StockDB 验收记录不可用：{str(exc)[:160]}")
            stockdb_session, stockdb_completion = "", "unavailable"
        expected = _latest_not_after(official_sessions, cutoff)
        local_complete = _latest_not_after(
            [*research_sessions, *(
                [stockdb_session] if stockdb_completion == "current_session_complete" else []
            )],
            cutoff,
        )
        cutoff_iso = current.astimezone(UTC).isoformat()
        coverage: dict[str, Any] = {
            "official_dates": official_sessions,
            "research_dates": research_sessions,
            "stockdb_session": stockdb_session,
            "stockdb_completion": stockdb_completion,
            "failures": failures,
        }
        if (
            stockdb_session
            and stockdb_completion == "current_session_complete"
            and (not expected or stockdb_session == expected)
        ):
            return SessionExpectation(
                stockdb_session, "stockdb:validated", True,
                "StockDB 完整验收记录", stockdb_completion,
                "Asia/Shanghai", cutoff_iso, coverage,
            )
        if expected and stockdb_session == expected and stockdb_completion != "unavailable":
            ready = stockdb_completion == "current_session_complete"
            return SessionExpectation(
                expected, "stockdb:validated", ready,
                "StockDB 完整验收记录" if ready else "provider 已发布但本地覆盖尚未完整",
                stockdb_completion, "Asia/Shanghai", cutoff_iso, coverage,
            )
        if expected and local_complete == expected:
            return SessionExpectation(
                expected, "research_lake", True, "已验证本地交易分区",
                "current_session_complete", "Asia/Shanghai", cutoff_iso, coverage,
            )
        if expected:
            return SessionExpectation(
                expected, "tushare:SSE", False,
                "交易时段已结束，等待 provider 发布及本地完整摄取",
                "current_session_closed_waiting_provider", "Asia/Shanghai", cutoff_iso, coverage,
            )
        if local_complete:
            return SessionExpectation(
                local_complete, "research_lake", True, "最近完整本地交易分区",
                "previous_session_complete", "Asia/Shanghai", cutoff_iso, coverage,
            )
        action = "请配置 Tushare 交易日历或先完成一次全市场日线同步"
        detail = "；".join(failures)
        return SessionExpectation(
            source="unavailable",
            ready=False,
            reason=f"{action}{f'（{detail}）' if detail else ''}",
            completion="calendar_unavailable",
            market_timezone="Asia/Shanghai",
            cutoff_at=current.astimezone(UTC).isoformat(),
            coverage=coverage,
        )

    def resolve_explicit(self, value: date, now: datetime | None = None) -> SessionExpectation:
        current = _normalize_now(now)
        if datetime.combine(value, DAILY_SIGNAL_CUTOFF, SHANGHAI) > current:
            raise ValueError("交易日目标尚未收盘或位于未来")
        start = value - timedelta(days=45)
        official = self._official_sessions(start, value)
        research = self._research_sessions(start, value)
        stockdb_session, stockdb_completion = self._stockdb_evidence(start, value, current)
        verified = {str(item)[:10] for item in [*official, *research]}
        if stockdb_session:
            verified.add(stockdb_session)
        coverage: dict[str, Any] = {
            "official_dates": [str(item)[:10] for item in official],
            "research_dates": [str(item)[:10] for item in research],
            "stockdb_session": stockdb_session,
            "stockdb_completion": stockdb_completion,
            "verified_dates": sorted(verified),
        }
        if value.isoformat() not in verified:
            raise ValueError("交易日目标不是已验证交易日")
        if stockdb_session == value.isoformat() and stockdb_completion == "current_session_complete":
            return SessionExpectation(
                value.isoformat(), "stockdb:validated", True, "StockDB 完整验收记录",
                stockdb_completion, "Asia/Shanghai", current.astimezone(UTC).isoformat(),
                coverage,
            )
        if value.isoformat() in {str(item)[:10] for item in research}:
            return SessionExpectation(
                value.isoformat(), "research_lake", True, "已验证本地交易分区",
                "previous_session_complete", "Asia/Shanghai",
                current.astimezone(UTC).isoformat(),
                coverage,
            )
        return SessionExpectation(
            value.isoformat(), "tushare:SSE", False,
            "交易日已验证，但 cutoff 前缺少完整本地数据证据",
            "current_session_closed_waiting_provider", "Asia/Shanghai",
            current.astimezone(UTC).isoformat(),
            coverage,
        )


_resolver = SessionExpectationResolver()


def expected_session(now: datetime | None = None) -> SessionExpectation:
    return _resolver.resolve(now)


def resolve_session_target(
    as_of: str = "", now: datetime | None = None,
) -> SessionExpectation:
    """Resolve an explicit or default close-data target.

    Close-data consumers must pass an explicit session when they are handling
    a validated StockDB event.  Interactive/default callers use the shared
    completed-session resolver; they must never derive a target from the
    host's calendar date alone.
    """
    value = str(as_of or "").strip()
    if value:
        try:
            parsed = date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError("交易日目标必须使用 YYYY-MM-DD 格式") from exc
        return _resolver.resolve_explicit(parsed, now)
    return expected_session(now)
