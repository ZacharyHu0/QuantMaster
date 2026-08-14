"""Exchange-local session clocks and daily-bar availability states."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from enum import StrEnum
from zoneinfo import ZoneInfo

from quantmaster.temporal import InformationBoundary, TemporalContractError


class MarketId(StrEnum):
    CN = "CN"
    HK = "HK"
    US = "US"


MARKET_TIMEZONES = {
    MarketId.CN: "Asia/Shanghai",
    MarketId.HK: "Asia/Hong_Kong",
    MarketId.US: "America/New_York",
}


class SessionPhase(StrEnum):
    CLOSED = "closed"
    PRE_OPEN = "pre_open"
    OPENING_AUCTION = "opening_auction"
    CONTINUOUS = "continuous"
    BREAK = "break"
    CLOSING_AUCTION = "closing_auction"
    POST_CLOSE = "post_close"


class DailyBarCompletion(StrEnum):
    PREVIOUS_SESSION_COMPLETE = "previous_session_complete"
    CURRENT_SESSION_PREOPEN = "current_session_preopen"
    CURRENT_SESSION_PARTIAL = "current_session_partial"
    CURRENT_SESSION_CLOSED_WAITING_PROVIDER = "current_session_closed_waiting_provider"
    CURRENT_SESSION_PROVIDER_PUBLISHED_WAITING_INGEST = (
        "current_session_provider_published_waiting_ingest"
    )
    CURRENT_SESSION_COMPLETE = "current_session_complete"
    CALENDAR_UNAVAILABLE = "calendar_unavailable"


@dataclass(frozen=True)
class SessionWindow:
    phase: SessionPhase
    starts_at: datetime
    ends_at: datetime
    matching: bool


@dataclass(frozen=True)
class MarketCalendar:
    market: MarketId
    sessions: frozenset[date]
    source: str
    revision: str
    verified: bool = True
    half_days: frozenset[date] = frozenset()

    @property
    def timezone(self) -> ZoneInfo:
        return ZoneInfo(MARKET_TIMEZONES[self.market])

    def require_session(self, value: date) -> date:
        if not self.verified or not self.source or not self.revision:
            raise TemporalContractError("交易日历证据不可用")
        if value not in self.sessions:
            raise TemporalContractError(f"{value.isoformat()} 不是已验证交易日")
        return value

    def previous_session(self, value: date, *, inclusive: bool = False) -> date | None:
        return max(
            (item for item in self.sessions if item <= value)
            if inclusive else (item for item in self.sessions if item < value),
            default=None,
        )

    def next_session(self, value: date, *, inclusive: bool = False) -> date | None:
        return min(
            (item for item in self.sessions if item >= value)
            if inclusive else (item for item in self.sessions if item > value),
            default=None,
        )

    def windows(self, session_date: date) -> tuple[SessionWindow, ...]:
        day = self.require_session(session_date)
        zone = self.timezone

        def item(phase: SessionPhase, start: time, end: time, matching: bool) -> SessionWindow:
            return SessionWindow(
                phase, datetime.combine(day, start, zone), datetime.combine(day, end, zone),
                matching,
            )

        if self.market == MarketId.CN:
            return (
                item(SessionPhase.OPENING_AUCTION, time(9, 15), time(9, 25), False),
                item(SessionPhase.PRE_OPEN, time(9, 25), time(9, 30), False),
                item(SessionPhase.CONTINUOUS, time(9, 30), time(11, 30), True),
                item(SessionPhase.BREAK, time(11, 30), time(13), False),
                item(SessionPhase.CONTINUOUS, time(13), time(14, 57), True),
                item(SessionPhase.CLOSING_AUCTION, time(14, 57), time(15), False),
            )
        if self.market == MarketId.HK:
            half = day in self.half_days
            result = [
                item(SessionPhase.PRE_OPEN, time(9), time(9, 20), False),
                item(SessionPhase.OPENING_AUCTION, time(9, 20), time(9, 22), False),
                item(SessionPhase.PRE_OPEN, time(9, 22), time(9, 30), False),
                item(SessionPhase.CONTINUOUS, time(9, 30), time(12), True),
            ]
            if not half:
                result.extend((
                    item(SessionPhase.BREAK, time(12), time(13), False),
                    item(SessionPhase.CONTINUOUS, time(13), time(16), True),
                ))
            result.append(item(
                SessionPhase.CLOSING_AUCTION,
                time(12) if half else time(16),
                time(12, 10) if half else time(16, 10), False,
            ))
            return tuple(result)
        close = time(13) if day in self.half_days else time(16)
        return (
            item(SessionPhase.OPENING_AUCTION, time(9, 30), time(9, 30, 1), False),
            item(SessionPhase.CONTINUOUS, time(9, 30, 1), close, True),
        )

    def close_at(self, session_date: date) -> datetime:
        return max(window.ends_at for window in self.windows(session_date))

    def phase_at(self, instant: datetime) -> SessionPhase:
        if instant.tzinfo is None or instant.utcoffset() is None:
            raise TemporalContractError("市场时钟必须包含时区")
        local = instant.astimezone(self.timezone)
        if local.date() not in self.sessions:
            return SessionPhase.CLOSED
        windows = self.windows(local.date())
        for window in windows:
            if window.starts_at <= local < window.ends_at:
                return window.phase
        if local < windows[0].starts_at:
            return SessionPhase.PRE_OPEN
        return SessionPhase.POST_CLOSE


@dataclass(frozen=True)
class DailyBarAvailability:
    session_date: date
    completion: DailyBarCompletion
    market_timezone: str
    cutoff_at: datetime
    provider_published_at: datetime | None = None
    ingested_at: datetime | None = None
    diagnostics: tuple[str, ...] = ()

    @property
    def formal_eligible(self) -> bool:
        return self.completion in {
            DailyBarCompletion.PREVIOUS_SESSION_COMPLETE,
            DailyBarCompletion.CURRENT_SESSION_COMPLETE,
        }


def assess_daily_bar(
    *,
    calendar: MarketCalendar,
    session_date: date,
    boundary: InformationBoundary,
    provider_published_at: datetime | None,
    ingested_at: datetime | None,
    coverage_complete: bool,
) -> DailyBarAvailability:
    if boundary.market_timezone != str(calendar.timezone):
        raise TemporalContractError("cutoff 的市场时区与交易日历不一致")
    try:
        calendar.require_session(session_date)
    except TemporalContractError as exc:
        return DailyBarAvailability(
            session_date, DailyBarCompletion.CALENDAR_UNAVAILABLE,
            str(calendar.timezone), boundary.cutoff_at, diagnostics=(str(exc),),
        )
    cutoff = boundary.cutoff_utc
    close = calendar.close_at(session_date).astimezone(UTC)
    local_cutoff_date = boundary.cutoff_at.astimezone(calendar.timezone).date()
    if session_date < local_cutoff_date:
        if (
            coverage_complete and provider_published_at and ingested_at
            and provider_published_at.astimezone(UTC) <= cutoff
            and ingested_at.astimezone(UTC) <= cutoff
        ):
            state = DailyBarCompletion.PREVIOUS_SESSION_COMPLETE
        else:
            state = DailyBarCompletion.CALENDAR_UNAVAILABLE
        return DailyBarAvailability(
            session_date, state, str(calendar.timezone), boundary.cutoff_at,
            provider_published_at, ingested_at,
            () if state != DailyBarCompletion.CALENDAR_UNAVAILABLE else (
                "历史日线缺少 cutoff 前的发布、摄取或完整覆盖证据",
            ),
        )
    if cutoff < calendar.windows(session_date)[0].starts_at.astimezone(UTC):
        state = DailyBarCompletion.CURRENT_SESSION_PREOPEN
    elif cutoff < close:
        state = DailyBarCompletion.CURRENT_SESSION_PARTIAL
    elif not provider_published_at or provider_published_at.astimezone(UTC) > cutoff:
        state = DailyBarCompletion.CURRENT_SESSION_CLOSED_WAITING_PROVIDER
    elif not ingested_at or ingested_at.astimezone(UTC) > cutoff or not coverage_complete:
        state = DailyBarCompletion.CURRENT_SESSION_PROVIDER_PUBLISHED_WAITING_INGEST
    else:
        state = DailyBarCompletion.CURRENT_SESSION_COMPLETE
    return DailyBarAvailability(
        session_date, state, str(calendar.timezone), boundary.cutoff_at,
        provider_published_at, ingested_at,
    )
