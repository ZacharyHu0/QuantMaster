"""Exchange-time and market-evidence contracts for paper order matching.

The module deliberately does not fetch data.  Callers must first inspect the
local :class:`~quantmaster.data.storage.BarStore` and may enqueue a remote
backfill only for the explicit gaps returned here.  Likewise, trading dates
must come from verified exchange/calendar evidence; weekdays are never used as
a holiday calendar.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from enum import StrEnum
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import pandas as pd

from quantmaster.data.base import Market
from quantmaster.data.semantics import NumericSemantics, PriceType
from quantmaster.market_capabilities import guess_market

if TYPE_CHECKING:
    from quantmaster.data.storage import BarStore


class PaperMarket(StrEnum):
    CN = "cn"
    HK = "hk"
    US = "us"


class MarketPhase(StrEnum):
    CLOSED = "closed"
    PRE_OPEN = "pre_open"
    OPENING_AUCTION = "opening_auction"
    CONTINUOUS = "continuous"
    LUNCH_BREAK = "lunch_break"
    CLOSING_AUCTION = "closing_auction"
    POST_CLOSE = "post_close"


@dataclass(frozen=True)
class CalendarEvidence:
    """Verified exchange sessions used by matching.

    ``sessions`` and ``half_days`` are exchange-local dates.  The caller is
    responsible for persisting the source/version that proved them.  Empty or
    unverified evidence fails closed instead of treating weekdays as sessions.
    """

    market: PaperMarket
    sessions: frozenset[date]
    source: str
    verified: bool = True
    half_days: frozenset[date] = frozenset()

    @classmethod
    def build(
        cls,
        market: PaperMarket | str,
        sessions: Iterable[date | str],
        *,
        source: str,
        verified: bool = True,
        half_days: Iterable[date | str] = (),
    ) -> CalendarEvidence:
        return cls(
            PaperMarket(market),
            frozenset(_as_date(value) for value in sessions),
            str(source).strip(),
            bool(verified),
            frozenset(_as_date(value) for value in half_days),
        )

    def require_session(self, value: date | str) -> date:
        session = _as_date(value)
        if not self.verified or not self.source:
            raise ValueError("交易日历证据未验证")
        if session not in self.sessions:
            raise ValueError(f"{session.isoformat()} 不在已验证交易日历中")
        return session

    def next_session(self, after: date | str) -> date | None:
        cursor = _as_date(after)
        if not self.verified or not self.source:
            return None
        return min((value for value in self.sessions if value > cursor), default=None)


@dataclass(frozen=True)
class SessionWindow:
    phase: MarketPhase
    starts_at: datetime
    ends_at: datetime
    matching: bool


@dataclass(frozen=True)
class MarketClock:
    market: PaperMarket
    timezone: str
    session: date | None
    phase: MarketPhase
    matching: bool
    next_match_at: datetime | None
    reason: str


@dataclass(frozen=True)
class LocalMarketDataGap:
    symbol: str
    requested_start: date
    requested_end: date
    available_sessions: tuple[date, ...]
    missing_sessions: tuple[date, ...]
    last_available_session: date | None
    local_status: str
    local_source: str

    @property
    def remote_needed(self) -> bool:
        """Only these explicit missing sessions may be remotely supplemented."""
        return bool(self.missing_sessions)


@dataclass(frozen=True)
class DailyBarEvidence:
    symbol: str
    session: date
    open_price: float
    observed_at: datetime
    source: str
    semantics: NumericSemantics | None = None


def market_for_symbol(symbol: str) -> PaperMarket:
    market = guess_market(symbol)
    if market == Market.CN:
        return PaperMarket.CN
    if market == Market.HK:
        return PaperMarket.HK
    if market == Market.US:
        return PaperMarket.US
    raise ValueError(f"模拟撮合尚不支持 {market.value} 市场：{symbol}")


def market_timezone(market: PaperMarket | str) -> ZoneInfo:
    return ZoneInfo({
        PaperMarket.CN: "Asia/Shanghai",
        PaperMarket.HK: "Asia/Hong_Kong",
        PaperMarket.US: "America/New_York",
    }[PaperMarket(market)])


def session_windows(
    evidence: CalendarEvidence,
    session: date | str,
) -> tuple[SessionWindow, ...]:
    day = evidence.require_session(session)
    timezone = market_timezone(evidence.market)

    def window(
        phase: MarketPhase, start: time, end: time, matching: bool,
    ) -> SessionWindow:
        return SessionWindow(
            phase,
            datetime.combine(day, start, timezone),
            datetime.combine(day, end, timezone),
            matching,
        )

    if evidence.market == PaperMarket.CN:
        return (
            window(MarketPhase.OPENING_AUCTION, time(9, 15), time(9, 25), False),
            window(MarketPhase.PRE_OPEN, time(9, 25), time(9, 30), False),
            window(MarketPhase.CONTINUOUS, time(9, 30), time(11, 30), True),
            window(MarketPhase.LUNCH_BREAK, time(11, 30), time(13), False),
            window(MarketPhase.CONTINUOUS, time(13), time(14, 57), True),
            window(MarketPhase.CLOSING_AUCTION, time(14, 57), time(15), False),
        )
    if evidence.market == PaperMarket.HK:
        closing_start = time(12) if day in evidence.half_days else time(16)
        values = [
            window(MarketPhase.PRE_OPEN, time(9), time(9, 20), False),
            # HKEX performs the opening match at a random instant from 09:20
            # through 09:22; the whole interval is conservatively an auction.
            window(MarketPhase.OPENING_AUCTION, time(9, 20), time(9, 22), False),
            window(MarketPhase.PRE_OPEN, time(9, 22), time(9, 30), False),
            window(MarketPhase.CONTINUOUS, time(9, 30), time(12), True),
        ]
        if day not in evidence.half_days:
            values.extend((
                window(MarketPhase.LUNCH_BREAK, time(12), time(13), False),
                window(MarketPhase.CONTINUOUS, time(13), time(16), True),
            ))
        values.append(
            window(
                MarketPhase.CLOSING_AUCTION,
                closing_start,
                time(12, 10) if day in evidence.half_days else time(16, 10),
                False,
            )
        )
        return tuple(values)
    close = time(13) if day in evidence.half_days else time(16)
    # This contract models the US core session.  Extended-hours execution must
    # be an explicit future policy, never an accidental side effect.
    return (
        window(MarketPhase.OPENING_AUCTION, time(9, 30), time(9, 30, 1), False),
        window(MarketPhase.CONTINUOUS, time(9, 30, 1), close, True),
    )


def market_clock(evidence: CalendarEvidence, now: datetime) -> MarketClock:
    timezone = market_timezone(evidence.market)
    current = _aware(now).astimezone(timezone)
    sessions = sorted(evidence.sessions)
    if not evidence.verified or not evidence.source:
        return MarketClock(
            evidence.market, str(timezone), None, MarketPhase.CLOSED, False, None,
            "交易日历证据未验证",
        )
    today = current.date()
    if today in evidence.sessions:
        windows = session_windows(evidence, today)
        for item in windows:
            if item.starts_at <= current < item.ends_at:
                next_match = (
                    current
                    if item.matching
                    else item.ends_at
                    if item.phase in {MarketPhase.OPENING_AUCTION, MarketPhase.CLOSING_AUCTION}
                    else _next_execution(windows, current)
                )
                return MarketClock(
                    evidence.market, str(timezone), today, item.phase, item.matching,
                    next_match, _phase_reason(item.phase),
                )
        next_today = _next_execution(windows, current)
        if next_today is not None:
            return MarketClock(
                evidence.market, str(timezone), today, MarketPhase.PRE_OPEN, False,
                next_today, "等待本交易日下一可撮合时段",
            )
    next_day = min((value for value in sessions if value > today), default=None)
    next_match = None
    if next_day is not None:
        next_match = _next_execution(
            session_windows(evidence, next_day),
            datetime.combine(next_day, time.min, market_timezone(evidence.market)),
        )
    return MarketClock(
        evidence.market,
        str(timezone),
        today if today in evidence.sessions else None,
        MarketPhase.POST_CLOSE if today in evidence.sessions else MarketPhase.CLOSED,
        False,
        next_match,
        "等待下一个已验证交易日" if next_day else "已验证日历范围内没有后续交易日",
    )


def inspect_local_daily_bars(
    symbol: str,
    start: date | str,
    end: date | str,
    evidence: CalendarEvidence,
    *,
    store: BarStore | None = None,
) -> LocalMarketDataGap:
    """Inspect StockDB/local cache without contacting any provider."""
    from quantmaster.data.storage import BarStore

    requested_start, requested_end = _as_date(start), _as_date(end)
    if requested_end < requested_start:
        raise ValueError("行情结束日不能早于开始日")
    if evidence.market != market_for_symbol(symbol):
        raise ValueError("证券市场与交易日历不一致")
    expected = tuple(sorted(
        value for value in evidence.sessions if requested_start <= value <= requested_end
    ))
    local_store = store or BarStore(read_only=True)
    frame = local_store.get(symbol)
    available: tuple[date, ...] = ()
    if frame is not None and not frame.empty:
        available = tuple(sorted({
            pd.Timestamp(value).date()
            for value in frame.index
            if requested_start <= pd.Timestamp(value).date() <= requested_end
        }))
    metadata = local_store.metadata(symbol) or {}
    available_set = set(available)
    missing = tuple(value for value in expected if value not in available_set)
    return LocalMarketDataGap(
        symbol=symbol,
        requested_start=requested_start,
        requested_end=requested_end,
        available_sessions=available,
        missing_sessions=missing,
        last_available_session=max(available, default=None),
        local_status=str(metadata.get("last_status") or ("ready" if available else "missing")),
        local_source=str(metadata.get("last_source") or "local-cache"),
    )


def select_next_open_bar(
    bars: Sequence[DailyBarEvidence],
    *,
    after_session: date | str,
    decision_at: datetime,
    evidence: CalendarEvidence,
) -> DailyBarEvidence | None:
    """Return the next evidenced open without look-ahead.

    A bar must be for the first verified session after the cursor, must already
    have been observed by ``decision_at``, and cannot be dated after the
    exchange-local decision date.  Skipping a missing first session would move
    the recovery cursor and is therefore rejected by returning ``None``.
    """
    next_session = evidence.next_session(after_session)
    if next_session is None:
        return None
    current = _aware(decision_at)
    local_now = current.astimezone(market_timezone(evidence.market))
    if next_session > local_now.date():
        return None
    windows = session_windows(evidence, next_session)
    if evidence.market == PaperMarket.CN:
        # The 09:25 auction result precedes a non-matching cooling-off period.
        # A daily open is executable only once continuous trading starts.
        open_available_at = next(item.starts_at for item in windows if item.matching)
    else:
        # Preserve each market's explicit opening-auction policy.
        open_available_at = next(
            item.ends_at for item in windows if item.phase == MarketPhase.OPENING_AUCTION
        )
    if current < open_available_at.astimezone(current.tzinfo):
        return None
    candidates = [bar for bar in bars if bar.session == next_session]
    if len(candidates) > 1:
        raise ValueError(f"{next_session.isoformat()} 存在重复开盘行情证据")
    if not candidates:
        return None
    bar = candidates[0]
    if market_for_symbol(bar.symbol) != evidence.market:
        raise ValueError("行情证券市场与交易日历不一致")
    observed = _aware(bar.observed_at)
    if observed > current:
        raise ValueError("拒绝使用在撮合决策之后才观测到的未来行情")
    if not bar.source.strip():
        raise ValueError("开盘行情缺少来源引用")
    if bar.semantics is None:
        raise ValueError("模拟撮合缺少价格数值语义")
    if bar.semantics.price_type != PriceType.RAW:
        raise ValueError("模拟撮合仅允许当时真实可交易 raw 价格")
    if bar.semantics.instrument != bar.symbol:
        raise ValueError("模拟撮合价格语义与订单标的不一致")
    if bar.semantics.intended_use != "paper_trading":
        raise ValueError("价格合同未授权用于模拟撮合")
    bar.semantics.require_formal()
    if not pd.notna(bar.open_price) or float(bar.open_price) <= 0:
        raise ValueError("开盘行情价格无效")
    return bar


def _next_execution(windows: Sequence[SessionWindow], current: datetime) -> datetime | None:
    candidates = [
        item.ends_at
        for item in windows
        if item.phase in {MarketPhase.OPENING_AUCTION, MarketPhase.CLOSING_AUCTION}
        and item.ends_at > current
    ]
    candidates.extend(
        item.starts_at for item in windows if item.matching and item.starts_at > current
    )
    return min(candidates, default=None)


def _phase_reason(phase: MarketPhase) -> str:
    return {
        MarketPhase.OPENING_AUCTION: "开盘集合竞价收单中，等待竞价结束价",
        MarketPhase.CONTINUOUS: "连续交易可撮合",
        MarketPhase.CLOSING_AUCTION: "收盘集合竞价收单中，等待竞价结束价",
        MarketPhase.PRE_OPEN: "等待竞价或连续交易开始",
        MarketPhase.LUNCH_BREAK: "午间休市，等待下午交易",
    }.get(phase, "市场当前不可撮合")


def _as_date(value: date | str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("撮合时间必须包含时区")
    return value.astimezone(UTC)
