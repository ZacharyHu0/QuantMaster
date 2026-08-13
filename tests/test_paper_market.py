from datetime import UTC, datetime

import pandas as pd
import pytest

from quantmaster.backtest.paper_market import (
    CalendarEvidence,
    DailyBarEvidence,
    MarketPhase,
    PaperMarket,
    inspect_local_daily_bars,
    market_clock,
    market_for_symbol,
    select_next_open_bar,
    session_windows,
)
from quantmaster.data.semantics import NumericSemantics, PriceType


def raw_paper_semantics(symbol="600000.SH"):
    return NumericSemantics(
        instrument=symbol, observation_time="exchange_session_open",
        price_type=PriceType.RAW, currency="CNY", price_unit="CNY/share",
        volume_unit="share", amount_unit="CNY", provider="free-stockdb",
        provider_interface="daily", intended_use="paper_trading",
    )
from quantmaster.data.storage import BarStore


def evidence(market, sessions, *, half_days=()):
    return CalendarEvidence.build(
        market, sessions, half_days=half_days, source=f"official:{market}",
    )


def test_cn_auction_lunch_and_holiday_are_calendar_driven():
    calendar = evidence(PaperMarket.CN, ["2026-09-30", "2026-10-08"])

    auction = market_clock(calendar, datetime.fromisoformat("2026-09-30T09:20:00+08:00"))
    lunch = market_clock(calendar, datetime.fromisoformat("2026-09-30T12:00:00+08:00"))
    holiday = market_clock(calendar, datetime.fromisoformat("2026-10-01T10:00:00+08:00"))

    assert auction.phase == MarketPhase.OPENING_AUCTION and not auction.matching
    assert auction.next_match_at.isoformat() == "2026-09-30T09:25:00+08:00"
    assert lunch.phase == MarketPhase.LUNCH_BREAK and not lunch.matching
    assert lunch.next_match_at.isoformat() == "2026-09-30T13:00:00+08:00"
    assert holiday.next_match_at.isoformat() == "2026-10-08T09:25:00+08:00"


def test_hk_full_and_half_day_sessions_include_random_auction_ranges():
    calendar = evidence(
        PaperMarket.HK, ["2026-12-23", "2026-12-24"], half_days=["2026-12-24"],
    )
    full = session_windows(calendar, "2026-12-23")
    half = session_windows(calendar, "2026-12-24")

    assert [(item.phase, item.starts_at.time(), item.ends_at.time()) for item in full][-1] == (
        MarketPhase.CLOSING_AUCTION, datetime.strptime("16:00", "%H:%M").time(),
        datetime.strptime("16:10", "%H:%M").time(),
    )
    assert [(item.phase, item.starts_at.time(), item.ends_at.time()) for item in half][-1] == (
        MarketPhase.CLOSING_AUCTION, datetime.strptime("12:00", "%H:%M").time(),
        datetime.strptime("12:10", "%H:%M").time(),
    )
    assert all(item.phase != MarketPhase.LUNCH_BREAK for item in half)


def test_us_dst_and_early_close_use_new_york_exchange_time():
    calendar = evidence(
        PaperMarket.US,
        ["2026-03-06", "2026-03-09", "2026-11-27"],
        half_days=["2026-11-27"],
    )
    winter = session_windows(calendar, "2026-03-06")[0].starts_at.astimezone(UTC)
    summer = session_windows(calendar, "2026-03-09")[0].starts_at.astimezone(UTC)
    early_close = session_windows(calendar, "2026-11-27")[-1].ends_at

    assert winter.hour == 14
    assert summer.hour == 13
    assert early_close.hour == 13


def test_unverified_calendar_fails_closed_and_never_invents_weekdays():
    calendar = CalendarEvidence.build(
        PaperMarket.CN, ["2026-08-14"], source="", verified=False,
    )
    result = market_clock(calendar, datetime.fromisoformat("2026-08-14T10:00:00+08:00"))

    assert result.phase == MarketPhase.CLOSED
    assert result.next_match_at is None
    with pytest.raises(ValueError, match="未验证"):
        session_windows(calendar, "2026-08-14")


def test_local_cache_is_inspected_first_and_reports_only_verified_gaps(tmp_path):
    store = BarStore(tmp_path / "bars")
    frame = pd.DataFrame(
        {"open": [10.0, 10.2], "high": [10.5, 10.4], "low": [9.8, 10.0],
         "close": [10.2, 10.1], "volume": [1000, 1200]},
        index=pd.to_datetime(["2026-08-10", "2026-08-12"]),
    )
    store.put(
        "600000.SH", frame, source="free-stockdb", request_start="2026-08-10",
        request_end="2026-08-12",
    )
    calendar = evidence(
        PaperMarket.CN, ["2026-08-10", "2026-08-11", "2026-08-12"],
    )

    gap = inspect_local_daily_bars(
        "600000.SH", "2026-08-10", "2026-08-12", calendar, store=store,
    )

    assert gap.local_source == "free-stockdb"
    assert gap.available_sessions == (pd.Timestamp("2026-08-10").date(), pd.Timestamp("2026-08-12").date())
    assert gap.missing_sessions == (pd.Timestamp("2026-08-11").date(),)
    assert gap.remote_needed is True


def test_recovery_cursor_never_skips_gap_or_uses_future_bar():
    calendar = evidence(PaperMarket.CN, ["2026-08-10", "2026-08-11", "2026-08-12"])
    bar_12 = DailyBarEvidence(
        "600000.SH", pd.Timestamp("2026-08-12").date(), 10.0,
        datetime.fromisoformat("2026-08-12T09:31:00+08:00"), "free-stockdb:daily",
        raw_paper_semantics(),
    )
    # The first unprocessed session is the 11th; a bar for the 12th cannot
    # advance the cursor across that missing evidence.
    assert select_next_open_bar(
        [bar_12], after_session="2026-08-10",
        decision_at=datetime.fromisoformat("2026-08-12T10:00:00+08:00"), evidence=calendar,
    ) is None

    bar_11_from_future = DailyBarEvidence(
        "600000.SH", pd.Timestamp("2026-08-11").date(), 10.0,
        datetime.fromisoformat("2026-08-11T15:30:00+08:00"), "free-stockdb:daily",
        raw_paper_semantics(),
    )
    with pytest.raises(ValueError, match="未来行情"):
        select_next_open_bar(
            [bar_11_from_future], after_session="2026-08-10",
            decision_at=datetime.fromisoformat("2026-08-11T10:00:00+08:00"),
            evidence=calendar,
        )


@pytest.mark.parametrize(
    ("symbol", "market"),
    [("600000.SH", PaperMarket.CN), ("00700.HK", PaperMarket.HK), ("AAPL.US", PaperMarket.US)],
)
def test_symbol_market_contract(symbol, market):
    assert market_for_symbol(symbol) == market
