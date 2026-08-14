from datetime import UTC, date, datetime

import pytest

from quantmaster.market_sessions import (
    DailyBarCompletion,
    MarketCalendar,
    MarketId,
    SessionPhase,
    assess_daily_bar,
)
from quantmaster.temporal import (
    InformationBoundary,
    KnowledgeMode,
    TemporalContractError,
    information_time,
    parse_business_date,
    parse_instant,
)


def _calendar(market: MarketId, *sessions: date, half_days=()) -> MarketCalendar:
    return MarketCalendar(
        market, frozenset(sessions), "official-fixture", "2026-test", True,
        frozenset(half_days),
    )


def test_business_date_and_cutoff_are_distinct_contracts() -> None:
    assert parse_business_date("2026-08-13") == date(2026, 8, 13)
    with pytest.raises(TemporalContractError, match="业务日期"):
        parse_business_date(datetime(2026, 8, 13, tzinfo=UTC))
    with pytest.raises(TemporalContractError, match="时区"):
        InformationBoundary(
            date(2026, 8, 13), datetime(2026, 8, 13, 15), "Asia/Shanghai",
        )


def test_provider_parser_never_guesses_timezone_or_unix_unit() -> None:
    with pytest.raises(TemporalContractError, match="s 或 ms"):
        parse_instant(1786582800000, field="published_at")
    with pytest.raises(TemporalContractError, match="缺少时区"):
        parse_instant("2026-08-13T09:00:00", field="published_at")
    assert parse_instant(
        1786582800000, field="published_at", unix_unit="ms",
    ).tzinfo == UTC
    assert parse_instant(
        "2026-08-13T09:00:00", field="published_at",
        default_timezone="Asia/Shanghai",
    ).isoformat() == "2026-08-13T01:00:00+00:00"


def test_replay_modes_choose_observed_or_trusted_published_explicitly() -> None:
    published = datetime(2026, 8, 1, tzinfo=UTC)
    observed = datetime(2026, 8, 10, tzinfo=UTC)
    assert information_time(
        published_at=published, first_observed_at=observed,
        mode=KnowledgeMode.STRICT_OBSERVED,
    ) == observed
    assert information_time(
        published_at=published, first_observed_at=observed,
        mode=KnowledgeMode.TRUSTED_PUBLISHED,
    ) == published


def test_daily_bar_requires_close_provider_ingest_and_complete_coverage() -> None:
    session = date(2026, 8, 13)
    calendar = _calendar(MarketId.CN, session)

    def state(cutoff: datetime, published=None, ingested=None, complete=False):
        boundary = InformationBoundary(session, cutoff, "Asia/Shanghai")
        return assess_daily_bar(
            calendar=calendar, session_date=session, boundary=boundary,
            provider_published_at=published, ingested_at=ingested,
            coverage_complete=complete,
        ).completion

    zone = calendar.timezone
    assert state(datetime(2026, 8, 13, 10, tzinfo=zone)) == (
        DailyBarCompletion.CURRENT_SESSION_PARTIAL
    )
    assert state(datetime(2026, 8, 13, 15, 5, tzinfo=zone)) == (
        DailyBarCompletion.CURRENT_SESSION_CLOSED_WAITING_PROVIDER
    )
    published = datetime(2026, 8, 13, 15, 10, tzinfo=zone)
    assert state(datetime(2026, 8, 13, 15, 20, tzinfo=zone), published) == (
        DailyBarCompletion.CURRENT_SESSION_PROVIDER_PUBLISHED_WAITING_INGEST
    )
    ingested = datetime(2026, 8, 13, 15, 15, tzinfo=zone)
    assert state(
        datetime(2026, 8, 13, 15, 20, tzinfo=zone), published, ingested, True,
    ) == DailyBarCompletion.CURRENT_SESSION_COMPLETE


def test_market_windows_cover_hk_half_day_and_us_dst() -> None:
    hk_day = date(2026, 12, 24)
    hk = _calendar(MarketId.HK, hk_day, half_days=(hk_day,))
    assert hk.close_at(hk_day).hour == 12
    assert hk.close_at(hk_day).minute == 10

    us_before = date(2026, 3, 6)
    us_after = date(2026, 3, 9)
    us = _calendar(MarketId.US, us_before, us_after)
    before_open = us.windows(us_before)[0].starts_at.astimezone(UTC)
    after_open = us.windows(us_after)[0].starts_at.astimezone(UTC)
    assert before_open.hour == 14
    assert after_open.hour == 13
    assert us.phase_at(datetime(2026, 3, 9, 14, tzinfo=UTC)) in {
        SessionPhase.OPENING_AUCTION, SessionPhase.CONTINUOUS,
    }


def test_previous_and_next_sessions_never_use_calendar_day_arithmetic() -> None:
    calendar = _calendar(
        MarketId.CN, date(2026, 9, 30), date(2026, 10, 9),
    )
    assert calendar.previous_session(date(2026, 10, 7)) == date(2026, 9, 30)
    assert calendar.next_session(date(2026, 10, 1)) == date(2026, 10, 9)
