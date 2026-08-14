from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from quantmaster.data import registry


def _bars(index: pd.DatetimeIndex) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 10.5,
            "volume": 100.0,
        },
        index=index,
    )


@pytest.fixture(autouse=True)
def _known_units(monkeypatch):
    monkeypatch.setattr(
        registry,
        "_unit_contract",
        lambda _symbol: (registry.BarDataQuality("degraded", "", "").units, ""),
    )


def test_naive_intraday_without_provider_timezone_fails_closed() -> None:
    frame = _bars(pd.DatetimeIndex(["2026-08-07 10:00:00"]))

    quality = registry._assess_intraday_frame(
        frame,
        "2026-08-07 09:30:00",
        "2026-08-07 10:00:00",
        symbol="600000.SH",
        frequency="30m",
        source="ambiguous-provider",
    )

    assert quality.status == "unavailable"
    assert quality.timezone == "Asia/Shanghai"
    assert any(issue.startswith("TIME_UNZONED:") for issue in quality.issues)


def test_hk_uses_hong_kong_timezone_and_hk_lunch_window() -> None:
    index = pd.DatetimeIndex([
        "2026-08-07 10:30", "2026-08-07 11:30",
        "2026-08-07 14:00", "2026-08-07 15:00", "2026-08-07 16:00",
    ])
    frame = _bars(index)
    frame.attrs["timezone"] = "Asia/Hong_Kong"

    quality = registry._assess_intraday_frame(
        frame,
        "2026-08-07 09:30:00",
        "2026-08-07 16:00:00",
        symbol="00700.HK",
        frequency="60m",
        source="fixture",
    )

    assert quality.status == "degraded"
    assert quality.coverage_ratio == 1.0
    assert quality.timezone == "Asia/Hong_Kong"
    assert quality.calendar_source == "hk:observed-session-dates"
    assert not any("桶覆盖率" in issue for issue in quality.issues)


def test_intraday_start_labels_map_to_end_buckets_and_flag_off_grid() -> None:
    day = pd.Timestamp("2026-08-07")
    expected: set[pd.Timestamp] = set()
    observed: set[pd.Timestamp] = set()

    off_grid_rows = registry._add_intraday_window_evidence(
        pd.DatetimeIndex([
            day + pd.Timedelta(hours=9, minutes=30),
            day + pd.Timedelta(hours=9, minutes=35),
            day + pd.Timedelta(hours=9, minutes=37),
        ]),
        expected,
        observed,
        session_start=day + pd.Timedelta(hours=9, minutes=30),
        session_end=day + pd.Timedelta(hours=11, minutes=30),
        requested_start=day + pd.Timedelta(hours=9, minutes=30),
        requested_end=day + pd.Timedelta(hours=11, minutes=30),
        frequency_minutes=5,
    )

    assert day + pd.Timedelta(hours=9, minutes=35) in expected
    assert observed == {
        day + pd.Timedelta(hours=9, minutes=35),
        day + pd.Timedelta(hours=9, minutes=40),
    }
    assert off_grid_rows == 1


@pytest.mark.parametrize(
    ("session", "utc_stamp"),
    [
        ("2026-03-06", "2026-03-06T14:31:00+00:00"),
        ("2026-03-09", "2026-03-09T13:31:00+00:00"),
    ],
)
def test_us_utc_bars_follow_new_york_dst(session: str, utc_stamp: str) -> None:
    frame = _bars(pd.DatetimeIndex([utc_stamp]))

    quality = registry._assess_intraday_frame(
        frame,
        f"{session} 09:30:00",
        f"{session} 09:31:00",
        symbol="AAPL.US",
        frequency="1m",
        source="fixture",
    )

    assert quality.status == "degraded"
    assert quality.coverage_ratio == 1.0
    assert quality.timezone == "America/New_York"
    assert quality.observed_start == f"{session}T09:31:00"


def test_futures_without_product_session_template_are_unsupported() -> None:
    frame = _bars(pd.DatetimeIndex(["2026-08-07T02:00:00+00:00"]))

    quality = registry._assess_intraday_frame(
        frame,
        "2026-08-07T00:00:00+00:00",
        "2026-08-07T03:00:00+00:00",
        symbol="CL.CONTINUOUS",
        frequency="60m",
        source="fixture",
    )

    assert quality.status == "unavailable"
    assert quality.timezone == "unknown"
    assert any(issue.startswith("MARKET_SESSION_UNSUPPORTED:") for issue in quality.issues)


def test_current_us_daily_bar_is_partial_before_close(monkeypatch) -> None:
    monkeypatch.setattr(
        registry,
        "market_now",
        lambda: datetime(2026, 3, 9, 10, 0, tzinfo=ZoneInfo("America/New_York")),
    )
    frame = _bars(pd.DatetimeIndex(["2026-03-09"]))

    quality = registry._assess_daily_frame(
        frame,
        "2026-03-09",
        "2026-03-09",
        symbol="AAPL.US",
        source="fixture",
    )

    assert quality.status == "degraded"
    assert quality.partial is True
    assert quality.timezone == "America/New_York"
    assert any(issue.startswith("CURRENT_SESSION_PARTIAL:") for issue in quality.issues)


def test_closed_daily_bar_waits_for_provider_publication(monkeypatch) -> None:
    monkeypatch.setattr(
        registry,
        "market_now",
        lambda: datetime(2026, 3, 9, 17, 0, tzinfo=ZoneInfo("America/New_York")),
    )
    frame = _bars(pd.DatetimeIndex(["2026-03-09"]))

    quality = registry._assess_daily_frame(
        frame,
        "2026-03-09",
        "2026-03-09",
        symbol="AAPL.US",
        source="fixture",
    )

    assert quality.partial is True
    assert any(
        issue.startswith("CURRENT_SESSION_CLOSED_WAITING_PROVIDER:")
        for issue in quality.issues
    )
