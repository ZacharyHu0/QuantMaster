"""Evidence-based A-share session resolution around long holidays."""

from datetime import datetime
from zoneinfo import ZoneInfo

from quantmaster.trading_sessions import (
    SessionExpectation,
    SessionExpectationResolver,
    _latest_not_after,
    _normalize_now,
)


class FixtureResolver(SessionExpectationResolver):
    def __init__(self, sessions):
        super().__init__()
        self.sessions = sessions

    def _official_sessions(self, start, end):
        del start, end
        return self.sessions

    @staticmethod
    def _research_sessions(start, end):
        del start, end
        return []

    @staticmethod
    def _bar_sessions(end):
        del end
        return []


class ResearchFallbackResolver(FixtureResolver):
    def _official_sessions(self, start, end):
        del start, end
        raise RuntimeError("official offline")

    @staticmethod
    def _research_sessions(start, end):
        del start, end
        return ["invalid", "2026-08-01", "2026-08-04"]


class BarFallbackResolver(FixtureResolver):
    @staticmethod
    def _research_sessions(start, end):
        del start, end
        raise ValueError("catalog corrupt")

    @staticmethod
    def _bar_sessions(end):
        del end
        return ["2026-08-04"]


def test_spring_festival_and_national_day_never_use_generic_weekdays():
    timezone = ZoneInfo("Asia/Shanghai")
    spring = FixtureResolver(["2026-02-12", "2026-02-13", "2026-02-24"])
    assert spring.resolve(datetime(2026, 2, 18, 20, tzinfo=timezone)).session == "2026-02-13"

    national = FixtureResolver(["2026-09-29", "2026-09-30", "2026-10-09"])
    assert national.resolve(datetime(2026, 10, 7, 20, tzinfo=timezone)).session == "2026-09-30"


def test_morning_restart_catches_up_previous_verified_session():
    timezone = ZoneInfo("Asia/Shanghai")
    resolver = FixtureResolver(["2026-08-03", "2026-08-04"])
    result = resolver.resolve(datetime(2026, 8, 5, 9, tzinfo=timezone))
    assert result.ready is True
    assert result.session == "2026-08-04"
    assert result.source == "tushare:SSE"


def test_cold_start_without_calendar_returns_actionable_safe_skip():
    result = FixtureResolver([]).resolve(
        datetime(2026, 8, 4, 20, tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    assert result.ready is False
    assert result.session == ""
    assert "Tushare" in result.reason


def test_session_helpers_normalize_dates_and_expose_fallback_evidence(isolated_config):
    timezone = ZoneInfo("Asia/Shanghai")
    naive = _normalize_now(datetime(2026, 8, 4, 20))
    assert naive.tzinfo == timezone
    assert _normalize_now(datetime(2026, 8, 4, 12, tzinfo=ZoneInfo("UTC"))).hour == 20
    assert _latest_not_after(
        ["invalid", "2026-08-04T15:00:00", "2026-08-05"], naive.date(),
    ) == "2026-08-04"
    assert SessionExpectation("2026-08-04", "fixture", True, "verified").as_dict() == {
        "session": "2026-08-04", "source": "fixture", "ready": True, "reason": "verified",
    }

    research = ResearchFallbackResolver([]).resolve(naive)
    assert research.source == "research_lake"
    assert research.session == "2026-08-04"
    bars = BarFallbackResolver([]).resolve(naive)
    assert bars.source == "bar_catalog"
    assert bars.session == "2026-08-04"
    assert SessionExpectationResolver._research_sessions(
        naive.date(), naive.date(),
    ) == []
