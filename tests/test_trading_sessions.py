"""Evidence-based A-share session resolution around long holidays."""

import json
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

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


class UntrustedBarCatalogResolver(FixtureResolver):
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
    assert result.ready is False
    assert result.session == "2026-08-04"
    assert result.source == "tushare:SSE"
    assert result.completion == "current_session_closed_waiting_provider"


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
        "completion": "calendar_unavailable",
        "market_timezone": "Asia/Shanghai",
        "cutoff_at": "",
    }

    research = ResearchFallbackResolver([]).resolve(naive)
    assert research.source == "research_lake"
    assert research.session == "2026-08-04"
    bars = UntrustedBarCatalogResolver([]).resolve(naive)
    assert bars.source == "unavailable"
    assert bars.session == ""
    assert bars.ready is False
    assert SessionExpectationResolver._research_sessions(
        naive.date(), naive.date(),
    ) == []


def test_default_close_data_end_uses_verified_session_or_previous_market_date(monkeypatch):
    from quantmaster import trading_sessions

    monkeypatch.setattr(
        trading_sessions,
        "resolve_session_target",
        lambda: SessionExpectation("2026-08-13", "fixture", True, "verified"),
    )
    assert trading_sessions.default_close_data_end() == "2026-08-13"
    assert trading_sessions.default_close_data_end("2026-07-31") == "2026-07-31"

    monkeypatch.setattr(
        trading_sessions,
        "resolve_session_target",
        lambda: SessionExpectation(reason="calendar offline"),
    )
    monkeypatch.setattr(trading_sessions, "market_date", lambda: date(2026, 8, 15))
    assert trading_sessions.default_close_data_end() == "2026-08-14"


def test_unverified_bar_majority_cannot_invent_a_holiday_session():
    resolver = UntrustedBarCatalogResolver([])

    result = resolver.resolve(
        datetime(2026, 10, 1, 20, tzinfo=ZoneInfo("Asia/Shanghai")),
    )

    assert resolver._bar_sessions(result.session or datetime(2026, 10, 1).date()) == [
        "2026-08-04",
    ]
    assert result.ready is False
    assert result.session == ""


def test_newer_strict_stockdb_marker_advances_stale_research_lake(
    isolated_config, monkeypatch,
):
    isolated_config.data.free_stockdb_root = str(isolated_config.data_root / "stockdb-runtime")
    root = isolated_config.free_stockdb_root
    root.mkdir(parents=True, exist_ok=True)
    (root / ".quantmaster-update.json").write_text(json.dumps({
        "schema_version": 2,
        "validated_session": "2026-08-13",
        "target_session": "2026-08-13",
        "updated_at": "2026-08-13T18:33:49+08:00",
        "validation": {
            "accepted": True,
            "complete": True,
            "target_session": "2026-08-13",
            "actual_session": "2026-08-13",
        },
    }), encoding="utf-8")
    resolver = ResearchFallbackResolver([])
    monkeypatch.setattr(
        resolver, "_research_sessions", lambda _start, _end: ["2026-08-12"],
    )

    result = resolver.resolve(
        datetime(2026, 8, 13, 20, tzinfo=ZoneInfo("Asia/Shanghai")),
    )

    assert result.session == "2026-08-13"
    assert result.source == "stockdb:validated"


def test_stockdb_marker_is_fail_closed_and_obeys_close_cutoff(
    isolated_config, monkeypatch,
):
    isolated_config.data.free_stockdb_root = str(isolated_config.data_root / "stockdb-runtime")
    root = isolated_config.free_stockdb_root
    root.mkdir(parents=True, exist_ok=True)
    marker = root / ".quantmaster-update.json"
    resolver = FixtureResolver([])
    monkeypatch.setattr(resolver, "_research_sessions", lambda _start, _end: [])
    base = {
        "schema_version": 2,
        "validated_session": "2026-08-13",
        "target_session": "2026-08-13",
        "updated_at": "2026-08-13T18:33:49+08:00",
        "validation": {
            "accepted": True,
            "complete": True,
            "target_session": "2026-08-13",
            "actual_session": "2026-08-13",
        },
    }

    marker.write_text(json.dumps(base), encoding="utf-8")
    before_cutoff = resolver.resolve(
        datetime(2026, 8, 13, 18, tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    assert before_cutoff.ready is False

    for invalid in (
        {**base, "schema_version": 1},
        {**base, "validation": {**base["validation"], "accepted": False}},
        {**base, "validation": {**base["validation"], "actual_session": "2026-08-12"}},
    ):
        marker.write_text(json.dumps(invalid), encoding="utf-8")
        assert resolver.resolve(
            datetime(2026, 8, 13, 20, tzinfo=ZoneInfo("Asia/Shanghai")),
        ).ready is False
    marker.write_text("{broken", encoding="utf-8")
    assert resolver.resolve(
        datetime(2026, 8, 13, 20, tzinfo=ZoneInfo("Asia/Shanghai")),
    ).ready is False


def test_explicit_target_rejects_future_open_session_and_non_session():
    resolver = FixtureResolver(["2026-08-13", "2026-08-14"])
    timezone = ZoneInfo("Asia/Shanghai")

    with pytest.raises(ValueError, match="尚未收盘"):
        resolver.resolve_explicit(
            datetime(2026, 8, 14).date(),
            datetime(2026, 8, 14, 10, tzinfo=timezone),
        )
    with pytest.raises(ValueError, match="不是已验证交易日"):
        resolver.resolve_explicit(
            datetime(2026, 8, 15).date(),
            datetime(2026, 8, 17, 20, tzinfo=timezone),
        )


def test_explicit_verified_session_without_local_completion_fails_closed():
    resolver = FixtureResolver(["2026-08-13"])

    result = resolver.resolve_explicit(
        datetime(2026, 8, 13).date(),
        datetime(2026, 8, 14, 20, tzinfo=ZoneInfo("Asia/Shanghai")),
    )

    assert result.ready is False
    assert result.source == "tushare:SSE"
    assert result.completion == "current_session_closed_waiting_provider"
