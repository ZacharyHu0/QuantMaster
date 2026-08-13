from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

import quantmaster.data.registry as registry
from quantmaster.data.base import DataSource, Market
from quantmaster.data.cache_contracts import CacheResultKind
from quantmaster.data.cache_freshness import (
    BarRefreshBatchStore,
    assess_daily_freshness,
)
from quantmaster.data.storage import BarStore
from quantmaster.trading_sessions import SessionExpectation


def _bars(start: str, end: str) -> pd.DataFrame:
    index = pd.bdate_range(start, end)
    return pd.DataFrame({
        "open": 10.0,
        "high": 11.0,
        "low": 9.0,
        "close": 10.5,
        "volume": 100.0,
    }, index=index)


def test_display_stale_while_revalidate_uses_verified_session_and_age() -> None:
    current = datetime(2026, 8, 10, 20, tzinfo=ZoneInfo("Asia/Shanghai"))
    result = assess_daily_freshness(
        symbol="600000.SH",
        frame=_bars("2026-08-03", "2026-08-07"),
        requested_end="2026-08-10",
        checked_at=pd.Timestamp("2026-08-07 19:00", tz="Asia/Shanghai").timestamp(),
        purpose="display",
        now=current,
        expectation=SessionExpectation("2026-08-10", "tushare:SSE", True, "official"),
        display_ttl_seconds=86400,
    )

    assert result.state == "stale"
    assert result.stale_while_revalidate is True
    assert result.expected_session == "2026-08-10"
    assert "2026-08-07" in result.refresh_reason
    assert result.age_seconds == 262800


def test_historical_freshness_ignores_wall_ttl_but_rejects_future_rows() -> None:
    old = assess_daily_freshness(
        symbol="600000.SH",
        frame=_bars("2024-01-02", "2024-01-10"),
        requested_end="2024-01-10",
        checked_at=1,
        purpose="formal_research",
        now=datetime(2026, 8, 10, tzinfo=ZoneInfo("Asia/Shanghai")),
        display_ttl_seconds=1,
    )
    future = assess_daily_freshness(
        symbol="600000.SH",
        frame=_bars("2024-01-02", "2024-01-12"),
        requested_end="2024-01-10",
        checked_at=1,
        purpose="historical",
        now=datetime(2026, 8, 10, tzinfo=ZoneInfo("Asia/Shanghai")),
    )

    assert old.state == "fresh"
    assert old.formal_eligible is True
    assert future.state == "invalid_future"
    assert future.future_rows == 2
    assert future.formal_eligible is False


def test_daily_quality_rejects_future_rows(monkeypatch) -> None:
    monkeypatch.setattr(
        registry,
        "_local_sessions",
        lambda start, end: (pd.bdate_range(start, end), "fixture-calendar"),
    )

    quality = registry._assess_daily_frame(
        _bars("2024-01-02", "2024-01-12"),
        "2024-01-02",
        "2024-01-10",
        symbol="600000.SH",
        source="fixture",
    )

    assert quality.status == "unavailable"
    assert quality.future_rows == 2
    assert quality.formal_eligible is False


def test_partial_batch_persists_reason_and_resumes_only_missing(tmp_path) -> None:
    store = BarRefreshBatchStore(tmp_path / "bars")
    symbols = ["600000.SH", "000001.SZ"]
    batch_id, requested, resumed = store.begin_or_resume(
        symbols, "2026-08-03", "2026-08-07", frequency="1d", provider="fixture",
    )
    assert requested == tuple(symbols)
    assert resumed is False

    store.record_success(batch_id, "600000.SH")
    store.record_failure(batch_id, "000001.SZ", "TLS handshake failed", "tls_error")
    summary = store.summary(batch_id)

    assert summary["status"] == CacheResultKind.PARTIAL.value
    assert summary["succeeded_count"] == 1
    assert summary["pending_count"] == 1
    assert summary["pending"][0]["reason"] == "TLS handshake failed"
    assert summary["pending"][0]["diagnostic_code"] == "tls_error"
    assert summary["requested"] == symbols
    assert summary["completed"] == ["600000.SH"]
    assert summary["missing"][0]["reason"] == "source_unavailable"
    assert summary["complete"] is False
    assert summary["reason_counts"] == {"source_unavailable": 1}

    resumed_id, retry, resumed = store.begin_or_resume(
        symbols, "2026-08-03", "2026-08-07", frequency="1d", provider="fixture",
    )
    assert resumed_id == batch_id
    assert retry == ("000001.SZ",)
    assert resumed is True


def test_panel_partial_retry_skips_already_persisted_success(tmp_path, monkeypatch) -> None:
    store = BarStore(tmp_path / "bars")
    calls: list[str] = []

    class SometimesBroken(DataSource):
        name = "fixture"
        markets = (Market.CN,)

        def daily(self, symbol, start, end):
            calls.append(symbol)
            if symbol == "000001.SZ" and calls.count(symbol) == 1:
                raise ConnectionError("DNS temporary failure")
            return _bars(start, end)

    monkeypatch.setattr(registry, "_default_bar_store", lambda: store)
    monkeypatch.setattr(
        registry,
        "_request_factories",
        lambda **_kwargs: {Market.CN: [SometimesBroken]},
    )
    monkeypatch.setattr(
        registry,
        "_local_sessions",
        lambda start, end: (pd.bdate_range(start, end), "fixture-calendar"),
    )

    first = registry.refresh_bar_panel(
        ["600000.SH", "000001.SZ"],
        "2026-08-03",
        "2026-08-07",
        field="close",
        source_name="fixture",
    )
    assert first.quality.partial is True
    assert first.quality.missing_symbols == ("000001.SZ",)
    assert store.get("600000.SH") is not None

    second = registry.refresh_bar_panel(
        ["600000.SH", "000001.SZ"],
        "2026-08-03",
        "2026-08-07",
        field="close",
        source_name="fixture",
    )

    assert calls.count("600000.SH") == 1
    assert calls.count("000001.SZ") == 2
    assert second.quality.missing_symbols == ()
    assert list(second.data.columns) == ["600000.SH", "000001.SZ"]
    batch = next(item["refresh_batch"] for item in second.provenance if "refresh_batch" in item)
    assert batch["status"] == CacheResultKind.SUCCESS.value
    assert batch["pending_count"] == 0
