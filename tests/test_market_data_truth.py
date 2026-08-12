from __future__ import annotations

import json

import pandas as pd
import pytest

from quantmaster.data import registry
from quantmaster.data.base import (
    BarDataEnvelope,
    DataSource,
    Market,
    MarketDataUnavailable,
)
from quantmaster.data.free_stockdb_ingest import StockDBIngestService
from quantmaster.data.free_stockdb_source import FreeStockDBSource, _compact_time
from quantmaster.data.storage import BarStore


def _bars(index: pd.DatetimeIndex, value: float = 10.0) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "open": value,
            "high": value,
            "low": value,
            "close": value,
            "volume": 1_000_000.0,
        },
        index=index,
    )


def test_stockdb_frame_without_batch_manifest_is_preview_only(monkeypatch):
    dates = pd.bdate_range("2026-07-01", "2026-08-07")
    local = _bars(dates)
    local["amount"] = 10_000_000.0
    local.attrs.update(
        unit_status="unverified_vendor_contract",
        adjustment_status="unverified_vendor_contract",
    )
    monkeypatch.setattr(
        registry,
        "_local_sessions",
        lambda _start, _end: (dates, "tushare:SSE"),
    )
    quality = registry._assess_daily_frame(
        local,
        "2026-07-01",
        "2026-08-07",
        symbol="600000.SH",
        source="free-stockdb",
    )
    assert quality.status == "degraded"
    assert quality.analysis_eligible is True
    assert quality.formal_eligible is False
    assert any("单位" in issue for issue in quality.issues)
    assert any("复权" in issue for issue in quality.issues)


def test_stockdb_frame_assessment_never_calls_tushare(monkeypatch):
    dates = pd.bdate_range("2026-07-01", "2026-08-07")
    local = _bars(dates)
    local.attrs.update(
        unit_status="unverified_vendor_contract",
        adjustment_status="unverified_vendor_contract",
    )
    monkeypatch.setattr(
        "quantmaster.data.tushare_source.TushareSource.daily",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("不应补证")),
    )
    monkeypatch.setattr(
        registry,
        "_local_sessions",
        lambda _start, _end: (dates, "tushare:SSE"),
    )

    quality = registry._assess_daily_frame(
        local,
        "2026-07-01",
        "2026-08-07",
        symbol="600000.SH",
        source="free-stockdb",
    )

    assert quality.status == "degraded"
    assert quality.analysis_eligible is True
    assert quality.formal_eligible is False
    assert quality.to_dict()["analysis_eligible"] is True
    assert any("复权" in issue for issue in quality.issues)


def test_stockdb_quality_has_no_per_symbol_cross_source_contract(monkeypatch):
    dates = pd.bdate_range("2026-03-23", periods=100)
    local = _bars(dates)
    local["amount"] = 10_000_000.0
    local.attrs.update(
        unit_status="unverified_vendor_contract",
        adjustment_status="unverified_vendor_contract",
    )
    monkeypatch.setattr(
        "quantmaster.data.tushare_source.TushareSource.daily",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("不应补证")),
    )
    monkeypatch.setattr(
        registry,
        "_local_sessions",
        lambda _start, _end: (dates, "tushare:SSE"),
    )
    quality = registry._assess_daily_frame(
        local, str(dates[0].date()), str(dates[-1].date()),
        symbol="600000.SH", source="free-stockdb",
    )
    assert quality.status == "degraded"
    assert quality.formal_eligible is False


def test_stockdb_missing_amount_stays_explicitly_degraded(monkeypatch):
    dates = pd.bdate_range("2026-03-23", periods=20)
    local = _bars(dates)
    local.attrs.update(
        unit_status="unverified_vendor_contract",
        adjustment_status="unverified_vendor_contract",
    )
    monkeypatch.setattr(
        "quantmaster.data.tushare_source.TushareSource.daily",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("不应补证")),
    )
    monkeypatch.setattr(
        registry,
        "_local_sessions",
        lambda _start, _end: (dates, "tushare:SSE"),
    )
    quality = registry._assess_daily_frame(
        local, str(dates[0].date()), str(dates[-1].date()),
        symbol="600000.SH", source="free-stockdb",
    )
    assert quality.status == "degraded"
    assert quality.formal_eligible is False


def test_usable_stockdb_data_does_not_fan_out_to_remote_provider(
    tmp_path, isolated_config, monkeypatch,
):
    dates = pd.bdate_range("2026-07-01", "2026-08-07")
    local = _bars(dates, 10.0)
    local.attrs.update(
        unit_status="unverified_vendor_contract",
        adjustment_status="unverified_vendor_contract",
    )
    remote = _bars(dates, 20.0)
    calls: list[str] = []

    class LocalStockDB(DataSource):
        name = "free-stockdb-test"
        markets = (Market.CN,)

        def daily(self, _symbol, _start, _end):
            calls.append(self.name)
            return local

    class VerifiedFallback(DataSource):
        name = "tushare-fallback"
        markets = (Market.CN,)

        def daily(self, _symbol, _start, _end):
            calls.append(self.name)
            return remote

    monkeypatch.setattr(
        registry,
        "_request_factories",
        lambda **_kwargs: {Market.CN: [LocalStockDB, VerifiedFallback]},
    )
    monkeypatch.setattr(
        registry,
        "_local_sessions",
        lambda _start, _end: (dates, "tushare:SSE"),
    )
    monkeypatch.setattr(
        registry,
        "_unit_contract",
        lambda _symbol: (
            (
                ("open", "CNY/share"), ("high", "CNY/share"),
                ("low", "CNY/share"), ("close", "CNY/share"),
                ("volume", "share"), ("amount", "CNY"),
            ),
            "",
        ),
    )
    store = BarStore(root=tmp_path / "bars")

    result = registry._full_refresh(
        "600000.SH",
        "2026-07-01",
        "2026-08-07",
        None,
        store,
        "normal",
    )

    assert calls == ["free-stockdb-test"]
    assert result["close"].eq(10.0).all()
    assert store.metadata("600000.SH")["last_source"] == "free-stockdb-test"
    assert json.loads(store.metadata("600000.SH")["quality_json"])["status"] == "degraded"


def test_usable_stockdb_increment_does_not_fan_out_to_remote_provider(
    tmp_path, isolated_config, monkeypatch,
):
    dates = pd.bdate_range("2026-07-20", "2026-08-07")
    local = _bars(dates)
    local.attrs.update(
        unit_status="unverified_vendor_contract",
        adjustment_status="unverified_vendor_contract",
    )
    calls: list[str] = []

    class LocalStockDB(DataSource):
        name = "free-stockdb-test"
        markets = (Market.CN,)

        def daily(self, _symbol, _start, _end):
            calls.append(self.name)
            return local

    class Remote(DataSource):
        name = "tushare-fallback"
        markets = (Market.CN,)

        def daily(self, _symbol, _start, _end):
            calls.append(self.name)
            raise AssertionError("本地增量可用时不应联网")

    monkeypatch.setattr(
        registry,
        "_request_factories",
        lambda **_kwargs: {Market.CN: [LocalStockDB, Remote]},
    )
    monkeypatch.setattr(
        registry, "_local_sessions", lambda _start, _end: (dates, "local-calendar"),
    )
    store = BarStore(root=tmp_path / "bars")

    saved, errors, changed = registry._fetch_segment(
        "600000.SH", "2026-07-20", "2026-08-07", "right", None, store, "normal",
    )

    assert saved is not None and not saved.empty
    assert changed is True
    assert errors == []
    assert calls == ["free-stockdb-test"]


def test_stockdb_native_batch_path_never_supplements_contract_evidence(
    isolated_config, monkeypatch,
):
    isolated_config.data.primary_provider = "free-stockdb"
    dates = pd.bdate_range("2026-07-01", "2026-08-07")
    local = _bars(dates)
    local["amount"] = 10_000_000.0
    local.attrs.update(
        unit_status="unverified_vendor_contract",
        adjustment_status="unverified_vendor_contract",
    )

    monkeypatch.setattr(FreeStockDBSource, "native_batch_available", lambda _self: True)
    monkeypatch.setattr(
        FreeStockDBSource,
        "daily_many",
        lambda _self, symbols, _start, _end: {
            symbol: local.copy() for symbol in symbols
        },
    )

    monkeypatch.setattr(
        "quantmaster.data.tushare_source.TushareSource.daily",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("不应逐股票补证")),
    )
    monkeypatch.setattr(
        registry,
        "_local_sessions",
        lambda _start, _end: (dates, "tushare:SSE"),
    )

    result = registry.refresh_bar_panel(
        ["600000.SH"], "2026-07-01", "2026-08-07",
    )

    assert result.quality.status == "degraded", result.quality.issues
    assert result.quality.formal_eligible is False
    pd.testing.assert_series_equal(
        result.data["close"]["600000.SH"],
        local["close"],
        check_names=False,
        check_freq=False,
    )


def test_dense_short_tail_cannot_claim_a_long_requested_range(tmp_path, monkeypatch):
    store = BarStore(tmp_path / "bars")

    class ShortTail(DataSource):
        name = "short-tail"
        markets = (Market.CN,)

        def daily(self, symbol, start, end):
            return _bars(pd.bdate_range("2024-03-18", "2024-03-29"))

    class Complete(DataSource):
        name = "complete"
        markets = (Market.CN,)

        def daily(self, symbol, start, end):
            return _bars(pd.bdate_range(start, end))

    monkeypatch.setattr(
        registry, "_factories", lambda: {Market.CN: [ShortTail, Complete]},
    )

    result = registry.refresh_history(
        "600000.SH", "2024-01-02", "2024-03-29", store=store,
    )

    assert result.data.index.min() == pd.Timestamp("2024-01-02")
    assert store.metadata("600000.SH")["last_source"] == "complete"
    chain = result.provenance
    assert chain[-1]["observed_start"] == "2024-01-02"


def test_duplicate_daily_keys_are_rejected_before_cache_write(tmp_path, monkeypatch):
    store = BarStore(tmp_path / "bars")

    class Duplicate(DataSource):
        name = "duplicate"
        markets = (Market.CN,)

        def daily(self, symbol, start, end):
            dates = pd.bdate_range(start, end)
            return _bars(dates.insert(1, dates[0]))

    class Complete(DataSource):
        name = "complete"
        markets = (Market.CN,)

        def daily(self, symbol, start, end):
            return _bars(pd.bdate_range(start, end))

    monkeypatch.setattr(
        registry, "_factories", lambda: {Market.CN: [Duplicate, Complete]},
    )

    result = registry.refresh_history(
        "600000.SH", "2024-01-02", "2024-03-29", store=store,
    )

    assert not result.data.index.duplicated().any()
    assert store.metadata("600000.SH")["last_source"] == "complete"


def test_failed_refresh_returns_computable_cache_with_persisted_degraded_quality(
    tmp_path, monkeypatch,
):
    store = BarStore(tmp_path / "bars")
    frame = _bars(pd.bdate_range("2024-01-02", "2024-03-29"))
    store.put("600000.SH", frame)
    store.mark_checked(
        "600000.SH",
        "2024-01-02",
        "2024-03-29",
        source="free-stockdb",
        quality={
            "status": "verified",
            "observed_start": "2024-01-02",
            "observed_end": "2024-03-29",
            "issues": [],
        },
    )

    class Broken(DataSource):
        name = "broken"
        markets = (Market.CN,)

        def daily(self, symbol, start, end):
            raise RuntimeError("offline")

    monkeypatch.setattr(registry, "_factories", lambda: {Market.CN: [Broken]})

    result = registry.refresh_history(
        "600000.SH",
        "2024-01-02",
        "2024-03-29",
        store=store,
        mode="full",
    )

    assert not result.data.empty
    assert result.quality.status == "degraded"
    assert result.quality.stale is True
    assert store.quality("600000.SH")["status"] == "degraded"


def test_panel_envelope_discloses_missing_requested_symbols(tmp_path, monkeypatch):
    store = BarStore(tmp_path / "bars")
    frame = _bars(pd.to_datetime(["2024-03-29"]))
    store.put("600000.SH", frame)
    store.mark_checked(
        "600000.SH", "2024-03-29", "2024-03-29", source="fixture",
        quality={"status": "degraded", "issues": ["缺少权威交易日历"]},
    )
    monkeypatch.setattr(registry, "_default_bar_store", lambda: store)
    monkeypatch.setattr(
        registry,
        "_load_bar_panel_frame",
        lambda *_args, **_kwargs: pd.DataFrame(
            {"600000.SH": [10.0]}, index=pd.to_datetime(["2024-03-29"]),
        ),
    )

    result = registry.refresh_bar_panel(
        ["600000.SH", "000001.SZ"],
        "2024-03-29",
        "2024-03-29",
        field="close",
    )

    assert isinstance(result, BarDataEnvelope)
    assert result.quality.status == "unavailable"
    assert result.quality.partial is True
    assert result.quality.missing_symbols == ("000001.SZ",)


def test_structurally_invalid_cache_remains_unavailable(tmp_path):
    store = BarStore(tmp_path / "bars")
    dates = pd.to_datetime(["2024-03-28", "2024-03-28", "2024-03-29"])
    frame = _bars(dates)
    store.put("600000.SH", frame)

    result = registry._bar_envelope(
        frame,
        symbol="600000.SH",
        start="2024-03-28",
        end="2024-03-29",
        store=store,
        frequency="1d",
    )

    assert result.quality.status == "unavailable"
    assert result.quality.duplicate_rows == 2


def test_marking_stale_preserves_source_and_persists_event(tmp_path):
    store = BarStore(tmp_path / "bars")
    store.put("600000.SH", _bars(pd.to_datetime(["2024-03-29"])))
    store.mark_checked(
        "600000.SH",
        "2024-03-29",
        "2024-03-29",
        source="free-stockdb",
        quality={"status": "verified", "observed_start": "2024-03-29"},
    )

    store.mark_status("600000.SH", "stale")

    metadata = store.metadata("600000.SH")
    assert metadata["last_source"] == "free-stockdb"
    result = json.loads(metadata["source_chain_json"])
    assert result
    assert result[-1]["status"] == "stale"
    assert result[-1]["source"] == "free-stockdb"


def test_units_are_instrument_driven_and_unknown_by_default(isolated_config):
    units, issue = registry._unit_contract("AAPL.US")

    assert dict(units)["close"] == "USD/share"
    assert issue == ""
    assert dict(registry.BarDataQuality("degraded", "", "").units)["close"] == "unknown"


def test_public_stockdb_http_is_not_a_trusted_market_source(isolated_config):
    isolated_config.data.free_stockdb_url = "http://8.138.149.215:7899"

    with pytest.raises(ValueError, match="本机回环"):
        FreeStockDBSource()

    assert "free-stockdb-online" not in {
        factory.name for factory in registry._factories()[Market.CN]
    }


def test_missing_adjustment_factor_is_explicitly_degraded():
    raw = pd.DataFrame(
        {
            "symbol": ["600000.SH", "000001.SZ"],
            "date": pd.to_datetime(["2024-03-29", "2024-03-29"]),
            "open": [10.0, 20.0],
            "high": [10.0, 20.0],
            "low": [10.0, 20.0],
            "close": [10.0, 20.0],
        }
    )
    factors = pd.DataFrame(
        {
            "symbol": ["600000.SH"],
            "date": pd.to_datetime(["2024-01-01"]),
            "adj_factor": [2.0],
        }
    )

    result = StockDBIngestService._research_prices(raw, factors)

    status = result.set_index("symbol")["adjustment_status"].to_dict()
    assert status == {"000001.SZ": "degraded", "600000.SH": "verified"}
    assert result.set_index("symbol").loc["000001.SZ", "price_adjustment"] == "raw_missing_factor"


def test_stockdb_daily_without_factor_lineage_is_degraded(monkeypatch):
    frame = _bars(pd.bdate_range("2024-03-01", "2024-03-29"))
    frame.attrs["unit_status"] = "verified"
    monkeypatch.setattr(
        registry,
        "_local_sessions",
        lambda start, end: (pd.bdate_range(start, end), "fixture-calendar"),
    )

    quality = registry._assess_daily_frame(
        frame,
        "2024-03-01",
        "2024-03-29",
        symbol="600000.SH",
        source="free-stockdb",
    )

    assert quality.status == "degraded"
    assert quality.adjustment == "qfq_requested_unverified"
    assert any("复权因子记录" in issue for issue in quality.issues)


def test_aware_intraday_query_is_converted_to_shanghai_wall_time():
    assert _compact_time("2024-03-29T01:30:00+00:00", intraday=True) == "20240329093000"


def test_unknown_primary_provider_fails_explicitly(isolated_config):
    isolated_config.data.primary_provider = "stockdb-typo"
    with pytest.raises(ValueError, match="未知主数据源"):
        registry._factories()


def test_mixed_source_extension_cannot_upgrade_old_degraded_range(tmp_path, monkeypatch):
    store = BarStore(tmp_path / "bars")
    units = tuple((field, "CNY/share" if field != "volume" else "share") for field in (
        "open", "high", "low", "close", "volume", "amount",
    ))
    monkeypatch.setattr(registry, "_unit_contract", lambda _symbol: (units, ""))
    monkeypatch.setattr(
        registry,
        "_local_sessions",
        lambda start, end: (pd.bdate_range(start, end), "fixture-calendar"),
    )
    old = _bars(pd.bdate_range("2024-01-02", "2024-01-10"))
    fresh = _bars(pd.bdate_range("2024-01-11", "2024-01-19"), 11.0)
    store.put(
        "600000.SH", old, replace=True,
        request_start="2024-01-02", request_end="2024-01-10",
        source="free-stockdb",
        quality={
            "status": "degraded", "issues": ["StockDB 因子链未验证"],
            "sources": ["free-stockdb"], "observed_start": "2024-01-02",
            "observed_end": "2024-01-10", "partial": False, "stale": False,
        },
        replace_coverage=True,
    )
    merged = pd.concat((old, fresh)).sort_index()
    store.put(
        "600000.SH", merged, replace=True,
        request_start="2024-01-11", request_end="2024-01-19",
        source="tushare",
        quality={
            "status": "verified", "issues": [], "sources": ["tushare"],
            "observed_start": "2024-01-11", "observed_end": "2024-01-19",
            "coverage_ratio": 1.0, "partial": False, "stale": False,
        },
    )

    old_result = registry._bar_envelope(
        old, symbol="600000.SH", start="2024-01-02", end="2024-01-10",
        store=store, frequency="1d",
    )
    new_result = registry._bar_envelope(
        fresh, symbol="600000.SH", start="2024-01-11", end="2024-01-19",
        store=store, frequency="1d",
    )

    assert old_result.quality.status == "degraded"
    assert "StockDB 因子链未验证" in old_result.quality.issues
    assert {item["source"] for item in old_result.provenance} == {"free-stockdb"}
    assert new_result.quality.status == "verified"
    assert {item["source"] for item in new_result.provenance} == {"tushare"}


def test_public_envelope_verified_tail_cannot_upgrade_unverified_history(
    tmp_path, monkeypatch,
):
    store = BarStore(tmp_path / "bars")
    dates = pd.bdate_range("2023-01-02", "2023-12-29")
    frame = _bars(dates)
    store.put("600000.SH", frame)
    with store._conn() as connection:
        connection.execute(
            "UPDATE bar_meta SET source_chain_json='[]',quality_json='{}',"
            "last_status='ready' WHERE symbol='600000.SH'",
        )
    store.mark_checked(
        "600000.SH",
        "2023-12-29",
        "2023-12-29",
        source="tushare",
        quality={
            "status": "verified", "stale": False, "partial": False,
            "issues": [], "sources": ["tushare"],
        },
    )
    units = tuple((field, "CNY/share" if field != "volume" else "share") for field in (
        "open", "high", "low", "close", "volume", "amount",
    ))
    monkeypatch.setattr(registry, "_unit_contract", lambda _symbol: (units, ""))
    monkeypatch.setattr(
        registry,
        "_local_sessions",
        lambda _start, _end: (dates, "fixture-calendar"),
    )

    result = registry._bar_envelope(
        frame,
        symbol="600000.SH",
        start="2023-01-02",
        end="2023-12-29",
        store=store,
        frequency="1d",
    )

    assert result.quality.status == "degraded"
    assert result.quality.partial is True
    assert "版本化来源链没有覆盖完整请求区间" in result.quality.issues
    assert result.provenance[-1]["status"] == "lineage_gap"


def test_spot_uses_oldest_row_timestamp_and_rejects_mixed_freshness(monkeypatch):
    now = pd.Timestamp("2026-08-09 10:00:00", tz="Asia/Shanghai")
    frame = pd.DataFrame({
        "code": ["600000", "000001"],
        "price": [10.0, 11.0],
        "change_pct": [1.0, -1.0],
        "updated_at": [now - pd.Timedelta(days=1), now],
        "source": ["fixture", "fixture"],
    })
    monkeypatch.setattr(
        registry, "_load_spot_frame",
        lambda *_args, **_kwargs: (frame, ({"source": "fixture"},), ()),
    )
    monkeypatch.setattr(registry, "market_now", lambda: now.to_pydatetime())
    monkeypatch.setattr(registry, "market_date", lambda: now.date())
    monkeypatch.setattr(
        registry, "_unit_contract",
        lambda _symbol: (registry.BarDataQuality("degraded", "", "").units, ""),
    )

    result = registry.refresh_spot(["600000.SH", "000001.SZ"])

    assert result.quality.status == "unavailable"
    assert result.quality.stale is True
    assert result.quality.observed_start.startswith("2026-08-08")
    assert any("不能用最新一行代表整批" in issue for issue in result.quality.issues)


def test_intraday_rows_concentrated_in_one_bucket_are_unavailable(monkeypatch):
    day = pd.Timestamp("2026-08-07")
    timestamps = pd.DatetimeIndex([
        day + pd.Timedelta(hours=9, minutes=30, seconds=second)
        for second in range(1, 49)
    ]).tz_localize("Asia/Shanghai")
    frame = _bars(timestamps)
    monkeypatch.setattr(
        registry, "_local_sessions",
        lambda _start, _end: (pd.DatetimeIndex([day]), "fixture-calendar"),
    )
    monkeypatch.setattr(
        registry, "_unit_contract",
        lambda _symbol: (registry.BarDataQuality("degraded", "", "").units, ""),
    )

    quality = registry._assess_intraday_frame(
        frame,
        "2026-08-07 09:30:00",
        "2026-08-07 15:00:00",
        symbol="600000.SH",
        frequency="5m",
        source="fixture",
    )

    assert quality.status == "unavailable"
    assert quality.coverage_ratio is not None and quality.coverage_ratio < 0.10
    assert any("交易时段桶覆盖率" in issue for issue in quality.issues)


def test_intraday_frequency_contract_rejects_unbounded_values(monkeypatch):
    day = pd.Timestamp("2026-08-07")
    frame = _bars(pd.DatetimeIndex([day + pd.Timedelta(hours=10)]))
    monkeypatch.setattr(
        registry,
        "_local_sessions",
        lambda _start, _end: (pd.DatetimeIndex([day]), "fixture-calendar"),
    )
    monkeypatch.setattr(
        registry,
        "_unit_contract",
        lambda _symbol: (registry.BarDataQuality("degraded", "", "").units, ""),
    )

    quality = registry._assess_intraday_frame(
        frame,
        "2026-08-07 09:30:00",
        "2026-08-07 15:00:00",
        symbol="600000.SH",
        frequency=f"{'9' * 10_000}m",
        source="fixture",
    )

    assert quality.status == "unavailable"
    assert any("无法确认分钟行情的时间间隔" in issue for issue in quality.issues)


def test_finite_but_impossible_daily_ohlcv_is_unavailable(monkeypatch):
    frame = pd.DataFrame({
        "open": [-1.0], "high": [1.0], "low": [99.0], "close": [50.0],
        "volume": [-100.0],
    }, index=pd.to_datetime(["2026-08-07"]))
    monkeypatch.setattr(
        registry, "_local_sessions",
        lambda _start, _end: (pd.to_datetime(["2026-08-07"]), "fixture-calendar"),
    )

    quality = registry._assess_daily_frame(
        frame, "2026-08-07", "2026-08-07", symbol="600000.SH", source="fixture",
    )

    assert quality.status == "unavailable"
    assert any("价格高低关系或成交量不合理" in issue for issue in quality.issues)


def test_all_sources_empty_raises_structured_unavailable(tmp_path, monkeypatch):
    store = BarStore(tmp_path / "bars")

    class Empty(DataSource):
        name = "empty"
        markets = (Market.CN,)

        def daily(self, symbol, start, end):
            return pd.DataFrame()

    monkeypatch.setattr(registry, "_factories", lambda: {Market.CN: [Empty]})

    with pytest.raises(MarketDataUnavailable) as caught:
        registry.refresh_history("600000.SH", "2026-08-03", "2026-08-07", store=store)

    assert caught.value.quality.status == "unavailable"
    assert caught.value.quality.missing_symbols == ("600000.SH",)
    assert caught.value.provenance
