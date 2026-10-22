from __future__ import annotations

import pandas as pd
import pytest

from quantmaster.data.reference_market import (
    INDEX_TIMEZONES,
    ReferenceFetch,
    ReferenceMarketUnavailable,
    _normalize_index,
    fetch_reference,
    is_reference_symbol,
    refresh_reference_panel,
)
from quantmaster.data.storage import BarStore


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.bdate_range("2026-07-20", periods=5),
            "open": [100, 101, 102, 103, 104],
            "high": [101, 102, 103, 104, 105],
            "low": [99, 100, 101, 102, 103],
            "close": [100.5, 101.5, 102.5, 103.5, 104.5],
        }
    )


def test_reference_market_prefers_validated_sina_route(monkeypatch):
    monkeypatch.setattr(
        "quantmaster.data.reference_market._akshare_route",
        lambda symbol, start: ("sina:us-index", _frame),
    )
    monkeypatch.setattr(
        "quantmaster.data.yfinance_source.YFinanceSource.daily",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("Yahoo 不应被调用")),
    )

    result = fetch_reference("SPX.INDEX", "2026-07-20", "2026-07-31")

    assert result.source == "sina:us-index"
    assert list(result.frame.columns) == ["open", "high", "low", "close"]
    assert result.attempts == ()


def test_reference_market_falls_back_per_symbol_and_records_attempt(monkeypatch):
    def failed_sina():
        raise ConnectionError("Sina offline")

    yahoo = _frame().set_index("date")
    monkeypatch.setattr(
        "quantmaster.data.reference_market._akshare_route",
        lambda symbol, start: ("sina:test", failed_sina),
    )
    monkeypatch.setattr(
        "quantmaster.data.yfinance_source.YFinanceSource.daily",
        lambda *args, **kwargs: yahoo,
    )

    result = fetch_reference("SPX.INDEX", "2026-07-20", "2026-07-31")

    assert result.source == "yfinance"
    assert result.attempts[0]["source"] == "sina:test"
    assert result.frame.index.max() == pd.Timestamp("2026-07-24")


def test_reference_market_normalizes_tushare_fx_trade_date(monkeypatch):
    raw = pd.DataFrame([{
        "trade_date": "20260730",
        "bid_open": 6.74,
        "bid_high": 6.76,
        "bid_low": 6.73,
        "bid_close": 6.75,
    }])
    monkeypatch.setattr(
        "quantmaster.data.reference_market._akshare_route", lambda symbol, start: None,
    )
    monkeypatch.setattr(
        "quantmaster.data.reference_market._tushare_cny", lambda start, end: raw,
    )

    result = fetch_reference("USD-CNY.FX", "2026-07-01", "2026-07-31")

    assert result.source == "tushare:fx"
    assert result.frame.index.tolist() == [pd.Timestamp("2026-07-30")]
    assert result.frame.iloc[0]["close"] == 6.75


def test_dollar_index_uses_akshare_before_yahoo(monkeypatch):
    import sys
    from types import SimpleNamespace

    calls = []
    raw = pd.DataFrame({
        "日期": ["2024-01-02", "2024-01-03"],
        "开盘": [102.0, 102.5], "最高": [103.0, 103.5],
        "最低": [101.0, 101.5], "收盘": [102.5, 103.0],
    })

    def global_index(**kwargs):
        calls.append(kwargs)
        return raw

    monkeypatch.setitem(sys.modules, "akshare", SimpleNamespace(
        index_global_hist_em=global_index,
    ))
    result = fetch_reference("DXY.INDEX", "2024-01-01", "2024-01-05")
    assert result.source == "akshare:global-index"
    assert calls == [{"symbol": "美元指数"}]
    assert result.frame["close"].iloc[-1] == 103.0


def test_refresh_reference_symbol_uses_dedicated_route_and_writes_cache(tmp_path, monkeypatch):
    from quantmaster.data import registry

    raw = _frame().assign(volume=0.0).set_index("date")
    calls: list[tuple[str, str, str]] = []

    def reference(symbol: str, start: str, end: str):
        calls.append((symbol, start, end))
        return type("Fetch", (), {"frame": raw, "source": "sina:foreign-futures"})()

    monkeypatch.setattr(registry, "fetch_reference", reference, raising=False)
    monkeypatch.setattr(
        "quantmaster.data.reference_market.fetch_reference", reference,
    )
    monkeypatch.setattr(
        "quantmaster.data.reference_market.is_reference_symbol", lambda symbol: symbol == "GC.CONTINUOUS",
    )
    monkeypatch.setattr(
        registry, "_request_factories", lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("通用 provider 不应被调用"),
        ),
    )

    store = BarStore(root=tmp_path / "bars")
    result = registry.refresh_history(
        "GC.CONTINUOUS", "2026-07-20", "2026-07-24", store=store,
    )

    assert calls == [("GC.CONTINUOUS", "2026-07-20", "2026-07-24")]
    assert result.data.index.equals(raw.index)
    assert store.metadata("GC.CONTINUOUS")["last_source"] == "sina:foreign-futures"


def test_refresh_reference_panel_reuses_fresh_complete_cache(tmp_path, monkeypatch):
    symbol = "SPX.INDEX"
    frame = _normalize_index(_frame(), symbol, "sina:us-index", "2026-07-20", "2026-07-24")
    store = BarStore(root=tmp_path / "bars")
    store.put(
        symbol,
        frame,
        request_start="2026-07-20",
        request_end="2026-07-24",
        source="sina:us-index",
    )
    monkeypatch.setattr(
        "quantmaster.data.reference_market.fetch_reference",
        lambda *_args: (_ for _ in ()).throw(AssertionError("fresh cache should win")),
    )

    frames, failures = refresh_reference_panel(
        [symbol], "2026-07-20", "2026-07-24", "auto", store,
    )

    assert failures == {}
    assert frames[symbol].equals(frame)


def test_refresh_reference_panel_isolates_provider_failure(tmp_path, monkeypatch):
    ready, failed = "SPX.INDEX", "DXY.INDEX"
    frame = _frame().assign(volume=0.0).set_index("date")
    store = BarStore(root=tmp_path / "bars")
    store.put(
        failed,
        frame,
        request_start="2026-07-20",
        request_end="2026-07-24",
        source="yfinance",
    )

    def fetch(symbol: str, _start: str, _end: str) -> ReferenceFetch:
        if symbol == failed:
            raise ReferenceMarketUnavailable(symbol, [{
                "source": "yfinance", "code": "offline", "detail": "offline",
            }])
        valid = _normalize_index(frame, symbol, "sina:us-index", _start, _end)
        return ReferenceFetch(frame=valid, source="sina:us-index", attempts=())

    monkeypatch.setattr("quantmaster.data.reference_market.fetch_reference", fetch)
    frames, failures = refresh_reference_panel(
        [ready, failed], "2026-07-20", "2026-07-24", "incremental", store,
    )

    assert ready not in failures, failures
    assert set(frames) == {ready, failed}
    assert failures[failed]["error_code"] == "MarketDataUnavailable"
    assert store.metadata(failed)["last_status"] == "stale"
    assert store.metadata(ready)["last_source"] == "sina:us-index"


@pytest.mark.parametrize("symbol", INDEX_TIMEZONES)
def test_overseas_refresh_cache_and_offline_read_never_use_domestic_routes(symbol, tmp_path, monkeypatch):
    from quantmaster.data import registry
    from quantmaster.data.maintenance import DataRefreshManager

    calls = []
    source = "sina:us-index"

    def fetch():
        calls.append(symbol)
        return _frame()

    monkeypatch.setattr("quantmaster.data.reference_market._akshare_route",
                        lambda *_: (source, fetch))
    monkeypatch.setattr(registry, "_request_factories", lambda **_: pytest.fail("domestic index API"))
    monkeypatch.setattr(registry, "_local_sessions", lambda *_: pytest.fail("CN calendar"))
    store = BarStore(tmp_path / "bars")
    assert is_reference_symbol(symbol)
    first = registry.refresh_history(symbol, "2026-07-20", "2026-07-24", store=store)
    assert first.quality.status == "degraded"
    assert first.quality.timezone == INDEX_TIMEZONES[symbol]
    assert first.quality.expected_session == ""
    assert not first.quality.stale
    assert not first.quality.formal_eligible
    assert first.data["close"].tolist() == _frame()["close"].tolist()
    assert "volume" not in first.data
    prepared = DataRefreshManager._refresh_one(store, symbol, "2026-07-20", "2026-07-24")
    assert prepared["code"] == "reference_only"
    before = store.metadata(symbol)
    monkeypatch.setattr("quantmaster.data.reference_market.fetch_reference",
                        lambda *_: pytest.fail("offline read contacted a source"))
    local = registry.read_history(symbol, "2026-07-20", "2026-07-24", store=store)
    assert local.require_data().equals(first.data)
    assert store.metadata(symbol) == before
    assert calls == [symbol]


@pytest.mark.parametrize("defect", ["identity", "duplicate", "nan", "negative", "ohlc", "sparse"])
def test_invalid_overseas_response_preserves_cached_bytes(defect, tmp_path, monkeypatch):
    from quantmaster.data import registry

    symbol = "SPX.INDEX"
    raw = _frame()
    monkeypatch.setattr("quantmaster.data.reference_market._akshare_route",
                        lambda *_: ("sina:us-index", lambda: raw))
    monkeypatch.setattr("quantmaster.data.yfinance_source.YFinanceSource.daily",
                        lambda *_: (_ for _ in ()).throw(ConnectionError("offline")))
    monkeypatch.setattr(registry, "_request_factories", lambda **_: pytest.fail("domestic fallback"))
    store = BarStore(tmp_path / "bars")
    good = registry.refresh_history(symbol, "2026-07-20", "2026-07-24", store=store)
    digest = store.metadata(symbol)["content_sha256"]
    raw = _frame()
    if defect == "identity":
        raw.attrs["instrument"] = "DJI.INDEX"
    elif defect == "duplicate":
        raw.loc[1, "date"] = raw.loc[0, "date"]
    elif defect == "nan":
        raw.loc[1, "close"] = float("nan")
    elif defect == "negative":
        raw.loc[1, "close"] = -1
    elif defect == "ohlc":
        raw.loc[1, "high"] = 1
    else:
        raw = raw.iloc[:1]
    failed = registry.refresh_history(symbol, "2026-07-20", "2026-08-20", store=store, mode="full")
    assert failed.quality.stale
    assert failed.data.equals(good.data)
    assert store.metadata(symbol)["content_sha256"] == digest


def test_overseas_rebuild_replaces_legacy_contract_and_failed_rebuild_keeps_it(tmp_path, monkeypatch):
    from quantmaster.data import registry

    symbol = "SPX.INDEX"
    store = BarStore(tmp_path / "bars")
    legacy = _frame().assign(volume=0).set_index("date")
    store.put(symbol, legacy, source="tushare", request_start="2026-07-20", request_end="2026-07-24")
    previous = registry.read_history(symbol, "2026-07-20", "2026-07-24", store=store)
    assert previous.quality.status == "unavailable"
    raw = _frame()
    monkeypatch.setattr("quantmaster.data.reference_market._akshare_route",
                        lambda *_: ("sina:us-index", lambda: raw))
    monkeypatch.setattr(registry, "_request_factories", lambda **_: pytest.fail("domestic fallback"))
    fixed = registry.refresh_history(symbol, "2026-07-20", "2026-07-24", store=store)
    assert fixed.quality.preview_eligible and not fixed.quality.stale
    assert "INDEX_REFERENCE_CONTRACT_MISSING" not in " ".join(fixed.quality.issues)
    assert fixed.data.attrs["unit_status"] == "reference_index_points_v1"
    digest = store.metadata(symbol)["content_sha256"]
    raw = raw.drop(index=2)
    failed = registry.refresh_history(symbol, "2026-07-20", "2026-07-24", store=store, mode="full")
    assert failed.quality.stale
    assert store.metadata(symbol)["content_sha256"] == digest


def test_sina_dollar_decoder_uses_official_field_order_and_no_fake_volume(monkeypatch):
    from types import SimpleNamespace

    from quantmaster.data.reference_market import _sina_dollar_index

    requests = []

    def get(url, **kwargs):
        requests.append((url, kwargs))
        return SimpleNamespace(text='/* banner */quantmaster("2026-07-24,100,99,102,101,");',
                               raise_for_status=lambda: None)

    monkeypatch.setattr("requests.get", get)
    raw = _sina_dollar_index()
    assert raw.iloc[0].to_dict() == {
        "date": "2026-07-24", "open": "100", "low": "99", "high": "102", "close": "101",
    }
    assert "volume" not in raw
    assert requests[0][1] == {"params": {"symbol": "DINIW"}, "timeout": 15}


def test_hk_quoted_precision_tolerance_does_not_accept_bad_prices(monkeypatch):
    from quantmaster.data import registry

    raw = _frame()
    raw.loc[0, ["open", "high", "low", "close"]] = [100, 101, 99, 101.0003]
    frame = _normalize_index(raw, "HSI.INDEX", "sina:hk-index", "2026-07-20", "2026-07-24")
    quality = registry._assess_daily_frame(frame, "2026-07-20", "2026-07-24", symbol="HSI.INDEX")
    assert quality.preview_eligible
    frame.loc[frame.index[0], "close"] = 102
    invalid = registry._assess_daily_frame(frame, "2026-07-20", "2026-07-24", symbol="HSI.INDEX")
    assert not invalid.preview_eligible


@pytest.mark.parametrize(("symbol", "method", "alias"), [
    ("SPX.INDEX", "index_us_stock_sina", ".INX"),
    ("IXIC.INDEX", "index_us_stock_sina", ".IXIC"),
    ("DJI.INDEX", "index_us_stock_sina", ".DJI"),
    ("HSI.INDEX", "stock_hk_index_daily_sina", "HSI"),
    ("HSTECH.INDEX", "stock_hk_index_daily_sina", "HSTECH"),
    ("N225.INDEX", "index_global_hist_sina", "日经225指数"),
    ("KS11.INDEX", "index_global_hist_sina", "首尔综合指数"),
    ("DXY.INDEX", "index_global_hist_em", "美元指数"),
])
def test_all_overseas_adapter_aliases(symbol, method, alias, monkeypatch):
    import sys
    from types import SimpleNamespace

    from quantmaster.data.reference_market import _akshare_route

    calls = []
    monkeypatch.setitem(sys.modules, "akshare", SimpleNamespace(**{
        method: lambda **kwargs: calls.append(kwargs) or _frame(),
    }))
    monkeypatch.setattr("quantmaster.data.reference_market.akshare_call",
                        lambda _key, operation, lane, **kwargs: operation(**kwargs))
    _source, operation = _akshare_route(symbol, "2026-07-20")
    operation()
    assert calls == [{"symbol": alias}]


def test_invalid_sina_ohlc_falls_back_to_global_tushare_without_weakening_validation(monkeypatch):
    raw = _frame()
    raw.loc[0, "low"] = 1000
    monkeypatch.setattr("quantmaster.data.reference_market._akshare_route",
                        lambda *_: ("sina:global-index", lambda: raw))
    monkeypatch.setattr("quantmaster.data.reference_market._tushare_index", lambda *_: _frame())
    result = fetch_reference("KS11.INDEX", "2026-07-20", "2026-07-24")
    assert result.source == "tushare:global-index"
    assert result.attempts[0]["source"] == "sina:global-index"
    assert result.frame["low"].iloc[0] == 99


def test_overseas_display_shows_available_history_but_formal_range_stays_strict(tmp_path, monkeypatch):
    from quantmaster.data import registry
    from quantmaster.data.cache_freshness import CachePurpose
    from quantmaster.server.capabilities import market_history

    monkeypatch.setattr("quantmaster.data.reference_market._akshare_route",
                        lambda *_: ("sina:us-index", _frame))
    store = BarStore(tmp_path / "bars")
    registry.refresh_history("SPX.INDEX", "2026-07-20", "2026-07-24", store=store)
    local = registry.read_history("SPX.INDEX", "2023-07-20", "2026-07-24", store=store)
    assert local.require_data()["close"].tolist() == _frame()["close"].tolist()
    assert local.quality.requested_start == "2023-07-20"
    assert local.quality.partial and not local.quality.formal_eligible
    assert "REFERENCE_HISTORY_PARTIAL" in " ".join(local.quality.issues)
    formal = registry.read_history("SPX.INDEX", "2023-07-20", "2026-07-24",
                                   store=store, purpose=CachePurpose.FORMAL_RESEARCH)
    assert formal.quality.status == "unavailable"
    monkeypatch.setattr("quantmaster.data.read_bars", lambda *_args, **_kwargs: local)
    history = market_history("SPX.INDEX", "2023-07-20", "2026-07-24")
    assert len(history["kline"]) == 5
    assert history["kline"][0] == ["2026-07-20", 100, 100.5, 99, 101, None]


def test_index_increment_does_not_rebase_old_points_or_relabel_provider_history(tmp_path, monkeypatch):
    from quantmaster.data import registry

    raw = pd.concat([_frame(), _frame().assign(date=pd.bdate_range("2026-07-27", periods=5))])
    source = "sina:us-index"
    monkeypatch.setattr("quantmaster.data.reference_market._akshare_route",
                        lambda *_: (source, lambda: raw))
    store = BarStore(tmp_path / "bars")
    first = registry.refresh_history("SPX.INDEX", "2026-07-20", "2026-07-31", store=store)
    raw = raw.copy()
    raw.loc[raw["date"] >= "2026-07-27", ["open", "high", "low", "close"]] *= 2
    registry.refresh_history("SPX.INDEX", "2026-07-20", "2026-08-03", store=store)
    assert store.get("SPX.INDEX")["close"].iloc[0] == first.data["close"].iloc[0]
    source = "yfinance"
    rebuilt = registry.refresh_history("SPX.INDEX", "2026-07-20", "2026-08-04", store=store)
    assert rebuilt.data.attrs["provider_interface"] == "yfinance"
    assert rebuilt.quality.sources == ("yfinance",)
    assert not rebuilt.quality.stale


def test_refresh_reference_panel_isolates_sdk_capability_failure(tmp_path, monkeypatch):
    ready, failed = "SPX.INDEX", "DXY.INDEX"
    frame = _frame().assign(volume=0.0).set_index("date")
    store = BarStore(root=tmp_path / "bars")
    store.put(
        failed,
        frame,
        request_start="2026-07-20",
        request_end="2026-07-24",
        source="yfinance",
    )

    def fetch(symbol: str, _start: str, _end: str) -> ReferenceFetch:
        if symbol == failed:
            raise AttributeError("SDK method missing")
        valid = _normalize_index(frame, symbol, "sina:us-index", _start, _end)
        return ReferenceFetch(frame=valid, source="sina:us-index", attempts=())

    monkeypatch.setattr("quantmaster.data.reference_market.fetch_reference", fetch)
    frames, failures = refresh_reference_panel(
        [ready, failed], "2026-07-20", "2026-07-24", "incremental", store,
    )

    assert ready not in failures, failures
    assert set(frames) == {ready, failed}
    assert failures[failed]["error_code"] == "AttributeError"
    assert store.metadata(failed)["last_status"] == "stale"
