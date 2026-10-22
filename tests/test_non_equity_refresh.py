"""Non-equity Tushare caches must never enter the stock factor repair contract."""
from types import SimpleNamespace

import pandas as pd
import pytest

from quantmaster.data import registry
from quantmaster.data.instruments import InstrumentStore
from quantmaster.data.maintenance import DataRefreshManager
from quantmaster.data.resilience import EndpointFrameCache
from quantmaster.data.storage import BarStore
from quantmaster.data.tushare_source import TushareSource


@pytest.mark.parametrize("symbol,asset_type,endpoint", [
    ("000300.SH", "index", "index_daily"),
    ("399001.SZ", "index", "index_daily"),
    ("399006.SZ", "index", "index_daily"),
    ("510300.SH", "etf", "fund_daily"),
])
@pytest.mark.parametrize("missing_old_date", [False, True])
def test_auto_non_equity_refresh_preserves_history_and_quality_gates(
    isolated_config, tmp_path, monkeypatch, symbol, asset_type, endpoint, missing_old_date,
):
    InstrumentStore().upsert([{
        "symbol": symbol, "code": symbol.partition(".")[0], "market": "CN",
        "asset_type": asset_type, "exchange": symbol.rpartition(".")[2], "currency": "CNY",
    }], source="test", source_priority=100)
    days = pd.bdate_range("2024-01-02", "2024-01-12")
    raw = pd.DataFrame({
        "ts_code": symbol, "trade_date": days.strftime("%Y%m%d"),
        "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.0,
        "vol": 100.0, "amount": 1000.0,
    })
    calls = []

    def fetch(**params):
        calls.append(endpoint)
        value = raw[raw.trade_date.between(params["start_date"], params["end_date"])].copy()
        return value.iloc[1:] if missing_old_date else value

    source = TushareSource(EndpointFrameCache("tushare", root=tmp_path / "endpoints"))
    source._api = SimpleNamespace(**{endpoint: fetch})
    monkeypatch.setattr(source, "trade_calendar", lambda start, end: days)
    monkeypatch.setattr("quantmaster.data.tushare_source.TUSHARE_LIMITER.wait", lambda: None)
    monkeypatch.setattr(
        "quantmaster.data.tushare_source.provider_call", lambda lane, key, fetch, **kw: fetch(),
    )
    monkeypatch.setattr(registry, "_request_factories", lambda **kw: {
        registry.guess_market(symbol): [lambda: source],
    })
    monkeypatch.setattr(registry, "_local_sessions", lambda start, end: (
        days[(days >= start) & (days <= end)], "test-calendar",
    ))
    monkeypatch.setattr(registry, "_repair_history_locked", lambda *a, **kw: pytest.fail(
        "index/fund refresh entered equity factor repair",
    ))
    store = BarStore()
    old = source._normalize_market_frame(raw.iloc[:5])
    store.put(symbol, old, replace=True, source="tushare")
    store.mark_status(symbol, "stale")
    original = store._path(symbol).read_bytes()

    result = registry.refresh_history(symbol, "2024-01-02", "2024-01-12", store=store)

    assert calls == [endpoint]
    assert old.index.difference(store.get(symbol).index).empty
    if missing_old_date:
        assert store._path(symbol).read_bytes() == original
        assert result.quality.stale
        assert not result.quality.formal_eligible
    else:
        assert result.data.index.max() == days[-1]
        assert result.quality.status == "verified"
        assert result.quality.formal_eligible
        assert not result.quality.stale
        # An already-fresh request reuses the local result with no extra provider calls.
        registry.refresh_history(symbol, "2024-01-02", "2024-01-12", store=store)
        assert calls == [endpoint]


def test_non_equity_maintenance_does_not_relabel_gaps_as_stock_factor_failure(
    isolated_config, monkeypatch,
):
    quality = SimpleNamespace(
        stale=False, observed_end="2024-01-12", semantic_diagnostic_code="",
        coverage_ratio=1.0, units=(("close", "CNY"),), sources=("tushare",),
    )
    envelope = SimpleNamespace(quality=quality, data=pd.DataFrame(), provenance=[])
    monkeypatch.setattr(registry, "_repair_candidate_quality", lambda *a: pytest.fail(
        "index maintenance entered equity factor validation",
    ))
    assert not DataRefreshManager._prepared_with_formal_gaps(
        envelope, "000300.SH", "2024-01-02", "2024-01-12",
    )


def test_index_refresh_replaces_colliding_stockdb_cache(isolated_config, monkeypatch):
    from quantmaster.data.free_stockdb_source import FreeStockDBProviderError, FreeStockDBSource

    symbol = "000001.SH"
    assert not DataRefreshManager._uses_stockdb(symbol)
    source = FreeStockDBSource()
    monkeypatch.setattr(source, "_sdk_data", lambda *a, **kw: pytest.fail("index queried bare stock code"))
    with pytest.raises(FreeStockDBProviderError, match="指数"):
        source.daily(symbol, "2024-01-02", "2024-01-04")
    days = pd.bdate_range("2024-01-02", "2024-01-04")
    wrong = pd.DataFrame(dict(open=10., high=11., low=9., close=10., volume=100.), index=days)
    wrong.index.name = "date"
    store = BarStore()
    store.put(symbol, wrong, replace=True, source="free-stockdb")
    local = registry.read_history(symbol, "2024-01-02", "2024-01-04", store=store)
    assert not local.quality.analysis_eligible
    index_source = TushareSource()
    raw = pd.DataFrame(dict(ts_code=symbol, trade_date=days.strftime("%Y%m%d"),
                            open=3000., high=3100., low=2900., close=3000., vol=100., amount=1000.))
    monkeypatch.setattr(index_source, "_call", lambda *a, **kw: raw)
    monkeypatch.setattr(index_source, "trade_calendar", lambda start, end: days)
    monkeypatch.setattr(registry, "_request_factories", lambda **kw: {
        registry.Market.CN: [lambda: source, lambda: index_source],
    })
    monkeypatch.setattr(registry, "_local_sessions", lambda start, end: (days, "test-calendar"))
    result = registry.refresh_history(symbol, "2024-01-02", "2024-01-04", store=store)
    assert result.require_data().close.tolist() == [3000.] * 3
    assert result.quality.formal_eligible
    assert store.get(symbol).attrs["provider_interface"] == "tushare:index_daily"
    # A clean local page read and an actual calculation must use the repaired index.
    local = registry.read_history(symbol, "2024-01-02", "2024-01-04", store=store)
    assert local.require_data().close.pct_change().dropna().eq(0).all()
    assert local.quality.formal_eligible


def test_index_contract_rejects_identity_and_unit_conflicts(isolated_config, monkeypatch):
    from quantmaster.data.resilience import ProviderContractChanged

    source = TushareSource()
    monkeypatch.setattr(source, "cached_trade_calendar", lambda *a: None)
    monkeypatch.setattr(source, "trade_calendar", lambda *a: (_ for _ in ()).throw(RuntimeError("offline")))
    raw = pd.DataFrame(dict(ts_code=["399001.SZ"], trade_date=["20240102"],
                            open=[3000.], high=[3100.], low=[2900.], close=[3000.], vol=[10.], amount=[20.]))
    with pytest.raises(ProviderContractChanged):
        source._index_frame("000300.SH", "2024-01-02", "2024-01-02", raw, fetch_calendar=True)
    result = source._index_frame("399001.SZ", "2024-01-02", "2024-01-02", raw, fetch_calendar=True)
    assert result.volume.iloc[0] == 1000 and result.amount.iloc[0] == 20000
    quality = registry._assess_daily_frame(
        result, "2024-01-02", "2024-01-02", symbol="399001.SZ", source="tushare",
    )
    assert quality.analysis_eligible and not quality.formal_eligible
    result.attrs["units"]["volume"] = "lot"
    quality = registry._assess_daily_frame(
        result, "2024-01-02", "2024-01-02", symbol="399001.SZ", source="tushare",
    )
    assert dict(quality.units)["volume"] == "unknown" and not quality.formal_eligible


def test_index_increment_keeps_whole_history_calendar(isolated_config, monkeypatch):
    symbol = "399001.SZ"
    days = pd.bdate_range("2024-01-02", "2024-01-12")
    raw = pd.DataFrame(dict(ts_code=symbol, trade_date=days.strftime("%Y%m%d"),
                            open=3000., high=3100., low=2900., close=3000., vol=100., amount=1000.))
    def daily(**params):
        return raw[raw.trade_date.between(params["start_date"], params["end_date"])].copy()
    def calendar(**params):
        dates = pd.date_range(params["start_date"], params["end_date"])
        return pd.DataFrame(dict(exchange="SSE", cal_date=dates.strftime("%Y%m%d"),
                                 is_open=(dates.dayofweek < 5).astype(int), pretrade_date=""))
    source = TushareSource()
    source._api = SimpleNamespace(index_daily=daily, trade_cal=calendar)
    monkeypatch.setattr("quantmaster.data.tushare_source.TUSHARE_LIMITER.wait", lambda: None)
    monkeypatch.setattr(
        "quantmaster.data.tushare_source.provider_call", lambda lane, key, fetch, **kw: fetch(),
    )
    monkeypatch.setattr(registry, "_request_factories", lambda **kw: {registry.Market.CN: [lambda: source]})
    monkeypatch.setattr(registry, "_local_sessions", lambda start, end: (pd.DatetimeIndex([]), "unavailable"))
    store = BarStore()
    first = registry.refresh_history(symbol, "2024-01-02", "2024-01-05", store=store)
    assert first.quality.formal_eligible
    extended = registry.refresh_history(symbol, "2024-01-02", "2024-01-12", store=store)
    assert extended.quality.formal_eligible
    assert first.data.index.difference(extended.data.index).empty
    assert registry.read_history(symbol, "2024-01-02", "2024-01-12", store=store).quality.formal_eligible
