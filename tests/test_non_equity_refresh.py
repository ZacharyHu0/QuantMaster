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
        assert result.quality.status == ("verified" if asset_type == "etf" else "degraded")
        if asset_type == "index":
            assert not result.quality.formal_eligible  # Missing index units stay explicit.
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
