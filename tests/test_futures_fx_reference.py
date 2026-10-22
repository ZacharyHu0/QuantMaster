"""Reference refresh must be usable without inventing formal trading evidence."""
from __future__ import annotations

import pandas as pd
import pytest

from quantmaster.data import registry
from quantmaster.data.maintenance import DataRefreshManager
from quantmaster.data.reference_market import PRICE_REFERENCES, _normalize_price, reference_contract
from quantmaster.data.storage import BarStore


def bars():
    return pd.DataFrame({
        "date": pd.bdate_range("2026-07-20", periods=5),
        "open": [100.] * 5, "high": [102.] * 5,
        "low": [99.] * 5, "close": [101.] * 5,
    })


def source_for(symbol):
    return ("sina:USDCNY" if symbol.endswith(".FX") else
            "sina:foreign-futures" if symbol.endswith(".CONTINUOUS") else "sina:domestic-futures")


@pytest.mark.parametrize("symbol", sorted(PRICE_REFERENCES))
def test_real_reference_contract_refresh_reuses_cache_and_serves_partial_chart(symbol, tmp_path, monkeypatch):
    from quantmaster.server.capabilities import market_history

    calls = []
    source = source_for(symbol)
    monkeypatch.setattr("quantmaster.data.reference_market._akshare_route",
                        lambda *_: (source, lambda: calls.append(symbol) or bars()))
    monkeypatch.setattr(registry, "_request_factories", lambda **_: pytest.fail("generic fallback"))
    store = BarStore(tmp_path / "bars")
    # Legacy unbound prices must be rebuilt, never retroactively certified.
    legacy = bars().assign(volume=0).set_index("date") * 10
    store.put(symbol, legacy, source="tushare:fx", request_start="2023-07-20", request_end="2026-07-24")
    old = registry.read_history(symbol, "2026-07-20", "2026-07-24", store=store)
    assert old.quality.status == "unavailable"
    first = registry.refresh_history(symbol, "2023-07-20", "2026-07-24", store=store)
    assert first.require_data()["close"].tolist() == [101.] * 5
    assert reference_contract(first.data, symbol)
    assert first.quality.preview_eligible and not first.quality.formal_eligible
    assert first.quality.partial and not first.quality.stale
    assert first.quality.requested_start == "2023-07-20"
    assert first.quality.observed_start == "2026-07-20"
    assert "volume" not in first.data
    second = registry.refresh_history(symbol, "2023-07-20", "2026-07-24", store=store)
    assert second.data.equals(first.data)
    prepared = DataRefreshManager._refresh_one(store, symbol, "2023-07-20", "2026-07-24")
    assert prepared["code"] == "reference_only"
    assert calls == [symbol]
    local = registry.read_history(symbol, "2023-07-20", "2026-07-24", store=store)
    monkeypatch.setattr("quantmaster.data.read_bars", lambda *_a, **_kw: local)
    monkeypatch.setattr("quantmaster.server.capabilities.default_close_data_end",
                        lambda: pytest.fail("CN calendar"))
    chart = market_history(symbol)
    if symbol.endswith(".CONTINUOUS"):
        assert chart["series_type"] == "close"
        assert chart["kline"][0] == ["2026-07-20", None, 101., None, None, None]
        assert "REFERENCE_CLOSE_ONLY" in " ".join(first.quality.issues)
    else:
        assert chart["kline"][0] == ["2026-07-20", 100., 101., 99., 102., None]


@pytest.mark.parametrize("defect", ["identity", "missing", "duplicate", "nan", "ohlc", "sparse", "lost_date"])
def test_bad_reference_refresh_retains_verified_cache(defect, tmp_path, monkeypatch):
    symbol, source = "AU0.SHF", "sina:domestic-futures"
    raw = bars()
    monkeypatch.setattr("quantmaster.data.reference_market._akshare_route", lambda *_: (source, lambda: raw))
    store = BarStore(tmp_path / "bars")
    good = registry.refresh_history(symbol, "2026-07-20", "2026-07-24", store=store)
    digest = store.metadata(symbol)["content_sha256"]
    raw = bars()
    if defect == "identity":
        raw.attrs["instrument"] = "HG.CONTINUOUS"
    elif defect == "missing":
        raw = raw.drop(columns="open")
    elif defect == "duplicate":
        raw.loc[1, "date"] = raw.loc[0, "date"]
    elif defect == "nan":
        raw.loc[1, "close"] = float("nan")
    elif defect == "ohlc":
        raw.loc[1, "high"] = 1
    elif defect == "lost_date":
        raw = raw.drop(index=2)
    else:
        raw = raw.iloc[:1]
    failed = registry.refresh_history(symbol, "2026-07-20", "2026-08-20", store=store, mode="full")
    assert failed.quality.stale
    assert failed.data.equals(good.data)
    assert store.metadata(symbol)["content_sha256"] == digest


def test_continuous_increment_never_rescales_old_prices(tmp_path, monkeypatch):
    raw = pd.concat([bars(), bars().assign(date=pd.bdate_range("2026-07-27", periods=5))])
    monkeypatch.setattr("quantmaster.data.reference_market._akshare_route",
                        lambda *_: ("sina:domestic-futures", lambda: raw))
    store = BarStore(tmp_path / "bars")
    first = registry.refresh_history("AU0.SHF", "2026-07-20", "2026-07-31", store=store)
    raw = raw.copy()
    raw.loc[raw["date"] >= "2026-07-27", ["open", "high", "low", "close"]] *= 2
    second = registry.refresh_history("AU0.SHF", "2026-07-20", "2026-08-03", store=store)
    assert not second.quality.stale
    assert store.get("AU0.SHF")["close"].iloc[0] == first.data["close"].iloc[0]


def test_energy_reference_preserves_actual_negative_prices():
    raw = bars()
    raw.loc[0, ["open", "high", "low", "close"]] = [-10, -5, -40, -37]
    frame = _normalize_price(raw, "CL.CONTINUOUS", "sina:foreign-futures", "2026-07-20", "2026-07-24")
    assert frame["close"].iloc[0] == -37


def test_fx_chart_preserves_provider_quote_precision(tmp_path, monkeypatch):
    from quantmaster.server.capabilities import market_history

    raw = bars()
    raw[["open", "high", "low", "close"]] = [6.6748, 6.6772, 6.6723, 6.6733]
    monkeypatch.setattr("quantmaster.data.reference_market._akshare_route",
                        lambda *_: ("sina:USDCNY", lambda: raw))
    store = BarStore(tmp_path / "bars")
    envelope = registry.refresh_history("USD-CNY.FX", "2026-07-20", "2026-07-24", store=store)
    monkeypatch.setattr("quantmaster.data.read_bars", lambda *_a, **_kw: envelope)
    chart = market_history("USD-CNY.FX", "2026-07-20", "2026-07-24")
    assert chart["kline"][0] == ["2026-07-20", 6.6748, 6.6733, 6.6723, 6.6772, None]
