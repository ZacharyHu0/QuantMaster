"""Offline regressions for the operator's repeated US10Y identity failure."""
from __future__ import annotations

import pandas as pd
import pytest

from quantmaster.data import registry
from quantmaster.data.maintenance import DataRefreshManager
from quantmaster.data.storage import BarStore
from quantmaster.market_capabilities import Market, guess_market


@pytest.fixture(autouse=True)
def no_yahoo(monkeypatch):
    monkeypatch.setattr("quantmaster.data.yfinance_source.YFinanceSource.daily",
                        lambda *_: pytest.fail("unverified Yahoo yield scale"))


def test_us10y_actual_identity_failure_is_not_retried():
    failure = DataRefreshManager._failure(ValueError(
        "标的缺少已确认市场身份: US10Y.RATE",
    ))
    assert failure["code"] == "identity_missing"
    assert failure["retryable"] is False
    assert guess_market("US10Y.RATE") == Market.US
    with pytest.raises(ValueError, match="市场身份"):
        guess_market("UNKNOWN.RATE")


def test_us10y_refresh_uses_yield_contract_and_fresh_cache(tmp_path, monkeypatch):
    calls = []

    def fetch():
        calls.append(1)
        return pd.DataFrame({
            "日期": pd.bdate_range("2026-07-20", periods=5),
            "美国国债收益率10年": [4.1, 4.2, 4.0, 0.0, 4.3],
            "中国国债收益率10年": [1.0] * 5,
        })

    monkeypatch.setattr("quantmaster.data.reference_market._akshare_route",
                        lambda *_: ("akshare:us-treasury", fetch))
    monkeypatch.setattr(registry, "_request_factories", lambda **_: pytest.fail("equity fallback"))
    store = BarStore(root=tmp_path / "bars")
    first = registry.refresh_history("US10Y.RATE", "2026-07-20", "2026-07-24", store=store)
    second = registry.refresh_history("US10Y.RATE", "2026-07-20", "2026-07-24", store=store)
    assert calls == [1]
    assert list(first.data.columns) == ["close"]
    assert first.data["close"].tolist() == [4.1, 4.2, 4.0, 0.0, 4.3]
    assert second.quality.timezone == "America/New_York"
    assert dict(second.quality.units)["close"] == "percent_points"
    assert second.quality.status == "degraded"
    assert not second.quality.formal_eligible
    assert second.quality.expected_session == ""
    assert second.data.attrs["instrument"] == "US10Y.RATE"
    result = DataRefreshManager._refresh_one(store, "US10Y.RATE", "2026-07-20", "2026-07-24")
    assert result is not None and result["code"] == "reference_only"
    assert "error" not in result


def test_us10y_actual_adapter_field_identity(monkeypatch):
    import sys
    from types import SimpleNamespace

    from quantmaster.data.reference_market import fetch_reference

    calls = []

    def bond_zh_us_rate(**kwargs):
        calls.append(kwargs)
        return pd.DataFrame({"日期": ["2026-07-24"], "美国国债收益率10年": [4.25]})

    monkeypatch.setitem(sys.modules, "akshare", SimpleNamespace(bond_zh_us_rate=bond_zh_us_rate))
    result = fetch_reference("US10Y.RATE", "2026-07-20", "2026-07-24")
    assert calls == [{"start_date": "20260720"}]
    assert result.frame.iloc[0]["close"] == 4.25
    assert result.frame.attrs["provider_interface"] == "bond_zh_us_rate:EMG00001310"


@pytest.mark.parametrize("raw", [
    pd.DataFrame({"日期": ["2026-07-24"], "中国国债收益率10年": [1.0]}),
    pd.DataFrame({"日期": ["2026-07-24"], "美国国债收益率10年": [float("inf")]}),
    pd.DataFrame({"日期": ["2026-07-24"] * 2, "美国国债收益率10年": [4.2, 4.3]}),
])
def test_us10y_rejects_invalid_provider_contract(raw, monkeypatch):
    from quantmaster.data.reference_market import ReferenceMarketUnavailable, fetch_reference

    monkeypatch.setattr("quantmaster.data.reference_market._akshare_route",
                        lambda *_: ("akshare:us-treasury", lambda: raw))
    with pytest.raises(ReferenceMarketUnavailable):
        fetch_reference("US10Y.RATE", "2026-07-20", "2026-07-24")


def test_us10y_source_failure_never_falls_through_equities(tmp_path, monkeypatch):
    from quantmaster.data.base import MarketDataUnavailable

    def offline():
        raise ConnectionError("provider connection unavailable")

    monkeypatch.setattr("quantmaster.data.reference_market._akshare_route",
                        lambda *_: ("akshare:us-treasury", offline))
    monkeypatch.setattr(registry, "_request_factories", lambda **_: pytest.fail("equity fallback"))
    store = BarStore(root=tmp_path / "bars")
    with pytest.raises(MarketDataUnavailable, match="connection unavailable"):
        registry.refresh_history("US10Y.RATE", "2026-07-20", "2026-07-24", store=store)
    assert store.get("US10Y.RATE") is None


def test_us10y_new_york_bound_and_panel_share_registry(tmp_path, monkeypatch):
    from quantmaster.data.reference_market import refresh_reference_panel, yield_request_end

    now = pd.Timestamp("2026-09-17T09:00:00+08:00")
    assert yield_request_end("2026-09-17", now) == "2026-09-15"
    assert yield_request_end("2026-09-10", now) == "2026-09-10"
    monkeypatch.setattr(registry, "market_now", lambda: now.to_pydatetime())
    monkeypatch.setattr(registry, "_local_sessions", lambda *_: pytest.fail("CN calendar"))
    calls = []

    def route(symbol, start):
        calls.append((symbol, start))
        return "akshare:us-treasury", lambda: pd.DataFrame({
            "日期": ["2026-09-14", "2026-09-15", "2026-09-16"],
            "美国国债收益率10年": [4.1, 4.2, 4.3],
        })

    monkeypatch.setattr("quantmaster.data.reference_market._akshare_route", route)
    store = BarStore(root=tmp_path / "bars")
    for _ in range(2):
        frames, failures = refresh_reference_panel(
            ["US10Y.RATE"], "2026-09-14", "2026-09-17", "auto", store,
        )
        assert failures == {}
        assert frames["US10Y.RATE"].index.max() == pd.Timestamp("2026-09-15")
    assert len(calls) == 1
    assert store.metadata("US10Y.RATE")["coverage_end"] == "2026-09-15"


def test_us10y_old_price_bars_are_not_reinterpreted(tmp_path):
    frame = pd.DataFrame({column: [4.25] for column in ("open", "high", "low", "close", "volume")},
                         index=pd.DatetimeIndex(["2026-07-24"], name="date"))
    store = BarStore(root=tmp_path / "bars")
    store.put("US10Y.RATE", frame, source="yfinance")
    result = registry.read_history("US10Y.RATE", "2026-07-24", "2026-07-24", store=store)
    assert result.quality.status == "unavailable"
    assert not result.quality.formal_eligible


def test_us10y_increment_does_not_rescale_prior_yields(tmp_path, monkeypatch):
    raw = pd.DataFrame({"日期": ["2026-07-20", "2026-07-21"], "美国国债收益率10年": [4.0, 4.1]})
    monkeypatch.setattr("quantmaster.data.reference_market._akshare_route",
                        lambda *_: ("akshare:us-treasury", lambda: raw))
    store = BarStore(root=tmp_path / "bars")
    registry.refresh_history("US10Y.RATE", "2026-07-20", "2026-07-21", store=store)
    raw.loc[1, "美国国债收益率10年"] = 4.2
    raw.loc[2] = ["2026-07-22", 4.3]
    result = registry.refresh_history("US10Y.RATE", "2026-07-20", "2026-07-22", store=store)
    assert result.data["close"].tolist() == [4.0, 4.2, 4.3]


def test_us10y_full_refresh_and_failed_refresh_preserve_evidence(tmp_path, monkeypatch):
    raw = pd.DataFrame({"日期": ["2026-07-24"], "美国国债收益率10年": [4.25]})
    monkeypatch.setattr("quantmaster.data.reference_market._akshare_route",
                        lambda *_: ("akshare:us-treasury", lambda: raw))
    monkeypatch.setattr(registry, "_request_factories", lambda **_: pytest.fail("equity fallback"))
    store = BarStore(root=tmp_path / "bars")
    first = registry.refresh_history("US10Y.RATE", "2026-07-24", "2026-07-24", store=store, mode="full")
    assert first.quality.preview_eligible and not first.quality.formal_eligible
    monkeypatch.setattr("quantmaster.data.reference_market._akshare_route", lambda *_: None)
    second = registry.refresh_history("US10Y.RATE", "2026-07-24", "2026-07-24", store=store, mode="full")
    assert second.data.equals(first.data)
    assert second.quality.stale
    assert store.metadata("US10Y.RATE")["last_status"] == "refresh_failed"


def test_us10y_instrument_is_nontradable_reference(tmp_path, monkeypatch):
    from quantmaster.data.instruments import InstrumentStore, validate_bar_capability

    store = InstrumentStore(path=tmp_path / "master.sqlite")
    instrument = store.get("US10Y.RATE")
    assert instrument is not None and instrument.asset_type == "yield"
    assert not instrument.tradable
    assert instrument.provider_symbol == "EMG00001310"
    assert instrument.timezone == "America/New_York"
    monkeypatch.setattr("quantmaster.data.instruments.InstrumentStore", lambda: store)
    assert validate_bar_capability("US10Y.RATE") == instrument


def test_us10y_zero_yield_card_and_history_are_json_safe(tmp_path, monkeypatch):
    import json

    from quantmaster.market.overview import _market_item
    from quantmaster.server.capabilities import market_history

    raw = pd.DataFrame({
        "日期": ["2026-07-22", "2026-07-23", "2026-07-24"],
        "美国国债收益率10年": [-0.5, 0.0, 4.25],
    })
    monkeypatch.setattr("quantmaster.data.reference_market._akshare_route",
                        lambda *_: ("akshare:us-treasury", lambda: raw))
    store = BarStore(root=tmp_path / "bars")
    envelope = registry.refresh_history("US10Y.RATE", "2026-07-22", "2026-07-24", store=store)
    card = _market_item("US10Y.RATE", "美债10年收益率", envelope.data, store.metadata("US10Y.RATE"))
    assert card is not None and card["change_pct"] is None and card["last"] == 4.25
    monkeypatch.setattr("quantmaster.data.read_bars", lambda *_args, **_kwargs: envelope)
    history = market_history("US10Y.RATE", "2026-07-23", "2026-07-24")
    assert history["series_type"] == "yield"
    assert history["kline"][0][2] == -0.5
    assert history["kline"][-1] == ["2026-07-24", None, 4.25, None, None, None]
    assert not history["data_quality"]["formal_eligible"]
    json.dumps({"card": card, "history": history}, allow_nan=False)
