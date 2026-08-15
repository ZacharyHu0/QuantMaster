"""自选、关注与持有列表测试（全程离线）。"""

import pandas as pd
from fastapi.testclient import TestClient

from quantmaster.data.base import BarDataEnvelope, BarDataQuality
from quantmaster.data.storage import BarStore
from quantmaster.portfolio import AssetListStore, Ledger, TradeRecord
from quantmaster.server.app import app


def test_asset_list_store_add_update_remove(tmp_path):
    store = AssetListStore(tmp_path / "lists.sqlite")
    item = store.add("favorites", "600519", "贵州茅台")
    assert item["symbol"] == "600519.SH"
    assert store.add("favorites", "600519.SH")["name"] == "贵州茅台"

    store.add("following", "000001.SZ", "平安银行")
    assert [row["symbol"] for row in store.list("following")] == ["000001.SZ"]
    assert store.remove("favorites", "600519") is True
    assert store.list("favorites") == []


def test_asset_lists_api_includes_cached_quotes_and_holdings():
    client = TestClient(app)
    token = client.get("/api/v1/session").json()["csrf_token"]
    client.headers["X-CSRF-Token"] = token
    dates = pd.bdate_range("2024-01-02", periods=2)
    bars = pd.DataFrame({
        "open": [10.0, 11.0], "high": [10.5, 11.5], "low": [9.5, 10.5],
        "close": [10.0, 11.0], "volume": [1000.0, 1200.0],
    }, index=dates)
    BarStore().put("600519.SH", bars)
    Ledger().add_trade(TradeRecord(
        date="2024-01-02", symbol="600519.SH", side="buy", price=10.0, shares=100,
    ))

    response = client.post(
        "/api/v1/portfolio/lists/favorites", json={"symbol": "600519", "name": "贵州茅台"})
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["favorites"][0]["last"] == 11.0
    assert data["favorites"][0]["change_pct"] == 10.0
    assert data["holdings"][0]["symbol"] == "600519.SH"
    assert data["holdings"][0]["shares"] == 100
    assert data["holdings"][0]["unrealized_pnl"] == 100.0

    response = client.delete("/api/v1/portfolio/lists/favorites/600519.SH")
    assert response.status_code == 200
    assert response.json()["favorites"] == []


def test_market_overview_groups_personal_stocks_with_memberships(monkeypatch):
    from quantmaster.market import overview as market_overview

    AssetListStore().add("favorites", "600519", "贵州茅台")
    AssetListStore().add("following", "600519", "贵州茅台")
    Ledger().add_trade(TradeRecord(
        date="2026-07-20", symbol="600519.SH", side="buy", price=1500.0, shares=10,
    ))
    dates = pd.bdate_range("2026-07-20", periods=3)
    bars = pd.DataFrame({"close": [1500.0, 1515.0, 1530.0]}, index=dates)
    market_envelope = BarDataEnvelope(
        data=bars,
        quality=BarDataQuality(
            status="verified",
            requested_start="2026-07-01",
            requested_end=str(dates[-1].date()),
            observed_start=str(dates[0].date()),
            observed_end=str(dates[-1].date()),
            coverage_ratio=1.0,
            sources=("fixture",),
            timezone="Asia/Shanghai",
            adjustment="qfq",
        ),
        provenance=({"source": "fixture"},),
    )
    monkeypatch.setattr(
        "quantmaster.data.refresh_history", lambda *args, **kwargs: market_envelope,
    )
    monkeypatch.setattr(market_overview, "_market_groups", dict)

    result = market_overview.build_market_overview_data("2026-07-01")

    items = result["groups"][market_overview.PERSONAL_MARKET_GROUP]
    assert result["group_counts"][market_overview.PERSONAL_MARKET_GROUP] == 1
    assert len(items) == 1
    assert items[0]["symbol"] == "600519.SH"
    assert items[0]["name"] == "贵州茅台"
    assert items[0]["memberships"] == ["favorites", "following", "holdings"]
    assert items[0]["nav"]


def test_personal_market_replaces_code_only_labels_with_security_names(monkeypatch):
    from quantmaster.market import overview as market_overview

    AssetListStore().add("favorites", "600519", "600519.SH")
    monkeypatch.setattr(
        "quantmaster.data.read_stock_names",
        lambda symbols, **_kwargs: {"600519.SH": "贵州茅台"},
    )

    symbols, memberships = market_overview._personal_market_symbols()

    assert symbols["600519.SH"] == "贵州茅台"
    assert memberships["600519.SH"] == ["favorites"]
