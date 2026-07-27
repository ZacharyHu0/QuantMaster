"""自选、关注与持有列表测试（全程离线）。"""

import pandas as pd
from fastapi.testclient import TestClient

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
        "/api/assets/lists/favorites", json={"symbol": "600519", "name": "贵州茅台"})
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["favorites"][0]["last"] == 11.0
    assert data["favorites"][0]["change_pct"] == 10.0
    assert data["holdings"][0]["symbol"] == "600519.SH"
    assert data["holdings"][0]["shares"] == 100
    assert data["holdings"][0]["unrealized_pnl"] == 100.0

    response = client.delete("/api/assets/lists/favorites/600519.SH")
    assert response.status_code == 200
    assert response.json()["favorites"] == []
