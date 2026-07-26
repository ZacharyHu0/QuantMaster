"""FastAPI 服务测试（只测不依赖外部网络的端点）。"""

from fastapi.testclient import TestClient

from quantmaster.server.app import app

client = TestClient(app)


class TestBasics:
    def test_health(self):
        resp = client.get("/api/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_index_serves_html(self):
        resp = client.get("/")
        assert resp.status_code == 200
        assert "QuantMaster" in resp.text

    def test_factors_list(self):
        resp = client.get("/api/factors")
        assert resp.status_code == 200
        factors = resp.json()["factors"]
        assert any(f["name"] == "mom_20d" for f in factors)


class TestLedgerAPI:
    def test_trade_and_report_flow(self):
        resp = client.post("/api/ledger/cashflow",
                           json={"date": "2024-01-01", "amount": 100000, "kind": "deposit"})
        assert resp.status_code == 200
        resp = client.post("/api/ledger/trade", json={
            "date": "2024-01-02", "symbol": "600519.SH", "side": "buy",
            "price": 100.0, "shares": 100, "fee": 5.0})
        assert resp.status_code == 200

        resp = client.get("/api/ledger/trades")
        assert len(resp.json()["trades"]) == 1

    def test_invalid_trade_rejected(self):
        resp = client.post("/api/ledger/trade", json={
            "date": "2024-01-02", "symbol": "600519.SH", "side": "hold",
            "price": 100.0, "shares": 100})
        assert resp.status_code == 400

    def test_bad_factor_expression_400(self):
        resp = client.post("/api/factors/test",
                           json={"expression": "__import__('os')", "universe": "demo"})
        assert resp.status_code == 400

    def test_validate_bad_expression_400(self):
        resp = client.post("/api/factors/validate",
                           json={"expression": "eval(close)", "split": "2024-01-01"})
        assert resp.status_code == 400

    def test_ledger_nav_empty(self):
        resp = client.get("/api/ledger/nav")
        assert resp.status_code == 200
        data = resp.json()
        assert data["dates"] == []
        assert data["excess_annual"] == 0.0
