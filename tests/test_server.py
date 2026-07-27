"""FastAPI 服务测试（只测不依赖外部网络的端点）。"""

import json

import pandas as pd
from fastapi.testclient import TestClient

from quantmaster import __version__
from quantmaster.release import RELEASE_DATE
from quantmaster.server.app import app

client = TestClient(app)


class TestBasics:
    def test_health(self):
        resp = client.get("/api/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"
        assert resp.json()["version"] == __version__
        assert resp.json()["release_date"] == RELEASE_DATE
        assert len(resp.headers["X-Request-ID"]) == 12

    def test_release_info(self):
        resp = client.get("/api/release")
        assert resp.status_code == 200
        data = resp.json()
        assert data["version"] == __version__
        assert data["release_date"] == RELEASE_DATE
        assert data["releases"][0]["version"] == __version__
        assert data["releases"][0]["sections"]

    def test_index_serves_html(self):
        resp = client.get("/")
        assert resp.status_code == 200
        assert "QuantMaster" in resp.text
        assert 'data-tab="decision"' in resp.text
        assert 'id="kline-frequency"' in resp.text
        assert 'class="panel market-detail-panel" id="kline-panel"' in resp.text
        assert "function marketChangeSeries" in resp.text
        assert "data:[{yAxis:0}]" in resp.text
        assert "type:'dashed'" in resp.text
        assert "prefers-reduced-motion" in resp.text
        assert "createLoadProgress" in resp.text
        assert "createMarketStreamRenderer" in resp.text
        assert "createDecisionStreamRenderer" in resp.text
        assert "existing.getDom() !== el" in resp.text
        assert "ACTIVE_TAB_STORAGE_KEY" in resp.text
        assert "sessionStorage.getItem(ACTIVE_TAB_STORAGE_KEY)" in resp.text
        assert "activateTab(restoredControl, {persist:false, load:false})" in resp.text
        assert 'class="snapshot-table"' in resp.text
        assert 'class="snapshot-period"' in resp.text
        assert 'class="snapshot-pick"' in resp.text
        assert "event.partial" in resp.text
        assert "/api/decision/dashboard/stream" in resp.text
        assert 'id="asset-workbench"' in resp.text
        assert 'id="tab-candidates"' in resp.text
        assert 'id="candidate-workspace"' in resp.text
        assert 'href="/static/candidates.css"' in resp.text
        assert 'src="/static/candidates.js"' in resp.text
        assert 'data-regime-window="10y"' in resp.text
        assert "名称 / 代码 / 板块" in resp.text
        assert 'id="runtime-info"' in resp.text
        assert 'id="runtime-drawer-frame"' in resp.text
        assert "window.QuantMasterRunInfo" in resp.text
        assert 'data-runtime-filter="problem"' in resp.text
        assert 'data-runtime-filter="running"' in resp.text
        assert '<summary>诊断信息</summary>' in resp.text
        assert "runtimeInfo.begin(source, '正在加载数据'" in resp.text
        assert "if (safeLevel === 'error') setExpanded(true)" not in resp.text
        assert "window.QuantMasterAPI" in resp.text
        assert "unhandledrejection" in resp.text
        assert 'id="release-trigger"' in resp.text
        assert 'id="release-popover"' in resp.text
        assert f'v{__version__}' not in resp.text  # 版本由 data 属性无闪烁注入，脚本负责呈现
        assert f'data-version="{__version__}"' in resp.text
        assert f'data-release-date="{RELEASE_DATE}"' in resp.text
        assert "%%QM_VERSION%%" not in resp.text
        nav_markup = resp.text.split('<nav id="nav"', 1)[1].split("</nav>", 1)[0]
        assert 'data-tab="settings"' not in nav_markup
        assert [
            nav_markup.index(f'data-tab="{tab}"')
            for tab in ("market", "news", "candidates", "decision", "lab", "backtest",
                        "paper", "ledger", "automation")
        ] == sorted([
            nav_markup.index(f'data-tab="{tab}"')
            for tab in ("market", "news", "candidates", "decision", "lab", "backtest",
                        "paper", "ledger", "automation")
        ])
        assert 'class="header-settings" data-tab="settings"' in resp.text
        settings_markup = resp.text.split('class="header-settings"', 1)[1].split("</button>", 1)[0]
        assert "header-settings-label" not in settings_markup
        assert settings_markup.count("<rect x=\"10.65\"") == 8
        assert client.get("/static/candidates.css").status_code == 200
        assert client.get("/static/candidates.js").status_code == 200

    def test_validation_error_has_request_id(self):
        resp = client.post("/api/decision/dashboard/stream", json={
            "universe": "demo", "start": "2023-01-01", "horizon": 99,
        })
        assert resp.status_code == 422
        assert resp.json()["error_id"] == resp.headers["X-Request-ID"]

    def test_stream_error_message_redacts_credentials(self):
        from quantmaster.server import app as app_module

        message = app_module._safe_client_error(
            RuntimeError("token=very-secret-value Bearer abcdef123456 sk-live-secret123"))
        assert "very-secret-value" not in message
        assert "abcdef123456" not in message
        assert "sk-live-secret123" not in message
        assert message.count("***") == 3

    def test_factors_list(self):
        resp = client.get("/api/factors")
        assert resp.status_code == 200
        factors = resp.json()["factors"]
        assert any(f["name"] == "mom_20d" for f in factors)

    def test_selection_history_empty(self):
        resp = client.get("/api/selection/history")
        assert resp.status_code == 200
        assert resp.json() == {"snapshots": []}

    def test_market_overview_emits_each_completed_item(self, monkeypatch):
        from quantmaster.server import app as app_module

        dates = pd.bdate_range("2026-07-20", periods=3)
        frame = pd.DataFrame({"close": [100.0, 101.0, 102.0]}, index=dates)
        monkeypatch.setattr("quantmaster.data.load_history", lambda *args, **kwargs: frame)
        events = []

        result = app_module._market_overview_data(
            "2026-07-01", lambda *args: events.append(args))
        partials = [args[3] for args in events if len(args) > 3 and args[3]]
        final_count = sum(len(items) for items in result["groups"].values())

        assert len(partials) == final_count
        assert all(partial["kind"] == "market_item" for partial in partials)
        assert all(partial["item"]["nav"] for partial in partials)

    def test_decision_dashboard_contract(self, panel, monkeypatch):
        symbols = list(panel["close"].columns)
        mapping = {symbol: "行业A" if i < 4 else "行业B"
                   for i, symbol in enumerate(symbols)}
        names = {symbol: f"股票{i}" for i, symbol in enumerate(symbols)}
        monkeypatch.setattr("quantmaster.data.universe.load_universe", lambda name: symbols)
        def load_panel_with_progress(*args, **kwargs):
            callback = kwargs.get("progress")
            if callback:
                for index, symbol in enumerate(symbols, start=1):
                    callback(index, len(symbols), symbol, True)
            return panel

        monkeypatch.setattr("quantmaster.data.load_panel", load_panel_with_progress)
        monkeypatch.setattr("quantmaster.data.load_stock_names", lambda values: names)
        monkeypatch.setattr("quantmaster.data.industry.load_industry_map", lambda: mapping)

        resp = client.post("/api/decision/dashboard", json={
            "universe": "demo", "start": "2023-01-01", "top_n": 4,
            "horizon": 3, "save": True,
        })
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert set(data) == {"market", "selection", "history"}
        assert data["market"]["current"]["state"] in {
            "strong_up", "up", "range", "down", "strong_down"}
        assert len(data["market"]["sectors"]) == 2
        assert len(data["selection"]["picks"]) == 4
        assert all(pick["name"] == names[pick["symbol"]]
                   for pick in data["selection"]["picks"])
        assert data["history"][0]["signal_date"] == data["selection"]["signal_date"]

        streamed = client.post("/api/decision/dashboard/stream", json={
            "universe": "demo", "start": "2023-01-01", "top_n": 4,
            "horizon": 3, "save": True,
        })
        assert streamed.status_code == 200
        events = [json.loads(line) for line in streamed.text.splitlines() if line]
        request_id = streamed.headers["X-Request-ID"]
        assert request_id
        assert all(event["request_id"] == request_id for event in events)
        updates = [event for event in events if event["type"] == "progress"]
        assert [event["progress"] for event in updates] == sorted(
            event["progress"] for event in updates)
        assert updates[-1]["progress"] == 100
        assert any(event["phase"] == "同步候选行情" for event in updates)
        partial_kinds = [
            event["partial"]["kind"] for event in updates if event.get("partial")
        ]
        assert partial_kinds.count("decision_symbol") == len(symbols)
        assert partial_kinds[-4:] == [
            "decision_market", "decision_sectors",
            "decision_selection", "decision_history",
        ]
        assert next(
            event["progress"] for event in updates
            if event.get("partial", {}).get("kind") == "decision_market"
        ) < 100
        result = next(event["data"] for event in events if event["type"] == "result")
        assert len(result["selection"]["picks"]) == 4


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
