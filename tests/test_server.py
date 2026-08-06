"""FastAPI 服务测试（只测不依赖外部网络的端点）。"""

import ast
import html
import json
import re
import sys
from html.parser import HTMLParser

import pandas as pd
from fastapi.testclient import TestClient

from quantmaster import __version__
from quantmaster.backtest.metrics import RISK_FREE, TRADING_DAYS
from quantmaster.release import RELEASE_DATE
from quantmaster.runtime.process import run_process
from quantmaster.server.app import app

client = TestClient(app)
_csrf = client.get("/api/v1/session").json()["csrf_token"]
client.headers["X-CSRF-Token"] = _csrf


class TestBasics:
    def test_health(self):
        resp = client.get("/api/v1/diagnostics")
        assert resp.status_code == 200
        assert resp.json()["level"] in {"ok", "warning", "error"}
        assert resp.json()["checked_at"]
        assert isinstance(resp.json()["issues"], list)
        assert len(resp.headers["X-Request-ID"]) == 12
        assert client.get("/api/v1/health").status_code == 404

    def test_liveness_is_store_free_and_diagnostics_are_separate(self, monkeypatch):
        monkeypatch.setattr(
            "quantmaster.server.problems.collect_health_report",
            lambda: (_ for _ in ()).throw(AssertionError("liveness must not probe stores")),
        )
        live = client.get("/api/v1/health/live")
        assert live.status_code == 200
        assert live.json() == {
            "status": "ok", "version": __version__, "release_date": RELEASE_DATE,
        }
        ready = client.get("/api/v1/health/ready")
        assert ready.status_code == 200
        assert ready.json()["status"] == "ready"

    def test_local_boundary_csrf_and_security_headers(self):
        anonymous = TestClient(app)
        blocked = anonymous.post("/api/v1/research/selection/daily", json={})
        assert blocked.status_code == 403
        assert blocked.json()["problem"]["code"] == "csrf_missing"

        session = anonymous.get("/api/v1/session")
        token = session.json()["csrf_token"]
        accepted = anonymous.post(
            "/api/v1/research/decision/dashboard/stream",
            json={"universe": "demo", "start": "2023-01-01", "horizon": 99},
            headers={"X-CSRF-Token": token},
        )
        assert accepted.status_code == 422
        cross_origin = anonymous.post(
            "/api/v1/research/decision/dashboard/stream",
            json={"universe": "demo", "start": "2023-01-01", "horizon": 99},
            headers={"X-CSRF-Token": token, "Origin": "https://attacker.example"},
        )
        assert cross_origin.status_code == 403
        assert cross_origin.json()["problem"]["code"] == "origin_rejected"
        cross_origin_read = anonymous.get(
            "/api/v1/health/live",
            headers={"Origin": "https://attacker.example"},
        )
        assert cross_origin_read.status_code == 403
        assert cross_origin_read.json()["problem"]["code"] == "origin_rejected"

        untrusted_host = TestClient(app).get("/", headers={"Host": "attacker.example"})
        assert untrusted_host.status_code == 403
        remote = TestClient(app, client=("203.0.113.8", 50000))
        assert remote.get("/").status_code == 403

        page = anonymous.get("/")
        assert page.headers["X-Content-Type-Options"] == "nosniff"
        assert page.headers["X-Frame-Options"] == "DENY"
        csp = page.headers["Content-Security-Policy"]
        assert "script-src 'self'" in csp
        assert "style-src 'self'" in csp
        assert "'nonce-" not in csp
        assert "%%QM_CSP_NONCE%%" not in page.text
        assert '<link rel="stylesheet" href="/static/app.css">' in page.text
        assert '<script src="/static/app.js"></script>' in page.text
        assert "<script nonce=" not in page.text
        assert anonymous.get("/static/app.css").status_code == 200
        app_script = anonymous.get("/static/app.js")
        assert app_script.status_code == 200
        assert "csrfRefreshPromise" in app_script.text
        assert "csrfCodes.has(String(rejection?.problem?.code || ''))" in app_script.text

        stale_browser = TestClient(app)
        stale_browser.cookies.set(
            "qm_csrf", "1.stale.invalid", domain="testserver.local", path="/",
        )
        refreshed_page = stale_browser.get("/")
        refreshed_token = stale_browser.cookies.get(
            "qm_csrf", domain="testserver.local", path="/",
        )
        assert refreshed_page.status_code == 200
        assert refreshed_token and refreshed_token != "1.stale.invalid"
        accepted_after_refresh = stale_browser.post(
            "/api/v1/research/decision/dashboard/stream",
            json={"universe": "demo", "start": "2023-01-01", "horizon": 99},
            headers={"X-CSRF-Token": refreshed_token},
        )
        assert accepted_after_refresh.status_code == 422

    def test_release_info(self):
        resp = client.get("/api/v1/release")
        assert resp.status_code == 200
        data = resp.json()
        assert data["version"] == __version__
        assert data["release_date"] == RELEASE_DATE
        assert data["releases"][0]["version"] == __version__
        assert data["releases"][0]["sections"]
        assert len(data["releases"]) == 10
        assert data["history_url"].endswith("/CHANGELOG.md")

    def test_index_serves_html(self):
        resp = client.get("/")
        assert resp.status_code == 200
        assert "QuantMaster" in resp.text
        assert 'data-tab="decision"' in resp.text
        assert 'id="kline-frequency"' in resp.text
        assert 'class="panel market-detail-panel" id="kline-panel"' in resp.text
        assert 'id="major-indexes" data-market-group="A股指数"' in resp.text
        assert "{'A股指数':majorIndexes}" in resp.text
        assert "主要指数区块已保留" in resp.text
        assert "function marketChangeSeries" in resp.text
        assert "function marketSparkParsedDate" in resp.text
        assert "function marketSparkMonth" in resp.text
        assert "function marketSparkOption" in resp.text
        assert "type:'category',data:categories,show:true" in resp.text
        assert "id:'market-spark-latest'" in resp.text
        assert "区间涨跌" in resp.text
        assert "当日涨跌" in resp.text
        assert "PERSONAL_MARKET_GROUP = '我的股票'" in resp.text
        assert "market-section-title" in resp.text
        assert "mkt-memberships" in resp.text
        assert "名称与代码 · 标注提及次数" in resp.text
        assert "queueMarketReload" in resp.text
        assert "data:[{yAxis:0}]" in resp.text
        assert "type:'dashed'" in resp.text
        assert "prefers-reduced-motion" in resp.text
        assert "--scrollbar-track: #151514" in resp.text
        assert "scrollbar-color: var(--scrollbar-thumb) var(--scrollbar-track)" in resp.text
        assert "scrollbar-gutter:stable" in resp.text
        assert 'class="backtest-trades-scroll"' in resp.text
        assert "createLoadProgress" in resp.text
        assert "createMarketStreamRenderer" in resp.text
        assert "createDecisionStreamRenderer" in resp.text
        assert "function loadKlineSeries" in resp.text
        assert "function renderKlineSeries" in resp.text
        assert "function openDecisionKline" in resp.text
        assert "data-decision-kline-trigger" in resp.text
        assert "data-decision-asset-toggle" in resp.text
        assert "showKline(row.dataset.symbol" not in resp.text
        assert 'src="/static/charts.js"' in resp.text
        assert 'href="/static/charts.css"' in resp.text
        assert 'href="/static/brand-intro.css"' in resp.text
        assert 'src="/static/brand-intro.js"' in resp.text
        assert 'href="/static/brand/quantmaster-favicon.svg"' in resp.text
        assert 'src="/static/brand/quantmaster-mark-inverse.svg"' in resp.text
        assert 'id="brand-replay"' in resp.text
        assert "window.QuantCharts.activateTab(tab)" in resp.text
        assert "ACTIVE_TAB_STORAGE_KEY" in resp.text
        assert "sessionStorage.getItem(ACTIVE_TAB_STORAGE_KEY)" in resp.text
        assert "activateTab(restoredControl, {persist:false, load:false})" in resp.text
        assert 'class="snapshot-table"' in resp.text
        assert 'class="snapshot-period"' in resp.text
        assert 'class="snapshot-pick"' in resp.text
        assert "event.partial" in resp.text
        assert "/api/v1/research/decision/dashboard/stream" in resp.text
        assert 'id="asset-workbench"' in resp.text
        assert 'id="tab-candidates"' in resp.text
        assert 'id="candidate-workspace"' in resp.text
        assert 'href="/static/candidates.css"' in resp.text
        assert 'src="/static/candidates.js"' in resp.text
        assert 'id="tab-stock-analysis"' in resp.text
        assert 'href="/static/stock-analysis.css"' in resp.text
        assert 'src="/static/stock-analysis.js"' in resp.text
        assert 'id="tab-help"' in resp.text
        assert 'class="header-help" data-tab="help"' in resp.text
        assert 'href="/static/help.css"' in resp.text
        assert 'src="/static/help.js"' in resp.text
        assert f'data-trading-days="{TRADING_DAYS}"' in resp.text
        assert f'data-risk-free="{RISK_FREE}"' in resp.text
        assert 'data-regime-window="10y"' in resp.text
        assert "名称 / 代码 / 板块" in resp.text
        assert 'id="runtime-info"' in resp.text
        assert 'id="data-refresh-preview"' in resp.text
        assert 'id="data-refresh-start-button"' in resp.text
        assert 'id="data-refresh-resume"' in resp.text
        assert 'id="runtime-drawer-frame"' in resp.text
        assert "window.QuantMasterRunInfo" in resp.text
        assert "window.QuantMasterProblemDialog" in resp.text
        assert "window.QuantMasterNDJSON" in resp.text
        assert "runtimeInfo.sync('health'" in resp.text
        assert 'id="operation-problem-dialog"' in resp.text
        assert 'class="quality-summary"' in resp.text
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
        assert "%%QM_TRADING_DAYS%%" not in resp.text
        assert "%%QM_RISK_FREE%%" not in resp.text
        nav_markup = resp.text.split('<nav id="nav"', 1)[1].split("</nav>", 1)[0]
        assert 'data-tab="settings"' not in nav_markup
        assert [
            nav_markup.index(f'data-tab="{tab}"')
            for tab in ("market", "news", "stock-analysis", "candidates", "decision", "lab", "backtest",
                        "paper", "ledger", "automation")
        ] == sorted([
            nav_markup.index(f'data-tab="{tab}"')
            for tab in ("market", "news", "stock-analysis", "candidates", "decision", "lab", "backtest",
                        "paper", "ledger", "automation")
        ])
        assert 'class="header-settings" data-tab="settings"' in resp.text
        assert resp.text.index('class="header-help"') < resp.text.index('class="header-settings"')
        settings_markup = resp.text.split('class="header-settings"', 1)[1].split("</button>", 1)[0]
        assert "header-settings-label" not in settings_markup
        assert settings_markup.count("<rect x=\"10.65\"") == 8
        assert client.get("/static/candidates.css").status_code == 200
        assert client.get("/static/candidates.js").status_code == 200
        assert client.get("/static/stock-analysis.css").status_code == 200
        assert client.get("/static/stock-analysis.js").status_code == 200
        assert client.get("/static/help.css").status_code == 200
        assert client.get("/static/help.js").status_code == 200
        brand_script = client.get("/static/brand-intro.js")
        brand_styles = client.get("/static/brand-intro.css")
        brand_mark = client.get("/static/brand/quantmaster-mark.svg")
        brand_logo = client.get("/static/brand/quantmaster-logo.svg")
        assert brand_script.status_code == 200
        assert brand_styles.status_code == 200
        assert brand_mark.status_code == 200
        assert brand_logo.status_code == 200
        assert "prefers-reduced-motion: reduce" in brand_styles.text
        assert "window.__recording === true" in brand_script.text
        assert "window.QuantMasterBrandIntro" in brand_script.text
        chart_script = client.get("/static/charts.js")
        chart_styles = client.get("/static/charts.css")
        assert chart_script.status_code == 200
        assert chart_styles.status_code == 200
        assert "function motionProfile(kind, count)" in chart_script.text
        assert "count > 1000" in chart_script.text
        assert "count > 240" in chart_script.text
        assert "count > 60" in chart_script.text
        assert "function replayChart(id)" in chart_script.text
        assert "window.ResizeObserver" in chart_script.text
        assert "prefers-reduced-motion: reduce" in chart_styles.text
        assert "--chart-primary" in chart_styles.text
        help_content = client.get("/static/help-content.html")
        assert help_content.status_code == 200
        assert 'data-help-topic="start"' in help_content.text
        assert 'data-help-topic="checklist"' in help_content.text
        assert help_content.text.count('data-calculator="') == 6
        assert help_content.text.count('data-lab="') == 4
        assert "2026-07-28" in help_content.text

    def test_help_handbook_structure_and_examples(self):
        text = client.get("/static/help-content.html").text
        topics = re.findall(r'data-help-topic="([^"]+)"', text)
        assert topics == [
            "start", "market", "trading", "data", "mathematics", "statistics",
            "inference", "asset-pricing", "numerical-pricing", "derivatives",
            "fixed-income", "factors", "validation", "composition", "backtest",
            "risk", "machine-learning", "research-protocol", "models",
            "workflow", "checklist", "calculators",
        ]
        assert len(topics[:-1]) == 21
        assert re.findall(r'data-help-part-intro="([^"]+)"', text) == [
            "market", "mathematics", "pricing", "signals", "portfolio",
            "production",
        ]
        assert re.findall(r'data-help-nav-part="([^"]+)"', text) == [
            "market", "mathematics", "pricing", "signals", "portfolio",
            "production",
        ]
        chapter_parts = re.findall(
            r'class="help-chapter(?: [^"]+)?"[^>]+data-help-part="([^"]+)"', text
        )
        assert chapter_parts == [
            *("market" for _ in range(4)),
            *("mathematics" for _ in range(3)),
            *("pricing" for _ in range(4)),
            *("signals" for _ in range(2)),
            *("portfolio" for _ in range(4)),
            *("production" for _ in range(4)),
            "appendix",
        ]
        assert text.count("<details data-self-test>") == 42
        assert text.count('data-code-language="python"') == 11

        ids = re.findall(r'\bid="([^"]+)"', text)
        assert len(ids) == len(set(ids))
        search_units = re.findall(r'<[^>]+data-search-unit[^>]*>', text)
        assert search_units
        assert all(re.search(r'\bid="[^"]+"', tag) for tag in search_units)

        class VisibleText(HTMLParser):
            def __init__(self):
                super().__init__()
                self.parts = []

            def handle_data(self, data):
                self.parts.append(data)

        parser = VisibleText()
        parser.feed(text)
        assert len("".join("".join(parser.parts).split())) >= 45_000

        for topic, anchor in re.findall(r'href="#help/([^/"#]+)(?:/([^"#]+))?"', text):
            assert topic in topics
            if anchor:
                assert anchor in ids

        code_blocks = re.findall(
            r'<code data-code-language="python">(.*?)</code>', text, flags=re.DOTALL
        )
        assert len(code_blocks) == 11
        for block in code_blocks:
            code = html.unescape(block)
            ast.parse(code)
            run_process(
                [sys.executable, "-c", code], check=True, capture_output=True,
                text=True, timeout=10,
            )

    def test_validation_error_has_request_id(self):
        resp = client.post("/api/v1/research/decision/dashboard/stream", json={
            "universe": "demo", "start": "2023-01-01", "horizon": 99,
        })
        assert resp.status_code == 422
        assert resp.json()["error_id"] == resp.headers["X-Request-ID"]

    def test_api_contract_rejects_nonfinite_and_unknown_fields(self):
        nonfinite = client.post(
            "/api/v1/portfolio/ledger/trade",
            content=(
                b'{"date":"2024-01-02","symbol":"600519.SH","side":"buy",'
                b'"price":NaN,"shares":100}'
            ),
            headers={"Content-Type": "application/json"},
        )
        assert nonfinite.status_code == 422
        assert nonfinite.json()["problem"]["code"] == "request_validation_failed"

        unknown = client.post("/api/v1/research/data/plans", json={
            "start": "2024-01-02", "end": "2024-01-03", "assets": ["stock"],
            "unexpected": True,
        })
        assert unknown.status_code == 422
        assert unknown.json()["error_id"] == unknown.headers["X-Request-ID"]

    def test_backtest_partial_data_returns_confirmation_problem(self, monkeypatch):
        dates = pd.bdate_range("2026-06-01", periods=25)
        panel = {
            "open": pd.DataFrame({"A": range(10, 35)}, index=dates),
            "close": pd.DataFrame({"A": range(11, 36)}, index=dates),
        }
        monkeypatch.setattr(
            "quantmaster.data.universe.load_universe", lambda name: ["A", "B"],
        )
        monkeypatch.setattr("quantmaster.data.load_panel", lambda *args, **kwargs: panel)

        resp = client.post("/api/v1/backtest/run", json={
            "strategy": "factor", "factor": "mom_20d", "universe": "demo",
            "start": "2026-06-01", "top_n": 1,
        })

        assert resp.status_code == 409
        data = resp.json()
        assert data["problem"]["code"] == "partial_market_data"
        assert data["problem"]["blocking"] is True
        assert data["problem"]["can_continue"] is True
        assert data["data_quality"]["missing_symbols"] == ["B"]

    def test_stream_error_message_redacts_credentials(self):
        from quantmaster.server import app as app_module

        message = app_module._safe_client_error(
            RuntimeError("token=very-secret-value Bearer abcdef123456 sk-live-secret123"))
        assert "very-secret-value" not in message
        assert "abcdef123456" not in message
        assert "sk-live-secret123" not in message
        assert message.count("***") == 3

    def test_factors_list(self):
        resp = client.get("/api/v1/research/factors")
        assert resp.status_code == 200
        factors = resp.json()["factors"]
        assert any(f["name"] == "mom_20d" for f in factors)

    def test_selection_history_empty(self):
        resp = client.get("/api/v1/research/selection/history")
        assert resp.status_code == 200
        assert resp.json() == {"snapshots": []}

    def test_market_overview_emits_each_completed_item(self, monkeypatch):
        from quantmaster.server import app as app_module

        dates = pd.bdate_range("2026-07-20", periods=3)
        frame = pd.DataFrame({"close": [100.0, 101.0, 102.0]}, index=dates)
        monkeypatch.setattr("quantmaster.data.load_history", lambda *args, **kwargs: frame)
        monkeypatch.setattr(
            app_module,
            "_sync_reference_market",
            lambda symbols, start, end, refresh, store: (
                {symbol: frame for symbol in symbols},
                {},
            ),
        )
        events = []

        result = app_module._market_overview_data(
            "2026-07-01", lambda *args: events.append(args))
        partials = [args[3] for args in events if len(args) > 3 and args[3]]
        final_count = sum(len(items) for items in result["groups"].values())

        assert len(partials) == final_count
        assert all(partial["kind"] == "market_item" for partial in partials)
        assert all(partial["item"]["nav"] for partial in partials)

    def test_market_overview_includes_tech_focused_major_indexes(self):
        from quantmaster.server import app as app_module

        indexes = app_module._market_groups()["A股指数"]

        assert indexes["000688.SH"] == "科创50"
        assert indexes["000698.SH"] == "科创100"
        assert indexes["399006.SZ"] == "创业板指"
        assert indexes["399673.SZ"] == "创业板50"

    def test_market_overview_emits_local_cache_before_failed_sync(self, monkeypatch):
        from quantmaster.data.storage import BarStore
        from quantmaster.server import app as app_module

        symbol = "^GSPC.US"
        dates = pd.bdate_range("2026-07-20", periods=3)
        BarStore().put(symbol, pd.DataFrame({"close": [100.0, 101.0, 102.0]}, index=dates))
        monkeypatch.setattr(app_module, "_market_groups", lambda: {
            "全球市场": {symbol: "标普500"},
        })
        def failed_reference_sync(symbols, start, end, refresh, store):
            for candidate in symbols:
                store.mark_status(candidate, "stale")
            return ({}, {
                symbol: {
                    "error_code": "offline",
                    "message": "所有参考数据源离线",
                    "source_attempts": [],
                },
            })

        monkeypatch.setattr(app_module, "_sync_reference_market", failed_reference_sync)
        events = []
        result = app_module._market_overview_data(
            "2026-07-01", lambda *args: events.append(args))

        partials = [event[3] for event in events if len(event) > 3 and event[3]]
        assert partials[0]["stage"] == "cache"
        assert result["groups"]["全球市场"][0]["symbol"] == symbol
        assert result["groups"]["全球市场"][0]["cache_status"] == "stale"

    def test_market_overview_exposes_unavailable_reference_details(self, monkeypatch):
        from quantmaster.server import app as app_module

        symbol = "DX-Y.NYB.US"
        monkeypatch.setattr(app_module, "_personal_market_symbols", lambda: ({}, {}))
        monkeypatch.setattr(app_module, "_market_groups", lambda: {
            "商品与汇率": {symbol: "美元指数"},
        })
        monkeypatch.setattr(
            app_module,
            "_sync_reference_market",
            lambda symbols, start, end, refresh, store: ({}, {
                symbol: {
                    "error_code": "all_sources_unavailable",
                    "message": "Yahoo 正在限流",
                    "source_attempts": [{"source": "yfinance", "code": "429"}],
                },
            }),
        )

        result = app_module._market_overview_data("2026-07-01")

        assert result["groups"]["商品与汇率"] == []
        assert result["group_statuses"]["商品与汇率"]["unavailable"] == 1
        assert result["group_statuses"]["商品与汇率"]["issues"][0]["symbol"] == symbol
        assert result["unavailable_items"][0]["symbol"] == symbol
        assert result["unavailable_items"][0]["message"] == "Yahoo 正在限流"

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

        resp = client.post("/api/v1/research/decision/dashboard", json={
            "universe": "demo", "start": "2023-01-01", "top_n": 4,
            "horizon": 3, "save": True,
        })
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert set(data) == {
            "market", "selection", "history", "model_snapshot", "data_quality",
        }
        assert data["market"]["current"]["state"] in {
            "strong_up", "up", "range", "down", "strong_down"}
        assert len(data["market"]["sectors"]) == 2
        assert len(data["selection"]["picks"]) == 4
        assert data["selection"]["profile"] == "risk_adjusted"
        assert data["selection"]["model_version"].startswith("hybrid-v2:")
        assert data["model_snapshot"]["engine_version"] == "hybrid-v2"
        assert all(
            "probability_up" in pick and "component_scores" in pick
            for pick in data["selection"]["picks"]
        )
        assert all(pick["name"] == names[pick["symbol"]]
                   for pick in data["selection"]["picks"])
        assert data["history"][0]["signal_date"] == data["selection"]["signal_date"]

        streamed = client.post("/api/v1/research/decision/dashboard/stream", json={
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
        assert partial_kinds[-5:] == [
            "decision_market", "decision_sectors", "decision_policy",
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
        resp = client.post("/api/v1/portfolio/ledger/cashflow",
                           json={"date": "2024-01-01", "amount": 100000, "kind": "deposit"})
        assert resp.status_code == 200
        resp = client.post("/api/v1/portfolio/ledger/trade", json={
            "date": "2024-01-02", "symbol": "600519.SH", "side": "buy",
            "price": 100.0, "shares": 100, "fee": 5.0})
        assert resp.status_code == 200

        resp = client.get("/api/v1/portfolio/ledger/trades")
        assert len(resp.json()["trades"]) == 1

    def test_invalid_trade_rejected(self):
        resp = client.post("/api/v1/portfolio/ledger/trade", json={
            "date": "2024-01-02", "symbol": "600519.SH", "side": "hold",
            "price": 100.0, "shares": 100})
        assert resp.status_code == 422

    def test_bad_factor_expression_400(self):
        resp = client.post("/api/v1/research/factors/test",
                           json={"expression": "__import__('os')", "universe": "demo"})
        assert resp.status_code == 400

    def test_validate_bad_expression_400(self):
        resp = client.post("/api/v1/research/factors/validate",
                           json={"expression": "eval(close)", "split": "2024-01-01"})
        assert resp.status_code == 400

    def test_ledger_nav_empty(self):
        resp = client.get("/api/v1/portfolio/ledger/nav")
        assert resp.status_code == 200
        data = resp.json()
        assert data["dates"] == []
        assert data["excess_annual"] == 0.0
