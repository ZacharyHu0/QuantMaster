"""FastAPI 服务测试（只测不依赖外部网络的端点）。"""

import ast
import html
import inspect
import json
import re
import sys
from html.parser import HTMLParser

import pandas as pd
from fastapi.testclient import TestClient

from quantmaster import __version__
from quantmaster.backtest.metrics import RISK_FREE, TRADING_DAYS
from quantmaster.data.base import BarDataEnvelope, BarDataQuality
from quantmaster.data.universe import UniverseSnapshot
from quantmaster.release import RELEASE_DATE
from quantmaster.runtime.process import run_process
from quantmaster.server.app import app, liveness, readiness

client = TestClient(app)
_csrf = client.get("/api/v1/session").json()["csrf_token"]
client.headers["X-CSRF-Token"] = _csrf


def _verified_market_data(data, *, start="2023-01-01", end="2026-08-08", symbols=()):
    quality = BarDataQuality(
        status="verified",
        requested_start=start,
        requested_end=end,
        observed_start=start,
        observed_end=end,
        coverage_ratio=1.0,
        calendar_source="test-calendar",
        sources=("test-source",),
        timezone="Asia/Shanghai",
        adjustment="verified",
        requested_symbols=tuple(symbols),
        observed_symbols=tuple(symbols),
    )
    return BarDataEnvelope(
        data=data,
        quality=quality,
        provenance=({"source": "test-source", "evidence": "fixture"},),
    )


def _unavailable_market_data(data, *, symbols=()):
    return BarDataEnvelope(
        data=data,
        quality=BarDataQuality(
            status="unavailable",
            requested_start="2026-08-01",
            requested_end="2026-08-08",
            coverage_ratio=0.0,
            sources=("test-source",),
            issues=("all sources returned no rows",),
            partial=True,
            requested_symbols=tuple(symbols),
            missing_symbols=tuple(symbols),
        ),
        provenance=({"source": "test-source", "outcome": "empty"},),
    )


class TestBasics:
    def test_health(self):
        resp = client.get("/api/v1/diagnostics")
        assert resp.status_code == 200
        assert resp.json()["level"] in {"ok", "warning", "error"}
        assert resp.json()["checked_at"]
        assert isinstance(resp.json()["issues"], list)
        assert len(resp.headers["X-Request-ID"]) == 12
        assert client.get("/api/v1/health").status_code == 404

    def test_market_history_read_failure_is_service_unavailable(self, monkeypatch):
        def fail_load(*_args, **_kwargs):
            raise TypeError("Cannot compare tz-naive and tz-aware timestamps")

        monkeypatch.setattr("quantmaster.data.read_bars", fail_load)

        response = client.get("/api/v1/market/history/DX-Y.NYB.US")

        assert response.status_code == 503
        assert "行情暂不可用" in response.json()["detail"]

    def test_market_history_uses_rolling_default_and_stable_wire_shape(self, monkeypatch):
        calls = []
        frame = pd.DataFrame(
            {
                "open": [10.1234], "high": [11.2345], "low": [9.8765],
                "close": [10.9876], "volume": [1234.4],
            },
            index=pd.DatetimeIndex(["2026-08-07"]),
        )

        def fake_load(symbol, start, end, *, frequency):
            calls.append((symbol, start, end, frequency))
            return _verified_market_data(
                frame, start=start, end=end, symbols=(symbol,),
            )

        monkeypatch.setattr("quantmaster.data.read_bars", fake_load)
        response = client.get(
            "/api/v1/market/history/600519.SH?frequency=1d&end=2026-08-08",
        )

        assert response.status_code == 200
        assert calls == [("600519.SH", "2023-08-08", "2026-08-08", "1d")]
        payload = response.json()
        assert payload["symbol"] == "600519.SH"
        assert payload["frequency"] == "1d"
        assert payload["kline"] == [
            ["2026-08-07", 10.123, 10.988, 9.877, 11.235, 1234.0]
        ]
        assert payload["data_quality"]["status"] == "verified"
        assert payload["provenance"] == [
            {"source": "test-source", "evidence": "fixture"}
        ]

    def test_market_history_unavailable_preserves_truth_contract(self, monkeypatch):
        monkeypatch.setattr(
            "quantmaster.data.read_bars",
            lambda *_args, **_kwargs: _unavailable_market_data(
                pd.DataFrame(), symbols=("600519.SH",),
            ),
        )

        response = client.get(
            "/api/v1/market/history/600519.SH?start=2026-08-01&end=2026-08-08",
        )

        assert response.status_code == 503
        payload = response.json()
        assert payload["problem"]["code"] == "market_data_unavailable"
        assert payload["data_quality"]["status"] == "unavailable"
        assert payload["data_quality"]["missing_symbols"] == ["600519.SH"]
        assert payload["provenance"] == [
            {"source": "test-source", "outcome": "empty"}
        ]

    def test_liveness_is_store_free_and_diagnostics_are_separate(self, monkeypatch):
        # These probes must stay on the event loop: a synchronous FastAPI
        # handler queues behind the default threadpool under load, turning the
        # watchdog's constant-time probe into a false availability failure.
        assert inspect.iscoroutinefunction(liveness)
        assert inspect.iscoroutinefunction(readiness)
        monkeypatch.setattr(
            "quantmaster.server.problems.collect_health_report",
            lambda: (_ for _ in ()).throw(AssertionError("liveness must not probe stores")),
        )
        live = client.get("/api/v1/health/live")
        assert live.status_code == 200
        assert {
            key: live.json()[key]
            for key in ("status", "version", "release_date")
        } == {
            "status": "ok", "version": __version__, "release_date": RELEASE_DATE,
        }
        assert live.json()["web_threads"] >= 1
        assert live.json()["thread_status"] in {"ok", "warning"}
        ready = client.get("/api/v1/health/ready")
        assert ready.status_code == 200
        assert ready.json()["status"] == "ready"

    def test_readiness_does_not_create_a_cold_data_root(
        self, isolated_config, tmp_path, monkeypatch,
    ):
        """A health probe is a pure observation, never a hidden bootstrap."""
        cold_root = tmp_path / "cold-data-root"
        isolated_config.data.root = str(cold_root)
        assert not cold_root.exists()

        # The route must use the in-memory state installed by the controlled
        # configuration switch, not re-read config or create a directory.
        from quantmaster.config import set_config

        set_config(isolated_config)
        monkeypatch.setattr(
            "quantmaster.config.get_config",
            lambda: (_ for _ in ()).throw(
                AssertionError("readiness must use its cached configuration state")
            ),
        )

        ready = client.get("/api/v1/health/ready")

        assert ready.status_code == 200
        assert ready.json() == {
            "status": "not_ready",
            "version": __version__,
            "data_root": str(cold_root),
        }
        assert not cold_root.exists()

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
        assert '/static/app.css?rev=' in page.text
        assert "%%QM_APP_CSS_REV%%" not in page.text
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

    def test_manual_reload_endpoint_requires_supervisor_and_writes_trigger(
        self, monkeypatch, tmp_path,
    ):
        trigger = tmp_path / "reload.trigger"
        monkeypatch.delenv("QM_SERVER_RELOAD_WORKER", raising=False)
        monkeypatch.setenv("QM_SERVER_RELOAD_TRIGGER_PATH", str(trigger))
        unavailable = client.post("/api/v1/system/reload")
        assert unavailable.status_code == 409
        assert not trigger.exists()

        monkeypatch.setenv("QM_SERVER_RELOAD_WORKER", "1")
        accepted = client.post("/api/v1/system/reload")
        assert accepted.status_code == 202
        assert accepted.json()["accepted"] is True
        assert "FreeStockDB" in accepted.json()["message"]
        assert trigger.read_text(encoding="ascii").isdigit()

    def test_index_serves_html(self):
        resp = client.get("/")
        app_script = client.get("/static/app.js").text
        app_styles = client.get("/static/app.css").text
        settings_styles = client.get("/static/settings.css").text
        assert resp.status_code == 200
        assert "QuantMaster" in resp.text
        assert 'data-tab="decision"' in resp.text
        assert 'id="kline-frequency"' in resp.text
        assert 'class="panel market-detail-panel" id="kline-panel"' in resp.text
        assert 'id="major-indexes" data-market-group="A股指数"' in resp.text
        assert 'id="market-fear-greed"' in resp.text
        assert "{'A股指数':majorIndexes}" in app_script
        assert "主要指数区块已保留" in app_script
        assert "function marketChangeSeries" in app_script
        assert "function marketSparkParsedDate" in app_script
        assert "function marketSparkMonth" in app_script
        assert "function marketSparkOption" in app_script
        assert "categories[dataIndex]" in app_script
        assert "Number.isFinite(parsedValue)" in app_script
        assert "type:'category',data:categories,show:true" in app_script
        assert "id:'market-spark-latest'" in app_script
        assert "区间涨跌" in app_script
        assert "当日涨跌" in app_script
        assert "PERSONAL_MARKET_GROUP = '我的股票'" in app_script
        assert "market-section-title" in resp.text
        assert "mkt-memberships" in app_script
        assert "名称与代码 · 标注提及次数" in resp.text
        assert "/static/settings.css?rev=" in resp.text
        assert "/static/settings.js?rev=" in resp.text
        assert ".settings-diagnostic-grid" in settings_styles
        assert "align-items: start; margin-bottom: 24px" in settings_styles
        assert resp.text.count('class="automation-list-field"') == 2
        assert '.automation-list-field textarea { min-height: 168px; }' in settings_styles
        assert "%%QM_SETTINGS_CSS_REV%%" not in resp.text
        assert "%%QM_SETTINGS_JS_REV%%" not in resp.text
        assert "/static/news.css?rev=" in resp.text
        assert "/static/news.js?rev=" in resp.text
        for stylesheet in ("app", "lab", "after-close"):
            assert f"/static/{stylesheet}.css?rev=" in resp.text
        assert "queueMarketReload" in app_script
        assert "data:[{yAxis:0}]" in app_script
        assert "type:'dashed'" in app_script
        assert "prefers-reduced-motion" in app_styles
        assert "--scrollbar-track: #151514" in app_styles
        assert "scrollbar-color: var(--scrollbar-thumb) var(--scrollbar-track)" in app_styles
        assert "scrollbar-gutter:stable" in app_styles
        assert ".segmented {" in app_styles
        assert "overflow-x:auto; overflow-y:hidden" in app_styles
        assert "createLoadProgress" in app_script
        assert "createMarketStreamRenderer" in app_script
        assert "/api/v1/market/fear-greed" in app_script
        assert 'id="market-fear-greed-time"' in resp.text
        assert "function fearGreedAsOf" in app_script
        assert ".formatToParts(parsed)" in app_script
        assert "`${year}${Number(parts.month)}月${Number(parts.day)}日" in app_script
        assert "function fearGreedGaugeLabels" in app_script
        assert "[0,25,45,55,75,100].map" in app_script
        assert "font:'600 13px" in app_script
        assert 'class="eyebrow market-sentiment-title"' in resp.text
        assert 'class="market-sentiment-value"' not in resp.text
        assert "CNN 当日恐贪指数 ${scoreText}，${label}" in app_script
        assert ".market-sentiment-panel { display:grid; gap:var(--space-sm)" in app_styles
        assert ".fear-greed-gauge { width:100%; height:156px" in app_styles
        assert ".fear-greed-history { width:100%; height:142px" in app_styles
        assert "title:{show:true,offsetCenter:[0,'94%'],color:CHART_COLORS.ink2" in app_script
        assert "fontSize:14,fontWeight:600,lineHeight:16" in app_script
        assert "detail:{offsetCenter:[0,'42%']" in app_script
        assert "fontSize:30,fontWeight:720,lineHeight:32" in app_script
        assert "splitNumber:20" in app_script
        assert "valueAnimation:!REDUCED_MOTION" in app_script
        assert "animationDuration:REDUCED_MOTION ? 0 : 640" in app_script
        assert "duration:640,easing:'cubicInOut'" in app_script
        assert "formatter:'≤10 · 罕见恐惧'" in app_script
        assert "position:'insideStartTop',distance:6" in app_script
        assert "黄色虚线：CNN ≤10，属于罕见恐惧区间；分数越低越恐惧" in resp.text
        assert "黄色虚线表示 10 分罕见恐惧参考阈值" in resp.text
        assert "__qmMotion:true" in app_script
        assert "id:'fear-greed-dial'" in app_script
        assert "!chart.__qmFearGreedEntered" in app_script
        assert "function replayFearGreedGaugeAnimation" in app_script
        assert "control.dataset.marketPage === 'quotes'" in app_script
        assert "control.getAttribute('aria-selected') !== 'true'" in app_script
        assert "replayFearGreedGaugeAnimation(view)" in app_script
        assert "chart.__qmFearGreedAnimationRevision !== animationRevision" in app_script
        assert "marketFearGreed,width,height,0" in app_script
        assert "target.animationDurationUpdate = 640" in app_script
        assert "target.animationEasingUpdate = 'cubicInOut'" in app_script
        assert "function fearGreedGaugeNeedle" in app_script
        assert "function fearGreedGaugeRotation" in app_script
        assert "210 - Math.max(0,Math.min(100,value)) * 2.4" in app_script
        assert "rotation:fearGreedGaugeRotation(0)" in app_script
        assert "id:'fear-greed-needle',type:'group'" in app_script
        assert "pointer:{show:false}" in app_script
        assert "keyframes:[" in app_script
        assert "undefined,true" in app_script
        assert "var explicitMotion = option.__qmMotion === true" in client.get("/static/charts.js").text
        assert "data-opportunity-rsi" in app_script
        assert 'class="mkt-rsi-label"><span>RSI(14)</span><small>日线</small>' in app_script
        assert "function rsiSparkPoints" in app_script
        assert "function rsiSparkMarkup" in app_script
        assert "function bindRsiSparkInteraction" in app_script
        assert "getUTCMonth() - 3" in app_script
        assert "mkt-rsi-date-label" in app_script
        assert "mkt-rsi-tooltip" in app_script
        assert "root.onpointermove" in app_script
        assert "dotRadius * width / bounds.width" in app_script
        assert "dotRadius * chartHeight / bounds.height" in app_script
        assert "createDecisionStreamRenderer" in app_script
        assert "function loadKlineSeries" in app_script
        assert "function renderKlineSeries" in app_script
        assert "const KLINE_CACHE_LIMIT = 64" in app_script
        assert "KLINE_DAILY_TTL_MS = 5 * 60 * 1000" in app_script
        assert "panel.scrollIntoView({behavior:'auto',block:'start'})" in app_script
        assert "previousController?.abort()" in app_script
        assert "const klineSeriesInflight = new Map()" in app_script
        assert "new IntersectionObserver" in app_script
        assert "requestIdleCallback" in app_script
        assert "2023-01-01" not in app_script.split("function klineStartDate", 1)[1].split("function", 1)[0]
        assert "id:'market-kline-x-wheel'" in app_script
        assert "id:'market-kline-x-slider'" in app_script
        assert "xAxisIndex:[0,1],filterMode:'none'" in app_script
        assert "function openDecisionKline" in app_script
        assert "data-decision-kline-trigger" in app_script
        assert 'data-decision-detail="${esc(decisionKlineState.symbol)}"><td colspan="10">' in app_script
        assert "data-decision-asset-toggle" in app_script
        assert "showKline(row.dataset.symbol" not in app_script
        assert 'src="/static/charts.js"' in resp.text
        assert 'href="/static/charts.css"' in resp.text
        assert 'href="/static/brand-intro.css"' in resp.text
        assert 'src="/static/brand-intro.js"' in resp.text
        assert 'href="/static/brand/quantmaster-favicon.svg"' in resp.text
        assert 'src="/static/brand/quantmaster-mark-inverse.svg"' in resp.text
        assert 'option value="swing"' not in resp.text
        assert 'id="brand-replay"' in resp.text
        assert "window.QuantCharts.activateTab(tab)" in app_script
        assert "ACTIVE_TAB_STORAGE_KEY" in app_script
        assert "sessionStorage.getItem(ACTIVE_TAB_STORAGE_KEY)" in app_script
        assert "activateTab(restoredControl, {persist:false, load:false})" in app_script
        assert 'class="snapshot-table"' in app_script
        assert 'class="snapshot-period"' in app_script
        assert 'class="snapshot-pick"' in app_script
        assert "function decisionFollowUpMarkup" in app_script
        assert "function decisionSnapshotPicksMarkup" in app_script
        assert "function toggleDecisionSnapshotRow" in app_script
        assert 'class="snapshot-record-row"' in app_script
        assert 'class="snapshot-detail-row"' in app_script
        assert "e.target.closest('[data-snapshot-toggle]')" in app_script
        assert "前三目标持仓" in app_script
        assert "股价变动验证" in app_script
        assert 'colspan="5"' in app_script
        assert ".snapshot-progress {" in app_styles
        assert '.snapshot-validation[data-status="completed"]' in app_styles
        assert "event.partial" in app_script
        assert "/api/v1/research/decision/dashboard/stream" in app_script
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
        assert 'data-regime-window="10y"' in app_script
        assert "名称 / 代码 / 板块" in app_script
        assert 'id="runtime-info"' in resp.text
        assert 'id="data-refresh-preview"' in resp.text
        assert 'id="data-refresh-start-button"' in resp.text
        assert 'id="data-refresh-resume"' in resp.text
        assert "同步观察行情" in resp.text
        assert "不更新全市场 free-stockdb" in resp.text
        assert "自动更新本地库" in resp.text
        assert "立即更新并验证" in resp.text
        assert resp.text.count('class="settings-nav-group"') == 5
        assert resp.text.count("data-settings-section=") == 11
        for panel in ("local-data", "online-data", "research-data"):
            assert f'data-settings-panel="{panel}"' in resp.text
        assert 'id="research-artifacts"' in resp.text
        assert "版本化研究产物" in resp.text
        assert "普通看盘、候选扫描和常规回测无需操作" in resp.text
        assert "跨资产研究生产湖" not in resp.text
        for diagnostic in (
            "llm-models", "llm-web-search", "storage", "data-sources", "tushare", "lab", "server",
        ):
            assert f'data-diagnostic="{diagnostic}"' in resp.text
        assert 'id="settings-section-select"' in resp.text
        assert 'id="runtime-drawer-frame"' in resp.text
        assert "window.QuantMasterRunInfo" in app_script
        assert "window.QuantMasterProblemDialog" in app_script
        assert "window.QuantMasterNDJSON" in app_script
        assert "runtimeInfo.sync('health'" in app_script
        assert "function ingestDataQuality" in app_script
        assert "function dataProvenanceSummary" in app_script
        assert "Object.entries(quality.units)" in app_script
        assert "if (data.data_quality) ingestDataQuality(data" in app_script
        assert "数据来源与口径已验证" in app_script
        assert "证据链" in app_script
        assert "使用降级数据继续计算" in app_script
        assert "effective_as_of || quality.observed_end" in app_script
        assert 'id="operation-problem-dialog"' in resp.text
        assert 'data-runtime-filter="problem"' in resp.text
        assert 'data-runtime-filter="running"' in resp.text
        assert '<summary>诊断信息</summary>' in app_script
        assert '<dt>远端失败</dt>' in app_script
        assert '<dt>本地拦截</dt>' in app_script
        assert "remoteFailures:problem.remote_failures" in app_script
        assert "runtimeInfo.begin(source, '正在加载数据'" in app_script
        assert "if (safeLevel === 'error') setExpanded(true)" not in app_script
        assert "window.QuantMasterAPI" in app_script
        assert "unhandledrejection" in app_script
        assert 'id="release-trigger"' in resp.text
        assert 'id="release-popover"' in resp.text
        assert 'id="release-reload-button"' in resp.text
        assert 'id="release-reload-status"' in resp.text
        assert 'id="stockdb-update-trigger"' in resp.text
        assert 'id="stockdb-update-popover"' in resp.text
        assert 'id="stockdb-data-date"' in resp.text
        assert 'id="stockdb-updated-at"' not in resp.text
        assert 'id="stockdb-popover-updated-at"' in resp.text
        assert 'id="free-stockdb-release"' in resp.text
        release_popover = resp.text.split(
            '<aside class="release-popover" id="release-popover"', 1,
        )[1].split("</aside>", 1)[0]
        stockdb_popover = resp.text.split(
            '<aside class="release-popover stockdb-update-popover" '
            'id="stockdb-update-popover"', 1,
        )[1].split("</aside>", 1)[0]
        assert 'id="free-stockdb-release"' not in release_popover
        assert 'id="free-stockdb-release"' in stockdb_popover
        assert 'id="stockdb-popover-session"' in stockdb_popover
        assert '/api/v1/settings/free-stockdb/vendor-notice' in app_script
        assert "/api/v1/system/reload" in app_script
        assert "api('/api/v1/settings/free-stockdb')" in app_script
        assert 'qm-free-stockdb-release-seen' in app_script
        assert f'v{__version__}' not in resp.text  # 版本由 data 属性无闪烁注入，脚本负责呈现
        assert f'data-version="{__version__}"' in resp.text
        assert f'data-release-date="{RELEASE_DATE}"' in resp.text
        assert "%%QM_VERSION%%" not in resp.text
        assert "%%QM_TRADING_DAYS%%" not in resp.text
        assert "%%QM_RISK_FREE%%" not in resp.text
        nav_markup = resp.text.split('<nav id="nav"', 1)[1].split("</nav>", 1)[0]
        assert 'data-tab="settings"' not in nav_markup
        assert [nav_markup.index(f'data-workspace="{workspace}"') for workspace in (
            "observe", "select", "research", "trade", "automation",
        )] == sorted([
            nav_markup.index(f'data-workspace="{workspace}"') for workspace in (
                "observe", "select", "research", "trade", "automation",
            )
        ])
        assert (
            'data-workspace="automation" data-workspace-page="automation" data-tab="automation"'
            in nav_markup
        )
        assert 'data-workspace-pages="automation"' not in resp.text
        workspace_pages = (
            "quotes", "temperature", "style", "rotation", "industry", "themes", "etfs", "news",
            "after-close", "candidates", "stock-analysis", "decision", "lab", "backtest", "paper",
            "ledger",
        )
        assert [resp.text.index(f'data-workspace-page="{page}"') for page in workspace_pages] == sorted(
            resp.text.index(f'data-workspace-page="{page}"') for page in workspace_pages
        )
        assert 'class="header-settings" data-tab="settings"' in resp.text
        assert resp.text.index('class="header-help"') < resp.text.index('class="header-settings"')
        help_markup = resp.text.split('class="header-help"', 1)[1].split("</button>", 1)[0]
        assert 'aria-label="手册"' in help_markup
        assert "<span>手册</span>" in help_markup
        assert "<circle" not in help_markup
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
        settings_script = client.get("/static/settings.js")
        after_close_script = client.get("/static/after-close.js")
        after_close_styles = client.get("/static/after-close.css")
        assert chart_script.status_code == 200
        assert chart_styles.status_code == 200
        assert settings_script.status_code == 200
        assert after_close_script.status_code == 200
        assert after_close_styles.status_code == 200
        assert "function motionProfile(kind, count)" in chart_script.text
        assert "count > 1000" in chart_script.text
        assert "count > 240" in chart_script.text
        assert "count > 60" in chart_script.text
        assert "function replayChart(id)" in chart_script.text
        assert "data-chart-replay" not in chart_script.text
        assert "qm-chart-replay" not in chart_styles.text
        assert "window.ResizeObserver" in chart_script.text
        assert "prefers-reduced-motion: reduce" in chart_styles.text
        assert "--chart-primary" in chart_styles.text
        assert "scheduleFreeStockDbPoll" in settings_script.text
        assert "freeStockDbPollFailures < 5" in settings_script.text
        assert "['failed', 'manual_required'].includes(stockdb.update_result)" in settings_script.text
        assert "stockdb.target_session" in settings_script.text
        assert "succeededWithWarnings" in after_close_script.text
        assert '[data-tone="warning"]' in after_close_styles.text
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

    def test_legacy_synchronous_backtest_route_is_removed(self):
        resp = client.post("/api/v1/backtest/run", json={})
        assert resp.status_code == 404

    def test_stream_error_message_redacts_credentials(self):
        from quantmaster.server import app as app_module

        message = app_module._safe_client_error(
            RuntimeError("token=very-secret-value Bearer abcdef123456 sk-live-secret123"))
        assert "very-secret-value" not in message
        assert "abcdef123456" not in message
        assert "sk-live-secret123" not in message
        assert message.count("***") == 3

    def test_research_failures_do_not_expose_exception_details(self, monkeypatch):
        secret = "token=server-secret-value"

        def fail_with_secret(*_args, **_kwargs):
            raise RuntimeError(secret)

        monkeypatch.setattr(
            "quantmaster.data.universe.load_universe_analysis_snapshot",
            fail_with_secret,
        )
        cases = (
            ("/api/v1/market/regime", {}, "市场状态分析失败，请检查请求参数后重试。"),
            (
                "/api/v1/research/selection/daily",
                {},
                "每日选股分析失败，请检查请求参数后重试。",
            ),
            (
                "/api/v1/research/decision/dashboard",
                {},
                "决策工作台计算失败，请检查请求参数后重试。",
            ),
            (
                "/api/v1/research/factors/test",
                {"expression": "mom_20d"},
                "因子检验失败，请检查请求参数后重试。",
            ),
        )

        for path, payload, public_detail in cases:
            response = client.post(path, json=payload)
            assert response.status_code == 400
            assert response.json()["detail"] == public_detail
            assert secret not in response.text

    def test_factors_list(self):
        resp = client.get("/api/v1/research/factors")
        assert resp.status_code == 200
        factors = resp.json()["factors"]
        assert any(f["name"] == "mom_20d" for f in factors)

    def test_selection_history_empty(self):
        resp = client.get("/api/v1/research/selection/history")
        assert resp.status_code == 200
        assert resp.json() == {"snapshots": []}

    def test_selection_history_accepts_integer_horizon_from_query_string(self):
        resp = client.get(
            "/api/v1/research/selection/history",
            params={
                "universe": "demo", "profile": "risk_adjusted",
                "horizon": "3", "limit": "10",
            },
        )
        assert resp.status_code == 200
        assert resp.json() == {"snapshots": []}

        invalid = client.get(
            "/api/v1/research/selection/history", params={"horizon": "2"},
        )
        assert invalid.status_code == 422

    def test_page_reads_never_bootstrap_news_or_portfolio_databases(self, isolated_config):
        """A cold page request is a bounded local read, never a schema bootstrap."""
        data_root = isolated_config.data_root
        news_db = data_root / "news.sqlite"
        assets_db = data_root / "asset_lists.sqlite"
        ledger_db = data_root / "ledger_default.sqlite"
        decisions_db = data_root / "decisions.sqlite"
        assert not news_db.exists()
        assert not assets_db.exists()
        assert not ledger_db.exists()
        assert not decisions_db.exists()

        news = client.get("/api/v1/news")
        assert news.status_code == 503
        assert news.json()["problem"]["code"] == "snapshot_unavailable"
        assert not news_db.exists()

        assets = client.get("/api/v1/portfolio/lists")
        assert assets.status_code == 200
        assert assets.json()["favorites"] == []
        assert assets.json()["following"] == []
        assert assets.json()["holdings"] == []
        assert not assets_db.exists()
        assert not ledger_db.exists()

        trades = client.get("/api/v1/portfolio/ledger/trades")
        assert trades.status_code == 200
        assert trades.json() == {"trades": []}
        assert not ledger_db.exists()

        decisions = client.get("/api/v1/research/selection/history")
        assert decisions.status_code == 200
        assert decisions.json() == {"snapshots": []}
        assert not decisions_db.exists()

    def test_decision_follow_up_uses_t1_open_and_freezes_at_horizon(self):
        from quantmaster.decision import decision_follow_up

        dates = pd.to_datetime([
            "2026-08-04", "2026-08-05", "2026-08-06", "2026-08-07",
        ])
        snapshot = {
            "signal_date": "2026-08-03",
            "holding_horizon_days": 3,
            "picks": [{"rank": 1, "symbol": "600000.SH", "name": "浦发银行"}],
        }
        bars = pd.DataFrame({
            "open": [10.0, 10.8, 11.4, 20.0],
            "close": [10.5, 11.0, 12.0, 25.0],
        }, index=dates)

        validation = decision_follow_up(snapshot, {"600000.SH": bars})

        assert validation["status"] == "completed"
        assert validation["completed_sessions"] == 3
        assert validation["entry_date"] == "2026-08-04"
        assert validation["evaluation_date"] == "2026-08-06"
        assert validation["data_as_of_date"] == "2026-08-07"
        assert validation["picks"][0]["entry_price"] == 10.0
        assert validation["picks"][0]["price"] == 12.0
        assert validation["picks"][0]["return"] == 0.2

    def test_decision_follow_up_uses_only_positive_target_weights(self):
        from quantmaster.decision import decision_follow_up

        dates = pd.to_datetime(["2026-08-04", "2026-08-05"])
        snapshot = {
            "signal_date": "2026-08-03",
            "holding_horizon_days": 2,
            "position_state": "invested",
            "picks": [
                {"rank": 1, "symbol": "A", "target_weight": 0.75},
                {"rank": 2, "symbol": "B", "target_weight": 0.25},
                {"rank": 3, "symbol": "WATCH", "target_weight": 0.0},
            ],
        }
        prices = {
            "A": pd.DataFrame({"open": [10, 11], "close": [11, 12]}, index=dates),
            "B": pd.DataFrame({"open": [20, 19], "close": [19, 18]}, index=dates),
            "WATCH": pd.DataFrame({"open": [1, 1], "close": [10, 20]}, index=dates),
        }

        validation = decision_follow_up(snapshot, prices)

        assert [pick["symbol"] for pick in validation["picks"]] == ["A", "B"]
        assert validation["average_return"] == 0.125

    def test_decision_follow_up_marks_intentional_flat_without_validation(self):
        from quantmaster.decision import decision_follow_up

        validation = decision_follow_up(
            {
                "signal_date": "2026-08-03",
                "holding_horizon_days": 3,
                "position_state": "flat",
                "picks": [{"symbol": "WATCH", "target_weight": 0.0}],
            },
            {},
        )

        assert validation["status"] == "flat"
        assert validation["method"] == "no_position_validation"
        assert validation["picks"] == []

    def test_selection_history_adds_in_progress_top3_validation(self):
        from quantmaster.data.storage import BarStore
        from quantmaster.decision import DecisionStore

        symbols = ["600000.SH", "000001.SZ", "600001.SH"]
        report = {
            "signal_date": "2026-08-03",
            "holding_horizon_days": 3,
            "profile": "risk_adjusted",
            "policy_hash": "policy",
            "model_version": "hybrid-v2:test",
            "recommended_exposure": 0.6,
            "picks": [
                {"rank": rank, "symbol": symbol, "name": f"股票{rank}"}
                for rank, symbol in enumerate(symbols, start=1)
            ],
        }
        DecisionStore().save(
            report,
            "demo",
            panel={
                "close": pd.DataFrame(
                    [[10.0]],
                    index=pd.to_datetime(["2026-08-03"]),
                    columns=[symbols[0]],
                ),
            },
        )
        dates = pd.to_datetime(["2026-08-03", "2026-08-04", "2026-08-05"])
        closes = ([9.8, 10.5, 12.0], [20.1, 19.0, 18.0], [30.0, 30.3, 30.0])
        opens = ([9.7, 10.0, 11.0], [20.2, 20.0, 19.0], [30.1, 30.0, 30.2])
        store = BarStore()
        for symbol, open_values, close_values in zip(symbols, opens, closes, strict=True):
            store.put(symbol, pd.DataFrame({
                "open": open_values,
                "high": [max(a, b) for a, b in zip(open_values, close_values, strict=True)],
                "low": [min(a, b) for a, b in zip(open_values, close_values, strict=True)],
                "close": close_values,
                "volume": [1_000_000.0] * 3,
            }, index=dates))

        response = client.get("/api/v1/research/selection/history", params={
            "universe": "demo", "profile": "risk_adjusted", "horizon": 3,
        })

        assert response.status_code == 200
        validation = response.json()["snapshots"][0]["follow_up_validation"]
        assert validation["status"] == "in_progress"
        assert validation["completed_sessions"] == 2
        assert validation["progress"] == 0.6667
        assert [item["return"] for item in validation["picks"]] == [0.2, -0.1, 0.0]
        assert validation["average_return"] == 0.033333

    def test_selection_history_marks_untrusted_legacy_snapshot_as_preview(self):
        from quantmaster.decision import DecisionStore

        report = {
            "signal_date": "2026-08-03",
            "holding_horizon_days": 3,
            "profile": "risk_adjusted",
            "policy_hash": "legacy-preview",
            "model_version": "hybrid-v2:test",
            "picks": [],
        }
        store = DecisionStore()
        store.save(
            report,
            "demo",
            panel={
                "close": pd.DataFrame(
                    [[10.0]], index=pd.to_datetime(["2026-08-03"]), columns=["600000.SH"],
                ),
            },
        )
        with store._conn() as connection:
            connection.execute("UPDATE selection_snapshots SET payload_sha256='' ")

        response = client.get("/api/v1/research/selection/history", params={
            "universe": "demo", "profile": "risk_adjusted", "horizon": 3,
        })

        assert response.status_code == 200, response.text
        snapshot = response.json()["snapshots"][0]
        assert snapshot["snapshot"]["state"] == "degraded"
        assert snapshot["eligibility"]["preview_allowed"] is True
        assert snapshot["eligibility"]["formal_allowed"] is False

    def test_market_overview_emits_each_completed_item(self, monkeypatch):
        from quantmaster.server import app as app_module

        dates = pd.bdate_range("2026-07-20", periods=3)
        frame = pd.DataFrame({"close": [100.0, 101.0, 102.0]}, index=dates)
        monkeypatch.setattr(
            "quantmaster.data.refresh_history",
            lambda *args, **kwargs: _verified_market_data(
                frame,
                start=str(dates[0].date()),
                end=str(dates[-1].date()),
                symbols=(str(args[0]),),
            ),
        )
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

    def test_market_overview_route_reads_only_a_published_snapshot(self, monkeypatch, isolated_config):
        from quantmaster.market import overview_snapshot
        from quantmaster.runtime.derived import DerivedArtifactCatalog
        from quantmaster.server import app as app_module

        payload = {
            "meta": {"as_of": "2026-08-10", "stale": False, "stale_reasons": []},
            "groups": {"A股指数": []},
            "data_quality": {"status": "verified", "issues": []},
        }
        root = isolated_config.data_root / "market-test-derived"

        def catalog_factory(*_args, **kwargs):
            return DerivedArtifactCatalog(root, read_only=bool(kwargs.get("read_only")))

        monkeypatch.setattr(overview_snapshot, "DerivedArtifactCatalog", catalog_factory)
        monkeypatch.setattr(app_module, "_market_overview_data", lambda **_kwargs: payload)
        published = overview_snapshot.publish_market_overview_snapshot()
        assert published["id"]
        cached = overview_snapshot.read_market_overview_snapshot()
        wire_payload, wire = overview_snapshot.read_market_overview_snapshot_wire()
        assert cached["snapshot"]["id"] == published["id"]
        assert wire_payload is cached
        assert wire.startswith(b'{"data":')

        def must_not_rebuild(**_kwargs):
            raise AssertionError("页面 GET 不得扫描 BarStore 或重建市场快照")

        monkeypatch.setattr(app_module, "_market_overview_data", must_not_rebuild)
        response = TestClient(app).get("/api/v1/market/overview")

        assert response.status_code == 200, response.text
        body = response.json()
        assert body["data"] == payload
        assert body["snapshot"]["id"] == published["id"]
        assert body["snapshot"]["state"] == "fresh"
        assert response.headers["etag"]
        assert response.headers["content-type"].startswith("application/json")

    def test_market_overview_reports_structured_cold_snapshot_state(self, monkeypatch, isolated_config):
        from quantmaster.market import overview_snapshot
        from quantmaster.runtime.derived import DerivedArtifactCatalog

        root = isolated_config.data_root / "market-cold-derived"
        monkeypatch.setattr(
            overview_snapshot,
            "DerivedArtifactCatalog",
            lambda *_args, **kwargs: DerivedArtifactCatalog(
                root, read_only=bool(kwargs.get("read_only")),
            ),
        )

        response = TestClient(app).get("/api/v1/market/overview")

        assert response.status_code == 503
        assert response.json()["problem"]["code"] == "snapshot_unavailable"

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
        universe_snapshot = UniverseSnapshot(
            name="demo",
            symbols=tuple(symbols),
            observed_at="2023-01-01T00:00:00+00:00",
            effective_as_of="2023-01-01",
            content_hash="fixture-universe",
            source="fixture",
        )
        monkeypatch.setattr(
            "quantmaster.data.universe.load_universe_analysis_snapshot",
            lambda _name, **_kwargs: universe_snapshot,
        )
        def load_panel_with_progress(*args, **kwargs):
            callback = kwargs.get("progress")
            if callback:
                for index, symbol in enumerate(symbols, start=1):
                    callback(index, len(symbols), symbol, True)
            return _verified_market_data(
                panel,
                start=str(args[1]),
                end=str(args[2]),
                symbols=tuple(args[0]),
            )

        monkeypatch.setattr("quantmaster.data.read_panel", load_panel_with_progress)
        monkeypatch.setattr("quantmaster.data.read_stock_names", lambda values: names)
        monkeypatch.setattr(
            "quantmaster.data.industry.load_industry_analysis_context",
            lambda **_kwargs: (mapping, {
                "status": "verified", "content_hash": "fixture-industry",
                "formal_eligible": True, "issues": [],
            }),
        )

        resp = client.post("/api/v1/research/decision/dashboard", json={
            "universe": "demo", "start": "2023-01-01", "top_n": 4,
            "horizon": 3, "save": True,
        })
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert set(data) == {
            "market", "selection", "history", "model_snapshot", "data_quality",
            "calculation_quality", "provenance", "persistence",
            "universe_evidence", "industry_evidence",
        }
        assert data["market"]["current"]["state"] in {
            "strong_up", "up", "range", "down", "strong_down"}
        assert len(data["market"]["sectors"]) == 2
        assert len(data["selection"]["picks"]) == 4
        assert data["selection"]["profile"] == "risk_adjusted"
        assert data["selection"]["model_version"].startswith("hybrid-v3:")
        assert {
            "market_base_exposure", "opportunity_scale", "recommended_exposure",
            "cash_weight", "qualified_count", "position_state", "position_reasons",
        } <= data["selection"].keys()
        assert all(
            {"target_weight", "allocation_strength", "allocation_components"} <= pick.keys()
            for pick in data["selection"]["picks"]
        )
        assert data["model_snapshot"]["engine_version"] == "hybrid-v3-position-control"
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

    def test_decision_stream_unavailable_preserves_truth_contract(self, monkeypatch):
        symbols = ["600000.SH", "000001.SZ"]
        monkeypatch.setattr(
            "quantmaster.data.universe.load_universe_analysis_snapshot",
            lambda _name, **_kwargs: UniverseSnapshot(
                name="demo",
                symbols=tuple(symbols),
                observed_at="2026-08-01T00:00:00+00:00",
                effective_as_of="2026-08-01",
                content_hash="fixture-universe",
                source="fixture",
            ),
        )
        monkeypatch.setattr(
            "quantmaster.data.read_panel",
            lambda *_args, **_kwargs: _unavailable_market_data(
                {}, symbols=tuple(symbols),
            ),
        )

        response = client.post(
            "/api/v1/research/decision/dashboard/stream",
            json={"universe": "demo", "start": "2026-08-01", "save": True},
        )

        assert response.status_code == 200
        events = [json.loads(line) for line in response.text.splitlines() if line]
        error = next(event for event in events if event["type"] == "error")
        assert error["problem"]["code"] == "market_data_unavailable"
        assert error["data_quality"]["status"] == "unavailable"
        assert error["data_quality"]["missing_symbols"] == symbols
        assert error["provenance"] == [
            {"source": "test-source", "outcome": "empty"}
        ]


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
