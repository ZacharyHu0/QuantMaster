"""FastAPI 服务测试（只测不依赖外部网络的端点）。"""

import ast
import html
import inspect
import json
import re
import sqlite3
import sys
from html.parser import HTMLParser
from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient

from quantmaster import __version__
from quantmaster.backtest.metrics import RISK_FREE, TRADING_DAYS
from quantmaster.data.base import BarDataEnvelope, BarDataQuality
from quantmaster.data.universe import UniverseSnapshot
from quantmaster.release import RELEASE_DATE
from quantmaster.runtime.process import run_process
from quantmaster.server.app import app, liveness

client = TestClient(app)
_csrf = client.get("/api/v1/session").json()["csrf_token"]
client.headers["X-CSRF-Token"] = _csrf

STATIC_ROOT = Path(__file__).resolve().parents[1] / "quantmaster" / "server" / "static"


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
    def test_workspace_shell_uses_native_lazy_modules_within_raw_budgets(self):
        page = client.get("/").text
        initial_urls = re.findall(
            r'<(?:script[^>]+src|link[^>]+href)="([^"?]+)', page,
        )
        initial_names = {
            Path(url).name for url in initial_urls
            if url.startswith("/static/") and Path(url).suffix in {".css", ".js"}
        }

        assert initial_names == {
            "brand-intro.css", "brand-intro.js", "app.css", "app.js", "data-state.js",
            "ink-theme.css", "theme.js", "workspace-loader.js",
        }
        assert '<script type="module" src="/static/workspace-loader.js"></script>' in page
        assert all(
            name not in initial_names
            for name in ("echarts.min.js", "help.js", "lab.js", "rotation.js")
        )

        workspace_root = STATIC_ROOT / "workspaces"
        adapters = sorted(workspace_root.glob("*.js"))
        assert [path.stem for path in adapters] == ["account", "research", "runtime", "today"]
        for adapter in adapters:
            source = adapter.read_text(encoding="utf-8")
            assert re.search(r"export\s+(?:async\s+)?function\s+mount\b", source)
            assert re.search(r"export\s+(?:async\s+)?function\s+unmount\b", source)
            assert re.search(r"export\s+(?:async\s+)?function\s+refresh\b", source)
            assert "window." not in source
            assert not re.search(
                r"(?<![.\w])(loadMarket|loadAssetLists|loadDecisionHistory|loadLedger)\s*\(",
                source,
            )

        loader = (STATIC_ROOT / "workspace-loader.js").read_text(encoding="utf-8")
        assert loader.count("import(") == 4
        assert all(
            f"./workspaces/{name}.js" in loader
            for name in ("today", "research", "account", "runtime")
        )
        assert "Object.freeze" in loader

        app_source = (STATIC_ROOT / "app.js").read_text(encoding="utf-8")
        assert "window.QuantMasterShell = Object.freeze" in app_source

        initial_paths = [
            STATIC_ROOT / Path(url).name for url in initial_urls
            if url.startswith("/static/") and Path(url).suffix in {".css", ".js"}
        ]
        attribution = {
            path.relative_to(STATIC_ROOT).as_posix(): path.stat().st_size
            for path in initial_paths
        }
        attribution["index.html"] = len(page.encode("utf-8"))
        assert sum(attribution.values()) <= 1024 * 1024, attribution

    def test_health(self):
        resp = client.get("/api/v1/diagnostics")
        assert resp.status_code == 200
        assert resp.json()["level"] in {"ok", "warning", "error"}
        assert resp.json()["checked_at"]
        assert isinstance(resp.json()["issues"], list)
        assert len(resp.headers["X-Request-ID"]) == 12
        health = client.get("/api/v1/health")
        assert health.status_code == 200
        assert health.json()["status"] == "ok"
        assert client.get("/api/v1/health/live").status_code == 200
        assert client.get("/api/v1/health/ready").status_code == 404

    def test_health_reports_exact_application_identity(self, monkeypatch):
        from quantmaster.runtime.identity import (
            BUILD_SHA_ENV,
            RUNTIME_GENERATION_ENV,
            SLOT_ID_ENV,
        )

        build_sha = "0123456789abcdef0123456789abcdef01234567"
        monkeypatch.setenv(BUILD_SHA_ENV, build_sha)
        monkeypatch.setenv(SLOT_ID_ENV, build_sha)
        monkeypatch.setenv(RUNTIME_GENERATION_ENV, "a" * 32)
        monkeypatch.setenv("QM_WEB_GENERATION", "7")

        health = client.get("/api/v1/health").json()

        assert health["build_sha"] == build_sha
        assert health["slot_id"] == build_sha
        assert health["runtime_generation"] == "a" * 32
        assert health["generation"] == "7"

    def test_cache_observability_ui_contract_is_accessible_and_responsive(self):
        page = client.get("/").text
        script = client.get("/static/app.js").text
        styles = client.get("/static/settings.css").text

        assert 'aria-labelledby="cache-observability-title"' in page
        assert 'id="cache-observability-state" role="status" aria-live="polite"' in page
        assert 'aria-label="缓存状态摘要"' in page
        assert "renderCacheObservability(data.cache)" in script
        assert "数据截至 ${asOf}" in script
        assert "已完成 ${completed} / ${requested}" in script
        assert "CACHE_NAMESPACE_UNOBSERVED" not in script
        assert "这不代表缓存为空或健康" in script
        assert ".cache-namespace > summary:focus-visible" in styles
        assert "min-height:44px" in styles
        assert "@media (max-width: 680px)" in styles

    def test_news_lock_is_retryable_service_unavailable(self, monkeypatch):
        from quantmaster.server import news as news_module

        class LockedNewsStore:
            def __init__(self, **_kwargs):
                pass

            def stats(self, *, days):
                raise sqlite3.OperationalError("database is locked")

        monkeypatch.setattr(news_module, "NewsStore", LockedNewsStore)
        response = client.get("/api/v1/news/stats?days=30")

        assert response.status_code == 503
        payload = response.json()
        assert payload["code"] == "snapshot_unavailable"
        assert payload["retryable"] is True
        assert payload["retry_after"] == 2
        assert payload["request_id"] == response.headers["X-Request-ID"]

    def test_market_history_read_failure_is_service_unavailable(self, monkeypatch):
        def fail_load(*_args, **_kwargs):
            raise TypeError("Cannot compare tz-naive and tz-aware timestamps")

        monkeypatch.setattr("quantmaster.data.read_bars", fail_load)

        response = client.get(
            "/api/v1/market/history/DX-Y.NYB.US", params={"end": "2026-08-08"},
        )

        assert response.status_code == 503
        assert "行情暂不可用" in response.json()["detail"]

    def test_calendar_unavailable_is_a_structured_block(self, monkeypatch):
        from quantmaster.server import capabilities
        from quantmaster.trading_sessions import SessionExpectation, SessionTargetUnavailable

        def unavailable(_as_of=None):
            raise SessionTargetUnavailable(
                SessionExpectation(reason="缺少已验证交易日历"),
            )

        monkeypatch.setattr(capabilities, "default_close_data_end", unavailable)
        response = client.get("/api/v1/market/history/600519.SH")

        assert response.status_code == 503
        payload = response.json()
        assert payload["problem"]["code"] == "calendar_unavailable"
        assert payload["problem"]["blocking"] is True
        assert payload["data_quality"]["completion"] == "calendar_unavailable"
        assert payload["data_quality"]["calendar"]["ready"] is False

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
        monkeypatch.setattr(
            "quantmaster.server.problems.collect_health_report",
            lambda: (_ for _ in ()).throw(AssertionError("liveness must not probe stores")),
        )
        monkeypatch.setattr(
            "quantmaster.runtime.worker.runtime_worker_status",
            lambda: (_ for _ in ()).throw(AssertionError("liveness must not probe worker")),
        )
        live = client.get("/api/v1/health")
        assert live.status_code == 200
        assert {
            key: live.json()[key]
            for key in ("status", "version", "release_date")
        } == {
            "status": "ok", "version": __version__, "release_date": RELEASE_DATE,
        }
        assert live.json()["web_threads"] >= 1
        assert live.json()["thread_status"] in {"ok", "warning"}
        assert live.json()["core_ready"] is True
        assert live.json()["readiness_status"] == "ready"

    def test_retry_settings_apply_ignores_scalar_document_sections(self, monkeypatch):
        from quantmaster.server import management
        from quantmaster.settings import SettingsDocument

        class SettingsProjection:
            path = Path("settings.yaml")

            @staticmethod
            def public():
                return {**SettingsDocument().model_dump(), "config_revision": 1}

        monkeypatch.setattr(management, "settings_manager", SettingsProjection())
        monkeypatch.setattr(management, "_require_csrf", lambda _request: None)

        captured = {}

        def queue(saved):
            captured.update(saved)
            saved["generation"] = 9
            return {"status": "queued"}

        monkeypatch.setattr(management, "_queue_runtime_apply", queue)
        monkeypatch.setattr(management, "_runtime_status", lambda: {"worker": {}})

        response = client.post("/api/v1/settings/apply", headers={"X-CSRF-Token": _csrf})

        assert response.status_code == 202
        assert "config_version" not in captured["changed_fields"]

    def test_diagnostics_expose_sanitized_runtime_status_and_core_storage_problem(self, monkeypatch):
        from quantmaster.server import diagnostics as diagnostics_module

        monkeypatch.setattr(
            "quantmaster.server.readiness.runtime_status",
            lambda: {
                "web": {
                    "pid": 123, "host": "127.0.0.1", "port": 8686,
                    "generation": "7", "version": __version__,
                },
                "readiness": {"storage_ready": False, "web_bound": True},
                "supervisor": {"status": "running", "available": True, "reason": ""},
                "storage": {"status": "unavailable", "data_root": "C:/safe"},
                "scheduler": {"status": "running", "managed_by": "runtime-worker"},
            },
        )
        monkeypatch.setattr(
            "quantmaster.data.free_stockdb_runtime.free_stockdb_runtime.status",
            lambda: {"state": "running", "message": "Bearer secret-token"},
        )
        diagnostics_module._cached = None
        diagnostics_module._refresh()
        payload = diagnostics_module.diagnostics(refresh=False)

        assert payload["runtime"]["web"]["generation"] == "7"
        assert payload["runtime"]["scheduler"]["managed_by"] == "runtime-worker"
        problem = next(item for item in payload["issues"] if item["code"] == "core_storage_unavailable")
        assert problem["correlation_id"] == "readiness-storage"
        assert problem["consecutive_count"] >= 1
        assert problem["first_seen"] and problem["last_seen"]
        assert "secret-token" not in str(payload)
        assert payload["cache"]["summary"]["namespace_count"] >= 1
        assert "错误码" in (client.get("/static/app.js").text)
        assert "syncRuntime(data.runtime)" in (client.get("/static/app.js").text)
        app_script = client.get("/static/app.js").text
        assert "storage.purpose" in app_script
        assert "storage.active_writers" in app_script
        assert "storage.diagnostic_code" in app_script
        lab_script = client.get("/static/lab.js").text
        assert "已持久化分区" in lab_script
        assert "安全重试点" in lab_script
        assert "预计峰值空间" in lab_script

    def test_runtime_lifecycle_presenter_is_optional_and_sanitized(self):
        page = client.get("/").text
        script = client.get("/static/app.js").text
        styles = client.get("/static/app.css").text

        assert 'id="runtime-lifecycle"' in page
        assert "runtime.lifecycle" in script
        assert "safeLifecycleText" in script
        assert "timeout_issues" in script
        assert '.runtime-lifecycle[data-state="draining"]' in styles

    def test_local_boundary_csrf_and_security_headers(self):
        anonymous = TestClient(app)
        blocked = anonymous.post("/api/v1/research/selection/daily", json={})
        assert blocked.status_code == 403
        assert blocked.json()["problem"]["code"] == "csrf_missing"
        assert blocked.json()["code"] == "csrf_missing"
        assert blocked.json()["retryable"] is False

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
            "/api/v1/health",
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

    def test_update_routes_require_local_staged_identity_and_remove_reload(self, monkeypatch, tmp_path):
        from quantmaster.runtime import update

        root = tmp_path / "app"
        monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "localappdata"))
        monkeypatch.setattr(update, "installed_app_root", lambda: root)
        status = client.get("/api/v1/system/update")
        assert status.status_code == 200
        assert status.json()["status"] == "empty"
        assert status.json()["staged"] == []
        assert client.post("/api/v1/system/reload").status_code == 404

        invalid = client.post(
            "/api/v1/system/update/activate",
            json={"build_sha": "not-a-sha"},
        )
        assert invalid.status_code == 422
        assert client.post(
            "/api/v1/system/update/activate",
            json={"build_sha": "0" * 40},
        ).status_code == 409

    def test_ashare_fear_greed_route_reads_local_snapshot(self, monkeypatch):
        from quantmaster import market

        monkeypatch.setattr(
            market,
            "read_ashare_fear_greed",
            lambda symbol: {
                "status": "ready",
                "symbol": symbol,
                "score": 33.39,
                "history": [],
            },
        )
        response = client.get(
            "/api/v1/market/ashare-fear-greed",
            params={"symbol": "沪深300"},
        )
        assert response.status_code == 200
        assert response.json()["symbol"] == "沪深300"
        assert response.json()["score"] == 33.39
        assert client.get("/api/v1/market/ashare-fear-greed").status_code == 422
        assert client.get(
            "/api/v1/market/ashare-fear-greed",
            params={"symbol": "不存在"},
        ).status_code == 422

    def test_index_serves_html(self):
        resp = client.get("/")
        app_script = client.get("/static/app.js").text
        candidates_script = client.get("/static/candidates.js").text
        charts_script = client.get("/static/charts.js").text
        news_script = client.get("/static/news.js").text
        stock_analysis_script = client.get("/static/stock-analysis.js").text
        today_charts = client.get("/static/today-charts.js").text
        app_styles = client.get("/static/app.css").text
        settings_script = client.get("/static/settings.js").text
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
        assert "function marketSparkOption" not in app_script
        assert "function month" in today_charts
        assert "区间涨跌" in today_charts
        assert "当日涨跌" in today_charts
        assert "PERSONAL_MARKET_GROUP = '我的股票'" in app_script
        assert "market-section-title" in resp.text
        assert "mkt-memberships" in app_script
        assert "名称与代码 · 标注提及次数" in resp.text
        assert "/static/settings.css" not in resp.text
        assert "/static/settings.js" not in resp.text
        assert ".settings-diagnostic-grid" in settings_styles
        assert "align-items: start; margin-bottom: 24px" in settings_styles
        assert resp.text.count('class="automation-list-field"') == 2
        assert '.automation-list-field textarea { min-height: 168px; }' in settings_styles
        assert "%%QM_SETTINGS_CSS_REV%%" not in resp.text
        assert "%%QM_SETTINGS_JS_REV%%" not in resp.text
        assert "/static/news.css" not in resp.text
        assert "/static/news.js" not in resp.text
        for stylesheet in ("app", "lab", "after-close"):
            assert (f"/static/{stylesheet}.css" in resp.text) is (stylesheet == "app")
        assert "queueMarketReload" in app_script
        assert "context.setLineDash([3, 3])" in today_charts
        assert "context.setLineDash([4, 3])" in today_charts
        assert "result.itemStyle.color = result.lineStyle.color" in charts_script
        assert "formatter:params =>" in app_script
        assert "point.seriesName === '牛熊分' ? fixed(numeric,1)" in app_script
        assert "color:CHART_COLORS.warning,type:'dashed'" in app_script
        assert "prefers-reduced-motion" in app_styles
        assert "prefers-reduced-transparency:reduce" in app_styles
        assert "prefers-contrast:more" in app_styles
        assert (
            ':where(button,[role="button"],[role="tab"],.mkt-item):active{transform:scale(.98)}'
            in app_styles
        )
        assert ".tab.active{animation:qm-tab .22s var(--ease-out-quart) both}" in app_styles
        assert "--scrollbar-track: #151514" in app_styles
        assert "scrollbar-color: var(--scrollbar-thumb) var(--scrollbar-track)" in app_styles
        assert "scrollbar-gutter:stable" in app_styles
        assert ".segmented {" in app_styles
        assert "overflow-x:auto; overflow-y:hidden" in app_styles
        assert "createLoadProgress" in app_script
        assert "createMarketStreamRenderer" in app_script
        assert "/api/v1/market/fear-greed" in app_script
        assert "/api/v1/market/fear-greed/refresh" in app_script
        assert "/api/v1/settings/free-stockdb/update" in app_script
        assert "/api/v1/after-close/scan" in app_script
        assert 'id="market-after-close-sync"' in resp.text
        assert 'id="market-stale-banner"' in resp.text
        assert 'id="market-fear-greed-refresh"' in resp.text
        assert "/api/v1/market/ashare-fear-greed?symbol=" in app_script
        assert 'id="market-fear-greed-time"' in resp.text
        assert 'id="market-ashare-fear-greed"' in resp.text
        assert 'id="market-ashare-fear-greed-symbol"' in resp.text
        assert 'value="沪深300"' in resp.text
        assert 'id="market-ashare-fear-greed-benchmark"' in resp.text
        assert "encodeURIComponent(selected)" in app_script
        assert "function loadAshareMarketFearGreed" in app_script
        assert "FundDB A股恐贪指数" in resp.text
        assert "function renderAshareFearGreedVisuals" in app_script
        assert "function fearGreedAsOf" in app_script
        assert ".formatToParts(parsed)" in app_script
        assert "`${year}${Number(parts.month)}月${Number(parts.day)}日" in app_script
        assert "export function renderFearGreedGauge" in today_charts
        assert "export function renderFearGreedHistory" in today_charts
        assert "export function renderMarketSpark" in today_charts
        assert 'class="eyebrow market-sentiment-title"' in resp.text
        assert 'class="market-sentiment-value"' not in resp.text
        assert "CNN 当日恐贪指数 ${scoreText}，${label}" in app_script
        assert ".market-sentiment-panel { display:grid; gap:var(--space-sm)" in app_styles
        assert ".fear-greed-gauge { position:relative" in app_styles
        assert ".fear-greed-history { position:relative" in app_styles
        assert ".mkt-spark-shell { position:relative" in app_styles
        assert "getContext('2d')" in today_charts
        assert "new ResizeObserver" in today_charts
        assert "prefers-reduced-motion: reduce" in today_charts
        assert "黄色虚线：CNN ≤10，属于罕见恐惧区间；分数越低越恐惧" in resp.text
        assert "黄色虚线：A股恐贪 ≤10，属于罕见恐惧区间；分数越低越恐惧" in resp.text
        assert "黄色虚线表示 10 分罕见恐惧参考阈值" in resp.text
        assert "requestAnimationFrame" in today_charts
        assert "disposeTodayCharts" in today_charts
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
        assert 'src="/static/charts.js"' not in resp.text
        assert 'href="/static/charts.css"' not in resp.text
        assert 'href="/static/brand-intro.css"' in resp.text
        assert 'src="/static/brand-intro.js"' in resp.text
        assert 'href="/static/brand/quantmaster-favicon.svg"' in resp.text
        assert 'src="/static/brand/quantmaster-mark-inverse.svg"' in resp.text
        assert 'option value="swing"' not in resp.text
        assert 'id="brand-replay"' in resp.text
        loader_script = client.get("/static/workspace-loader.js").text
        assert "const WORKSPACES" in loader_script
        assert "const PAGE_KEY = 'quantmaster.workspacePage.v2'" in loader_script
        assert "await previous.adapter.unmount()" in loader_script
        assert "await adapter.mount" in loader_script
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
        assert 'href="/static/candidates.css"' not in resp.text
        assert 'src="/static/candidates.js"' not in resp.text
        assert 'id="tab-stock-analysis"' in resp.text
        assert 'href="/static/stock-analysis.css"' not in resp.text
        assert 'src="/static/stock-analysis.js"' not in resp.text
        assert 'id="tab-help"' in resp.text
        assert 'class="header-help" data-tab="help"' in resp.text
        assert 'href="/static/help.css"' not in resp.text
        assert 'src="/static/help.js"' not in resp.text
        assert f'data-trading-days="{TRADING_DAYS}"' in resp.text
        assert f'data-risk-free="{RISK_FREE}"' in resp.text
        assert 'data-regime-window="10y"' in app_script
        assert "名称 / 代码 / 板块" in app_script
        assert 'id="runtime-info"' in resp.text
        assert 'id="runtime-markets"' in resp.text
        assert 'id="runtime-market-list"' in resp.text
        assert "window.QuantMasterTemporalStatus" in app_script
        assert "current_session_provider_published_waiting_ingest" in app_script
        assert 'id="data-refresh-preview"' in resp.text
        assert 'id="data-refresh-start-button"' in resp.text
        assert 'id="data-refresh-resume"' in resp.text
        assert 'id="contract-migration-status"' in resp.text
        assert 'data-contract-count="blank"' in resp.text
        assert 'id="contract-migration-investigation"' in resp.text
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
        assert "行情语义待确认，仅保留普通展示" in app_script
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
        assert "function responseError(" in app_script
        assert "runtimeInfo.add(problem.severity" in app_script
        assert "const apiFlights = new Map()" in app_script
        assert "if (canDedupe && apiFlights.has(flightKey))" in app_script
        assert "/api/v1/research/selection/history" in app_script
        assert "/api/v1/decisions" not in app_script
        assert "/api/v1/settings/universes" in candidates_script
        assert "/api/v1/candidates" not in candidates_script
        assert "/api/v1/market/stock-analyses" in stock_analysis_script
        assert "/api/v1/stock-analyses" not in stock_analysis_script
        assert "/api/v1/news?" in news_script
        assert "/api/v1/news/headlines" not in news_script
        assert "/api/v1/data/contract-migrations" in settings_script
        assert (
            "const initialSection = document.querySelector('[data-settings-section].active')"
            in settings_script
        )
        assert '<option value="apply">' not in resp.text
        assert "离线停写已验证" in settings_script
        assert "estimated_remaining_seconds" in settings_script
        assert "unknown_results" in settings_script
        assert ".contract-migration-counts" in settings_styles
        assert "unhandledrejection" in app_script
        assert 'id="release-trigger"' in resp.text
        assert 'id="release-popover"' in resp.text
        assert 'id="release-reload-button"' not in resp.text
        assert 'id="release-reload-status"' not in resp.text
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
        assert "/api/v1/system/reload" not in app_script
        assert "/api/v1/system/update/activate" not in app_script
        assert 'id="operations-progress"' in resp.text
        assert 'data-workspace-page="operations"' in resp.text
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
        assert re.findall(
            r'<button data-workspace="([^"]+)"[^>]*>([^<]+)</button>', nav_markup,
        ) == [
            ("today", "今日"),
            ("research", "研究"),
            ("account", "账户"),
            ("runtime", "运行"),
        ]
        assert re.findall(r'data-workspace-pages="([^"]+)"', resp.text) == [
            "today", "research", "account", "runtime",
        ]
        workspace_pages = (
            "quotes", "temperature", "style", "rotation", "industry", "themes", "etfs", "news",
            "after-close", "candidates", "stock-analysis", "decision", "lab", "backtest", "paper",
            "ledger", "automation", "operations",
        )
        assert [resp.text.index(f'data-workspace-page="{page}"') for page in workspace_pages] == sorted(
            resp.text.index(f'data-workspace-page="{page}"') for page in workspace_pages
        )
        assert 'id="tab-factor"' not in resp.text
        assert 'id="tab-mine"' not in resp.text
        assert all(
            f'data-workspace="{legacy}"' not in resp.text
            for legacy in ("observe", "select", "trade", "automation")
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
        assert "/api/v1/after-close/diagnostics?limit=500" in after_close_script.text
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
        monkeypatch.setattr(
            "quantmaster.server.capabilities.default_close_data_end",
            lambda _as_of=None: "2026-08-08",
        )
        monkeypatch.setattr(
            "quantmaster.trading_sessions.default_close_data_end",
            lambda _as_of=None: "2026-08-08",
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

    def test_selection_history_loads_unhashed_legacy_snapshot(self):
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
        response = client.get("/api/v1/research/selection/history", params={
            "universe": "demo", "profile": "risk_adjusted", "horizon": 3,
        })

        assert response.status_code == 200, response.text
        snapshot = response.json()["snapshots"][0]
        assert snapshot["signal_date"] == "2026-08-03"
        assert snapshot["picks"] == []

    def test_market_overview_emits_each_completed_item(self, monkeypatch):
        from quantmaster.market import overview as market_overview

        monkeypatch.setattr(market_overview, "default_close_data_end", lambda: "2026-07-22")
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
            "quantmaster.data.reference_market.refresh_reference_panel",
            lambda symbols, start, end, refresh, store: (
                {symbol: frame for symbol in symbols},
                {},
            ),
        )
        events = []

        result = market_overview.build_market_overview_data(
            "2026-07-01", lambda *args: events.append(args))
        partials = [args[3] for args in events if len(args) > 3 and args[3]]
        final_count = sum(len(items) for items in result["groups"].values())

        assert len(partials) == final_count
        assert all(partial["kind"] == "market_item" for partial in partials)
        assert all(partial["item"]["nav"] for partial in partials)

    def test_market_overview_includes_tech_focused_major_indexes(self):
        from quantmaster.market import overview as market_overview

        indexes = market_overview._market_groups()["A股指数"]

        assert indexes["000688.SH"] == "科创50"
        assert indexes["000698.SH"] == "科创100"
        assert indexes["399006.SZ"] == "创业板指"
        assert indexes["399673.SZ"] == "创业板50"

    def test_market_overview_route_reads_only_a_published_snapshot(self, monkeypatch, isolated_config):
        from quantmaster.market import overview as market_overview
        from quantmaster.market import overview_snapshot
        from quantmaster.runtime.derived import DerivedArtifactCatalog

        payload = {
            "meta": {"as_of": "2026-08-10", "stale": False, "stale_reasons": []},
            "groups": {"A股指数": []},
            "data_quality": {"status": "verified", "issues": []},
        }
        root = isolated_config.data_root / "market-test-derived"

        def catalog_factory(*_args, **kwargs):
            return DerivedArtifactCatalog(root, read_only=bool(kwargs.get("read_only")))

        monkeypatch.setattr(overview_snapshot, "DerivedArtifactCatalog", catalog_factory)
        monkeypatch.setattr(
            market_overview, "build_market_overview_data", lambda **_kwargs: payload,
        )
        published = overview_snapshot.publish_market_overview_snapshot()
        assert published["id"]
        cached = overview_snapshot.read_market_overview_snapshot()
        wire_payload, wire = overview_snapshot.read_market_overview_snapshot_wire()
        assert cached["snapshot"]["id"] == published["id"]
        assert wire_payload is cached
        assert wire.startswith(b'{"data":')

        def must_not_rebuild(**_kwargs):
            raise AssertionError("页面 GET 不得扫描 BarStore 或重建市场快照")

        monkeypatch.setattr(market_overview, "build_market_overview_data", must_not_rebuild)
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
        from quantmaster.market import overview as market_overview

        monkeypatch.setattr(market_overview, "default_close_data_end", lambda: "2026-07-22")
        symbol = "SPX.INDEX"
        dates = pd.bdate_range("2026-07-20", periods=3)
        BarStore().put(symbol, pd.DataFrame({"close": [100.0, 101.0, 102.0]}, index=dates))
        monkeypatch.setattr(market_overview, "_market_groups", lambda: {
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

        monkeypatch.setattr(
            "quantmaster.data.reference_market.refresh_reference_panel",
            failed_reference_sync,
        )
        events = []
        result = market_overview.build_market_overview_data(
            "2026-07-01", lambda *args: events.append(args))

        partials = [event[3] for event in events if len(event) > 3 and event[3]]
        assert partials[0]["stage"] == "cache"
        assert result["groups"]["全球市场"][0]["symbol"] == symbol
        assert result["groups"]["全球市场"][0]["cache_status"] == "stale"

    def test_market_overview_exposes_unavailable_reference_details(self, monkeypatch):
        from quantmaster.market import overview as market_overview

        monkeypatch.setattr(market_overview, "default_close_data_end", lambda: "2026-07-22")
        symbol = "DXY.INDEX"
        monkeypatch.setattr(market_overview, "_personal_market_symbols", lambda: ({}, {}))
        monkeypatch.setattr(market_overview, "_market_groups", lambda: {
            "商品与汇率": {symbol: "美元指数"},
        })
        monkeypatch.setattr(
            "quantmaster.data.reference_market.refresh_reference_panel",
            lambda symbols, start, end, refresh, store: ({}, {
                symbol: {
                    "error_code": "all_sources_unavailable",
                    "message": "Yahoo 正在限流",
                    "source_attempts": [{"source": "yfinance", "code": "429"}],
                },
            }),
        )

        result = market_overview.build_market_overview_data("2026-07-01")

        assert result["groups"]["商品与汇率"][0]["state"] == "unavailable"
        assert result["groups"]["商品与汇率"][0]["message"] == "Yahoo 正在限流"
        assert result["group_statuses"]["商品与汇率"]["unavailable"] == 1
        assert result["group_statuses"]["商品与汇率"]["issues"][0]["symbol"] == symbol
        assert result["unavailable_items"][0]["symbol"] == symbol
        assert result["unavailable_items"][0]["message"] == "Yahoo 正在限流"

    def test_market_fear_greed_refresh_is_explicit_and_forced(self, monkeypatch):
        calls = []

        def refresh(*, force):
            calls.append(force)
            return {"status": "ready", "score": 12.5}

        monkeypatch.setattr("quantmaster.market.load_cnn_fear_greed", refresh)

        response = client.post("/api/v1/market/fear-greed/refresh", json={})

        assert response.status_code == 200, response.text
        assert response.json() == {"status": "ready", "score": 12.5}
        assert calls == [True]

    def test_decision_dashboard_contract(self, panel, monkeypatch):
        symbols = list(panel["close"].columns)
        monkeypatch.setattr(
            "quantmaster.server.capabilities.default_close_data_end",
            lambda _as_of=None: "2023-12-29",
        )
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

        # A row whose columns disagree with its current payload is diagnosed;
        # the fresh dashboard result remains available without a decoder guess.
        from quantmaster.decision import DecisionStore

        store = DecisionStore()
        with store._conn() as connection:
            connection.execute(
                "INSERT INTO selection_snapshots "
                "(signal_date,universe,horizon,profile,policy_hash,model_version,"
                "payload,created_at) "
                "SELECT '2022-12-30',universe,horizon,profile,'legacy-unhashed',"
                "model_version,payload,created_at-1 FROM selection_snapshots LIMIT 1"
            )

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
        assert result["history"] == []
        assert result["status"] == "completed_with_issues"
        assert any(
            issue.get("code") == "history_unavailable"
            for issue in result["issues"]
        )

    def test_decision_dashboard_refreshes_low_coverage_and_persists_partial_panel(
        self, panel, monkeypatch,
    ):
        from quantmaster.data.base import BarDataEnvelope, BarDataQuality

        monkeypatch.setattr(
            "quantmaster.server.capabilities.default_close_data_end",
            lambda _as_of=None: "2023-12-29",
        )
        symbols = list(panel["close"].columns)
        missing = symbols[-1]
        local_panel = {
            field: frame.drop(columns=[missing]) for field, frame in panel.items()
        }
        local_quality = BarDataQuality(
            "degraded", "2023-01-01", "2023-12-31",
            coverage_ratio=0.80, partial=True,
            requested_symbols=tuple(symbols),
            observed_symbols=tuple(symbols[:-1]),
            missing_symbols=(missing,),
        )
        refreshed_quality = BarDataQuality(
            "degraded", "2023-01-01", "2023-12-31",
            coverage_ratio=0.95, partial=True, issues=("仍缺少少量交易日",),
            requested_symbols=tuple(symbols), observed_symbols=tuple(symbols),
        )
        monkeypatch.setattr(
            "quantmaster.data.universe.load_universe_analysis_snapshot",
            lambda _name, **_kwargs: UniverseSnapshot(
                name="demo", symbols=tuple(symbols),
                observed_at="2023-12-31T00:00:00+00:00",
                effective_as_of="2023-12-31", content_hash="fixture-universe",
                source="fixture",
            ),
        )
        monkeypatch.setattr(
            "quantmaster.data.read_panel",
            lambda *_args, **_kwargs: BarDataEnvelope(local_panel, local_quality),
        )
        refreshed = BarDataEnvelope(panel, refreshed_quality)
        monkeypatch.setattr(
            "quantmaster.data.refresh_panel", lambda *_args, **_kwargs: refreshed,
        )
        monkeypatch.setattr("quantmaster.data.read_stock_names", lambda values: {
            symbol: symbol for symbol in values
        })
        monkeypatch.setattr(
            "quantmaster.data.industry.load_industry_analysis_context",
            lambda **_kwargs: ({symbol: "行业A" for symbol in symbols}, {
                "status": "verified", "content_hash": "fixture-industry",
                "formal_eligible": True, "issues": [],
            }),
        )

        response = client.post("/api/v1/research/decision/dashboard", json={
            "universe": "demo", "start": "2023-01-01", "top_n": 4,
            "horizon": 3, "save": True,
        })

        assert response.status_code == 200, response.text
        result = response.json()
        assert result["persistence"]["saved"] is True
        assert result["data_quality"]["coverage_ratio"] == 0.95
        assert result["data_quality"]["decision_formal_eligible"] is True

    def test_decision_dashboard_uses_sufficient_local_coverage_when_refresh_fails(
        self, panel, monkeypatch,
    ):
        from quantmaster.data.base import BarDataEnvelope, BarDataQuality

        monkeypatch.setattr(
            "quantmaster.server.capabilities.default_close_data_end",
            lambda _as_of=None: "2023-12-29",
        )
        symbols = list(panel["close"].columns)
        quality = BarDataQuality(
            "degraded", "2023-01-01", "2023-12-31",
            coverage_ratio=0.95, partial=True, issues=("缺少少量交易日",),
            requested_symbols=tuple(symbols), observed_symbols=tuple(symbols),
        )
        monkeypatch.setattr(
            "quantmaster.data.universe.load_universe_analysis_snapshot",
            lambda _name, **_kwargs: UniverseSnapshot(
                name="demo", symbols=tuple(symbols),
                observed_at="2023-12-31T00:00:00+00:00",
                effective_as_of="2023-12-31", content_hash="fixture-universe",
                source="fixture",
            ),
        )
        monkeypatch.setattr(
            "quantmaster.data.read_panel",
            lambda *_args, **_kwargs: BarDataEnvelope(panel, quality),
        )
        monkeypatch.setattr(
            "quantmaster.data.refresh_panel",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("offline")),
        )
        monkeypatch.setattr("quantmaster.data.read_stock_names", lambda values: {
            symbol: symbol for symbol in values
        })
        monkeypatch.setattr(
            "quantmaster.data.industry.load_industry_analysis_context",
            lambda **_kwargs: ({symbol: "行业A" for symbol in symbols}, {
                "status": "verified", "content_hash": "fixture-industry",
                "formal_eligible": True, "issues": [],
            }),
        )

        response = client.post("/api/v1/research/decision/dashboard", json={
            "universe": "demo", "start": "2023-01-01", "top_n": 4,
            "horizon": 3, "save": True,
        })

        assert response.status_code == 200, response.text
        result = response.json()
        assert result["persistence"]["saved"] is True
        assert result["data_quality"]["coverage_ratio"] == 0.95
        assert result["data_quality"]["issues"] == ["缺少少量交易日"]

    def test_decision_stream_unavailable_preserves_truth_contract(self, monkeypatch):
        symbols = ["600000.SH", "000001.SZ"]
        monkeypatch.setattr(
            "quantmaster.server.capabilities.default_close_data_end",
            lambda _as_of=None: "2026-08-08",
        )
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
        monkeypatch.setattr(
            "quantmaster.data.refresh_panel",
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
        resp = client.post(
            "/api/v1/research/factors/test",
            json={
                "expression": "__import__('os')",
                "universe": "demo",
                "end": "2026-08-08",
            },
        )
        assert resp.status_code == 400

    def test_validate_bad_expression_400(self):
        resp = client.post(
            "/api/v1/research/factors/validate",
            json={"expression": "eval(close)", "split": "2024-01-01", "end": "2026-08-08"},
        )
        assert resp.status_code == 400

    def test_ledger_nav_empty(self):
        resp = client.get("/api/v1/portfolio/ledger/nav")
        assert resp.status_code == 200
        data = resp.json()
        assert data["dates"] == []
        assert data["excess_annual"] == 0.0
