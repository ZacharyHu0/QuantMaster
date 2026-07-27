"""Chromium 设置中心与 CSV 主流程；按约定仅在 Ubuntu CI 执行。"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

from quantmaster import __version__
from quantmaster.release import RELEASE_DATE

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="浏览器验收仅在 Ubuntu CI 执行")
playwright_sync = pytest.importorskip("playwright.sync_api")


@pytest.fixture(scope="module")
def live_server(tmp_path_factory):
    root = tmp_path_factory.mktemp("qm-ui")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    env = os.environ.copy()
    env["QM_DATA_ROOT"] = str(root / "data")
    project = Path(__file__).parents[1]
    env["PYTHONPATH"] = str(project) + os.pathsep + env.get("PYTHONPATH", "")
    process = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "quantmaster.server.app:app",
         "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
        cwd=root,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    url = f"http://127.0.0.1:{port}"
    for _ in range(100):
        try:
            if httpx.get(f"{url}/api/health", timeout=0.3).status_code == 200:
                break
        except httpx.HTTPError:
            time.sleep(0.1)
    else:
        process.terminate()
        raise RuntimeError("测试服务启动失败")
    yield url, root
    process.terminate()
    process.wait(timeout=10)


def test_settings_universe_and_csv_flow(live_server, tmp_path):
    url, _ = live_server
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1360, "height": 900})
        page.goto(url)
        release = page.locator("#release-trigger")
        release.wait_for(state="visible")
        assert f"v{__version__}" in release.inner_text()
        assert RELEASE_DATE.replace("-", ".") in release.inner_text()
        release.click()
        page.locator("#release-popover").wait_for(state="visible")
        assert "更新日志" in page.locator("#release-popover").inner_text()

        settings = page.get_by_role("button", name="设置", exact=True)
        assert settings.bounding_box()["x"] > page.locator("#nav").bounding_box()["x"]
        assert settings.inner_text() == ""
        assert settings.locator(".settings-gear").count() == 1
        settings.click()
        page.locator("#settings-config-path").wait_for(state="visible")

        page.locator('[name="llm.provider"]').select_option("openai-compatible")
        page.locator('[name="llm.base_url"]').fill("http://127.0.0.1:9/v1")
        page.locator('[name="llm.model"]').fill("manual-local-model")
        page.locator('[data-check="llm-models"]').click()
        page.locator('[data-check-result="llm-models"]').wait_for()
        assert "失败" in page.locator('[data-check-result="llm-models"]').inner_text()
        assert page.locator('[name="llm.model"]').input_value() == "manual-local-model"
        page.wait_for_function("document.querySelector('#settings-save-state').innerText.includes('已保存')")

        page.locator('[data-settings-section="automation"]').click()
        assert page.locator('[name="automation.enabled"]').is_visible()
        page.locator('[name="automation.retention_days"]').fill("120")
        page.wait_for_function("document.querySelector('#settings-save-state').innerText.includes('已自动保存')")

        page.locator('[data-settings-section="backup"]').click()
        page.locator('#snapshot-form [name="name"]').fill("UI baseline")
        page.locator('#snapshot-form button').click()
        page.get_by_text("UI baseline", exact=True).wait_for()

        page.locator('[data-settings-section="universe"]').click()
        page.locator('#universe-new').click()
        page.locator('#universe-form [name="name"]').fill("ui_pool")
        page.locator('#universe-form [name="symbols"]').fill("600519\n000001\n600519.SH")
        page.locator('#universe-form button[type="submit"]').click()
        page.locator('[data-universe="ui_pool"]').wait_for()
        assert "2 只" in page.locator('[data-universe="ui_pool"]').inner_text()

        csv = tmp_path / "broker.csv"
        csv.write_text(
            "成交日期,证券代码,买卖方向,成交价格,成交数量,佣金\n"
            "2024-01-02,600519,买入,10,100,5\n"
            "坏日期,000001,卖出,11,100,5\n",
            encoding="utf-8-sig",
        )
        page.get_by_role("button", name="实盘", exact=True).click()
        page.locator('#broker-csv').set_input_files(csv)
        page.locator('#csv-preview-form button').click()
        page.locator('#csv-submit-actions').wait_for(state="visible")
        assert "坏行" in page.locator('#csv-preview').inner_text()
        page.locator('#csv-submit').click()
        page.wait_for_function("document.querySelector('#csv-import-status').innerText.includes('未导入')")
        page.locator('[name="csv-mode"][value="valid"]').check()
        page.locator('#csv-submit').click()
        page.wait_for_function(
            "document.querySelector('#csv-import-status').innerText.includes('已导入 1 笔')"
        )
        assert page.locator('#csv-download-errors').is_visible()

        page.set_viewport_size({"width": 390, "height": 844})
        page.get_by_role("button", name="设置", exact=True).click()
        page.locator('[data-settings-section="data"]').click()
        assert page.locator('[data-settings-panel="data"]').is_visible()
        columns = page.locator(".settings-shell").evaluate(
            "el => getComputedStyle(el).gridTemplateColumns"
        )
        assert columns != "188px"
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        browser.close()


def test_decision_chart_survives_progressive_rerender(live_server):
    url, _ = live_server
    decision = {
        "market": {
            "current": {
                "as_of": "2026-07-27", "universe_size": 3, "state_label": "震荡",
                "bull_score": 52, "trend_score": 0.05, "advance_ratio": 0.5,
                "above_ma20_ratio": 0.55, "macd_hist": 0.01, "amount_ratio": 1.1,
                "volatility_20d": 0.02,
            },
            "forecast_validation": [], "future": [], "sectors": [],
            "past": [
                {"date": "2026-07-24", "bull_score": 48,
                 "advance_ratio": 0.45, "above_ma20_ratio": 0.50},
                {"date": "2026-07-25", "bull_score": 50,
                 "advance_ratio": 0.50, "above_ma20_ratio": 0.53},
                {"date": "2026-07-26", "bull_score": 52,
                 "advance_ratio": 0.55, "above_ma20_ratio": 0.58},
            ],
        },
        "selection": {
            "recommended_exposure": 0.5, "holding_horizon_days": 3,
            "signal_date": "2026-07-27", "picks": [], "risk_note": "测试",
        },
        "history": [{
            "signal_date": "2026-07-26", "holding_horizon_days": 3,
            "recommended_exposure": 0.5,
            "picks": [
                {"name": "贵州茅台", "symbol": "600519.SH"},
                {"name": "宁德时代", "symbol": "300750.SZ"},
                {"name": "招商银行", "symbol": "600036.SH"},
            ],
        }],
    }
    empty_market = '{"type":"result","data":{"groups":{}},"request_id":"test"}\n'
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.route(
            "**/api/market/overview/stream",
            lambda route: route.fulfill(
                status=200, content_type="application/x-ndjson", body=empty_market),
        )
        page.goto(url)
        state = page.evaluate(
            """data => {
              document.querySelectorAll('.tab').forEach(section =>
                section.classList.toggle('active', section.id === 'tab-decision'));
              renderDecision(data);
              renderDecision(data);
              const element = document.getElementById('regime-chart');
              const chart = charts['regime-chart'];
              return {
                canvasCount: element.querySelectorAll('canvas').length,
                boundConnected: chart.getDom().isConnected,
                boundToVisible: chart.getDom() === element,
              };
            }""",
            decision,
        )
        assert state == {
            "canvasCount": 1, "boundConnected": True, "boundToVisible": True,
        }
        period = page.locator(".snapshot-period").last
        assert period.evaluate("element => getComputedStyle(element).whiteSpace") == "nowrap"
        assert period.bounding_box()["width"] >= 72
        picks = page.locator(".snapshot-pick")
        assert picks.count() == 3
        pick_tops = [picks.nth(index).bounding_box()["y"] for index in range(3)]
        assert pick_tops == sorted(set(pick_tops))
        browser.close()


def test_active_tab_survives_reload(live_server):
    url, _ = live_server
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.goto(url)
        page.get_by_role("button", name="回测", exact=True).click()
        assert page.locator("#tab-backtest").is_visible()
        assert page.evaluate("sessionStorage.getItem('quantmaster.activeTab')") == "backtest"

        page.reload()
        assert page.locator("#tab-backtest").is_visible()
        assert page.locator('header [data-tab="backtest"]').evaluate(
            "element => element.classList.contains('active')"
        )

        page.evaluate("sessionStorage.setItem('quantmaster.activeTab', 'missing-page')")
        page.reload()
        assert page.locator("#tab-market").is_visible()
        assert page.evaluate("sessionStorage.getItem('quantmaster.activeTab')") is None
        browser.close()


def test_runtime_messages_are_compact_and_diagnostic(live_server):
    url, _ = live_server
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.goto(url)
        page.evaluate("document.getElementById('runtime-clear').click()")
        page.evaluate(
            """() => {
              const key = window.QuantMasterRunInfo.begin(
                '诊断测试', '正在加载数据', {path:'GET /api/test'});
              window.QuantMasterRunInfo.phase('诊断测试', {
                phase:'读取行情', detail:'第一阶段', request_id:'req-test'
              }, 'GET /api/test', key);
              window.QuantMasterRunInfo.phase('诊断测试', {
                phase:'计算指标', detail:'第二阶段', request_id:'req-test'
              }, 'GET /api/test', key);
            }"""
        )
        test_entries = page.locator(".runtime-entry", has_text="诊断测试")
        assert test_entries.count() == 1
        assert "计算指标" in test_entries.inner_text()

        page.evaluate(
            """window.QuantMasterRunInfo.add(
              'error', '诊断测试', '服务端处理失败', {
                detail:'database is locked', action:'稍后重试。',
                path:'POST /api/test', requestId:'req-test', key:'request:test'
              })"""
        )
        assert not page.locator("#runtime-info").evaluate(
            "element => element.classList.contains('expanded')"
        )
        page.locator("#runtime-summary").click()
        problem = page.locator('.runtime-entry[data-level="error"]', has_text="诊断测试")
        assert problem.is_visible()
        assert "稍后重试。" in problem.inner_text()
        diagnostics = problem.locator(".runtime-diagnostics")
        assert not diagnostics.evaluate("element => element.open")
        diagnostics.locator("summary").click()
        assert "database is locked" in diagnostics.inner_text()
        assert "POST /api/test" in diagnostics.inner_text()
        assert "req-test" in diagnostics.inner_text()

        page.evaluate(
            """window.QuantMasterRunInfo.add(
              'success', '诊断测试', '操作已恢复', {key:'request:test'})"""
        )
        assert page.locator(
            '.runtime-entry[data-level="error"]', has_text="服务端处理失败"
        ).count() == 0
        browser.close()
