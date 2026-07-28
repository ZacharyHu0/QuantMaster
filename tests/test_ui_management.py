"""Chromium 设置中心与 CSV 主流程；按约定仅在 Ubuntu CI 执行。"""

from __future__ import annotations

import json
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


def test_settings_candidate_and_csv_flow(live_server, tmp_path):
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
        assert page.locator("#nav button:not([hidden])").all_inner_texts() == [
            "市场", "资讯", "候选", "决策", "Quant Lab", "回测", "模拟盘", "实盘", "自动化",
        ]
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

        page.get_by_role("button", name="候选", exact=True).click()
        page.locator(".candidate-detail").wait_for()
        page.locator("#candidate-new").click()
        page.locator("#candidate-new-name").fill("ui_candidate")
        page.get_by_role("button", name="添加代码", exact=True).click()
        instrument_input = page.locator("#candidate-add-symbol")
        instrument_input.fill("700")
        page.locator("#candidate-instrument-options").wait_for(state="visible")
        choices = page.locator("#candidate-instrument-options").inner_text()
        assert "000700.SZ" in choices and "00700.HK" in choices
        instrument_input.press("ArrowDown")
        instrument_input.press("Enter")
        assert instrument_input.input_value() == "00700.HK"
        page.get_by_role("button", name="收起", exact=True).click()
        page.get_by_role("button", name="批量编辑", exact=True).click()
        page.locator("#candidate-bulk-text").fill("600519\n000001\n600519.SH")
        page.get_by_role("button", name="应用到草稿", exact=True).click()
        page.get_by_text("有尚未生效的更改", exact=True).wait_for()
        assert page.locator(".candidate-member-symbol").all_inner_texts() == [
            "600519.SH", "000001.SZ",
        ]
        page.get_by_role("button", name="创建候选", exact=True).click()
        page.get_by_role("heading", name="ui_candidate", exact=True).wait_for()
        assert page.locator('[data-candidate-name="ui_candidate"]').is_visible()

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
        page.get_by_role("button", name="候选", exact=True).click()
        assert page.locator("#candidate-mobile-select").is_visible()
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        page.get_by_role("button", name="设置", exact=True).click()
        page.locator('[data-settings-section="data"]').click()
        assert page.locator('[data-settings-panel="data"]').is_visible()
        columns = page.locator(".settings-shell").evaluate(
            "el => getComputedStyle(el).gridTemplateColumns"
        )
        assert columns != "188px"
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        browser.close()


def test_help_handbook_search_routes_and_calculators(live_server):
    url, _ = live_server
    trade_settings = {
        "commission_rate": 0.00025,
        "commission_min": 5.0,
        "stamp_tax_rate": 0.0005,
        "transfer_fee_rate": 0.00001,
        "slippage": 0.001,
        "lot_size": 100,
    }
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1360, "height": 900})
        page.route(
            "**/api/settings",
            lambda route: route.fulfill(
                status=200,
                content_type="application/json",
                body=json.dumps({"trade": trade_settings}),
            ),
        )
        page.goto(url)

        help_button = page.get_by_role("button", name="帮助", exact=True)
        settings_button = page.get_by_role("button", name="设置", exact=True)
        assert help_button.bounding_box()["x"] < settings_button.bounding_box()["x"]
        help_button.click()
        page.locator("#help-start").wait_for(state="visible")
        page.wait_for_function(
            "document.querySelector('#help-settings-status').innerText.startsWith('已载入')"
        )
        assert page.locator("#help-settings-status").inner_text().startswith("已载入")
        assert page.locator("#help-article h2").count() == 13
        assert page.evaluate("location.hash") == "#help/start"

        page.reload()
        page.locator("#help-start").wait_for(state="visible")
        assert page.locator("#tab-help").evaluate("el => el.classList.contains('active')")

        page.goto(f"{url}/#help/validation")
        page.locator("#help-validation").wait_for(state="visible")
        page.wait_for_function(
            "document.querySelector('[data-help-link=\"validation\"]')"
            ".getAttribute('aria-current') === 'location'"
        )
        assert page.locator('[data-help-link="validation"]').get_attribute("aria-current") == "location"

        search = page.locator("#help-search-input")
        search.fill("T+1")
        page.locator(".help-search-result").first.wait_for()
        assert "T+1" in page.locator("#help-search-results").inner_text()
        search.fill("RankIC")
        assert "RankIC" in page.locator("#help-search-results").inner_text()
        search.fill("完全不存在的量化词条xyz")
        assert "没有找到" in page.locator("#help-search-results").inner_text()
        page.locator("#help-search-clear").click()
        assert page.locator("#help-search-results").is_hidden()

        page.goto(f"{url}/#help/calculators")
        page.locator("#calc-compound").wait_for(state="visible")
        assert page.locator('#calc-compound [data-output="annual"]').inner_text() == "10.00%"
        assert page.locator('#calc-drawdown [data-output="recovery"]').inner_text() == "25.00%"
        assert page.locator('#calc-sharpe [data-output="sharpe"]').inner_text() == "0.512"
        assert page.locator('#calc-rankic [data-output="rankic"]').inner_text() == "1.0000"

        rank_pairs = page.locator('#calc-rankic [name="pairs"]')
        rank_pairs.fill("1, 3\n2, 2\n3, 1")
        assert page.locator('#calc-rankic [data-output="rankic"]').inner_text() == "-1.0000"
        rank_pairs.fill("1, 1\n1, 2\n2, 3")
        assert page.locator('#calc-rankic [data-output="rankic"]').inner_text() == "0.8660"
        rank_pairs.fill("1, 1\n1, 2\n1, 3")
        assert "常数" in page.locator("#calc-rankic [data-error]").inner_text()

        assert page.locator('#calc-cost [data-output="buy_total"]').inner_text() == "¥10,015.10"
        assert page.locator('#calc-cost [data-output="sell_net"]').inner_text() == "¥10,179.60"
        assert page.locator('#calc-cost [data-output="pnl"]').inner_text() == "¥164.50"
        assert page.locator('#calc-cost [data-output="breakeven"]').inner_text() == "10.0352 元"
        assert page.locator('#calc-position [data-output="shares"]').inner_text() == "700 股"
        page.locator('#calc-position [name="stop"]').fill("21")
        assert "止损报价必须低于入场报价" in page.locator("#calc-position [data-error]").inner_text()

        page.goto(f"{url}/#help/models")
        page.locator('#help-models [data-help-tab="decision"]').click()
        assert page.locator("#tab-decision").evaluate("el => el.classList.contains('active')")

        for width, height in ((1360, 900), (900, 900), (390, 844)):
            page.set_viewport_size({"width": width, "height": height})
            page.goto(f"{url}/#help/start")
            page.locator("#help-start").wait_for(state="visible")
            assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        assert page.locator(".help-mobile-toc").is_visible()
        browser.close()


def test_help_settings_failure_keeps_manual_calculators(live_server):
    url, _ = live_server
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 900, "height": 800})
        page.route("**/api/settings", lambda route: route.fulfill(status=503, body="unavailable"))
        page.goto(f"{url}/#help/calculators")
        page.locator("#calc-cost").wait_for(state="visible")
        page.wait_for_function(
            "document.querySelector('#help-settings-status').innerText.includes('未能读取项目设置')"
        )
        assert "未能读取项目设置" in page.locator("#help-settings-status").inner_text()
        assert page.locator('#calc-cost [data-trade-key="commission_rate"]').is_editable()
        assert page.locator('#calc-cost [data-output="buy_total"]').inner_text() != "—"
        browser.close()


def test_help_content_retry(live_server):
    url, _ = live_server
    attempts = 0

    def route_help_content(route):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            route.fulfill(status=503, body="temporarily unavailable")
        else:
            route.continue_()

    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 900, "height": 800})
        page.route("**/static/help-content.html", route_help_content)
        page.goto(f"{url}/#help/start")
        page.locator("#help-retry").wait_for(state="visible")
        assert "手册暂时没有载入" in page.locator("#help-root").inner_text()
        page.locator("#help-retry").click()
        page.locator("#help-start").wait_for(state="visible")
        assert attempts == 2
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


def test_decision_pick_expands_inline_and_toggles_asset_lists(live_server):
    url, _ = live_server
    decision = {
        "market": {
            "current": {
                "as_of": "2026-07-27", "universe_size": 2, "state_label": "震荡",
                "bull_score": 52, "trend_score": 0.05, "advance_ratio": 0.5,
                "above_ma20_ratio": 0.55, "macd_hist": 0.01, "amount_ratio": 1.1,
                "volatility_20d": 0.02,
            },
            "forecast_validation": [], "future": [], "sectors": [], "past": [],
        },
        "selection": {
            "recommended_exposure": 0.5, "holding_horizon_days": 3,
            "signal_date": "2026-07-27", "risk_note": "测试风险说明",
            "picks": [
                {
                    "rank": 1, "symbol": "600519.SH", "name": "贵州茅台",
                    "industry": "白酒", "score": 82, "action": "buy",
                    "last_close": 1500, "money_ratio": 1.2, "expected_return": 0.03,
                    "stop_loss": 0.04, "take_profit": 0.08, "reasons": ["趋势向上"],
                },
                {
                    "rank": 2, "symbol": "300750.SZ", "name": "宁德时代",
                    "industry": "电池", "score": 76, "action": "buy",
                    "last_close": 260, "money_ratio": 1.1, "expected_return": 0.02,
                    "stop_loss": 0.04, "take_profit": 0.07, "reasons": ["资金改善"],
                },
            ],
        },
        "history": [],
    }
    lists = {
        "favorites": [{"symbol": "600519.SH", "name": "贵州茅台"}],
        "following": [], "holdings": [],
    }
    history_calls = []

    def history_handler(route):
        request_url = route.request.url
        history_calls.append(request_url)
        if "frequency=1m" in request_url:
            route.fulfill(status=404, json={"detail": "1 分钟行情暂不可用"})
            return
        symbol = request_url.split("/api/market/history/", 1)[1].split("?", 1)[0]
        frequency = request_url.split("frequency=", 1)[1].split("&", 1)[0]
        route.fulfill(json={
            "symbol": symbol, "frequency": frequency,
            "kline": [
                ["2026-07-24", 10, 10.5, 9.8, 10.8, 1000],
                ["2026-07-25", 10.5, 11, 10.2, 11.2, 1200],
            ],
        })

    def asset_handler(route):
        request = route.request
        tail = request.url.split("/api/assets/lists", 1)[1].split("?", 1)[0].strip("/")
        if request.method == "POST":
            list_name = tail.split("/", 1)[0]
            item = request.post_data_json
            lists[list_name] = [
                existing for existing in lists[list_name]
                if existing["symbol"] != item["symbol"]
            ]
            lists[list_name].insert(0, item)
        elif request.method == "DELETE":
            list_name, symbol = tail.split("/", 1)
            lists[list_name] = [
                item for item in lists[list_name] if item["symbol"] != symbol
            ]
        route.fulfill(json=lists)

    empty_market = '{"type":"result","data":{"groups":{}},"request_id":"test"}\n'
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.route(
            "**/api/market/overview/stream*",
            lambda route: route.fulfill(
                status=200, content_type="application/x-ndjson", body=empty_market),
        )
        page.route("**/api/market/history/**", history_handler)
        page.route("**/api/assets/lists**", asset_handler)
        page.goto(url)
        page.evaluate(
            """data => {
              document.querySelectorAll('header [data-tab]').forEach(button =>
                button.classList.toggle('active', button.dataset.tab === 'decision'));
              document.querySelectorAll('.tab').forEach(section =>
                section.classList.toggle('active', section.id === 'tab-decision'));
              renderDecision(data);
            }""",
            decision,
        )

        first_row = page.locator('tr[data-symbol="600519.SH"]')
        first_trigger = first_row.locator("[data-decision-kline-trigger]")
        first_trigger.click()
        page.locator("#decision-kline canvas").wait_for()
        assert page.locator("#tab-decision").is_visible()
        assert not page.locator("#tab-market").is_visible()
        assert page.locator(".decision-detail-row").count() == 1
        assert first_row.evaluate(
            "row => row.nextElementSibling.classList.contains('decision-detail-row')"
        )
        assert first_trigger.get_attribute("aria-expanded") == "true"
        assert page.locator(
            '[data-decision-asset-toggle="favorites"]'
        ).inner_text() == "已自选"

        page.locator('[data-decision-frequency="60m"]').click()
        page.locator("#decision-kline canvas").wait_for()
        assert any("frequency=60m" in request_url for request_url in history_calls)

        following = page.locator('[data-decision-asset-toggle="following"]')
        following.click()
        page.wait_for_function(
            "document.querySelector('[data-decision-asset-toggle=following]').innerText === '已关注'"
        )
        assert first_trigger.get_attribute("aria-expanded") == "true"
        following.click()
        page.wait_for_function(
            "document.querySelector('[data-decision-asset-toggle=following]').innerText === '加入关注'"
        )
        assert page.locator(".decision-detail-row").count() == 1

        second_trigger = page.locator(
            'tr[data-symbol="300750.SZ"] [data-decision-kline-trigger]'
        )
        second_trigger.click()
        page.locator("#decision-kline canvas").wait_for()
        assert page.locator(".decision-detail-row").count() == 1
        assert first_trigger.get_attribute("aria-expanded") == "false"
        assert second_trigger.get_attribute("aria-expanded") == "true"
        second_trigger.click()
        assert page.locator(".decision-detail-row").count() == 0

        first_trigger.focus()
        page.keyboard.press("Enter")
        page.locator("#decision-kline canvas").wait_for()
        zoom_equivalent_widths = (1600, 1422, 1280, 1164, 1024, 853, 731, 640, 390)
        for viewport_width in zoom_equivalent_widths:
            if page.locator(".decision-detail-row").count():
                first_trigger.evaluate("button => button.click()")
            page.set_viewport_size({"width": viewport_width, "height": 844})
            scroller = page.locator(".decision-table-scroll")
            collapsed_scroll_width = scroller.evaluate("element => element.scrollWidth")
            expected_scroll_left = scroller.evaluate(
                """element => {
                  element.scrollLeft = Math.min(80, element.scrollWidth - element.clientWidth);
                  return element.scrollLeft;
                }"""
            )
            first_trigger.evaluate("button => button.click()")
            page.locator("#decision-kline canvas").wait_for()
            page.evaluate(
                "() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)))"
            )
            metrics = page.evaluate(
                """() => {
                  const scroller = document.querySelector('.decision-table-scroll');
                  const shell = document.querySelector('.decision-detail-shell');
                  const chart = document.querySelector('.decision-kline-canvas');
                  const scrollRect = scroller.getBoundingClientRect();
                  const shellRect = shell.getBoundingClientRect();
                  const chartRect = chart.getBoundingClientRect();
                  return {
                    clientWidth: scroller.clientWidth,
                    scrollWidth: scroller.scrollWidth,
                    scrollLeft: scroller.scrollLeft,
                    scrollRight: scrollRect.right,
                    shellLeft: shellRect.left,
                    shellRight: shellRect.right,
                    shellWidth: shellRect.width,
                    chartRight: chartRect.right,
                    documentFits: document.documentElement.scrollWidth <= window.innerWidth,
                  };
                }"""
            )
            assert metrics["shellWidth"] <= metrics["clientWidth"] + 1, metrics
            assert metrics["shellLeft"] >= scroller.bounding_box()["x"] - 1, metrics
            assert metrics["shellRight"] <= metrics["scrollRight"] + 1, metrics
            assert metrics["chartRight"] <= metrics["scrollRight"] + 1, metrics
            assert metrics["scrollWidth"] <= collapsed_scroll_width + 1, metrics
            assert abs(metrics["scrollLeft"] - expected_scroll_left) <= 1, metrics
            assert metrics["documentFits"], metrics

        page.locator('[data-decision-frequency="1m"]').click()
        page.locator(".decision-kline-error").wait_for()
        assert "1 分钟行情暂不可用" in page.locator(".decision-kline-error").inner_text()
        assert page.locator("#tab-decision").is_visible()
        browser.close()


def test_personal_market_group_is_first_and_shows_memberships(live_server):
    url, _ = live_server
    personal = {
        "symbol": "600519.SH", "name": "贵州茅台", "last": 1530.0,
        "change_pct": 0.99, "nav": [[1784505600000, 1.0], [1784592000000, 1.02]],
        "as_of": "2026-07-21", "cache_status": "ready",
        "memberships": ["favorites", "holdings"],
    }
    index = {
        "symbol": "000300.SH", "name": "沪深300", "last": 4600.0,
        "change_pct": -0.2, "nav": [[1784505600000, 1.0], [1784592000000, 0.998]],
        "as_of": "2026-07-21", "cache_status": "ready",
    }
    stream = "\n".join([
        json.dumps({
            "type": "progress", "progress": 10, "phase": "读取本地市场缓存",
            "detail": "贵州茅台", "partial": {
                "kind": "market_item", "group": "我的股票", "item": personal,
            }, "request_id": "market-test",
        }, ensure_ascii=False),
        json.dumps({
            "type": "result", "data": {
                "groups": {"我的股票": [personal], "A股指数": [index]},
            }, "request_id": "market-test",
        }, ensure_ascii=False),
    ]) + "\n"
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.route(
            "**/api/market/overview/stream*",
            lambda route: route.fulfill(
                status=200, content_type="application/x-ndjson", body=stream),
        )
        page.goto(url)
        personal_section = page.locator('[data-market-group="我的股票"]')
        index_section = page.locator('[data-market-group="A股指数"]')
        personal_section.wait_for()
        index_section.wait_for()

        assert personal_section.locator(".market-section-title").inner_text() == "我的股票"
        assert personal_section.locator(".mkt-memberships").inner_text() == "自选 · 持有"
        assert personal_section.locator(".mkt-item").count() == 1
        assert personal_section.bounding_box()["y"] < index_section.bounding_box()["y"]

        page.set_viewport_size({"width": 390, "height": 844})
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
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
        collapsed_box = page.locator("#runtime-info").bounding_box()
        assert collapsed_box is not None
        assert abs(collapsed_box["x"] - 16) < 1
        assert abs(900 - collapsed_box["y"] - collapsed_box["height"] - 12) < 1
        assert collapsed_box["width"] <= 360
        page.locator("#runtime-summary").click()
        expanded_box = page.locator("#runtime-info").bounding_box()
        assert expanded_box is not None
        assert abs(expanded_box["x"] - 16) < 1
        assert collapsed_box["width"] < expanded_box["width"] <= 520
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


def test_active_health_issue_survives_clear_and_confirmation_dialog_is_explicit(live_server):
    url, _ = live_server
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.goto(url)
        page.evaluate(
            """window.QuantMasterRunInfo.sync('test-health', [{
              id:'provider:test', revision:'one', severity:'warning', source:'行情数据源',
              title:'测试数据源暂停请求', message:'上游暂不可用',
              action:'稍后由系统自动探测恢复。'
            }])"""
        )
        page.locator("#runtime-clear").click()
        assert page.locator(".runtime-entry", has_text="测试数据源暂停请求").count() == 1

        page.evaluate("window.QuantMasterRunInfo.sync('test-health', [])")
        assert page.locator(".runtime-entry", has_text="测试数据源暂停请求").count() == 0

        page.evaluate(
            """window.__problemDecision = null;
            window.QuantMasterProblemDialog.open({
              id:'backtest:partial', severity:'warning', title:'回测数据不完整',
              message:'一只候选缺少行情。', action:'建议先补齐数据。',
              blocking:true, can_continue:true, items:['000001.SZ']
            }, {usable_symbol_count:4, requested_symbol_count:5,
                actual_start:'2026-01-01', actual_end:'2026-07-27',
                executable_signals:9, selected_signals:10})
              .then(value => { window.__problemDecision = value; });"""
        )
        dialog = page.locator("#operation-problem-dialog")
        assert dialog.is_visible()
        assert "回测数据不完整" in dialog.inner_text()
        assert dialog.locator("#operation-problem-continue").is_visible()
        dialog.locator("#operation-problem-continue").click()
        page.wait_for_function("window.__problemDecision === true")
        assert not dialog.is_visible()
        browser.close()


def test_automation_subscriptions_audit_and_source_save_feedback(live_server):
    url, _ = live_server
    target = {
        "id": "feishu_owner", "channel": "feishu", "label": "飞书管理员私聊",
        "target": "oc_test", "account_id": "cli_app", "chat_type": "direct",
        "enabled": True, "preset": "balanced", "overrides": {}, "status": "healthy",
        "last_error": "", "owner_actor": "feishu:cli_app:ou_owner", "has_context": False,
        "updated_at": "2026-07-27T10:00:00+00:00",
    }
    overview = {
        "enabled": True, "timezone": "Asia/Shanghai", "runtime": "running",
        "bot_accounts": [], "jobs": [], "recent_events": [], "targets": [target],
        "inbound": {
            "feishu": {
                "total": 0, "last_received_at": "",
                "direct": {"total": 0, "last_received_at": ""},
                "group": {"total": 0, "last_received_at": ""},
            },
            "weixin": {"total": 0, "last_received_at": ""},
        },
    }
    source = {
        "id": "sse", "name": "上海证券交易所", "kind": "builtin",
        "group_name": "official", "url": "https://www.sse.com.cn/",
        "item_limit": 30, "factor_weight": 1, "enabled": True, "is_official": True,
        "built_in": True, "auth_type": "none", "auth_header": "",
        "auth_configured": False, "parser": {}, "last_error": "", "last_run": "",
    }
    audit_requests = {"count": 0}

    def policy_handler(route):
        body = route.request.post_data_json
        target["preset"] = body["preset"]
        target["overrides"] = body.get("overrides") or {}
        if body.get("enabled") is not None:
            target["enabled"] = body["enabled"]
        route.fulfill(status=200, json=target)

    def audit_handler(route):
        audit_requests["count"] += 1
        route.fulfill(status=200, json={"items": [{
            "created_at": "2026-07-27T10:00:00+00:00", "actor": "web",
            "action": "update_policy", "object_type": "target",
            "object_id": "feishu_owner", "result": "ok",
        }]})

    def source_handler(route):
        if route.request.method == "GET":
            route.fulfill(status=200, json={"items": [source]})
        else:
            route.fulfill(status=200, json=source)

    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 390, "height": 844})
        page.route("**/api/automation/overview", lambda route: route.fulfill(json=overview))
        page.route("**/api/automation/audit*", audit_handler)
        page.route("**/api/automation/targets/*/policy", policy_handler)
        page.route("**/api/news/sources*", source_handler)
        page.goto(url)

        page.get_by_role("button", name="自动化", exact=True).click()
        page.locator('[data-target-card="feishu_owner"]').wait_for()
        assert not page.locator("#automation-audit-panel").evaluate("element => element.open")
        assert audit_requests["count"] == 0

        for kind in ("important_news", "market_turn", "market_close", "task_report", "task_failure"):
            page.locator(f'[data-target="feishu_owner"][data-event-type="{kind}"]').uncheck()
            page.wait_for_function(
                "document.querySelector('[data-target-card=\"feishu_owner\"] .target-feedback')"
                ".classList.contains('success')"
            )
        assert target["overrides"]["event_types"] == []
        assert "自动化与 Bot 监听仍会继续运行" in page.locator(
            '[data-target-card="feishu_owner"] .target-content-note'
        ).inner_text()

        page.locator("#automation-audit-panel summary").click()
        page.get_by_text("update_policy", exact=True).wait_for()
        assert audit_requests["count"] == 1
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")

        page.get_by_role("button", name="设置", exact=True).click()
        page.locator('[data-settings-section="sources"]').click()
        page.locator('[data-source-id="sse"]').click()
        page.locator('#news-source-editor button[type="submit"]').click()
        page.wait_for_function(
            "document.querySelector('#news-source-feedback').classList.contains('success')"
        )
        assert "已保存" in page.locator("#news-source-feedback").inner_text()
        browser.close()
