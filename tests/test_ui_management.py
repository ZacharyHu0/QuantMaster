"""Chromium 管理主流程；只在显式浏览器 lane 执行。"""

from __future__ import annotations

import json
import os
import re
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

from quantmaster import __version__
from quantmaster.release import RELEASE_DATE

pytestmark = pytest.mark.skipif(
    os.environ.get("QM_RUN_UI") != "1",
    reason="浏览器验收使用独立 lane；设置 QM_RUN_UI=1 可显式启用",
)
playwright_sync = pytest.importorskip("playwright.sync_api")


def _wait_for_text(locator, text: str, *, timeout: float = 30_000) -> None:
    playwright_sync.expect(locator).to_contain_text(text, timeout=timeout)


def _wait_for_class(locator, class_name: str, *, timeout: float = 30_000) -> None:
    playwright_sync.expect(locator).to_have_class(
        re.compile(rf"(?:^|\s){re.escape(class_name)}(?:\s|$)"), timeout=timeout,
    )


def _wait_for_document_fit(page, *, timeout: float = 30_000) -> None:
    deadline = time.monotonic() + timeout / 1000
    dimensions = {"scrollWidth": -1, "innerWidth": -1}
    while time.monotonic() < deadline:
        dimensions = page.evaluate(
            "({scrollWidth: document.documentElement.scrollWidth, innerWidth: window.innerWidth})"
        )
        if dimensions["scrollWidth"] <= dimensions["innerWidth"]:
            return
        page.wait_for_timeout(50)
    raise AssertionError(f"页面存在横向溢出: {dimensions}")


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
            if httpx.get(f"{url}/api/v1/health/live", timeout=0.3).status_code == 200:
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
        assert page.locator("#nav .workspace-nav button").all_inner_texts() == [
            "观察", "选股", "研究", "交易", "自动化",
        ]
        assert page.locator('[data-workspace-pages="observe"] button').all_inner_texts() == [
            "行情", "市场温度", "市场风格", "轮动总览", "行业周期", "细分题材", "宽基资金", "资讯",
        ]
        page.get_by_role("button", name="自动化", exact=True).click()
        assert page.locator(".workspace-context").is_hidden()
        settings.click()
        config_path = page.locator("#settings-config-path")
        config_path.wait_for(state="visible")
        playwright_sync.expect(config_path).not_to_have_text("正在读取配置…")

        page.locator('[name="llm.provider"]').select_option("openai-compatible")
        page.locator('[name="llm.base_url"]').fill("http://127.0.0.1:9/v1")
        page.locator('[name="llm.model"]').fill("manual-local-model")
        page.locator('[name="llm.reasoning_effort"]').select_option("high")
        page.locator('[name="llm.max_concurrency"]').fill("2")
        page.locator('[data-check="llm-models"]').click()
        model_check = page.locator('[data-check-result="llm-models"]')
        playwright_sync.expect(model_check).to_have_class(
            re.compile(r"(?:^|\s)(?:error|warning)(?:\s|$)"),
        )
        check_text = model_check.inner_text()
        assert "失败" in check_text or "尚未配置" in check_text
        assert "检测中" not in check_text
        assert page.locator('[name="llm.model"]').input_value() == "manual-local-model"
        assert page.locator('[name="llm.reasoning_effort"]').input_value() == "high"
        assert page.locator('[name="llm.max_concurrency"]').input_value() == "2"
        _wait_for_class(page.locator("#settings-save-state"), "saved")

        page.locator('[data-settings-section="automation"]').click()
        assert page.locator('[name="automation.enabled"]').is_visible()
        page.locator('[name="automation.retention_days"]').fill("120")
        _wait_for_class(page.locator("#settings-save-state"), "saved")

        page.locator('[data-settings-section="backup"]').click()
        page.locator('#snapshot-form [name="name"]').fill("UI baseline")
        page.locator('#snapshot-form button').click()
        page.get_by_text("UI baseline", exact=True).first.wait_for()

        page.get_by_role("button", name="候选", exact=True).click()
        page.locator(".candidate-detail").wait_for()
        page.locator("#candidate-new").click()
        preset_buttons = page.locator("[data-candidate-index-preset]")
        assert preset_buttons.count() == 9
        assert preset_buttons.nth(0).get_attribute("data-candidate-index-preset") == "000688.SH"
        assert "科创50" in preset_buttons.nth(0).inner_text()
        assert "中证1000" in preset_buttons.nth(8).inner_text()
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
        member_symbols = page.locator(".candidate-member-symbol")
        playwright_sync.expect(member_symbols).to_have_count(2)
        assert member_symbols.all_inner_texts() == [
            "600519.SH", "000001.SZ",
        ]
        page.get_by_role("button", name="创建候选", exact=True).click()
        page.get_by_role("heading", name="ui_candidate", exact=True).wait_for()
        page.locator('[data-candidate-name="ui_candidate"]').wait_for(state="visible")

        csv = tmp_path / "broker.csv"
        csv.write_text(
            "成交日期,证券代码,买卖方向,成交价格,成交数量,佣金\n"
            "2024-01-02,600519,买入,10,100,5\n"
            "坏日期,000001,卖出,11,100,5\n",
            encoding="utf-8-sig",
        )
        page.get_by_role("button", name="实盘", exact=True).click()
        page.locator('#broker-csv').set_input_files(csv)
        preview_button = page.locator('#csv-preview-form button')
        preview_button.wait_for(state="visible")
        preview_button.click()
        page.locator('#csv-submit-actions').wait_for(state="visible")
        _wait_for_text(page.locator('#csv-preview'), "坏行")
        page.locator('#csv-submit').click()
        _wait_for_text(page.locator("#csv-import-status"), "未导入")
        page.locator('[name="csv-mode"][value="valid"]').check()
        page.locator('#csv-submit').click()
        _wait_for_text(page.locator("#csv-import-status"), "已导入 1 笔")
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
            "**/api/v1/settings",
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
        _wait_for_text(page.locator("#help-settings-status"), "已载入")
        assert page.locator("#help-settings-status").inner_text().startswith("已载入")
        assert page.locator("#help-article h2").count() == 28
        assert page.locator(".help-sidebar .help-nav-part").count() == 6
        assert page.locator(".help-sidebar .help-nav-part > ol").count() == 6
        assert page.evaluate("location.hash") == "#help/start"

        page.reload()
        page.locator("#help-start").wait_for(state="visible")
        assert page.locator("#tab-help").evaluate("el => el.classList.contains('active')")

        page.goto(f"{url}/#help/validation")
        page.locator("#help-validation").wait_for(state="visible")
        playwright_sync.expect(page.locator('[data-help-link="validation"]')).to_have_attribute(
            "aria-current", "location",
        )
        assert page.locator('[data-help-link="validation"]').get_attribute("aria-current") == "location"
        assert page.locator('[data-help-nav-part="signals"]').evaluate(
            "element => element.classList.contains('active')"
        )

        search = page.locator("#help-search-input")
        search.fill("T+1")
        page.locator(".help-search-result").first.wait_for()
        assert "T+1" in page.locator("#help-search-results").inner_text()
        page.locator("#help-search-clear").click()
        assert page.locator("#help-search-results").is_hidden()

        page.goto(f"{url}/#help/calculators")
        page.locator("#calc-compound").wait_for(state="visible")
        assert page.locator('#calc-compound [data-output="annual"]').inner_text() == "10.00%"

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
        symbol = request_url.split("/api/v1/market/history/", 1)[1].split("?", 1)[0]
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
        tail = request.url.split("/api/v1/portfolio/lists", 1)[1].split("?", 1)[0].strip("/")
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
            "**/api/v1/market/overview/stream*",
            lambda route: route.fulfill(
                status=200, content_type="application/x-ndjson", body=empty_market),
        )
        page.route("**/api/v1/market/history/**", history_handler)
        page.route("**/api/v1/portfolio/lists**", asset_handler)
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
        playwright_sync.expect(following).to_have_text("已关注")
        assert first_trigger.get_attribute("aria-expanded") == "true"
        following.click()
        playwright_sync.expect(following).to_have_text("加入关注")
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


def test_major_indexes_are_first_and_personal_group_shows_memberships(live_server):
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
            "**/api/v1/market/overview/stream*",
            lambda route: route.fulfill(
                status=200, content_type="application/x-ndjson", body=stream),
        )
        page.goto(url)
        personal_section = page.locator('[data-market-group="我的股票"]')
        index_section = page.locator('[data-market-group="A股指数"]')
        personal_section.wait_for()
        index_section.wait_for()

        assert personal_section.locator(".market-section-title").inner_text() == "我的股票"
        assert index_section.locator(".market-section-title").inner_text() == "主要指数"
        assert personal_section.locator(".mkt-memberships").inner_text() == "自选 · 持有"
        assert "贵州茅台" in personal_section.locator(".nm").inner_text()
        assert "600519.SH" in personal_section.locator(".nm").inner_text()
        assert personal_section.locator(".mkt-item").count() == 1
        assert index_section.locator(".mkt-item").count() == 1
        assert index_section.locator("canvas").is_visible()
        assert index_section.locator(".spark").bounding_box()["height"] >= 70
        assert "区间 -0.20%" in index_section.locator(".mkt-spark-foot").inner_text()
        assert "07.20—07.21" in index_section.locator(".mkt-spark-period").inner_text()
        spark_id = index_section.locator(".spark").get_attribute("id")
        spark_option = page.evaluate(
            """id => {
              const option = charts[id].getOption();
              return {
                series: option.series.map(item => item.name),
                tooltip: option.tooltip[0].show,
                axisType: option.xAxis[0].type,
                axisVisible: option.xAxis[0].show,
                axisDates: option.xAxis[0].data,
                axisLabelSize: option.xAxis[0].axisLabel.fontSize,
                areaOpacity: option.series[0].areaStyle.opacity,
                lineColor: option.series[0].lineStyle.color,
                endpointPoints: option.series[1].data.length,
                tooltipText: option.tooltip[0].formatter([{
                  seriesId:'market-spark-trend', dataIndex:1,
                  value:[1784592000000,-0.2],
                }]),
              };
            }""",
            spark_id,
        )
        assert spark_option == {
            "series": ["区间走势", "最新位置"],
            "tooltip": True,
            "axisType": "category",
            "axisVisible": True,
            "axisDates": [1784505600000, 1784592000000],
            "axisLabelSize": 9,
            "areaOpacity": 0.1,
            "lineColor": "#24a06b",
            "endpointPoints": 1,
            "tooltipText": (
                "07.21<br><span style=\"color:#24a06b\">●</span> "
                "区间涨跌&nbsp;&nbsp;<b>-0.20%</b><br>"
                "<span style=\"color:#24a06b\">●</span> "
                "当日涨跌&nbsp;&nbsp;<b>-0.20%</b>"
            ),
        }
        assert page.evaluate("marketSparkMonth(1784505600000, true)") == "2026.07"
        assert page.evaluate("marketSparkMonth('1784505600000', true)") == "2026.07"
        assert page.evaluate("marketSparkMonth(1784505600000, false)") == "07月"
        assert index_section.bounding_box()["y"] < personal_section.bounding_box()["y"]

        page.set_viewport_size({"width": 390, "height": 844})
        _wait_for_document_fit(page)
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        browser.close()


def test_backtest_factor_completion_supports_lab_names_and_comma_segments(live_server):
    url, _ = live_server
    factors = [
        {
            "name": "mom_20d", "description": "20 日动量", "source": "builtin",
        },
        {
            "name": "人工反转", "slug": "manual_a1b2c3d4e5",
            "description": "Quant Lab 人工表达式", "category": "人工研究",
            "status": "candidate", "source": "quant_lab",
        },
        {
            "name": "GP 候选 2", "slug": "gp_2222222222",
            "description": "遗传规划候选", "category": "AI 发现",
            "status": "draft", "source": "quant_lab",
        },
    ]
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(viewport={"width": 1280, "height": 900})
        page.route("**/api/v1/research/factors", lambda route: route.fulfill(json={"factors": factors}))
        page.goto(url)
        page.get_by_role("button", name="回测", exact=True).click()
        page.locator('#bt-form [name="strategy"]').select_option("factor")
        factor_input = page.locator("#bt-factor-input")
        factor_input.fill("mom_20d,")

        menu = page.locator("#bt-factor-options")
        menu.wait_for(state="visible")
        assert factor_input.get_attribute("aria-expanded") == "true"
        assert "人工反转" in menu.inner_text()
        assert "GP 候选 2" in menu.inner_text()
        assert "mom_20d" not in menu.inner_text()
        input_box = factor_input.bounding_box()
        menu_box = menu.bounding_box()
        assert menu_box["y"] >= input_box["y"] + input_box["height"] \
            or menu_box["y"] + menu_box["height"] <= input_box["y"]

        factor_input.type("人工")
        assert menu.locator('[role="option"]').count() == 1
        assert "Quant Lab 人工表达式" in menu.inner_text()
        factor_input.press("ArrowDown")
        factor_input.press("Enter")
        assert factor_input.input_value() == "mom_20d, 人工反转"
        assert factor_input.get_attribute("aria-expanded") == "false"

        factor_input.type(",")
        menu.wait_for(state="visible")
        assert "GP 候选 2" in menu.inner_text()
        assert "人工反转" not in menu.inner_text()
        menu.locator('[role="option"]', has_text="GP 候选 2").click()
        assert factor_input.input_value() == "mom_20d, 人工反转, GP 候选 2"

        factor_input.press("Escape")
        page.locator(".factor-completion-trigger").click()
        menu.wait_for(state="visible")
        assert factor_input.get_attribute("aria-expanded") == "true"
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
        page.route("**/api/v1/automation/overview", lambda route: route.fulfill(json=overview))
        page.route("**/api/v1/automation/audit*", audit_handler)
        page.route("**/api/v1/automation/targets/*/policy", policy_handler)
        page.route("**/api/v1/news/sources*", source_handler)
        page.goto(url)

        page.get_by_role("button", name="自动化", exact=True).click()
        page.locator('[data-target-card="feishu_owner"]').wait_for()
        assert not page.locator("#automation-audit-panel").evaluate("element => element.open")
        assert audit_requests["count"] == 0

        for kind in ("important_news", "market_turn", "market_close", "task_report", "task_failure"):
            page.locator(f'[data-target="feishu_owner"][data-event-type="{kind}"]').uncheck()
            _wait_for_class(page.locator(
                '[data-target-card="feishu_owner"] .target-feedback',
            ), "success")
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
        _wait_for_class(page.locator("#news-source-feedback"), "success")
        assert "已保存" in page.locator("#news-source-feedback").inner_text()
        browser.close()


def test_rotation_deep_links_cold_states_and_narrow_layout(live_server):
    url, _ = live_server
    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        page = browser.new_page(
            viewport={"width": 390, "height": 844},
            reduced_motion="reduce",
        )
        page_errors = []
        page.on("pageerror", lambda error: page_errors.append(str(error)))
        page.goto(f"{url}/#market/temperature")

        page.locator("#market-temperature-view").wait_for(state="visible")
        assert page.locator("#market-temperature-view h2").inner_text() == "市场温度"
        assert page.locator("#market-quotes-view").is_hidden()
        _wait_for_text(page.locator("#market-temperature-content"), "等待")

        page.get_by_role("tab", name="市场风格", exact=True).click()
        page.locator("#market-style-view").wait_for(state="visible")
        assert page.url.endswith("#market/style")

        page.get_by_role("button", name="轮动", exact=True).click()
        page.locator("#rotation-overview-view").wait_for(state="visible")
        assert page.url.endswith("#rotation/overview")
        _wait_for_text(page.locator("#rotation-overview-content"), "等待")
        assert page.locator("#rotation-overview-view #rotation-industry-scatter").count() == 0
        page.goto(f"{url}/#rotation/radar")
        page.locator("#rotation-overview-view").wait_for(state="visible")
        assert page.url.endswith("#rotation/overview")
        assert "· 0%" not in page.locator(
            '[data-rotation-meta="rotation"] .rotation-meta-line'
        ).inner_text()
        page.get_by_role("tab", name="行业周期", exact=True).click()
        page.locator("#rotation-industry-view").wait_for(state="visible")
        page.get_by_role("tab", name="细分题材", exact=True).click()
        page.locator("#rotation-themes-view").wait_for(state="visible")
        playwright_sync.expect(page.locator("#rotation-themes-content")).to_contain_text(
            re.compile("等待|计算"), timeout=30_000,
        )
        theme_meta = page.locator(
            '[data-rotation-meta="rotation"] .rotation-meta-line'
        ).inner_text()
        assert "等待快照" in theme_meta or "正在计算" in theme_meta
        assert "· 0%" not in theme_meta
        page.get_by_role("tab", name="宽基资金", exact=True).click()
        page.locator("#rotation-etf-view").wait_for(state="visible")

        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        assert page_errors == []
        browser.close()


def test_stock_analysis_progressive_restore_and_reduced_motion(live_server):
    url, _ = live_server
    keys = [
        ("fundamental", "①", "基本面"), ("technical", "②", "技术面"),
        ("news", "③", "消息面"), ("capital", "④", "资金面"),
        ("sentiment", "⑤", "市场心理面"), ("macro", "⑥", "宏观/政策面"),
    ]
    dimensions = [{
        "key": key, "number": number, "title": title, "score": 61 + index,
        "stance": "谨慎偏强", "status": "complete", "summary": f"{title}证据已完成复核。",
        "metrics": [{"label": "样本指标", "value": index, "display": str(index), "note": ""}],
        "signals": ["证据支持当前方向。"], "risks": ["仍需等待后续数据。"],
        "as_of": "2026-07-30", "generation": "llm_assisted", "degraded_reason": "",
        "evidence_ids": [f"ev_{index:020d}"],
        "evidence": [{
            "id": f"ev_{index:020d}", "title": f"{title}来源", "value": {"sample": index},
            "excerpt": "可核查摘要", "published_at": "2026-07-30", "data_as_of": "2026-07-30",
            "source": {"name": "官方来源", "level": 1, "url": f"https://example.com/{key}"},
        }],
    } for index, (key, number, title) in enumerate(keys)]
    dimensions[0]["summary"] = {
        "text": "基本面结构化信封已转换为正文。",
        "evidence_ids": [dimensions[0]["evidence_ids"][0]],
    }
    report = {
        "schema_version": "2.0", "instrument": {
            "symbol": "600519.SH", "name": "贵州茅台", "market_label": "中国内地",
        },
        "quote": {"current": 1500, "change_pct": 1.25}, "data_as_of": "2026-07-30",
        "overall": {
            "score": 65.5, "stance": "谨慎偏强", "coverage": 100, "confidence": 85,
            "thesis": "六维证据总体偏强，但仍需等待新披露。", "summary": "终审已检查证据时点与冲突。",
            "risks": ["市场波动可能放大。"],
        },
        "dimensions": dimensions,
        "scenarios": [{
            "title": "基准情景", "priority": "当前主场景",
            "condition": "价格维持区间。", "response": "等待新证据。",
        }],
        "warnings": [], "research": {
            "mode": "deep", "elapsed_seconds": 128, "evidence_count": 6,
            "sources": [{"id": "src_1"}],
        },
        "disclaimer": "仅作量化研究与记录，不构成投资建议。",
    }
    submitted = []
    event_calls = {"count": 0}

    def route_api(route):
        request = route.request
        path = request.url.split("?", 1)[0]
        if path.endswith("/api/v1/market/stock-analyses") and request.method == "POST":
            submitted.append(request.post_data_json)
            route.fulfill(status=202, json={
                "analysis_id": "analysis-stock", "job_id": "job-stock", "status": "queued",
            })
        elif path.endswith("/api/v1/market/stock-analyses/analysis-stock"):
            route.fulfill(json={"analysis_id": "analysis-stock", "status": "completed", "report": report})
        elif path.endswith("/api/v1/jobs/job-stock/events"):
            event_calls["count"] += 1
            items = [] if event_calls["count"] > 1 else [
                {"seq": index + 1, "type": "dimension_completed", "payload": {
                    "dimension": item["key"], "result": item, "completed": index + 1,
                }} for index, item in enumerate(dimensions)
            ]
            route.fulfill(json={"items": items})
        elif path.endswith("/api/v1/jobs/job-stock"):
            route.fulfill(json={
                "id": "job-stock", "status": "completed", "progress": 100,
                "phase": "分析完成", "estimated_remaining_seconds": 0,
            })
        else:
            route.fallback()

    with playwright_sync.sync_playwright() as manager:
        browser = manager.chromium.launch()
        context = browser.new_context(viewport={"width": 1280, "height": 900}, reduced_motion="reduce")
        page = context.new_page()
        page.route("**/api/v1/**", route_api)
        page.goto(url)
        page.locator('[data-tab="stock-analysis"]').click()
        page.locator("#stock-analysis-query").fill("600519.SH")
        assert page.locator('input[name="mode"][value="deep"]').is_checked()
        page.locator("#stock-analysis-form button.primary").click()
        page.get_by_text("六维证据总体偏强，但仍需等待新披露。").wait_for()
        page.get_by_text("基本面结构化信封已转换为正文。").wait_for()

        assert submitted == [{"query": "600519.SH", "mode": "deep"}]
        assert page.locator(".sa-dimension").count() == 6
        assert "evidence_ids" not in page.locator("#stock-analysis-report").inner_text()
        assert page.locator('.sa-citations a[href="https://example.com/fundamental"]').count() == 1
        assert page.locator("#stock-analysis-elapsed").inner_text()
        assert page.evaluate("getComputedStyle(document.querySelector('.sa-report')).animationName") == "none"

        page.reload()
        page.locator('[data-tab="stock-analysis"]').click()
        page.get_by_text("六维证据总体偏强，但仍需等待新披露。").wait_for()
        restored = page.evaluate("JSON.parse(localStorage.getItem('qm.stock-analysis.active.v2'))")
        assert restored["jobId"] == "job-stock"
        browser.close()
