from __future__ import annotations

import json

import numpy as np
import pandas as pd
from fastapi.testclient import TestClient

from quantmaster.analysis.stock import StockAnalysisService, analyze_technical
from quantmaster.automation.commands import stock_analysis_query
from quantmaster.automation.delivery import OutboxDispatcher
from quantmaster.automation.models import ActorContext
from quantmaster.automation.service import AutomationService
from quantmaster.automation.stock_cards import (
    stock_analysis_progress_card,
    stock_analysis_report_card,
)
from quantmaster.automation.store import AutomationStore
from quantmaster.server.app import app


def sample_bars() -> pd.DataFrame:
    dates = pd.bdate_range("2025-01-02", periods=180)
    trend = np.linspace(90, 126, len(dates))
    close = trend + np.sin(np.arange(len(dates)) / 5) * 2
    return pd.DataFrame({
        "open": close - 0.4,
        "high": close + 1.2,
        "low": close - 1.1,
        "close": close,
        "volume": np.linspace(1_000_000, 1_800_000, len(dates)),
        "amount": close * np.linspace(1_000_000, 1_800_000, len(dates)),
    }, index=dates)


def build_service() -> StockAnalysisService:
    bars = sample_bars()
    symbol = "600519.SH"
    dates = bars.index
    fundamentals = {
        "pe_ttm": pd.DataFrame({symbol: np.linspace(28, 22, len(dates))}, index=dates),
        "pb": pd.DataFrame({symbol: np.linspace(8, 6, len(dates))}, index=dates),
        "dv_ratio": pd.DataFrame({symbol: np.linspace(1.2, 2.1, len(dates))}, index=dates),
        "total_mv": pd.DataFrame({symbol: np.linspace(18_000_000, 20_000_000, len(dates))}, index=dates),
        "roe": pd.DataFrame({symbol: np.linspace(18, 22, len(dates))}, index=dates),
    }
    return StockAnalysisService(
        resolver=lambda query: {
            "status": "resolved",
            "instrument": {
                "symbol": symbol, "code": "600519", "name": "贵州茅台",
                "market": "CN", "market_label": "中国内地", "exchange": "SH",
                "asset_type": "stock", "currency": "CNY",
            },
        },
        history_loader=lambda *args, **kwargs: bars,
        fundamental_loader=lambda *args: fundamentals,
        news_loader=lambda *args: [{
            "title": "公司发布年度经营数据", "summary": "经营保持稳定",
            "sentiment": 0.35, "importance_score": 82, "source_name": "交易所公告",
            "published_at": "2025-09-10", "url": "https://example.com/notice",
            "sectors": ["白酒"],
        }],
        capital_loader=lambda *args: {
            "main_force": 25_000_000, "super_large": 18_000_000,
            "large": 7_000_000, "main_pct": 3.2, "date": "2025-09-10",
        },
        industry_loader=lambda *args: "白酒",
        llm_factory=None,
    )


def test_technical_analysis_has_full_indicator_set():
    result = analyze_technical(sample_bars())

    assert result["status"] == "complete"
    labels = {item["label"] for item in result["metrics"]}
    assert {"MA5 / MA20", "MA60", "RSI(14)", "MACD 柱", "K / D"}.issubset(labels)
    assert {"BOLL 上 / 下", "ATR(14)", "20 日支撑 / 压力", "5/20 日量比"}.issubset(labels)
    assert 0 <= result["score"] <= 100
    assert result["as_of"]


def test_stock_analysis_service_generates_six_dimensions_and_progress():
    events = []
    report = build_service().analyze(
        "贵州茅台",
        lambda progress, phase, detail="", **kwargs: events.append((progress, phase, detail)),
    )

    assert [item["key"] for item in report["dimensions"]] == [
        "fundamental", "technical", "news", "capital", "sentiment", "macro",
    ]
    assert events[0][0] == 5
    assert events[-1][0] == 100
    assert report["instrument"]["symbol"] == "600519.SH"
    assert report["overall"]["coverage"] >= 90
    assert report["overall"]["thesis"]
    assert len(report["scenarios"]) == 3
    assert report["generation_mode"] == "rules_only"
    assert "不构成投资建议" in report["disclaimer"]


def test_stock_analysis_intent_does_not_capture_market_query():
    assert stock_analysis_query("六维分析 600519") == "600519"
    assert stock_analysis_query("帮我看看贵州茅台") == "贵州茅台"
    assert stock_analysis_query("贵州茅台怎么样？") == "贵州茅台"
    assert stock_analysis_query("现在大盘怎么样？") == ""
    assert stock_analysis_query("查看今天的持仓") == ""


def test_feishu_cards_show_progress_and_complete_dimensions():
    report = build_service().analyze("600519", lambda *args, **kwargs: None)
    progress = stock_analysis_progress_card("600519", 54, "核查基本面", "读取估值与 ROE")
    progress_text = progress["elements"][0]["text"]["content"]
    assert "54%" in progress_text
    assert "核查基本面" in progress_text
    assert "原位更新" in progress["elements"][1]["elements"][0]["content"]

    card = stock_analysis_report_card(report)
    assert "贵州茅台" in card["header"]["title"]["content"]
    contents = "\n".join(
        item.get("text", {}).get("content", "") for item in card["elements"]
        if item.get("tag") == "div"
    )
    assert "① 基本面" in contents
    assert "⑥ 宏观/政策面" in contents
    assert "情景验证" in contents


def test_feishu_service_updates_one_card_to_final_report(tmp_path, monkeypatch):
    store = AutomationStore(tmp_path / "automation.sqlite")
    actor = ActorContext(
        channel="feishu", account_id="cli_app", target="oc_group",
        chat_type="group", sender_id="ou_user",
    )
    store.bind_target(
        "feishu_group", target=actor.target, account_id=actor.account_id,
        owner_actor="feishu:cli_app:ou_owner", actor="test",
    )
    report = build_service().analyze("600519", lambda *args, **kwargs: None)

    class FakeAnalyzer:
        def analyze(self, query, progress):
            progress(22, "读取行情", "正在读取")
            progress(68, "整理消息与资金", "正在整理")
            progress(100, "分析完成", "已完成", level="success")
            return report

    class FakeFeishu:
        def __init__(self):
            self.sent = []
            self.updated = []

        def send_card(self, *, chat_id, card):
            self.sent.append((chat_id, card))
            return "om_analysis"

        def update_card(self, *, message_id, card):
            self.updated.append((message_id, card))

    monkeypatch.setattr("quantmaster.analysis.stock.StockAnalysisService", FakeAnalyzer)
    service = AutomationService(store, OutboxDispatcher(store))
    service.feishu = FakeFeishu()

    result = service.handle_stock_analysis(actor, "600519")

    assert result["status"] == "completed"
    assert len(service.feishu.sent) == 1
    assert len(service.feishu.updated) >= 3
    assert service.feishu.updated[-1][1]["header"]["title"]["content"].endswith("六维分析")
    memory = store.conversation_context(
        channel="feishu", account_id=actor.account_id, chat_id=actor.target,
    )
    assert "六维分析完成" in memory[-1]["text"]


def test_stock_analysis_web_stream_uses_shared_engine(monkeypatch):
    report = build_service().analyze("600519", lambda *args, **kwargs: None)

    class FakeAnalyzer:
        def analyze(self, query, progress):
            progress(38, "计算技术面", "正在计算")
            progress(100, "分析完成", "已完成", level="success")
            return report

    monkeypatch.setattr("quantmaster.analysis.stock.StockAnalysisService", FakeAnalyzer)
    client = TestClient(app)
    client.headers["X-CSRF-Token"] = client.get("/api/v1/session").json()["csrf_token"]
    response = client.post("/api/v1/research/stock-analysis/stream", json={"query": "600519"})
    events = [json.loads(line) for line in response.text.splitlines() if line.strip()]

    assert response.status_code == 200
    assert [event["type"] for event in events] == ["progress", "progress", "result"]
    assert events[-1]["data"]["instrument"]["symbol"] == "600519.SH"
