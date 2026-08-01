"""资讯工作台：来源、缓存、API 与消息面因子测试（不触网）。"""

from __future__ import annotations

import json
import os
import time

import httpx
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from quantmaster.ai.crawler import AICrawler, NewsItem, NewsStore
from quantmaster.ai.news_sources import (
    NewsSourceStore,
    _request_headers,
    _without_auth,
    fetch_declarative_source,
)
from quantmaster.ai.sentiment import quality_sentiment_panel
from quantmaster.automation.service import ALLOWED_TASKS
from quantmaster.automation.store import DEFAULT_JOBS
from quantmaster.credentials import CredentialStore
from quantmaster.server.app import app
from quantmaster.server.management import _issue_csrf


class FakeCredentials:
    def __init__(self):
        self.values: dict[str, str] = {}

    def get(self, target: str) -> str | None:
        return self.values.get(target)

    def set(self, target: str, value: str) -> None:
        self.values[target] = value

    def delete(self, target: str) -> None:
        self.values.pop(target, None)


def source_value(**overrides) -> dict:
    value = {
        "name": "研究 RSS", "kind": "rss", "enabled": True,
        "group_name": "periodic", "url": "https://example.com/feed.xml",
        "item_limit": 20, "factor_weight": 1.2, "is_official": False,
        "parser": {}, "auth_type": "none", "auth_header": "",
    }
    value.update(overrides)
    return value


def test_source_crud_and_dynamic_credentials(tmp_path):
    credentials = FakeCredentials()
    store = NewsSourceStore(tmp_path / "news.sqlite", credentials=credentials)
    assert {item["id"] for item in store.list()} >= {
        "sina_live", "eastmoney_fast", "csrc", "sse", "szse",
    }

    created = store.create(source_value(
        name="鉴权 JSON", kind="json", auth_type="bearer",
        parser={"items_path": "data.items", "title_path": "title"},
    ), token="top-secret")
    target = CredentialStore.news_source_target(created["id"])
    assert credentials.values[target] == "top-secret"
    assert created["auth_configured"] is True
    assert "top-secret" not in json.dumps(created, ensure_ascii=False)

    updated = store.update(created["id"], {"enabled": False, "factor_weight": 0.5})
    assert updated["enabled"] is False
    assert updated["factor_weight"] == 0.5
    store.delete(created["id"])
    assert store.get(created["id"]) is None
    assert target not in credentials.values


def test_declarative_parsers(monkeypatch, tmp_path):
    store = NewsSourceStore(tmp_path / "news.sqlite", credentials=FakeCredentials())
    payloads = {
        "rss": b"<rss><channel><item><title>RSS title</title><description>RSS body</description>"
               b"<link>https://example.com/a</link></item></channel></rss>",
        "json": json.dumps({"data": {"items": [{"title": "JSON title", "body": "JSON body"}]}}).encode(),
        "html": b'<ul><li class="news"><a href="/a">HTML title</a><p>HTML body</p></li></ul>',
    }

    def fake_fetch(source, url, store_value, preview=False):
        return payloads[source["kind"]], url, "news_raw/test/raw.gz"

    monkeypatch.setattr("quantmaster.ai.news_sources._fetch_bytes", fake_fetch)
    rss = fetch_declarative_source(source_value(id="test_rss"), store)
    json_items = fetch_declarative_source(source_value(
        id="test_json", kind="json", url="https://example.com/api",
        parser={"items_path": "data.items", "title_path": "title", "content_path": "body"},
    ), store)
    html_items = fetch_declarative_source(source_value(
        id="test_html", kind="html", url="https://example.com/news",
        parser={"item_selector": "li.news", "title_selector": "a",
                "content_selector": "p", "url_selector": "a"},
    ), store)
    assert [rss[0].title, json_items[0].title, html_items[0].title] == [
        "RSS title", "JSON title", "HTML title",
    ]
    assert html_items[0].url == "https://example.com/a"


def test_auth_headers_are_removed_for_cross_origin_detail():
    source = source_value(auth_type="bearer")
    headers = _request_headers(source, "do-not-leak")
    assert headers["Authorization"] == "Bearer do-not-leak"
    assert "Authorization" not in _without_auth(headers, source)


def test_source_rejects_credentials_in_url(tmp_path):
    store = NewsSourceStore(tmp_path / "news.sqlite", credentials=FakeCredentials())
    with pytest.raises(ValueError, match="API Token"):
        store.create(source_value(url="https://example.com/feed?api_key=secret"))


def test_raw_response_cleanup(tmp_path):
    store = NewsSourceStore(tmp_path / "news.sqlite", credentials=FakeCredentials())
    key = store.save_response(
        "sina_live", "https://example.com/feed", b"cached response",
        httpx.Headers({"etag": "v1"}), 200,
    )
    path = tmp_path / key
    old = time.time() - 10 * 86400
    os.utime(path, (old, old))
    assert store.cleanup_raw(7) == 1
    assert not path.exists()


def test_quality_factor_uses_first_seen_and_defers_after_close(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    store.save([NewsItem(
        source="test", title="盘后利空", content="盘后利空",
        symbols=["600519.SH"], sentiment=-0.8, confidence=1,
        importance_score=100, analysis_status="complete",
    )])
    first_seen = pd.Timestamp("2024-05-06 16:00", tz="Asia/Shanghai").timestamp()
    with store._conn() as conn:
        conn.execute(
            "UPDATE news SET first_seen_at=?,analysis_status='complete'",
            (first_seen,),
        )
    index = pd.bdate_range("2024-05-06", "2024-05-08")
    factor = quality_sentiment_panel(index, ["600519.SH"], store=store)
    assert pd.isna(factor.loc["2024-05-06", "600519.SH"])
    assert factor.loc["2024-05-07", "600519.SH"] == -0.8
    assert -0.8 < factor.loc["2024-05-08", "600519.SH"] < 0


def test_news_list_truncates_body_but_detail_is_complete(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    content = "正文" * 1500
    store.save([NewsItem(source="test", title="长正文", content=content)])
    listed = store.recent(1)[0]
    assert listed["content_truncated"] is True
    assert len(listed["content"]) == 2000
    assert store.detail(listed["id"])["content"] == content


def test_llm_annotations_are_constrained():
    item = NewsItem(source="test", title="不可信输出", content="内容")
    from quantmaster.ai.crawler import AICrawler

    AICrawler._apply_result(item, {
        "symbols": ["600519.sh", "DROP TABLE", "600519.SH"],
        "sectors": ["电子", "DROP TABLE", "化工", "电子"],
        "event_type": "任意代码", "sentiment": "nan", "confidence": "inf",
        "scope": "private", "urgency": "now", "summary": "摘要",
    }, {"600519.SH": "食品饮料"})
    assert item.symbols == ["600519.SH"]
    assert item.sectors == ["电子", "基础化工", "食品饮料"]
    assert item.event_type == "其他"
    assert item.sentiment == 0
    assert item.confidence == 0
    assert item.scope == "market"
    assert item.urgency == "normal"


def test_news_stats_calculate_market_and_independent_sector_scores(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    store._industry_map = {}
    store.save([
        NewsItem(
            source="test", title="电子需求回暖", content="电子需求回暖", sectors=["电子"],
            sentiment=0.8, confidence=1, importance_score=100, analysis_status="complete",
        ),
        NewsItem(
            source="test", title="电子成本上升", content="电子成本上升", sectors=["电子"],
            sentiment=-0.2, confidence=1, importance_score=100, analysis_status="complete",
        ),
        NewsItem(
            source="test", title="银行息差承压", content="银行息差承压", sectors=["银行"],
            sentiment=-0.5, confidence=1, importance_score=100, analysis_status="complete",
        ),
        NewsItem(
            source="mirror", title="电子需求回暖转载", content="电子需求回暖", sectors=["电子"],
            sentiment=-1, confidence=0.9, importance_score=100, analysis_status="complete",
        ),
    ])

    assert next(item for item in store.recent(10) if item["title"] == "银行息差承压")["sectors"] == ["银行"]
    stats = store.stats(30)
    assert stats["total"] == 4
    assert stats["market_sentiment"]["score"] == pytest.approx(3.33, abs=0.02)
    assert stats["market_sentiment"]["label"] == "中性"
    assert stats["market_sentiment"]["event_count"] == 3
    sectors = {item["sector"]: item for item in stats["sector_scores"]}
    assert sectors["电子"]["score"] == pytest.approx(30.0, abs=0.02)
    assert sectors["电子"]["event_count"] == 2
    assert sectors["电子"]["positive"] == 1
    assert sectors["银行"]["score"] == pytest.approx(-50.0, abs=0.02)
    assert sectors["银行"]["label"] == "明显偏空"
    assert stats["display_scale"] == {
        "mode": "adaptive_bucket_v1",
        "theoretical_abs_max": 100,
        "market_abs_max": 10,
        "sector_abs_max": 60,
    }


def test_annotation_stream_yields_each_persisted_batch(tmp_path):
    from quantmaster.ai.crawler import AICrawler

    class FakeLLM:
        def chat_json(self, prompt, system=""):
            count = int(prompt.split("分析以下 ", 1)[1].split(" 条", 1)[0])
            return [{
                "symbols": ["600519.SH"], "sectors": ["食品饮料"], "event_type": "业绩",
                "sentiment": 0.4, "summary": f"批次标注 {index + 1}",
                "scope": "market", "urgency": "normal", "confidence": 0.8,
            } for index in range(count)]

    store = NewsStore(tmp_path / "news.sqlite")
    store.save([
        NewsItem(source="test", title=f"待标注 {index}", content=f"正文 {index}")
        for index in range(7)
    ])
    events = AICrawler(client=FakeLLM(), store=store).enrich_pending_events(
        limit=7, batch_size=3,
    )
    started = next(events)
    assert started == {
        "type": "start", "total": 7, "processed": 0,
        "completed": 0, "failed": 0, "batch_count": 3,
    }

    first_batch = next(events)
    assert first_batch["type"] == "batch"
    assert first_batch["processed"] == 3
    assert first_batch["completed"] == 3
    assert len(first_batch["updated_items"]) == 3
    assert first_batch["updated_items"][0]["sectors"] == ["食品饮料"]
    assert len(store.query(status="complete", limit=20)["items"]) == 3
    assert len(store.query(status="pending", limit=20)["items"]) == 4

    remaining = list(events)
    assert [event["type"] for event in remaining] == ["batch", "batch", "complete"]
    assert remaining[-1]["completed"] == 7
    assert len(store.query(status="complete", limit=20)["items"]) == 7


def test_annotation_stream_api_contract(monkeypatch):
    from quantmaster.server import news as news_module

    class FakeStore:
        def reset_analysis(self, ids):
            return len(ids or [])

    class FakeCrawler:
        def __init__(self):
            self.store = FakeStore()

        def enrich_pending_events(self, **kwargs):
            yield {"type": "start", "total": 2, "processed": 0,
                   "completed": 0, "failed": 0, "batch_count": 1}
            yield {"type": "batch", "batch": 1, "batch_count": 1,
                   "processed": 2, "total": 2, "completed": 2, "failed": 0,
                   "batch_completed": 2, "batch_failed": 0,
                   "completed_ids": [1, 2], "updated_items": [], "error": ""}
            yield {"type": "complete", "processed": 2, "completed": 2,
                   "failed": 0, "completed_ids": [1, 2]}

    monkeypatch.setattr(news_module, "AICrawler", FakeCrawler)
    client = TestClient(app)
    token = _issue_csrf()
    client.cookies.set("qm_csrf", token)
    with client.stream(
        "POST", "/api/v1/news/reanalyze/stream",
        json={"limit": 10, "batch_size": 2}, headers={"X-CSRF-Token": token},
    ) as response:
        events = [json.loads(line) for line in response.iter_lines() if line]
        assert response.status_code == 200
        assert response.headers["X-Accel-Buffering"] == "no"
    assert [event["type"] for event in events] == ["start", "batch", "complete"]


def test_news_api_csrf_and_ui_contract():
    client = TestClient(app)
    assert client.get("/api/v1/news/sources").status_code == 200
    assert client.post("/api/v1/news/sources", json={"source": source_value()}).status_code == 403

    token = _issue_csrf()
    client.cookies.set("qm_csrf", token)
    response = client.post(
        "/api/v1/news/sources", json={"source": source_value()},
        headers={"X-CSRF-Token": token},
    )
    assert response.status_code == 200
    assert response.json()["group_name"] == "periodic"

    page = client.get("/").text
    assert 'id="news-factor-chart"' in page
    assert '<div id="news-factor-chart"' in page
    assert '<svg id="news-factor-chart"' not in page
    assert 'id="news-market-label"' in page
    assert 'id="news-sector-scores"' in page
    assert 'id="news-annotation-progress"' in page
    assert 'data-settings-section="sources"' in page
    assert "/static/news.js" in page
    assert "/static/news.css" in page
    chart_source = client.get("/static/news.js").text
    assert "mkChart('news-factor-chart')" in chart_source
    assert "CHART_COLORS.primary" in chart_source


def test_periodic_news_job_is_registered():
    assert DEFAULT_JOBS["fast_news_scan"][1]["minutes"] == 10
    assert DEFAULT_JOBS["official_news_scan"][1]["minutes"] == 15
    assert DEFAULT_JOBS["periodic_news_scan"][1]["minutes"] == 60
    assert "periodic_news_scan" in ALLOWED_TASKS


def test_analysis_failures_enter_bounded_dead_letter_and_recover(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    store.save([NewsItem(source="unit", title="待恢复资讯", content="测试内容")])
    item_id = store.max_id()

    for _ in range(3):
        store.analysis_failure([item_id], "request timeout", "timeout")

    dead = store.detail(item_id)
    assert dead["analysis_status"] == "dead_letter"
    assert dead["analysis_attempts"] == 3
    assert dead["last_failure_code"] == "timeout"
    with store._conn() as connection:
        connection.execute("UPDATE news SET next_retry_at=0 WHERE id=?", (item_id,))

    class HealthyClient:
        def chat_json(self, prompt, system=None):
            return [{
                "symbols": [], "sectors": [], "event_type": "其他",
                "sentiment": 0, "summary": "恢复完成", "scope": "market",
                "urgency": "normal", "confidence": 0.8,
            }]

    result = AICrawler(client=HealthyClient(), store=store).recover_dead_letters(limit=20)
    recovered = store.detail(item_id)
    assert result["selected"] == 1
    assert result["completed"] == 1
    assert recovered["analysis_status"] == "complete"
    assert recovered["analysis_recovery_count"] == 1
