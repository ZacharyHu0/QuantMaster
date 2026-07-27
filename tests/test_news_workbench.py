"""资讯工作台：来源、缓存、API 与消息面因子测试（不触网）。"""

from __future__ import annotations

import json
import os
import time

import httpx
import pandas as pd
from fastapi.testclient import TestClient

from quantmaster.ai.crawler import NewsItem, NewsStore
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


def test_news_api_csrf_and_ui_contract():
    client = TestClient(app)
    assert client.get("/api/news/sources").status_code == 200
    assert client.post("/api/news/sources", json={"source": source_value()}).status_code == 403

    token = _issue_csrf()
    client.cookies.set("qm_csrf", token)
    response = client.post(
        "/api/news/sources", json={"source": source_value()},
        headers={"X-CSRF-Token": token},
    )
    assert response.status_code == 200
    assert response.json()["group_name"] == "periodic"

    page = client.get("/").text
    assert 'id="news-factor-chart"' in page
    assert 'data-settings-section="sources"' in page
    assert "/static/news.js" in page
    assert "/static/news.css" in page


def test_periodic_news_job_is_registered():
    assert DEFAULT_JOBS["fast_news_scan"][1]["minutes"] == 10
    assert DEFAULT_JOBS["official_news_scan"][1]["minutes"] == 15
    assert DEFAULT_JOBS["periodic_news_scan"][1]["minutes"] == 60
    assert "periodic_news_scan" in ALLOWED_TASKS
