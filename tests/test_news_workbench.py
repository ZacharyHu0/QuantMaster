"""资讯工作台：来源、缓存、API 与消息面因子测试（不触网）。"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from quantmaster.ai.crawler import AICrawler, NewsItem, NewsStore, _claim_heartbeat
from quantmaster.ai.llm import LLMError
from quantmaster.ai.news_contracts import FetchBatch
from quantmaster.ai.news_sources import (
    NewsSourceStore,
    _request_headers,
    _without_auth,
    fetch_declarative_source,
)
from quantmaster.ai.sentiment import news_sentiment_readiness, quality_sentiment_panel
from quantmaster.automation.news import importance_score as alert_importance_score
from quantmaster.automation.service import ALLOWED_TASKS
from quantmaster.automation.store import DEFAULT_JOBS
from quantmaster.credentials import CredentialError, CredentialStore
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


def test_claim_heartbeat_retries_sqlite_lock_and_stops_cleanly() -> None:
    retried = threading.Event()

    class Claims:
        def __init__(self) -> None:
            self.calls = 0

        def heartbeat(self, token: str, owner: str, *, lease_seconds: float) -> int:
            assert (token, owner, lease_seconds) == ("claim-token", "worker", 1.0)
            self.calls += 1
            if self.calls == 1:
                raise sqlite3.OperationalError("database is locked")
            retried.set()
            return 1

    claims = Claims()
    with _claim_heartbeat(claims, "claim-token", "worker", lease_seconds=1.0) as alive:
        assert retried.wait(3)
        assert alive.is_set()

    assert claims.calls >= 2
    assert not any(
        thread.name == "qm-news-claim-heartbeat" and thread.is_alive()
        for thread in threading.enumerate()
    )


def source_value(**overrides) -> dict:
    value = {
        "name": "研究 RSS", "kind": "rss", "enabled": True,
        "group_name": "periodic", "url": "https://example.com/feed.xml",
        "item_limit": 20, "factor_weight": 1.2, "is_official": False,
        "max_age_hours": 1080,
        "parser": {}, "auth_type": "none", "auth_header": "",
    }
    value.update(overrides)
    return value


def official_news(store: NewsStore, **values) -> NewsItem:
    values.setdefault("source", "sse")
    values.setdefault("is_official", True)
    values.setdefault("content_scope", "full_article")
    hosts = {
        "sse": "www.sse.com.cn",
        "pboc": "www.pbc.gov.cn",
        "nbs_release": "www.stats.gov.cn",
        "nbs_interpretation": "www.stats.gov.cn",
        "ndrc": "www.ndrc.gov.cn",
    }
    identity = uuid.uuid4().hex
    values.setdefault("url", f"https://{hosts[str(values['source'])]}/evidence/{identity}")
    values.setdefault("provider_item_id", str(values["url"]))
    published_at_epoch = float(values.setdefault("published_at_epoch", time.time()))
    values.setdefault(
        "published_at", datetime.fromtimestamp(published_at_epoch, UTC).isoformat(),
    )
    if not values.get("raw_cache_key"):
        payload = str(values.get("content") or values.get("title") or "").encode("utf-8")
        values["raw_cache_key"] = store.sources.save_response(
            str(values["source"]), str(values["url"]),
            payload, httpx.Headers(), 200, official=True,
        )
    item = NewsItem(**values)
    store.sources.bind_articles([item])
    store.sources.register_ingest_batch(
        FetchBatch(
            source_id=item.source,
            articles=[item],
            watermark=item.provider_item_id,
            complete=True,
        ),
        f"test-{identity}",
    )
    return item


class _RouteNewsSources:
    def __init__(self, calls):
        self.calls = calls
        self.values = {"source-1": {"id": "source-1", **source_value()}}

    def list(self):
        return list(self.values.values())

    def create(self, value, token=""):
        if value["name"] == "boom":
            raise CredentialError("credential unavailable")
        self.calls.append(("create", token))
        return {"id": "created", **value}

    def update(self, source_id, value, token_action="keep", token=""):
        if source_id == "missing":
            raise KeyError("资讯来源不存在")
        self.calls.append(("update", (token_action, token)))
        return {"id": source_id, **value}

    def delete(self, source_id):
        if source_id == "missing":
            raise KeyError("资讯来源不存在")
        self.calls.append(("delete", source_id))

    def get(self, source_id):
        return self.values.get(source_id)


class _RouteNewsStore:
    def __init__(self, calls):
        self.calls = calls

    def reset_analysis(self, ids):
        self.calls.append(("reset", tuple(ids)))
        return len(ids)

    def stats(self, days=30):
        return {"days": days}

    def event_focus(self, days=7):
        return {"days": days, "top_symbols": []}

    def query(self, **kwargs):
        self.calls.append(("query", kwargs))
        return {"items": [], "filters": kwargs}

    def detail(self, item_id):
        return None if item_id == 404 else {"id": item_id}


class _RouteNewsCrawler:
    def __init__(self, calls):
        self.calls = calls
        self.source_store = _RouteNewsSources(calls)
        self.store = _RouteNewsStore(calls)

    def preview(self, value, token=""):
        self.calls.append(("preview", token))
        return [{"title": value["name"]}]

    def _fetch_source(self, source, limit=3, preview=False):
        self.calls.append(("test", (limit, preview)))
        return [NewsItem(
            source=source["id"], title="sample", content="body", url="https://example.com",
            published_at="2026-08-04T09:00:00+08:00",
        )]

    def run(self, **kwargs):
        self.calls.append(("run", kwargs))
        return {"status": "ok", **kwargs}

    def recover_dead_letters(self, **kwargs):
        self.calls.append(("dead_letter", kwargs))
        return {"mode": "dead_letter"}

    def retry_failed(self, **kwargs):
        self.calls.append(("failed", kwargs))
        return {"mode": "failed"}

    def enrich_pending(self, **kwargs):
        self.calls.append(("pending", kwargs))
        return {"mode": "pending", "claimed": 0}


def test_source_crud_and_dynamic_credentials(tmp_path):
    credentials = FakeCredentials()
    store = NewsSourceStore(tmp_path / "news.sqlite", credentials=credentials)
    assert {item["id"] for item in store.list()} >= {
        "sina_live", "sse", "pboc",
        "nbs_release", "nbs_interpretation", "ndrc",
    }
    assert "eastmoney_fast" in {item["id"] for item in store.list(enabled=True)}

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


def test_source_list_projects_last_run_progress_without_exposing_secrets(tmp_path):
    store = NewsSourceStore(tmp_path / "news.sqlite", credentials=FakeCredentials())
    source = store.create(source_value(name="进度来源"))
    run_id = store.start_run(source["id"])
    store.finish_run(run_id, fetched=12, saved=7, pending=5, status="degraded")

    listed = next(item for item in store.list() if item["id"] == source["id"])
    assert listed["last_fetched"] == 12
    assert listed["last_saved"] == 7
    assert listed["last_pending"] == 5
    assert "top-secret" not in json.dumps(listed, ensure_ascii=False).lower()


def test_declarative_parsers(monkeypatch, tmp_path):
    store = NewsSourceStore(tmp_path / "news.sqlite", credentials=FakeCredentials())
    payloads = {
        "rss": b"<rss><channel><item><title>RSS title</title><description>RSS body</description>"
               b"<link>https://example.com/a</link>"
               b"<pubDate>Sun, 09 Aug 2026 10:00:00 +0800</pubDate></item></channel></rss>",
        "json": json.dumps({"data": {"items": [{
            "title": "JSON title", "body": "JSON body", "published": "2026-08-09 10:00:00",
        }]}}).encode(),
        "html": (
            b'<ul><li class="news"><a href="/a">HTML title</a><p>HTML body</p>'
            b'<time>2026-08-09 10:00:00</time></li></ul>'
        ),
    }

    def fake_fetch(source, url, store_value, preview=False):
        return payloads[source["kind"]], url, "news_raw/test/raw.gz"

    monkeypatch.setattr("quantmaster.ai.news_sources._fetch_bytes", fake_fetch)
    rss = fetch_declarative_source(source_value(id="test_rss"), store).articles
    json_items = fetch_declarative_source(source_value(
        id="test_json", kind="json", url="https://example.com/api",
        parser={
            "items_path": "data.items", "title_path": "title", "content_path": "body",
            "published_at_path": "published",
        },
    ), store).articles
    html_items = fetch_declarative_source(source_value(
        id="test_html", kind="html", url="https://example.com/news",
        parser={"item_selector": "li.news", "title_selector": "a",
                "content_selector": "p", "url_selector": "a",
                "published_at_selector": "time"},
    ), store).articles
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
        "pboc", "https://www.pbc.gov.cn/evidence/feed", b"cached response",
        httpx.Headers({"etag": "v1"}), 200, official=True,
    )
    path = tmp_path / key
    old = time.time() - 10 * 86400
    os.utime(path, (old, old))
    assert key in store.raw_gc_candidates(7)
    assert store.cleanup_raw(7) == 0
    assert path.exists()


def test_quality_factor_uses_publication_time_and_defers_after_close(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    first_seen = pd.Timestamp("2024-05-06 16:00", tz="Asia/Shanghai").timestamp()
    store.save([official_news(store,
        title="盘后利空", content="盘后利空",
        symbols=["600519.SH"], sentiment=-0.8, confidence=1,
        importance_score=100, analysis_status="complete",
        published_at_epoch=first_seen,
    )])
    with store._conn() as conn:
        conn.execute(
            "UPDATE news SET first_seen_at=?,content_version_at=?,analysis_updated_at=?,"
            "analysis_status='complete'",
            (first_seen, first_seen, first_seen),
        )
    index = pd.bdate_range("2024-05-06", "2024-05-08")
    factor = quality_sentiment_panel(index, ["600519.SH"], store=store)
    assert pd.isna(factor.loc["2024-05-06", "600519.SH"])
    assert factor.loc["2024-05-07", "600519.SH"] == -0.8
    assert -0.8 < factor.loc["2024-05-08", "600519.SH"] < 0


def test_quality_factor_backfills_late_analysis_to_publication_day(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    published = pd.Timestamp("2024-01-02 09:00", tz="Asia/Shanghai").timestamp()
    analyzed = pd.Timestamp("2024-01-10 10:00", tz="Asia/Shanghai").timestamp()
    store.save([official_news(
        store,
        title="延迟完成分析",
        content="延迟完成分析",
        symbols=["600519.SH"],
        sentiment=0.8,
        confidence=1,
        importance_score=100,
        analysis_status="complete",
        published_at_epoch=published,
    )])
    with store._conn() as conn:
        conn.execute(
            "UPDATE news SET first_seen_at=?,content_version_at=?,"
            "analysis_updated_at=?,analysis_status='complete'",
            (published, published, analyzed),
        )

    index = pd.bdate_range("2024-01-02", "2024-01-12")
    factor = quality_sentiment_panel(index, ["600519.SH"], store=store)

    assert factor.loc["2024-01-02", "600519.SH"] == pytest.approx(0.8)
    assert factor.loc["2024-01-03", "600519.SH"] < 0.8
    assert factor.loc["2024-01-10", "600519.SH"] > 0


def test_quality_factor_treats_seconds_after_close_as_next_session(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    available = pd.Timestamp("2024-05-06 15:00:59", tz="Asia/Shanghai").timestamp()
    store.save([official_news(
        store,
        title="收盘后五十九秒",
        content="收盘后五十九秒",
        symbols=["600519.SH"],
        sentiment=0.6,
        confidence=1,
        importance_score=100,
        analysis_status="complete",
        published_at_epoch=available,
    )])
    with store._conn() as conn:
        conn.execute(
            "UPDATE news SET first_seen_at=?,content_version_at=?,"
            "analysis_updated_at=?,analysis_status='complete'",
            (available, available, available),
        )

    index = pd.bdate_range("2024-05-06", "2024-05-07")
    factor = quality_sentiment_panel(index, ["600519.SH"], store=store)

    assert pd.isna(factor.loc["2024-05-06", "600519.SH"])
    assert factor.loc["2024-05-07", "600519.SH"] == pytest.approx(0.6)


def test_sandbox_factor_previews_short_incomplete_sina_without_future_analysis(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    sessions = pd.bdate_range("2026-07-27", periods=10)
    articles = []
    for number, day in enumerate(sessions, start=1):
        published = day.tz_localize("Asia/Shanghai") + pd.Timedelta(hours=10)
        articles.append(NewsItem(
            source="sina_live",
            title=f"新浪快讯 {number}",
            content=f"新浪快讯正文 {number}",
            url=f"https://zhibo.sina.com.cn/?id={number}",
            provider_item_id=f"sina-{number}",
            published_at=published.isoformat(),
            published_at_epoch=published.timestamp(),
            symbols=["600519.SH"],
            sentiment=-1.0 if number == len(sessions) else 0.6,
            confidence=1.0,
            importance_score=100,
            is_official=False,
            content_scope="provider_excerpt",
            analysis_status="complete",
        ))
    store.sources.register_ingest_batch(
        FetchBatch(
            source_id="sina_live",
            articles=articles,
            previous_watermark="sina-old",
            pending_watermark="sina-new",
            health="degraded",
            complete=False,
            error_code="snapshot_window_exhausted",
        ),
        "sina-incomplete",
    )
    assert store.save(articles) == len(articles)
    with store._conn() as conn:
        for number, article in enumerate(articles, start=1):
            available = article.published_at_epoch + 60
            if number == len(articles):
                available = (sessions[-1] + pd.Timedelta(days=5)).tz_localize(
                    "Asia/Shanghai",
                ).timestamp()
            conn.execute(
                "UPDATE news SET first_seen_at=?,content_version_at=?,"
                "analysis_updated_at=? WHERE provider_item_id=?",
                (available, available, available, article.provider_item_id),
            )

    production = quality_sentiment_panel(
        sessions, ["600519.SH"], store=store, tier="production",
    )
    preview = quality_sentiment_panel(
        sessions, ["600519.SH"], store=store, tier="sandbox",
    )

    assert production["600519.SH"].isna().all()
    assert preview["600519.SH"].notna().any()
    assert preview.loc[sessions[-1], "600519.SH"] > 0
    metadata = preview.attrs["news_factor"]
    assert metadata["tier"] == "sandbox"
    assert metadata["sample_start"] == sessions[0].strftime("%Y-%m-%d")
    assert metadata["sample_end"] == sessions[-1].strftime("%Y-%m-%d")
    assert metadata["sessions"] == 10
    assert metadata["coverage"] == pytest.approx(1.0)
    assert metadata["event_count"] == 10
    assert metadata["formal_eligible"] is False
    assert "history_sessions_below_1038" in metadata["reasons"]
    assert metadata["sources"] == [{
        "source_id": "sina_live",
        "source_name": "新浪财经 7×24",
        "source_group": "fast",
        "event_count": 10,
        "weight_multiplier": 0.25,
        "formal_row_count": 0,
        "formal_eligible": False,
        "reasons": [
            "formal_raw_evidence_missing",
            "history_sessions_below_1038",
            "ingest_window_incomplete",
            "non_official_source",
            "provider_excerpt_only",
            "sandbox_tier",
        ],
    }]
    readiness = news_sentiment_readiness(
        str(sessions[0].date()), str(sessions[-1].date()), store=store,
    )
    assert readiness["ready"] is False
    assert readiness["event_count"] == 0


def test_sandbox_factor_downweights_sina_fast_against_official_evidence(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    available = pd.Timestamp("2026-08-03 10:00", tz="Asia/Shanghai").timestamp()
    official = official_news(
        store,
        title="官方正面公告",
        content="官方正面公告正文",
        symbols=["600519.SH"],
        sentiment=0.6,
        confidence=1.0,
        importance_score=100,
        analysis_status="complete",
        published_at_epoch=available,
    )
    fast = NewsItem(
        source="sina_live",
        title="新浪负面快讯",
        content="新浪负面快讯正文",
        url="https://zhibo.sina.com.cn/?id=downweight",
        provider_item_id="sina-downweight",
        published_at=pd.Timestamp(available, unit="s", tz="UTC").isoformat(),
        published_at_epoch=available,
        symbols=["600519.SH"],
        sentiment=-1.0,
        confidence=1.0,
        importance_score=100,
        content_scope="provider_excerpt",
        analysis_status="complete",
    )
    store.sources.register_ingest_batch(
        FetchBatch(
            source_id="sina_live",
            articles=[fast],
            pending_watermark=fast.provider_item_id,
            complete=False,
        ),
        "sina-downweight-batch",
    )
    assert store.save([official, fast]) == 2
    with store._conn() as conn:
        conn.execute(
            "UPDATE news SET first_seen_at=?,content_version_at=?,analysis_updated_at=?",
            (available, available, available),
        )

    preview = quality_sentiment_panel(
        pd.DatetimeIndex(["2026-08-03"]),
        ["600519.SH"],
        store=store,
        tier="sandbox",
    )

    assert preview.iloc[0, 0] == pytest.approx((0.6 - 0.25) / 1.25)


def test_sandbox_factor_does_not_reinterpret_retired_importance_score(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    published = pd.Timestamp("2026-08-03 10:00", tz="Asia/Shanghai")
    item = NewsItem(
        source="sina_live",
        title="旧库可分析快讯",
        content="旧库可分析快讯正文",
        url="https://zhibo.sina.com.cn/?id=legacy-preview",
        provider_item_id="legacy-preview",
        published_at=published.strftime("%Y-%m-%d %H:%M:%S"),
        published_at_epoch=published.timestamp(),
        symbols=["600519.SH"],
        sentiment=0.8,
        confidence=1.0,
        importance_score=80,
        content_scope="provider_excerpt",
        analysis_status="complete",
    )
    assert store.save([item]) == 1
    available = published.timestamp() + 60
    with store._conn() as conn:
        conn.execute(
            "UPDATE news SET content_scope='unknown',published_at_epoch=0,"
            "factor_importance_score=NULL,factor_weight_at_analysis=NULL,"
            "first_seen_at=?,content_version_at=?,analysis_updated_at=?",
            (available, available, available),
        )

    sessions = pd.DatetimeIndex(["2026-08-03"])
    production = quality_sentiment_panel(
        sessions, ["600519.SH"], store=store, tier="production",
    )
    preview = quality_sentiment_panel(
        sessions, ["600519.SH"], store=store, tier="sandbox",
    )

    assert production.iloc[0, 0] is pd.NA or pd.isna(production.iloc[0, 0])
    assert preview.iloc[0, 0] is pd.NA or pd.isna(preview.iloc[0, 0])
    metadata = preview.attrs["news_factor"]
    assert metadata["formal_eligible"] is False
    assert "no_usable_publication_rows" in metadata["reasons"]


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
    })
    assert item.symbols == ["600519.SH"]
    assert item.sectors == ["电子", "基础化工"]
    assert item.event_type == "其他"
    assert item.sentiment == 0
    assert item.confidence == 0
    assert item.scope == "market"
    assert item.urgency == "normal"


def test_news_stats_calculate_market_and_independent_sector_scores(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    store._industry_map = {}
    store.save([
            official_news(store,
                title="电子需求回暖", content="电子需求回暖", sectors=["电子"],
            sentiment=0.8, confidence=1, importance_score=100, analysis_status="complete",
        ),
            official_news(store,
                title="电子成本上升", content="电子成本上升", sectors=["电子"],
            sentiment=-0.2, confidence=1, importance_score=100, analysis_status="complete",
        ),
            official_news(store,
                title="银行息差承压", content="银行息差承压", sectors=["银行"],
            sentiment=-0.5, confidence=1, importance_score=100, analysis_status="complete",
        ),
            official_news(store,
                source="pboc", title="电子需求回暖转载", content="电子需求回暖", sectors=["电子"],
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


def test_news_stats_exposes_global_analysis_queue_counts(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    store.save([NewsItem(source="unit", title="待标注", content="pending")])
    store.save([NewsItem(source="unit", title="退避失败", content="failed")])
    failed_id = store.max_id()
    store.analysis_failure([failed_id], "timeout", "timeout")
    store.save([NewsItem(source="unit", title="死信", content="dead")])
    dead_id = store.max_id()
    for _ in range(3):
        store.analysis_failure([dead_id], "offline", "network")
    with store._conn() as connection:
        connection.execute(
            "UPDATE news SET analysis_recovery_count=3,next_retry_at=0 WHERE id=?",
            (dead_id,),
        )

    queue = store.stats(30)["queue"]
    assert {key: queue[key] for key in (
        "pending", "failed", "recovery", "dead_letter",
        "manual_recoverable_dead_letter", "recoverable_dead_letter",
    )} == {
        "pending": 1,
        "failed": 1,
        "recovery": 0,
        "dead_letter": 1,
        "manual_recoverable_dead_letter": 1,
        "recoverable_dead_letter": 0,
    }
    assert queue["processing"] == 0
    assert queue["claims"] == {"total": 0, "active": 0, "expired": 0}
def test_news_stats_event_focus_includes_names_and_more_symbols(tmp_path, monkeypatch):
    store = NewsStore(tmp_path / "news.sqlite")
    symbols = [f"{index:06d}.SZ" for index in range(1, 21)]
    monkeypatch.setattr(
        "quantmaster.data.read_stock_names",
        lambda values, **_kwargs: {
            symbol: f"标的{index:02d}" for index, symbol in enumerate(values, 1)
        },
    )
    store.save([
            official_news(store,
                title=f"事件 {index}", content=f"事件正文 {index}",
            symbols=[symbol], sentiment=0.1, confidence=1,
            importance_score=100, analysis_status="complete",
        )
        for index, symbol in enumerate(symbols, 1)
    ])

    focused = store.stats(30)["top_symbols"]

    assert len(focused) == 20
    assert focused[0] == {"symbol": "000001.SZ", "name": "标的01", "count": 1}
    assert focused[-1] == {"symbol": "000020.SZ", "name": "标的20", "count": 1}


def test_live_industry_map_change_cannot_rewrite_historical_sector_factor(
    tmp_path, monkeypatch,
):
    path = tmp_path / "news.sqlite"
    store = NewsStore(path)
    now = time.time()
    item = official_news(
        store,
        title="分析时冻结行业",
        content="分析时确认属于银行的正式正文",
        symbols=["600001.SH"],
        sectors=["银行"],
        sentiment=0.6,
        confidence=1,
        importance_score=100,
        analysis_status="complete",
        published_at_epoch=now - 60,
    )
    assert store.save([item]) == 1
    before = store.stats(30)["sector_scores"]

    monkeypatch.setattr(
        "quantmaster.data.industry.load_cached_industry_map",
        lambda: {"600001.SH": "电子"},
    )
    reopened = NewsStore(path)
    after = reopened.stats(30)["sector_scores"]

    assert before == after
    assert [row["sector"] for row in after] == ["银行"]
    assert reopened.recent(1)[0]["sectors"] == ["银行"]


def test_annotation_only_freezes_sectors_returned_by_the_analysis():
    item = NewsItem(source="test", title="行业确认", content="正文")

    AICrawler._apply_result(item, {
        "symbols": ["600001.SH"],
        "sectors": ["银行"],
        "event_type": "行业",
    })

    assert item.symbols == ["600001.SH"]
    assert item.sectors == ["银行"]


@pytest.mark.parametrize("field,value", [
    ("symbols", "not-json"),
    ("sectors", '{"行业":"银行"}'),
])
def test_news_reader_rejects_corrupt_dimension_json(tmp_path, field, value):
    store = NewsStore(tmp_path / "news.sqlite")
    assert store.save([official_news(
        store, title="损坏维度", content="损坏维度正文",
        symbols=["600001.SH"], sectors=["银行"], analysis_status="complete",
    )]) == 1
    with store._conn() as connection:
        connection.execute(f"UPDATE news SET {field}=? WHERE id=1", (value,))

    with pytest.raises(RuntimeError, match=rf"{field} (JSON|must be a JSON array)"):
        store.recent(1)


def test_news_public_row_only_exposes_alert_importance_score(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    assert store.save([official_news(
        store, title="重要资讯", content="重要资讯正文",
        importance_score=82, analysis_status="complete",
    )]) == 1

    public = store.recent(1)[0]

    assert public["alert_importance_score"] == 82
    assert "importance_score" not in public


def test_news_event_focus_uses_rolling_window_and_quality_gates(tmp_path, monkeypatch):
    now = 2_000_000_000.0
    monkeypatch.setattr("quantmaster.ai.crawler.time.time", lambda: now)
    monkeypatch.setattr(
        "quantmaster.data.read_stock_names",
        lambda values, **_kwargs: {symbol: f"名称-{symbol}" for symbol in values},
    )
    store = NewsStore(tmp_path / "news.sqlite")
    store._industry_map = {}
    custom_source = store.sources.create(source_value(name="标注资讯"))
    seen_at = {
        "窗口内": now - 60,
        "窗口内转载": now - 30,
        "精确边界": now - 86400,
        "三日窗口": now - 2 * 86400,
        "窗口外": now - 3 * 86400 - 1,
    }
    items = [
        official_news(store,
            title="窗口内", content="窗口内正文", symbols=["600001.SH"],
            confidence=1, importance_score=100, analysis_status="complete",
            published_at_epoch=seen_at["窗口内"],
        ),
        official_news(store,
            source="pboc", title="窗口内转载", content="窗口内正文",
            symbols=["600001.SH"], confidence=.9, importance_score=100,
            analysis_status="complete",
            published_at_epoch=seen_at["窗口内转载"],
        ),
        official_news(store,
            title="精确边界", content="精确边界正文", symbols=["000001.SZ"],
            confidence=1, importance_score=100, analysis_status="complete",
            published_at_epoch=seen_at["精确边界"],
        ),
        official_news(store,
            title="三日窗口", content="三日窗口正文", symbols=["000002.SZ"],
            confidence=1, importance_score=100, analysis_status="complete",
            published_at_epoch=seen_at["三日窗口"],
        ),
        official_news(store,
            title="窗口外", content="窗口外正文", symbols=["000003.SZ"],
            confidence=1, importance_score=100, analysis_status="complete",
            published_at_epoch=seen_at["窗口外"],
        ),
        official_news(store,
            title="低置信度", content="低置信度正文", symbols=["000004.SZ"],
            confidence=0, importance_score=100, analysis_status="complete",
            published_at_epoch=now - 60,
        ),
        official_news(store,
            title="零质量", content="零质量正文", symbols=["000005.SZ"],
            confidence=1, importance_score=0, analysis_status="complete",
            published_at_epoch=now - 60,
        ),
        official_news(store,
            title="未完成", content="未完成正文", symbols=["000006.SZ"],
            confidence=1, importance_score=100, analysis_status="pending",
            published_at_epoch=now - 60,
        ),
        NewsItem(
            source=custom_source["id"], title="已标注自定义资讯", content="自定义资讯正文",
            url="https://example.com/custom", published_at_epoch=now - 60,
            published_at=datetime.fromtimestamp(now - 60, UTC).isoformat(),
            symbols=["300001.SZ"], confidence=1, importance_score=100,
            content_scope="feed_summary", analysis_status="complete",
        ),
    ]
    assert store.save(items) == len(items)
    with store._conn() as connection:
        for title, timestamp in seen_at.items():
            connection.execute(
                "UPDATE news SET first_seen_at=? WHERE title=?",
                (timestamp, title),
            )

    one_day = store.event_focus(1)
    three_days = store.event_focus(3)
    thirty_days = store.event_focus(30)

    assert one_day == {
        "days": 1,
        "top_symbols": [
            {"symbol": "000001.SZ", "name": "名称-000001.SZ", "count": 1},
            {"symbol": "300001.SZ", "name": "名称-300001.SZ", "count": 1},
            {"symbol": "600001.SH", "name": "名称-600001.SH", "count": 1},
        ],
    }
    assert [item["symbol"] for item in three_days["top_symbols"]] == [
        "000001.SZ", "000002.SZ", "300001.SZ", "600001.SH",
    ]
    assert [item["symbol"] for item in thirty_days["top_symbols"]] == [
        "000001.SZ", "000002.SZ", "000003.SZ", "300001.SZ", "600001.SH",
    ]
    with pytest.raises(ValueError, match="仅支持"):
        store.event_focus(2)


def test_market_sentiment_respects_requested_cutoff_and_quality_deduplication(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    cutoff = 1_800_000_000.0
    items = [
            official_news(store,
            title=f"盘中利好 {index}",
            content=f"独立内容 {index}",
            sentiment=1,
            confidence=1,
            importance_score=100,
            analysis_status="complete",
            published_at_epoch=cutoff - 60,
        )
        for index in range(21)
    ]
    items.extend([
        official_news(store,
            source="pboc",
            title="盘中利好转载",
            content="独立内容 0",
            sentiment=-1,
            confidence=1,
            importance_score=100,
            analysis_status="complete",
            published_at_epoch=cutoff - 60,
        ),
        official_news(store,
            title="盘后利空",
            content="盘后独立内容",
            sentiment=-1,
            confidence=1,
            importance_score=100,
            analysis_status="complete",
            published_at_epoch=cutoff + 60,
        ),
    ])
    store.save(items)
    with store._conn() as connection:
        connection.execute(
            "UPDATE news SET first_seen_at=? WHERE title LIKE '盘中%'",
            (cutoff - 60,),
        )
        connection.execute(
            "UPDATE news SET first_seen_at=? WHERE title='盘后利空'",
            (cutoff + 60,),
        )

    at_close = store.market_sentiment(as_of=cutoff, days=30)
    after_close = store.market_sentiment(as_of=cutoff + 120, days=30)

    assert at_close["event_count"] == 21
    assert at_close["score"] == 100.0
    assert at_close["lookback_days"] == 30
    assert after_close["event_count"] == 22
    assert after_close["score"] < at_close["score"]


def test_market_sentiment_separates_event_cutoff_from_later_evidence_visibility(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    event_cutoff = 1_800_000_000.0
    knowledge_cutoff = event_cutoff + 3600
    items = [
        official_news(
            store,
            title=f"盘后完成标注 {index}",
            content=f"盘后完成标注正文 {index}",
            sentiment=0.5,
            confidence=1,
            importance_score=100,
            analysis_status="complete",
            published_at_epoch=event_cutoff - 60,
        )
        for index in range(21)
    ]
    items.append(official_news(
        store,
        title="事件截止后发布",
        content="事件截止后发布正文",
        sentiment=-1,
        confidence=1,
        importance_score=100,
        analysis_status="complete",
        published_at_epoch=event_cutoff + 60,
    ))
    store.save(items)
    with store._conn() as connection:
        connection.execute(
            "UPDATE news SET first_seen_at=?,content_version_at=?,analysis_updated_at=?",
            (knowledge_cutoff, knowledge_cutoff, knowledge_cutoff),
        )

    pit = store.market_sentiment(as_of=event_cutoff, days=30)
    operational = store.market_sentiment(
        as_of=event_cutoff, days=30, knowledge_as_of=knowledge_cutoff,
    )

    assert pit["event_count"] == 0
    assert operational["event_count"] == 21
    assert operational["knowledge_as_of_epoch"] == knowledge_cutoff


def test_holdings_context_update_cannot_rewrite_same_as_of_market_sentiment(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    now = time.time()
    contextual = official_news(
        store,
        source="sse",
        title="公司经营信息",
        content="公司经营信息正文",
        symbols=["600519.SH"],
        sentiment=0.8,
        confidence=1,
        importance_score=40,
        analysis_status="complete",
        published_at_epoch=now - 120,
    )
    market = official_news(
        store,
        source="pboc",
        title="市场流动性信息",
        content="市场流动性信息正文",
        sentiment=-0.2,
        confidence=1,
        importance_score=100,
        analysis_status="complete",
        published_at_epoch=now - 60,
    )
    assert store.save([contextual, market]) == 2
    as_of = now + 10
    before = store.market_sentiment(as_of=as_of, days=1)
    score, scope, _reasons = alert_importance_score(
        contextual, {"600519.SH"}, set(),
    )
    assert score != contextual.importance_score
    store.update_context(1, importance_score=score, scope=scope, urgency="high")

    assert store.market_sentiment(as_of=as_of, days=1) == before
    with store._conn() as connection:
        row = connection.execute(
            "SELECT factor_importance_score,alert_importance_score "
            "FROM news WHERE id=1"
        ).fetchone()
    assert tuple(row) == (40, score)


def test_source_weight_change_cannot_rewrite_prior_as_of_market_sentiment(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    now = time.time()
    assert store.save([
        official_news(
            store,
            source="sse",
            title="交易所正面信息",
            content="交易所正面信息正文",
            sentiment=0.8,
            confidence=1,
            importance_score=100,
            analysis_status="complete",
            published_at_epoch=now - 120,
        ),
        official_news(
            store,
            source="pboc",
            title="央行负面信息",
            content="央行负面信息正文",
            sentiment=-0.4,
            confidence=1,
            importance_score=100,
            analysis_status="complete",
            published_at_epoch=now - 60,
        ),
    ]) == 2
    as_of = now + 10
    before = store.market_sentiment(as_of=as_of, days=1)

    store.sources.update("sse", {"factor_weight": 3})

    assert store.market_sentiment(as_of=as_of, days=1) == before
    rows = store.factor_rows(end_epoch=as_of)
    assert [row["source_weight"] for row in rows] == [1, 1]


def test_v4_contract_requires_explicit_migration_without_promoting_factor_fields(tmp_path):
    path = tmp_path / "news.sqlite"
    store = NewsStore(path)
    now = time.time()
    assert store.save([
        official_news(
            store,
            source="sse",
            title="旧版已分析消息",
            content="旧版已分析消息正文",
            sentiment=0.7,
            confidence=1,
            importance_score=100,
            analysis_status="complete",
            published_at_epoch=now - 60,
        )
    ]) == 1
    with store._conn() as connection:
        connection.execute(
            "UPDATE news_store_meta SET value='4' WHERE key='schema_version'"
        )
        connection.execute("ALTER TABLE news DROP COLUMN alert_importance_score")
        connection.execute("ALTER TABLE news DROP COLUMN factor_weight_at_analysis")
        connection.execute("ALTER TABLE news DROP COLUMN factor_importance_score")

    with pytest.raises(RuntimeError, match="需先执行一次性迁移"):
        NewsStore(path)
    with sqlite3.connect(path) as connection:
        row = connection.execute(
            "SELECT importance_score FROM news"
        ).fetchone()
        version = connection.execute(
            "SELECT value FROM news_store_meta WHERE key='schema_version'"
        ).fetchone()[0]
    assert tuple(row) == (100,)
    assert version == "4"


def test_news_event_focus_is_sorted_and_limited_to_24(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "quantmaster.data.read_stock_names",
        lambda values, **_kwargs: dict.fromkeys(values, "测试标的"),
    )
    store = NewsStore(tmp_path / "news.sqlite")
    store._industry_map = {}
    symbols = [f"{index:06d}.SZ" for index in range(1, 27)]
    store.save([
            official_news(store,
                title=f"事件 {symbol}", content=f"正文 {symbol}",
            symbols=[symbol], confidence=1, importance_score=100,
            analysis_status="complete",
        )
        for symbol in reversed(symbols)
    ])

    focused = store.event_focus(7)["top_symbols"]

    assert len(focused) == 24
    assert [item["symbol"] for item in focused] == symbols[:24]


def test_annotation_stream_yields_each_persisted_batch(tmp_path, isolated_config):
    from quantmaster.ai.crawler import AICrawler

    isolated_config.news.annotation_max_concurrency = 1

    class FakeLLM:
        def chat_json(self, prompt, system="", **kwargs):
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
        "completed": 0, "failed": 0, "retry_scheduled": 0,
        "dead_letter": 0, "batch_count": 3, "claimed": 0,
        "in_progress": 0, "recovered_leases": 0,
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


def test_annotation_batches_use_news_specific_concurrency(tmp_path, isolated_config):
    import threading

    isolated_config.news.annotation_max_concurrency = 3
    active = maximum = 0
    lock = threading.Lock()
    three_workers_started = threading.Event()
    release_workers = threading.Event()

    class ConcurrentLLM:
        def chat_json(self, prompt, system="", **kwargs):
            nonlocal active, maximum
            with lock:
                active += 1
                maximum = max(maximum, active)
                if active == 3:
                    three_workers_started.set()
            # The assertion is about the configured concurrency ceiling,
            # not Windows scheduler luck in a 50ms sleep.  Hold the first
            # workers until all three have deterministically entered.
            if three_workers_started.wait(2):
                release_workers.set()
            release_workers.wait(2)
            with lock:
                active -= 1
            return [{
                "symbols": [], "sectors": [], "event_type": "其他",
                "sentiment": 0, "summary": "并发标注", "scope": "market",
                "urgency": "normal", "confidence": 0.8,
            }]

    store = NewsStore(tmp_path / "news.sqlite")
    store.save([
        NewsItem(source="test", title=f"并发 {index}", content=f"正文 {index}")
        for index in range(6)
    ])

    result = AICrawler(client=ConcurrentLLM(), store=store).enrich_pending(
        limit=6, batch_size=1,
    )

    assert result["completed"] == 6
    assert maximum == 3


def test_separate_crawlers_share_one_news_pipeline(tmp_path, isolated_config):
    """Scheduled fast/official jobs must not multiply annotation concurrency."""
    import threading
    from concurrent.futures import ThreadPoolExecutor

    isolated_config.news.annotation_max_concurrency = 2
    active = maximum = 0
    counter_lock = threading.Lock()

    class SharedLLM:
        def chat_json(self, prompt, system="", **kwargs):
            nonlocal active, maximum
            with counter_lock:
                active += 1
                maximum = max(maximum, active)
            time.sleep(0.05)
            with counter_lock:
                active -= 1
            return [{
                "symbols": [], "sectors": [], "event_type": "其他",
                "sentiment": 0, "summary": "流水线串行", "scope": "market",
                "urgency": "normal", "confidence": 0.8,
            }]

    path = tmp_path / "news.sqlite"
    store = NewsStore(path)
    store.save([
        NewsItem(source="test", title=f"跨任务 {index}", content=f"正文 {index}")
        for index in range(8)
    ])
    crawlers = [
        AICrawler(client=SharedLLM(), store=NewsStore(path)),
        AICrawler(client=SharedLLM(), store=NewsStore(path)),
    ]

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(
            lambda crawler: crawler.enrich_pending(limit=4, batch_size=1), crawlers,
        ))

    assert sum(result["completed"] for result in results) == 8
    assert maximum == 2


def test_storage_error_does_not_increment_model_failure_attempts(
    tmp_path, monkeypatch,
):
    import sqlite3

    class SuccessfulLLM:
        def chat_json(self, prompt, system="", **kwargs):
            return [{
                "symbols": [], "sectors": [], "event_type": "其他",
                "sentiment": 0, "summary": "模型已正常返回", "scope": "market",
                "urgency": "normal", "confidence": 0.8,
            }]

    store = NewsStore(tmp_path / "news.sqlite")
    store.save([NewsItem(source="test", title="待持久化", content="模型结果正常")])
    item_id = store.max_id()
    monkeypatch.setattr(
        store, "update_analyses",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            sqlite3.OperationalError("database is locked")
        ),
    )

    result = AICrawler(client=SuccessfulLLM(), store=store).enrich_pending(limit=1)

    detail = store.detail(item_id)
    assert result["failed"] == 1
    assert result["retry_scheduled"] == 0
    assert result["failure_details"][0]["code"] == "storage_busy"
    assert detail["analysis_status"] == "pending"
    assert detail["analysis_attempts"] == 0


def test_concurrent_llm_results_serialize_before_claim_reads(tmp_path, monkeypatch):
    """Successful provider batches must not deadlock while upgrading SQLite locks."""

    import threading
    from concurrent.futures import ThreadPoolExecutor

    store = NewsStore(tmp_path / "news.sqlite")
    store.save([
        NewsItem(source="test", title="并发落库 1", content="模型已返回 1"),
        NewsItem(source="test", title="并发落库 2", content="模型已返回 2"),
    ])
    first = store.claims.claim(
        owner="worker", task_type="news:pending", mode="pending", limit=1,
    )
    second = store.claims.claim(
        owner="worker", task_type="news:pending", mode="pending", limit=1,
    )
    assert len(first.ids) == len(second.ids) == 1

    barrier = threading.Barrier(2)
    original_owns = store.claims.owns

    def synchronized_owns(news_id, token, owner, *, now=None, connection=None):
        # On the broken deferred path neither connection owns a write lock at
        # this point, so both readers are deliberately aligned before their
        # incompatible lock upgrades.  BEGIN IMMEDIATE makes this branch
        # unreachable and the second batch waits before reading its claim.
        if connection is not None and not connection.in_transaction:
            barrier.wait(timeout=2)
        return original_owns(
            news_id, token, owner, now=now, connection=connection,
        )

    monkeypatch.setattr(store.claims, "owns", synchronized_owns)

    def persist(batch, summary):
        item_id = batch.ids[0]
        item = NewsItem(
            source="test", title=summary, content=summary, db_id=item_id,
            summary=summary, confidence=0.8, analysis_status="complete",
        )
        return store.update_analyses(
            [(item_id, item)], claim_token=batch.token, claim_owner="worker",
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(persist, first, "已落库 1"),
            executor.submit(persist, second, "已落库 2"),
        ]
        written = [future.result(timeout=5) for future in futures]

    assert sorted(item_id for batch in written for item_id in batch) == sorted([
        first.ids[0], second.ids[0],
    ])
    assert len(store.query(status="complete", limit=10)["items"]) == 2


def test_annotation_failure_uses_structured_code_and_provider_backoff(tmp_path):
    class SlowLLM:
        def chat_json(self, prompt, system=None, **kwargs):
            raise LLMError(
                "模型在 180 秒内未返回结果", code="read_timeout",
                retryable=True, retry_after=240,
            )

    store = NewsStore(tmp_path / "news.sqlite")
    store.save([NewsItem(source="test", title="慢响应", content="等待退避")])
    item_id = store.max_id()
    started = time.time()

    result = AICrawler(client=SlowLLM(), store=store).enrich_pending(limit=1)

    detail = store.detail(item_id)
    assert result["failed"] == 1
    assert result["retry_scheduled"] == 1
    assert result["dead_letter"] == 0
    assert result["failure_details"][0]["code"] == "read_timeout"
    assert detail["analysis_status"] == "failed"
    assert detail["last_failure_code"] == "read_timeout"
    assert detail["next_retry_at"] >= started + 239


def test_news_detail_explains_existing_generic_analysis_error(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    store.save([NewsItem(source="test", title="网关响应异常", content="等待诊断")])
    item_id = store.max_id()
    store.analysis_failure(
        [item_id], "资讯分析失败，请查看本机日志", "invalid_response",
    )

    detail = store.detail(item_id)

    assert detail["analysis_error"] == (
        "模型接口返回的内容无法解析。常见原因是 API 地址指向网页、网关返回错误页，"
        "或接口协议不兼容；请检查 API 地址和模型服务。"
    )


def test_news_failure_keeps_safe_upstream_response_summary(tmp_path):
    message = (
        "OpenAI 协议 API 返回了无法解析的响应（HTTP 200 · text/html，"
        "页面标题：Sub2API - AI API Gateway）"
    )

    class HTMLGateway:
        def chat_json(self, prompt, system=None, **kwargs):
            raise LLMError(message, code="invalid_response", retryable=True)

    store = NewsStore(tmp_path / "news.sqlite")
    store.save([NewsItem(source="test", title="错误页", content="等待诊断")])
    item_id = store.max_id()

    result = AICrawler(client=HTMLGateway(), store=store).enrich_pending(limit=1)

    assert result["failure_details"][0]["message"] == message
    assert store.detail(item_id)["analysis_error"] == message


def test_non_retryable_annotation_error_enters_dead_letter_immediately(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    store.save([NewsItem(source="test", title="鉴权失败", content="不可自动恢复")])
    item_id = store.max_id()

    outcome = store.analysis_failure(
        [item_id], "OpenAI API 请求失败（HTTP 401）", "http_401", retryable=False,
    )

    assert outcome["dead_letter"] == 1
    assert outcome["retry_scheduled"] == 0
    detail = store.detail(item_id)
    assert detail["analysis_status"] == "dead_letter"
    assert detail["analysis_attempts"] == 1


def test_annotation_reanalyze_api_enqueues_task(monkeypatch):
    from quantmaster.server import news as news_module

    captured: dict[str, object] = {}

    class FakeJobs:
        def submit_reanalyze(self, spec):
            captured["spec"] = spec
            return ({
                "id": "job-news-reanalyze", "type": "news.reanalyze",
                "status": "queued", "progress": 0, "phase": "等待执行",
                "detail": "", "links": {},
            }, True)

        @staticmethod
        def public(job):
            return {
                "domain": "news", **job,
                "links": {
                    "self": "/api/v1/jobs/job-news-reanalyze",
                    "events": "/api/v1/jobs/job-news-reanalyze/events",
                    "cancel": "/api/v1/jobs/job-news-reanalyze/cancel",
                    "retry": "/api/v1/jobs/job-news-reanalyze/retry",
                },
            }

    monkeypatch.setattr(news_module, "get_news_jobs", lambda: FakeJobs())
    client = TestClient(app)
    token = _issue_csrf()
    client.cookies.set("qm_csrf", token)
    response = client.post(
        "/api/v1/news/reanalyze",
        json={"limit": 10, "batch_size": 2}, headers={"X-CSRF-Token": token},
    )

    assert response.status_code == 202
    assert response.json()["domain"] == "news"
    assert response.json()["type"] == "news.reanalyze"
    assert response.json()["links"]["events"].endswith("/events")
    assert captured["spec"] == {
        "limit": 10, "batch_size": 2, "mode": "pending",
    }


def test_annotation_reanalyze_api_leaves_limits_to_current_settings(monkeypatch):
    from quantmaster.server import news as news_module

    captured: dict[str, object] = {}

    class FakeJobs:
        def submit_reanalyze(self, spec):
            captured["spec"] = spec
            return ({
                "id": "job-settings-reanalyze", "type": "news.reanalyze",
                "status": "queued", "progress": 0, "phase": "等待执行",
                "detail": "", "links": {},
            }, True)

        @staticmethod
        def public(job):
            return {"domain": "news", **job, "links": {}}

    monkeypatch.setattr(news_module, "get_news_jobs", lambda: FakeJobs())
    client = TestClient(app)
    token = _issue_csrf()
    client.cookies.set("qm_csrf", token)
    response = client.post(
        "/api/v1/news/reanalyze", json={"mode": "pending"},
        headers={"X-CSRF-Token": token},
    )

    assert response.status_code == 202
    assert captured["spec"] == {"mode": "pending"}


def test_news_reanalyze_submission_snapshots_current_annotation_settings(monkeypatch):
    from types import SimpleNamespace

    from quantmaster.ai.news_jobs import NewsJobs

    class FakeRuntime:
        def register(self, *_args, **_kwargs):
            pass

        def submit(self, _task_type, spec, **_kwargs):
            return ({"id": "job-settings", "spec": spec}, True)

    monkeypatch.setattr(
        "quantmaster.ai.news_jobs.get_config",
        lambda: SimpleNamespace(news=SimpleNamespace(
            annotation_items_per_run=1000,
            annotation_batch_size=37,
        )),
    )
    monkeypatch.setattr(
        "quantmaster.ai.news_jobs.NewsStore.max_id", lambda _self: 0,
    )

    job, created = NewsJobs(FakeRuntime()).submit_reanalyze({"mode": "pending"})

    assert created is True
    assert job["spec"] == {
        "ids": [], "limit": 1000, "batch_size": 37, "mode": "pending",
    }


def test_failed_annotation_reanalyze_preserves_mode_in_task_spec(monkeypatch):
    from quantmaster.server import news as news_module

    calls: dict[str, object] = {}

    class FakeJobs:
        def submit_reanalyze(self, spec):
            calls["spec"] = spec
            return ({
                "id": "job-failed-reanalyze", "type": "news.reanalyze",
                "status": "queued", "progress": 0, "phase": "等待执行",
                "detail": "", "links": {},
            }, True)

        @staticmethod
        def public(job):
            return {"domain": "news", **job, "links": {}}

    monkeypatch.setattr(news_module, "get_news_jobs", lambda: FakeJobs())
    client = TestClient(app)
    token = _issue_csrf()
    client.cookies.set("qm_csrf", token)
    response = client.post(
        "/api/v1/news/reanalyze",
        json={"mode": "failed", "limit": 10, "batch_size": 2},
        headers={"X-CSRF-Token": token},
    )

    assert response.status_code == 202
    assert calls["spec"] == {
        "limit": 10, "batch_size": 2, "mode": "failed",
    }


def test_reanalyze_stream_route_removed():
    client = TestClient(app)
    token = _issue_csrf()
    client.cookies.set("qm_csrf", token)
    response = client.post(
        "/api/v1/news/reanalyze/stream",
        json={"mode": "failed", "limit": 10, "batch_size": 2},
        headers={"X-CSRF-Token": token},
    )
    assert response.status_code == 404


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
    assert 'id="news-retry-failed"' in page
    assert 'id="news-pending-action-count"' in page
    assert 'id="news-failed-action-count"' in page
    assert 'id="news-dead-action-count"' in page
    assert 'id="news-focus-window"' in page
    assert page.count('data-news-focus-days=') == 4
    assert 'class="news-focus-window"' in page
    assert 'data-news-focus-days="7" aria-pressed="true"' in page
    assert 'id="news-focus-feedback"' in page
    assert 'name="llm.max_concurrency"' in page
    assert 'name="news.annotation_timeout"' in page
    assert 'name="news.annotation_max_concurrency"' in page
    assert 'name="news.annotation_model"' in page
    assert 'name="news.annotation_reasoning_effort"' in page
    assert 'data-settings-section="sources"' in page
    assert "/static/news.js" not in page
    assert "/static/news.css" not in page
    assert "%%QM_NEWS_" not in page
    chart_source = client.get("/static/news.js").text
    assert "limit: 100, batch_size: 5" not in chart_source
    news_styles = client.get("/static/news.css").text
    assert "mkChart('news-factor-chart')" in chart_source
    assert "CHART_COLORS.primary" in chart_source
    assert "runAnnotation(event.currentTarget, 'failed')" in chart_source
    assert "function failureTemplate(item)" in chart_source
    assert "/api/v1/news/event-focus?days=${requestedDays}" in chart_source
    assert "requestId !== state.eventFocusRequest" in chart_source
    assert "button.dataset.newsFocusDays" in chart_source
    assert "loadEventFocus(state.eventFocusRetryDays)" in chart_source
    assert "过去 ${days} 日暂无达到质量门槛的标的提及" in chart_source
    assert "--news-scroll-edge: 12px" in news_styles
    assert '.news-focus-window button[aria-pressed="true"]::after' in news_styles
    assert news_styles.count("padding-inline-end: var(--news-scroll-edge)") == 2
    assert "data-news-retry" in chart_source
    assert "原因：${html(reason)}" in chart_source
    assert "queue?.manual_recoverable_dead_letter" in chart_source
    assert "正在认领可手动恢复的暂停项" in chart_source
    assert "恢复暂停项" in chart_source
    assert "死信" not in chart_source
    assert "恢复次数已用尽" not in chart_source
    assert "/ 3 次" not in chart_source
    assert "recoveryWaiting" not in chart_source


def test_periodic_news_job_is_registered():
    assert DEFAULT_JOBS["fast_news_scan"][1]["minutes"] == 20
    assert DEFAULT_JOBS["official_news_scan"][1]["minutes"] == 120
    assert DEFAULT_JOBS["periodic_news_scan"][1]["minutes"] == 360
    assert "window" not in DEFAULT_JOBS["fast_news_scan"][1]
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
        def chat_json(self, prompt, system=None, **kwargs):
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


def test_manual_dead_letter_recovery_selects_all_ready_items_without_20_item_cap(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    store.save([
        NewsItem(source="unit", title=f"死信 {index}", content="等待批量恢复")
        for index in range(25)
    ])
    ids = [item["id"] for item in store.query(limit=30)["items"]]
    for _ in range(3):
        store.analysis_failure(ids, "request timeout", "timeout")
    with store._conn() as connection:
        connection.execute("UPDATE news SET next_retry_at=0")

    selected = store.prepare_dead_letter_recovery(limit=None)

    assert len(selected) == 25
    assert store.stats()["queue"]["recovery"] == 25


def test_failed_annotations_can_be_retried_before_backoff_expires(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    store.save([NewsItem(source="unit", title="失败资讯", content="等待人工重试")])
    failed_id = store.max_id()
    store.save([NewsItem(source="unit", title="新资讯", content="仍未尝试")])
    pending_id = store.max_id()
    store.analysis_failure([failed_id], "request timeout", "timeout")
    before = store.detail(failed_id)
    assert before["analysis_status"] == "failed"
    assert before["next_retry_at"] > time.time()

    class HealthyClient:
        def chat_json(self, prompt, system=None, **kwargs):
            return [{
                "symbols": [], "sectors": [], "event_type": "其他",
                "sentiment": 0, "summary": "人工重试完成", "scope": "market",
                "urgency": "normal", "confidence": 0.8,
            }]

    result = AICrawler(client=HealthyClient(), store=store).retry_failed(
        limit=10, batch_size=5,
    )

    assert result["selected"] == 1
    assert result["completed"] == 1
    assert store.detail(failed_id)["analysis_status"] == "complete"
    assert store.detail(pending_id)["analysis_status"] == "pending"


def test_news_claims_are_mutually_exclusive_and_token_fenced(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    store.save([NewsItem(source="unit", title="并发资讯", content="只应标注一次")])
    item_id = store.max_id()
    first = store.claims.claim(
        owner="worker-one", task_type="news:pending", mode="pending", limit=1,
    )
    second = store.claims.claim(
        owner="worker-two", task_type="news:pending", mode="pending", limit=1,
    )
    assert first.ids == (item_id,)
    assert second.ids == ()

    with store._conn() as connection:
        connection.execute(
            "UPDATE news_analysis_claims SET lease_expires_at=0 WHERE news_id=?", (item_id,),
        )
    takeover = store.claims.claim(
        owner="worker-two", task_type="news:pending", mode="pending", limit=1,
    )
    assert takeover.ids == (item_id,)
    assert takeover.recovered_leases == 1

    annotated = NewsItem(source="unit", title="并发资讯", content="只应标注一次")
    annotated.summary = "旧 worker"
    assert store.update_analysis(
        item_id, annotated, claim_token=first.token, claim_owner="worker-one",
    ) is False
    annotated.summary = "新 worker"
    assert store.update_analysis(
        item_id, annotated, claim_token=takeover.token, claim_owner="worker-two",
    ) is True
    assert store.detail(item_id)["summary"] == "新 worker"


def test_news_claim_helpers_reject_unsafe_ids_and_handle_empty_ownership(tmp_path):
    from quantmaster.ai.news_claims import NewsClaimStore, normalize_news_ids

    assert normalize_news_ids([]) == []
    assert normalize_news_ids([3, 3, 1]) == [3, 1]
    with pytest.raises(ValueError, match="正整数"):
        normalize_news_ids([0])
    with pytest.raises(ValueError, match="1000"):
        normalize_news_ids(list(range(1, 1002)))

    store = NewsStore(tmp_path / "news.sqlite")
    claims = NewsClaimStore(store.path)
    empty = claims.claim(
        owner="worker", task_type="empty", mode="pending", limit=5, ids=[],
    )
    assert empty.ids == ()
    assert claims.heartbeat("missing", "worker") == 0
    assert claims.owns(1, "missing", "worker") is False
    assert claims.release("", "worker") == 0
    assert claims.stats() == {"total": 0, "active": 0, "expired": 0}


def test_manual_dead_letter_recovery_ignores_health_and_backoff(tmp_path, monkeypatch):
    store = NewsStore(tmp_path / "news.sqlite")
    store.save([NewsItem(source="unit", title="立即恢复", content="dead")])
    item_id = store.max_id()
    for _ in range(3):
        store.analysis_failure([item_id], "offline", "network")
    with store._conn() as connection:
        connection.execute(
            "UPDATE news SET analysis_recovery_count=3,next_retry_at=? WHERE id=?",
            (time.time() + 7 * 86400, item_id),
        )
    monkeypatch.setattr(store, "llm_recently_healthy", lambda: False)

    class HealthyClient:
        def chat_json(self, prompt, system=None, **kwargs):
            return [{
                "symbols": [], "sectors": [], "event_type": "其他",
                "sentiment": 0, "summary": "人工恢复", "scope": "market",
                "urgency": "normal", "confidence": 0.8,
            }]

    result = AICrawler(client=HealthyClient(), store=store).recover_dead_letters(
        limit=None, manual=True,
    )
    assert result["claimed"] == 1
    assert store.detail(item_id)["analysis_status"] == "complete"


def test_unbounded_failed_retry_processes_more_than_sqlite_parameter_limit(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    store.save([
        NewsItem(source="unit", title=f"失败队列 {index}", content="retry")
        for index in range(1005)
    ])
    ids = list(range(1, store.max_id() + 1))
    store.analysis_failure(ids, "timeout", "timeout")

    class FailingClient:
        def chat_json(self, prompt, system=None, **kwargs):
            raise LLMError("provider offline", code="network", retryable=True)

    result = AICrawler(client=FailingClient(), store=store).retry_failed(
        limit=None, batch_size=50,
    )
    assert result["claimed"] == 1005
    assert result["processed"] == 1005
    assert result["failed"] == 1005


def test_news_stats_deduplicates_in_sql_and_uses_stats_index(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    store.save([
        official_news(store, title="重复一", content="相同正文"),
        official_news(store, title="重复二", content="相同正文"),
    ])
    ids = list(range(1, store.max_id() + 1))
    for item_id in ids:
        item = NewsItem(source="unit", title="完成", content="相同正文")
        item.sentiment = 0.5
        item.confidence = 0.8
        item.importance_score = 90
        store.update_analysis(item_id, item)
    assert store.stats()["market_sentiment"]["event_count"] == 1
    with store._conn() as connection:
        plan = connection.execute(
            "EXPLAIN QUERY PLAN SELECT id FROM news "
            "WHERE analysis_status='complete' AND first_seen_at>=? AND confidence>=?",
            (0, 0.35),
        ).fetchall()
    assert any("idx_news_stats" in str(tuple(row)) for row in plan)


def test_news_storage_never_repairs_current_contract_on_open(tmp_path):
    path = tmp_path / "news.sqlite"
    store = NewsStore(path)
    store._industry_map = {}
    store.save([
        NewsItem(
            source="unit", title="维度投影", content="维度投影正文",
            symbols=["600519.SH"], sectors=["食品饮料"], sentiment=0.4,
            confidence=0.9, importance_score=80, analysis_status="complete",
        )
    ])
    item_id = store.max_id()
    with store._conn() as connection:
        connection.execute("DELETE FROM news_store_meta")
        connection.execute("UPDATE news SET content_hash='' WHERE id=?", (item_id,))

    with pytest.raises(RuntimeError, match="需先执行一次性迁移"):
        NewsStore(path)
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT content_hash FROM news WHERE id=?", (item_id,),
        ).fetchone()[0] == ""
        assert connection.execute("SELECT COUNT(*) FROM news_store_meta").fetchone()[0] == 0


def test_news_route_helpers_cover_crud_filters_and_reanalysis_modes(monkeypatch):
    from quantmaster.server import news as news_module

    monkeypatch.setattr(news_module, "_require_csrf", lambda request: None)
    monkeypatch.setattr(news_module, "_require_local", lambda request: None)
    calls: list[tuple[str, object]] = []
    monkeypatch.setattr(
        news_module, "NewsSourceStore", lambda *args, **kwargs: _RouteNewsSources(calls),
    )
    monkeypatch.setattr(
        news_module, "NewsStore", lambda *args, **kwargs: _RouteNewsStore(calls),
    )
    monkeypatch.setattr(news_module, "AICrawler", lambda: _RouteNewsCrawler(calls))

    class RouteNewsJobs:
        def submit(self, **kwargs):
            calls.append(("news_submit", kwargs))
            return {
                "id": "job-news", "type": "news.crawl", "status": "queued",
                "created": True, "coalesced": False, **kwargs,
            }, True

        def public(self, value):
            return {
                **value,
                "links": {"self": "/api/v1/jobs/job-news"},
            }

        def submit_source_run(self, source_id, *, skip_llm):
            job, created = self.submit(
                sources=[source_id], group="", limit=30, skip_llm=skip_llm,
            )
            job["type"] = "news.source_run"
            return job, created

        def submit_reanalyze(self, spec):
            job, created = self.submit(**spec)
            job["type"] = "news.reanalyze"
            if str(spec.get("mode") or "pending") == "pending" and spec.get("ids"):
                job["reset"] = len(spec["ids"])
            return job, created

    monkeypatch.setattr(news_module, "get_news_jobs", lambda: RouteNewsJobs())
    request = object()
    source = news_module.SourceValue.model_validate(source_value())

    assert news_module.sources_list(request)["items"][0]["id"] == "source-1"
    created = news_module.source_create(
        news_module.SourceCreate(source=source, token="create-secret"), request,
    )
    assert created["id"] == "created"
    assert news_module.source_preview(
        news_module.SourcePreview(source=source, token="preview-secret"), request,
    )["items"][0]["title"] == "研究 RSS"
    updated = news_module.source_update(
        "source-1",
        news_module.SourceUpdate(
            source={"enabled": False}, token_action="replace", token="replacement",
        ),
        request,
    )
    assert updated["enabled"] is False
    assert news_module.source_delete("source-1", request) == {"deleted": "source-1"}
    assert news_module.source_test("source-1", request)["items"][0]["content"] == "body"
    source_job = news_module.source_run("source-1", request, skip_llm=True)
    assert source_job["type"] == "news.source_run"
    assert source_job["skip_llm"] is True

    with pytest.raises(news_module.HTTPException) as missing_source:
        news_module.source_test("missing", request)
    assert missing_source.value.status_code == 404
    with pytest.raises(news_module.HTTPException) as missing_update:
        news_module.source_update(
            "missing", news_module.SourceUpdate(source={"enabled": True}), request,
        )
    assert missing_update.value.status_code == 404
    with pytest.raises(news_module.HTTPException) as credential_error:
        news_module.source_create(
            news_module.SourceCreate(
                source=source.model_copy(update={"name": "boom"}), token="secret",
            ),
            request,
        )
    assert credential_error.value.status_code == 409
    public_error = str(news_module._error(RuntimeError("Bearer secret-value")).detail)
    assert public_error == "资讯操作未能完成，详细原因已写入服务端日志"
    assert "secret-value" not in public_error

    with pytest.raises(news_module.HTTPException) as invalid_stats_days:
        news_module.news_stats(request, news_module.Response(), days=9999)
    assert invalid_stats_days.value.status_code == 422
    assert news_module.news_event_focus(request, news_module.Response(), days=7) == {
        "days": 7, "top_symbols": [],
    }
    valid_focus = TestClient(app).get("/api/v1/news/event-focus?days=7")
    assert valid_focus.status_code == 200
    assert valid_focus.json() == {"days": 7, "top_symbols": []}
    assert TestClient(app).get("/api/v1/news/event-focus?days=2").status_code == 422
    queried = news_module.news_query(
        request,
        limit=20,
        cursor=9,
        q="央行",
        source="source-1",
        group="official",
        event_type="政策",
        sentiment="positive",
        scope="market",
        symbol="600000.SH",
        status="complete",
        date_from="2026-08-01",
        date_to="2026-08-04",
        sort="importance",
    )
    assert queried["filters"]["date_to"] > queried["filters"]["date_from"]
    assert news_module._epoch(None) is None
    with pytest.raises(news_module.HTTPException, match="日期格式"):
        news_module._epoch("not-a-date")

    fast_job = news_module.news_crawl(
        request,
        news_module.CrawlRequest(group="fast", limit=2),
        skip_llm=True,
    )
    assert fast_job["group"] == "fast"
    assert fast_job["skip_llm"] is True
    assert news_module.news_crawl(request, None, skip_llm=None)["limit"] == 30
    assert not any(name == "run" for name, _value in calls)
    assert news_module.news_reanalyze(
        news_module.ReanalyzeRequest(mode="dead_letter", batch_size=3), request,
    )["mode"] == "dead_letter"
    assert news_module.news_reanalyze(
        news_module.ReanalyzeRequest(mode="failed", ids=[1, 1, 2]), request,
    )["mode"] == "failed"
    assert news_module.news_reanalyze(
        news_module.ReanalyzeRequest(mode="pending", limit=4), request,
    )["mode"] == "pending"
    pending = news_module.news_reanalyze(
        news_module.ReanalyzeRequest(mode="pending", ids=[7, 8]), request,
    )
    assert pending["reset"] == 2
    assert news_module.news_detail(7, request) == {"id": 7}
    with pytest.raises(news_module.HTTPException) as missing_item:
        news_module.news_detail(404, request)
    assert missing_item.value.status_code == 404


def test_news_read_only_dashboard_uses_published_materialization(tmp_path, monkeypatch):
    """The workbench GET contract must not re-run formal aggregation SQL."""

    path = tmp_path / "news.sqlite"
    writer = NewsStore(path)
    published = writer.publish_dashboard_materializations()
    assert published["snapshots"]["stats:30"]

    def should_not_aggregate(*_args, **_kwargs):
        raise AssertionError("GET 不能重新聚合资讯")

    monkeypatch.setattr("quantmaster.ai.crawler.aggregate_news_stats", should_not_aggregate)
    monkeypatch.setattr("quantmaster.ai.crawler.aggregate_news_event_focus", should_not_aggregate)
    reader = NewsStore(path, read_only=True)
    stats = reader.stats(30)
    focus = reader.event_focus(7)

    assert stats["meta"]["snapshot_id"] == published["snapshots"]["stats:30"]
    assert stats["meta"]["algorithm_version"] == "QM_NEWS_DASHBOARD_V1"
    assert focus["meta"]["snapshot_id"] == published["snapshots"]["event_focus:7"]


def test_news_crawl_submission_uses_versioned_unified_singleflight(tmp_path, monkeypatch):
    from quantmaster.ai.news_jobs import NewsJobs
    from quantmaster.runtime.jobs import UnifiedJobRuntime, UnifiedJobStore

    runtime = UnifiedJobRuntime(
        UnifiedJobStore(tmp_path / "jobs.sqlite"), dispatch=False,
    )
    monkeypatch.setattr("quantmaster.ai.news_jobs._input_fingerprint", lambda _spec: "news-v1")
    jobs = NewsJobs(runtime)
    first, created = jobs.submit(group="fast", limit=20)
    second, second_created = jobs.submit(group="fast", limit=20)

    assert created is True
    assert second_created is False
    assert first["id"] == second["id"]
    public = jobs.public(second)
    assert public["type"] == "news.crawl"
    assert public["input_fingerprint"] == "news-v1"
    assert public["links"]["self"] == f"/api/v1/jobs/{first['id']}"


def test_manual_reanalyze_does_not_reuse_completed_job(tmp_path):
    from quantmaster.ai.news_jobs import NewsJobs
    from quantmaster.runtime.jobs import JobOutcome, UnifiedJobRuntime, UnifiedJobStore

    runtime = UnifiedJobRuntime(
        UnifiedJobStore(tmp_path / "jobs.sqlite"), dispatch=False,
    )
    jobs = NewsJobs(runtime)
    spec = {"mode": "pending", "limit": 10, "batch_size": 2}
    first, created = jobs.submit_reanalyze(spec)
    assert created is True
    assert first["input_fingerprint"] == ""

    owner = "test-owner"
    assert runtime.store.claim(first["id"], owner)
    claimed = runtime.store.get(first["id"])
    token = claimed["lease_token"]
    artifact = runtime.store.write_artifact(
        first["id"], "news.reanalyze.result", {"completed": 1},
        {"schema_version": "1.0"}, owner=owner, lease_token=token,
    )
    runtime.store.finish(
        first["id"], owner, JobOutcome("completed", "done", artifact["id"]),
        lease_token=token,
    )

    second, second_created = jobs.submit_reanalyze(spec)
    assert second_created is True
    assert second["id"] != first["id"]


def test_reanalyze_job_reports_batch_progress_and_partial_failure(monkeypatch):
    from quantmaster.ai import news_jobs

    class FakeStore:
        @staticmethod
        def reset_analysis(_ids):
            return 0

    class FakeCrawler:
        store = FakeStore()

        @staticmethod
        def enrich_pending_events(**_kwargs):
            yield {"type": "start", "total": 4}
            yield {
                "type": "batch", "total": 4, "processed": 2,
                "completed": 1, "failed": 1,
            }
            yield {
                "type": "complete", "processed": 4, "completed": 3,
                "failed": 1, "retry_scheduled": 1, "dead_letter": 0,
                "failure_details": [{"code": "timeout"}],
                "completed_ids": [1, 2, 3], "claimed": 4,
            }

    class FakeContext:
        input_fingerprint = ""

        def __init__(self):
            self.progress_events = []
            self.artifact_payload = None

        def progress(self, value, phase, detail=""):
            self.progress_events.append((value, phase, detail))

        @staticmethod
        def ensure_active():
            return None

        @staticmethod
        def cancelled():
            return False

        def write_artifact(self, _kind, payload, _metadata):
            self.artifact_payload = payload
            return {"id": "artifact-news"}

    monkeypatch.setattr(news_jobs, "AICrawler", FakeCrawler)
    context = FakeContext()
    outcome = news_jobs.run_news_reanalyze_job(
        context, {"mode": "pending", "limit": 4, "batch_size": 2},
    )

    assert outcome.status == "completed_with_errors"
    assert "成功 3 条，失败 1 条" in outcome.detail
    assert context.artifact_payload["failed"] == 1
    assert any(
        value == 49 and "已处理 2/4 条" in detail
        for value, _phase, detail in context.progress_events
    )


def test_news_annotation_ui_polls_real_job_progress():
    source = (Path(__file__).parents[1] / "quantmaster/server/static/news.js").read_text(
        encoding="utf-8",
    )
    assert "async function watchNewsTask(task, label, onProgress = null)" in source
    assert "if (onProgress) onProgress(current);" in source
    assert "const job = await watchNewsTask(task, copy.report, current =>" in source
