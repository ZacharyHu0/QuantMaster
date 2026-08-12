from __future__ import annotations

import gzip
import hashlib
import json
import os
import sqlite3
import time

import httpx
import pandas as pd
import pytest

from quantmaster.ai.crawler import AICrawler, NewsItem, NewsStore
from quantmaster.ai.news_contracts import (
    BUILTIN_SOURCES,
    FetchBatch,
    FetchedArticle,
    NewsContractError,
    evaluate_freshness,
    read_raw_evidence,
)
from quantmaster.ai.news_providers import (
    fetch_builtin_source,
    fetch_csrc,
    fetch_eastmoney_fast,
    fetch_ndrc,
    fetch_pboc,
    fetch_sina_live,
    fetch_sse,
    fetch_szse,
)
from quantmaster.ai.news_sources import (
    NewsSourceStore,
    _allow_builtin_fake_ip,
    _ensure_public_url,
    _fetch_bytes,
    fetch_declarative_source,
)


def _source(source_id: str) -> dict:
    return next(dict(item) for item in BUILTIN_SOURCES if item["id"] == source_id)


def _official_url(source_id: str, suffix: str) -> str:
    hosts = {
        "csrc": "www.csrc.gov.cn",
        "sse": "www.sse.com.cn",
        "szse": "www.szse.cn",
        "pboc": "www.pbc.gov.cn",
        "nbs_release": "www.stats.gov.cn",
        "nbs_interpretation": "www.stats.gov.cn",
        "ndrc": "www.ndrc.gov.cn",
    }
    return f"https://{hosts[source_id]}/evidence/{suffix.lstrip('/')}"


def _official_raw(store: NewsStore, source_id: str, content: str) -> str:
    payload = content.encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    return store.sources.save_response(
        source_id, _official_url(source_id, digest), payload,
        httpx.Headers(), 200, official=True,
    )


def _bind_official(store: NewsStore, *items: NewsItem) -> None:
    for index, item in enumerate(items):
        if not item.url:
            item.url = _official_url(item.source, item.provider_item_id or item.fingerprint or "item")
        if not item.provider_item_id:
            item.provider_item_id = item.url
        store.sources.bind_articles([item])
        store.sources.register_ingest_batch(
            FetchBatch(
                source_id=item.source,
                articles=[item],
                watermark=item.provider_item_id,
                complete=True,
            ),
            f"test-{time.time_ns()}-{index}",
        )


def test_ingest_batch_audit_deduplicates_identical_article_identity(tmp_path):
    store = NewsSourceStore(tmp_path / "news.sqlite")
    article = FetchedArticle(
        source="sina_live", provider_item_id="same-id", title="同一条快讯",
        content="上游批次重复返回的同一条内容", url="https://example.com/same-id",
        published_at="2026-08-12T01:00:00+00:00",
        published_at_epoch=1786496400.0,
    )
    batch = FetchBatch(
        source_id="sina_live", articles=[article, article],
        watermark="same-id", complete=True,
    )

    store.register_ingest_batch(batch, "duplicate-batch")

    with store._conn() as connection:
        assert connection.execute(
            "SELECT article_count FROM news_ingest_batches WHERE batch_id=?",
            ("duplicate-batch",),
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(*) FROM news_ingest_batch_articles WHERE batch_id=?",
            ("duplicate-batch",),
        ).fetchone()[0] == 1


def _replay_article(
    *,
    provider_item_id: str = "replay-id",
    raw_cache_key: str = "raw/replay",
    evidence_binding_hash: str = "replay-evidence",
):
    return FetchedArticle(
        source="sina_live", provider_item_id=provider_item_id, title="重放快讯",
        content="用于验证 durable batch 重放证据不可变性的内容。",
        url="https://example.com/replay", published_at="2026-08-12T01:00:00+00:00",
        published_at_epoch=1786496400.0, evidence_binding_hash=evidence_binding_hash,
        raw_cache_key=raw_cache_key,
    )


def test_ingest_batch_replay_accepts_identical_metadata_and_article_evidence(tmp_path):
    store = NewsSourceStore(tmp_path / "news.sqlite")
    batch_id = "replay-batch"
    first = FetchBatch(source_id="sina_live", articles=[_replay_article()], watermark="replay-id")
    window_id = store.register_ingest_batch(first, batch_id)

    replay = FetchBatch(source_id="sina_live", articles=[_replay_article()], watermark="replay-id")
    assert store.register_ingest_batch(replay, batch_id) == window_id

    with store._conn() as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM news_ingest_batches WHERE batch_id=?", (batch_id,),
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT COUNT(*) FROM news_ingest_batch_articles WHERE batch_id=?", (batch_id,),
        ).fetchone()[0] == 1


@pytest.mark.parametrize(
    ("replay",),
    [
        pytest.param(
            FetchBatch(
                source_id="sina_live", articles=[_replay_article()], watermark="replay-id",
                health="degraded",
            ),
            id="batch-metadata-differs",
        ),
        pytest.param(
            FetchBatch(
                source_id="sina_live", articles=[_replay_article(raw_cache_key="raw/changed")],
                watermark="replay-id",
            ),
            id="article-evidence-field-differs",
        ),
        pytest.param(
            FetchBatch(source_id="sina_live", articles=[], watermark="replay-id"),
            id="article-is-missing",
        ),
        pytest.param(
            FetchBatch(
                source_id="sina_live",
                articles=[
                    _replay_article(),
                    _replay_article(
                        provider_item_id="added-id", evidence_binding_hash="added-evidence",
                    ),
                ],
                watermark="replay-id",
            ),
            id="article-is-added",
        ),
    ],
)
def test_ingest_batch_replay_rejects_metadata_or_article_evidence_differences(tmp_path, replay):
    store = NewsSourceStore(tmp_path / "news.sqlite")
    batch_id = "replay-batch"
    store.register_ingest_batch(
        FetchBatch(source_id="sina_live", articles=[_replay_article()], watermark="replay-id"),
        batch_id,
    )

    with pytest.raises(NewsContractError, match="已持久化证据不一致") as error:
        store.register_ingest_batch(replay, batch_id)
    assert error.value.code == "ingest_batch_replay_conflict"


def _official_detail_html(source_id: str, *, published: str = "2026-08-09") -> bytes:
    body = "可独立复核的官方详情正文，包含完整语义与必要上下文。" * 4
    if source_id == "sse":
        return (
            f'<div class="article-infor"><i>{published}</i></div>'
            f'<div class="allZoom">{body}</div>'
        ).encode()
    if source_id == "pboc":
        return (
            f'<meta name="PubDate" content="{published}">'
            f'<div id="zoom">{body}</div>'
        ).encode()
    return (
        f'<meta name="PubDate" content="{published} 18:10:26">'
        f'<div class="article_con"><div class="TRS_Editor">{body}</div></div>'
    ).encode()


def test_builtin_groups_have_real_enabled_definitions(tmp_path):
    store = NewsSourceStore(tmp_path / "news.sqlite")
    grouped = {
        group: [item["id"] for item in store.list(enabled=True, group_name=group)]
        for group in ("fast", "official", "periodic")
    }
    assert grouped["fast"] == ["sina_live", "eastmoney_fast"]
    assert {"csrc", "sse", "szse", "pboc"} <= set(grouped["official"])
    assert {"nbs_release", "nbs_interpretation", "ndrc"} <= set(grouped["periodic"])
    enabled = {item["id"] for item in store.list(enabled=True)}
    assert {"eastmoney_fast", "csrc", "szse"} <= enabled
    assert all(float(item["max_age_hours"]) > 0 for item in BUILTIN_SOURCES)
    assert (store.get("sse") or {})["factor_eligible"] is True
    assert (store.get("csrc") or {})["factor_eligible"] is True
    assert (store.get("szse") or {})["factor_eligible"] is True
    assert (store.get("nbs_release") or {})["factor_eligible"] is True
    jin10 = store.get("jin10_authorized") or {}
    assert jin10["enabled"] is False
    assert jin10["needs_credentials"] is True
    assert jin10["url"] == "https://open.jin10.com/"
    with pytest.raises(ValueError, match="授权适配器"):
        store.update("jin10_authorized", {"enabled": True})


def test_restored_legacy_builtin_toggles_persist_across_store_reopen(tmp_path):
    database = tmp_path / "news.sqlite"
    store = NewsSourceStore(database)
    restored = ("eastmoney_fast", "csrc", "szse")
    for source_id in restored:
        store.update(source_id, {"enabled": False})
    assert all(not (store.get(source_id) or {})["enabled"] for source_id in restored)

    reopened = NewsSourceStore(database)
    for source_id in restored:
        reopened.update(source_id, {"enabled": True})

    reopened_again = NewsSourceStore(database)
    assert all((reopened_again.get(source_id) or {})["enabled"] for source_id in restored)


def test_builtin_source_preview_uses_provider_without_binding_evidence(monkeypatch):
    source = {
        **_source("sina_live"),
        "item_limit": 50,
    }
    article = NewsItem(
        source="sina_live", title="测试快讯", content="测试正文",
        published_at="2026-08-10T09:00:00+08:00",
    )
    bound: list[object] = []

    class SourceStore:
        def bind_articles(self, items):
            bound.extend(items)

    monkeypatch.setattr(
        "quantmaster.ai.crawler.fetch_builtin_source",
        lambda value, store, *, limit: FetchBatch(
            source_id=value["id"],
            articles=[FetchedArticle(
                source=article.source, title=article.title, content=article.content,
                published_at=article.published_at,
            )],
        ),
    )
    crawler = AICrawler.__new__(AICrawler)
    crawler.source_store = SourceStore()

    preview = crawler._fetch_source(source, limit=3, preview=True)

    assert [item.title for item in preview] == ["测试快讯"]
    assert bound == []


def test_eastmoney_current_trace_contract_and_watermark(monkeypatch):
    payload = {
        "code": "1",
        "data": {
            "sortEnd": "cursor-2",
            "fastNewsList": [
                {
                    "code": "new-2", "title": "东方财富最新快讯",
                    "summary": "具有明确时间和内容的最新快讯。",
                    "showTime": "2026-08-09 10:02:00",
                },
                {
                    "code": "old-1", "title": "旧水位快讯",
                    "summary": "旧水位内容。", "showTime": "2026-08-09 10:01:00",
                },
            ],
        },
    }
    requested: list[str] = []

    def fake_fetch(source, url, store):
        requested.append(url)
        return json.dumps(payload).encode(), url, "news_raw/eastmoney_fast/feed.gz"

    monkeypatch.setattr("quantmaster.ai.news_providers._fetch_bytes", fake_fetch)
    monkeypatch.setattr("quantmaster.ai.news_providers.time.time", lambda: 1786241400.0)

    batch = fetch_eastmoney_fast(_source("eastmoney_fast"), object(), "old-1", 30)

    assert "req_trace=quantmaster" in requested[0]
    assert [item.provider_item_id for item in batch.articles] == ["new-2"]
    assert batch.articles[0].content_scope == "provider_excerpt"
    assert batch.watermark == "new-2"
    assert batch.complete is True


def test_eastmoney_missing_id_stops_at_committed_publication_floor(monkeypatch):
    """A deleted provider cursor must not turn every run into an older backfill."""
    payload = {
        "code": "1",
        "data": {
            "sortEnd": "older-cursor",
            "fastNewsList": [
                {
                    "code": "new-head", "title": "水位之后的新快讯",
                    "summary": "应当正常保存的新内容。",
                    "showTime": "2026-08-11 10:05:00",
                },
                {
                    "code": "older-than-floor", "title": "早于已确认日期的历史快讯",
                    "summary": "不应重新灌入的历史内容。",
                    "showTime": "2026-08-11 03:59:59",
                },
            ],
        },
    }

    monkeypatch.setattr(
        "quantmaster.ai.news_providers._fetch_bytes",
        lambda source, url, store: (
            json.dumps(payload).encode(), url, "news_raw/eastmoney_fast/floor.gz",
        ),
    )
    source = {
        **_source("eastmoney_fast"),
        "_state_latest_published_at": 1786413600.0,  # 10:00; retain a 6h overlap
    }

    batch = fetch_eastmoney_fast(source, object(), "deleted-old-id", 30)

    assert [item.provider_item_id for item in batch.articles] == ["new-head"]
    assert batch.watermark == "new-head"
    assert batch.complete is True


def test_eastmoney_resumed_backfill_closes_without_archiving_older_pages(monkeypatch):
    payload = {
        "code": "1",
        "data": {
            "sortEnd": "still-older",
            "fastNewsList": [{
                "code": "historical", "title": "历史快讯",
                "summary": "已越过本地确认时间下界。",
                "showTime": "2026-08-10 17:59:59",
            }],
        },
    }
    monkeypatch.setattr(
        "quantmaster.ai.news_providers._fetch_bytes",
        lambda source, url, store: (
            json.dumps(payload).encode(), url, "news_raw/eastmoney_fast/resume.gz",
        ),
    )
    source = {
        **_source("eastmoney_fast"),
        "_state_pending_watermark": "new-head",
        "_state_next_cursor": "resume-cursor",
        "_state_latest_published_at": 1786377600.0,  # 2026-08-11 00:00 Asia/Shanghai
    }

    batch = fetch_eastmoney_fast(source, object(), "deleted-old-id", 30)

    assert batch.articles == []
    assert batch.watermark == "new-head"
    assert batch.pending_watermark == ""
    assert batch.complete is True


def test_empty_completed_gap_closes_ingest_window(tmp_path):
    store = NewsSourceStore(tmp_path / "news.sqlite")
    incomplete = FetchBatch(
        source_id="eastmoney_fast",
        articles=[FetchedArticle(
            source="eastmoney_fast", provider_item_id="new-head", title="新快讯",
            content="新快讯正文", url="https://kuaixun.eastmoney.com/7_24.html",
        )],
        watermark="old-id", previous_watermark="old-id",
        pending_watermark="new-head", complete=False,
    )
    window_id = store.register_ingest_batch(incomplete, "incomplete-batch")

    completed = FetchBatch(
        source_id="eastmoney_fast", articles=[], watermark="new-head",
        previous_watermark="old-id", complete=True,
    )
    assert store.register_ingest_batch(completed, "boundary-batch") == window_id

    with store._conn() as connection:
        assert tuple(connection.execute(
            "SELECT status,completed_batch_id FROM news_ingest_windows WHERE window_id=?",
            (window_id,),
        ).fetchone()) == ("complete", "boundary-batch")
        assert connection.execute(
            "SELECT article_count FROM news_ingest_batches WHERE batch_id=?",
            ("boundary-batch",),
        ).fetchone()[0] == 0


def test_csrc_current_json_contract_preserves_full_official_content(monkeypatch):
    payload = {
        "data": {
            "results": [{
                "manuscriptId": "7649538",
                "title": "证监会当前要闻标题",
                "content": "证监会当前要闻的完整正文，具有足够上下文并可由原始响应独立复核。" * 2,
                "url": "//www.csrc.gov.cn/csrc/c100028/c7649538/content.shtml",
                "publishedTimeStr": "2026-08-09 09:30:00",
            }],
        },
    }

    def fake_fetch(source, url, store):
        headers = source["parser"]["headers"]
        assert headers["Referer"].endswith("/c100028/common_xq_list.shtml")
        assert headers["X-Requested-With"] == "XMLHttpRequest"
        return (
            json.dumps(payload, ensure_ascii=False).encode(), url,
            "news_raw/csrc/listing.gz",
        )

    monkeypatch.setattr("quantmaster.ai.news_providers._fetch_bytes", fake_fetch)
    monkeypatch.setattr("quantmaster.ai.news_providers.time.time", lambda: 1786241400.0)

    batch = fetch_csrc(_source("csrc"), object(), "", 30)

    assert [item.provider_item_id for item in batch.articles] == ["7649538"]
    assert batch.articles[0].content_scope == "full_article"
    assert batch.articles[0].is_official is True
    assert batch.articles[0].url == (
        "https://www.csrc.gov.cn/csrc/c100028/c7649538/content.shtml"
    )
    assert batch.complete is True


def test_szse_current_listing_scripts_bind_verified_detail(monkeypatch):
    body = "深交所通知公告的完整正文，具有足够上下文并可由详情原始响应独立复核。" * 3
    listing = b"""
        <li><div class="title"><script>
        var curHref = './t20260809_621999.html';
        //var curTitle = 'commented title';
        var curTitle = '\\u6df1\\u4ea4\\u6240\\u5f53\\u524d\\u901a\\u77e5\\u516c\\u544a';
        </script><span class="time">2026-08-09</span></div></li>
    """
    detail = (
        '<div class="news-detail-con"><div class="des-content">'
        f'{body}</div></div><span>时间：2026-08-09</span>'
    ).encode()

    def fake_fetch(source, url, store):
        if url == source["url"]:
            return listing, url, "news_raw/szse/listing.gz"
        return detail, url, "news_raw/szse/detail.gz"

    monkeypatch.setattr("quantmaster.ai.news_providers._fetch_bytes", fake_fetch)
    monkeypatch.setattr("quantmaster.ai.news_providers.time.time", lambda: 1786241400.0)

    batch = fetch_szse(_source("szse"), object(), "", 30)

    assert [item.title for item in batch.articles] == ["深交所当前通知公告"]
    assert batch.articles[0].content == body
    assert batch.articles[0].content_scope == "full_article"
    assert batch.articles[0].raw_cache_key == "news_raw/szse/detail.gz"
    assert batch.complete is True


def test_public_hostname_accepts_clash_fake_ip_without_allowing_literal(monkeypatch):
    monkeypatch.setattr(
        "quantmaster.ai.news_sources.socket.getaddrinfo",
        lambda *_args, **_kwargs: [(2, 1, 6, "", ("198.18.0.169", 443))],
    )

    _ensure_public_url(
        "https://www.sse.com.cn/disclosure/",
        allow_fake_ip=True,
    )

    with pytest.raises(ValueError, match="私有网络"):
        _ensure_public_url(
            "https://198.18.0.169/disclosure/",
            allow_fake_ip=True,
        )


def test_fake_ip_exception_is_limited_to_frozen_builtin_hosts(monkeypatch):
    monkeypatch.setattr(
        "quantmaster.ai.news_sources.socket.getaddrinfo",
        lambda *_args, **_kwargs: [(2, 1, 6, "", ("198.18.0.169", 443))],
    )
    sse = _source("sse")

    assert _allow_builtin_fake_ip(sse, sse["url"]) is True
    assert _allow_builtin_fake_ip(
        {"id": "custom", "kind": "rss"},
        "https://attacker.example/feed",
    ) is False
    assert _allow_builtin_fake_ip(sse, "https://attacker.example/feed") is False


@pytest.mark.parametrize(
    ("source_id", "url"),
    [
        ("pboc", "https://attacker.example/detail.html"),
        ("pboc", "file://www.pbc.gov.cn/detail.html"),
        ("sse", "http://www.sse.com.cn/disclosure/detail.html"),
        ("sse", "https://www.sse.com.cn:444/disclosure/detail.html"),
    ],
)
def test_official_detail_requires_frozen_https_origin(tmp_path, source_id, url):
    store = NewsSourceStore(tmp_path / "news.sqlite")

    with pytest.raises(NewsContractError) as error:
        _fetch_bytes(_source(source_id), url, store)

    assert error.value.code == "official_host_mismatch"
    with pytest.raises(NewsContractError) as archive_error:
        store.save_response(
            source_id, url, b"foreign response", httpx.Headers(), 200, official=True,
        )
    assert archive_error.value.code == "official_host_mismatch"
    with store._conn() as connection:
        assert connection.execute("SELECT COUNT(*) FROM news_raw_manifest").fetchone()[0] == 0


def test_official_redirect_cannot_leave_frozen_host(monkeypatch, tmp_path):
    store = NewsSourceStore(tmp_path / "news.sqlite")
    real_client = httpx.Client
    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(str(request.url))
        return httpx.Response(
            302,
            headers={"location": "https://attacker.example/stolen-body"},
            request=request,
        )

    monkeypatch.setattr(
        "quantmaster.ai.news_sources.socket.getaddrinfo",
        lambda *_args, **_kwargs: [(2, 1, 6, "", ("93.184.216.34", 443))],
    )
    monkeypatch.setattr(
        "quantmaster.ai.news_sources.httpx.Client",
        lambda **_kwargs: real_client(
            transport=httpx.MockTransport(handler), follow_redirects=False,
        ),
    )

    source = store.get("sse") or {}
    with pytest.raises(NewsContractError) as error:
        _fetch_bytes(source, source["url"], store)

    assert error.value.code == "official_host_mismatch"
    assert requests == [source["url"]]
    with store._conn() as connection:
        assert connection.execute("SELECT COUNT(*) FROM news_raw_manifest").fetchone()[0] == 0


def test_article_binding_rejects_non_https_url_with_allowed_hostname(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    raw_key = _official_raw(store, "pboc", "valid raw bytes")
    item = NewsItem(
        source="pboc", provider_item_id="file-scheme", title="伪造条目",
        content="伪造正文", url="file://www.pbc.gov.cn/detail.html",
        published_at="2026-08-09T01:00:00+00:00", published_at_epoch=time.time() - 60,
        is_official=True, content_scope="full_article", raw_cache_key=raw_key,
    )

    with pytest.raises(NewsContractError) as error:
        store.sources.bind_articles([item])

    assert error.value.code == "article_evidence_invalid"


def test_fake_ip_exception_does_not_allow_rfc1918_hostname_resolution(monkeypatch):
    monkeypatch.setattr(
        "quantmaster.ai.news_sources.socket.getaddrinfo",
        lambda *_args, **_kwargs: [(2, 1, 6, "", ("192.168.1.10", 443))],
    )

    with pytest.raises(ValueError, match="私有网络"):
        _ensure_public_url("https://attacker.example/feed")


def test_only_builtin_nbs_raw_evidence_gets_the_larger_bounded_limit(tmp_path):
    payload = b"x" * (5 * 1024 * 1024 + 1)
    digest = hashlib.sha256(payload).hexdigest()
    for source_id in ("nbs_release", "sse"):
        path = tmp_path / "news_raw" / source_id / "2026-08-09" / f"{digest}.gz"
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wb") as handle:
            handle.write(payload)
    nbs_key = f"news_raw/nbs_release/2026-08-09/{digest}.gz"
    sse_key = f"news_raw/sse/2026-08-09/{digest}.gz"

    assert read_raw_evidence(tmp_path / "news.sqlite", nbs_key) == payload
    assert read_raw_evidence(tmp_path / "news.sqlite", sse_key) is None


def test_sina_pages_backfill_to_watermark(monkeypatch):
    pages = {
        1: [
            {
                "id": "105", "rich_text": "最新快讯", "create_time": "2026-08-09 10:05:00",
                "docurl": "https://finance.sina.cn/7x24/2026-08-09/detail-live-105.d.html",
            },
            {"id": "104", "rich_text": "第二条", "create_time": "2026-08-09 10:04:00"},
        ],
        2: [
            {"id": "103", "rich_text": "第三条", "create_time": "2026-08-09 10:03:00"},
            {"id": "102", "rich_text": "水位", "create_time": "2026-08-09 10:02:00"},
        ],
    }

    def fake_fetch(source, url, store):
        page = 2 if "page=2" in url else 1
        payload = {"result": {"data": {"feed": {"list": pages[page]}}}}
        return json.dumps(payload).encode(), url, f"sha256:page-{page}"

    monkeypatch.setattr("quantmaster.ai.news_providers._fetch_bytes", fake_fetch)
    batch = fetch_sina_live(_source("sina_live"), object(), "102", 50)
    assert [item.provider_item_id for item in batch.articles] == ["105", "104", "103"]
    assert batch.watermark == "105"
    assert batch.complete is True
    assert batch.articles[0].published_at == "2026-08-09T02:05:00+00:00"
    assert batch.articles[0].url == (
        "https://finance.sina.cn/7x24/2026-08-09/detail-live-105.d.html"
    )
    assert batch.raw_cache_keys == ["sha256:page-1", "sha256:page-2"]


def test_sina_backfill_commits_only_after_cross_round_gap_is_closed(monkeypatch, tmp_path):
    store = NewsSourceStore(tmp_path / "news.sqlite")
    store.record_batch(FetchBatch(
        source_id="sina_live", watermark="100", latest_published_at=1786240800.0,
    ))
    calls: list[int] = []
    page_sizes: list[int] = []

    def fake_fetch(source, url, fetch_store):
        page = int(url.split("page=", 1)[1].split("&", 1)[0])
        page_size = int(url.split("page_size=", 1)[1].split("&", 1)[0])
        calls.append(page)
        page_sizes.append(page_size)
        entries = [{
            "id": str(113 - page), "rich_text": f"第 {page} 页快讯",
            "create_time": f"2026-08-09 10:{59 - page:02d}:00",
        }]
        if page == 12:
            entries.append({
                "id": "100", "rich_text": "旧 committed 水位",
                "create_time": "2026-08-09 10:40:00",
            })
        payload = {"result": {"data": {"feed": {"list": entries}}}}
        return json.dumps(payload).encode(), url, f"sha256:page-{page}"

    monkeypatch.setattr("quantmaster.ai.news_providers._fetch_bytes", fake_fetch)
    first = fetch_builtin_source(store.get("sina_live") or {}, store, limit=1)
    assert calls == list(range(1, 11))
    assert page_sizes == [1] * 10
    assert first.complete is False
    assert first.watermark == "100"
    assert first.pending_watermark == "112"
    assert first.next_cursor == "11:1"
    store.record_batch(first)
    assert store.state("sina_live")["watermark"] == "100"

    calls.clear()
    page_sizes.clear()
    second = fetch_builtin_source(store.get("sina_live") or {}, store, limit=50)
    assert calls == [1, 11, 12]
    assert page_sizes == [1, 1, 1]
    assert second.complete is True
    assert second.watermark == "112"
    assert [item.provider_item_id for item in second.articles] == ["102", "101"]
    store.record_batch(second)
    state = store.state("sina_live")
    assert state["watermark"] == "112"
    assert state["pending_watermark"] == ""
    assert state["backfill_cursor"] == ""


def test_sina_resume_commits_pending_when_old_watermark_is_page_head(monkeypatch, tmp_path):
    store = NewsSourceStore(tmp_path / "news.sqlite")
    store.record_batch(FetchBatch(
        source_id="sina_live", watermark="100", latest_published_at=1786240800.0,
    ))
    calls: list[int] = []

    def fake_fetch(source, url, fetch_store):
        page = int(url.split("page=", 1)[1].split("&", 1)[0])
        calls.append(page)
        if page == 11:
            entries = [{
                "id": "100", "rich_text": "旧 committed 水位",
                "create_time": "2026-08-09 10:40:00",
            }]
        else:
            entries = [{
                "id": str(200 - page), "rich_text": f"第 {page} 页快讯",
                "create_time": f"2026-08-09 10:{59 - page:02d}:00",
            }]
        payload = {"result": {"data": {"feed": {"list": entries}}}}
        return json.dumps(payload).encode(), url, f"sha256:page-{page}"

    monkeypatch.setattr("quantmaster.ai.news_providers._fetch_bytes", fake_fetch)
    first = fetch_builtin_source(store.get("sina_live") or {}, store, limit=1)
    assert first.complete is False
    assert first.watermark == "100"
    assert first.pending_watermark == "199"
    assert first.next_cursor == "11:1"
    store.record_batch(first)

    calls.clear()
    second = fetch_builtin_source(store.get("sina_live") or {}, store, limit=1)
    assert calls == [1, 11]
    assert second.articles == []
    assert second.complete is True
    assert second.previous_watermark == "100"
    assert second.watermark == "199"
    store.record_batch(second)
    state = store.state("sina_live")
    assert state["watermark"] == "199"
    assert state["pending_watermark"] == ""
    assert state["backfill_cursor"] == ""


def test_sina_resumed_backfill_keeps_live_head_fresh_and_closes_removed_id_gap(
    monkeypatch, tmp_path,
):
    store = NewsSourceStore(tmp_path / "news.sqlite")
    store.record_batch(FetchBatch(
        source_id="sina_live", watermark="100",
        latest_published_at=1786424400.0,
    ))
    store.record_batch(FetchBatch(
        source_id="sina_live", watermark="100", previous_watermark="100",
        pending_watermark="105", next_cursor="211:30", complete=False,
        latest_published_at=1786424400.0,
    ))
    calls: list[int] = []

    def fake_fetch(source, url, fetch_store):
        page = int(url.split("page=", 1)[1].split("&", 1)[0])
        calls.append(page)
        entries = ([
            {
                "id": "200", "rich_text": "当前最新快讯",
                "create_time": "2026-08-12 17:30:00",
            },
            {
                "id": "105", "rich_text": "已归档的 pending 头部",
                "create_time": "2026-08-11 13:07:10",
            },
        ] if page == 1 else [{
            "id": "90", "rich_text": "已越过本地发布时间下界的旧条目",
            "create_time": "2026-08-10 12:00:00",
        }])
        payload = {"result": {"data": {"feed": {"list": entries}}}}
        return json.dumps(payload).encode(), url, f"sha256:page-{page}"

    monkeypatch.setattr("quantmaster.ai.news_providers._fetch_bytes", fake_fetch)
    monkeypatch.setattr(
        "quantmaster.ai.news_providers.time.time", lambda: 1786527600.0,
    )

    batch = fetch_builtin_source(store.get("sina_live") or {}, store, limit=30)

    assert calls == [1, 211]
    assert [item.provider_item_id for item in batch.articles] == ["200"]
    assert batch.latest_published_at == 1786527000.0
    assert batch.complete is True
    assert batch.watermark == "105"
    assert batch.next_cursor == ""


def test_sina_pinned_old_item_does_not_hide_newer_items_after_watermark(monkeypatch):
    entries = [
        {"id": "100", "rich_text": "置顶旧消息", "create_time": "2026-08-09 10:00:00"},
        {"id": "102", "rich_text": "置顶项后的新消息", "create_time": "2026-08-09 10:02:00"},
        {"id": "101", "rich_text": "另一条新消息", "create_time": "2026-08-09 10:01:00"},
    ]
    payload = {"result": {"data": {"feed": {"list": entries}}}}
    monkeypatch.setattr(
        "quantmaster.ai.news_providers._fetch_bytes",
        lambda source, url, store: (json.dumps(payload).encode(), url, "sha256:pinned"),
    )
    batch = fetch_sina_live(_source("sina_live"), object(), "100", 50)
    assert batch.complete is True
    assert batch.watermark == "102"
    assert [item.provider_item_id for item in batch.articles] == ["102", "101"]


def test_store_rejects_watermark_advance_from_incomplete_batch(tmp_path):
    store = NewsSourceStore(tmp_path / "news.sqlite")
    store.record_batch(FetchBatch(
        source_id="sina_live", watermark="old", latest_published_at=1786240800.0,
    ))
    store.record_batch(FetchBatch(
        source_id="sina_live", watermark="new", previous_watermark="old",
        pending_watermark="new", next_cursor="11", complete=False,
        latest_published_at=1786244400.0,
    ))
    state = store.state("sina_live")
    assert state["watermark"] == "old"
    assert state["pending_watermark"] == "new"
    assert state["backfill_cursor"] == "11"
    assert state["health"] == "degraded"
    assert state["last_error_code"] == "watermark_not_reached"


def test_later_page_304_keeps_head_articles_and_committed_watermark(monkeypatch):
    source = {
        **_source("sina_live"),
        "_state_latest_published_at": 1786240800.0,
    }

    def fake_fetch(source_value, url, store):
        if "page=2" in url:
            return None, url, ""
        payload = {"result": {"data": {"feed": {"list": [{
            "id": "105", "rich_text": "已解析的头页快讯",
            "create_time": "2026-08-09 10:05:00",
        }]}}}}
        return json.dumps(payload).encode(), url, "sha256:page-1"

    monkeypatch.setattr("quantmaster.ai.news_providers._fetch_bytes", fake_fetch)
    monkeypatch.setattr(
        "quantmaster.ai.news_providers.time.time", lambda: 1786241400.0,
    )
    batch = fetch_sina_live(source, object(), "100", 50)
    assert [item.provider_item_id for item in batch.articles] == ["105"]
    assert batch.watermark == "100"
    assert batch.pending_watermark == "105"
    assert batch.next_cursor == "2:50"
    assert batch.complete is False
    assert batch.error_code == "page_not_modified_during_backfill"


def test_sina_empty_contract_fails(monkeypatch):
    monkeypatch.setattr(
        "quantmaster.ai.news_providers._fetch_bytes",
        lambda *args, **kwargs: (
            json.dumps({"result": {"data": {"feed": {"list": []}}}}).encode(),
            "https://zhibo.sina.com.cn/api/zhibo/feed", "sha256:empty",
        ),
    )
    with pytest.raises(NewsContractError, match="空列表"):
        fetch_sina_live(_source("sina_live"), object(), "", 50)


@pytest.mark.parametrize(
    ("source_id", "fetcher", "href"),
    [
        ("sse", fetch_sse, "/disclosure/announcement/general/jjzssgg/c/c_20260809_10828292.shtml"),
        ("pboc", fetch_pboc, "/goutongjiaoliu/113456/113469/5900745/index.html"),
        ("ndrc", fetch_ndrc, "/xwdt/xwfb/202608/t20260809_1400000.html"),
    ],
)
def test_official_parsers_accept_articles_and_reject_navigation(
    monkeypatch, source_id, fetcher, href,
):
    listing_html = (
        '<nav><a href="/regulation/listing/measures/">发行上市审核监管</a></nav>'
        f'<ul><li><a href="{href}">真正的官方发布内容</a><span>2026-08-09</span></li></ul>'
    ).encode()

    def fake_fetch(source, url, store):
        if url == source["url"]:
            return listing_html, source["url"], f"news_raw/{source_id}/listing.gz"
        return _official_detail_html(source_id), url, f"news_raw/{source_id}/detail.gz"

    monkeypatch.setattr(
        "quantmaster.ai.news_providers._fetch_bytes",
        fake_fetch,
    )
    batch = fetcher(_source(source_id), object(), "", 30)
    assert len(batch.articles) == 1
    assert batch.articles[0].title == "真正的官方发布内容"
    assert batch.articles[0].is_official is True
    assert batch.articles[0].content_scope == "full_article"
    assert len(batch.articles[0].content) >= 40
    assert batch.articles[0].raw_cache_key == f"news_raw/{source_id}/detail.gz"
    assert batch.raw_cache_keys == [
        f"news_raw/{source_id}/listing.gz", f"news_raw/{source_id}/detail.gz",
    ]
    assert batch.complete is True
    assert "发行上市审核监管" not in [item.title for item in batch.articles]


def test_pboc_listing_date_may_be_a_sibling_of_the_anchor_parent(monkeypatch):
    href = "/goutongjiaoliu/113456/113469/2026080708481628617/index.html"
    listing = (
        '<table><tr><td><font class="newslist_style">'
        f'<a href="{href}">中国人民银行真实新闻发布标题</a>'
        '</font><span class="hui12">2026-08-07</span></td></tr></table>'
    ).encode()

    def fake_fetch(source, url, store):
        if url == source["url"]:
            return listing, source["url"], "news_raw/pboc/listing.gz"
        return _official_detail_html("pboc", published="2026-08-07"), url, (
            "news_raw/pboc/detail.gz"
        )

    monkeypatch.setattr("quantmaster.ai.news_providers._fetch_bytes", fake_fetch)

    batch = fetch_pboc(_source("pboc"), object(), "", 3)

    assert len(batch.articles) == 1
    assert batch.articles[0].content_scope == "full_article"
    assert (
        pd.Timestamp(batch.articles[0].published_at).tz_convert("Asia/Shanghai").date().isoformat()
        == "2026-08-07"
    )


def test_official_limit_gap_does_not_advance_committed_watermark(monkeypatch):
    hrefs = [
        "/disclosure/announcement/general/jjzssgg/c/c_20260809_10828295.shtml",
        "/disclosure/announcement/general/jjzssgg/c/c_20260809_10828294.shtml",
        "/disclosure/announcement/general/jjzssgg/c/c_20260809_10828293.shtml",
        "/disclosure/announcement/general/jjzssgg/c/c_20260809_10828292.shtml",
    ]
    html = "<ul>" + "".join(
        f'<li><a href="{href}">足够长的官方公告标题 {index}</a>'
        "<span>2026-08-09</span></li>"
        for index, href in enumerate(hrefs)
    ) + "</ul>"
    def fake_fetch(source, url, store):
        if url == source["url"]:
            return html.encode(), source["url"], "news_raw/sse/limit.gz"
        return _official_detail_html("sse"), url, f"news_raw/sse/{url.rsplit('_', 1)[-1]}.gz"

    monkeypatch.setattr("quantmaster.ai.news_providers._fetch_bytes", fake_fetch)
    committed = f"https://www.sse.com.cn{hrefs[-1]}"
    batch = fetch_sse(_source("sse"), object(), committed, 2)
    assert len(batch.articles) == 2
    assert batch.complete is False
    assert batch.watermark == committed
    assert batch.pending_watermark.endswith("10828295.shtml")
    assert batch.error_code == "snapshot_window_exhausted"
    assert batch.next_cursor == ""


def test_official_parser_sorts_entire_snapshot_before_cutting_at_watermark(monkeypatch):
    old_href = "/disclosure/announcement/general/jjzssgg/c/c_20260808_10828291.shtml"
    new_href = "/disclosure/announcement/general/jjzssgg/c/c_20260809_10828292.shtml"
    listing = (
        f'<li><a href="{old_href}">DOM 首位的旧水位公告</a><span>2026-08-08</span></li>'
        f'<li><a href="{new_href}">水位之后出现的最新公告</a><span>2026-08-09</span></li>'
    ).encode()

    def fake_fetch(source, url, store):
        if url == source["url"]:
            return listing, source["url"], "news_raw/sse/listing.gz"
        return _official_detail_html("sse"), url, "news_raw/sse/detail.gz"

    monkeypatch.setattr("quantmaster.ai.news_providers._fetch_bytes", fake_fetch)
    committed = f"https://www.sse.com.cn{old_href}"
    batch = fetch_sse(_source("sse"), object(), committed, 1)
    assert [item.provider_item_id for item in batch.articles] == [
        f"https://www.sse.com.cn{new_href}",
    ]
    assert batch.complete is True
    assert batch.watermark.endswith("10828292.shtml")


def test_official_detail_failure_keeps_listing_only_and_committed_watermark(monkeypatch):
    href = "/disclosure/announcement/general/jjzssgg/c/c_20260809_10828292.shtml"
    listing = (
        f'<li><a href="{href}">详情暂时不可复核的官方公告</a><span>2026-08-09</span></li>'
    ).encode()

    def fake_fetch(source, url, store):
        if url == source["url"]:
            return listing, source["url"], "news_raw/sse/listing.gz"
        return b'<div class="allZoom">too short</div>', url, "news_raw/sse/short.gz"

    monkeypatch.setattr("quantmaster.ai.news_providers._fetch_bytes", fake_fetch)
    batch = fetch_sse(_source("sse"), object(), "old-watermark", 30)
    assert batch.complete is False
    assert batch.watermark == "old-watermark"
    assert batch.pending_watermark.endswith("10828292.shtml")
    assert batch.error_code == "official_detail_incomplete"
    assert batch.articles[0].content_scope == "listing_title_only"
    assert batch.articles[0].raw_cache_key == "news_raw/sse/listing.gz"


def test_official_detail_304_recovers_verified_detail_raw(monkeypatch, tmp_path):
    store = NewsSourceStore(tmp_path / "news.sqlite")
    source = store.get("sse") or {}
    href = "/disclosure/announcement/general/jjzssgg/c/c_20260809_10828292.shtml"
    detail_url = f"https://www.sse.com.cn{href}"
    listing = (
        f'<li><a href="{href}">可从详情缓存恢复的官方公告</a><span>2026-08-09</span></li>'
    ).encode()
    detail = _official_detail_html("sse")
    listing_key = store.save_response(
        "sse", source["url"], listing, httpx.Headers(), 200, official=True,
    )
    detail_key = store.save_response(
        "sse", detail_url, detail, httpx.Headers({"etag": "detail-v1"}), 200,
        official=True,
    )

    def fake_fetch(source_value, url, fetch_store):
        if url == source_value["url"]:
            return listing, url, listing_key
        fetch_store.touch_not_modified("sse", url)
        return None, url, ""

    monkeypatch.setattr("quantmaster.ai.news_providers._fetch_bytes", fake_fetch)
    batch = fetch_sse(source, store, "", 30)
    assert batch.complete is True
    assert batch.articles[0].content_scope == "full_article"
    assert batch.articles[0].raw_cache_key == detail_key
    assert batch.articles[0].content == _clean_expected_detail_text()


def _clean_expected_detail_text() -> str:
    body = "可独立复核的官方详情正文，包含完整语义与必要上下文。" * 4
    return body


def test_rss_snapshot_gap_stays_degraded_without_a_verified_cursor(monkeypatch, tmp_path):
    store = NewsSourceStore(tmp_path / "news.sqlite")
    store.record_batch(FetchBatch(
        source_id="nbs_release", watermark="old-guid",
        latest_published_at=1786240800.0,
    ))
    items = "".join(
        "<item>"
        f"<guid>new-{index}</guid><title>统计发布条目 {index}</title>"
        f"<description>统计发布正文摘要 {index}</description>"
        f"<pubDate>Sun, 09 Aug 2026 10:0{index}:00 +0800</pubDate>"
        f"<link>https://www.stats.gov.cn/item-{index}.html</link>"
        "</item>"
        for index in (3, 2, 1)
    ) + (
        "<item><guid>old-guid</guid><title>旧水位条目</title>"
        "<description>旧水位正文摘要</description>"
        "<pubDate>Sun, 09 Aug 2026 10:00:00 +0800</pubDate>"
        "<link>https://www.stats.gov.cn/old.html</link></item>"
    )
    content = f"<rss><channel>{items}</channel></rss>".encode()
    monkeypatch.setattr(
        "quantmaster.ai.news_providers._fetch_bytes",
        lambda source, url, fetch_store: (content, source["url"], "news_raw/nbs/feed.gz"),
    )

    first = fetch_builtin_source(store.get("nbs_release") or {}, store, limit=2)
    assert first.complete is False
    assert first.watermark == "old-guid"
    assert first.pending_watermark == "new-3"
    assert first.error_code == "snapshot_window_exhausted"
    assert first.next_cursor == ""
    assert all(item.content_scope == "feed_summary" for item in first.articles)
    store.record_batch(first)

    second = fetch_builtin_source(store.get("nbs_release") or {}, store, limit=2)
    assert second.complete is False
    assert second.watermark == "old-guid"
    assert second.pending_watermark == "new-3"
    assert second.error_code == "snapshot_window_exhausted"
    store.record_batch(second)
    state = store.state("nbs_release")
    assert state["watermark"] == "old-guid"
    assert state["health"] == "degraded"
    assert state["last_error_code"] == "snapshot_window_exhausted"


def test_stale_initial_official_snapshot_fails_closed(monkeypatch):
    html = (
        '<ul><li><a href="/disclosure/announcement/general/jjzssgg/c/'
        'c_20210101_10000001.shtml">多年以前的历史公告</a><span>2021-01-01</span></li></ul>'
    ).encode()
    monkeypatch.setattr(
        "quantmaster.ai.news_providers._fetch_bytes",
        lambda source, url, store: (html, source["url"], "news_raw/sse/stale.gz"),
    )
    monkeypatch.setattr("quantmaster.ai.news_providers.time.time", lambda: 1786240800.0)
    with pytest.raises(NewsContractError) as error:
        fetch_sse(_source("sse"), object(), "", 30)
    assert error.value.code == "stale_initial_batch"


def test_stale_incremental_official_snapshot_is_degraded(monkeypatch):
    html = (
        '<ul><li><a href="/disclosure/announcement/general/jjzssgg/c/'
        'c_20210101_10000001.shtml">多年以前的历史公告</a><span>2021-01-01</span></li></ul>'
    ).encode()
    monkeypatch.setattr(
        "quantmaster.ai.news_providers._fetch_bytes",
        lambda source, url, store: (html, source["url"], "news_raw/sse/stale.gz"),
    )
    monkeypatch.setattr("quantmaster.ai.news_providers.time.time", lambda: 1786240800.0)
    batch = fetch_sse(_source("sse"), object(), "missing-watermark", 30)
    assert batch.health == "degraded"
    assert batch.error_code == "stale_provider"


def test_304_rechecks_persisted_content_freshness(monkeypatch, tmp_path):
    store = NewsSourceStore(tmp_path / "news.sqlite")
    latest = 1786240800.0
    store.record_batch(FetchBatch(
        source_id="sse", watermark="official-watermark", health="healthy",
        latest_published_at=latest,
    ))
    previous_success = store.state("sse")["last_success_at"]
    monkeypatch.setattr(
        "quantmaster.ai.news_providers._fetch_bytes",
        lambda source, url, fetch_store: (None, source["url"], ""),
    )
    monkeypatch.setattr(
        "quantmaster.ai.news_providers.time.time",
        lambda: latest + (169 * 3600),
    )
    batch = fetch_builtin_source(store.get("sse") or {}, store, limit=30)
    assert batch.health == "degraded"
    assert batch.error_code == "stale_provider"
    assert batch.latest_published_at == latest

    monkeypatch.setattr("quantmaster.ai.news_sources._utc_iso", lambda: "2099-01-01T00:00:00+00:00")
    store.record_batch(batch)
    state = store.state("sse")
    assert state["health"] == "degraded"
    assert state["last_success_at"] == previous_success
    assert state["latest_published_at"] == latest
    assert state["last_error_code"] == "stale_provider"
    assert "超过 168.0 小时契约" in state["last_error"]


def test_304_without_persisted_content_time_fails(monkeypatch, tmp_path):
    store = NewsSourceStore(tmp_path / "news.sqlite")
    monkeypatch.setattr(
        "quantmaster.ai.news_providers._fetch_bytes",
        lambda source, url, fetch_store: (None, source["url"], ""),
    )
    with pytest.raises(NewsContractError) as error:
        fetch_builtin_source(store.get("sse") or {}, store, limit=30)
    assert error.value.code == "missing_latest_published_at"


def test_custom_source_cannot_claim_official(tmp_path):
    store = NewsSourceStore(tmp_path / "news.sqlite")
    with pytest.raises(ValueError, match="不能声明为官方"):
        store.create({
            "name": "伪官方", "kind": "rss", "group_name": "periodic",
            "url": "https://example.test/feed", "is_official": True,
        })


def test_custom_source_cannot_use_official_group(tmp_path):
    store = NewsSourceStore(tmp_path / "news.sqlite")
    with pytest.raises(ValueError, match="不能使用官方分组"):
        store.create({
            "name": "伪官方分组", "kind": "rss", "group_name": "official",
            "url": "https://example.test/feed", "is_official": False,
        })


def test_builtin_tunable_contract_survives_store_reconstruction(tmp_path):
    path = tmp_path / "news.sqlite"
    store = NewsSourceStore(path)
    updated = store.update("sse", {
        "group_name": "periodic", "item_limit": 100, "max_age_hours": 24,
    })
    assert updated["group_name"] == "periodic"
    assert updated["item_limit"] == 100
    assert updated["max_age_hours"] == 24

    reopened = NewsSourceStore(path)
    persisted = reopened.get("sse") or {}
    assert persisted["group_name"] == "periodic"
    assert persisted["item_limit"] == 100
    assert persisted["max_age_hours"] == 24


def test_future_latest_published_time_cannot_make_provider_healthy():
    now = 1786240800.0
    with pytest.raises(NewsContractError) as error:
        evaluate_freshness(
            _source("sse"), now + 301, "committed", now=now,
        )
    assert error.value.code == "future_latest_published_at"


def test_ndrc_news_freshness_threshold_is_not_monthly_source_length(tmp_path):
    source = _source("ndrc")
    assert source["max_age_hours"] == 336
    now = 1786240800.0
    health, code, _message = evaluate_freshness(
        source, now - 337 * 3600, "committed", now=now,
    )
    assert health == "degraded"
    assert code == "stale_provider"
    path = tmp_path / "news.sqlite"
    store = NewsSourceStore(path)
    with store._conn() as connection:
        connection.execute(
            "UPDATE news_sources SET max_age_hours=1080,updated_at=created_at WHERE id='ndrc'",
        )
    assert (NewsSourceStore(path).get("ndrc") or {})["max_age_hours"] == 336


def test_official_and_custom_parsers_reject_future_item_times(monkeypatch):
    official = (
        '<ul><li><a href="/disclosure/announcement/general/jjzssgg/c/'
        'c_20990101_99999999.shtml">未来官方公告条目</a><span>2099-01-01</span></li></ul>'
    ).encode()
    monkeypatch.setattr(
        "quantmaster.ai.news_providers._fetch_bytes",
        lambda source, url, store: (official, source["url"], "news_raw/sse/future.gz"),
    )
    with pytest.raises(NewsContractError) as official_error:
        fetch_sse(_source("sse"), object(), "old", 30)
    assert official_error.value.code == "future_published_at"

    source = {
        "id": "custom-json", "name": "自定义 JSON", "kind": "json",
        "group_name": "periodic", "url": "https://example.test/news",
        "item_limit": 2, "factor_weight": 1, "is_official": False,
        "max_age_hours": 24, "parser": {
            "items_path": "items", "title_path": "title", "id_path": "id",
            "published_at_path": "published_at",
        },
        "auth_type": "none", "auth_header": "",
    }
    payload = {"items": [{
        "id": "future", "title": "未来自定义消息", "published_at": "2099-01-01",
    }]}
    monkeypatch.setattr(
        "quantmaster.ai.news_sources._fetch_bytes",
        lambda *args, **kwargs: (json.dumps(payload).encode(), source["url"], "sha256:future"),
    )
    with pytest.raises(NewsContractError) as custom_error:
        fetch_declarative_source(source, object())
    assert custom_error.value.code == "future_published_at"


def test_custom_batch_keeps_committed_watermark_when_response_has_gap(monkeypatch):
    source = {
        "id": "custom-json", "name": "自定义 JSON", "kind": "json",
        "group_name": "periodic", "url": "https://example.test/news",
        "item_limit": 2, "factor_weight": 1, "is_official": False,
        "max_age_hours": 24, "parser": {
            "items_path": "items", "title_path": "title", "id_path": "id",
            "published_at_path": "published_at",
        },
        "auth_type": "none", "auth_header": "",
    }
    payload = {"items": [
        {"id": "new-2", "title": "第二条自定义消息", "published_at": "2026-08-09 10:02:00"},
        {"id": "new-1", "title": "第一条自定义消息", "published_at": "2026-08-09 10:01:00"},
    ]}
    monkeypatch.setattr(
        "quantmaster.ai.news_sources._fetch_bytes",
        lambda *args, **kwargs: (
            json.dumps(payload).encode(), source["url"], "sha256:custom",
        ),
    )
    monkeypatch.setattr(
        "quantmaster.ai.news_sources.time.time", lambda: 1786241400.0,
    )
    batch = fetch_declarative_source(
        source, object(), state={
            "watermark": "old", "latest_published_at": 1786240800.0,
        },
    )
    assert batch.complete is False
    assert batch.watermark == "old"
    assert batch.pending_watermark == "new-2"
    assert batch.error_code == "watermark_not_reached"


def test_raw_policy_archives_official_and_hashes_nonofficial(tmp_path):
    store = NewsSourceStore(tmp_path / "news.sqlite")
    unofficial = store.save_response(
        "sina_live", "https://example.test/fast", b"fast body", httpx.Headers(), 200,
    )
    official = store.save_response(
        "pboc", _official_url("pboc", "official"), b"official body", httpx.Headers(), 200,
        official=True,
    )
    assert unofficial.startswith("sha256:")
    assert not (tmp_path / unofficial).exists()
    assert official.startswith("news_raw/")
    assert (tmp_path / official).exists()


def test_official_raw_write_atomically_repairs_existing_corrupt_blob(tmp_path):
    store = NewsSourceStore(tmp_path / "news.sqlite")
    payload = b"official evidence that must remain recoverable"
    key = store.save_response(
        "pboc", _official_url("pboc", "repair"), payload, httpx.Headers(), 200,
        official=True,
    )
    (tmp_path / key).write_bytes(b"truncated gzip")
    assert read_raw_evidence(store.path, key) is None

    repaired = store.save_response(
        "pboc", _official_url("pboc", "repair"), payload, httpx.Headers(), 200,
        official=True,
    )
    assert repaired == key
    assert read_raw_evidence(store.path, key) == payload


def test_formal_factors_require_recoverable_manifested_raw_and_survive_304(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    now = time.time()
    valid_url = _official_url("pboc", "valid")
    valid_raw = store.sources.save_response(
        "pboc", valid_url, b"valid official full article", httpx.Headers({"etag": "v1"}),
        200, official=True,
    )
    tampered_raw = store.sources.save_response(
        "pboc", _official_url("pboc", "tampered"), b"original official article",
        httpx.Headers(), 200, official=True,
    )
    with gzip.open(tmp_path / tampered_raw, "wb") as handle:
        handle.write(b"different bytes under the original digest name")
    missing_raw = f"news_raw/pboc/2026-08-09/{'0' * 64}.gz"
    common = {
        "source": "pboc", "is_official": True, "content_scope": "full_article",
        "published_at": "2026-08-09T01:00:00+00:00",
        "published_at_epoch": now - 60, "fetched_at": now,
        "confidence": 1, "importance_score": 100, "analysis_status": "complete",
    }
    valid = NewsItem(
            **common, provider_item_id="valid", title="有效证据", content="有效官方正文",
            raw_cache_key=valid_raw, symbols=["600001.SH"], sentiment=0.5, url=valid_url,
        )
    empty = NewsItem(
            **common, provider_item_id="empty", title="空证据", content="空证据正文",
            raw_cache_key="", symbols=["600002.SH"], sentiment=0.6,
        )
    missing = NewsItem(
            **common, provider_item_id="missing", title="缺失证据", content="缺失证据正文",
            raw_cache_key=missing_raw, symbols=["600003.SH"], sentiment=0.7,
        )
    tampered = NewsItem(
            **common, provider_item_id="tampered", title="篡改证据", content="篡改证据正文",
            raw_cache_key=tampered_raw, symbols=["600004.SH"], sentiment=0.8,
        )
    _bind_official(store, valid)
    assert store.save([
        valid,
        empty, missing, tampered,
    ]) == 4

    before = store.factor_rows(end_epoch=now + 10)
    assert [row["sentiment"] for row in before] == [0.5]
    assert store.market_sentiment(as_of=now + 10, days=1)["event_count"] == 1
    assert store.event_focus(1)["top_symbols"][0]["symbol"] == "600001.SH"

    store.sources.touch_not_modified("pboc", valid_url)
    after = store.factor_rows(end_epoch=now + 10)
    assert [row["sentiment"] for row in after] == [0.5]


def test_same_source_raw_binding_cannot_be_borrowed_by_another_article(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    now = time.time()
    raw_key = _official_raw(store, "pboc", "one immutable official response")
    original = NewsItem(
        source="pboc", provider_item_id="official-one", title="已绑定官方条目",
        content="已绑定的官方解析正文", url=_official_url("pboc", "official-one"),
        published_at="2026-08-09T01:00:00+00:00", published_at_epoch=now - 60,
        is_official=True, content_scope="full_article", raw_cache_key=raw_key,
        symbols=["600001.SH"], sentiment=0.4, confidence=1,
        importance_score=100, analysis_status="complete",
    )
    _bind_official(store, original)
    forged = NewsItem(
        source="pboc", provider_item_id="official-two", title="借用证据的另一条目",
        content="与已绑定解析产物不同的伪造正文",
        url=_official_url("pboc", "official-two"),
        published_at=original.published_at, published_at_epoch=original.published_at_epoch,
        is_official=True, content_scope="full_article", raw_cache_key=raw_key,
        evidence_binding_hash=original.evidence_binding_hash,
        symbols=["600002.SH"], sentiment=0.9, confidence=1,
        importance_score=100, analysis_status="complete",
    )

    assert store.save([original, forged]) == 2

    rows = store.factor_rows(end_epoch=now + 10)
    assert [row["sentiment"] for row in rows] == [0.4]
    assert [item["symbol"] for item in store.event_focus(1)["top_symbols"]] == ["600001.SH"]


def test_incomplete_limit_window_is_formally_locked_until_full_retry_commits(tmp_path):
    path = tmp_path / "news.sqlite"
    store = NewsStore(path)
    now = time.time()
    top = NewsItem(
        source="pboc", provider_item_id="window-head", title="上限内利好",
        content="列表上限内的正面正文", url=_official_url("pboc", "window-head"),
        published_at="2026-08-09T01:01:00+00:00", published_at_epoch=now - 60,
        is_official=True, content_scope="full_article",
        raw_cache_key=_official_raw(store, "pboc", "top raw"),
        symbols=["600001.SH"], sentiment=1, confidence=1,
        importance_score=100, analysis_status="complete",
    )
    omitted = NewsItem(
        source="pboc", provider_item_id="limit-plus-one", title="第 limit+1 条利空",
        content="被不完整快照遗漏且足以反转结论的负面正文",
        url=_official_url("pboc", "limit-plus-one"),
        published_at="2026-08-09T01:01:00+00:00", published_at_epoch=now - 60,
        is_official=True, content_scope="full_article",
        raw_cache_key=_official_raw(store, "pboc", "omitted raw"),
        symbols=["600001.SH"], sentiment=-1, confidence=1,
        importance_score=100, analysis_status="complete",
    )
    store.sources.bind_articles([top, omitted])
    incomplete = FetchBatch(
        source_id="pboc", articles=[top], watermark="committed-old",
        previous_watermark="committed-old", pending_watermark="window-head",
        complete=False, health="degraded", error_code="snapshot_window_exhausted",
    )
    pending_window = store.sources.register_ingest_batch(incomplete, "limit-window-partial")
    assert store.save([top]) == 1

    assert store.factor_rows(end_epoch=now + 10) == []
    assert store.market_sentiment(as_of=now + 10, days=1)["event_count"] == 0
    with store._conn() as connection:
        assert connection.execute(
            "SELECT status FROM news_ingest_windows WHERE window_id=?",
            (pending_window,),
        ).fetchone()[0] == "pending"

    reopened = NewsStore(path)
    assert reopened.factor_rows(end_epoch=now + 10) == []
    assert reopened.market_sentiment(as_of=now + 10, days=1)["event_count"] == 0

    complete = FetchBatch(
        source_id="pboc", articles=[top, omitted], watermark="window-head",
        previous_watermark="committed-old", complete=True, health="healthy",
    )
    completed_window = reopened.sources.register_ingest_batch(complete, "limit-window-full")
    assert completed_window == pending_window
    assert reopened.save([top, omitted]) == 1

    rows = reopened.factor_rows(end_epoch=now + 10)
    assert sorted(row["sentiment"] for row in rows) == [-1, 1]
    market = reopened.market_sentiment(as_of=now + 10, days=1)
    assert market["event_count"] == 2
    assert market["score"] == 0


def test_durable_item_queue_keeps_committed_items_and_only_recovers_pending(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    first = NewsItem(source="sina_live", provider_item_id="one", title="first", content="one")
    second = NewsItem(source="sina_live", provider_item_id="two", title="second", content="two")
    batch = FetchBatch(
        source_id="sina_live", articles=[
            FetchedArticle(source="sina_live", provider_item_id="one", title="first", content="one"),
            FetchedArticle(source="sina_live", provider_item_id="two", title="second", content="two"),
        ], previous_watermark="old", watermark="two", complete=True,
    )
    window = store.sources.register_ingest_batch(batch, "durable-two-items")
    # Simulate an interruption after the first atomic row commit.
    first.ingest_window_id = second.ingest_window_id = window
    first.ingest_batch_id = second.ingest_batch_id = "durable-two-items"
    assert store.save([first]) == 1
    pending = store.sources.pending_ingest_items("sina_live")
    assert [row["provider_item_id"] for row in pending] == ["two"]
    store.sources.record_batch(batch)
    assert store.sources.state("sina_live")["watermark"] != "two"
    with pytest.raises(NewsContractError, match="durable queue"):
        store.sources.complete_ingest_window(window, "durable-two-items")

    reopened = NewsStore(tmp_path / "news.sqlite")
    # Recovery must never redownload/rewrite the completed business key.
    assert [row["provider_item_id"] for row in reopened.sources.pending_ingest_items("sina_live")] == ["two"]
    assert reopened.save([second]) == 1
    assert reopened.sources.pending_ingest_items("sina_live") == []
    reopened.sources.complete_ingest_window(window, "durable-two-items")


def test_item_failure_diagnostic_redacts_credentials_and_has_parser_metadata(tmp_path):
    sources = NewsSourceStore(tmp_path / "news.sqlite")
    sources.record_item_diagnostic(
        "sina_live", stage="detail_fetch", diagnostic_code="unexpected_content_type",
        raw_response_ref="https://example.test/a?api_key=secret&cursor=1",
        content_type="text/html; charset=gb18030", encoding="gb18030",
        parser_version="sina-v2", detail="response body intentionally omitted",
    )
    with sources._conn() as connection:
        row = connection.execute(
            "SELECT stage,diagnostic_code,raw_response_ref,content_type,encoding,parser_version,detail "
            "FROM news_ingest_failure_diagnostics",
        ).fetchone()
    assert tuple(row[:2]) == ("detail_fetch", "unexpected_content_type")
    assert "secret" not in row[2]
    assert "api_key=<redacted>" in row[2]
    assert tuple(row[3:]) == (
        "text/html; charset=gb18030", "gb18030", "sina-v2",
        "response body intentionally omitted",
    )


def test_append_only_raw_manifest_keeps_prior_feed_batch_factor_eligible(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    now = time.time()
    feed_url = "https://www.stats.gov.cn/sj/zxfb/rss.xml"
    raw_one = store.sources.save_response(
        "nbs_release", feed_url, b"official feed batch one", httpx.Headers(), 200,
        official=True,
    )
    first = NewsItem(
        source="nbs_release", provider_item_id="feed-item-one", title="第一批统计公告",
        content="第一批可恢复的统计公告摘要", raw_cache_key=raw_one,
        is_official=True, content_scope="feed_summary",
        published_at="2026-08-09T01:00:00+00:00", published_at_epoch=now - 120,
        symbols=["600001.SH"], sentiment=0.2, confidence=1,
        importance_score=100, analysis_status="complete",
    )
    _bind_official(store, first)
    assert store.save([first]) == 1
    assert len(store.factor_rows(end_epoch=now + 10)) == 1

    raw_two = store.sources.save_response(
        "nbs_release", feed_url, b"official feed batch two", httpx.Headers(), 200,
        official=True,
    )
    second = NewsItem(
        source="nbs_release", provider_item_id="feed-item-two", title="第二批统计公告",
        content="第二批可恢复的统计公告摘要", raw_cache_key=raw_two,
        is_official=True, content_scope="feed_summary",
        published_at="2026-08-09T01:01:00+00:00", published_at_epoch=now - 60,
        symbols=["600002.SH"], sentiment=0.3, confidence=1,
        importance_score=100, analysis_status="complete",
    )
    _bind_official(store, second)
    assert store.save([second]) == 1
    assert read_raw_evidence(store.path, raw_one) == b"official feed batch one"
    assert len(store.factor_rows(end_epoch=now + 10)) == 2
    with store._conn() as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM news_raw_manifest WHERE source_id='nbs_release' "
            "AND url=?",
            (feed_url,),
        ).fetchone()[0] == 2


def test_legacy_unbound_raw_stays_factor_ineligible_when_304_restores_manifest(tmp_path):
    path = tmp_path / "news.sqlite"
    store = NewsStore(path)
    now = time.time()
    detail_url = "https://www.pbc.gov.cn/legacy-valid/index.html"
    raw_key = store.sources.save_response(
        "pboc", detail_url, b"legacy but cryptographically valid official raw",
        httpx.Headers({"etag": "legacy-v1"}), 200, official=True,
    )
    item = NewsItem(
        source="pboc", provider_item_id="legacy-valid", title="旧库有效官方证据",
        content="旧库中已有且仍可恢复的官方正文", raw_cache_key=raw_key,
        is_official=True, content_scope="full_article",
        published_at="2026-08-09T01:00:00+00:00", published_at_epoch=now - 60,
        symbols=["600001.SH"], sentiment=0.4, confidence=1,
        importance_score=100, analysis_status="complete",
    )
    assert store.save([item]) == 1
    store.sources.touch_not_modified("pboc", detail_url)
    with store._conn() as connection:
        connection.execute("DROP TABLE news_raw_manifest")

    migrated = NewsStore(path)
    assert migrated.factor_rows(end_epoch=now + 10) == []
    with migrated._conn() as connection:
        manifest = connection.execute(
            "SELECT status_code FROM news_raw_manifest WHERE source_id='pboc' "
            "AND raw_cache_key=?",
            (raw_key,),
        ).fetchone()
        assert manifest[0] == 304
        connection.execute("DELETE FROM news_raw_manifest")

    migrated.sources.touch_not_modified("pboc", detail_url)
    with migrated._conn() as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM news_raw_manifest WHERE source_id='pboc' "
            "AND raw_cache_key=?",
            (raw_key,),
        ).fetchone()[0] == 1
    assert migrated.factor_rows(end_epoch=now + 10) == []


def test_v6_bootstrap_forces_200_until_current_official_window_is_bound(tmp_path):
    path = tmp_path / "news.sqlite"
    store = NewsStore(path)
    now = time.time()
    detail_url = _official_url("pboc", "bootstrap-current")
    legacy_raw = store.sources.save_response(
        "pboc", detail_url, b"legacy unbound response",
        httpx.Headers({"etag": "legacy-etag"}), 200, official=True,
    )
    legacy = NewsItem(
        source="pboc", provider_item_id="legacy-unbound", title="旧未绑定条目",
        content="旧解析正文", url=detail_url,
        published_at="2026-08-09T00:00:00+00:00", published_at_epoch=now - 120,
        is_official=True, content_scope="full_article", raw_cache_key=legacy_raw,
        symbols=["600001.SH"], sentiment=0.8, confidence=1,
        importance_score=100, analysis_status="complete",
    )
    assert store.save([legacy]) == 1
    store.sources.record_batch(FetchBatch(
        source_id="pboc", watermark="committed-before-v6",
        latest_published_at=now - 120,
    ))
    assert store.sources.cache_headers("pboc", detail_url) == {
        "If-None-Match": "legacy-etag",
    }
    store.sources.touch_not_modified("pboc", detail_url)
    with store._conn() as connection:
        connection.execute("DROP TABLE news_raw_manifest")
        connection.execute("DROP TABLE news_article_evidence_manifest")

    migrated = NewsStore(path)
    state = migrated.sources.state("pboc")
    assert state["watermark"] == "committed-before-v6"
    assert state["evidence_bootstrap_pending"] == 1
    assert migrated.sources.cache_headers("pboc", detail_url) == {}
    assert migrated.factor_rows(end_epoch=now + 10) == []

    current_raw = migrated.sources.save_response(
        # The provider may legitimately return byte-identical content.  The
        # fresh 200 cache status, not a changed digest, releases the 304 trap.
        "pboc", detail_url, b"legacy unbound response",
        httpx.Headers({"etag": "current-etag"}), 200, official=True,
    )
    assert current_raw == legacy_raw
    current = NewsItem(
        source="pboc", provider_item_id="current-bound", title="当前已绑定条目",
        content="当前窗口重新解析的正文", url=detail_url,
        published_at="2026-08-09T00:01:00+00:00", published_at_epoch=now - 60,
        is_official=True, content_scope="full_article", raw_cache_key=current_raw,
        symbols=["600002.SH"], sentiment=0.3, confidence=1,
        importance_score=100, analysis_status="complete",
    )
    migrated.sources.bind_articles([current])
    migrated.sources.register_ingest_batch(
        FetchBatch(
            source_id="pboc", articles=[current], watermark=current.provider_item_id,
            complete=True,
        ),
        "bootstrap-current-complete",
    )
    assert migrated.save([current]) == 1
    assert migrated.sources.cache_headers("pboc", detail_url) == {}
    migrated.sources.complete_evidence_bootstrap("pboc")

    assert migrated.sources.cache_headers("pboc", detail_url) == {
        "If-None-Match": "current-etag",
    }
    assert [row["sentiment"] for row in migrated.factor_rows(end_epoch=now + 10)] == [0.3]


def test_event_identity_keeps_repeat_notices_across_days_and_dedupes_reposts(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    now = time.time()
    repeated_content = "周期性标准通知正文完全相同"
    first_day = now - 2 * 86400
    second_day = now - 86400
    first_raw = _official_raw(store, "pboc", "第一日详情 raw")
    second_raw = _official_raw(store, "pboc", "第二日详情 raw")
    repost_raw = _official_raw(store, "sse", "第二日跨源转载 raw")
    common = {
        "content": repeated_content, "is_official": True,
        "content_scope": "full_article", "symbols": ["600001.SH"],
        "confidence": 1, "importance_score": 100, "analysis_status": "complete",
    }
    first = NewsItem(
            **common, source="pboc", provider_item_id="repeat-day-one",
            title="第一日标准通知", raw_cache_key=first_raw,
            published_at="2026-08-07T01:00:00+00:00", published_at_epoch=first_day,
            sentiment=0.2,
        )
    second = NewsItem(
            **common, source="pboc", provider_item_id="repeat-day-two",
            title="第二日标准通知", raw_cache_key=second_raw,
            published_at="2026-08-08T01:00:00+00:00", published_at_epoch=second_day,
            sentiment=0.3,
        )
    repost = NewsItem(
            **common, source="sse", provider_item_id="repeat-day-two-repost",
            title="第二日标准通知转载", raw_cache_key=repost_raw,
            published_at="2026-08-08T01:01:00+00:00", published_at_epoch=second_day + 60,
            sentiment=0.9,
        )
    _bind_official(store, first, second, repost)
    assert store.save([first, second, repost]) == 3

    market = store.market_sentiment(as_of=now + 10, days=7)
    focus = store.event_focus(7)
    assert market["event_count"] == 2
    assert len(focus["top_symbols"]) == 1
    assert focus["top_symbols"][0]["symbol"] == "600001.SH"
    assert focus["top_symbols"][0]["count"] == 2


def test_store_keeps_published_and_fetched_times_separate(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    item = NewsItem(
        source="unit", provider_item_id="provider-1", title="时点契约", content="正文",
        published_at="2026-08-09T01:00:00+00:00", published_at_epoch=1786237200.0,
        fetched_at=1786240800.0,
    )
    assert store.save([item]) == 1
    row = store.recent()[0]
    assert row["provider_item_id"] == "provider-1"
    assert row["published_at_epoch"] == 1786237200.0
    assert row["fetched_at"] == 1786240800.0


def test_same_provider_id_revision_updates_current_and_archives_previous(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    source_store = store.sources
    first_raw = source_store.save_response(
        "pboc", _official_url("pboc", "list-v1"), b"official raw v1",
        httpx.Headers(), 200, official=True,
    )
    second_raw = source_store.save_response(
        "pboc", _official_url("pboc", "list-v2"), b"official raw v2",
        httpx.Headers(), 200, official=True,
    )
    first = NewsItem(
        source="pboc", provider_item_id="official-42", title="同一官方条目",
        content="第一版较长的正文内容", raw_cache_key=first_raw,
        published_at="2026-08-09T01:00:00+00:00", published_at_epoch=1786237200.0,
        fetched_at=1786240800.0, is_official=True,
        symbols=["600519.SH"], sentiment=0.8, confidence=1,
        importance_score=100, analysis_status="complete",
    )
    _bind_official(store, first)
    assert store.save([first]) == 1
    original = store.recent()[0]

    revised = NewsItem(
        source="pboc", provider_item_id="official-42", title="同一官方条目（修订）",
        content="修订版", raw_cache_key=second_raw,
        published_at="2026-08-09T01:00:00+00:00", published_at_epoch=1786237200.0,
        fetched_at=1786244400.0, is_official=True,
    )
    _bind_official(store, revised)
    assert store.save([revised]) == 1
    current = store.recent()[0]
    assert current["id"] == original["id"]
    assert current["content"] == "修订版"
    assert current["content_hash"] == store.content_hash(revised)
    assert current["raw_cache_key"] == second_raw
    assert current["analysis_status"] == "pending"
    assert current["symbols"] == []
    with store._conn() as connection:
        archived = dict(connection.execute(
            "SELECT * FROM news_revisions WHERE news_id=?",
            (current["id"],),
        ).fetchone())
    assert archived["revision_number"] == 1
    assert archived["content"] == "第一版较长的正文内容"
    assert archived["content_hash"] == original["content_hash"]
    assert archived["raw_cache_key"] == first_raw
    old = 1780000000.0
    for key in (first_raw, second_raw):
        path = tmp_path / key
        os.utime(path, (old, old))
    assert first_raw not in source_store.raw_gc_candidates(0)
    assert second_raw not in source_store.raw_gc_candidates(0)
    assert source_store.cleanup_raw(0) == 0
    assert (tmp_path / first_raw).exists()
    assert (tmp_path / second_raw).exists()


def test_detail_failure_cannot_downgrade_existing_verified_full_article(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    full_raw = _official_raw(store, "sse", "已经验证并完成分析的官方详情全文")
    listing_raw = _official_raw(store, "sse", "官方列表快照")
    published = time.time() - 60
    full = NewsItem(
        source="sse", provider_item_id="stable-detail", title="不可降级的官方公告",
        content="已经验证并完成分析的官方详情全文", raw_cache_key=full_raw,
        is_official=True, content_scope="full_article",
        published_at="2026-08-09T01:00:00+00:00", published_at_epoch=published,
        symbols=["600519.SH"], sentiment=0.6, confidence=1,
        importance_score=100, analysis_status="complete",
    )
    _bind_official(store, full)
    assert store.save([full]) == 1
    before = store.recent()[0]

    listing_only = NewsItem(
        source="sse", provider_item_id="stable-detail", title="不可降级的官方公告",
        content="不可降级的官方公告", raw_cache_key=listing_raw,
        is_official=True, content_scope="listing_title_only",
        published_at="2026-08-09T01:00:00+00:00", published_at_epoch=published,
    )
    if not listing_only.url:
        listing_only.url = _official_url("sse", listing_only.provider_item_id)
    store.sources.bind_articles([listing_only])
    store.sources.register_ingest_batch(
        FetchBatch(
            source_id="sse", articles=[listing_only], watermark="stable-detail",
            previous_watermark="stable-detail", pending_watermark="stable-detail",
            complete=False, health="degraded", error_code="official_detail_incomplete",
        ),
        "listing-detail-incomplete",
    )
    assert store.save([listing_only]) == 0
    after = store.recent()[0]
    assert after["content"] == before["content"]
    assert after["content_hash"] == before["content_hash"]
    assert after["raw_cache_key"] == full_raw
    assert after["content_scope"] == "full_article"
    assert after["analysis_status"] == "complete"
    assert after["sentiment"] == 0.6
    with store._conn() as connection:
        assert connection.execute("SELECT COUNT(*) FROM news_revisions").fetchone()[0] == 0


def test_distinct_provider_ids_with_same_title_and_time_keep_both_evidence_rows(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    common = {
        "source": "pboc",
        "title": "同标题同发布时间公告",
        "published_at": "2026-08-09T01:00:00+00:00",
        "published_at_epoch": 1786237200.0,
        "is_official": True,
        "content_scope": "full_text",
    }
    items = [
        NewsItem(
            **common, provider_item_id="official-a", content="公告 A 正文",
            raw_cache_key="news_raw/pboc/a.gz",
        ),
        NewsItem(
            **common, provider_item_id="official-b", content="公告 B 正文",
            raw_cache_key="news_raw/pboc/b.gz",
        ),
    ]
    assert store.save(items) == 2
    rows = store.recent(limit=10)
    assert {row["provider_item_id"] for row in rows} == {"official-a", "official-b"}
    assert {row["raw_cache_key"] for row in rows} == {
        "news_raw/pboc/a.gz", "news_raw/pboc/b.gz",
    }
    with store._conn() as connection:
        assert connection.execute("SELECT COUNT(*) FROM news_revisions").fetchone()[0] == 0
        table_sql = str(connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='news'",
        ).fetchone()[0]).replace(" ", "").lower()
    assert "unique(source,title,published_at)" not in table_sql


def test_v3_title_identity_migration_preserves_archive_and_removes_constraint(tmp_path):
    path = tmp_path / "news.sqlite"
    with sqlite3.connect(path) as connection:
        connection.executescript("""
            CREATE TABLE news (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT,title TEXT,content TEXT,url TEXT,published_at TEXT,
                symbols TEXT,sectors TEXT,event_type TEXT,sentiment REAL,summary TEXT,
                created_at REAL,UNIQUE(source,title,published_at));
            CREATE TABLE news_store_meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);
            INSERT INTO news_store_meta(key,value) VALUES ('schema_version','3');
            INSERT INTO news(
                source,title,content,url,published_at,symbols,sectors,event_type,
                sentiment,summary,created_at
            ) VALUES (
                'pboc','旧版公告','旧版正文','https://example.test/legacy',
                '2026-08-09','[]','[]','其他',0,'未知完成时点的旧分析',1786240800
            );
        """)

    store = NewsStore(path)
    raw_key = _official_raw(store, "pboc", "旧版正文")
    with store._conn() as connection:
        connection.execute(
            "UPDATE news SET is_official=1,content_scope='full_text',raw_cache_key=?,"
            "published_at_epoch=?,confidence=1,importance_score=100 WHERE title='旧版公告'",
            (raw_key, time.time() - 60),
        )
        tables = {str(row[0]) for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'",
        )}
        current = dict(connection.execute("SELECT * FROM news").fetchone())
        archived = dict(connection.execute("SELECT * FROM news_legacy_v3").fetchone())
        table_sql = str(connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='news'",
        ).fetchone()[0]).replace(" ", "").lower()
        foreign_key_errors = connection.execute("PRAGMA foreign_key_check").fetchall()
    assert "news_legacy_v3" in tables
    assert current["title"] == archived["title"] == "旧版公告"
    assert current["content_version_at"] == 1786240800
    assert current["analysis_status"] == "complete"
    assert current["analysis_updated_at"] == 0
    assert "unique(source,title,published_at)" not in table_sql
    assert foreign_key_errors == []
    assert store.factor_rows(end_epoch=time.time() + 10) == []
    assert store.market_sentiment(as_of=time.time() + 10, days=1)["event_count"] == 0


def test_historical_factors_backfill_analysis_to_publication_window(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    now = time.time()
    first_seen = now - 7200
    as_of = now - 3600
    raw_key = _official_raw(store, "pboc", "可复核的官方全文")
    item = NewsItem(
        source="pboc", provider_item_id="late-analysis", title="稍后才完成分析",
        content="可复核的官方全文", is_official=True, content_scope="full_article",
        published_at="2026-08-09T01:00:00+00:00", published_at_epoch=first_seen,
        raw_cache_key=raw_key,
    )
    _bind_official(store, item)
    assert store.save([item]) == 1
    item_id = store.max_id()
    with store._conn() as connection:
        connection.execute(
            "UPDATE news SET first_seen_at=?,content_version_at=? WHERE id=?",
            (first_seen, first_seen, item_id),
        )
    analyzed = NewsItem(
        source="pboc", title=item.title, content=item.content,
        symbols=["600519.SH"], sentiment=0.8, confidence=1,
        importance_score=100,
    )
    assert store.update_analysis(item_id, analyzed)
    historical = store.factor_rows(end_epoch=as_of)
    assert len(historical) == 1
    assert historical[0]["sentiment"] == 0.8
    assert store.market_sentiment(as_of=as_of, days=1)["event_count"] == 0
    assert len(store.factor_rows(end_epoch=now + 10)) == 1


def test_historical_factor_uses_latest_analysis_at_original_publication_time(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    now = time.time()
    first_seen = now - 7200
    as_of = now - 3600
    original_raw = _official_raw(store, "pboc", "修订前官方全文")
    original = NewsItem(
        source="pboc", provider_item_id="revised-later", title="修订前公告",
        content="修订前官方全文", is_official=True, content_scope="full_article",
        published_at="2026-08-09T01:00:00+00:00", published_at_epoch=first_seen,
        raw_cache_key=original_raw,
        symbols=["600519.SH"], sentiment=0.6, confidence=1,
        importance_score=100, analysis_status="complete",
    )
    _bind_official(store, original)
    assert store.save([original]) == 1
    item_id = store.max_id()
    with store._conn() as connection:
        connection.execute(
            "UPDATE news SET first_seen_at=?,content_version_at=?,analysis_updated_at=? "
            "WHERE id=?",
            (first_seen, first_seen, first_seen, item_id),
        )
    assert len(store.factor_rows(end_epoch=as_of)) == 1

    revised_raw = _official_raw(store, "pboc", "修订后官方全文")
    revised = NewsItem(
        source="pboc", provider_item_id="revised-later", title="修订后公告",
        content="修订后官方全文", is_official=True, content_scope="full_article",
        published_at="2026-08-09T01:00:00+00:00", published_at_epoch=first_seen,
        raw_cache_key=revised_raw,
    )
    _bind_official(store, revised)
    assert store.save([revised]) == 1
    reanalyzed = NewsItem(
        source="pboc", title=revised.title, content=revised.content,
        symbols=["600519.SH"], sentiment=-0.7, confidence=1,
        importance_score=100,
    )
    assert store.update_analysis(item_id, reanalyzed)
    historical = store.factor_rows(end_epoch=as_of)
    assert len(historical) == 1
    assert historical[0]["sentiment"] == -0.7
    current = store.factor_rows(end_epoch=now + 10)
    assert len(current) == 1
    assert current[0]["sentiment"] == -0.7


def test_old_published_notice_backfilled_today_is_not_a_current_event(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    now = time.time()
    published = now - 39 * 86400
    raw_key = _official_raw(store, "pboc", "七月发布但八月才回补的官方公告正文")
    item = NewsItem(
        source="pboc", provider_item_id="old-backfill", title="旧公告今日回补",
        content="七月发布但八月才回补的官方公告正文", is_official=True,
        content_scope="full_article", raw_cache_key=raw_key,
        published_at="2026-07-01T01:00:00+00:00", published_at_epoch=published,
        fetched_at=now, symbols=["600519.SH"], sentiment=1, confidence=1,
        importance_score=100, analysis_status="complete",
    )
    _bind_official(store, item)
    assert store.save([item]) == 1
    assert store.factor_rows(start_epoch=now - 7 * 86400, end_epoch=now + 10) == []
    assert store.market_sentiment(as_of=now + 10, days=7)["event_count"] == 0
    assert store.event_focus(7)["top_symbols"] == []


def test_listing_title_only_official_item_is_not_a_trusted_factor(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    title_raw = _official_raw(store, "sse", "只有列表标题")
    full_raw = _official_raw(store, "pboc", "有正文证据的官方消息")
    published = time.time() - 60
    title_only = NewsItem(
            source="sse", provider_item_id="title-only", title="只有列表标题",
            content="只有列表标题", is_official=True,
            content_scope="listing_title_only", sentiment=1, confidence=1,
            importance_score=100, analysis_status="complete", raw_cache_key=title_raw,
            published_at_epoch=published, published_at="2026-08-09T01:00:00+00:00",
        )
    full_text = NewsItem(
            source="pboc", provider_item_id="full-text", title="有正文证据",
            content="有正文证据的官方消息", is_official=True,
            content_scope="full_article", sentiment=-0.5, confidence=1,
            importance_score=100, analysis_status="complete", raw_cache_key=full_raw,
            published_at_epoch=published, published_at="2026-08-09T01:00:00+00:00",
        )
    _bind_official(store, title_only, full_text)
    store.save([title_only, full_text])
    rows = store.factor_rows()
    assert len(rows) == 1
    assert rows[0]["sentiment"] == -0.5


def test_nonofficial_custom_source_is_excluded_from_trusted_factor(tmp_path):
    store = NewsStore(tmp_path / "news.sqlite")
    custom = store.sources.create({
        "name": "非官方研究源", "kind": "rss", "group_name": "periodic",
        "url": "https://example.test/feed", "max_age_hours": 24,
    })
    official_raw = _official_raw(store, "sse", "官方可信利空")
    published = time.time() - 60
    unofficial = NewsItem(
            source=custom["id"], title="非官方强烈利好", content="非官方强烈利好",
            symbols=["600519.SH"], sentiment=1, confidence=1,
            importance_score=100, analysis_status="complete", is_official=False,
        )
    official = NewsItem(
            source="sse", title="官方可信利空", content="官方可信利空",
            symbols=["600519.SH"], sentiment=-0.5, confidence=1,
            importance_score=100, analysis_status="complete", is_official=True,
            content_scope="full_article", raw_cache_key=official_raw,
            published_at_epoch=published, published_at="2026-08-09T01:00:00+00:00",
        )
    _bind_official(store, official)
    store.save([unofficial, official])
    rows = store.factor_rows()
    assert len(rows) == 1
    assert rows[0]["sentiment"] == -0.5
    assert custom["factor_eligible"] is False
