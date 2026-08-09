"""Built-in news providers with strict parsing, watermarks, and auditable batches."""

from __future__ import annotations

import json
import re
import sqlite3
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlencode, urljoin
from zoneinfo import ZoneInfo

import httpx
from bs4 import BeautifulSoup

from quantmaster.ai.news_contracts import (
    FetchBatch,
    FetchedArticle,
    HealthStatus,
    NewsContractError,
    NewsProviderError,
    evaluate_freshness,
    normalize_published_at,
)
from quantmaster.ai.news_sources import NewsSourceStore, _clean_text, _fetch_bytes, _parse_rss
from quantmaster.credentials import CredentialError

Provider = Callable[[dict[str, Any], NewsSourceStore, str, int], FetchBatch]
OFFICIAL_DETAIL_MIN_CHARS = 40


def _freshness(
    source: dict[str, Any], latest_published_at: float, previous_watermark: str,
) -> tuple[HealthStatus, str, str]:
    return evaluate_freshness(
        source, latest_published_at, previous_watermark, now=time.time(),
    )


def _unchanged_batch(
    source: dict[str, Any], watermark: str, raw_keys: list[str], latest_published_at: float,
) -> FetchBatch:
    health, error_code, message = _freshness(source, latest_published_at, watermark)
    return FetchBatch(
        source_id=source["id"], watermark=watermark, previous_watermark=watermark,
        health=health, complete=True,
        raw_cache_keys=list(dict.fromkeys(key for key in raw_keys if key)),
        error_code=error_code, message=message, latest_published_at=latest_published_at,
    )


def _not_modified_batch(source: dict[str, Any], watermark: str) -> FetchBatch:
    latest_published_at = float(source.get("_state_latest_published_at") or 0.0)
    health, error_code, message = _freshness(source, latest_published_at, watermark)
    pending_watermark = str(source.get("_state_pending_watermark") or "")
    if pending_watermark:
        return FetchBatch(
            source_id=source["id"], watermark=watermark,
            previous_watermark=watermark, health="degraded", complete=False,
            error_code=error_code or "backfill_not_completed_on_304",
            message=message or "来源返回 304，但先前缺口尚未补齐；保留 committed 水位",
            latest_published_at=latest_published_at,
            pending_watermark=pending_watermark,
            next_cursor=str(source.get("_state_next_cursor") or ""),
        )
    return FetchBatch(
        source_id=source["id"], watermark=watermark, previous_watermark=watermark,
        health="not_modified" if health == "healthy" else health,
        complete=True, error_code=error_code, message=message,
        latest_published_at=latest_published_at,
    )


def _batch(
    source: dict[str, Any],
    articles: list[FetchedArticle],
    previous_watermark: str,
    raw_keys: list[str],
    *,
    complete: bool = True,
    pending_watermark: str = "",
    next_cursor: str = "",
    incomplete_code: str = "watermark_not_reached",
    incomplete_message: str = "回补达到本轮安全页数上限，尚未遇到上次水位",
) -> FetchBatch:
    if not articles:
        raise NewsContractError(
            f"{source['name']} 没有解析出任何可信条目", code="empty_provider_batch",
        )
    latest_published_at = max(
        max(item.published_at_epoch for item in articles),
        float(source.get("_state_latest_published_at") or 0.0),
    )
    health, freshness_code, freshness_message = _freshness(
        source, latest_published_at, previous_watermark,
    )
    candidate_watermark = (
        pending_watermark
        or str(source.get("_state_pending_watermark") or "")
        or articles[0].provider_item_id
    )
    watermark = candidate_watermark if complete else previous_watermark
    if not complete and not freshness_code:
        health = "degraded"
    return FetchBatch(
        source_id=source["id"], articles=articles, watermark=watermark,
        previous_watermark=previous_watermark,
        health=health, complete=complete,
        raw_cache_keys=list(dict.fromkeys(key for key in raw_keys if key)),
        error_code=freshness_code or ("" if complete else incomplete_code),
        message=freshness_message or ("" if complete else incomplete_message),
        latest_published_at=latest_published_at,
        pending_watermark="" if complete else candidate_watermark,
        next_cursor="" if complete else next_cursor,
    )


def _empty_incomplete_batch(
    source: dict[str, Any],
    previous_watermark: str,
    raw_keys: list[str],
    *,
    pending_watermark: str,
    next_cursor: str,
    error_code: str,
    message: str,
) -> FetchBatch:
    latest_published_at = float(source.get("_state_latest_published_at") or 0.0)
    freshness_health, freshness_code, freshness_message = _freshness(
        source, latest_published_at, previous_watermark,
    )
    return FetchBatch(
        source_id=source["id"], watermark=previous_watermark,
        previous_watermark=previous_watermark,
        health="degraded" if freshness_health == "healthy" else freshness_health,
        complete=False,
        raw_cache_keys=list(dict.fromkeys(key for key in raw_keys if key)),
        error_code=freshness_code or error_code,
        message=freshness_message or message,
        latest_published_at=latest_published_at,
        pending_watermark=pending_watermark,
        next_cursor=next_cursor,
    )


def fetch_sina_live(
    source: dict[str, Any], store: NewsSourceStore, watermark: str, limit: int,
) -> FetchBatch:
    """Fetch Sina pages until the previous immutable feed id is reached."""
    page_size = max(1, min(100, int(source.get("item_limit") or limit or 50)))
    max_pages = 10
    cursor_value = str(source.get("_state_next_cursor") or "")
    try:
        cursor_parts = cursor_value.split(":", 1)
        start_page = max(1, int(cursor_parts[0] or 1))
        if len(cursor_parts) == 2:
            page_size = max(1, min(100, int(cursor_parts[1])))
    except (TypeError, ValueError):
        start_page = 1
    fetched_at = time.time()
    articles: list[FetchedArticle] = []
    raw_keys: list[str] = []
    reached = not watermark
    latest_published_at = 0.0
    pending_watermark = str(source.get("_state_pending_watermark") or "")
    candidate_watermark = pending_watermark
    candidate_published_at = -1.0
    last_page = start_page
    exhausted_without_watermark = False
    for page in range(start_page, start_page + max_pages):
        last_page = page
        query = urlencode({"page": page, "page_size": page_size, "zhibo_id": 152, "tag_id": 0})
        page_source = source if page == 1 else {**source, "_conditional_cache": False}
        content, final_url, raw_key = _fetch_bytes(
            page_source, f"{source['url']}?{query}", store,
        )
        if content is None:
            if page == 1 and start_page == 1 and not articles:
                return _not_modified_batch(source, watermark)
            if articles:
                return _batch(
                    source, articles, watermark, raw_keys, complete=False,
                    pending_watermark=pending_watermark or candidate_watermark,
                    next_cursor=f"{page}:{page_size}",
                    incomplete_code="page_not_modified_during_backfill",
                    incomplete_message="回补后续页返回 304，本轮保留已解析数据并等待续抓",
                )
            return _empty_incomplete_batch(
                source, watermark, raw_keys,
                pending_watermark=pending_watermark or candidate_watermark,
                next_cursor=f"{page}:{page_size}",
                error_code="page_not_modified_during_backfill",
                message="回补页返回 304，保留既有水位并等待续抓",
            )
        raw_keys.append(raw_key)
        try:
            payload = json.loads(content)
            entries = payload["result"]["data"]["feed"]["list"]
        except (KeyError, TypeError, json.JSONDecodeError) as exc:
            raise NewsContractError("新浪快讯响应结构已变化", code="sina_contract") from exc
        if not isinstance(entries, list) or not entries:
            if page == 1:
                raise NewsContractError("新浪快讯返回空列表", code="sina_empty")
            exhausted_without_watermark = bool(watermark)
            break
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            provider_id = str(entry.get("id") or entry.get("feed_id") or "").strip()
            if not provider_id:
                continue
            published, published_epoch = normalize_published_at(
                entry.get("create_time") or entry.get("created_at") or entry.get("timestamp"),
            )
            latest_published_at = max(latest_published_at, published_epoch)
            if not pending_watermark and published_epoch > candidate_published_at:
                candidate_watermark = provider_id
                candidate_published_at = published_epoch
            if watermark and provider_id == watermark:
                reached = True
                continue
            text = _clean_text(entry.get("rich_text") or entry.get("content") or entry.get("title"))
            if not text:
                continue
            link = str(entry.get("url") or entry.get("docurl") or "").strip()
            if not link:
                link = f"https://zhibo.sina.com.cn/?id={provider_id}"
            articles.append(FetchedArticle(
                source=source["id"], provider_item_id=provider_id,
                title=text[:120], content=text, url=urljoin(final_url, link),
                published_at=published, published_at_epoch=published_epoch,
                fetched_at=fetched_at, is_official=False, raw_cache_key=raw_key,
                content_scope="provider_excerpt",
            ))
        if reached:
            break
        if not watermark:  # Initial install intentionally starts at the current head.
            reached = True
            break
    if not articles and watermark and reached:
        latest_published_at = max(
            latest_published_at,
            float(source.get("_state_latest_published_at") or 0.0),
        )
        pending_watermark = pending_watermark or candidate_watermark
        if pending_watermark and pending_watermark != watermark:
            health, error_code, message = _freshness(
                source, latest_published_at, watermark,
            )
            return FetchBatch(
                source_id=source["id"], watermark=pending_watermark,
                previous_watermark=watermark, health=health, complete=True,
                raw_cache_keys=list(dict.fromkeys(key for key in raw_keys if key)),
                error_code=error_code, message=message,
                latest_published_at=latest_published_at,
            )
        return _unchanged_batch(source, watermark, raw_keys, latest_published_at)
    if latest_published_at:
        source = {
            **source,
            "_state_latest_published_at": max(
                latest_published_at,
                float(source.get("_state_latest_published_at") or 0.0),
            ),
        }
    articles.sort(
        key=lambda item: (item.published_at_epoch, item.provider_item_id),
        reverse=True,
    )
    pending_watermark = pending_watermark or candidate_watermark
    return _batch(
        source, articles, watermark, raw_keys, complete=reached,
        pending_watermark=pending_watermark,
        next_cursor=(
            "" if reached or exhausted_without_watermark
            else f"{last_page + 1}:{page_size}"
        ),
        incomplete_code=(
            "watermark_disappeared" if exhausted_without_watermark
            else "watermark_not_reached"
        ),
        incomplete_message=(
            "来源历史已耗尽但未找到 committed 水位，拒绝跳过缺口"
            if exhausted_without_watermark
            else "回补达到本轮安全页数上限，尚未遇到上次水位"
        ),
    )


def _extract_date(text: str, url: str) -> tuple[str, float]:
    match = re.search(r"(20\d{2})[-/年](\d{1,2})[-/月](\d{1,2})", text)
    if not match:
        match = re.search(r"/(20\d{2})-(\d{2})-(\d{2})/", url)
    if not match:
        match = re.search(r"/(20\d{2})(\d{2})(\d{2})/", url)
    if not match:
        raise NewsContractError("官方条目缺少可验证发布时间", code="official_missing_time")
    value = f"{match.group(1)}-{int(match.group(2)):02d}-{int(match.group(3)):02d}"
    return normalize_published_at(value)


def _local_date(epoch: float):
    return datetime.fromtimestamp(epoch, UTC).astimezone(ZoneInfo("Asia/Shanghai")).date()


def _official_detail(
    source: dict[str, Any],
    store: NewsSourceStore,
    article: FetchedArticle,
    *,
    content_selector: str,
    published_selector: str,
    published_attribute: str = "",
) -> tuple[str, str, str, float] | None:
    """Fetch and validate one official detail page without trusting list metadata alone."""
    try:
        content, final_url, raw_key = _fetch_bytes(source, article.url, store)
        if content is None:
            cached = store.cached_response(source["id"], final_url)
            if cached is None:
                return None
            content, raw_key = cached
        soup = BeautifulSoup(content, "html.parser")
        body_node = soup.select_one(content_selector)
        body = _clean_text(body_node.get_text(" ", strip=True) if body_node else "")
        if len(body) < OFFICIAL_DETAIL_MIN_CHARS:
            return None
        published_node = soup.select_one(published_selector)
        if published_node is None:
            return None
        published_value = (
            str(published_node.get(published_attribute) or "")
            if published_attribute
            else published_node.get_text(" ", strip=True)
        )
        _published, published_epoch = normalize_published_at(published_value)
        if not published_epoch or _local_date(published_epoch) != _local_date(
            article.published_at_epoch
        ):
            return None
        if not raw_key.startswith("news_raw/"):
            return None
        return body, final_url, raw_key, published_epoch
    except (
        CredentialError,
        NewsProviderError,
        httpx.HTTPError,
        sqlite3.Error,
        OSError,
        OverflowError,
        TypeError,
        ValueError,
    ):
        return None


def _official_html_provider(
    source: dict[str, Any], store: NewsSourceStore, watermark: str, limit: int,
    *,
    link_pattern: str,
    detail_content_selector: str,
    detail_published_selector: str,
    detail_published_attribute: str = "",
) -> FetchBatch:
    content, final_url, raw_key = _fetch_bytes(source, source["url"], store)
    if content is None:
        return _not_modified_batch(source, watermark)
    soup = BeautifulSoup(content, "html.parser")
    matcher = re.compile(link_pattern, re.I)
    fetched_at = time.time()
    parsed_articles: list[FetchedArticle] = []
    seen: set[str] = set()
    latest_published_at = 0.0
    for anchor in soup.select("a[href]"):
        href = str(anchor.get("href") or "").strip()
        url = urljoin(final_url, href)
        if not matcher.search(url) or url in seen:
            continue
        title = _clean_text(anchor.get("title") or anchor.get_text(" ", strip=True))
        if len(title) < 6:
            continue
        seen.add(url)
        provider_id = url
        node = anchor
        published = ""
        published_epoch = 0.0
        for _depth in range(4):
            node = node.parent
            if node is None:
                break
            context = _clean_text(node.get_text(" ", strip=True))
            try:
                published, published_epoch = _extract_date(context, url)
                break
            except NewsContractError as exc:
                if exc.code != "official_missing_time":
                    raise
        if not published_epoch:
            continue
        latest_published_at = max(latest_published_at, published_epoch)
        parsed_articles.append(FetchedArticle(
            source=source["id"], provider_item_id=provider_id,
            title=title[:240], content=title, url=url,
            published_at=published, published_at_epoch=published_epoch,
            fetched_at=fetched_at, is_official=True, raw_cache_key=raw_key,
            content_scope="listing_title_only",
        ))
    parsed_articles.sort(
        key=lambda item: (item.published_at_epoch, item.provider_item_id), reverse=True,
    )
    reached = not watermark
    candidates = parsed_articles
    if watermark:
        candidates = []
        for article in parsed_articles:
            if article.provider_item_id == watermark:
                reached = True
                break
            candidates.append(article)
    item_limit = max(1, min(limit, int(source.get("item_limit") or limit)))
    articles = candidates[:item_limit]
    window_complete = reached and len(candidates) <= item_limit
    if not articles and watermark and reached:
        return _unchanged_batch(source, watermark, [raw_key], latest_published_at)
    raw_keys = [raw_key]
    detail_failures = 0
    if articles:
        with ThreadPoolExecutor(max_workers=min(4, len(articles))) as executor:
            futures = {
                executor.submit(
                    _official_detail,
                    source,
                    store,
                    article,
                    content_selector=detail_content_selector,
                    published_selector=detail_published_selector,
                    published_attribute=detail_published_attribute,
                ): article
                for article in articles
            }
            for future in as_completed(futures):
                article = futures[future]
                detail = future.result()
                if detail is None:
                    detail_failures += 1
                    continue
                body, detail_url, detail_raw_key, detail_published_epoch = detail
                article.content = body
                article.url = detail_url
                article.raw_cache_key = detail_raw_key
                article.content_scope = "full_article"
                article.published_at_epoch = detail_published_epoch
                article.published_at = datetime.fromtimestamp(
                    detail_published_epoch, UTC,
                ).isoformat(timespec="seconds")
                raw_keys.append(detail_raw_key)
    complete = (window_complete or not watermark) and detail_failures == 0
    if detail_failures:
        incomplete_code = "official_detail_incomplete"
        incomplete_message = (
            f"{detail_failures} 条官方详情正文或发布时间未通过契约；"
            "保留 committed 水位并等待重试"
        )
    else:
        incomplete_code = "snapshot_window_exhausted"
        incomplete_message = (
            "官方列表快照在条目上限内未出现 committed 水位；"
            "该来源没有已验证的分页契约，保留旧水位并持续降级"
        )
    return _batch(
        source, articles, watermark, raw_keys, complete=complete,
        pending_watermark=parsed_articles[0].provider_item_id if parsed_articles else "",
        incomplete_code=incomplete_code,
        incomplete_message=incomplete_message,
    )


def fetch_sse(source: dict[str, Any], store: NewsSourceStore, watermark: str, limit: int) -> FetchBatch:
    return _official_html_provider(
        source, store, watermark, limit,
        link_pattern=r"sse\.com\.cn/disclosure/announcement/general/.+/c/c_20\d{6}_\d+\.shtml(?:$|[?#])",
        detail_content_selector=".allZoom",
        detail_published_selector=".article-infor i",
    )


def fetch_pboc(source: dict[str, Any], store: NewsSourceStore, watermark: str, limit: int) -> FetchBatch:
    return _official_html_provider(
        source, store, watermark, limit,
        link_pattern=r"pbc\.gov\.cn/goutongjiaoliu/113456/113469/\d+/index\.html(?:$|[?#])",
        detail_content_selector="#zoom",
        detail_published_selector="meta[name='PubDate']",
        detail_published_attribute="content",
    )


def fetch_ndrc(source: dict[str, Any], store: NewsSourceStore, watermark: str, limit: int) -> FetchBatch:
    return _official_html_provider(
        source, store, watermark, limit,
        link_pattern=r"ndrc\.gov\.cn/xwdt/xwfb/(?:20\d{4}/)?t?20\d+_\d+\.html(?:$|[?#])",
        detail_content_selector=".article_con .TRS_Editor",
        detail_published_selector="meta[name='PubDate']",
        detail_published_attribute="content",
    )


def fetch_nbs_rss(
    source: dict[str, Any], store: NewsSourceStore, watermark: str, limit: int,
) -> FetchBatch:
    content, final_url, raw_key = _fetch_bytes(source, source["url"], store)
    if content is None:
        return _not_modified_batch(source, watermark)
    configured = {**source, "item_limit": max(1, min(limit, int(source.get("item_limit") or limit)))}
    articles = _parse_rss(configured, content, final_url, raw_key)
    articles.sort(key=lambda item: (item.published_at_epoch, item.provider_item_id), reverse=True)
    latest_published_at = max((item.published_at_epoch for item in articles), default=0.0)
    if watermark:
        selected: list[FetchedArticle] = []
        reached = False
        for article in articles:
            if article.provider_item_id == watermark:
                reached = True
                break
            selected.append(article)
        articles = selected
    else:
        reached = True
    if not articles and watermark and reached:
        return _unchanged_batch(source, watermark, [raw_key], latest_published_at)
    return _batch(
        source, articles, watermark, [raw_key], complete=reached,
        pending_watermark=articles[0].provider_item_id if articles else "",
        incomplete_code="snapshot_window_exhausted",
        incomplete_message=(
            "官方 RSS 快照在条目上限内未出现 committed 水位；该来源没有游标契约，"
            "保留旧水位并持续降级"
        ),
    )


BUILTIN_PROVIDERS: dict[str, Provider] = {
    "sina_live": fetch_sina_live,
    "sse": fetch_sse,
    "pboc": fetch_pboc,
    "nbs_release": fetch_nbs_rss,
    "nbs_interpretation": fetch_nbs_rss,
    "ndrc": fetch_ndrc,
}


def fetch_builtin_source(
    source: dict[str, Any], store: NewsSourceStore, *, limit: int,
) -> FetchBatch:
    provider = BUILTIN_PROVIDERS.get(str(source.get("id") or ""))
    if provider is None:
        raise NewsProviderError("内置来源采集器不存在", code="missing_provider", retryable=False)
    state = store.state(source["id"])
    configured = {
        **source,
        "item_limit": max(1, min(limit, int(source.get("item_limit") or limit))),
        "_state_latest_published_at": float(state.get("latest_published_at") or 0.0),
        "_state_pending_watermark": str(state.get("pending_watermark") or ""),
        "_state_next_cursor": str(state.get("backfill_cursor") or ""),
        "_conditional_cache": not bool(state.get("pending_watermark")),
    }
    return provider(configured, store, str(state.get("watermark") or ""), limit)
