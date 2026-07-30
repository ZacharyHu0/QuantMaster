"""资讯工作台、来源配置与采集操作 API。"""

from __future__ import annotations

import re
from collections.abc import Iterator
from datetime import datetime
from typing import Any, Literal
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import ConfigDict, Field, SecretStr

from quantmaster.ai.crawler import AICrawler, NewsStore
from quantmaster.ai.news_sources import NewsSourceStore
from quantmaster.credentials import CredentialError
from quantmaster.runtime.contracts import ContractModel
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.server.management import _require_csrf, _require_local

router = APIRouter(prefix="/api/news")


class StrictModel(ContractModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class ParserRules(StrictModel):
    headers: dict[str, str] = Field(default_factory=dict)
    encoding: str = Field(default="utf-8-sig", max_length=40)
    items_path: str = Field(default="", max_length=300)
    title_path: str = Field(default="", max_length=300)
    content_path: str = Field(default="", max_length=300)
    url_path: str = Field(default="", max_length=300)
    published_at_path: str = Field(default="", max_length=300)
    item_selector: str = Field(default="", max_length=500)
    title_selector: str = Field(default="", max_length=500)
    content_selector: str = Field(default="", max_length=500)
    url_selector: str = Field(default="", max_length=500)
    url_attribute: str = Field(default="href", max_length=100)
    published_at_selector: str = Field(default="", max_length=500)
    detail_content_selector: str = Field(default="", max_length=500)


class SourceValue(StrictModel):
    name: str = Field(..., min_length=1, max_length=80)
    kind: Literal["rss", "json", "html"]
    enabled: bool = True
    group_name: Literal["fast", "official", "periodic"] = "periodic"
    url: str = Field(..., min_length=8, max_length=2048)
    item_limit: int = Field(default=30, ge=1, le=100)
    factor_weight: float = Field(default=1.0, ge=0, le=3)
    is_official: bool = False
    parser: ParserRules = Field(default_factory=ParserRules)
    auth_type: Literal["none", "bearer", "header"] = "none"
    auth_header: str = Field(default="", max_length=64)


class SourceCreate(StrictModel):
    source: SourceValue
    token: SecretStr | None = None


class SourceUpdate(StrictModel):
    source: dict[str, Any]
    token_action: Literal["keep", "replace", "clear"] = "keep"
    token: SecretStr | None = None


class SourcePreview(StrictModel):
    source: SourceValue
    token: SecretStr | None = None


class CrawlRequest(StrictModel):
    sources: list[str] | None = None
    group: Literal["fast", "official", "periodic"] | None = None
    limit: int = Field(default=30, ge=1, le=100)
    skip_llm: bool = False


class ReanalyzeRequest(StrictModel):
    ids: list[int] | None = None
    limit: int = Field(default=100, ge=1, le=1000)
    batch_size: int = Field(default=5, ge=1, le=10)


def _error(exc: Exception) -> HTTPException:
    if isinstance(exc, KeyError):
        return HTTPException(404, str(exc).strip("'"))
    if isinstance(exc, CredentialError):
        return HTTPException(409, str(exc))
    message = re.sub(
        r"(?i)((?:api[_-]?key|token|authorization)\s*[=:]\s*)[^\s,;]+",
        r"\1***", str(exc),
    )
    message = re.sub(r"(?i)(bearer\s+)[^\s,;]+", r"\1***", message)
    return HTTPException(400, message[:500])


def _epoch(value: str | None, *, end: bool = False) -> float | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(400, "日期格式应为 YYYY-MM-DD") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
    if end and len(value) <= 10:
        parsed = parsed.replace(hour=23, minute=59, second=59)
    return parsed.timestamp()


@router.get("/sources")
def sources_list(request: Request) -> dict:
    _require_local(request)
    return {"items": NewsSourceStore().list(), "groups": ["fast", "official", "periodic"]}


@router.post("/sources")
def source_create(value: SourceCreate, request: Request) -> dict:
    _require_csrf(request)
    try:
        token = value.token.get_secret_value() if value.token else ""
        return NewsSourceStore().create(value.source.model_dump(), token=token)
    except Exception as exc:
        raise _error(exc) from exc


@router.post("/sources/preview")
def source_preview(value: SourcePreview, request: Request) -> dict:
    _require_csrf(request)
    try:
        token = value.token.get_secret_value() if value.token else ""
        return {"items": AICrawler().preview(value.source.model_dump(), token=token)}
    except Exception as exc:
        raise _error(exc) from exc


@router.put("/sources/{source_id}")
def source_update(source_id: str, value: SourceUpdate, request: Request) -> dict:
    _require_csrf(request)
    try:
        token = value.token.get_secret_value() if value.token else ""
        return NewsSourceStore().update(
            source_id, value.source, token_action=value.token_action, token=token,
        )
    except Exception as exc:
        raise _error(exc) from exc


@router.delete("/sources/{source_id}")
def source_delete(source_id: str, request: Request) -> dict:
    _require_csrf(request)
    try:
        NewsSourceStore().delete(source_id)
        return {"deleted": source_id}
    except Exception as exc:
        raise _error(exc) from exc


@router.post("/sources/{source_id}/test")
def source_test(source_id: str, request: Request) -> dict:
    _require_csrf(request)
    try:
        crawler = AICrawler()
        source = crawler.source_store.get(source_id)
        if source is None:
            raise KeyError("资讯来源不存在")
        items = crawler._fetch_source(source, limit=3, preview=True)
        return {"items": [
            {"title": item.title, "content": item.content[:500], "url": item.url,
             "published_at": item.published_at}
            for item in items
        ]}
    except Exception as exc:
        raise _error(exc) from exc


@router.post("/sources/{source_id}/run")
def source_run(source_id: str, request: Request, skip_llm: bool = False) -> dict:
    _require_csrf(request)
    try:
        if NewsSourceStore().get(source_id) is None:
            raise KeyError("资讯来源不存在")
        return AICrawler().run(sources=[source_id], skip_llm=skip_llm)
    except Exception as exc:
        raise _error(exc) from exc


@router.get("/stats")
def news_stats(request: Request, days: int = 30) -> dict:
    _require_local(request)
    return NewsStore().stats(days=max(1, min(days, 3650)))


@router.get("")
def news_query(
    request: Request, limit: int = 50, cursor: int | None = None, q: str = "",
    source: str = "", group: str = "", event_type: str = "", sentiment: str = "",
    scope: str = "", symbol: str = "", status: str = "", date_from: str = "",
    date_to: str = "", sort: Literal["recent", "importance"] = "recent",
) -> dict:
    _require_local(request)
    return NewsStore().query(
        limit=limit, cursor=cursor, q=q, source=source, group_name=group,
        event_type=event_type, sentiment=sentiment, scope=scope, symbol=symbol,
        status=status, date_from=_epoch(date_from), date_to=_epoch(date_to, end=True), sort=sort,
    )


@router.post("/crawl")
def news_crawl(request: Request, value: CrawlRequest | None = None,
               skip_llm: bool | None = None) -> dict:
    _require_csrf(request)
    try:
        value = value or CrawlRequest()
        return AICrawler().run(
            sources=value.sources, group=value.group, limit=value.limit,
            skip_llm=value.skip_llm if skip_llm is None else skip_llm,
        )
    except Exception as exc:
        raise _error(exc) from exc


@router.post("/reanalyze")
def news_reanalyze(value: ReanalyzeRequest, request: Request) -> dict:
    _require_csrf(request)
    try:
        crawler = AICrawler()
        if value.ids is None:
            return crawler.enrich_pending(limit=value.limit, batch_size=value.batch_size)
        reset = crawler.store.reset_analysis(value.ids)
        return {
            "reset": reset,
            **crawler.enrich_pending(
                ids=value.ids, limit=value.limit, batch_size=value.batch_size,
            ),
        }
    except Exception as exc:
        raise _error(exc) from exc


@router.post("/reanalyze/stream")
def news_reanalyze_stream(value: ReanalyzeRequest, request: Request) -> StreamingResponse:
    """逐批发送真实标注进度；每个 batch 事件都包含刚刚落库的资讯。"""
    _require_csrf(request)
    crawler = AICrawler()
    if value.ids is not None:
        crawler.store.reset_analysis(value.ids)

    def generate() -> Iterator[str]:
        try:
            for event in crawler.enrich_pending_events(
                ids=value.ids, limit=value.limit, batch_size=value.batch_size,
            ):
                yield strict_json_dumps(event) + "\n"
        except Exception as exc:
            error = _error(exc)
            yield strict_json_dumps({"type": "error", "message": error.detail}) + "\n"

    return StreamingResponse(
        generate(), media_type="application/x-ndjson",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/{item_id}")
def news_detail(item_id: int, request: Request) -> dict:
    _require_local(request)
    item = NewsStore().detail(item_id)
    if item is None:
        raise HTTPException(404, "资讯不存在")
    return item
