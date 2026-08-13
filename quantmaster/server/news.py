"""资讯工作台、来源配置与采集操作 API。"""

from __future__ import annotations

import hashlib
import logging
import sqlite3
from datetime import datetime
from typing import Any, Literal
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import ConfigDict, Field, SecretStr, field_validator

from quantmaster.ai.crawler import AICrawler, NewsStore
from quantmaster.ai.news_contracts import BUILTIN_SOURCES
from quantmaster.ai.news_jobs import get_news_jobs
from quantmaster.ai.news_sources import NewsSourceStore
from quantmaster.credentials import CredentialError
from quantmaster.runtime.contracts import ContractModel
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.runtime.problems import OperationProblem, make_problem
from quantmaster.server.management import _require_csrf, _require_local

router = APIRouter(prefix="/api/v1/news")
logger = logging.getLogger(__name__)


class StrictModel(ContractModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class ParserRules(StrictModel):
    headers: dict[str, str] = Field(default_factory=dict)
    encoding: str = Field(default="utf-8-sig", max_length=40)
    items_path: str = Field(default="", max_length=300)
    id_path: str = Field(default="", max_length=300)
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
    max_age_hours: float | None = Field(default=None, ge=1, le=8760)
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
    limit: int | None = Field(default=None, ge=1, le=1000)
    batch_size: int | None = Field(default=None, ge=1, le=50)
    mode: Literal["pending", "failed", "dead_letter"] = "pending"

    @field_validator("ids")
    @classmethod
    def validate_ids(cls, value: list[int] | None) -> list[int] | None:
        if value is None:
            return None
        selected = list(dict.fromkeys(value))
        if len(selected) > 1000:
            raise ValueError("一次最多处理 1000 个资讯 ID")
        if any(item <= 0 for item in selected):
            raise ValueError("资讯 ID 必须是正整数")
        return selected


def _error(exc: Exception) -> HTTPException:
    """Classify known local failures without flattening them to a misleading 400."""
    if isinstance(exc, KeyError):
        logger.info("资讯资源不存在", extra={"event": "news_request_rejected", "error_code": "not_found"})
        return HTTPException(404, "资讯资源不存在")
    if isinstance(exc, (ValueError, TypeError)):
        return HTTPException(422, "资讯请求参数无效")
    if isinstance(exc, CredentialError):
        logger.info(
            "资讯凭据操作未完成",
            extra={"event": "news_request_rejected", "error_code": "credential_error"},
        )
        return HTTPException(409, "凭据操作失败，请检查本机凭据设置")
    if isinstance(exc, sqlite3.OperationalError):
        text = str(exc).casefold()
        if "locked" in text or "busy" in text:
            return HTTPException(503, "资讯数据库暂时繁忙，请稍后重试", headers={"Retry-After": "2"})
    logger.exception(
        "资讯 API 未处理异常（%s）", type(exc).__name__,
        extra={"event": "news_request_failed", "error_code": "internal_error"},
    )
    return HTTPException(500, "资讯操作未能完成，详细原因已写入服务端日志")


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


def _local_snapshot(callable_):
    """Run a news page read without turning an absent/locked cache into writes.

    Read-only stores intentionally open SQLite with ``mode=ro`` and a 250 ms
    lock budget.  A cold install or a writer that owns the database is therefore
    a recoverable resource state, not a reason to initialise or migrate from a
    page request.
    """
    try:
        return callable_()
    except (FileNotFoundError, sqlite3.OperationalError) as exc:
        locked = isinstance(exc, sqlite3.OperationalError) and any(
            token in str(exc).casefold() for token in ("locked", "busy")
        )
        raise OperationProblem(
            503,
            make_problem(
                "snapshot_unavailable",
                severity="warning",
                source="资讯快照",
                title="资讯本地快照暂不可用",
                message="尚未发布可读取的资讯快照，或写入任务暂时占用数据库。",
                action="可稍后重试；如为首次启动，请在后台完成一次资讯采集。",
                blocking=False,
                can_continue=True,
                retryable=True,
                retry_after=2 if locked else None,
                resource="news",
            ),
        ) from exc


def _cold_builtin_sources() -> list[dict[str, Any]]:
    """Expose the immutable bundled source catalog on a first-run GET.

    This is configuration metadata, not a fabricated news snapshot.  It lets
    the page render a clear cold state without creating ``news.sqlite`` from a
    reader process; the first submitted crawl or explicit source edit remains
    responsible for initialising the writable store.
    """

    factor_source_ids = {
        "csrc", "sse", "szse", "pboc", "nbs_release", "nbs_interpretation", "ndrc",
    }
    return [
        {
            **dict(item),
            "enabled": bool(item.get("enabled", True)),
            "built_in": True,
            "needs_credentials": bool(item.get("needs_credentials", False)),
            "factor_eligible": str(item.get("id") or "") in factor_source_ids,
            "health": "cold",
            "watermark": "",
            "pending_watermark": "",
            "last_status": "",
            "last_success_at": "",
            "last_error": "",
            "auth_configured": False,
            "parser": {},
        }
        for item in BUILTIN_SOURCES
    ]


def _snapshot_etag(
    request: Request, response: Response, payload: dict[str, Any], parameters: dict[str, Any],
) -> dict[str, Any] | Response:
    """Set a stable conditional-read token for a published read model."""

    meta = payload.get("meta") if isinstance(payload, dict) else None
    snapshot_id = str(meta.get("snapshot_id") or "") if isinstance(meta, dict) else ""
    if not snapshot_id:
        return payload
    canonical = strict_json_dumps(parameters, sort_keys=True)
    etag = '"' + hashlib.sha256(
        f"{snapshot_id}\n{canonical}".encode(),
    ).hexdigest() + '"'
    response.headers["ETag"] = etag
    requested = str(request.headers.get("if-none-match") or "")
    if requested == "*" or etag in {value.strip() for value in requested.split(",")}:
        return Response(status_code=304, headers={"ETag": etag})
    return payload


@router.get("/sources")
def sources_list(request: Request) -> dict:
    _require_local(request)
    try:
        items = NewsSourceStore(read_only=True).list()
        cold = False
    except FileNotFoundError:
        items = _cold_builtin_sources()
        cold = True
    return {
        "items": items,
        "groups": ["fast", "official", "periodic"],
        "cold": cold,
        "stale_reasons": ["尚未建立本地资讯来源状态"] if cold else [],
    }


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


@router.post("/sources/{source_id}/run", status_code=202)
def source_run(source_id: str, request: Request, skip_llm: bool = False) -> dict:
    _require_csrf(request)
    try:
        if NewsSourceStore().get(source_id) is None:
            raise KeyError("资讯来源不存在")
        jobs = get_news_jobs()
        job, created = jobs.submit_source_run(source_id, skip_llm=skip_llm)
        public = jobs.public(job)
        public["created"] = bool(created)
        public["coalesced"] = bool(job.get("coalesced"))
        return public
    except Exception as exc:
        raise _error(exc) from exc


@router.get("/stats", response_model=None)
def news_stats(
    request: Request, response: Response, days: int = 30,
) -> dict | Response:
    _require_local(request)
    if not 1 <= days <= 3650:
        raise HTTPException(422, "统计时间范围必须在 1 至 3650 日之间")
    value = _local_snapshot(
        lambda: NewsStore(read_only=True).stats(days=days),
    )
    return _snapshot_etag(request, response, value, {"days": days})


@router.get("/event-focus", response_model=None)
def news_event_focus(
    request: Request, response: Response, days: int = 7,
) -> dict | Response:
    _require_local(request)
    if days not in {1, 3, 7, 30}:
        raise HTTPException(422, "事件聚焦窗口仅支持 1、3、7、30 日")
    value = _local_snapshot(lambda: NewsStore(read_only=True).event_focus(days))
    return _snapshot_etag(request, response, value, {"days": days})


@router.get("")
def news_query(
    request: Request, limit: int = 50, cursor: int | None = None, q: str = "",
    source: str = "", group: str = "", event_type: str = "", sentiment: str = "",
    scope: str = "", symbol: str = "", status: str = "", date_from: str = "",
    date_to: str = "", sort: Literal["recent", "importance"] = "recent",
) -> dict:
    _require_local(request)
    return _local_snapshot(lambda: NewsStore(read_only=True).query(
        limit=limit, cursor=cursor, q=q, source=source, group_name=group,
        event_type=event_type, sentiment=sentiment, scope=scope, symbol=symbol,
        status=status, date_from=_epoch(date_from), date_to=_epoch(date_to, end=True), sort=sort,
    ))


def _submit_crawl(value: CrawlRequest) -> dict[str, Any]:
    """Submit remote work; never run a crawler in an ASGI request."""

    jobs = get_news_jobs()
    job, created = jobs.submit(
        sources=value.sources,
        group=value.group,
        limit=value.limit,
        skip_llm=value.skip_llm,
    )
    public = jobs.public(job)
    public["created"] = bool(created)
    public["coalesced"] = bool(job.get("coalesced"))
    return public


@router.post("/crawl", status_code=202)
def news_crawl(request: Request, value: CrawlRequest | None = None,
               skip_llm: bool | None = None) -> dict:
    _require_csrf(request)
    try:
        value = value or CrawlRequest()
        if skip_llm is not None:
            value = value.model_copy(update={"skip_llm": skip_llm})
        return _submit_crawl(value)
    except Exception as exc:
        raise _error(exc) from exc


@router.post("/reanalyze", status_code=202)
def news_reanalyze(value: ReanalyzeRequest, request: Request) -> dict:
    _require_csrf(request)
    try:
        jobs = get_news_jobs()
        job, created = jobs.submit_reanalyze(value.model_dump(exclude_none=True))
        public = jobs.public(job)
        public["created"] = bool(created)
        public["coalesced"] = bool(job.get("coalesced"))
        return public
    except Exception as exc:
        raise _error(exc) from exc


@router.get("/{item_id}")
def news_detail(item_id: int, request: Request) -> dict:
    _require_local(request)
    item = _local_snapshot(lambda: NewsStore(read_only=True).detail(item_id))
    if item is None:
        raise HTTPException(404, "资讯不存在")
    return item
