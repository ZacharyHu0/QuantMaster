"""Public v1 market-temperature and rotation APIs."""

from __future__ import annotations

import csv
import io
from datetime import UTC, datetime
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response
from pydantic import Field

from quantmaster.rotation.contracts import (
    RotationJobSpec,
    RotationPreferencesUpdate,
    RotationRefreshRequest,
)
from quantmaster.rotation.service import get_rotation_service, get_rotation_worker
from quantmaster.runtime.contracts import ContractModel
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.server.security import require_csrf

router = APIRouter(prefix="/api/v1", tags=["rotation"])


class EtfScanBody(ContractModel):
    as_of: str = Field(default="", pattern=r"^$|^\d{4}-\d{2}-\d{2}$")


def _pagination(
    values: list[dict[str, Any]],
    page: int,
    page_size: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    total = len(values)
    pages = max(1, (total + page_size - 1) // page_size)
    current = min(max(1, page), pages)
    start = (current - 1) * page_size
    return values[start : start + page_size], {
        "page": current,
        "page_size": page_size,
        "total": total,
        "pages": pages,
        "has_previous": current > 1,
        "has_next": current < pages,
    }


def _number(value: Any, fallback: float = float("-inf")) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return fallback


def _page_size(value: int | None, default: int = 50) -> int:
    selected = default if value is None else value
    if selected not in {25, 50, 100}:
        raise HTTPException(422, "page_size 仅允许 25、50 或 100")
    return selected


def _rotation_window(value: int) -> int:
    if value not in {1, 3, 5, 20}:
        raise HTTPException(422, "轮动观察窗口仅支持 1、3、5、20 日")
    return value


def _materialize_group_score(item: dict[str, Any], window: int) -> dict[str, Any]:
    """Expose the selected window while keeping legacy score and grade fields."""
    value = dict(item)
    selected = dict((item.get("scores") or {}).get(str(window)) or {})
    if not selected:
        legacy = item.get("score") if isinstance(item.get("score"), dict) else {}
        selected = {
            "window": window,
            "score": legacy.get("score", item.get("rotation_score")),
            "grade": legacy.get("grade", item.get("grade") or ""),
            "available_weight": legacy.get("available_weight"),
            "minimum_weight": legacy.get("minimum_weight"),
            "items": list(legacy.get("items") or []),
        }
    selected["window"] = window
    value.pop("scores", None)
    value["score"] = selected
    value["rotation_score"] = selected.get("score")
    value["grade"] = str(selected.get("grade") or "")
    value["score_available_weight"] = selected.get("available_weight")
    return value


_THEME_FOCUS_CRITERIA = (
    ("rotation", "轮动改善"),
    ("excess", "相对收益为正"),
    ("breadth", "上涨宽度过半"),
    ("amount", "量能活跃"),
    ("grade", "周期结构 A/B"),
)


def _theme_focus_items(
    items: list[dict[str, Any]],
    window: int,
    *,
    limit: int = 4,
) -> list[dict[str, Any]]:
    """Rank auditable theme evidence without inventing a new composite score."""
    ranked: list[dict[str, Any]] = []
    for item in items:
        current = dict((item.get("signals") or {}).get(str(window)) or {})
        reasons = []
        checks = {
            "rotation": _number(current.get("rotation_change_pp"), 0.0) > 0,
            "excess": _number(current.get("excess_return"), 0.0) > 0,
            "breadth": _number(current.get("advance_ratio"), 0.0) >= 0.5,
            "amount": _number(current.get("amount_activity"), 0.0) > 0,
            "grade": str(item.get("grade") or "") in {"A", "B"},
        }
        for criterion, label in _THEME_FOCUS_CRITERIA:
            if checks[criterion]:
                reasons.append({"id": criterion, "label": label})
        ranked.append(
            {
                **item,
                "focus": {
                    "evidence_count": len(reasons),
                    "evidence_total": len(_THEME_FOCUS_CRITERIA),
                    "reasons": reasons,
                },
            }
        )

    def sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
        current = (item.get("signals") or {}).get(str(window)) or {}
        return (
            -int((item.get("focus") or {}).get("evidence_count") or 0),
            -_number(item.get("rotation_score")),
            -_number(current.get("rotation_change_pp")),
            -_number(current.get("excess_return")),
            -_number(item.get("coverage")),
            str(item.get("name") or "").casefold(),
            str(item.get("code") or ""),
        )

    return sorted(ranked, key=sort_key)[:limit]


def _iso_time(value: Any) -> str:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), UTC).isoformat()
    return str(value or "")


def public_rotation_job(value: dict[str, Any]) -> dict[str, Any]:
    job_id = str(value.get("id") or "")
    status = str(value.get("status") or "unknown")
    return {
        "domain": "rotation",
        "id": job_id,
        "status": status,
        "progress": max(0, min(100, int(value.get("progress") or 0))),
        "phase": str(value.get("phase") or ""),
        "detail": str(value.get("detail") or value.get("error") or "")[:1000],
        "attempt": max(1, int(value.get("attempt") or 1)),
        "cancel_requested": bool(value.get("cancel_requested")),
        "created_at": _iso_time(value.get("created_at")),
        "updated_at": _iso_time(value.get("updated_at") or value.get("created_at")),
        "can_cancel": status in {"queued", "running", "cancelling"},
        "can_retry": status in {"completed", "failed", "cancelled"},
        "result": value.get("result"),
        "links": {
            "self": f"/api/v1/jobs/rotation/{job_id}",
            "events": f"/api/v1/jobs/rotation/{job_id}/events",
            "cancel": f"/api/v1/jobs/rotation/{job_id}/cancel",
            "retry": f"/api/v1/jobs/rotation/{job_id}/retry",
        },
    }


def get_rotation_job(job_id: str) -> dict[str, Any]:
    value = get_rotation_service().jobs.get(job_id)
    if value is None:
        raise KeyError(job_id)
    return value


def list_rotation_jobs(limit: int) -> list[dict[str, Any]]:
    return get_rotation_service().jobs.list(limit)


def rotation_job_events(job_id: str, after: int, limit: int) -> list[dict[str, Any]]:
    return get_rotation_service().jobs.events(job_id, after, limit)


def cancel_rotation_job(job_id: str) -> dict[str, Any]:
    return get_rotation_service().jobs.cancel(job_id)


def retry_rotation_job(job_id: str) -> dict[str, Any]:
    value = get_rotation_service().jobs.retry(job_id)
    get_rotation_worker().start()
    return value


@router.get("/market/temperature")
def market_temperature() -> dict[str, Any]:
    return get_rotation_service().snapshot("temperature")


@router.get("/market/structure")
def market_structure() -> dict[str, Any]:
    return get_rotation_service().snapshot("structure")


@router.post("/market/analytics/refresh", status_code=202)
def refresh_market_analytics(value: RotationRefreshRequest) -> dict[str, Any]:
    if value.source == "auto":
        # A button click is an explicit operator recovery attempt.  Let the first
        # Tushare call enter the circuit's half-open probe instead of repeatedly
        # rebuilding a stale snapshot throughout the previous cooldown window.
        from quantmaster.data.resilience import PROVIDER_HEALTH

        PROVIDER_HEALTH.reset("tushare")
    worker = get_rotation_worker()
    worker.start()
    job = worker.submit(RotationJobSpec.model_validate(value.model_dump(mode="json")))
    return public_rotation_job(job)


@router.get("/rotation/overview")
def rotation_overview() -> dict[str, Any]:
    return get_rotation_service().overview()


@router.get("/rotation/industries")
def rotation_industries(
    level: Literal["all", "L1", "L2"] = "all",
    query: str = Query("", max_length=80),
    window: int = 5,
) -> dict[str, Any]:
    window = _rotation_window(window)
    service = get_rotation_service()
    snapshot = service.snapshot("industries")
    values = [_materialize_group_score(item, window) for item in snapshot.get("data", {}).get("items") or []]
    selected_l2 = set(service.store.preferences()["l2_codes"])
    needle = query.strip().casefold()
    items = [
        item
        for item in values
        if (str(item.get("level")) == "L1" or str(item.get("code") or "").upper() in selected_l2)
        if (level == "all" or str(item.get("level")) == level)
        and (
            not needle
            or needle in str(item.get("name") or "").casefold()
            or needle in str(item.get("code") or "").casefold()
        )
    ]
    data = {key: value for key, value in snapshot.get("data", {}).items() if key != "details"}
    data.update({"items": items, "window": window})
    return {"meta": snapshot["meta"], "data": data}


@router.get("/rotation/industries/{code}")
def rotation_industry_detail(code: str, window: int = 5) -> dict[str, Any]:
    window = _rotation_window(window)
    result = get_rotation_service().detail("industries", code)
    if result is None:
        raise HTTPException(404, f"行业不存在或尚未达到覆盖门槛: {code}")
    return {"meta": result["meta"], "data": _materialize_group_score(result["data"], window)}


@router.get("/rotation/themes")
def rotation_themes(
    query: str = Query("", max_length=80),
    limit: int | None = Query(None, ge=1, le=500),
    page: int | None = Query(None, ge=1),
    page_size: int | None = Query(None, ge=1, le=100),
    stage: str = Query("", max_length=80),
    grade: Literal["", "A", "B", "C", "D"] = "",
    sort: Literal["change", "score", "excess", "amount", "coverage", "name"] = "change",
    order: Literal["asc", "desc"] = "desc",
    window: int = 5,
) -> dict[str, Any]:
    window = _rotation_window(window)
    service = get_rotation_service()
    snapshot = service.snapshot("themes")
    values = [_materialize_group_score(item, window) for item in snapshot.get("data", {}).get("items") or []]
    needle = query.strip().casefold()
    items = [
        item
        for item in values
        if (
            not needle
            or needle in str(item.get("name") or "").casefold()
            or needle in str(item.get("code") or "").casefold()
            or needle in str((item.get("primary_industry") or {}).get("name") or "").casefold()
        )
        and (not stage or str(item.get("stage") or "") == stage)
        and (not grade or str(item.get("grade") or "") == grade)
    ]
    data = {key: value for key, value in snapshot.get("data", {}).items() if key != "details"}
    data.update(
        {
            "focus_items": _theme_focus_items(values, window),
            "focus_definition": {
                "criteria": [{"id": criterion, "label": label} for criterion, label in _THEME_FOCUS_CRITERIA],
                "limit": 4,
                "window": window,
            },
        }
    )
    if page is None and page_size is None:
        selected_limit = limit or int(service.store.preferences()["theme_limit"])
        data.update(
            {
                "items": items[:selected_limit],
                "total": len(items),
                "limit": selected_limit,
                "window": window,
            }
        )
        return {"meta": snapshot["meta"], "data": data}

    def sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
        signal = (item.get("signals") or {}).get(str(window), {})
        if sort == "name":
            return (str(item.get("name") or "").casefold(), str(item.get("code") or ""))
        fields = {
            "score": item.get("rotation_score"),
            "excess": signal.get("excess_return"),
            "amount": signal.get("amount_activity"),
            "coverage": item.get("coverage"),
            "change": signal.get("rotation_change_pp"),
        }
        return (_number(fields[sort]), str(item.get("name") or ""), str(item.get("code") or ""))

    reverse = order == "desc"
    items.sort(key=sort_key, reverse=reverse)
    visible, pagination = _pagination(items, page or 1, _page_size(page_size))
    data.update({"items": visible, "pagination": pagination, "window": window})
    return {"meta": snapshot["meta"], "data": data}


@router.get("/rotation/themes/{code}")
def rotation_theme_detail(code: str, window: int = 5) -> dict[str, Any]:
    window = _rotation_window(window)
    result = get_rotation_service().detail("themes", code)
    if result is None:
        raise HTTPException(404, f"题材不存在或尚未达到覆盖门槛: {code}")
    return {"meta": result["meta"], "data": _materialize_group_score(result["data"], window)}


@router.get("/rotation/etf-flows/items")
def rotation_etf_flow_items(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    query: str = Query("", max_length=80),
    category: str = Query("", max_length=80),
    sort: Literal["flow", "daily", "streak", "name"] = "flow",
    order: Literal["asc", "desc"] = "desc",
    window: int = 5,
) -> dict[str, Any]:
    window = _rotation_window(window)
    snapshot = get_rotation_service().snapshot("etf_flows")
    values = list(snapshot.get("data", {}).get("items") or [])
    needle = query.strip().casefold()
    items = [
        item
        for item in values
        if (
            not needle
            or needle in str(item.get("name") or "").casefold()
            or needle in str(item.get("symbol") or "").casefold()
            or needle in str(item.get("benchmark") or "").casefold()
        )
        and (not category or str(item.get("category") or "") == category)
    ]

    def sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
        if sort == "name":
            return (str(item.get("name") or "").casefold(), str(item.get("symbol") or ""))
        value = (
            item.get("flows", {}).get(str(window))
            if sort == "flow"
            else item.get("flow")
            if sort == "daily"
            else item.get("flow_streak_sessions")
        )
        return (_number(value), str(item.get("name") or ""), str(item.get("symbol") or ""))

    items.sort(key=sort_key, reverse=order == "desc")
    visible, pagination = _pagination(items, page, _page_size(page_size))
    return {
        "meta": snapshot["meta"],
        "data": {
            "items": visible,
            "pagination": pagination,
            "categories": sorted({str(item.get("category") or "未分类") for item in values}),
        },
    }


@router.get("/rotation/etfs")
def rotation_etfs(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    query: str = Query("", max_length=80),
    category: str = Query("", max_length=80),
    rankable: bool | None = None,
    sort: Literal["rank", "score", "amount", "return", "name"] = "rank",
    order: Literal["asc", "desc"] = "asc",
    snapshot_id: str = Query("", max_length=80),
) -> dict[str, Any]:
    from quantmaster.rotation.etf_research import get_etf_research_service

    store = get_etf_research_service().store
    snapshot = store.get(snapshot_id) if snapshot_id else store.latest()
    if snapshot is None:
        return {
            "meta": {"quality": {"status": "cold", "issues": ["尚未生成 ETF 研究快照"]}},
            "data": {"items": [], "categories": [], "pagination": _pagination([], 1, page_size)[1]},
        }
    needle = query.strip().casefold()
    items = [item.to_dict() for item in snapshot.items]
    items = [
        item
        for item in items
        if (
            (not needle or needle in item["symbol"].casefold() or needle in item["name"].casefold())
            and (not category or item["category"] == category)
            and (rankable is None or bool(item["rankable"]) is rankable)
        )
    ]

    def sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
        metric = item.get("metrics") or {}
        values = {
            "rank": item.get("category_rank"),
            "score": item.get("score"),
            "amount": metric.get("avg_amount_20d"),
            "return": metric.get("return_20d"),
            "name": item.get("name"),
        }
        if sort == "name":
            return (str(values[sort]).casefold(), item["symbol"])
        value = values[sort]
        # Missing rows always remain behind rankable observations.
        return (value is None, _number(value, 0), item["symbol"])

    if sort == "name":
        items.sort(key=sort_key, reverse=order == "desc")
    else:
        items.sort(
            key=lambda item: (
                sort_key(item)[0],
                -sort_key(item)[1] if order == "desc" else sort_key(item)[1],
                sort_key(item)[2],
            )
        )
    visible, pagination = _pagination(items, page, page_size)
    return {
        "meta": {
            "snapshot_id": snapshot.snapshot_id,
            "ingest_id": snapshot.ingest_id,
            "artifact_id": snapshot.artifact_id,
            "as_of": snapshot.as_of_date,
            "quality": snapshot.coverage,
            "staleness": snapshot.staleness,
            "score_version": snapshot.score_version,
        },
        "data": {
            "items": visible,
            "categories": list(snapshot.categories),
            "pagination": pagination,
            "provenance": snapshot.provenance,
        },
    }


@router.get("/rotation/etfs/snapshots")
def rotation_etf_snapshots(limit: int = Query(50, ge=1, le=500)) -> dict[str, Any]:
    from quantmaster.rotation.etf_research import get_etf_research_service

    return {"items": get_etf_research_service().store.history(limit)}


@router.get("/rotation/etfs/snapshots/{snapshot_id}/coverage")
def rotation_etf_snapshot_coverage(snapshot_id: str) -> dict[str, Any]:
    from quantmaster.rotation.etf_research import get_etf_research_service

    snapshot = get_etf_research_service().store.get(snapshot_id)
    if snapshot is None:
        raise HTTPException(404, "ETF 研究快照不存在")
    semantic_counts: dict[str, int] = {}
    minute_complete = 0
    minute_rows = 0
    excluded: dict[str, int] = {}
    for item in snapshot.items:
        semantic_counts[item.share_semantic_status] = (
            semantic_counts.get(
                item.share_semantic_status,
                0,
            )
            + 1
        )
        minute_complete += int(bool(item.minute_evidence.get("complete_session")))
        minute_rows += int(item.minute_evidence.get("rows") or 0)
        if item.excluded_reason:
            excluded[item.excluded_reason] = excluded.get(item.excluded_reason, 0) + 1
    return {
        "meta": {
            "snapshot_id": snapshot.snapshot_id,
            "ingest_id": snapshot.ingest_id,
            "as_of": snapshot.as_of_date,
            "staleness": snapshot.staleness,
        },
        "data": {
            "coverage": snapshot.coverage,
            "share_semantic_counts": semantic_counts,
            "minute_complete_symbols": minute_complete,
            "minute_rows": minute_rows,
            "excluded_reasons": excluded,
            "total_symbols": len(snapshot.items),
        },
    }


@router.get("/rotation/etfs/export/{snapshot_id}")
def rotation_etf_export(
    snapshot_id: str,
    format: Literal["json", "csv"] = "json",
) -> Response:
    from quantmaster.rotation.etf_research import get_etf_research_service

    snapshot = get_etf_research_service().store.get(snapshot_id)
    if snapshot is None:
        raise HTTPException(404, "ETF 研究快照不存在")
    if format == "json":
        return Response(
            strict_json_dumps(snapshot.to_dict(), indent=2),
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="{snapshot_id}.json"'},
        )
    output = io.StringIO(newline="")
    fields = [
        "category",
        "category_rank",
        "symbol",
        "name",
        "score",
        "as_of_date",
        "rankable",
        "excluded_reason",
        "return_5d",
        "return_20d",
        "return_60d",
        "avg_amount_20d",
        "drawdown_20d",
        "total_share",
        "shares_effective_date",
        "share_lag_sessions",
        "price_source",
        "share_source",
    ]
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()
    for item in snapshot.items:
        writer.writerow(
            {
                "category": item.category,
                "category_rank": item.category_rank,
                "symbol": item.symbol,
                "name": item.name,
                "score": item.score,
                "as_of_date": item.as_of_date,
                "rankable": item.rankable,
                "excluded_reason": item.excluded_reason,
                **{key: item.metrics.get(key) for key in fields if key in item.metrics},
                "shares_effective_date": item.shares_effective_date,
                "share_lag_sessions": item.share_lag_sessions,
                "price_source": item.provenance.get("price"),
                "share_source": item.provenance.get("shares"),
            }
        )
    return Response(
        output.getvalue().encode("utf-8-sig"),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{snapshot_id}.csv"'},
    )


@router.post("/rotation/etfs/scan", status_code=202)
def rotation_etf_scan(body: EtfScanBody, request: Request) -> dict[str, Any]:
    require_csrf(request)
    from quantmaster.rotation.etf_jobs import get_etf_research_jobs

    job, created = get_etf_research_jobs().submit(as_of=body.as_of)
    return {**get_etf_research_jobs().public(job), "created": created}


@router.get("/rotation/etfs/jobs/{job_id}")
def rotation_etf_job(job_id: str) -> dict[str, Any]:
    from quantmaster.rotation.etf_jobs import get_etf_research_jobs

    try:
        return get_etf_research_jobs().public(get_etf_research_jobs().get(job_id))
    except KeyError:
        raise HTTPException(404, "ETF 研究任务不存在") from None


@router.post("/rotation/etfs/jobs/{job_id}/cancel")
def rotation_etf_job_cancel(job_id: str, request: Request) -> dict[str, Any]:
    require_csrf(request)
    from quantmaster.rotation.etf_jobs import get_etf_research_jobs

    try:
        jobs = get_etf_research_jobs()
        return jobs.public(jobs.cancel(job_id))
    except KeyError:
        raise HTTPException(404, "ETF 研究任务不存在") from None


@router.post("/rotation/etfs/jobs/{job_id}/retry", status_code=202)
def rotation_etf_job_retry(job_id: str, request: Request) -> dict[str, Any]:
    require_csrf(request)
    from quantmaster.rotation.etf_jobs import get_etf_research_jobs

    try:
        jobs = get_etf_research_jobs()
        return jobs.public(jobs.retry(job_id))
    except KeyError:
        raise HTTPException(404, "ETF 研究任务不存在") from None
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(409, str(exc)) from None


@router.get("/rotation/etfs/{symbol}")
def rotation_etf_detail(symbol: str, snapshot_id: str = Query("", max_length=80)) -> dict[str, Any]:
    from quantmaster.rotation.etf_research import get_etf_research_service

    store = get_etf_research_service().store
    snapshot = store.get(snapshot_id) if snapshot_id else store.latest()
    if snapshot is None:
        raise HTTPException(404, "尚无 ETF 研究快照")
    canonical = symbol.upper()
    item = next((value for value in snapshot.items if value.symbol == canonical), None)
    if item is None:
        raise HTTPException(404, f"ETF 不在当前研究股票池: {canonical}")
    return {
        "meta": {
            "snapshot_id": snapshot.snapshot_id,
            "as_of": snapshot.as_of_date,
            "staleness": snapshot.staleness,
        },
        "data": item.to_dict(),
    }


@router.get("/rotation/etf-flows")
def rotation_etf_flows(include_items: bool = True) -> dict[str, Any]:
    snapshot = get_rotation_service().snapshot("etf_flows")
    if include_items:
        return snapshot
    data = dict(snapshot.get("data") or {})
    items = list(data.pop("items", []) or [])
    data["item_total"] = len(items)
    return {"meta": snapshot["meta"], "data": data}


@router.get("/rotation/taxonomy/industries")
def rotation_taxonomy() -> dict[str, Any]:
    return get_rotation_service().taxonomy()


@router.get("/rotation/preferences")
def rotation_preferences() -> dict[str, Any]:
    return {"data": get_rotation_service().store.preferences()}


@router.put("/rotation/preferences")
def update_rotation_preferences(value: RotationPreferencesUpdate) -> dict[str, Any]:
    service = get_rotation_service()
    taxonomy = service.taxonomy()["data"]
    known_l2 = {str(item["code"]) for item in taxonomy.get("l2") or []}
    unknown = [code for code in value.l2_codes if code.upper() not in known_l2]
    if unknown:
        raise HTTPException(422, f"不是当前申万二级行业代码: {', '.join(unknown[:5])}")
    saved = service.store.save_preferences(value.model_dump(mode="json"))
    return {"data": saved}
