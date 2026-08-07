"""Public v1 market-temperature and rotation APIs."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query

from quantmaster.rotation.contracts import (
    RotationJobSpec,
    RotationPreferencesUpdate,
    RotationRefreshRequest,
)
from quantmaster.rotation.service import get_rotation_service, get_rotation_worker

router = APIRouter(prefix="/api/v1", tags=["rotation"])


def _pagination(
    values: list[dict[str, Any]], page: int, page_size: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    total = len(values)
    pages = max(1, (total + page_size - 1) // page_size)
    current = min(max(1, page), pages)
    start = (current - 1) * page_size
    return values[start:start + page_size], {
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
) -> dict[str, Any]:
    service = get_rotation_service()
    snapshot = service.snapshot("industries")
    values = snapshot.get("data", {}).get("items") or []
    selected_l2 = set(service.store.preferences()["l2_codes"])
    needle = query.strip().casefold()
    items = [
        item for item in values
        if (
            str(item.get("level")) == "L1"
            or str(item.get("code") or "").upper() in selected_l2
        )
        if (level == "all" or str(item.get("level")) == level)
        and (
            not needle
            or needle in str(item.get("name") or "").casefold()
            or needle in str(item.get("code") or "").casefold()
        )
    ]
    data = {key: value for key, value in snapshot.get("data", {}).items() if key != "details"}
    data["items"] = items
    return {"meta": snapshot["meta"], "data": data}


@router.get("/rotation/industries/{code}")
def rotation_industry_detail(code: str) -> dict[str, Any]:
    result = get_rotation_service().detail("industries", code)
    if result is None:
        raise HTTPException(404, f"行业不存在或尚未达到覆盖门槛: {code}")
    return result


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
    window: Literal[1, 3, 5, 20] = 5,
) -> dict[str, Any]:
    service = get_rotation_service()
    snapshot = service.snapshot("themes")
    values = snapshot.get("data", {}).get("items") or []
    needle = query.strip().casefold()
    items = [
        item for item in values
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
    if page is None and page_size is None:
        selected_limit = limit or int(service.store.preferences()["theme_limit"])
        data.update({"items": items[:selected_limit], "total": len(items), "limit": selected_limit})
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
    data.update({"items": visible, "pagination": pagination})
    return {"meta": snapshot["meta"], "data": data}


@router.get("/rotation/themes/{code}")
def rotation_theme_detail(code: str) -> dict[str, Any]:
    result = get_rotation_service().detail("themes", code)
    if result is None:
        raise HTTPException(404, f"题材不存在或尚未达到覆盖门槛: {code}")
    return result


@router.get("/rotation/etf-flows/items")
def rotation_etf_flow_items(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=100),
    query: str = Query("", max_length=80),
    category: str = Query("", max_length=80),
    sort: Literal["flow", "daily", "streak", "name"] = "flow",
    order: Literal["asc", "desc"] = "desc",
    window: Literal[1, 3, 5, 20] = 5,
) -> dict[str, Any]:
    snapshot = get_rotation_service().snapshot("etf_flows")
    values = list(snapshot.get("data", {}).get("items") or [])
    needle = query.strip().casefold()
    items = [
        item for item in values
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
            item.get("flows", {}).get(str(window)) if sort == "flow"
            else item.get("flow") if sort == "daily"
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
