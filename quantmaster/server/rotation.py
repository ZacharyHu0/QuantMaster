"""Public v1 market-temperature and rotation APIs."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query

from quantmaster.rotation.contracts import (
    RotationJobSpec,
    RotationPreferencesUpdate,
    RotationRefreshRequest,
)
from quantmaster.rotation.service import get_rotation_service, get_rotation_worker

router = APIRouter(prefix="/api/v1", tags=["rotation"])


def _iso_time(value: Any) -> str:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), timezone.utc).isoformat()
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
) -> dict[str, Any]:
    service = get_rotation_service()
    snapshot = service.snapshot("themes")
    values = snapshot.get("data", {}).get("items") or []
    needle = query.strip().casefold()
    items = [
        item for item in values
        if not needle
        or needle in str(item.get("name") or "").casefold()
        or needle in str(item.get("code") or "").casefold()
    ]
    selected_limit = limit or int(service.store.preferences()["theme_limit"])
    data = {key: value for key, value in snapshot.get("data", {}).items() if key != "details"}
    data.update({"items": items[:selected_limit], "total": len(items), "limit": selected_limit})
    return {"meta": snapshot["meta"], "data": data}


@router.get("/rotation/themes/{code}")
def rotation_theme_detail(code: str) -> dict[str, Any]:
    result = get_rotation_service().detail("themes", code)
    if result is None:
        raise HTTPException(404, f"题材不存在或尚未达到覆盖门槛: {code}")
    return result


@router.get("/rotation/etf-flows")
def rotation_etf_flows() -> dict[str, Any]:
    return get_rotation_service().snapshot("etf_flows")


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
