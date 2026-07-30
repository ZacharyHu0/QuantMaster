"""Unified public task ledger across persistent QuantMaster workers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query

from quantmaster.backtest.spec import BacktestSpec
from quantmaster.backtest.workbench import get_backtest_worker
from quantmaster.data.maintenance import data_refresh_manager
from quantmaster.lab.store import LabStore
from quantmaster.research.jobs import get_research_job_manager

router = APIRouter(prefix="/api/v1/jobs", tags=["jobs"])
JobDomain = Literal["research", "data", "lab", "backtests"]
_DOMAINS: tuple[JobDomain, ...] = ("research", "data", "lab", "backtests")
_ACTIVE = frozenset({"queued", "running", "cancelling", "paused", "interrupted"})
_RETRYABLE = frozenset({
    "failed", "cancelled", "interrupted", "completed", "completed_with_errors",
    "completed_with_warnings", "paused",
})


def _iso_time(value: Any) -> str:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), timezone.utc).isoformat()
    return str(value or "")


def _public_job(domain: JobDomain, value: dict[str, Any]) -> dict[str, Any]:
    status = str(value.get("status") or "unknown")
    job_id = str(value.get("id") or "")
    return {
        "domain": domain,
        "id": job_id,
        "status": status,
        "progress": max(0, min(100, int(value.get("progress") or 0))),
        "phase": str(value.get("phase") or value.get("current_task") or ""),
        "detail": str(value.get("detail") or value.get("error") or "")[:1000],
        "attempt": max(1, int(value.get("attempt") or 1)),
        "cancel_requested": bool(value.get("cancel_requested")),
        "created_at": _iso_time(value.get("created_at")),
        "updated_at": _iso_time(
            value.get("updated_at") or value.get("heartbeat_at") or value.get("created_at")
        ),
        "can_cancel": status in _ACTIVE,
        "can_retry": status in _RETRYABLE,
        "links": {
            "self": f"/api/v1/jobs/{domain}/{job_id}",
            "events": f"/api/v1/jobs/{domain}/{job_id}/events",
            "cancel": f"/api/v1/jobs/{domain}/{job_id}/cancel",
            "retry": f"/api/v1/jobs/{domain}/{job_id}/retry",
        },
    }


def _get(domain: JobDomain, job_id: str) -> dict[str, Any]:
    if domain == "research":
        manager = get_research_job_manager()
        return manager.public(manager.get(job_id))
    if domain == "data":
        return data_refresh_manager.get(job_id)
    if domain == "lab":
        value = LabStore().job(job_id)
    else:
        value = get_backtest_worker().service.store.get(job_id)
    if value is None:
        raise KeyError(job_id)
    return value


def _list(domain: JobDomain, limit: int) -> list[dict[str, Any]]:
    if domain == "research":
        manager = get_research_job_manager()
        return [manager.public(value) for value in manager.list(limit)]
    if domain == "data":
        return data_refresh_manager.list(limit)
    if domain == "lab":
        return LabStore().jobs(limit)
    return get_backtest_worker().service.store.list(limit)


def _events(domain: JobDomain, job_id: str, after: int, limit: int) -> list[dict[str, Any]]:
    _get(domain, job_id)
    if domain == "research":
        return get_research_job_manager().catalog.job_events(job_id, after, limit)
    if domain == "data":
        return data_refresh_manager.events(job_id, after, limit)
    if domain == "lab":
        return LabStore().events(job_id, after, limit)
    return get_backtest_worker().service.store.events(job_id, after, limit)


def _cancel(domain: JobDomain, job_id: str) -> dict[str, Any]:
    if domain == "research":
        return get_research_job_manager().public(get_research_job_manager().cancel(job_id))
    if domain == "data":
        return data_refresh_manager.cancel(job_id)
    if domain == "lab":
        return LabStore().request_cancel(job_id)
    return get_backtest_worker().service.store.cancel(job_id)


def _retry(domain: JobDomain, job_id: str) -> dict[str, Any]:
    if domain == "research":
        manager = get_research_job_manager()
        return manager.public(manager.resume(job_id))
    if domain == "data":
        return data_refresh_manager.resume(job_id)
    if domain == "lab":
        return LabStore().retry_job(job_id)

    worker = get_backtest_worker()
    source = worker.service.store.get(job_id)
    if source is None:
        raise KeyError(job_id)
    if str(source.get("status")) not in _RETRYABLE:
        raise ValueError("当前回测不能重试")
    spec = BacktestSpec.model_validate(source["config"])
    created = worker.service.store.create(spec)
    worker.service.store.append_event(created["id"], {
        "type": "retry_of", "source_job_id": job_id,
    })
    worker.service.store.append_event(job_id, {
        "type": "retried_as", "job_id": created["id"],
    })
    worker.start()
    return created


def _not_found(exc: KeyError) -> HTTPException:
    return HTTPException(404, f"任务不存在: {exc.args[0] if exc.args else ''}")


@router.get("")
def list_jobs(
    domain: JobDomain | None = None,
    limit: int = Query(50, ge=1, le=200),
) -> dict[str, Any]:
    domains = (domain,) if domain else _DOMAINS
    items = [
        _public_job(current, value)
        for current in domains
        for value in _list(current, limit)
    ]
    items.sort(key=lambda value: value["created_at"], reverse=True)
    return {"items": items[:limit], "domains": list(_DOMAINS)}


@router.get("/{domain}/{job_id}")
def get_job(domain: JobDomain, job_id: str) -> dict[str, Any]:
    try:
        return _public_job(domain, _get(domain, job_id))
    except KeyError as exc:
        raise _not_found(exc) from None


@router.get("/{domain}/{job_id}/events")
def get_job_events(
    domain: JobDomain,
    job_id: str,
    after: int = Query(0, ge=0),
    limit: int = Query(500, ge=1, le=2000),
) -> dict[str, Any]:
    try:
        return {"domain": domain, "id": job_id, "items": _events(domain, job_id, after, limit)}
    except KeyError as exc:
        raise _not_found(exc) from None


@router.post("/{domain}/{job_id}/cancel")
def cancel_job(domain: JobDomain, job_id: str) -> dict[str, Any]:
    try:
        return _public_job(domain, _cancel(domain, job_id))
    except KeyError as exc:
        raise _not_found(exc) from None
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(409, str(exc)) from None


@router.post("/{domain}/{job_id}/retry", status_code=202)
def retry_job(domain: JobDomain, job_id: str) -> dict[str, Any]:
    try:
        return _public_job(domain, _retry(domain, job_id))
    except KeyError as exc:
        raise _not_found(exc) from None
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(409, str(exc)) from None
