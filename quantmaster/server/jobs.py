"""Unified public task ledger across persistent QuantMaster workers."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query

from quantmaster.analysis.stock_jobs import get_stock_analysis_jobs
from quantmaster.automation.runtime import get_runtime
from quantmaster.backtest.spec import BacktestSpec
from quantmaster.backtest.workbench import get_backtest_worker
from quantmaster.data.maintenance import data_refresh_manager
from quantmaster.lab.store import LabStore
from quantmaster.research.jobs import get_research_job_manager
from quantmaster.server.rotation import (
    cancel_rotation_job,
    get_rotation_job,
    list_rotation_jobs,
    retry_rotation_job,
    rotation_job_events,
)

router = APIRouter(prefix="/api/v1/jobs", tags=["jobs"])
JobDomain = Literal[
    "research", "data", "lab", "backtests", "rotation", "repairs", "automation",
    "after_close",
]
_DOMAINS: tuple[JobDomain, ...] = (
    "research", "data", "lab", "backtests", "repairs", "automation", "after_close",
    "rotation",
)
_ACTIVE = frozenset({"queued", "running", "cancelling", "paused", "interrupted"})
_RETRYABLE = frozenset({
    "failed", "cancelled", "interrupted", "completed", "completed_with_errors",
    "completed_with_warnings", "paused",
    "needs_confirmation",
})


def _iso_time(value: Any) -> str:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), UTC).isoformat()
    return str(value or "")


def _public_job(domain: JobDomain, value: dict[str, Any]) -> dict[str, Any]:
    status = str(value.get("status") or "unknown")
    job_id = str(value.get("id") or "")
    legacy_read_only = domain == "backtests" and bool(value.get("legacy_read_only"))
    return {
        "domain": domain,
        "id": job_id,
        "type": str(value.get("type") or f"{domain}.job"),
        "status": status,
        "created": bool(value.get("created")),
        "coalesced": bool(value.get("coalesced")),
        "input_fingerprint": str(value.get("input_fingerprint") or ""),
        "algorithm_version": str(value.get("algorithm_version") or ""),
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
        "can_retry": status in _RETRYABLE and not legacy_read_only,
        "links": {
            "self": f"/api/v1/jobs/{job_id}",
            "events": f"/api/v1/jobs/{job_id}/events",
            "cancel": f"/api/v1/jobs/{job_id}/cancel",
            "retry": f"/api/v1/jobs/{job_id}/retry",
        },
    }


def _get(domain: JobDomain, job_id: str) -> dict[str, Any]:
    if domain == "after_close":
        from quantmaster.after_close.jobs import get_after_close_jobs

        return get_after_close_jobs().get(job_id)
    if domain == "automation":
        automation_job = get_runtime().service.jobs.store.get(job_id)
        if not str(automation_job.get("type") or "").startswith("automation."):
            raise KeyError(job_id)
        return automation_job
    if domain == "repairs":
        from quantmaster.data.repair import get_data_repair_manager

        return get_data_repair_manager().get(job_id)
    if domain == "research":
        manager = get_research_job_manager()
        return manager.public(manager.get(job_id))
    if domain == "data":
        return data_refresh_manager.get(job_id)
    if domain == "rotation":
        return get_rotation_job(job_id)
    if domain == "lab":
        raw_value = LabStore().job(job_id)
    else:
        raw_value = get_backtest_worker().service.store.get(job_id)
    if raw_value is None:
        raise KeyError(job_id)
    return dict(raw_value)


def _list(domain: JobDomain, limit: int) -> list[dict[str, Any]]:
    if domain == "after_close":
        from quantmaster.after_close.jobs import get_after_close_jobs

        return get_after_close_jobs().list(limit)
    if domain == "automation":
        return [
            value for value in get_runtime().service.jobs.store.list(max(limit * 4, limit))
            if str(value.get("type") or "").startswith("automation.")
        ][:limit]
    if domain == "repairs":
        from quantmaster.data.repair import get_data_repair_manager

        return get_data_repair_manager().list(limit=limit)
    if domain == "research":
        manager = get_research_job_manager()
        return [manager.public(value) for value in manager.list(limit)]
    if domain == "data":
        return data_refresh_manager.list(limit)
    if domain == "rotation":
        return list_rotation_jobs(limit)
    if domain == "lab":
        return LabStore().jobs(limit)
    return get_backtest_worker().service.store.list(limit)


def _events(domain: JobDomain, job_id: str, after: int, limit: int) -> list[dict[str, Any]]:
    if domain == "after_close":
        from quantmaster.after_close.jobs import get_after_close_jobs

        _get(domain, job_id)
        return get_after_close_jobs().runtime.store.events(job_id, after, limit)
    if domain == "automation":
        _get(domain, job_id)
        return get_runtime().service.jobs.store.events(job_id, after, limit)
    if domain == "repairs":
        from quantmaster.data.repair import get_data_repair_manager

        return get_data_repair_manager().events(job_id, after)[:limit]
    _get(domain, job_id)
    if domain == "research":
        return get_research_job_manager().catalog.job_events(job_id, after, limit)
    if domain == "data":
        return data_refresh_manager.events(job_id, after, limit)
    if domain == "rotation":
        return rotation_job_events(job_id, after, limit)
    if domain == "lab":
        return LabStore().events(job_id, after, limit)
    return get_backtest_worker().service.store.events(job_id, after, limit)


def _cancel(domain: JobDomain, job_id: str) -> dict[str, Any]:
    if domain == "after_close":
        from quantmaster.after_close.jobs import get_after_close_jobs

        _get(domain, job_id)
        return get_after_close_jobs().runtime.store.cancel(job_id)
    if domain == "automation":
        _get(domain, job_id)
        return get_runtime().service.jobs.store.cancel(job_id)
    if domain == "repairs":
        from quantmaster.data.repair import get_data_repair_manager

        return get_data_repair_manager().cancel(job_id)
    if domain == "research":
        return get_research_job_manager().public(get_research_job_manager().cancel(job_id))
    if domain == "data":
        return data_refresh_manager.cancel(job_id)
    if domain == "rotation":
        return cancel_rotation_job(job_id)
    if domain == "lab":
        return LabStore().request_cancel(job_id)
    return get_backtest_worker().service.store.cancel(job_id)


def _retry(domain: JobDomain, job_id: str) -> dict[str, Any]:
    if domain == "after_close":
        from quantmaster.after_close.jobs import get_after_close_jobs

        _get(domain, job_id)
        return get_after_close_jobs().runtime.retry(job_id)
    if domain == "automation":
        _get(domain, job_id)
        return get_runtime().service.jobs.retry(job_id)
    if domain == "repairs":
        from quantmaster.data.repair import get_data_repair_manager

        return get_data_repair_manager().retry(job_id)
    if domain == "research":
        manager = get_research_job_manager()
        return manager.public(manager.resume(job_id))
    if domain == "data":
        return data_refresh_manager.resume(job_id)
    if domain == "rotation":
        return retry_rotation_job(job_id)
    if domain == "lab":
        return LabStore().retry_job(job_id)

    worker = get_backtest_worker()
    source = worker.service.store.get(job_id)
    if source is None:
        raise KeyError(job_id)
    if source.get("legacy_read_only"):
        raise ValueError("旧 Swing 回测仅供历史查看，不能重试")
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


def _find_domain_job(job_id: str) -> tuple[JobDomain, dict[str, Any]]:
    """Resolve a task ID once, without exposing domain-specific polling URLs.

    Domains still own their storage adapters during the migration, but callers
    have one durable task URL.  This also keeps new UI code from guessing
    whether an ETF, after-close or rotation job uses a separate ledger.
    """

    for domain in _DOMAINS:
        try:
            value = _get(domain, job_id)
        except KeyError:
            continue
        if value is not None:
            return domain, value
    raise KeyError(job_id)


def _stock_analysis_job(job_id: str) -> dict[str, Any] | None:
    try:
        return get_stock_analysis_jobs().public_job(job_id)
    except KeyError:
        return None


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
    if domain is None:
        items.extend(get_stock_analysis_jobs().list(limit))
    items.sort(key=lambda value: value["created_at"], reverse=True)
    return {"items": items[:limit], "domains": list(_DOMAINS)}


@router.get("/{job_id}/events")
def get_registered_job_events(
    job_id: str, after: int = Query(0, ge=0),
    limit: int = Query(500, ge=1, le=2000),
) -> dict[str, Any]:
    try:
        domain, _value = _find_domain_job(job_id)
        return {"domain": domain, "id": job_id, "items": _events(domain, job_id, after, limit)}
    except KeyError as exc:
        stock = _stock_analysis_job(job_id)
        if stock is None:
            raise _not_found(exc) from None
        return {
            "domain": str(stock.get("domain") or "market"),
            "id": job_id,
            "items": get_stock_analysis_jobs().events(job_id, after, limit),
        }


@router.post("/{job_id}/cancel")
def cancel_registered_job(job_id: str) -> dict[str, Any]:
    try:
        domain, _value = _find_domain_job(job_id)
        return _public_job(domain, _cancel(domain, job_id))
    except KeyError as exc:
        stock = _stock_analysis_job(job_id)
        if stock is None:
            raise _not_found(exc) from None
        return get_stock_analysis_jobs().cancel(job_id)
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(409, str(exc)) from None


@router.post("/{job_id}/retry", status_code=202)
def retry_registered_job(job_id: str) -> dict[str, Any]:
    try:
        domain, _value = _find_domain_job(job_id)
        return _public_job(domain, _retry(domain, job_id))
    except KeyError as exc:
        stock = _stock_analysis_job(job_id)
        if stock is None:
            raise _not_found(exc) from None
        return get_stock_analysis_jobs().retry(job_id)
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(409, str(exc)) from None


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


@router.get("/{job_id}")
def get_registered_job(job_id: str) -> dict[str, Any]:
    try:
        domain, value = _find_domain_job(job_id)
        return _public_job(domain, value)
    except KeyError as exc:
        stock = _stock_analysis_job(job_id)
        if stock is None:
            raise _not_found(exc) from None
        return stock
