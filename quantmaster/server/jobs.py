"""Unified public task ledger across persistent QuantMaster workers."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query

from quantmaster.analysis.stock_jobs import get_stock_analysis_jobs
from quantmaster.automation.runtime import get_runtime
from quantmaster.backtest.spec import BacktestSpec
from quantmaster.backtest.workbench import BacktestStore, get_backtest_worker
from quantmaster.config import get_config
from quantmaster.data.maintenance import data_refresh_manager
from quantmaster.data.repair import DataRepairManager
from quantmaster.lab.jobs import LAB_JOB_TYPES, LabJobManager, get_lab_job_manager
from quantmaster.research.jobs import (
    get_research_job_manager,
    list_research_jobs,
    read_research_job,
    research_job_events,
)
from quantmaster.runtime.jobs import UnifiedJobStore
from quantmaster.runtime.problems import OperationProblem, make_problem
from quantmaster.server.rotation import (
    cancel_rotation_job,
    retry_rotation_job,
)

router = APIRouter(prefix="/api/v1/jobs", tags=["jobs"])
JobDomain = Literal[
    "research", "data", "lab", "backtests", "rotation", "repairs", "automation",
    "after_close", "news", "settings",
]
_DOMAINS: tuple[JobDomain, ...] = (
    "research", "data", "lab", "backtests", "repairs", "automation", "after_close",
    # Keep the long-standing rotation position stable for clients that render
    # the registry in its declared order.  News is a peer domain, not a new
    # terminal/special case in that public contract.
    "news", "settings", "rotation",
)
_ACTIVE = frozenset({"queued", "running", "cancelling", "paused", "interrupted"})
_RETRYABLE = frozenset({
    "failed", "cancelled", "interrupted", "completed", "completed_with_errors",
    "completed_with_warnings", "paused",
    "needs_confirmation",
})
_UNIFIED_DOMAIN_TYPES: dict[str, frozenset[str]] = {
    "after_close": frozenset({"after_close.scan"}),
    "news": frozenset({"news.crawl", "news.source_run", "news.reanalyze"}),
    "settings": frozenset({"settings.apply", "settings.diagnostic"}),
    "lab": LAB_JOB_TYPES | frozenset({"lab.cloud_suggestion"}),
    "rotation": frozenset({"rotation.refresh", "rotation.etf.scan"}),
}


def _data_worker_command(operation: str, job_id: str) -> dict[str, Any]:
    """Keep generic data task mutations inside runtime-worker's IPC actor."""

    from quantmaster.runtime.worker_ipc import (
        WorkerCommandError,
        WorkerCommandUnavailable,
        call_worker_command,
    )

    try:
        return call_worker_command(operation, {"job_id": str(job_id)})
    except WorkerCommandUnavailable as exc:
        raise OperationProblem(
            503,
            make_problem(
                "worker_unavailable",
                severity="warning",
                source="后台 runtime-worker",
                title="后台执行器不可用",
                message=str(exc),
                action="页面仍可读取本地快照；请重启 QuantMaster 后再操作数据任务。",
                blocking=True,
                can_continue=True,
            ),
        ) from exc
    except WorkerCommandError as exc:
        if exc.code == "job_not_found":
            raise KeyError(job_id) from exc
        raise ValueError(str(exc)) from exc


def _read_unified_store() -> UnifiedJobStore:
    """Open the shared job ledger without bootstrapping a worker or schema."""

    return UnifiedJobStore(get_config().data_root / "jobs.sqlite", read_only=True)


def _read_unified_job(
    job_id: str,
    *,
    types: frozenset[str] = frozenset(),
    prefix: str = "",
) -> dict[str, Any]:
    try:
        value = _read_unified_store().get(job_id)
    except (FileNotFoundError, sqlite3.Error) as exc:
        raise KeyError(job_id) from exc
    job_type = str(value.get("type") or "")
    if (types and job_type not in types) or (prefix and not job_type.startswith(prefix)):
        raise KeyError(job_id)
    return value


def _list_unified_jobs(
    limit: int,
    *,
    types: frozenset[str] = frozenset(),
    prefix: str = "",
) -> list[dict[str, Any]]:
    try:
        values = _read_unified_store().list(max(limit * 4, limit))
    except (FileNotFoundError, sqlite3.Error):
        return []
    return [
        value
        for value in values
        if (
            (not types or str(value.get("type") or "") in types)
            and (not prefix or str(value.get("type") or "").startswith(prefix))
        )
    ][:limit]


def _read_unified_events(job_id: str, after: int, limit: int) -> list[dict[str, Any]]:
    try:
        return _read_unified_store().events(job_id, after, limit)
    except (FileNotFoundError, sqlite3.Error) as exc:
        raise KeyError(job_id) from exc


def _read_unified_artifact(artifact_id: str) -> dict[str, Any] | None:
    if not artifact_id:
        return None
    try:
        return _read_unified_store().artifact(artifact_id)
    except (FileNotFoundError, KeyError, sqlite3.Error, RuntimeError, ValueError):
        return None


def _read_backtest_store() -> BacktestStore:
    return BacktestStore(read_only=True)


def _read_repair_manager() -> DataRepairManager:
    return DataRepairManager(read_only=True)


def _iso_time(value: Any) -> str:
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), UTC).isoformat()
    return str(value or "")


def _manual_retry_required(value: dict[str, Any], status: str) -> bool:
    """Expose revision-fenced recovery without leaking configuration data."""
    if status != "interrupted":
        return False
    try:
        if UnifiedJobStore._legacy_llm_without_revision(value):
            return True
        scope = str(value.get("llm_scope") or "")
        if not scope:
            return False
        from quantmaster.runtime.llm import get_llm_execution_coordinator

        return not get_llm_execution_coordinator().current(
            scope, str(value.get("llm_revision") or ""),
        )
    except (OSError, RuntimeError, sqlite3.Error):
        return True


def _artifact_payload(value: dict[str, Any]) -> dict[str, Any] | None:
    artifact = _read_unified_artifact(str(value.get("result_artifact_id") or ""))
    payload = artifact.get("payload") if isinstance(artifact, dict) else None
    return payload if isinstance(payload, dict) else None


def _apply_artifact_projection(
    public: dict[str, Any], domain: str, value: dict[str, Any],
) -> None:
    job_type = str(value.get("type") or "")
    if job_type == "rotation.etf.scan":
        tier = str((value.get("spec") or {}).get("tier") or "production")
        public.update({
            "tier": tier,
            "formal_eligible": tier == "production",
            "message": str(value.get("detail") or ""),
        })
        payload = _artifact_payload(value)
        if payload is not None:
            snapshot_id = str(payload.get("snapshot_id") or "")
            public["result"] = {
                "snapshot_id": snapshot_id,
                "preview_id": snapshot_id if tier == "sandbox" else "",
                "tier": tier,
                "formal_eligible": bool(payload.get("formal_eligible")),
                "artifact_id": str(value.get("result_artifact_id") or ""),
            }
    if domain in {"news", "settings"}:
        payload = _artifact_payload(value)
        if payload is not None:
            public["result"] = dict(payload)
    if domain == "lab" and job_type in _UNIFIED_DOMAIN_TYPES["lab"]:
        payload = _artifact_payload(value)
        if payload is not None:
            public["result"] = dict(payload)


def _apply_domain_projection(public: dict[str, Any], domain: str, value: dict[str, Any]) -> None:
    if domain == "data":
        public.update({
            key: value.get(key)
            for key in (
                "scope", "universe_name", "start_date", "end_date", "next_index",
                "total", "succeeded", "failed", "failures", "current_symbol",
            )
            if key in value
        })
    if domain == "lab":
        if str(value.get("type") or "") in LAB_JOB_TYPES:
            projected = LabJobManager._project(_read_unified_store(), value)
            public.update({
                key: projected.get(key)
                for key in (
                    "kind", "params", "result", "preflight", "error_info", "telemetry",
                    "dataset_id", "resource_class", "worker", "heartbeat_at", "started_at",
                    "finished_at", "error", "error_code", "checkpoint", "outcome",
                )
                if key in projected
            })
            return
        public.update({
            key: value.get(key)
            for key in (
                "kind", "params", "result", "preflight", "error_info", "telemetry",
                "dataset_id", "resource_class", "worker", "heartbeat_at", "started_at",
                "finished_at", "error", "error_code",
            )
            if key in value
        })


def _public_job(domain: str, value: dict[str, Any]) -> dict[str, Any]:
    status = str(value.get("status") or "unknown")
    job_id = str(value.get("id") or "")
    legacy_read_only = domain == "backtests" and bool(value.get("legacy_read_only"))
    public = {
        "domain": domain,
        "id": job_id,
        "type": str(value.get("type") or f"{domain}.job"),
        "status": status,
        "created": bool(value.get("created")),
        "coalesced": bool(value.get("coalesced")),
        "reused": bool(value.get("reused")),
        "outcome": str(value.get("outcome") or ""),
        "input_fingerprint": str(value.get("input_fingerprint") or ""),
        "algorithm_version": str(value.get("algorithm_version") or ""),
        "progress": max(0, min(100, int(value.get("progress") or 0))),
        "phase": str(value.get("phase") or value.get("current_task") or ""),
        "detail": str(value.get("detail") or value.get("error") or "")[:1000],
        "attempt": max(1, int(value.get("attempt") or 1)),
        "cancel_requested": bool(value.get("cancel_requested")),
        "manual_retry_required": _manual_retry_required(value, status),
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
    _apply_artifact_projection(public, domain, value)
    _apply_domain_projection(public, domain, value)
    return public


def _rotation_job(job_id: str) -> dict[str, Any]:
    """Resolve rotation work from the shared ledger without a Web worker."""

    return _read_unified_job(job_id, types=_UNIFIED_DOMAIN_TYPES["rotation"])


def _get_repairs(job_id: str) -> dict[str, Any]:
    try:
        return _read_repair_manager().get(job_id)
    except (FileNotFoundError, sqlite3.Error) as exc:
        raise KeyError(job_id) from exc


def _get_research(job_id: str) -> dict[str, Any]:
    try:
        return read_research_job(job_id)
    except (FileNotFoundError, sqlite3.Error) as exc:
        raise KeyError(job_id) from exc


def _get_lab(job_id: str) -> dict[str, Any]:
    return _read_unified_job(job_id, types=_UNIFIED_DOMAIN_TYPES["lab"])


def _get_backtest(job_id: str) -> dict[str, Any]:
    try:
        raw_value = _read_backtest_store().get(job_id)
    except (FileNotFoundError, sqlite3.Error) as exc:
        raise KeyError(job_id) from exc
    if raw_value is None:
        raise KeyError(job_id)
    return dict(raw_value)


def _get(domain: JobDomain, job_id: str) -> dict[str, Any]:
    unified = _UNIFIED_DOMAIN_TYPES.get(domain)
    if unified is not None and domain != "lab":
        return _read_unified_job(job_id, types=unified)
    if domain == "automation":
        return _read_unified_job(job_id, prefix="automation.")
    handlers = {
        "repairs": _get_repairs,
        "research": _get_research,
        "data": data_refresh_manager.get,
        "rotation": _rotation_job,
        "lab": _get_lab,
        "backtests": _get_backtest,
    }
    handler = handlers.get(domain)
    if handler is None:
        raise KeyError(job_id)
    return handler(job_id)


def _list_repairs(limit: int) -> list[dict[str, Any]]:
    try:
        return _read_repair_manager().list(limit=limit)
    except (FileNotFoundError, sqlite3.Error):
        return []


def _list_research(limit: int) -> list[dict[str, Any]]:
    try:
        return list_research_jobs(limit)
    except (FileNotFoundError, sqlite3.Error):
        return []


def _list_lab(limit: int) -> list[dict[str, Any]]:
    return _list_unified_jobs(limit, types=_UNIFIED_DOMAIN_TYPES["lab"])


def _list_backtests(limit: int) -> list[dict[str, Any]]:
    try:
        return _read_backtest_store().list(limit)
    except (FileNotFoundError, sqlite3.Error):
        return []


def _list(domain: JobDomain, limit: int) -> list[dict[str, Any]]:
    unified = _UNIFIED_DOMAIN_TYPES.get(domain)
    if unified is not None and domain != "lab":
        return _list_unified_jobs(limit, types=unified)
    if domain == "automation":
        return _list_unified_jobs(limit, prefix="automation.")
    handlers = {
        "repairs": _list_repairs,
        "research": _list_research,
        "data": data_refresh_manager.list,
        "rotation": lambda size: _list_unified_jobs(
            size, types=_UNIFIED_DOMAIN_TYPES["rotation"],
        ),
        "lab": _list_lab,
        "backtests": _list_backtests,
    }
    return handlers.get(domain, lambda _size: [])(limit)


def _events_repairs(job_id: str, after: int, limit: int) -> list[dict[str, Any]]:
    try:
        _get("repairs", job_id)
        return _read_repair_manager().events(job_id, after)[:limit]
    except (FileNotFoundError, sqlite3.Error) as exc:
        raise KeyError(job_id) from exc


def _events_research(job_id: str, after: int, limit: int) -> list[dict[str, Any]]:
    try:
        _get("research", job_id)
        return research_job_events(job_id, after, limit)
    except (FileNotFoundError, sqlite3.Error) as exc:
        raise KeyError(job_id) from exc


def _events_lab(job_id: str, after: int, limit: int) -> list[dict[str, Any]]:
    _get("lab", job_id)
    return _read_unified_events(job_id, after, limit)


def _events_backtests(job_id: str, after: int, limit: int) -> list[dict[str, Any]]:
    try:
        _get("backtests", job_id)
        return _read_backtest_store().events(job_id, after, limit)
    except (FileNotFoundError, sqlite3.Error) as exc:
        raise KeyError(job_id) from exc


def _events(domain: JobDomain, job_id: str, after: int, limit: int) -> list[dict[str, Any]]:
    if (domain in _UNIFIED_DOMAIN_TYPES and domain != "lab") or domain == "automation":
        _get(domain, job_id)
        return _read_unified_events(job_id, after, limit)
    handlers = {
        "repairs": _events_repairs,
        "research": _events_research,
        "data": data_refresh_manager.events,
        "rotation": lambda task_id, offset, size: (
            _rotation_job(task_id), _read_unified_events(task_id, offset, size),
        )[1],
        "lab": _events_lab,
        "backtests": _events_backtests,
    }
    handler = handlers.get(domain)
    if handler is None:
        raise KeyError(job_id)
    return handler(job_id, after, limit)


def _cancel_news(job_id: str) -> dict[str, Any]:
    from quantmaster.ai.news_jobs import get_news_jobs

    _get("news", job_id)
    return get_news_jobs().runtime.store.cancel(job_id)


def _cancel_settings(job_id: str) -> dict[str, Any]:
    from quantmaster.server.settings_jobs import get_settings_jobs

    jobs = get_settings_jobs()
    _get("settings", job_id)
    value = jobs.runtime_for(job_id).store.cancel(job_id)
    jobs.cleanup_cancelled_credentials()
    return value


def _cancel_after_close(job_id: str) -> dict[str, Any]:
    from quantmaster.after_close.jobs import get_after_close_jobs

    _get("after_close", job_id)
    return get_after_close_jobs().runtime.store.cancel(job_id)


def _cancel_automation(job_id: str) -> dict[str, Any]:
    _get("automation", job_id)
    return get_runtime().service.jobs.store.cancel(job_id)


def _cancel_repairs(job_id: str) -> dict[str, Any]:
    from quantmaster.data.repair import get_data_repair_manager

    return get_data_repair_manager().cancel(job_id)


def _cancel_research(job_id: str) -> dict[str, Any]:
    manager = get_research_job_manager()
    return manager.public(manager.cancel(job_id))


def _cancel_rotation(job_id: str) -> dict[str, Any]:
    value = _rotation_job(job_id)
    if str(value.get("type") or "") == "rotation.etf.scan":
        from quantmaster.rotation.etf_jobs import get_etf_research_jobs

        return get_etf_research_jobs().cancel(job_id)
    return cancel_rotation_job(job_id)


def _cancel_lab(job_id: str) -> dict[str, Any]:
    value = _get("lab", job_id)
    if str(value.get("type") or "") == "lab.cloud_suggestion":
        from quantmaster.lab.llm_jobs import get_lab_llm_jobs

        return get_lab_llm_jobs().runtime.store.cancel(job_id)
    get_lab_job_manager().cancel(job_id)
    return _read_unified_job(job_id, types=LAB_JOB_TYPES)


def _cancel(domain: JobDomain, job_id: str) -> dict[str, Any]:
    handlers = {
        "news": _cancel_news,
        "settings": _cancel_settings,
        "after_close": _cancel_after_close,
        "automation": _cancel_automation,
        "repairs": _cancel_repairs,
        "research": _cancel_research,
        "data": lambda task_id: _data_worker_command("data.refresh.cancel", task_id),
        "rotation": _cancel_rotation,
        "lab": _cancel_lab,
        "backtests": lambda task_id: get_backtest_worker().service.store.cancel(task_id),
    }
    handler = handlers.get(domain)
    if handler is None:
        raise KeyError(job_id)
    return handler(job_id)


def _retry_news(job_id: str) -> dict[str, Any]:
    from quantmaster.ai.news_jobs import get_news_jobs

    _get("news", job_id)
    return get_news_jobs().runtime.retry(job_id)


def _retry_settings(job_id: str) -> dict[str, Any]:
    from quantmaster.server.settings_jobs import get_settings_jobs

    jobs = get_settings_jobs()
    _get("settings", job_id)
    return jobs.runtime_for(job_id).retry(job_id)


def _retry_after_close(job_id: str) -> dict[str, Any]:
    from quantmaster.after_close.jobs import get_after_close_jobs

    _get("after_close", job_id)
    return get_after_close_jobs().runtime.retry(job_id)


def _retry_automation(job_id: str) -> dict[str, Any]:
    _get("automation", job_id)
    return get_runtime().service.jobs.retry(job_id)


def _retry_repairs(job_id: str) -> dict[str, Any]:
    from quantmaster.data.repair import get_data_repair_manager

    return get_data_repair_manager().retry(job_id)


def _retry_research(job_id: str) -> dict[str, Any]:
    manager = get_research_job_manager()
    return manager.public(manager.resume(job_id))


def _retry_rotation(job_id: str) -> dict[str, Any]:
    value = _rotation_job(job_id)
    if str(value.get("type") or "") == "rotation.etf.scan":
        from quantmaster.rotation.etf_jobs import get_etf_research_jobs

        return get_etf_research_jobs().retry(job_id)
    return retry_rotation_job(job_id)


def _retry_lab(job_id: str) -> dict[str, Any]:
    value = _get("lab", job_id)
    if str(value.get("type") or "") == "lab.cloud_suggestion":
        from quantmaster.lab.llm_jobs import get_lab_llm_jobs

        return get_lab_llm_jobs().runtime.retry(job_id)
    get_lab_job_manager().retry(job_id)
    return _read_unified_job(job_id, types=LAB_JOB_TYPES)


def _retry_backtest(job_id: str) -> dict[str, Any]:
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


def _retry(domain: JobDomain, job_id: str) -> dict[str, Any]:
    handlers = {
        "news": _retry_news,
        "settings": _retry_settings,
        "after_close": _retry_after_close,
        "automation": _retry_automation,
        "repairs": _retry_repairs,
        "research": _retry_research,
        "data": lambda task_id: _data_worker_command("data.refresh.retry", task_id),
        "rotation": _retry_rotation,
        "lab": _retry_lab,
        "backtests": _retry_backtest,
    }
    handler = handlers.get(domain)
    if handler is None:
        raise KeyError(job_id)
    return handler(job_id)


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
        value = _read_unified_job(job_id, types=frozenset({"market.stock_analysis"}))
        return _public_job("market", value)
    except (KeyError, FileNotFoundError, sqlite3.Error):
        return None


def _stock_analysis_jobs(limit: int) -> list[dict[str, Any]]:
    return [
        _public_job("market", value)
        for value in _list_unified_jobs(limit, types=frozenset({"market.stock_analysis"}))
    ]


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
        items.extend(_stock_analysis_jobs(limit))
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
            "items": _read_unified_events(job_id, after, limit),
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
