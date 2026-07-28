"""AI Quant Lab REST API。"""

from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from quantmaster.config import get_config
from quantmaster.lab.service import LabService

router = APIRouter(prefix="/api/lab", tags=["quant-lab"])
_service: LabService | None = None
_service_path = ""


def get_lab_service() -> LabService:
    global _service, _service_path
    expected = str((get_config().data_root / "lab.sqlite").resolve())
    if _service is None or expected != _service_path:
        _service = LabService()
        _service_path = expected
    return _service


def _fail(exc: Exception) -> HTTPException:
    if isinstance(exc, KeyError):
        return HTTPException(404, str(exc).strip("'"))
    if isinstance(exc, (ValueError, RuntimeError, FileNotFoundError)):
        return HTTPException(400, str(exc))
    return HTTPException(500, "Quant Lab 操作失败")


class FactorCreate(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    expression: str = Field(min_length=1, max_length=2000)
    description: str = Field(default="", max_length=2000)
    category: str = Field(default="人工研究", max_length=80)
    rationale: str = Field(default="", max_length=4000)
    parent_id: str = Field(default="", max_length=64)


class JobCreate(BaseModel):
    kind: Literal["prepare_data", "validate", "discover_genetic", "discover_llm", "train"]
    params: dict[str, Any] = Field(default_factory=dict)


class Decision(BaseModel):
    actor: str = Field(default="web", min_length=1, max_length=120)
    reason: str = Field(default="", max_length=4000)


class Deployment(BaseModel):
    universe: str = Field(default="csi800", min_length=1, max_length=40)
    horizon: Literal[1, 3, 5, 7] = 3
    profile: Literal["all", "risk_adjusted", "short_term", "stable"] = "all"
    scope: Literal["exact", "a_share"] = "exact"
    actor: str = Field(default="web", min_length=1, max_length=120)


class SuggestionRequest(BaseModel):
    use_cloud: bool = False
    sample_consent: bool = False
    anonymous_sample: dict[str, Any] | None = None


@router.get("/overview")
def overview() -> dict:
    return get_lab_service().overview()


@router.get("/capabilities")
def capabilities() -> dict:
    return get_lab_service().capabilities()


@router.get("/factors")
def factors(
    status: str | None = None,
    category: str | None = None,
    search: str = "",
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> dict:
    return get_lab_service().store.list_factors(
        status=status, category=category, search=search, limit=limit, offset=offset)


@router.get("/factors/{version_id}")
def factor_version(version_id: str) -> dict:
    value = get_lab_service().store.version(version_id)
    if value is None:
        raise HTTPException(404, "因子版本不存在")
    return value


@router.post("/factors")
def create_factor(body: FactorCreate) -> dict:
    try:
        return get_lab_service().create_expression(**body.model_dump())
    except Exception as exc:
        raise _fail(exc) from exc


@router.post("/factors/{version_id}/approve")
def approve(version_id: str, body: Decision) -> dict:
    try:
        return get_lab_service().store.approve(
            version_id, actor=body.actor, reason=body.reason)
    except Exception as exc:
        raise _fail(exc) from exc


@router.post("/factors/{version_id}/reject")
def reject(version_id: str, body: Decision) -> dict:
    try:
        return get_lab_service().store.reject(
            version_id, actor=body.actor, reason=body.reason)
    except Exception as exc:
        raise _fail(exc) from exc


@router.post("/factors/{version_id}/deploy")
def deploy(version_id: str, body: Deployment) -> dict:
    try:
        return get_lab_service().store.deploy(
            version_id, universe=body.universe, horizon=body.horizon, actor=body.actor,
            profile=body.profile, scope=body.scope)
    except Exception as exc:
        raise _fail(exc) from exc


@router.post("/factors/{version_id}/suggestions")
def suggest(version_id: str, body: SuggestionRequest) -> dict:
    try:
        return get_lab_service().suggest_revision(
            version_id, use_cloud=body.use_cloud,
            sample_consent=body.sample_consent, sample=body.anonymous_sample,
        )
    except Exception as exc:
        raise _fail(exc) from exc


@router.post("/suggestions/{suggestion_id}/apply")
def apply_suggestion(suggestion_id: str, body: Decision) -> dict:
    try:
        return get_lab_service().apply_suggestion(suggestion_id, actor=body.actor)
    except Exception as exc:
        raise _fail(exc) from exc


@router.post("/suggestions/{suggestion_id}/dismiss")
def dismiss_suggestion(suggestion_id: str) -> dict:
    try:
        return get_lab_service().store.resolve_suggestion(suggestion_id, "dismissed")
    except Exception as exc:
        raise _fail(exc) from exc


@router.post("/jobs", status_code=202)
def enqueue_job(body: JobCreate) -> dict:
    try:
        return get_lab_service().enqueue(body.kind, body.params)
    except Exception as exc:
        raise _fail(exc) from exc


@router.get("/jobs")
def jobs(limit: int = Query(50, ge=1, le=500)) -> dict:
    return {"items": get_lab_service().store.jobs(limit)}


@router.get("/jobs/{job_id}")
def job(job_id: str) -> dict:
    value = get_lab_service().store.job(job_id)
    if value is None:
        raise HTTPException(404, "任务不存在")
    return value


@router.post("/jobs/{job_id}/cancel")
def cancel_job(job_id: str) -> dict:
    try:
        return get_lab_service().store.request_cancel(job_id)
    except Exception as exc:
        raise _fail(exc) from exc


@router.post("/jobs/{job_id}/retry", status_code=202)
def retry_job(job_id: str) -> dict:
    try:
        return get_lab_service().store.retry_job(job_id)
    except Exception as exc:
        raise _fail(exc) from exc


@router.get("/jobs/{job_id}/events")
def job_events(
    job_id: str, after: int = Query(0, ge=0), limit: int = Query(500, ge=1, le=2000),
) -> dict:
    if get_lab_service().store.job(job_id) is None:
        raise HTTPException(404, "任务不存在")
    return {"items": get_lab_service().store.events(job_id, after, limit)}


@router.get("/experiments")
def experiments(limit: int = Query(50, ge=1, le=500)) -> dict:
    return {"items": get_lab_service().store.list_experiments(limit)}
