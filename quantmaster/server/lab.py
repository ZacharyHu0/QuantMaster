"""AI Quant Lab REST API。"""

from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query, Response
from pydantic import Field

from quantmaster.config import get_config
from quantmaster.lab.errors import LabError, classify_lab_error
from quantmaster.lab.horizons import SUPPORTED_HORIZONS
from quantmaster.lab.service import LabService as _LabService
from quantmaster.lab.service import get_lab_service as _get_lab_service
from quantmaster.runtime.contracts import ContractModel
from quantmaster.runtime.json import StrictJSONResponse as JSONResponse
from quantmaster.runtime.problems import OperationProblem, make_problem

router = APIRouter(prefix="/api/v1/lab", tags=["quant-lab"])


def _published_lab_service() -> _LabService:
    """Return the immutable Lab ledger view or an explicit cold-start error."""

    service = _get_lab_service(read_only=True)
    if service.store.path.is_file():
        return service
    raise OperationProblem(
        503,
        make_problem(
            "snapshot_unavailable",
            severity="warning",
            source="Quant Lab 快照",
            title="Quant Lab 尚无已发布快照",
            message="后台 worker 尚未完成 Lab 账本初始化或没有可展示的本地快照。",
            action="可先继续浏览其他页面；启动 runtime-worker 或提交显式数据准备任务后重试。",
            blocking=True,
            can_continue=True,
        ),
    )


def _fail(exc: Exception) -> JSONResponse:
    if isinstance(exc, KeyError):
        error = LabError("NOT_FOUND", str(exc).strip("'"), status_code=404)
    elif isinstance(exc, ValueError):
        error = LabError("INVALID_REQUEST", str(exc), status_code=400)
    else:
        error = classify_lab_error(exc)
    return JSONResponse(
        status_code=error.status_code,
        content={
            "detail": error.message,
            "error": error.to_dict(),
            "problem": {
                "id": f"lab:{error.code.lower()}", "code": error.code,
                "severity": "warning" if error.status_code == 409 else "error",
                "source": "Quant Lab", "title": "任务未能运行",
                "message": error.message, "action": error.action,
                "blocking": True, "can_continue": False,
            },
        },
    )


class FactorCreate(ContractModel):
    name: str = Field(min_length=1, max_length=120)
    expression: str = Field(min_length=1, max_length=2000)
    description: str = Field(default="", max_length=2000)
    category: str = Field(default="人工研究", max_length=80)
    rationale: str = Field(default="", max_length=4000)
    parent_id: str = Field(default="", max_length=64)


class FactorCorrelationRequest(ContractModel):
    version_ids: list[str] = Field(min_length=2, max_length=30)
    universe: str = Field(default="csi800", min_length=1, max_length=40)
    start: str = Field(default="2015-01-01", pattern=r"^\d{4}-\d{2}-\d{2}$")
    end: str = Field(default="", pattern=r"^(?:\d{4}-\d{2}-\d{2})?$")
    horizon: Literal[1, 3, 5, 7, 10, 20, 30] = 3


class JobCreate(ContractModel):
    kind: Literal[
        "prepare_data", "validate", "discover_genetic", "discover_llm",
        "optimize", "bias_audit", "discover_python", "research_cycle", "shadow_score",
    ]
    params: dict[str, Any] = Field(default_factory=dict)


class PreflightCreate(ContractModel):
    operation: Literal[
        "prepare_data", "validate", "discover_genetic", "discover_llm",
        "optimize", "bias_audit", "discover_python", "research_cycle", "shadow_score",
        "approve", "deploy",
    ]
    params: dict[str, Any] = Field(default_factory=dict)


class StudyCreate(ContractModel):
    universe: str = Field(default="csi800", min_length=1, max_length=40)
    start: str = "2015-01-01"
    end: str = ""
    models: list[Literal["multi-transformer", "multi-tcn", "multi-gru", "ridge"]] = Field(
        default_factory=lambda: ["multi-transformer", "multi-tcn", "multi-gru", "ridge"],
    )
    budget_hours: float = Field(default=10.0, gt=0, le=10)
    max_trials: int = Field(default=40, ge=1, le=500)
    top_n: int = Field(default=20, ge=1, le=200)
    sequence_length: int = Field(default=20, ge=1, le=240)
    research_tier: Literal["production", "sandbox"] = "production"
    protocol: dict[str, Any] = Field(default_factory=dict)
    features: dict[str, Any] = Field(default_factory=dict)


class AuditCreate(ContractModel):
    version_id: str = Field(min_length=1, max_length=64)
    universe: str = Field(default="csi800", min_length=1, max_length=40)
    start: str = "2015-01-01"
    end: str


class MiningPreview(ContractModel):
    start: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    end: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    horizon: Literal[1, 3, 5, 7, 10, 20, 30] = 3


class Decision(ContractModel):
    actor: str = Field(default="web", min_length=1, max_length=120)
    reason: str = Field(default="", max_length=4000)


class Deployment(ContractModel):
    universe: str = Field(default="csi800", min_length=1, max_length=40)
    horizon: Literal[1, 3, 5, 7, 10, 20, 30] = 3
    profile: Literal["all", "risk_adjusted", "short_term", "stable"] = "all"
    scope: Literal["exact", "a_share"] = "exact"
    actor: str = Field(default="web", min_length=1, max_length=120)


class SuggestionRequest(ContractModel):
    use_cloud: bool = False
    outbound_confirmed: bool = False
    sample_consent: bool = False
    anonymous_sample: dict[str, Any] | None = None


class StrategyPromotion(ContractModel):
    target: Literal["paper", "champion", "degraded", "retired"]
    actor: str = Field(default="web", min_length=1, max_length=120)
    reason: str = Field(min_length=1, max_length=4000)


@router.get("/overview")
def overview() -> dict:
    return _published_lab_service().overview()


@router.get("/dashboard")
def dashboard() -> dict:
    from quantmaster.runtime.worker import runtime_worker_status

    # Do not import/construct the Lab worker in a disposable Web generation.
    # Its supervisor publishes a tiny heartbeat instead.
    return {**_published_lab_service().dashboard(), "worker": runtime_worker_status()}


@router.get("/workbench")
def workbench(
    horizon: int | None = Query(default=None),
) -> dict:
    if horizon is not None and horizon not in SUPPORTED_HORIZONS:
        raise HTTPException(422, detail="horizon 只支持 1/3/5/7/10/20/30")
    return _published_lab_service().workbench(horizon)


@router.get("/strategies")
def strategies(
    horizon: int | None = Query(default=None),
    status: str = "",
    limit: int = Query(100, ge=1, le=500),
) -> dict:
    if horizon is not None and horizon not in SUPPORTED_HORIZONS:
        raise HTTPException(422, detail="horizon 只支持 1/3/5/7/10/20/30")
    return {"items": _published_lab_service().store.strategies(
        horizon=horizon, status=status, limit=limit,
    )}


@router.get("/strategies/{strategy_id}")
def strategy(strategy_id: str) -> dict:
    value = _published_lab_service().store.strategy(strategy_id)
    if value is None:
        raise HTTPException(404, "策略候选不存在")
    return value


@router.get("/strategies/{strategy_id}/return-curve")
def strategy_return_curve(strategy_id: str) -> dict:
    try:
        return _published_lab_service().store.strategy_return_curve(strategy_id)
    except (LabError, KeyError, TypeError, ValueError) as exc:
        return _fail(exc)  # type: ignore[return-value]


@router.post("/strategies/{strategy_id}/promotions")
def promote_strategy(strategy_id: str, body: StrategyPromotion) -> dict:
    try:
        return _get_lab_service().store.promote_strategy(
            strategy_id, target=body.target, actor=body.actor, reason=body.reason,
        )
    except (LabError, KeyError, TypeError, ValueError) as exc:
        return _fail(exc)  # type: ignore[return-value]


@router.post("/preflight")
def preflight(body: PreflightCreate) -> dict:
    return _get_lab_service().preflight(body.operation, body.params)


@router.get("/capabilities")
def capabilities() -> dict:
    return _published_lab_service().capabilities()


@router.get("/factors")
def factors(
    status: str | None = None,
    category: str | None = None,
    search: str = "",
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> dict:
    return _published_lab_service().store.list_factors(
        status=status, category=category, search=search, limit=limit, offset=offset)


@router.post("/factors/correlation-matrix")
def factor_correlation_matrix(body: FactorCorrelationRequest) -> dict:
    try:
        return _get_lab_service().factor_correlation_matrix(**body.model_dump())
    except (LabError, KeyError, TypeError, ValueError) as exc:
        return _fail(exc)  # type: ignore[return-value]


@router.get("/factors/{version_id}/history")
def factor_version_history(version_id: str) -> dict:
    try:
        return {"items": _published_lab_service().store.version_history(version_id)}
    except (LabError, KeyError, TypeError, ValueError) as exc:
        return _fail(exc)  # type: ignore[return-value]


@router.get("/factors/{version_id}/robustness")
def factor_robustness(
    version_id: str,
    horizon: int = Query(...),
) -> dict:
    if horizon not in SUPPORTED_HORIZONS:
        raise HTTPException(422, detail="horizon 只支持 1/3/5/7/10/20/30")
    try:
        return _published_lab_service().robustness_evidence(version_id, horizon)
    except (LabError, KeyError, TypeError, ValueError) as exc:
        return _fail(exc)  # type: ignore[return-value]


@router.get("/factors/{version_id}")
def factor_version(version_id: str) -> dict:
    value = _published_lab_service().store.version(version_id)
    if value is None:
        raise HTTPException(404, "因子版本不存在")
    return value


@router.post("/factors")
def create_factor(body: FactorCreate) -> dict:
    try:
        return _get_lab_service().create_expression(**body.model_dump())
    except Exception as exc:
        return _fail(exc)  # type: ignore[return-value]


@router.post("/factors/{version_id}/approve")
def approve(version_id: str, body: Decision) -> dict:
    try:
        report = _get_lab_service().preflight("approve", {"version_id": version_id})
        from quantmaster.lab.preflight import require_runnable

        require_runnable(report)
        return _get_lab_service().store.approve(
            version_id, actor=body.actor, reason=body.reason)
    except Exception as exc:
        return _fail(exc)  # type: ignore[return-value]


@router.post("/factors/{version_id}/reject")
def reject(version_id: str, body: Decision) -> dict:
    try:
        return _get_lab_service().store.reject(
            version_id, actor=body.actor, reason=body.reason)
    except Exception as exc:
        return _fail(exc)  # type: ignore[return-value]


@router.post("/factors/{version_id}/deploy")
def deploy(version_id: str, body: Deployment) -> dict:
    try:
        report = _get_lab_service().preflight("deploy", {
            "version_id": version_id, "universe": body.universe,
        })
        from quantmaster.lab.preflight import require_runnable

        require_runnable(report)
        return _get_lab_service().store.deploy(
            version_id, universe=body.universe, horizon=body.horizon, actor=body.actor,
            profile=body.profile, scope=body.scope)
    except Exception as exc:
        return _fail(exc)  # type: ignore[return-value]


@router.post("/factors/{version_id}/suggestions")
def suggest(version_id: str, body: SuggestionRequest, response: Response) -> dict:
    try:
        if body.use_cloud:
            if not get_config().lab.allow_cloud_sample and not body.outbound_confirmed:
                raise LabError(
                    "OUTBOUND_CONFIRMATION_REQUIRED",
                    "当前设置要求每次发送云端样本前单独确认",
                    action="确认本次发送，或在设置中打开自动发送匿名云端样本",
                )
            from quantmaster.lab.llm_jobs import get_lab_llm_jobs
            from quantmaster.runtime.jobs import UnifiedJobRuntime

            job, _created = get_lab_llm_jobs().submit(
                version_id, body.sample_consent, body.anonymous_sample,
            )
            response.status_code = 202
            return UnifiedJobRuntime.public(job)
        return _get_lab_service().suggest_revision(
            version_id, use_cloud=body.use_cloud,
            sample_consent=body.sample_consent, sample=body.anonymous_sample,
        )
    except Exception as exc:
        return _fail(exc)  # type: ignore[return-value]


@router.post("/suggestions/{suggestion_id}/apply")
def apply_suggestion(suggestion_id: str, body: Decision) -> dict:
    try:
        return _get_lab_service().apply_suggestion(suggestion_id, actor=body.actor)
    except Exception as exc:
        return _fail(exc)  # type: ignore[return-value]


@router.post("/suggestions/{suggestion_id}/dismiss")
def dismiss_suggestion(suggestion_id: str) -> dict:
    try:
        return _get_lab_service().store.resolve_suggestion(suggestion_id, "dismissed")
    except Exception as exc:
        return _fail(exc)  # type: ignore[return-value]


@router.post("/jobs", status_code=202)
def enqueue_job(body: JobCreate) -> dict:
    try:
        return _get_lab_service().enqueue(body.kind, body.params)
    except Exception as exc:
        return _fail(exc)  # type: ignore[return-value]


@router.post("/mining/preview")
def mining_preview(body: MiningPreview) -> dict:
    try:
        return _get_lab_service().preview_python_mining(**body.model_dump())
    except Exception as exc:
        return _fail(exc)  # type: ignore[return-value]


@router.get("/mining/runs")
def mining_runs(limit: int = Query(30, ge=1, le=200)) -> dict:
    return {"items": _published_lab_service().store.mining_runs(limit)}


@router.get("/mining/runs/{run_id}")
def mining_run(run_id: str) -> dict:
    value = _published_lab_service().store.mining_run(run_id)
    if value is None:
        raise HTTPException(404, "AutoMiner 运行不存在")
    return value


@router.get("/mining/candidates/{candidate_id}")
def mining_candidate(candidate_id: str) -> dict:
    value = _published_lab_service().store.mining_candidate(candidate_id)
    if value is None:
        raise HTTPException(404, "AutoMiner 候选不存在")
    return value


@router.post("/studies", status_code=202)
def create_study(body: StudyCreate) -> dict:
    try:
        return _get_lab_service().create_study(body.model_dump())
    except Exception as exc:
        return _fail(exc)  # type: ignore[return-value]


@router.get("/studies")
def studies(
    limit: int = Query(50, ge=1, le=500),
    summary: bool = True,
) -> dict:
    return {"items": _published_lab_service().store.studies(limit, summary=summary)}


@router.get("/studies/{study_id}")
def study(study_id: str) -> dict:
    value = _published_lab_service().store.study(study_id)
    if value is None:
        raise HTTPException(404, "优化 Study 不存在")
    return value


@router.post("/studies/{study_id}/resume", status_code=202)
def resume_study(study_id: str) -> dict:
    try:
        return _get_lab_service().resume_study(study_id)
    except Exception as exc:
        return _fail(exc)  # type: ignore[return-value]


@router.post("/audits", status_code=202)
def create_audit(body: AuditCreate) -> dict:
    try:
        return _get_lab_service().enqueue("bias_audit", body.model_dump())
    except Exception as exc:
        return _fail(exc)  # type: ignore[return-value]


@router.get("/audits/{audit_id}")
def audit(audit_id: str) -> dict:
    value = _published_lab_service().store.bias_audit(audit_id)
    if value is None:
        raise HTTPException(404, "偏差审计不存在")
    return value


@router.get("/jobs")
def jobs(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    cursor: str | None = None,
    status: str | None = None,
    kind: str | None = None,
    summary: bool = True,
) -> dict:
    from quantmaster.lab.jobs import list_lab_jobs

    items = list_lab_jobs(
        limit, offset=offset, cursor=cursor, status=status, kind=kind, summary=summary,
    )
    return {
        "items": items,
        "next_cursor": items[-1]["id"] if len(items) == limit else "",
    }


@router.get("/experiments")
def experiments(
    limit: int = Query(50, ge=1, le=500),
    cursor: str | None = None,
    status: str | None = None,
    method: str | None = None,
    summary: bool = True,
) -> dict:
    items = _published_lab_service().store.list_experiments(
        limit, cursor=cursor, status=status, method=method, summary=summary,
    )
    return {
        "items": items,
        "next_cursor": items[-1]["id"] if len(items) == limit else "",
    }


@router.get("/experiments/{experiment_id}")
def experiment(experiment_id: str) -> dict:
    value = _published_lab_service().store.experiment(experiment_id)
    if value is None:
        raise HTTPException(404, "实验不存在")
    return value
