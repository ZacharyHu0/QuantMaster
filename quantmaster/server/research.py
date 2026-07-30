"""Local-only management API for research catalog, plans and persistent jobs."""

from __future__ import annotations

from datetime import date
from typing import Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import Field

from quantmaster.research import AssetClass, KernelBackend
from quantmaster.research.engine import ResearchEngine
from quantmaster.research.jobs import get_research_job_manager
from quantmaster.runtime.contracts import ContractModel
from quantmaster.server.management import _require_csrf, _require_local

router = APIRouter(prefix="/api/research/data", tags=["research-data"])


class ResearchPlanRequest(ContractModel):
    start: str = Field(default="2022-01-01", min_length=10, max_length=10)
    end: str | None = Field(default=None, min_length=10, max_length=10)
    assets: list[Literal["stock", "etf", "future"]] = Field(
        default_factory=lambda: ["stock"], min_length=1, max_length=3,
    )
    datasets: list[str] = Field(default_factory=list, max_length=20)
    specs: list[str] = Field(default_factory=list, max_length=100)
    mode: Literal["historical", "incremental"] = "historical"
    backend: Literal["auto", "python", "rust"] = "auto"

    def make_plan(self, engine: ResearchEngine):
        return engine.plan(
            self.start, self.end or str(date.today()),
            asset_classes=tuple(AssetClass(item) for item in self.assets),
            datasets=tuple(self.datasets) or None, spec_ids=tuple(self.specs) or None,
            mode=self.mode, backend=KernelBackend(self.backend),
        )


class MaterializeRequest(ContractModel):
    start: str = Field(default="2022-01-01", min_length=10, max_length=10)
    end: str | None = Field(default=None, min_length=10, max_length=10)
    asset: Literal["stock", "etf", "future"] = "stock"
    symbols: list[str] = Field(default_factory=list, max_length=10_000)


@router.get("/catalog")
def research_catalog(request: Request) -> dict:
    _require_local(request)
    return ResearchEngine().catalog()


@router.get("/capabilities")
def research_capabilities(request: Request) -> dict:
    _require_local(request)
    return ResearchEngine().capabilities()


@router.post("/plans")
def create_research_plan(request: Request, value: ResearchPlanRequest) -> dict:
    _require_csrf(request)
    try:
        return value.make_plan(ResearchEngine()).to_dict()
    except (ValueError, RuntimeError, KeyError) as exc:
        raise HTTPException(400, str(exc)) from None


@router.post("/jobs")
def create_research_job(request: Request, value: ResearchPlanRequest) -> dict:
    _require_csrf(request)
    try:
        manager = get_research_job_manager()
        return manager.public(manager.create(value.make_plan(manager.engine), value.mode))
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from None
    except (RuntimeError, KeyError) as exc:
        raise HTTPException(400, str(exc)) from None


@router.get("/jobs")
def list_research_jobs(request: Request, limit: int = 50) -> dict:
    _require_local(request)
    manager = get_research_job_manager()
    return {
        "items": [manager.public(item) for item in manager.list(max(1, min(limit, 200)))]
    }


@router.get("/jobs/{job_id}")
def get_research_job(job_id: str, request: Request) -> dict:
    _require_local(request)
    try:
        manager = get_research_job_manager()
        return manager.public(manager.get(job_id))
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from None


@router.get("/jobs/{job_id}/events")
def get_research_job_events(
    job_id: str,
    request: Request,
    after: int = 0,
    limit: int = 500,
) -> dict:
    _require_local(request)
    manager = get_research_job_manager()
    try:
        manager.get(job_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from None
    return {"items": manager.catalog.job_events(job_id, after, limit)}


@router.post("/jobs/{job_id}/cancel")
def cancel_research_job(job_id: str, request: Request) -> dict:
    _require_csrf(request)
    try:
        manager = get_research_job_manager()
        return manager.public(manager.cancel(job_id))
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from None
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from None


@router.post("/jobs/{job_id}/resume")
def resume_research_job(job_id: str, request: Request) -> dict:
    _require_csrf(request)
    try:
        manager = get_research_job_manager()
        return manager.public(manager.resume(job_id))
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from None
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from None


@router.post("/materialize")
def materialize_bar_store(request: Request, value: MaterializeRequest) -> dict:
    _require_csrf(request)
    engine = ResearchEngine()
    try:
        records = engine.lake.materialize_bar_store(
            value.symbols or None, value.start, value.end or str(date.today()),
            asset_class=AssetClass(value.asset),
        )
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(400, str(exc)) from None
    return {
        "asset_class": value.asset,
        "partitions": len(records),
        "rows": sum(int(item["row_count"]) for item in records),
    }
