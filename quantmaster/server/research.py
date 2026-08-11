"""Local-only management API for research catalog, plans and persistent jobs."""

from __future__ import annotations

import json
import sqlite3
from datetime import timedelta
from typing import Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import Field

from quantmaster.config import get_config
from quantmaster.research import AssetClass, KernelBackend
from quantmaster.research.catalog import ResearchCatalog
from quantmaster.research.engine import ResearchEngine
from quantmaster.research.jobs import ResearchJobManager, get_research_job_manager
from quantmaster.runtime.contracts import ContractModel
from quantmaster.runtime.problems import OperationProblem, make_problem
from quantmaster.server.management import _require_csrf, _require_local
from quantmaster.trading_sessions import market_date, resolve_session_target

router = APIRouter(prefix="/api/v1/research/data", tags=["research-data"])


def _read_catalog() -> ResearchCatalog:
    """Open published research metadata without starting a job manager.

    A page read must never create the directory, migrate a schema, recover a
    lease or start job threads.  The writer owns all of those actions.
    """

    path = get_config().data_root / "research_lake" / "_meta" / "catalog.sqlite"
    if not path.is_file():
        raise OperationProblem(
            503,
            make_problem(
                "snapshot_unavailable",
                severity="warning",
                source="研究数据快照",
                title="研究快照尚未发布",
                message="本地研究目录尚无可读取的已发布快照。",
                action="请提交研究构建任务；页面不会在读取时联网或初始化数据。",
                blocking=True,
                can_continue=False,
            ),
        )
    return ResearchCatalog(path, read_only=True)


def _read_engine() -> ResearchEngine:
    # Validate the catalog path before construction so a cold start becomes a
    # structured 503 instead of an implicit SQLite initialization attempt.
    _read_catalog()
    return ResearchEngine(read_only=True)


def _cold_catalog() -> dict:
    """Expose static planning metadata when no research snapshot exists.

    Dataset/spec definitions are code contracts, not derived market results;
    exposing them lets the page render an explicit cold state and submit a
    build without creating a catalog or fabricating coverage rows.
    """

    from quantmaster.research.adapters import DATASET_BY_ID
    from quantmaster.research.registry import built_in_registry

    return {
        "datasets": [item.to_dict() for item in DATASET_BY_ID.values()],
        "specs": built_in_registry().catalog(),
        "partitions": [],
        "meta": {
            "snapshot_id": "",
            "schema_version": 2,
            "algorithm_version": "research-catalog-v2",
            "input_fingerprint": "",
            "as_of": "",
            "generated_at": "",
            "stale": True,
            "cold": True,
            "stale_reasons": ["研究目录尚未发布"],
            "quality": {"status": "cold"},
        },
    }


def _default_close_data_end() -> str:
    expectation = resolve_session_target()
    if expectation.ready and expectation.session:
        return expectation.session
    return (market_date() - timedelta(days=1)).isoformat()


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
            self.start, self.end or _default_close_data_end(),
            asset_classes=tuple(AssetClass(item) for item in self.assets),
            datasets=tuple(self.datasets) or None, spec_ids=tuple(self.specs) or None,
            mode=self.mode, backend=KernelBackend(self.backend),
        )


class MaterializeRequest(ContractModel):
    start: str = Field(default="2022-01-01", min_length=10, max_length=10)
    end: str | None = Field(default=None, min_length=10, max_length=10)
    asset: Literal["stock", "etf", "future"] = "stock"
    symbols: list[str] = Field(default_factory=list, max_length=10_000)


class ResearchPreviewRequest(ContractModel):
    dataset_id: str = Field(default="stock_bars", min_length=1, max_length=80)
    trade_date: str | None = Field(default=None, min_length=10, max_length=10)
    limit: int = Field(default=500, ge=1, le=10_000)


@router.get("/catalog")
def research_catalog(request: Request) -> dict:
    _require_local(request)
    try:
        return _read_engine().catalog()
    except OperationProblem:
        return _cold_catalog()
    except sqlite3.Error as exc:
        raise HTTPException(503, "研究快照暂不可读") from exc


@router.get("/capabilities")
def research_capabilities(request: Request) -> dict:
    _require_local(request)
    try:
        return _read_engine().capabilities()
    except sqlite3.Error as exc:
        raise HTTPException(503, "研究快照暂不可读") from exc


@router.post("/preview")
def preview_research_partition(request: Request, value: ResearchPreviewRequest) -> dict:
    """Return an explicit sandbox preview without writing a Research Lake partition."""
    _require_csrf(request)
    trade_date = value.trade_date or _default_close_data_end()
    try:
        frame = ResearchEngine().preview_date(value.dataset_id, trade_date)
    except (KeyError, OSError, RuntimeError, TypeError, ValueError) as exc:
        raise HTTPException(400, str(exc)) from None
    rows = json.loads(
        frame.head(value.limit).to_json(orient="records", date_format="iso", force_ascii=False)
    )
    return {
        "tier": "sandbox",
        "dataset_id": value.dataset_id,
        "trade_date": trade_date,
        "row_count": len(frame),
        "returned_rows": len(rows),
        "quality": dict(frame.attrs.get("research_partition_quality") or {}),
        "rows": rows,
    }


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
    try:
        catalog = _read_catalog()
        return {
            "items": [
                ResearchJobManager.public(item)
                for item in catalog.jobs(max(1, min(limit, 200)))
            ]
        }
    except sqlite3.Error as exc:
        raise HTTPException(503, "研究任务快照暂不可读") from exc


@router.get("/jobs/{job_id}")
def get_research_job(job_id: str, request: Request) -> dict:
    _require_local(request)
    try:
        value = _read_catalog().job(job_id)
        if value is None:
            raise KeyError(job_id)
        return ResearchJobManager.public(value)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from None
    except sqlite3.Error as exc:
        raise HTTPException(503, "研究任务快照暂不可读") from exc


@router.get("/jobs/{job_id}/events")
def get_research_job_events(
    job_id: str,
    request: Request,
    after: int = 0,
    limit: int = 500,
) -> dict:
    _require_local(request)
    try:
        catalog = _read_catalog()
        if catalog.job(job_id) is None:
            raise KeyError(job_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from None
    except sqlite3.Error as exc:
        raise HTTPException(503, "研究任务快照暂不可读") from exc
    return {"items": catalog.job_events(job_id, after, limit)}


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
            value.symbols or None, value.start, value.end or _default_close_data_end(),
            asset_class=AssetClass(value.asset),
        )
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(400, str(exc)) from None
    return {
        "asset_class": value.asset,
        "partitions": len(records),
        "rows": sum(int(item["row_count"]) for item in records),
    }
