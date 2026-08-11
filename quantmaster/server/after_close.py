from __future__ import annotations

import csv
import io
from typing import Literal

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response
from pydantic import Field

from quantmaster.after_close.jobs import get_after_close_jobs
from quantmaster.after_close.service import get_after_close_service
from quantmaster.runtime.contracts import ContractModel
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.runtime.problems import OperationProblem, make_problem
from quantmaster.server.security import require_csrf, require_local

router = APIRouter(prefix="/api/v1/after-close", tags=["after-close"])


def _published_service():
    """Open the published ledger without bootstrapping a scan runtime."""

    service = get_after_close_service(read_only=True)
    if service.store.path.is_file():
        return service
    raise OperationProblem(
        503,
        make_problem(
            "snapshot_unavailable",
            severity="warning",
            source="盘后研究快照",
            title="尚无已发布盘后研究快照",
            message="后台 worker 尚未产出可展示的本地盘后研究结果。",
            action="继续浏览其他页面或提交一次盘后扫描任务后重试。",
            blocking=True,
            can_continue=True,
        ),
    )


class ScanBody(ContractModel):
    as_of: str = Field(default="", pattern=r"^$|^\d{4}-\d{2}-\d{2}$")
    force: bool = False


class CopyBody(ContractModel):
    name: str = Field(min_length=1, max_length=80)
    symbols: list[str] = Field(default_factory=list, max_length=200)


@router.get("/snapshots/latest")
def latest(request: Request) -> dict:
    require_local(request)
    service = _published_service()
    snapshot = service.store.public_latest()
    return {
        "snapshot": snapshot,
        "labels": service.store.labels(snapshot["snapshot_id"]) if snapshot else [],
    }


@router.get("/snapshots")
def history(request: Request, limit: int = Query(30, ge=1, le=500)) -> dict:
    require_local(request)
    return {"items": _published_service().store.history(limit)}


@router.get("/health")
def strategy_health(request: Request, limit: int = Query(100, ge=1, le=500)) -> dict:
    require_local(request)
    return _published_service().store.health(limit)


@router.get("/snapshots/{snapshot_id}")
def detail(snapshot_id: str, request: Request) -> dict:
    require_local(request)
    service = _published_service()
    snapshot = service.store.get(snapshot_id)
    if snapshot is None:
        raise HTTPException(404, "盘后快照不存在")
    return {"snapshot": snapshot.to_dict(), "labels": service.store.labels(snapshot_id)}


@router.post("/scan", status_code=202)
def scan(body: ScanBody, request: Request) -> dict:
    require_csrf(request)
    try:
        job, created = get_after_close_jobs().submit(as_of=body.as_of, force=body.force)
        return {**get_after_close_jobs().public(job), "created": created}
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(409, str(exc)) from None


@router.get("/export/{snapshot_id}")
def export_snapshot(
    snapshot_id: str, request: Request,
    format: Literal["json", "csv"] = "json",
) -> Response:
    require_local(request)
    snapshot = _published_service().store.get(snapshot_id)
    if snapshot is None:
        raise HTTPException(404, "盘后快照不存在")
    if format == "json":
        return Response(
            strict_json_dumps(snapshot.to_dict(), indent=2), media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="{snapshot_id}.json"'},
        )
    output = io.StringIO(newline="")
    fields = [
        "rank", "symbol", "name", "score", "as_of_date", "sectors", "reasons",
        "return_5d", "return_20d", "trend_20d", "avg_amount_20d", "amount_change",
        "volatility_20d", "drawdown_20d", "float_mv", "total_mv", "pe_ttm", "pb",
    ]
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()
    for candidate in snapshot.candidates:
        writer.writerow({
            "rank": candidate.rank, "symbol": candidate.symbol, "name": candidate.name,
            "score": candidate.score, "as_of_date": candidate.as_of_date,
            "sectors": " / ".join(item["name"] for item in candidate.sectors),
            "reasons": "；".join(candidate.reasons),
            **{key: candidate.metrics.get(key) for key in fields if key in candidate.metrics},
        })
    return Response(
        output.getvalue().encode("utf-8-sig"), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{snapshot_id}.csv"'},
    )


@router.post("/candidates/copy")
def copy_candidates(body: CopyBody, request: Request) -> dict:
    require_csrf(request)
    from quantmaster.data.universe import save_universe

    latest_value = _published_service().store.latest()
    if latest_value is None:
        raise HTTPException(409, "尚无盘后研究快照")
    allowed = {item.symbol for item in latest_value.candidates}
    selected = body.symbols or [item.symbol for item in latest_value.candidates]
    if not selected or any(symbol not in allowed for symbol in selected):
        raise HTTPException(422, "只能复制当前盘后研究候选")
    try:
        save_universe(body.name, selected)
    except (OSError, ValueError) as exc:
        raise HTTPException(400, str(exc)) from None
    return {"status": "ok", "name": body.name, "count": len(selected)}
