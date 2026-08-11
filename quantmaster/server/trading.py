"""回测工作台与多账户模拟盘 API。"""

from __future__ import annotations

import csv
import io
import logging
from typing import Literal

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import ConfigDict, Field

from quantmaster.backtest.paper_accounts import get_paper_service
from quantmaster.backtest.paper_automation import get_paper_automation_worker
from quantmaster.backtest.spec import BacktestSpec, PaperAccountSpec, StrategySpec
from quantmaster.backtest.workbench import BacktestService, BacktestStore, get_backtest_worker
from quantmaster.config import get_config
from quantmaster.runtime.contracts import ContractModel
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.runtime.problems import OperationProblem, make_problem
from quantmaster.server.management import _require_csrf

router = APIRouter(prefix="/api/v1")
logger = logging.getLogger(__name__)


def _service() -> BacktestService:
    return get_backtest_worker().service


def _read_backtests() -> BacktestStore:
    path = get_config().data_root / "backtests.sqlite"
    if not path.is_file():
        raise OperationProblem(
            503,
            make_problem(
                "snapshot_unavailable",
                severity="warning",
                source="回测账本",
                title="尚无已发布回测记录",
                message="后台 worker 尚未创建本地回测账本。",
                action="可提交显式回测任务，或继续浏览其他本地页面。",
                blocking=True,
                can_continue=True,
            ),
        )
    return BacktestStore(path=path, read_only=True)


def _read_paper_service():
    service = get_paper_service(read_only=True)
    if service.store.path.is_file():
        return service
    raise OperationProblem(
        503,
        make_problem(
            "snapshot_unavailable",
            severity="warning",
            source="模拟盘账本",
            title="尚无本地模拟盘账本",
            message="后台 worker 尚未创建任何可展示的模拟账户。",
            action="可先创建模拟账户，或继续浏览其他页面。",
            blocking=True,
            can_continue=True,
        ),
    )


def _wake_auto_account(account: dict) -> dict:
    if account.get("status") == "active" and account.get("mode") == "auto":
        get_paper_automation_worker().wake()
    return account


def _error(exc: Exception) -> HTTPException:
    logger.warning("交易 API 请求失败（%s）", type(exc).__name__, exc_info=True)
    if isinstance(exc, KeyError):
        return HTTPException(404, "交易资源不存在")
    if isinstance(exc, ValueError):
        return HTTPException(400, "交易请求参数或状态无效")
    return HTTPException(500, "交易请求执行失败，请查看本机日志")


@router.post("/backtests", status_code=202)
def create_backtest(spec: BacktestSpec, request: Request) -> dict:
    _require_csrf(request)
    worker = get_backtest_worker()
    try:
        run = worker.service.enqueue(spec)
    except ValueError:
        logger.warning("回测入队参数校验失败", exc_info=True)
        raise HTTPException(422, "回测参数无效，请检查策略、标的池和日期范围") from None
    worker.start()
    return run


@router.get("/backtests")
def list_backtests(limit: int = Query(50, ge=1, le=200)) -> dict:
    return {"items": _read_backtests().list(limit)}


@router.get("/backtests/{run_id}")
def get_backtest(run_id: str) -> dict:
    run = _read_backtests().get(run_id, include_artifact=True)
    if run is None:
        raise HTTPException(404, "回测不存在")
    return run


@router.get("/backtests/{run_id}/events")
def backtest_events(run_id: str, after: int = Query(0, ge=0)) -> dict:
    try:
        return {"items": _read_backtests().events(run_id, after=after)}
    except Exception as exc:
        raise _error(exc) from None


@router.post("/backtests/{run_id}/cancel")
def cancel_backtest(run_id: str, request: Request) -> dict:
    _require_csrf(request)
    try:
        return _service().store.cancel(run_id)
    except Exception as exc:
        raise _error(exc) from None


class CompareRequest(ContractModel):
    model_config = ConfigDict(extra="forbid")
    run_ids: list[str] = Field(..., min_length=2, max_length=4)


@router.post("/backtests/compare")
def compare_backtests(payload: CompareRequest, request: Request) -> dict:
    _require_csrf(request)
    try:
        return _service().compare(payload.run_ids)
    except Exception as exc:
        raise _error(exc) from None


@router.get("/backtests/{run_id}/export")
def export_backtest(run_id: str, format: Literal["json", "trades_csv"] = "json") -> Response:
    run = _read_backtests().get(run_id, include_artifact=True)
    if run is None:
        raise HTTPException(404, "回测不存在")
    artifact = run.get("artifact")
    if run["status"] != "completed" or not artifact:
        raise HTTPException(409, "回测尚未完成，不能导出")
    filename = f"quantmaster-backtest-{run_id[:8]}"
    if format == "json":
        return Response(
            strict_json_dumps(artifact, indent=2),
            media_type="application/json; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}.json"'},
        )
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(
        stream, fieldnames=["date", "symbol", "side", "price", "shares", "amount", "cost", "note"],
    )
    writer.writeheader()
    writer.writerows(artifact.get("trades") or [])
    return Response(
        stream.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}-trades.csv"'},
    )


class PromoteRequest(ContractModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(..., min_length=1, max_length=40)
    mode: Literal["manual", "auto"] = "manual"


@router.post("/backtests/{run_id}/paper-account", status_code=201)
def promote_backtest(run_id: str, payload: PromoteRequest, request: Request) -> dict:
    _require_csrf(request)
    run = _service().store.get(run_id)
    if run is None:
        raise HTTPException(404, "回测不存在")
    if run["status"] != "completed":
        raise HTTPException(409, "只有已完成回测才能创建模拟账户")
    if run.get("legacy_read_only"):
        raise HTTPException(409, "旧 Swing 回测仅供历史查看，不能创建模拟账户")
    try:
        spec = PaperAccountSpec.model_validate({
            "name": payload.name,
            "strategy": run["config"]["strategy"],
            "universe": run["config"]["universe"],
            "initial_capital": run["config"]["initial_capital"],
            "mode": payload.mode,
            "source_backtest_id": run_id,
        })
        return _wake_auto_account(get_paper_service().create_account(spec))
    except Exception as exc:
        raise _error(exc) from None


@router.post("/paper/accounts", status_code=201)
def create_paper_account(spec: PaperAccountSpec, request: Request) -> dict:
    _require_csrf(request)
    try:
        return _wake_auto_account(get_paper_service().create_account(spec))
    except Exception as exc:
        raise _error(exc) from None


@router.get("/paper/accounts")
def list_paper_accounts(include_archived: bool = False) -> dict:
    service = _read_paper_service()
    return {"items": service.store.accounts(include_archived=include_archived)}


@router.get("/paper/accounts/{account_id}")
def get_paper_account(account_id: str) -> dict:
    try:
        return _read_paper_service().account_details(account_id)
    except (KeyError, ValueError) as exc:
        raise _error(exc) from None


class PaperAccountUpdate(ContractModel):
    model_config = ConfigDict(extra="forbid")
    name: str | None = Field(None, min_length=1, max_length=40)
    status: Literal["active", "paused", "archived"] | None = None
    mode: Literal["manual", "auto"] | None = None
    strategy: StrategySpec | None = None
    universe: str | None = Field(None, min_length=1, max_length=40)


@router.patch("/paper/accounts/{account_id}")
def update_paper_account(account_id: str, payload: PaperAccountUpdate, request: Request) -> dict:
    _require_csrf(request)
    if all(value is None for value in (
        payload.name, payload.status, payload.mode, payload.strategy, payload.universe,
    )):
        raise HTTPException(422, "至少需要修改一个账户字段")
    try:
        account = get_paper_service().update_account(
            account_id,
            name=payload.name,
            status=payload.status,
            mode=payload.mode,
            strategy=payload.strategy,
            universe=payload.universe,
        )
        return _wake_auto_account(account)
    except Exception as exc:
        raise _error(exc) from None


@router.delete("/paper/accounts/{account_id}")
def delete_paper_account(account_id: str, request: Request) -> dict:
    """Archive an account without deleting its ledger or historical cycles."""
    _require_csrf(request)
    try:
        account = get_paper_service().archive_account(account_id)
        return {"deleted": True, "recoverable": True, "account": account}
    except (KeyError, ValueError) as exc:
        raise _error(exc) from None


class CloneAccountRequest(ContractModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(..., min_length=1, max_length=40)
    mode: Literal["manual", "auto"] = "manual"


@router.post("/paper/accounts/{account_id}/clone", status_code=201)
def clone_paper_account(account_id: str, payload: CloneAccountRequest, request: Request) -> dict:
    _require_csrf(request)
    try:
        return _wake_auto_account(
            get_paper_service().clone_account(account_id, name=payload.name, mode=payload.mode)
        )
    except Exception as exc:
        raise _error(exc) from None


@router.post("/paper/accounts/{account_id}/proposals")
def propose_paper_cycle(account_id: str, request: Request) -> dict:
    _require_csrf(request)
    try:
        return get_paper_service().propose(account_id)
    except Exception as exc:
        raise _error(exc) from None


@router.post("/paper/cycles/{cycle_id}/confirm")
def confirm_paper_cycle(cycle_id: str, request: Request) -> dict:
    _require_csrf(request)
    try:
        return get_paper_service().store.confirm(cycle_id)
    except Exception as exc:
        raise _error(exc) from None


@router.post("/paper/accounts/{account_id}/process")
def process_paper_account(account_id: str, request: Request) -> dict:
    _require_csrf(request)
    try:
        return get_paper_service().process(account_id)
    except Exception as exc:
        raise _error(exc) from None


@router.get("/paper/accounts/{account_id}/report")
def paper_account_report(account_id: str) -> dict:
    try:
        return _read_paper_service().report(account_id)
    except Exception as exc:
        raise _error(exc) from None


@router.get("/paper/accounts/{account_id}/cycles")
def paper_account_cycles(account_id: str, limit: int = Query(30, ge=1, le=200)) -> dict:
    service = _read_paper_service()
    if service.store.account(account_id) is None:
        raise HTTPException(404, "模拟账户不存在")
    return {"items": service.store.cycles(account_id, limit=limit)}
