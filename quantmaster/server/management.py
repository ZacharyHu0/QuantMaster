"""本机设置、候选、迁移与券商 CSV 导入 API。"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import date
from pathlib import Path
from typing import Annotated, Any, Literal

from fastapi import (
    APIRouter,
    Body,
    File,
    Form,
    HTTPException,
    Request,
    Response,
    UploadFile,
)
from pydantic import Field

from quantmaster.config import get_config
from quantmaster.credentials import CredentialError
from quantmaster.data.migration import (
    LegacyMigrationError,
    MigrationError,
    legacy_migration_manager,
    migration_manager,
    registered_migrations,
)
from quantmaster.runtime.activation import ActivationBlocked
from quantmaster.runtime.contracts import ContractModel
from quantmaster.runtime.problems import OperationProblem, make_problem
from quantmaster.server.security import (
    attach_csrf_cookie,
    is_local_request,
    issue_csrf,
    require_csrf,
    require_local,
)
from quantmaster.server.settings_control import register_settings_control
from quantmaster.settings import (
    SecretMutations,
    SettingsDocument,
    SettingsUpdate,
    document_from_config,
)
from quantmaster.trading_sessions import market_date

router = APIRouter(prefix="/api/v1")
settings_manager = migration_manager.config_manager
_running_server: dict[str, Any] = {}
_applied_migrations: set[str] = set()
logger = logging.getLogger(__name__)




def _require_runtime_worker() -> dict[str, Any]:
    """Refuse a command when the supervisor-owned worker lease is absent."""

    from quantmaster.runtime.worker import runtime_worker_status

    status = runtime_worker_status()
    if status.get("available"):
        return status
    raise OperationProblem(
        503,
        make_problem(
            "worker_unavailable",
            severity="warning",
            source="后台 runtime-worker",
            title="后台执行器不可用",
            message=str(status.get("reason") or "后台执行器未运行"),
            action="页面仍可读取本地快照；请重启 QuantMaster 后再提交刷新任务。",
            blocking=True,
            can_continue=True,
        ),
    )


def _data_refresh_worker_command(
    operation: str,
    payload: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Forward a mutation to the worker instead of writing its ledger in Web."""

    from quantmaster.runtime.worker_ipc import (
        WorkerCommandError,
        WorkerCommandUnavailable,
        call_worker_command,
    )

    worker = _require_runtime_worker()
    try:
        return worker, call_worker_command(operation, payload)
    except WorkerCommandUnavailable as exc:
        raise OperationProblem(
            503,
            make_problem(
                "worker_unavailable",
                severity="warning",
                source="后台 runtime-worker",
                title="后台执行器不可用",
                message=str(exc),
                action="页面仍可读取本地快照；请重启 QuantMaster 后再提交刷新任务。",
                blocking=True,
                can_continue=True,
            ),
        ) from exc
    except WorkerCommandError as exc:
        status = 404 if exc.code == "job_not_found" else 409 if exc.code == "command_conflict" else 400
        raise HTTPException(status, str(exc)) from None


def _public_error(status_code: int, detail: str, context: str) -> HTTPException:
    """Log the active exception locally while returning only stable public text."""
    logger.warning("%s", context, exc_info=True)
    return HTTPException(status_code, detail)


class StockDBTickRequest(ContractModel):
    symbol: str = Field(min_length=6, max_length=12)
    count: int = Field(default=1, ge=1, le=20)


class StockDBFundamentalsRequest(ContractModel):
    symbol: str = Field(min_length=6, max_length=12)
    dataset: Literal["cash_flow", "income", "balance", "valuation"]
    stat_date: str = Field(pattern=r"^\d{4}(?:q[1-4]|-\d{2}-\d{2})$")


class UpdateActivationRequest(ContractModel):
    build_sha: str = Field(pattern=r"^[0-9a-f]{40}$")


def _local(request: Request) -> bool:
    return is_local_request(request)


def _require_local(request: Request) -> None:
    require_local(request)


def _issue_csrf() -> str:
    return issue_csrf()


def _require_csrf(request: Request) -> None:
    require_csrf(request)


def capture_runtime_baseline() -> None:
    """在应用 lifespan 开始时记录真正需要重启才能改变的服务地址。"""
    cfg = get_config()
    _running_server.clear()
    _running_server.update({"host": cfg.server.host, "port": cfg.server.port})


def _runtime_status() -> dict[str, Any]:
    from quantmaster.runtime.worker import runtime_worker_status
    from quantmaster.server.settings_runtime import public_state

    cfg = get_config()
    configured = {"host": cfg.server.host, "port": cfg.server.port}
    running = _running_server or configured
    restart = [f"server.{name}" for name in ("host", "port") if running.get(name) != configured.get(name)]
    worker = runtime_worker_status()
    manager_path = getattr(settings_manager, "path", None)
    governance = (
        public_state(manager_path, worker_available=bool(worker.get("available")))
        if manager_path is not None
        else {
            "persisted_revision": settings_manager.public().get("config_revision", 0),
            "latest_generation": 0,
            "components": {},
            "drift": [],
        }
    )
    managed_state = "running" if worker.get("available") else "unavailable"
    return {
        "config_revision": governance["persisted_revision"],
        "persisted_revision": governance["persisted_revision"],
        "latest_generation": governance["latest_generation"],
        "components": governance["components"],
        "drift": governance["drift"],
        "server": {
            "status": "restart_required" if restart else "applied",
            "running": dict(running),
            "configured": configured,
            "restart_required": restart,
        },
        # This is a page/read endpoint.  Do not instantiate AutomationRuntime,
        # LabWorker, or the StockDB sidecar just to ask for their status: those
        # constructors own schemas, leases and threads.  The supervisor lease
        # is the published status projection; detailed live diagnostics remain
        # worker-owned and are refreshed asynchronously.
        "worker": worker,
        "automation": {
            "status": "disabled" if not cfg.automation.enabled else managed_state,
            "managed_by": "runtime-worker",
        },
        "free_stockdb": {
            "status": managed_state,
            "managed_by": "runtime-worker",
        },
        "lab": {
            "status": "disabled" if not cfg.lab.enabled else managed_state,
            "managed_by": "runtime-worker",
        },
    }


def _apply_free_stockdb(changed: list[str], result: dict[str, Any]) -> dict[str, Any]:
    from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime

    if not any(field.startswith("data.free_stockdb_") for field in changed):
        return {"status": "unchanged"}
    try:
        return free_stockdb_runtime.request_apply_config(changed)
    except (OSError, RuntimeError, ValueError):
        logger.warning("free-stockdb 托管运行时热应用失败", exc_info=True)
        result.setdefault("warnings", []).append("free-stockdb 设置已保存，但运行时热应用失败")
        return {"status": "degraded", "message": "托管运行时应用失败，请重启服务"}


def _report_apply_component(
    component: str, status: dict[str, Any], *, revision: int, generation: int,
    diagnostic_code: str, recommendation: str,
) -> None:
    from quantmaster.server.settings_runtime import report_component

    degraded = str(status.get("status") or "unchanged") == "degraded"
    report_component(
        settings_manager.path, component, revision=revision, generation=generation,
        status="failed" if degraded else "effective",
        error=str(status.get("message") or "") if degraded else "",
        diagnostic_code=diagnostic_code if degraded else "",
        recommendation=recommendation if degraded else "",
    )


def _apply_worker_components(
    changed: list[str], result: dict[str, Any], apply_status: dict[str, Any],
    *, revision: int, generation: int,
) -> None:
    from quantmaster.lab.worker import get_worker

    try:
        if "data.root" in changed:
            from quantmaster.automation.runtime import get_runtime

            active = get_runtime().start() if get_config().automation.enabled else False
            apply_status["automation"] = {
                "status": "applied" if active else "disabled"
                if not get_config().automation.enabled else "standby"
            }
        elif any(field.startswith("automation.") for field in changed):
            from quantmaster.automation.runtime import get_runtime

            apply_status["automation"] = get_runtime().apply_config(changed)
    except Exception:
        logger.warning("自动化运行时热应用失败", exc_info=True)
        apply_status["automation"] = {"status": "degraded", "message": "运行时热应用失败，请重启服务"}
        result.setdefault("warnings", []).append("自动化配置已保存，但运行时热应用失败")
    _report_apply_component(
        "automation", apply_status["automation"], revision=revision, generation=generation,
        diagnostic_code="automation_apply_failed", recommendation="重试应用；持续失败时重启服务",
    )
    try:
        worker = get_worker()
        if "data.root" in changed:
            if get_config().lab.enabled:
                worker.start()
                apply_status["lab"] = {"status": "applied"}
            else:
                apply_status["lab"] = {"status": "disabled"}
        elif any(field.startswith("lab.") for field in changed) or "automation.timezone" in changed:
            apply_status["lab"] = worker.apply_config(changed)
    except Exception:
        logger.warning("Quant Lab Worker 热应用失败", exc_info=True)
        apply_status["lab"] = {"status": "degraded", "message": "Worker 热应用失败，请重启服务"}
        result.setdefault("warnings", []).append("Quant Lab 配置已保存，但 Worker 热应用失败")
    _report_apply_component(
        "lab", apply_status["lab"], revision=revision, generation=generation,
        diagnostic_code="lab_apply_failed", recommendation="重试应用；持续失败时重启服务",
    )


def _report_remaining_components(
    llm_probe: dict[str, Any] | None, apply_status: dict[str, Any],
    *, revision: int, generation: int,
) -> None:
    from quantmaster.server.settings_runtime import report_component

    if llm_probe and str(llm_probe.get("status")) == "error":
        previous = _runtime_status().get("components", {}).get("llm", {})
        report_component(
            settings_manager.path, "llm", revision=revision, generation=generation,
            status="failed", effective_revision=int(previous.get("effective_revision") or 0),
            error=str(llm_probe.get("message") or llm_probe.get("error") or "candidate probe failed"),
            diagnostic_code=str(llm_probe.get("diagnostic_id") or "llm_probe_failed"),
            recommendation="修改 provider/凭据后重试应用，或回滚已保存版本",
        )
        apply_status["llm"] = {
            "status": "degraded", "message": llm_probe.get("message"),
            "diagnostic_id": llm_probe.get("diagnostic_id"),
        }
    else:
        report_component(
            settings_manager.path, "llm", revision=revision,
            generation=generation, status="effective",
        )
    for component in ("data-clients", "scheduler"):
        report_component(
            settings_manager.path, component, revision=revision,
            generation=generation, status="effective",
        )


def _apply_runtime(result: dict[str, Any]) -> dict[str, Any]:
    """按变更字段热应用进程内服务；配置落盘成功不因联网状态回滚。"""
    from quantmaster.config import get_config as current_config
    from quantmaster.config import set_config
    from quantmaster.server.settings_runtime import report_component

    changed = list(result.get("changed_fields") or [])
    revision = int(result.get("config_revision") or 0)
    generation = int(result.get("generation") or 0)
    latest = int(settings_manager.public().get("config_revision") or 0)
    if revision < latest:
        result["apply_status"] = {"config": {"status": "superseded"}}
        result["runtime"] = _runtime_status()
        return result
    # Runtime-worker owns its own process-local snapshot.  Reconcile from the
    # persisted source before applying components; never use the stale config
    # captured at worker startup.
    previous_runtime_config = current_config()
    candidate_runtime_config = settings_manager.load()
    llm_probe: dict[str, Any] | None = None
    if any(field.startswith("llm.") for field in changed):
        try:
            from quantmaster.server.settings_checks import list_llm_models
            from quantmaster.settings import document_from_config

            llm_probe = list_llm_models(
                document_from_config(candidate_runtime_config).llm,
                candidate_runtime_config.llm.api_key,
                isolated=True,
            )
            if str(llm_probe.get("status") or "warning") == "error":
                candidate_runtime_config.llm = previous_runtime_config.llm
        except (CredentialError, OSError, RuntimeError, TypeError, ValueError) as exc:
            logger.warning("LLM candidate probe failed", exc_info=True)
            llm_probe = {
                "status": "error",
                "message": "LLM 临时 client 探测失败",
                "diagnostic_id": "llm_probe_failed",
                "error": str(exc),
            }
            candidate_runtime_config.llm = previous_runtime_config.llm
    set_config(candidate_runtime_config)
    report_component(
        settings_manager.path, "runtime-worker", revision=revision,
        generation=generation, status="effective",
    )
    if result.get("restart_required"):
        server_previous = _runtime_status().get("components", {}).get("server", {})
        report_component(
            settings_manager.path, "server", revision=revision,
            generation=generation, status="restart_required",
            effective_revision=int(server_previous.get("effective_revision") or 0),
            recommendation="重启 QuantMaster 后应用监听地址变更",
        )
    else:
        report_component(
            settings_manager.path, "server", revision=revision,
            generation=generation, status="effective",
        )
    apply_status: dict[str, Any] = {
        "config": {"status": "applied"},
        "automation": {"status": "unchanged"},
        "lab": {"status": "unchanged"},
        "server": {"status": "restart_required" if result.get("restart_required") else "applied"},
    }
    apply_status["free_stockdb"] = _apply_free_stockdb(changed, result)
    _apply_worker_components(
        changed, result, apply_status, revision=revision, generation=generation,
    )
    if "data.root" in changed:
        try:
            from quantmaster.backtest.jobs import get_backtest_job_manager
            from quantmaster.backtest.paper_automation import get_paper_automation_worker
            from quantmaster.data.maintenance import data_refresh_manager
            from quantmaster.research.jobs import get_research_job_manager

            data_refresh_manager.start()
            get_research_job_manager().start()
            get_backtest_job_manager().start()
            get_paper_automation_worker().start()
            apply_status["data_workers"] = {"status": "applied"}
        except Exception:
            logger.warning("数据目录切换后后台执行器恢复失败", exc_info=True)
            apply_status["data_workers"] = {
                "status": "degraded",
                "message": "后台执行器恢复失败，请重启服务",
            }
            result.setdefault("warnings", []).append("数据目录已切换，但部分后台执行器需要重启服务后恢复")
    stock_status = str(apply_status["free_stockdb"].get("status") or "unchanged")
    report_component(
        settings_manager.path, "free-stockdb", revision=revision, generation=generation,
        status="failed" if stock_status == "degraded" else "effective",
        error=str(apply_status["free_stockdb"].get("message") or "") if stock_status == "degraded" else "",
        diagnostic_code="free_stockdb_apply_failed" if stock_status == "degraded" else "",
        recommendation="重试应用或重启服务" if stock_status == "degraded" else "",
    )
    _report_remaining_components(
        llm_probe, apply_status, revision=revision, generation=generation,
    )
    result["apply_status"] = apply_status
    result["runtime"] = _runtime_status()
    return result


register_settings_control(settings_manager, _apply_runtime)


def _llm_cancellation_after_save(
    saved: dict[str, Any], *, llm_secret_changed: bool = False,
) -> dict[str, Any]:
    """Rotate only scopes invalidated by an already-persisted settings change."""
    changed = {str(value) for value in saved.get("changed_fields") or ()}
    global_changed = llm_secret_changed or any(value.startswith("llm.") for value in changed)
    news_changed = any(value.startswith("news.annotation_") for value in changed)
    if not global_changed and not news_changed:
        return {"scopes": [], "queued_cancelled": 0, "running_cancelling": 0}
    from quantmaster.runtime.llm import get_llm_execution_coordinator

    rotation = get_llm_execution_coordinator().rotate(
        global_scope=global_changed,
        news_scope=global_changed or news_changed,
        reason="settings_saved",
    )
    # A cancelled settings diagnostic never reaches its handler/finally block.
    # Clear its opaque temporary credential reference immediately after the
    # ledger cancellation converges.
    try:
        from quantmaster.server.settings_jobs import get_settings_jobs

        get_settings_jobs().cleanup_cancelled_credentials()
    except (CredentialError, OSError, RuntimeError, ValueError):
        logger.warning("已取消设置检测的临时凭据清理延后", exc_info=True)
    scopes = ([] if not global_changed else ["global"]) + (
        ["news"] if global_changed or news_changed else []
    )
    return {
        "scopes": scopes,
        "queued_cancelled": int(rotation["queued_cancelled"]),
        "running_cancelling": int(rotation["running_cancelling"]),
    }


def _queue_runtime_apply(saved: dict[str, Any]) -> dict[str, Any]:
    from quantmaster.server.settings_jobs import get_settings_jobs
    from quantmaster.server.settings_runtime import begin_apply

    if not int(saved.get("generation") or 0):
        saved["generation"] = begin_apply(
            settings_manager.path, int(saved.get("config_revision") or 0),
        )
    jobs = get_settings_jobs()
    task, _created = jobs.submit_apply(saved)
    return jobs.public(task)


@router.get("/settings")
def get_settings(request: Request, response: Response) -> dict:
    _require_local(request)
    token = _issue_csrf()
    attach_csrf_cookie(response, request, token)
    return {
        **settings_manager.public(),
        "csrf_token": token,
        "remote_management": False,
        "runtime": _runtime_status(),
    }


@router.get("/settings/runtime")
def settings_runtime(request: Request) -> dict:
    _require_local(request)
    return _runtime_status()


@router.post("/settings/apply", status_code=202)
def retry_settings_apply(request: Request) -> dict:
    """Retry applying the latest persisted revision without saving again."""
    _require_csrf(request)
    revision = int(settings_manager.public().get("config_revision") or 0)
    if revision <= 0:
        raise HTTPException(409, "尚无已保存设置可应用")
    current = SettingsDocument.model_validate({
        key: value for key, value in settings_manager.public().items()
        if key in SettingsDocument.model_fields
    })
    changed_fields = [
        f"{group}.{field}"
        for group, section in current.model_dump().items()
        if isinstance(section, dict)
        for field in section
    ]
    saved = {
        "status": "ok",
        "config_revision": revision,
        "persisted_revision": revision,
        "changed_fields": changed_fields,
        "restart_required": [],
        "warnings": [],
    }
    task = _queue_runtime_apply(saved)
    return {"runtime_apply": task, "generation": saved["generation"], "runtime": _runtime_status()}


@router.get("/system/update")
def system_update(request: Request) -> dict[str, object]:
    """Expose cached local staging state without doing network I/O in the request."""

    _require_local(request)
    from quantmaster.runtime.update import update_status

    return update_status()


@router.post("/system/update/activate", status_code=202)
def activate_system_update(request: Request, payload: UpdateActivationRequest) -> dict[str, object]:
    """Start #61's external activation helper for one exact staged SHA."""

    _require_csrf(request)
    from quantmaster.runtime.update import start_activation

    try:
        return start_activation(payload.build_sha)
    except ActivationBlocked as exc:
        context = dict(exc.context)
        context.setdefault("build_sha", payload.build_sha)
        raise OperationProblem(
            409,
            make_problem(
                exc.code,
                severity="warning",
                source="本地稳定更新",
                title="候选版本未激活",
                message=exc.detail,
                action="刷新更新状态；补齐本地 staging 证据后重试。",
                blocking=True,
                can_continue=True,
                **context,
            ),
        ) from exc


@router.get("/settings/free-stockdb")
def free_stockdb_status(request: Request) -> dict:
    _require_local(request)
    from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime

    return free_stockdb_runtime.status()


def _bar_store_row(symbol: str, value: dict[str, Any]) -> tuple[str, str, bool, bool]:
    try:
        quality = json.loads(str(value.get("quality_json") or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        quality = {}
    if not isinstance(quality, dict):
        quality = {}
    status = str(value.get("last_status") or "")
    actual = str(value.get("end") or quality.get("observed_end") or "")[:10]
    stale = bool(quality.get("stale")) or status in {"stale", "refresh_failed"}
    partial = bool(quality.get("partial")) or status in {"degraded", "partial"}
    return str(symbol), actual, stale, partial


def _bar_store_audit_summary(
    expected_session: str,
    metadata: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    rows = [_bar_store_row(symbol, value) for symbol, value in metadata.items()]
    latest = max((row[1] for row in rows if row[1]), default="")
    missing = [row[0] for row in rows if expected_session and row[1] < expected_session]
    stale_symbols = [row[0] for row in rows if row[2]]
    partial_symbols = [row[0] for row in rows if row[3]]
    total = len(rows)
    covered = sum(bool(expected_session and row[1] >= expected_session) for row in rows)
    usable = sum(
        bool(expected_session and row[1] >= expected_session and not row[2] and not row[3])
        for row in rows
    )
    if not rows:
        state = "unavailable"
    elif not expected_session:
        state = "unverified"
    elif stale_symbols:
        state = "stale"
    elif partial_symbols or missing:
        state = "partial"
    else:
        state = "verified"
    issues: list[str] = []
    if not expected_session:
        issues.append("运行时尚无 validated_session；无法把 BarStore 覆盖与 StockDB 会话对齐")
    if stale_symbols or partial_symbols or missing:
        issues.append(
            "BarStore 逐标的覆盖未与 validated_session 对齐；"
            "请按标的重试行情刷新后再进行正式撮合"
        )
    return {
        "status": state,
        "expected_session": expected_session,
        "latest_session": latest,
        "symbol_count": total,
        "session_coverage_ratio": round(covered / total, 6) if expected_session and total else None,
        "usable_session_coverage_ratio": round(usable / total, 6) if expected_session and total else None,
        "stale_count": len(stale_symbols),
        "partial_count": len(partial_symbols),
        "missing_session_count": len(missing),
        "stale_symbols": stale_symbols[:50],
        "partial_symbols": partial_symbols[:50],
        "missing_session_symbols": missing[:50],
        "issues": issues,
    }


def _bar_store_audit(expected_session: str) -> dict[str, Any]:
    empty = _bar_store_audit_summary(expected_session, {})
    empty["issues"] = ["BarStore 元数据不可用；无法证明逐标的行情覆盖"]
    try:
        from quantmaster.data.storage import BarStore

        return _bar_store_audit_summary(
            expected_session,
            BarStore(read_only=True).metadata_many(),
        )
    except (OSError, RuntimeError, sqlite3.Error, TypeError, ValueError):
        logger.info("无法读取 BarStore 元数据用于 StockDB 审计", exc_info=True)
        return empty


@router.get("/data-sources/free-stockdb/audit")
def free_stockdb_audit(request: Request) -> dict[str, Any]:
    """Project published StockDB evidence; never probe from a page GET."""

    _require_local(request)
    from collections import Counter

    from quantmaster.data.free_stockdb_contracts import StockDBArtifactIdentity
    from quantmaster.data.free_stockdb_experimental import StockDBExperimentalOnline
    from quantmaster.data.free_stockdb_ingest import StockDBIngestStore
    from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime
    from quantmaster.data.free_stockdb_source import resolve_free_stockdb_sdk_path
    root = Path(get_config().data.free_stockdb_root).expanduser().resolve()
    mounts = (
        [
            {"name": item.name, "path": str(item), "automatic_union": False}
            for item in sorted(root.glob("data*"))
            if item.is_dir()
        ]
        if root.is_dir()
        else []
    )
    store = StockDBIngestStore()
    ingests = store.history(1)
    latest_ingest = ingests[0] if ingests else None
    catalog: list[dict[str, Any]] = []
    boards: list[dict[str, Any]] = []
    delisted: list[dict[str, Any]] = []
    if latest_ingest is not None:
        # A damaged artifact remains a local degraded result.  A diagnostic
        # page may not fall through to a live provider to replace it.
        try:
            raw_catalog = store.load_json(latest_ingest, "catalog")
            raw_boards = store.load_json(latest_ingest, "boards")
            raw_delisted = store.load_json(latest_ingest, "delisted")
            catalog = raw_catalog if isinstance(raw_catalog, list) else []
            boards = raw_boards if isinstance(raw_boards, list) else []
            delisted = raw_delisted if isinstance(raw_delisted, list) else []
        except (OSError, TypeError, ValueError):
            pass
    runtime = free_stockdb_runtime.status()
    validated_session = str(runtime.get("validated_session") or "")[:10]
    bar_store = _bar_store_audit(validated_session)
    artifact = StockDBArtifactIdentity.discover(
        resolve_free_stockdb_sdk_path(),
        root,
        data_session=str(runtime.get("validated_session") or ""),
    )
    from quantmaster.data.free_stockdb_compatibility import StockDBCompatibilityStore

    compatibility = StockDBCompatibilityStore(read_only=True).get(artifact.artifact_id)
    levels = Counter(str(item.get("level") or "OTHER") for item in boards)
    issues: list[str] = []
    if latest_ingest is None:
        issues.append("尚无已发布的本地 StockDB 摄取快照；请提交刷新任务")
    elif not (catalog or boards):
        issues.append("已发布摄取的目录产物不可读；保留旧快照并等待后台重建")
    if latest_ingest is not None and latest_ingest.status != "complete":
        issues.extend(str(issue) for issue in latest_ingest.issues)
    issues.extend(str(issue) for issue in bar_store.get("issues") or ())
    probe = {
        "status": "not_run_in_request",
        "cached": False,
        "message": "页面读取不会连接或探测 StockDB；请查看后台运行时验收状态",
    }
    local_state = "locally_validated" if latest_ingest is not None else "unavailable"

    def capability(
        state: str,
        assets: list[str],
        frequencies: list[str],
        *,
        verified: bool = False,
    ) -> dict[str, Any]:
        return {
            "state": state,
            "installed": bool(artifact.sdk.get("available")),
            # The audit endpoint intentionally does not probe StockDB.  False
            # would claim a failed connection; None means connection state is
            # unknown until the supervised runtime records a probe.
            "connected": None,
            "data_ready": bool(latest_ingest),
            "verified": verified,
            "asset_classes": assets,
            "frequencies": frequencies,
            "coverage": latest_ingest.coverage if latest_ingest else None,
            "as_of_date": latest_ingest.as_of_date if latest_ingest else "",
        }

    capabilities = {
        "daily_bars": capability(
            local_state,
            ["stock", "etf"],
            ["1d"],
        ),
        "daily_cross_section": capability(
            local_state,
            ["stock", "etf"],
            ["1d"],
        ),
        "intraday_bars": capability(
            local_state,
            ["stock", "etf"],
            ["1m", "5m", "15m", "30m", "60m"],
        ),
        "eod_snapshot": capability(
            local_state,
            ["stock", "etf"],
            ["1d"],
        ),
        "realtime_tick": capability(
            "experimental" if get_config().data.free_stockdb_experimental_tick_enabled else "disabled",
            ["stock"],
            ["tick"],
        ),
        "security_catalog": capability(
            local_state if catalog else "unavailable",
            ["stock", "etf", "fund"],
            ["snapshot"],
        ),
        "board_hierarchy": capability(
            local_state if boards else "unavailable",
            ["stock"],
            ["snapshot"],
        ),
        "etf_shares": capability(
            "semantic_lag_disclosed" if latest_ingest else "unavailable",
            ["etf"],
            ["1d"],
        ),
        "native_indicators": capability(
            (
                "verified"
                if compatibility and compatibility.status == "compatible"
                else "partially_verified"
                if compatibility and compatibility.status == "partial"
                else "unverified"
                if latest_ingest
                else "unavailable"
            ),
            ["stock", "etf"],
            ["1d"],
            verified=bool(compatibility and compatibility.status in {"compatible", "partial"}),
        ),
    }
    return {
        "status": "degraded" if latest_ingest else "unavailable",
        "upstream": "vendor-declared-unverified",
        "upstream_evidence": "not_provided",
        "distribution": "free-stockdb",
        "independent_cross_validation": False,
        "runtime": runtime,
        "probe": probe,
        "artifact": artifact.to_dict(),
        "compatibility": compatibility.to_dict() if compatibility else None,
        "compatibility_artifact_id": artifact.artifact_id,
        "native_acceleration_enabled": get_config().data.free_stockdb_native_acceleration_enabled,
        "capabilities": capabilities,
        "catalog": {"securities": len(catalog), "delisted_records": len(delisted)},
        "boards": {"total": len(boards), "levels": dict(sorted(levels.items()))},
        "mounts": mounts,
        "mount_policy": "diagnostic_only_no_automatic_union",
        "latest_ingest": ingests[0].to_dict() if ingests else None,
        "experimental_online": StockDBExperimentalOnline().status(),
        "issues": issues,
        "bar_store": bar_store,
    }


@router.get("/data-sources/free-stockdb/ingests")
def free_stockdb_ingests(
    request: Request,
    limit: int = 50,
) -> dict[str, Any]:
    _require_local(request)
    from quantmaster.data.free_stockdb_ingest import StockDBIngestStore

    limit = max(1, min(int(limit), 365))
    return {"items": [item.to_dict() for item in StockDBIngestStore().history(limit)]}


@router.get("/data-sources/free-stockdb/ingests/{ingest_id}")
def free_stockdb_ingest_detail(ingest_id: str, request: Request) -> dict[str, Any]:
    _require_local(request)
    from quantmaster.data.free_stockdb_ingest import StockDBIngestStore

    store = StockDBIngestStore()
    snapshot = store.get(ingest_id)
    if snapshot is None:
        raise HTTPException(404, "free-stockdb 摄取不存在或已损坏")
    references = store.references(ingest_id)
    return {
        "ingest": snapshot.to_dict(),
        "references": references,
        "protected": bool(references),
        "content_ready": {
            name: (store.content / f"{digest}.parquet").is_file()
            or (store.content / f"{digest}.json").is_file()
            for name, digest in snapshot.content_hashes.items()
        },
    }


@router.post("/data-sources/free-stockdb/experimental/tick")
def free_stockdb_experimental_tick(
    body: StockDBTickRequest,
    request: Request,
) -> dict[str, Any]:
    _require_csrf(request)
    from quantmaster.data.free_stockdb_experimental import StockDBExperimentalOnline

    try:
        return StockDBExperimentalOnline().tick(body.symbol, count=body.count)
    except PermissionError as exc:
        raise OperationProblem(
            403,
            make_problem(
                "permission_denied", source="本地服务", title="当前操作没有权限",
                message="当前配置不允许执行此操作。", action="检查本机设置或权限后重试。",
                blocking=True, retryable=False,
            ),
        ) from exc
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        raise HTTPException(422, str(exc)) from None


@router.post("/data-sources/free-stockdb/experimental/fundamentals")
def free_stockdb_experimental_fundamentals(
    body: StockDBFundamentalsRequest,
    request: Request,
) -> dict[str, Any]:
    _require_csrf(request)
    from quantmaster.data.free_stockdb_experimental import StockDBExperimentalOnline

    try:
        return StockDBExperimentalOnline().fundamentals(
            body.symbol,
            dataset=body.dataset,
            stat_date=body.stat_date,
        )
    except PermissionError as exc:
        raise OperationProblem(
            403,
            make_problem(
                "permission_denied", source="本地服务", title="当前操作没有权限",
                message="当前配置不允许执行此操作。", action="检查本机设置或权限后重试。",
                blocking=True, retryable=False,
            ),
        ) from exc
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        raise HTTPException(422, str(exc)) from None


@router.get("/settings/free-stockdb/vendor-notice")
def free_stockdb_vendor_notice(request: Request) -> dict:
    _require_local(request)
    from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime

    return free_stockdb_runtime.cached_vendor_notice()


@router.post("/settings/free-stockdb/update")
def update_free_stockdb(request: Request, response: Response) -> dict:
    _require_csrf(request)
    from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime

    accepted = free_stockdb_runtime.request_update("manual")
    response.status_code = 202 if accepted else 200
    return {"accepted": accepted, **free_stockdb_runtime.status()}


@router.post("/settings/free-stockdb/reset")
def reset_free_stockdb_retry(request: Request, response: Response) -> dict:
    _require_csrf(request)
    from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime

    result = free_stockdb_runtime.request_reset_service_retry()
    response.status_code = 202 if result.get("status") == "queued" else 200
    return result


@router.post("/settings/validate")
def validate_settings(request: Request, document: SettingsDocument) -> dict:
    _require_csrf(request)
    return settings_manager.validate(document)


@router.put("/settings")
def save_settings(request: Request, update: SettingsUpdate) -> dict:
    _require_csrf(request)
    try:
        result = settings_manager.save(update)
        from quantmaster.server.settings_runtime import report_component
        result["llm_cancellation"] = _llm_cancellation_after_save(
            result,
            llm_secret_changed=update.secrets.llm.action in {"replace", "clear"},
        )
        from quantmaster.server.settings_runtime import begin_apply

        result["generation"] = begin_apply(
            settings_manager.path, int(result.get("config_revision") or 0),
        )
        try:
            result["runtime_apply"] = _queue_runtime_apply(result)
        except (OSError, RuntimeError, ValueError) as exc:
            logger.warning("设置已保存但后台应用通知失败", exc_info=True)
            result["runtime_apply"] = {
                "status": "failed", "detail": "设置已保存，但后台应用通知失败",
                "diagnostic_code": "settings_apply_dispatch_failed",
                "recommendation": "请重试应用或重启服务",
            }
            result.setdefault("warnings", []).append(str(exc)[:200])
        report_component(
            settings_manager.path, "web",
            revision=int(result.get("config_revision") or 0),
            generation=int(result.get("generation") or 0),
            status="effective",
        )
        result["runtime"] = _runtime_status()
    except CredentialError as exc:
        raise HTTPException(409, str(exc)) from None
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from None
    return {**result, "settings": settings_manager.public()}


def _check_document(body: dict[str, Any]) -> tuple[SettingsDocument, SecretMutations]:
    settings_value = body.get("settings")
    source: dict[str, Any] = settings_value if isinstance(settings_value, dict) else body
    clean = {
        key: source[key]
        for key in ("config_version", "llm", "data", "trade", "news", "server", "automation", "lab")
        if key in source
    }
    if clean:
        document = SettingsDocument.model_validate(clean)
    else:
        document = SettingsDocument.model_validate(
            {key: settings_manager.public()[key] for key in SettingsDocument.model_fields}
        )
    secrets_value = body.get("secrets") or source.get("secrets") or {}
    return document, SecretMutations.model_validate(secrets_value)


def _diagnostic_secrets(
    document: SettingsDocument, mutations: SecretMutations,
) -> tuple[str, str, str]:
    current = settings_manager.load()
    same_llm_target = (
        document.llm.provider == current.llm.provider
        and document.llm.base_url.rstrip("/") == current.llm.base_url.rstrip("/")
    )
    llm_secret = {
        "replace": mutations.llm.value or "",
        "clear": "",
        "keep": current.llm.api_key if same_llm_target else "",
    }[mutations.llm.action]
    tushare_secret = {
        "replace": mutations.tushare.value or "",
        "clear": "",
        "keep": current.data.tushare_token,
    }[mutations.tushare.action]
    xiaoshi_secret = {
        "replace": mutations.xiaoshi.value or "",
        "clear": "",
        "keep": current.data.xiaoshi_api_key,
    }[mutations.xiaoshi.action]
    return llm_secret, tushare_secret, xiaoshi_secret


@router.post("/settings/check/{kind}")
def check_setting(
    kind: Literal[
        "llm-models", "llm-web-search", "tushare", "xiaoshi", "storage",
        "data-sources", "server", "lab",
    ],
    request: Request,
    response: Response,
    body: Annotated[dict[str, Any] | None, Body()] = None,
) -> dict:
    _require_csrf(request)
    from quantmaster.server.settings_checks import (
        check_data_sources,
        check_lab,
        check_server,
        check_storage,
        check_tushare,
        check_xiaoshi,
    )

    document, mutations = _check_document(body or {})
    llm_secret, tushare_secret, xiaoshi_secret = _diagnostic_secrets(document, mutations)
    if kind in {"llm-models", "llm-web-search"}:
        from quantmaster.server.settings_jobs import get_settings_jobs

        try:
            jobs = get_settings_jobs()
            task, _created = jobs.submit_diagnostic(kind, document, api_key=llm_secret)
        except CredentialError as exc:
            raise HTTPException(409, str(exc)) from None
        response.status_code = 202
        return jobs.public(task)
    if kind == "tushare":
        result = check_tushare(tushare_secret)
    elif kind == "xiaoshi":
        result = check_xiaoshi(xiaoshi_secret)
    elif kind == "storage":
        result = check_storage(document.data)
    elif kind == "data-sources":
        result = check_data_sources(document.llm.timeout, document.data)
    elif kind == "lab":
        result = check_lab(document.lab, document.data, tushare_secret)
    else:
        result = check_server(document.server)
    return settings_manager.record_check_result(
        kind,
        document,
        {"llm": llm_secret, "tushare": tushare_secret, "xiaoshi": xiaoshi_secret},
        result,
    )


class SnapshotCreate(ContractModel):
    name: str = Field(min_length=1, max_length=80)


@router.get("/settings/snapshots")
def list_snapshots(request: Request) -> dict:
    _require_local(request)
    return {"snapshots": settings_manager.list_snapshots()}


@router.post("/settings/snapshots")
def create_snapshot(request: Request, value: SnapshotCreate) -> dict:
    _require_csrf(request)
    try:
        return settings_manager.create_named_snapshot(value.name)
    except ValueError:
        raise _public_error(400, "设置快照名称或状态无效", "创建设置快照失败") from None


@router.get("/settings/snapshots/{snapshot_id}/diff")
def snapshot_diff(snapshot_id: str, request: Request) -> dict:
    _require_local(request)
    try:
        return {"diff": settings_manager.snapshot_diff(snapshot_id)}
    except (ValueError, FileNotFoundError):
        raise _public_error(404, "设置快照不存在或不可读取", "读取设置快照差异失败") from None


@router.post("/settings/snapshots/{snapshot_id}/rollback")
def rollback_snapshot(snapshot_id: str, request: Request) -> dict:
    _require_csrf(request)
    try:
        result = settings_manager.rollback(snapshot_id)
        result["llm_cancellation"] = _llm_cancellation_after_save(result)
        result["runtime_apply"] = _queue_runtime_apply(result)
        result["runtime"] = _runtime_status()
        return result
    except FileNotFoundError:
        raise _public_error(404, "设置快照不存在", "回滚设置快照失败") from None
    except ValueError:
        raise _public_error(400, "设置快照无效，无法回滚", "回滚设置快照失败") from None


@router.delete("/settings/snapshots/{snapshot_id}")
def delete_snapshot(snapshot_id: str, request: Request) -> dict:
    _require_csrf(request)
    try:
        settings_manager.delete_snapshot(snapshot_id)
        return {"status": "ok"}
    except FileNotFoundError:
        raise _public_error(404, "设置快照不存在", "删除设置快照失败") from None
    except ValueError:
        raise _public_error(400, "设置快照无效，无法删除", "删除设置快照失败") from None


class MigrationCreate(ContractModel):
    target: str = Field(min_length=1, max_length=4096)
    mode: Literal["copy", "switch"] = "copy"


class LegacyMigrationCreate(ContractModel):
    domain: str = Field(min_length=1, max_length=80)
    mode: Literal["dry_run"] = "dry_run"
    batch_size: int = Field(default=250, ge=1, le=5000)


@router.post("/data/migrations")
def create_migration(request: Request, value: MigrationCreate) -> dict:
    _require_csrf(request)
    try:
        return migration_manager.create(value.target, value.mode)
    except (TimeoutError, RuntimeError):
        raise _public_error(409, "已有数据迁移占用资源，请稍后重试", "创建数据迁移失败") from None
    except MigrationError:
        raise _public_error(400, "数据迁移参数或目标无效", "创建数据迁移失败") from None


@router.get("/data/migrations/{task_id}")
def get_migration(task_id: str, request: Request) -> dict:
    _require_local(request)
    try:
        task = migration_manager.get(task_id)
        if task.get("status") == "completed" and task_id not in _applied_migrations:
            _applied_migrations.add(task_id)
            # A migration status read is still a request-plane operation.  It
            # may trigger a runtime reconfiguration, but must never wait for
            # automation/Lab workers to stop or restart.
            task["apply"] = _queue_runtime_apply(
                {
                    "status": "ok",
                    "changed_fields": ["data.root"],
                    "restart_required": [],
                    "warnings": [],
                }
            )
        return task
    except KeyError:
        raise _public_error(404, "数据迁移任务不存在", "读取数据迁移任务失败") from None


@router.post("/data/migrations/{task_id}/cancel")
def cancel_migration(task_id: str, request: Request) -> dict:
    _require_csrf(request)
    try:
        return migration_manager.cancel(task_id)
    except KeyError:
        raise _public_error(404, "数据迁移任务不存在", "取消数据迁移任务失败") from None


@router.get("/data/contract-migrations")
def contract_migration_status(request: Request) -> dict:
    _require_local(request)
    return {
        "available_types": list(registered_migrations()),
        "latest": legacy_migration_manager.latest(),
    }


@router.post("/data/contract-migrations", status_code=202)
def create_contract_migration(request: Request, value: LegacyMigrationCreate) -> dict:
    _require_csrf(request)
    try:
        return legacy_migration_manager.create(
            value.domain, mode="dry_run", batch_size=value.batch_size,
        )
    except LegacyMigrationError:
        raise _public_error(
            409, "历史合同迁移当前无法启动", "创建历史合同迁移失败",
        ) from None


@router.get("/data/contract-migrations/{run_id}")
def get_contract_migration(run_id: str, request: Request) -> dict:
    _require_local(request)
    try:
        return legacy_migration_manager.get(run_id)
    except KeyError:
        raise _public_error(404, "历史合同迁移不存在", "读取历史合同迁移失败") from None


@router.post("/data/contract-migrations/{run_id}/pause")
def pause_contract_migration(run_id: str, request: Request) -> dict:
    _require_csrf(request)
    try:
        return legacy_migration_manager.pause(run_id)
    except KeyError:
        raise _public_error(404, "历史合同迁移不存在", "暂停历史合同迁移失败") from None
    except LegacyMigrationError:
        raise _public_error(409, "历史合同迁移当前无法暂停", "暂停历史合同迁移失败") from None


@router.post("/data/contract-migrations/{run_id}/resume", status_code=202)
def resume_contract_migration(run_id: str, request: Request) -> dict:
    _require_csrf(request)
    raise _public_error(409, "apply 续跑仅允许离线维护命令", "续跑历史合同迁移失败")


@router.post("/data/contract-migrations/{run_id}/rollback", status_code=202)
def rollback_contract_migration(run_id: str, request: Request) -> dict:
    _require_csrf(request)
    raise _public_error(409, "回滚仅允许离线维护命令", "回滚历史合同迁移失败")


class DataRefreshRequest(ContractModel):
    scope: Literal["market", "universe", "all_cached"] = "market"
    universe: str = Field(default="", max_length=80)
    start: str = Field(default="", max_length=10)


def _data_job_envelope(value: dict[str, Any], worker: dict[str, Any]) -> dict[str, Any]:
    """Return a newly submitted data refresh in the unified task shape."""

    result = dict(value)
    job_id = str(result.get("id") or "")
    result.update({
        "domain": "data",
        "type": "data.refresh",
        "worker": worker,
        "links": {
            "self": f"/api/v1/jobs/{job_id}",
            "events": f"/api/v1/jobs/{job_id}/events",
            "cancel": f"/api/v1/jobs/{job_id}/cancel",
            "retry": f"/api/v1/jobs/{job_id}/retry",
        },
    })
    return result


@router.post("/data/refresh/preview")
def preview_data_refresh(request: Request, value: DataRefreshRequest) -> dict:
    _require_csrf(request)
    _worker, result = _data_refresh_worker_command(
        "data.refresh.preview",
        {"scope": value.scope, "universe": value.universe, "start": value.start},
    )
    return result


@router.post("/data/refresh", status_code=202)
def create_data_refresh(request: Request, value: DataRefreshRequest) -> dict:
    _require_csrf(request)
    worker, result = _data_refresh_worker_command(
        "data.refresh.create",
        {"scope": value.scope, "universe": value.universe, "start": value.start},
    )
    return _data_job_envelope(result, worker)


class UniverseBody(ContractModel):
    name: str | None = None
    symbols: list[str] = Field(default_factory=list, max_length=10_000)


class UniverseRename(ContractModel):
    new_name: str


class UniversePreview(ContractModel):
    kind: Literal["manual", "index"] = "manual"
    symbols: list[str] = Field(default_factory=list, max_length=10_000)
    index_symbol: str = "000300.SH"
    selections: dict[str, str] = Field(default_factory=dict)


class UniverseNameRefresh(ContractModel):
    symbols: list[str] = Field(default_factory=list, max_length=10_000)


class InstrumentResolveBody(ContractModel):
    queries: list[str] = Field(default_factory=list, max_length=10_000)
    selections: dict[str, str] = Field(default_factory=dict)


def _universe_references(name: str) -> list[dict[str, str]]:
    cfg = settings_manager.load()
    references = []
    if cfg.automation.primary_universe.casefold() == name.casefold():
        references.append(
            {
                "key": "automation.primary_universe",
                "label": "自动化主候选",
            }
        )
    if cfg.lab.universe.casefold() == name.casefold():
        references.append(
            {
                "key": "lab.universe",
                "label": "Quant Lab 默认候选",
            }
        )
    return references


def _fixed_universe_metadata(item: dict) -> dict:
    built_in = item["name"].casefold() == "demo"
    return {
        **item,
        "kind": "fixed",
        "source": "built_in" if built_in else "custom",
        "research_quality": "sandbox",
        "references": _universe_references(item["name"]),
    }


def _universe_members(symbols: list[str]) -> list[dict[str, Any]]:
    from quantmaster.data.instruments import InstrumentStore

    store = InstrumentStore(read_only=True)
    instruments = store.get_many(symbols)
    result = []
    for symbol in symbols:
        instrument = instruments.get(str(symbol).strip().upper())
        result.append(
            {
                "symbol": symbol,
                "name": instrument.name if instrument else None,
                "market": instrument.market if instrument else None,
                "exchange": instrument.exchange if instrument else None,
                "asset_type": instrument.asset_type if instrument else None,
                "status": instrument.status if instrument else None,
                "source": instrument.source if instrument else None,
            }
        )
    return result


@router.get("/market/instruments/search")
def instrument_search(
    request: Request,
    q: str = "",
    limit: int = 20,
) -> dict:
    _require_local(request)
    from quantmaster.data.instruments import search_instruments

    return {"query": q, "items": search_instruments(q, limit=limit, read_only=True)}


@router.post("/market/instruments/resolve")
def instrument_resolve(request: Request, value: InstrumentResolveBody) -> dict:
    _require_csrf(request)
    from quantmaster.data.instruments import resolve_instruments

    return resolve_instruments(
        value.queries, selections=value.selections, read_only=True,
    )


def _rewrite_universe_references(old_name: str, new_name: str) -> tuple[list[str], dict | None]:
    document = document_from_config(settings_manager.load())
    changed: list[str] = []
    if document.automation.primary_universe.casefold() == old_name.casefold():
        document.automation.primary_universe = new_name
        changed.append("automation.primary_universe")
    if document.lab.universe.casefold() == old_name.casefold():
        document.lab.universe = new_name
        changed.append("lab.universe")
    if not changed:
        return [], None
    update = SettingsUpdate.model_validate(document.model_dump())
    saved = settings_manager.save(update)
    saved["llm_cancellation"] = _llm_cancellation_after_save(saved)
    saved["runtime_apply"] = _queue_runtime_apply(saved)
    saved["runtime"] = _runtime_status()
    return changed, saved


def _validate_replacement(name: str, references: list[dict[str, str]]) -> str:
    value = str(name).strip()
    if not value:
        raise ValueError("请选择替代候选")
    if value.casefold() == "csi800":
        if any(item["key"] == "automation.primary_universe" for item in references):
            raise ValueError("csi800 只适用于 Quant Lab，不能替代自动化主候选")
        return "csi800"
    from quantmaster.data.universe import load_universe_analysis

    load_universe_analysis(value)
    return value


@router.get("/settings/universes")
def universes(request: Request) -> dict:
    _require_local(request)
    from quantmaster.data.universe import INDEX_UNIVERSE_PRESETS, list_universes

    fixed = [_fixed_universe_metadata(item) for item in list_universes()]
    dynamic = {
        "name": "csi800",
        "count": None,
        "readonly": True,
        "kind": "dynamic",
        "source": "tushare:index_weight",
        "research_quality": "production",
        "references": _universe_references("csi800"),
    }
    data_root = Path(settings_manager.load().data.root).expanduser().resolve()
    conflicts = []
    conflict = data_root / "universe" / "csi800.json"
    if conflict.is_file():
        conflicts.append(
            {
                "name": "csi800",
                "path": str(conflict),
                "message": "检测到与系统动态候选同名的旧文件；文件已保留，请先改名再使用。",
            }
        )
    ordered = [fixed[0], dynamic, *fixed[1:]] if fixed else [dynamic]
    return {
        "universes": ordered,
        "index_presets": [dict(item) for item in INDEX_UNIVERSE_PRESETS],
        "conflicts": conflicts,
    }


@router.get("/settings/universes/{name}")
def universe_detail(name: str, request: Request, as_of: date | None = None) -> dict:
    _require_local(request)
    from quantmaster.data.universe import load_universe_analysis_snapshot

    try:
        if name.casefold() == "csi800":
            chosen = as_of or market_date()
            if chosen > market_date():
                raise ValueError("查看日期不能晚于今天")
            from quantmaster.lab.dataset import load_csi800_members_as_of

            dynamic = load_csi800_members_as_of(chosen.isoformat())
            symbols = dynamic["symbols"]
            return {
                "name": "csi800",
                "symbols": symbols,
                "members": _universe_members(symbols),
                "count": len(symbols),
                "readonly": True,
                "kind": "dynamic",
                "source": "tushare:index_weight",
                "research_quality": "production",
                "as_of": dynamic["as_of"],
                "snapshot_dates": dynamic["snapshot_dates"],
                "references": _universe_references("csi800"),
            }
        snapshot = load_universe_analysis_snapshot(
            name, as_of=as_of.isoformat() if as_of else None,
        )
        symbols = list(snapshot.symbols)
        built_in = name.casefold() == "demo"
        return {
            "name": "demo" if built_in else name,
            "symbols": symbols,
            "members": _universe_members(symbols),
            "count": len(symbols),
            "readonly": built_in,
            "kind": "fixed",
            "source": snapshot.source,
            "research_quality": "sandbox",
            "formal_eligible": snapshot.formal_eligible,
            "issues": list(snapshot.issues),
            "evidence": snapshot.to_dict(),
            "references": _universe_references(name),
        }
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from None
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(400, str(exc)) from None


@router.post("/settings/universes/preview")
def preview_universe(request: Request, value: UniversePreview) -> dict:
    _require_csrf(request)
    from quantmaster.data.instruments import resolve_instruments
    from quantmaster.data.universe import index_universe

    try:
        symbols = index_universe(value.index_symbol) if value.kind == "index" else value.symbols
        resolution = resolve_instruments(symbols, selections=value.selections)
        normalized = [item["instrument"]["symbol"] for item in resolution["resolved"]]
        errors = [{"value": item["query"], "message": item["message"]} for item in resolution["unresolved"]]
        return {
            "symbols": normalized,
            "members": _universe_members(normalized),
            "count": len(normalized),
            "preview": normalized[:100],
            "duplicates": resolution["duplicates"],
            "errors": errors,
            "ambiguous": resolution["ambiguous"],
            "unresolved": resolution["unresolved"],
            "corrections": resolution["corrections"],
        }
    except Exception as exc:
        raise HTTPException(400, str(exc)) from None


@router.post("/settings/universes/names/refresh")
def refresh_universe_names(request: Request, value: UniverseNameRefresh) -> dict:
    _require_csrf(request)
    from quantmaster.data.names import refresh_stock_names_if_needed
    from quantmaster.data.universe import normalize_symbols

    try:
        symbols = normalize_symbols(value.symbols)
        names = refresh_stock_names_if_needed(symbols)
        return {
            "names": names,
            "missing": [symbol for symbol in symbols if symbol not in names],
        }
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from None


@router.post("/settings/instruments/refresh")
def refresh_instruments(request: Request) -> dict:
    """Refresh external security catalogs only after an explicit local request."""
    _require_csrf(request)
    from quantmaster.data.instruments import refresh_instrument_master

    return refresh_instrument_master(force=True)


@router.post("/settings/universes")
def create_universe(request: Request, value: UniverseBody) -> dict:
    _require_csrf(request)
    if not value.name:
        raise HTTPException(400, "缺少候选名称")
    from quantmaster.data.universe import load_universe_analysis, save_universe

    try:
        try:
            load_universe_analysis(value.name)
        except FileNotFoundError:
            pass
        else:
            raise ValueError("候选已存在")
        _validate_universe_instruments(value.symbols)
        save_universe(value.name, value.symbols)
        return {"status": "ok", "name": value.name}
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from None


@router.put("/settings/universes/{name}")
def update_universe(name: str, request: Request, value: UniverseBody) -> dict:
    _require_csrf(request)
    from quantmaster.data.universe import load_universe_analysis, save_universe

    try:
        load_universe_analysis(name)
        _validate_universe_instruments(value.symbols)
        save_universe(name, value.symbols)
        return {"status": "ok", "name": name}
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from None
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from None


def _validate_universe_instruments(symbols: list[str]) -> None:
    """拒绝未知、歧义和没有可验证日线能力的标的。"""
    from quantmaster.data.instruments import resolve_instruments, validate_bar_capability
    from quantmaster.market_capabilities import (
        MarketCapability,
        require_market_capability,
    )

    resolution = resolve_instruments(symbols)
    if resolution["ambiguous"] or resolution["unresolved"]:
        detail = {
            "message": "候选包含尚未确认的证券",
            "ambiguous": resolution["ambiguous"],
            "unresolved": resolution["unresolved"],
        }
        raise HTTPException(422, detail)
    for item in resolution["resolved"]:
        try:
            require_market_capability(item["instrument"], MarketCapability.CANDIDATE)
            validate_bar_capability(item["instrument"]["symbol"], verify_foreign=True)
        except ValueError as exc:
            raise HTTPException(
                422,
                {
                    "message": str(exc),
                    "symbol": item["instrument"]["symbol"],
                },
            ) from None


@router.post("/settings/universes/{name}/rename")
def rename_universe_route(name: str, request: Request, value: UniverseRename) -> dict:
    _require_csrf(request)
    from quantmaster.data.universe import rename_universe

    renamed = False
    try:
        rename_universe(name, value.new_name)
        renamed = True
        changed, runtime = _rewrite_universe_references(name, value.new_name)
        return {
            "status": "ok",
            "name": value.new_name,
            "updated_references": changed,
            "runtime": runtime.get("runtime") if runtime else None,
        }
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from None
    except (ValueError, FileExistsError) as exc:
        if renamed:
            try:
                rename_universe(value.new_name, name)
            except Exception:
                pass
        raise HTTPException(400, str(exc)) from None


@router.delete("/settings/universes/{name}")
def delete_universe_route(name: str, request: Request, replacement: str | None = None) -> dict:
    _require_csrf(request)
    from quantmaster.data.universe import delete_universe

    try:
        if name.casefold() in {"demo", "csi800"}:
            raise ValueError("系统候选只读，请复制后再编辑")
        references = _universe_references(name)
        changed: list[str] = []
        runtime = None
        replacement_name = None
        if references:
            if not replacement:
                raise HTTPException(
                    409,
                    detail={
                        "message": "该候选正在使用中，请先选择替代候选。",
                        "references": references,
                        "requires_replacement": True,
                    },
                )
            replacement_name = _validate_replacement(replacement, references)
            if replacement_name.casefold() == name.casefold():
                raise ValueError("替代候选不能与待删除候选相同")
            changed, runtime = _rewrite_universe_references(name, replacement_name)
        delete_universe(name)
        return {
            "status": "ok",
            "replacement": replacement_name,
            "updated_references": changed,
            "runtime": runtime.get("runtime") if runtime else None,
        }
    except HTTPException:
        raise
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from None
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from None


async def _upload_bytes(file: UploadFile) -> bytes:
    content = await file.read(20 * 1024 * 1024 + 1)
    if len(content) > 20 * 1024 * 1024:
        raise HTTPException(413, "CSV 文件超过 20MB 限制")
    return content


@router.post("/portfolio/ledger/import/preview")
async def preview_ledger_csv(
    request: Request,
    file: Annotated[UploadFile, File()],
    mapping: Annotated[str | None, Form()] = None,
) -> dict:
    _require_csrf(request)
    from quantmaster.portfolio import Ledger
    from quantmaster.portfolio.csv_import import parse_broker_csv

    content = await _upload_bytes(file)
    ledger = Ledger()
    try:
        parsed = parse_broker_csv(content, mapping, ledger.fingerprints())
        return parsed.preview(batch_duplicate=ledger.has_import_hash(parsed.file_hash))
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from None


@router.post("/portfolio/ledger/import/submit")
async def submit_ledger_csv(
    request: Request,
    file: Annotated[UploadFile, File()],
    mapping: Annotated[str | None, Form()] = None,
    strict: Annotated[bool, Form()] = True,
    include_duplicates: Annotated[bool, Form()] = False,
) -> dict:
    _require_csrf(request)
    from quantmaster.portfolio import Ledger
    from quantmaster.portfolio.csv_import import parse_broker_csv

    content = await _upload_bytes(file)
    ledger = Ledger()
    try:
        parsed = parse_broker_csv(content, mapping, ledger.fingerprints())
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from None
    failed = [row.public() for row in parsed.rows if row.errors]
    if strict and failed:
        raise HTTPException(422, {"message": f"严格模式：{len(failed)} 行校验失败", "failed_rows": failed})
    if ledger.has_import_hash(parsed.file_hash) and not include_duplicates:
        raise HTTPException(409, "该文件已导入；如确需再次导入，请显式包含重复记录")
    rows = [row for row in parsed.valid_rows if include_duplicates or not row.duplicate]
    records = [row.record for row in rows if row.record]
    try:
        count = ledger.import_records(
            records, parsed.file_hash, file.filename or "trades.csv", parsed.encoding
        )
    except Exception:
        # 不返回数据库内部信息，也不把任何原始行写进日志。
        raise HTTPException(500, "数据库写入失败，全部记录已回滚") from None
    return {
        "status": "ok",
        "imported": count,
        "skipped_invalid": len(failed) if not strict else 0,
        "skipped_duplicates": len(parsed.valid_rows) - len(rows),
        "failed_rows": failed,
    }
