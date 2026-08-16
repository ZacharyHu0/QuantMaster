"""FastAPI 本地服务：JSON API + Web 仪表盘。

启动：qm serve  （或 uvicorn quantmaster.server.app:app）
浏览器访问 http://127.0.0.1:8686
"""

from __future__ import annotations

import logging
import os
import sqlite3
import time
import uuid
from contextlib import asynccontextmanager, nullcontext
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.staticfiles import StaticFiles

from quantmaster import __version__
from quantmaster.config import get_config
from quantmaster.data.base import DataEvidenceNotReady, MarketDataUnavailable
from quantmaster.logging_config import redact_sensitive_text
from quantmaster.runtime.json import StrictJSONResponse as JSONResponse
from quantmaster.runtime.problems import OperationProblem, make_problem
from quantmaster.server.capabilities import _progress_stream, _stream_runtime
from quantmaster.server.capabilities import (
    shutdown_stream_runtime as _shutdown_web_stream_executor,
)

logger = logging.getLogger(__name__)
_web_stream_runtime = None


def _configure_reload_worker_logging() -> bool:
    """Restore CLI logging in workers spawned directly by Uvicorn's reloader."""
    if os.environ.get("QM_SERVER_RELOAD_WORKER") != "1":
        return False
    from quantmaster.logging_config import configure_logging

    configure_logging(verbose=os.environ.get("QM_SERVER_RELOAD_VERBOSE") == "1")
    return True


def create_lifespan(*, bootstrap_rotation: bool):
    """Build the Web lifespan with explicit ownership of rotation bootstrap."""

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        _stream_runtime()
        from quantmaster.bootstrap import get_runtime_worker, get_worker_supervisor
        from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime
        from quantmaster.logging_config import current_log_path
        from quantmaster.server.management import capture_runtime_baseline
        from quantmaster.server.worker_components import register_worker_components

        register_worker_components()
        capture_runtime_baseline()
        free_stockdb_runtime.start()
        supervisor_state = get_worker_supervisor().start(
            bootstrap_rotation=bootstrap_rotation,
        )
        if supervisor_state == "disabled":
            get_runtime_worker().start(bootstrap_rotation=bootstrap_rotation)
        else:
            os.environ["QM_WEB_PROCESS"] = "1"
        cfg = get_config()
        log_path = current_log_path()
        logger.info(
            "QuantMaster %s 已就绪 · http://%s:%s",
            __version__, cfg.server.host, cfg.server.port,
        )
        logger.info(
            "后台 runtime-worker %s · 完整日志 %s",
            "本地测试/维护回退" if supervisor_state == "disabled" else "独立 Worker Supervisor 托管",
            str(log_path) if log_path else "仅终端",
        )
        try:
            yield
        finally:
            if supervisor_state == "disabled":
                get_runtime_worker().stop()
            else:
                get_worker_supervisor().stop()
            free_stockdb_runtime.stop()
            os.environ.pop("QM_WEB_PROCESS", None)
            _shutdown_web_stream_executor()
            from quantmaster.ai.llm import close_llm_http_clients

            close_llm_http_clients()
            logger.info("QuantMaster 已停止")

    return lifespan


lifespan = create_lifespan(bootstrap_rotation=True)


app = FastAPI(
    title="QuantMaster",
    version=__version__,
    lifespan=lifespan,
    default_response_class=JSONResponse,
)


def _new_request_id() -> str:
    """生成可在前端提示和后端日志间关联的短请求编号。"""
    return uuid.uuid4().hex[:12]


def _request_id(request: Request) -> str:
    return getattr(request.state, "request_id", "") or _new_request_id()


def _safe_client_error(exc: Exception) -> str:
    """保留可操作信息，同时避免第三方异常把凭据回显到浏览器。"""
    message = redact_sensitive_text(str(exc).strip() or "数据任务未完成")
    return message[:297] + "…" if len(message) > 300 else message


def _problem_response(
    request_id: str,
    problem: dict[str, Any],
    *,
    status_code: int,
    detail: object | None = None,
    **extra: Any,
) -> JSONResponse:
    """Return the public, redacted error envelope used by every API failure."""
    payload: dict[str, Any] = {
        "detail": problem["message"] if detail is None else detail,
        "problem": problem,
        "error_id": request_id,
        "request_id": request_id,
        "diagnostic_id": request_id,
        "code": problem["code"],
        "message": problem["message"],
        "retryable": bool(problem.get("retryable", status_code in {429, 502, 503})),
        "suggestion": problem.get("suggestion", problem.get("action", "")),
    }
    if problem.get("field"):
        payload["field"] = problem["field"]
    if problem.get("retry_after") is not None:
        payload["retry_after"] = problem["retry_after"]
    payload.update(extra)
    response = JSONResponse(status_code=status_code, content=jsonable_encoder(payload))
    if problem.get("retry_after") is not None:
        response.headers["Retry-After"] = str(problem["retry_after"])
    return response


def _validation_field(exc: RequestValidationError) -> str | None:
    """Extract a field path without including Pydantic's rejected input."""
    for item in exc.errors():
        location = item.get("loc") or ()
        parts = [str(value) for value in location if value not in {"body", "query", "path"}]
        if parts:
            return ".".join(parts)
    return None


def _http_exception_response(request_id: str, exc: HTTPException) -> JSONResponse:
    """Translate intentional plain HTTP errors into the public problem contract."""
    status = int(exc.status_code)
    code = {
        404: "resource_not_found", 409: "write_conflict",
        422: "request_validation_failed", 503: "service_temporarily_unavailable",
    }.get(status, "request_rejected")
    retryable = status in {429, 502, 503}
    problem = make_problem(
        code, source="本地服务", title="请求未能执行",
        message=_safe_client_error(Exception(str(exc.detail))),
        action="请稍后重试。" if retryable else "请检查请求内容后重试。",
        blocking=True, retryable=retryable, retry_after=2 if status == 503 else None,
    )
    return _problem_response(request_id, problem, status_code=status)


def _logged_bad_request(operation: str) -> HTTPException:
    """记录完整内部异常，但只向客户端返回稳定的操作级错误。"""
    logger.exception("%s失败", operation)
    return HTTPException(400, f"{operation}失败，请检查请求参数后重试。")


@app.exception_handler(RequestValidationError)
async def safe_validation_error(request: Request, exc: RequestValidationError):
    """设置请求的校验错误不回显输入值，防止替换中的密钥进入响应。"""
    request_id = _request_id(request)
    field = _validation_field(exc)
    problem = make_problem(
            "request_validation_failed",
            source="本地服务",
            title="提交内容需要修改",
            message="部分字段缺失或格式不正确。",
            action="按页面提示修改输入后重试。",
            blocking=True,
            field=field,
            retryable=False,
        )
    # Validation diagnostics deliberately omit rejected values and context:
    # source definitions and query strings may contain credentials.
    errors = [
        {key: value for key, value in item.items() if key not in {"input", "ctx"}}
        for item in exc.errors()
    ]
    return _problem_response(request_id, problem, status_code=422, detail=errors)


@app.exception_handler(OperationProblem)
async def operation_problem(request: Request, exc: OperationProblem):
    """向普通请求和前端返回一致、可恢复的问题语义。"""
    return _problem_response(
        _request_id(request), exc.problem, status_code=exc.status_code,
        data_quality=exc.data_quality,
    )


@app.exception_handler(MarketDataUnavailable)
async def market_data_unavailable(request: Request, exc: MarketDataUnavailable):
    """Preserve the complete market-data truth contract across HTTP failures."""
    problem = make_problem(
        "market_data_unavailable",
        source="行情数据",
        title="行情证据不可用",
        message=str(exc),
        action="检查缺失范围、来源与刷新状态后重试；不要据此生成正式决策。",
        blocking=True,
        can_continue=False,
    )
    return _problem_response(
        _request_id(request), problem, status_code=503,
        data_quality=exc.quality.to_dict(), provenance=list(exc.provenance),
    )


@app.exception_handler(DataEvidenceNotReady)
async def evidence_not_ready(request: Request, exc: DataEvidenceNotReady):
    """Formal gates are pure and fail fast; they never start a provider call."""
    problem = make_problem(
        "evidence_not_ready",
        severity="warning",
        source="数据证据",
        title="正式操作缺少合格证据",
        message="；".join(exc.quality.assess_eligibility().reasons)
        or "当前本地快照尚未通过正式验收",
        action="可先查看带标记的本地结果，并通过数据刷新任务补齐证据后再执行正式操作。",
        blocking=True,
        can_continue=False,
    )
    return _problem_response(
        _request_id(request), problem, status_code=409,
        data_quality=exc.quality.to_dict(), eligibility=exc.quality.assess_eligibility().to_dict(),
        provenance=list(exc.provenance), refresh={"status": "available", "resource": "bars"},
    )


@app.middleware("http")
async def request_context_and_migration_lock(request: Request, call_next):
    """Apply the local security boundary, request context and migration lock."""
    from quantmaster.data.migration import migration_manager
    from quantmaster.data.resilience import local_only_data_access
    from quantmaster.runtime.llm import enter_http_request, leave_http_request
    from quantmaster.runtime.maintenance import maintenance_barrier
    from quantmaster.server.security import (
        SecurityViolation,
        apply_security_headers,
        enforce_request_security,
    )

    started = time.perf_counter()
    request_id = _new_request_id()
    request.state.request_id = request_id
    path = request.url.path
    llm_request_token = enter_http_request()
    allowed = (
        path
        in {
            "/api/v1/health",
            "/api/v1/diagnostics",
            "/api/v1/release",
            "/api/v1/session",
            "/api/v1/system/update",
            "/",
        }
        or path.startswith(("/static/", "/api/v1/data/migrations"))
        or (path == "/api/v1/settings" and request.method == "GET")
    )
    try:
        enforce_request_security(request)
        if (
            (migration_manager.active or maintenance_barrier.active)
            and path.startswith("/api/v1/")
            and not allowed
        ):
            problem = make_problem(
                "data_migration_active",
                severity="warning",
                source="数据目录",
                title="数据目录正在迁移",
                message="迁移完成前，读取或写入数据的操作已暂停。",
                action="等待迁移完成后重试。",
                blocking=True,
            )
            response = _problem_response(request_id, problem, status_code=423)
        else:
            # HTTP handlers are a local snapshot/read-command boundary.  The
            # sole exception is the explicit operator provider probe; refresh
            # jobs execute in their own background context after this request
            # has returned.  This keeps a page cache miss from silently
            # becoming an upstream timeout.
            provider_probe = (
                request.method == "POST"
                and (
                    (
                        path.startswith("/api/v1/diagnostics/providers/")
                        and path.endswith("/probe")
                    )
                    or path == "/api/v1/settings/check/data-sources"
                )
            )
            access = nullcontext() if provider_probe else local_only_data_access()
            with access:
                response = await call_next(request)
    except SecurityViolation as exc:
        problem = make_problem(
            exc.code,
            source="本地服务",
            title="请求被安全策略拒绝",
            message=str(exc.detail),
            action=exc.action,
            blocking=True,
        )
        response = _problem_response(request_id, problem, status_code=exc.status_code)
    except OperationProblem as exc:
        # Starlette's exception handlers sit inside this request middleware.
        # Preserve a deliberate cold/degraded operation contract instead of
        # accidentally turning it into a generic 500 at the outer boundary.
        response = _problem_response(
            request_id, exc.problem, status_code=exc.status_code,
            data_quality=exc.data_quality,
        )
    except HTTPException as exc:
        response = _http_exception_response(request_id, exc)
    except Exception:
        logger.exception(
            "未处理的接口异常 request_id=%s method=%s path=%s",
            request_id,
            request.method,
            path,
        )
        problem = make_problem(
            "unhandled_server_error",
            source="本地服务",
            title="服务端处理失败",
            message="请求未能完成，详细原因已写入服务端日志。",
            action="稍后重试；如持续发生，请提供请求编号。",
            blocking=True,
        )
        response = _problem_response(request_id, problem, status_code=500)
    try:
        response.headers["X-Request-ID"] = request_id
        response.headers["X-QM-Worker-Generation"] = os.environ.get("QM_WEB_GENERATION", "0")
        duration_ms = (time.perf_counter() - started) * 1000
        response.headers["Server-Timing"] = f"app;dur={duration_ms:.2f}"
        if path != "/api/v1/health":
            try:
                route = getattr(request.scope.get("route"), "path", None) or path
                get_runtime_metrics_recorder = __import__(
                    "quantmaster.runtime.metrics", fromlist=["get_runtime_metrics_recorder"],
                ).get_runtime_metrics_recorder
                get_runtime_metrics_recorder().record_request(
                    route=str(route),
                    method=request.method,
                    status_code=response.status_code,
                    duration_ms=duration_ms,
                    response_bytes=int(response.headers.get("content-length") or 0),
                )
            except (ImportError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
                # Observability must never turn a successful page read into a failure.
                pass
        if path.startswith("/static/"):
            response.headers["Cache-Control"] = "no-cache, max-age=0, must-revalidate"
        apply_security_headers(response)
        if response.status_code >= 400:
            logger.warning(
                "接口返回失败 request_id=%s method=%s path=%s status=%s",
                request_id,
                request.method,
                path,
                response.status_code,
            )
        return response
    finally:
        leave_http_request(llm_request_token)


STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

from quantmaster.server.after_close import router as after_close_router  # noqa: E402
from quantmaster.server.automation import router as automation_router  # noqa: E402
from quantmaster.server.capabilities import liveness  # noqa: E402
from quantmaster.server.capabilities import router as capabilities_router  # noqa: E402
from quantmaster.server.jobs import router as jobs_router  # noqa: E402
from quantmaster.server.lab import router as lab_router  # noqa: E402
from quantmaster.server.management import router as management_router  # noqa: E402
from quantmaster.server.news import router as news_router  # noqa: E402
from quantmaster.server.research import router as research_router  # noqa: E402
from quantmaster.server.rotation import router as rotation_router  # noqa: E402
from quantmaster.server.stock_analysis import router as stock_analysis_router  # noqa: E402
from quantmaster.server.trading import router as trading_router  # noqa: E402

app.include_router(management_router)
app.include_router(after_close_router)
app.include_router(automation_router)
app.include_router(lab_router)
app.include_router(jobs_router)
app.include_router(news_router)
app.include_router(trading_router)
app.include_router(research_router)
app.include_router(rotation_router)
app.include_router(stock_analysis_router)
app.include_router(capabilities_router)

__all__ = ["_progress_stream", "app", "create_lifespan", "liveness", "serve"]

def serve() -> None:  # pragma: no cover - 入口
    from quantmaster.server.lifecycle import run_uvicorn_foreground

    cfg = get_config().server
    run_uvicorn_foreground(
        app,
        host=cfg.host,
        port=cfg.port,
        log_level="warning",
    )
