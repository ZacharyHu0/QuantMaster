"""FastAPI 本地服务：JSON API + Web 仪表盘。

启动：qm serve  （或 uvicorn quantmaster.server.app:app）
浏览器访问 http://127.0.0.1:8686
"""

from __future__ import annotations

import hashlib
import logging
import os
import queue
import sqlite3
import threading
import time
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager, nullcontext
from pathlib import Path
from typing import Any, Literal

import pandas as pd
from anyio import to_thread
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import Field

from quantmaster import __version__
from quantmaster.backtest.metrics import RISK_FREE, TRADING_DAYS
from quantmaster.config import get_config
from quantmaster.data.base import DataEvidenceNotReady, MarketDataUnavailable
from quantmaster.logging_config import redact_sensitive_text
from quantmaster.release import RELEASE_DATE, RELEASE_HISTORY_URL, RELEASES
from quantmaster.runtime.contracts import ContractModel
from quantmaster.runtime.identity import get_application_identity
from quantmaster.runtime.json import StrictJSONResponse as JSONResponse
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.runtime.problems import OperationProblem, make_problem
from quantmaster.trading_sessions import default_close_data_end, market_date

logger = logging.getLogger(__name__)

# HTTP workers are allowed only a small, fixed number of synchronous escape
# hatches (currently NDJSON progress generation). Normal page reads stay on
# local stores; a saturated slot fails explicitly instead of creating another
# unbounded thread.
WEB_BLOCKING_TOKENS = 16
WEB_THREAD_WARNING = 64
_web_blocking_slots = threading.BoundedSemaphore(WEB_BLOCKING_TOKENS)
_web_stream_lock = threading.Lock()
_web_stream_runtime = None


def _stream_runtime():
    global _web_stream_runtime
    from quantmaster.server.stream_runtime import WebStreamRuntime

    with _web_stream_lock:
        if _web_stream_runtime is None:
            _web_stream_runtime = WebStreamRuntime(max_workers=WEB_BLOCKING_TOKENS)
        return _web_stream_runtime


def _shutdown_web_stream_executor(timeout: float = 5.0) -> None:
    global _web_stream_runtime
    with _web_stream_lock:
        runtime = _web_stream_runtime
        _web_stream_runtime = None
    if runtime is not None:
        runtime.shutdown(timeout)


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
        reload_worker = os.environ.get("QM_SERVER_RELOAD_WORKER") == "1"
        _stream_runtime()
        _configure_reload_worker_logging()
        from quantmaster.bootstrap import get_runtime_worker, get_worker_supervisor
        from quantmaster.data.free_stockdb_runtime import free_stockdb_runtime
        from quantmaster.logging_config import current_log_path
        from quantmaster.server.management import capture_runtime_baseline

        capture_runtime_baseline()
        supervisor_state = "reload-attached"
        previous_web_process = os.environ.get("QM_WEB_PROCESS")
        if reload_worker:
            free_stockdb_runtime.attach_to_supervisor()
        else:
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
            "Web 代次 %s · 后台 runtime-worker %s · 完整日志 %s",
            os.environ.get("QM_WEB_GENERATION", "0"),
            "由重载监督器托管" if reload_worker else (
                "本地测试/维护回退"
                if supervisor_state == "disabled"
                else "独立 Worker Supervisor 托管"
            ),
            str(log_path) if log_path else "仅终端",
        )
        try:
            yield
        finally:
            if not reload_worker:
                if supervisor_state == "disabled":
                    get_runtime_worker().stop()
                else:
                    get_worker_supervisor().stop()
                free_stockdb_runtime.stop()
            if previous_web_process is None:
                os.environ.pop("QM_WEB_PROCESS", None)
            else:
                os.environ["QM_WEB_PROCESS"] = previous_web_process
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


def _series_to_points(s: pd.Series) -> list[list]:
    return [[str(k.date()), round(float(v), 6)] for k, v in s.dropna().items()]


def _json_scalar(value):
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


ProgressEmitter = Callable[..., None]


def _progress_stream(
    task: Callable[[ProgressEmitter], dict],
    request_id: str | None = None,
) -> StreamingResponse:
    """在线程中执行同步数据任务，以 NDJSON 持续发送真实阶段进度。"""
    request_id = request_id or _new_request_id()
    events: queue.Queue[dict | None] = queue.Queue()
    cancelled = threading.Event()

    def emit(
        progress: int,
        phase: str,
        detail: str = "",
        partial: dict | None = None,
        level: str = "info",
    ) -> None:
        if cancelled.is_set():
            from quantmaster.server.stream_runtime import StreamGenerationClosed

            raise StreamGenerationClosed("stream client or generation disconnected")
        event = {
            "type": "progress",
            "progress": max(0, min(100, int(progress))),
            "phase": phase,
            "detail": detail,
            "level": level if level in {"info", "success", "warning"} else "info",
            "request_id": request_id,
        }
        if partial is not None:
            # 立即复制/编码，避免工作线程继续修改同一个 market 字典后，
            # 队列里较早的阶段事件也被连带改变。
            event["partial"] = jsonable_encoder(partial)
        events.put(event)

    def run() -> None:
        try:
            events.put(
                {
                    "type": "result",
                    "data": task(emit),
                    "request_id": request_id,
                }
            )
        except OperationProblem as exc:
            logger.warning(
                "流式数据任务被业务门禁阻止 request_id=%s code=%s",
                request_id,
                exc.problem.get("code"),
            )
            event = {
                "type": "error",
                "message": exc.problem["message"],
                "problem": exc.problem,
                "error_id": request_id,
                "request_id": request_id,
            }
            if exc.data_quality is not None:
                event["data_quality"] = exc.data_quality
            events.put(event)
        except MarketDataUnavailable as exc:
            problem = make_problem(
                "market_data_unavailable",
                source="行情数据",
                title="行情证据不可用",
                message=str(exc),
                action="检查缺失范围、来源与刷新状态后重试；不要据此生成正式决策。",
                blocking=True,
                can_continue=False,
            )
            events.put({
                "type": "error",
                "message": str(exc),
                "problem": problem,
                "data_quality": exc.quality.to_dict(),
                "provenance": list(exc.provenance),
                "error_id": request_id,
                "request_id": request_id,
            })
        except Exception as exc:
            from quantmaster.server.stream_runtime import StreamGenerationClosed

            if isinstance(exc, StreamGenerationClosed):
                logger.info("流式数据任务已在安全边界停止 request_id=%s", request_id)
                return
            logger.exception("流式数据任务失败 request_id=%s", request_id)
            message = _safe_client_error(exc)
            problem = make_problem(
                "stream_task_failed",
                source="后台任务",
                title="数据任务未完成",
                message=message,
                action="重试一次；如仍失败，请复制请求编号排查后端日志。",
                blocking=True,
            )
            events.put(
                {
                    "type": "error",
                    "message": message,
                    "problem": problem,
                    "error_id": request_id,
                    "request_id": request_id,
                }
            )
        finally:
            events.put(None)

    if not _web_blocking_slots.acquire(blocking=False):
        raise HTTPException(
            503,
            "web_blocking_capacity_exhausted：当前页面任务过多，请稍后重试",
        )

    def bounded_run() -> None:
        try:
            run()
        finally:
            _web_blocking_slots.release()

    try:
        _stream_runtime().submit(
            bounded_run, request_id=request_id, cancel=cancelled,
        )
    except Exception:
        _web_blocking_slots.release()
        raise

    async def generate() -> AsyncIterator[str]:
        try:
            while True:
                event = await to_thread.run_sync(events.get, abandon_on_cancel=True)
                if event is None:
                    break
                yield strict_json_dumps(jsonable_encoder(event)) + "\n"
        finally:
            # Starlette closes this iterator on client disconnect.  The
            # producer then stops at its next emit/checkpoint boundary.
            cancelled.set()

    return StreamingResponse(
        generate(),
        media_type="application/x-ndjson",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "X-Request-ID": request_id,
        },
    )


# ---------- 页面 ----------


@app.get("/", include_in_schema=False)
def index(request: Request) -> HTMLResponse:
    from quantmaster.server.security import ensure_csrf_cookie

    template = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    settings_css_revision = str((STATIC_DIR / "settings.css").stat().st_mtime_ns)
    settings_js_revision = str((STATIC_DIR / "settings.js").stat().st_mtime_ns)
    automation_css_revision = str((STATIC_DIR / "automation.css").stat().st_mtime_ns)
    automation_js_revision = str((STATIC_DIR / "automation.js").stat().st_mtime_ns)
    news_css_revision = str((STATIC_DIR / "news.css").stat().st_mtime_ns)
    news_js_revision = str((STATIC_DIR / "news.js").stat().st_mtime_ns)
    rotation_css_revision = str((STATIC_DIR / "rotation.css").stat().st_mtime_ns)
    lab_css_revision = str((STATIC_DIR / "lab.css").stat().st_mtime_ns)
    after_close_css_revision = str((STATIC_DIR / "after-close.css").stat().st_mtime_ns)
    app_css_revision = str((STATIC_DIR / "app.css").stat().st_mtime_ns)
    page = (
        template.replace("%%QM_VERSION%%", __version__)
        .replace("%%QM_RELEASE_DATE%%", RELEASE_DATE)
        .replace("%%QM_SETTINGS_CSS_REV%%", settings_css_revision)
        .replace("%%QM_SETTINGS_JS_REV%%", settings_js_revision)
        .replace("%%QM_AUTOMATION_CSS_REV%%", automation_css_revision)
        .replace("%%QM_AUTOMATION_JS_REV%%", automation_js_revision)
        .replace("%%QM_NEWS_CSS_REV%%", news_css_revision)
        .replace("%%QM_NEWS_JS_REV%%", news_js_revision)
        .replace("%%QM_ROTATION_CSS_REV%%", rotation_css_revision)
        .replace("%%QM_LAB_CSS_REV%%", lab_css_revision)
        .replace("%%QM_AFTER_CLOSE_CSS_REV%%", after_close_css_revision)
        .replace("%%QM_APP_CSS_REV%%", app_css_revision)
        .replace("%%QM_TRADING_DAYS%%", str(TRADING_DAYS))
        .replace("%%QM_RISK_FREE%%", str(RISK_FREE))
    )
    csp = "; ".join(
        (
            "default-src 'self'",
            "script-src 'self'",
            "script-src-attr 'none'",
            "style-src 'self'",
            "style-src-attr 'unsafe-inline'",
            "img-src 'self' data:",
            "connect-src 'self'",
            "font-src 'self'",
            "object-src 'none'",
            "base-uri 'none'",
            "frame-ancestors 'none'",
            "form-action 'self'",
        )
    )
    response = HTMLResponse(
        page,
        headers={
            "Cache-Control": "no-cache",
            "Content-Security-Policy": csp,
        },
    )
    ensure_csrf_cookie(response, request)
    return response


@app.get("/api/v1/session")
def create_browser_session(request: Request, response: Response) -> dict:
    """Issue a stateless double-submit token for this local browser."""
    from quantmaster.server.security import attach_csrf_cookie, issue_csrf

    token = issue_csrf()
    attach_csrf_cookie(response, request, token)
    return {"csrf_token": token, "expires_in": 8 * 60 * 60, "local_only": True}


@app.get("/api/v1/health")
async def liveness() -> dict:
    """Sole constant-time health probe; it deliberately performs no store access.

    Optional provider/worker state is reported separately by ``/diagnostics``.
    """
    identity = get_application_identity()
    from quantmaster.server.readiness import readiness_status

    readiness = readiness_status(include_optional_services=False)
    threads = threading.active_count()
    return {
        "status": "ok",
        "core_ready": bool(readiness["core_ready"]),
        "readiness_status": str(readiness["status"]),
        "version": __version__,
        "release_date": RELEASE_DATE,
        "process_pid": os.getpid(),
        "generation": os.environ.get("QM_WEB_GENERATION", "0"),
        "build_sha": identity.build_sha,
        "slot_id": identity.slot_id,
        "runtime_generation": identity.runtime_generation,
        "web_threads": threads,
        "thread_status": "warning" if threads > WEB_THREAD_WARNING else "ok",
    }


@app.get("/api/v1/diagnostics")
def diagnostic_report() -> dict:
    from quantmaster.server.diagnostics import diagnostics

    # The runtime-worker refreshes this cache.  A diagnostic GET must never
    # construct stores or contend for SQLite while the page is already slow.
    return diagnostics(wait_for_first=False, refresh=False)


@app.post("/api/v1/diagnostics/providers/{lane}/probe")
def allow_provider_probe(lane: str) -> dict:
    """Allow one operator-requested recovery probe for a known provider lane."""
    from quantmaster.data.resilience import PROVIDER_HEALTH
    from quantmaster.server.diagnostics import invalidate_diagnostics

    known = PROVIDER_HEALTH.status(lane)
    if lane not in known:
        raise HTTPException(404, "数据源通道不存在")
    state = PROVIDER_HEALTH.reset(lane)[lane]
    invalidate_diagnostics()
    return {
        "lane": lane,
        "status": "probe_allowed",
        "state": state,
        "message": "下一次相关请求将绕过冷却并执行一次受控探测。",
    }


@app.get("/api/v1/release")
def release_info() -> dict:
    """前端版本入口使用的发布信息，与应用包版本保持一致。"""
    return {
        "version": __version__,
        "release_date": RELEASE_DATE,
        "releases": RELEASES[:10],
        "history_url": RELEASE_HISTORY_URL,
    }


# ---------- 市场 ----------


def _market_snapshot_etag(
    request: Request,
    response: Response,
    payload: dict,
    *,
    encoded: bytes | None = None,
) -> Any:
    snapshot = payload.get("snapshot") if isinstance(payload.get("snapshot"), dict) else {}
    meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    snapshot_id = str(snapshot.get("id") or meta.get("snapshot_id") or "")
    if not snapshot_id:
        return payload
    canonical_query = strict_json_dumps(
        sorted((str(key), str(value)) for key, value in request.query_params.multi_items()),
        sort_keys=True,
    )
    etag = '"' + hashlib.sha256(
        f"{snapshot_id}\n{canonical_query}".encode()
    ).hexdigest() + '"'
    headers = {
        "ETag": etag,
        "Cache-Control": "private, max-age=0, must-revalidate",
    }
    requested = str(request.headers.get("if-none-match") or "")
    if requested == "*" or etag in {value.strip() for value in requested.split(",")}:
        return Response(status_code=304, headers=headers)
    if encoded is not None:
        return Response(content=encoded, media_type="application/json", headers=headers)
    response.headers.update(headers)
    return payload


@app.get("/api/v1/market/overview")
def market_overview(
    request: Request,
    response: Response,
) -> Any:
    """Read one published local market snapshot; never rebuild or synchronize."""
    from quantmaster.market.overview_snapshot import read_market_overview_snapshot_wire

    payload, encoded = read_market_overview_snapshot_wire()
    return _market_snapshot_etag(
        request, response, payload, encoded=encoded,
    )


@app.get("/api/v1/market/fear-greed")
def market_fear_greed() -> dict:
    """Read the last local CNN snapshot; network refresh is a background action."""
    from quantmaster.market import read_cnn_fear_greed

    return read_cnn_fear_greed()


@app.get("/api/v1/market/history/{symbol}")
def market_history(
    symbol: str, start: str | None = None, end: str | None = None, frequency: str = "1d"
) -> dict:
    from quantmaster.data import read_bars
    from quantmaster.data.base import validate_frequency, validate_symbol

    end = end or (
        default_close_data_end() if frequency == "1d" else market_date().isoformat()
    )
    try:
        symbol = validate_symbol(symbol)
        frequency = validate_frequency(frequency)
    except ValueError:
        raise HTTPException(422, "标的代码或行情频率无效") from None
    if not start:
        end_stamp = pd.Timestamp(end)
        start_stamp = (
            end_stamp - pd.DateOffset(years=3)
            if frequency == "1d"
            else end_stamp - pd.Timedelta(days=12)
        )
        start = str(start_stamp.date())
    started = time.perf_counter()
    try:
        market_envelope = read_bars(symbol, start, end, frequency=frequency)
        df = market_envelope.require_data()
    except MarketDataUnavailable:
        raise
    except Exception:
        logger.warning("行情历史读取失败 symbol=%s frequency=%s", symbol, frequency, exc_info=True)
        raise HTTPException(503, f"{symbol} 行情暂不可用，请查看本机日志") from None
    loaded = time.perf_counter()
    positions = {column: offset + 1 for offset, column in enumerate(df.columns)}
    volume_position = positions.get("volume")
    kline = [
        [
            str(values[0].date()) if frequency == "1d" else str(values[0]),
            round(values[positions["open"]], 3),
            round(values[positions["close"]], 3),
            round(values[positions["low"]], 3),
            round(values[positions["high"]], 3),
            round(values[volume_position], 0) if volume_position is not None else 0.0,
        ]
        for values in df.itertuples(index=True, name=None)
    ]
    finished = time.perf_counter()
    total_ms = (finished - started) * 1000
    if total_ms >= 500:
        logger.warning(
            "行情历史读取缓慢 symbol=%s frequency=%s rows=%d load_ms=%.1f serialize_ms=%.1f total_ms=%.1f",
            symbol,
            frequency,
            len(kline),
            (loaded - started) * 1000,
            (finished - loaded) * 1000,
            total_ms,
        )
    return {
        "symbol": symbol,
        "frequency": frequency,
        "kline": kline,
        "data_quality": market_envelope.quality.to_dict(),
        "provenance": list(market_envelope.provenance),
    }


class RegimeRequest(ContractModel):
    universe: str = "demo"
    start: str = "2022-01-01"
    end: str | None = None
    sectors: bool = True
    sector_top: int = 10
    history: int = Field(60, ge=7, le=3000)


@app.post("/api/v1/market/regime")
def market_regime(req: RegimeRequest) -> dict:
    """当前/过去/未来市场状态，以及行业板块强弱。"""
    from quantmaster.data import read_panel
    from quantmaster.data.industry import load_industry_analysis_context
    from quantmaster.data.universe import load_universe_analysis_snapshot
    from quantmaster.market import analyze_market, analyze_sectors

    end = default_close_data_end(req.end)
    try:
        universe_snapshot = load_universe_analysis_snapshot(
            req.universe, as_of=end if req.end else None,
        )
        market_envelope = read_panel(list(universe_snapshot.symbols), req.start, end)
        panel = market_envelope.require_data()
        report = analyze_market(panel)
        past = report.pop("past").tail(req.history)
        report["past"] = [
            {"date": str(idx.date()), **{key: _json_scalar(value) for key, value in row.items()}}
            for idx, row in past.iterrows()
        ]
        report["sectors"] = []
        if req.sectors:
            mapping, industry_evidence = load_industry_analysis_context(
                as_of=end if req.end else None,
            )
            sectors = analyze_sectors(
                panel,
                mapping,
            ).head(req.sector_top)
            report["sectors"] = sectors.to_dict(orient="records")
            report["industry_evidence"] = industry_evidence
        report["data_quality"] = market_envelope.quality.to_dict()
        report["universe_evidence"] = universe_snapshot.to_dict()
        return report
    except MarketDataUnavailable:
        raise
    except Exception:
        raise _logged_bad_request("市场状态分析") from None


class SelectionRequest(ContractModel):
    universe: str = "demo"
    start: str = "2022-01-01"
    end: str | None = None
    top_n: int = 10
    horizon: Literal[1, 3, 5, 7, 10, 20, 30] = 3
    profile: Literal["risk_adjusted", "short_term", "stable"] = "risk_adjusted"
    cap_weight: float = Field(0.25, gt=0, le=1)
    include_industry: bool = True
    save: bool = False
    policy_mode: Literal["live", "historical_replay", "retrospective"] = "live"


@app.post("/api/v1/research/selection/daily")
def selection_daily(req: SelectionRequest) -> dict:
    """收盘后生成适合次日执行的 1–30 日预测选股决策。"""
    from quantmaster.data import read_panel, read_stock_names
    from quantmaster.data.industry import load_industry_analysis_context
    from quantmaster.data.universe import load_universe_analysis_snapshot
    from quantmaster.decision import hybrid_daily_selection, resolve_policy

    end = default_close_data_end(req.end)
    try:
        if req.end and req.policy_mode == "live":
            raise ValueError(
                "显式历史截止日不能使用 live 模式；请选择 historical_replay 或 retrospective"
            )
        if req.save and req.policy_mode == "retrospective":
            raise ValueError("retrospective 结果不能写入正式决策历史")
        universe_snapshot = load_universe_analysis_snapshot(
            req.universe, as_of=end if req.end else None,
        )
        symbols = list(universe_snapshot.symbols)
        market_envelope = read_panel(symbols, req.start, end)
        panel = market_envelope.require_data()
        mapping, industry_evidence = (
            load_industry_analysis_context(as_of=end if req.end else None)
            if req.include_industry else ({}, None)
        )
        names = read_stock_names(symbols)
        policy = resolve_policy(
            req.universe,
            req.horizon,
            req.profile,
            symbols=list(panel["close"].columns),
            as_of=end if req.policy_mode == "historical_replay" else "",
            mode=req.policy_mode,
        )
        decision_feature_inputs: dict[str, pd.DataFrame] = {}
        report = hybrid_daily_selection(
            panel,
            top_n=req.top_n,
            horizon=req.horizon,
            profile=req.profile,
            universe=req.universe,
            industry_map=mapping,
            name_map=names,
            policy_snapshot=policy,
            cap_weight=req.cap_weight,
            policy_mode=req.policy_mode,
            evidence_sink=decision_feature_inputs,
        )
        report["calculation_quality"] = report.get("data_quality")
        report["data_quality"] = market_envelope.quality.to_dict()
        report["market_provenance"] = list(market_envelope.provenance)
        report["universe_evidence"] = universe_snapshot.to_dict()
        report["industry_evidence"] = industry_evidence
        save_allowed = (
            market_envelope.quality.formal_eligible
            and universe_snapshot.formal_eligible
            and (
                not req.include_industry
                or bool((industry_evidence or {}).get("formal_eligible"))
            )
        )
        persistence = {
            "requested": req.save,
            "saved": False,
            "status": (
                "not_requested" if not req.save else "pending" if save_allowed else "blocked"
            ),
            "reason": "",
        }
        if req.save:
            if save_allowed:
                from quantmaster.decision import DecisionStore

                persistence.update(saved=True, status="saved")
                report["persistence"] = persistence
                DecisionStore().save(
                    report,
                    req.universe,
                    panel={**panel, **decision_feature_inputs},
                )
            else:
                persistence["reason"] = (
                    "正式决策未保存：行情、候选池或行业证据未通过正式门；计算结果仅供查看"
                )
        report["persistence"] = persistence
        return report
    except MarketDataUnavailable:
        raise
    except Exception:
        raise _logged_bad_request("每日选股分析") from None


def _decision_history_symbols(snapshots: list[dict]) -> list[str]:
    return list(dict.fromkeys(
        str(pick.get("symbol") or "")
        for snapshot in snapshots
        for pick in (
            [
                item for item in (snapshot.get("picks") or [])
                if float(item.get("target_weight") or 0) > 0
            ][:3]
            if any("target_weight" in item for item in (snapshot.get("picks") or []))
            else (snapshot.get("picks") or [])[:3]
        )
        if pick.get("symbol")
    ))


def _cached_decision_price_frames(snapshots: list[dict]) -> dict[str, pd.DataFrame]:
    """Read follow-up prices from the local cache without triggering a refresh."""
    symbols = _decision_history_symbols(snapshots)
    if not symbols:
        return {}
    from quantmaster.data.storage import BarStore

    store = BarStore()
    frames: dict[str, pd.DataFrame] = {}
    for symbol in symbols:
        result = store.read(symbol, enqueue_repair=False)
        if result.frame is not None:
            frames[symbol] = result.frame
    return frames


@app.get("/api/v1/research/selection/history")
def selection_history(
    universe: str | None = None,
    limit: int = 30,
    profile: str | None = None,
    horizon: int | None = None,
) -> dict:
    from quantmaster.data import read_stock_names
    from quantmaster.decision import DecisionStore, enrich_decision_snapshots

    if horizon is not None and horizon not in {1, 3, 5, 7, 10, 20, 30}:
        raise HTTPException(422, "horizon 只支持 1、3、5、7、10、20、30")
    snapshots = DecisionStore(read_only=True).history(
        universe,
        min(max(limit, 1), 200),
        profile=profile,
        horizon=horizon,
    )
    symbols = list(
        dict.fromkeys(
            pick.get("symbol", "")
            for snapshot in snapshots
            for pick in snapshot.get("picks", [])
            if pick.get("symbol")
        )
    )
    names = read_stock_names(symbols) if symbols else {}
    for snapshot in snapshots:
        for pick in snapshot.get("picks", []):
            if not pick.get("name") or pick.get("name") == "名称待同步":
                pick["name"] = names.get(pick.get("symbol"), "名称待同步")
    snapshots = enrich_decision_snapshots(
        snapshots, _cached_decision_price_frames(snapshots),
    )
    return {"snapshots": snapshots}


class DecisionDashboardRequest(ContractModel):
    universe: str = "demo"
    start: str = "2022-01-01"
    end: str | None = None
    top_n: int = Field(10, ge=1, le=50)
    horizon: Literal[1, 3, 5, 7, 10, 20, 30] = 3
    profile: Literal["risk_adjusted", "short_term", "stable"] = "risk_adjusted"
    cap_weight: float = Field(0.25, gt=0, le=1)
    sector_top: int = Field(10, ge=1, le=50)
    history: int = Field(2600, ge=7, le=3000)
    save: bool = True
    policy_mode: Literal["live", "historical_replay", "retrospective"] = "live"


_DECISION_MIN_MARKET_COVERAGE = 0.90


def _decision_market_formal_eligible(quality: Any) -> bool:
    """Admit a disclosed partial panel unless too much of the universe is absent."""
    coverage = quality.coverage_ratio
    return bool(
        quality.analysis_eligible
        and coverage is not None
        and float(coverage) >= _DECISION_MIN_MARKET_COVERAGE
    )


def _decision_panel_has_minimum_history(envelope: Any) -> bool:
    panel = envelope.data
    if not isinstance(panel, dict):
        return False
    close = panel.get("close")
    return bool(
        isinstance(close, pd.DataFrame)
        and len(close.dropna(how="all")) >= 20
    )


@app.post("/api/v1/research/decision/dashboard")
def decision_dashboard(req: DecisionDashboardRequest) -> dict:
    """决策工作台：只加载一次行情，同时生成市场、板块、选股和历史快照。"""
    try:
        return _decision_dashboard_data(req)
    except MarketDataUnavailable:
        raise
    except Exception:
        raise _logged_bad_request("决策工作台计算") from None


def _decision_dashboard_data(
    req: DecisionDashboardRequest,
    progress: ProgressEmitter | None = None,
) -> dict:
    from quantmaster.data import read_panel, read_stock_names, refresh_panel
    from quantmaster.data.industry import load_industry_analysis_context
    from quantmaster.data.universe import load_universe_analysis_snapshot
    from quantmaster.decision import (
        DecisionStore,
        enrich_decision_snapshots,
        hybrid_daily_selection,
        price_frames_from_panel,
        resolve_policy,
    )
    from quantmaster.market import analyze_market, analyze_sectors

    end = default_close_data_end(req.end)
    if req.end and req.policy_mode == "live":
        raise ValueError(
            "显式历史截止日不能使用 live 模式；请选择 historical_replay 或 retrospective"
        )
    if req.save and req.policy_mode == "retrospective":
        raise ValueError("retrospective 结果不能写入正式决策历史")
    universe_snapshot = load_universe_analysis_snapshot(
        req.universe, as_of=end if req.end else None,
    )
    symbols = list(universe_snapshot.symbols)
    if progress:
        progress(3, "准备候选", f"共 {len(symbols)} 只标的")

    def on_symbol(completed: int, total: int, symbol: str, success: bool) -> None:
        if progress:
            progress(
                5 + round(58 * completed / max(1, total)),
                "同步候选行情",
                f"{completed}/{total} · {symbol} · {'已读取本地快照' if success else '本地暂无数据'}",
                {
                    "kind": "decision_symbol",
                    "symbol": symbol,
                    "success": success,
                    "completed": completed,
                    "total": total,
                    "read_mode": "local_only",
                },
                "info" if success else "warning",
            )

    market_envelope = read_panel(
        symbols, req.start, end, progress=on_symbol if progress else None,
    )
    local_market_envelope = market_envelope
    if req.save and not market_envelope.quality.formal_eligible:
        if progress:
            progress(64, "补齐候选行情", "本地证据不足，尝试在线补齐并写回行情库")
        try:
            refreshed = refresh_panel(
                symbols,
                req.start,
                end,
                mode="auto",
                work_class="normal",
            )
            if (
                refreshed.quality.analysis_eligible
                and _decision_panel_has_minimum_history(refreshed)
            ):
                market_envelope = refreshed
        except (MarketDataUnavailable, OSError, RuntimeError, ValueError) as exc:
            logger.warning("选股行情在线补齐失败，继续评估本地覆盖：%s", exc)
            market_envelope = local_market_envelope
    if req.save and (
        not _decision_market_formal_eligible(market_envelope.quality)
        or not _decision_panel_has_minimum_history(market_envelope)
    ):
        raise MarketDataUnavailable(market_envelope.quality, market_envelope.provenance)
    panel = market_envelope.require_data()
    if progress:
        progress(67, "加载行业与名称", "优先复用本地缓存")
    mapping, industry_evidence = load_industry_analysis_context(
        as_of=end if req.end else None,
    )
    names = read_stock_names(symbols)
    if progress:
        progress(78, "计算牛熊与趋势", "汇总 MACD、资金量和市场宽度")
    market = analyze_market(panel)
    past = market.pop("past").tail(req.history)
    market["past"] = [
        {"date": str(idx.date()), **{key: _json_scalar(value) for key, value in row.items()}}
        for idx, row in past.iterrows()
    ]
    if progress:
        # 中间态先给最近约一年，足够默认 3M 视图且避免把最长 10Y 历史
        # 在 partial 与最终 result 中重复传输两遍；最终事件仍返回完整窗口。
        preview_market = {**market, "past": market["past"][-260:]}
        progress(
            84,
            "市场状态已就绪",
            "牛熊、宽度与未来概率可先查看",
            {"kind": "decision_market", "market": preview_market},
        )
        progress(86, "聚合板块强弱", "按行业计算趋势与上涨宽度")
    market["sectors"] = analyze_sectors(panel, mapping).head(req.sector_top).to_dict(orient="records")
    if progress:
        progress(
            90,
            "板块数据已就绪",
            f"已生成 {len(market['sectors'])} 个板块状态",
            {"kind": "decision_sectors", "sectors": market["sectors"]},
        )
        progress(91, "匹配 Quant Lab Champion", f"{req.profile} · {req.horizon} 日")
    policy = resolve_policy(
        req.universe,
        req.horizon,
        req.profile,
        symbols=list(panel["close"].columns),
        as_of=end if req.policy_mode == "historical_replay" else "",
        mode=req.policy_mode,
    )
    if progress:
        progress(
            92,
            "决策模型已就绪",
            policy["profile_label"],
            {"kind": "decision_policy", "policy": policy},
        )
        progress(93, "生成每日候选", f"目标持有 {req.horizon} 日")
    decision_feature_inputs: dict[str, pd.DataFrame] = {}
    selection = hybrid_daily_selection(
        panel,
        top_n=req.top_n,
        horizon=req.horizon,
        profile=req.profile,
        universe=req.universe,
        industry_map=mapping,
        name_map=names,
        policy_snapshot=policy,
        cap_weight=req.cap_weight,
        policy_mode=req.policy_mode,
        evidence_sink=decision_feature_inputs,
    )
    selection["calculation_quality"] = selection.get("data_quality")
    market_quality = market_envelope.quality.to_dict()
    market_quality["decision_formal_eligible"] = _decision_market_formal_eligible(
        market_envelope.quality
    )
    market_quality["decision_minimum_coverage"] = _DECISION_MIN_MARKET_COVERAGE
    selection["data_quality"] = market_quality
    selection["market_provenance"] = list(market_envelope.provenance)
    selection["universe_evidence"] = universe_snapshot.to_dict()
    selection["industry_evidence"] = industry_evidence
    save_allowed = (
        _decision_market_formal_eligible(market_envelope.quality)
        and universe_snapshot.formal_eligible
        and bool(industry_evidence.get("formal_eligible"))
    )
    persistence = {
        "requested": req.save,
        "saved": False,
        "status": (
            "not_requested" if not req.save else "pending" if save_allowed else "blocked"
        ),
        "reason": "",
    }
    if req.save and not save_allowed:
        persistence["reason"] = (
            "正式决策未保存：行情、候选池或行业证据未通过正式门；计算结果仅供查看"
        )
    selection["persistence"] = persistence
    if progress:
        progress(
            96,
            "每日候选已就绪",
            f"已生成 {len(selection.get('picks', []))} 只候选",
            {"kind": "decision_selection", "selection": selection},
        )
    store = DecisionStore()
    if req.save and save_allowed:
        if progress:
            progress(97, "保存决策快照", "写入本地 SQLite")
        persistence.update(saved=True, status="saved")
        selection["persistence"] = persistence
        store.save(
            selection,
            req.universe,
            panel={**panel, **decision_feature_inputs},
        )
    # The dashboard is a mixed current-result/history view.  A corrupt or
    # pre-hash legacy snapshot must remain visibly degraded, but must not make
    # the freshly computed selection fail after it has already reached 96%.
    # Strict consumers still use DecisionStore.history() and fail closed.
    issues: list[dict[str, Any]] = []
    try:
        history = store.history(req.universe, limit=10, profile=req.profile)
        # Legacy/corrupt history is optional display data. It must not erase a
        # freshly calculated decision.
        for snapshot in history:
            for pick in snapshot.get("picks", []):
                if not pick.get("name") or pick.get("name") == "名称待同步":
                    pick["name"] = names.get(pick.get("symbol"), "名称待同步")
        history_symbols = _decision_history_symbols(history)
        history = enrich_decision_snapshots(
            history, price_frames_from_panel(panel, history_symbols),
        )
    except (OSError, RuntimeError, TypeError, ValueError, sqlite3.Error) as exc:
        logger.warning("决策历史不可用，将返回当前计算结果: %s", _safe_client_error(exc))
        history = []
        issues.append(make_problem(
            "history_unavailable", severity="warning", source="决策历史",
            title="历史快照暂不可用", message="当前决策已完成，但历史验证暂时无法读取。",
            action="可稍后重试读取历史；当前计算结果未受影响。", blocking=False,
            can_continue=True, retryable=True, suggestion="稍后重试历史验证。",
            component="history",
        ))
    if progress:
        progress(
            99,
            "历史快照与验证已就绪" if not issues else "历史验证暂不可用",
            f"已读取 {len(history)} 条本地记录并核对后续价格" if not issues else "已保留当前决策结果",
            {"kind": "decision_history", "history": history},
            "warning" if issues else "info",
        )
    result = {
        "market": market,
        "selection": selection,
        "history": history,
        "model_snapshot": selection.get("model_snapshot"),
        "calculation_quality": selection.get("calculation_quality"),
        "data_quality": market_quality,
        "provenance": list(market_envelope.provenance),
        "persistence": persistence,
        "universe_evidence": universe_snapshot.to_dict(),
        "industry_evidence": industry_evidence,
    }
    if progress:
        result["status"] = "completed_with_issues" if issues else "completed"
        result["issues"] = issues
    if progress:
        progress(100, "决策数据已就绪", f"生成 {len(selection.get('picks', []))} 只候选")
    return result


@app.post("/api/v1/research/decision/dashboard/stream")
def decision_dashboard_stream(
    req: DecisionDashboardRequest,
    request: Request,
) -> StreamingResponse:
    """决策工作台流式进度；最终 result 事件携带完整原接口响应。"""
    return _progress_stream(
        lambda emit: _decision_dashboard_data(req, emit),
        _request_id(request),
    )


# ---------- 因子 ----------


@app.get("/api/v1/research/factors")
def factors_list() -> dict:
    from quantmaster.ai.sentiment import list_news_factors
    from quantmaster.factors.fundamental import list_fundamental_factors
    from quantmaster.factors.library import list_factors
    from quantmaster.lab.models import factor_name_key
    from quantmaster.lab.store import read_runtime_factors

    factors = list_factors() + list_fundamental_factors() + list_news_factors()
    for item in factors:
        item.setdefault("source", "builtin")
    known_names = {factor_name_key(item["name"]) for item in factors}
    try:
        for item in read_runtime_factors():
            key = factor_name_key(item["name"])
            if key in known_names:
                continue
            factors.append(item)
            known_names.add(key)
    except (OSError, RuntimeError, sqlite3.Error) as exc:
        # The core catalog remains useful if the optional Lab ledger is unavailable.
        logger.warning("Quant Lab 因子目录暂不可用于自动补全: %s", exc)
    return {"factors": factors}


class FactorTestRequest(ContractModel):
    expression: str = Field(..., description="因子名或表达式，如 rank(-delta(close, 5))")
    universe: str = "demo"
    start: str = "2022-01-01"
    end: str | None = None
    quantiles: int = 5
    neutralize: bool = False  # 行业中性化（行业内去均值）


@app.post("/api/v1/research/factors/test")
def factors_test(req: FactorTestRequest) -> dict:
    from quantmaster.factors import run_factor_test

    try:
        return run_factor_test(
            expression=req.expression,
            universe=req.universe,
            start=req.start,
            end=req.end,
            quantiles=req.quantiles,
            neutralize=req.neutralize,
            refresh=False,
        )
    except MarketDataUnavailable:
        raise
    except Exception:
        raise _logged_bad_request("因子检验") from None


class ValidateRequest(ContractModel):
    expression: str
    universe: str = "demo"
    start: str = "2022-01-01"
    end: str | None = None
    split: str
    n_splits: int = 4


@app.post("/api/v1/research/factors/validate")
def factors_validate(req: ValidateRequest) -> dict:
    """样本外验证：split 前训练、split 后验证，外加滚动分段稳定性。"""
    from quantmaster.backtest import train_test_ic, walk_forward_ic
    from quantmaster.data import read_panel
    from quantmaster.data.universe import load_universe_analysis_snapshot
    from quantmaster.factors.fundamental import resolve_factor

    end = default_close_data_end(req.end)
    try:
        universe_snapshot = load_universe_analysis_snapshot(
            req.universe, as_of=end if req.end else None,
        )
        symbols = list(universe_snapshot.symbols)
        factor = resolve_factor(req.expression, symbols, req.start, end)
        market_envelope = read_panel(symbols, req.start, end)
        panel = market_envelope.require_data()
        result = train_test_ic(factor, panel, split=req.split)
        segments = walk_forward_ic(factor, panel, n_splits=req.n_splits)
        result["segments"] = [
            {
                k: (None if pd.isna(v) else (float(v) if isinstance(v, (int, float)) else str(v)))
                for k, v in row.items()
            }
            for _, row in segments.iterrows()
        ]
    except MarketDataUnavailable:
        raise
    except Exception as e:
        raise HTTPException(400, str(e)) from e
    result["data_quality"] = market_envelope.quality.to_dict()
    result["universe_evidence"] = universe_snapshot.to_dict()
    return result


# ---------- 因子挖掘 ----------


class MineRequest(ContractModel):
    universe: str = "demo"
    start: str = "2022-01-01"
    end: str | None = None
    generations: int = 5
    population: int = 40
    top_n: int = 10
    seed: int = 42


@app.post("/api/v1/research/mining/genetic")
def mine_genetic(req: MineRequest) -> dict:
    from quantmaster.data import read_panel
    from quantmaster.data.universe import load_universe_analysis_snapshot
    from quantmaster.factors.mining import GeneticMiner

    end = default_close_data_end(req.end)
    try:
        universe_snapshot = load_universe_analysis_snapshot(
            req.universe, as_of=end if req.end else None,
        )
        market_envelope = read_panel(list(universe_snapshot.symbols), req.start, end)
        panel = market_envelope.require_data()
        miner = GeneticMiner(population=req.population, generations=req.generations, seed=req.seed)
        mined = miner.mine(panel, top_n=req.top_n, progress=False)
    except MarketDataUnavailable:
        raise
    except Exception as e:
        raise HTTPException(400, str(e)) from e
    return {
        "factors": [m.__dict__ for m in mined],
        "data_quality": market_envelope.quality.to_dict(),
        "universe_evidence": universe_snapshot.to_dict(),
    }


# ---------- 实盘账本 ----------


class AssetListIn(ContractModel):
    symbol: str = Field(..., min_length=1, max_length=40)
    name: str = Field("", max_length=80)


def _cached_asset_quote(symbol: str, store) -> dict:
    """只读本地日线缓存，列表浏览不会消耗 AKShare/Tushare 次数。"""
    cached = store.get(symbol)
    if cached is None or cached.empty or "close" not in cached:
        return {"last": None, "change_pct": None, "as_of": None}
    close = cached["close"].dropna()
    if close.empty:
        return {"last": None, "change_pct": None, "as_of": None}
    last = float(close.iloc[-1])
    change = float((last / close.iloc[-2] - 1) * 100) if len(close) > 1 else None
    return {
        "last": round(last, 4),
        "change_pct": round(change, 3) if change is not None else None,
        "as_of": str(pd.Timestamp(close.index[-1]).date()),
    }


def _asset_lists_payload() -> dict:
    from quantmaster.data.storage import BarStore
    from quantmaster.portfolio import AssetListStore, Ledger

    lists = AssetListStore(read_only=True).all()
    store = BarStore(read_only=True)
    quote_cache: dict[str, dict] = {}

    def quote(symbol: str) -> dict:
        if symbol not in quote_cache:
            quote_cache[symbol] = _cached_asset_quote(symbol, store)
        return quote_cache[symbol]

    payload: dict[str, list[dict]] = {}
    for list_name, items in lists.items():
        payload[list_name] = [{**item, **quote(item["symbol"])} for item in items]

    holdings = []
    for position in Ledger(read_only=True).positions():
        if position.shares <= 0:
            continue
        item = {"symbol": position.symbol, **quote(position.symbol)}
        last = item["last"] if item["last"] is not None else position.avg_cost
        item.update(
            {
                "shares": round(position.shares, 2),
                "avg_cost": round(position.avg_cost, 4),
                "market_value": round(position.shares * last, 2),
                "unrealized_pnl": round(position.shares * (last - position.avg_cost), 2),
                "pnl_pct": round(last / position.avg_cost - 1, 4) if position.avg_cost else None,
                "realized_pnl": round(position.realized_pnl, 2),
            }
        )
        holdings.append(item)
    payload["holdings"] = sorted(holdings, key=lambda item: item["market_value"], reverse=True)
    return payload


@app.get("/api/v1/portfolio/lists")
def asset_lists_get() -> dict:
    """自选、关注和实盘持有；报价仅复用本地缓存。"""
    return _asset_lists_payload()


@app.post("/api/v1/portfolio/lists/{list_name}")
def asset_lists_add(
    list_name: Literal["favorites", "following"],
    item: AssetListIn,
) -> dict:
    from quantmaster.portfolio import AssetListStore

    try:
        AssetListStore().add(list_name, item.symbol, item.name)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    return _asset_lists_payload()


@app.delete("/api/v1/portfolio/lists/{list_name}/{symbol}")
def asset_lists_remove(
    list_name: Literal["favorites", "following"],
    symbol: str,
) -> dict:
    from quantmaster.portfolio import AssetListStore

    try:
        AssetListStore().remove(list_name, symbol)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    return _asset_lists_payload()


class TradeIn(ContractModel):
    date: str = Field(min_length=10, max_length=10)
    symbol: str = Field(min_length=1, max_length=40)
    side: Literal["buy", "sell"]
    price: float = Field(gt=0)
    shares: float = Field(gt=0)
    fee: float = Field(default=0.0, ge=0)
    note: str = Field(default="", max_length=1000)


@app.post("/api/v1/portfolio/ledger/trade")
def ledger_add_trade(trade: TradeIn) -> dict:
    from quantmaster.portfolio import Ledger, TradeRecord

    try:
        Ledger().add_trade(TradeRecord(**trade.model_dump()))
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    return {"status": "ok"}


class CashflowIn(ContractModel):
    date: str = Field(min_length=10, max_length=10)
    amount: float
    kind: Literal["deposit", "withdraw", "dividend"] = "deposit"
    note: str = Field(default="", max_length=1000)


@app.post("/api/v1/portfolio/ledger/cashflow")
def ledger_add_cashflow(flow: CashflowIn) -> dict:
    from quantmaster.portfolio import Ledger

    try:
        Ledger().add_cashflow(flow.date, flow.amount, flow.kind, flow.note)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    return {"status": "ok"}


@app.get("/api/v1/portfolio/ledger/report")
def ledger_get_report() -> dict:
    from quantmaster.portfolio import Ledger, ledger_report

    return ledger_report(Ledger(read_only=True))


@app.get("/api/v1/portfolio/ledger/trades")
def ledger_get_trades() -> dict:
    from quantmaster.portfolio import Ledger

    df = Ledger(read_only=True).trades()
    return {"trades": df.to_dict(orient="records")}


@app.get("/api/v1/portfolio/ledger/nav")
def ledger_get_nav(benchmark: str = "000300.SH") -> dict:
    """实盘每日净值（TWR）与基准对比。行情走本地缓存，缺失标的按最近成交价估值。"""
    from quantmaster.data import read_history
    from quantmaster.data.storage import BarStore
    from quantmaster.portfolio import Ledger, daily_nav, nav_warnings, nav_with_benchmark

    ledger = Ledger(read_only=True)
    trades = ledger.trades()
    if trades.empty:
        return {
            "dates": [], "twr": [], "benchmark": [], "excess_annual": 0.0,
            "assets": [], "pnl": [],
            "data_quality": {
                "status": "verified", "partial": False, "stale": False,
                "issues": [], "by_symbol": {},
            },
            "market_provenance": {},
        }
    symbols = sorted(trades["symbol"].unique())
    start = str(pd.to_datetime(trades["date"]).min().date())
    end = market_date().isoformat()
    store = BarStore()
    prices: dict[str, pd.Series] = {}
    market_quality: dict[str, dict] = {}
    market_provenance: dict[str, list[dict]] = {}
    for symbol in symbols:
        try:
            envelope = read_history(symbol, start, end, store=store)
            prices[symbol] = envelope.require_data()["close"]
            market_quality[symbol] = envelope.quality.to_dict()
            market_provenance[symbol] = list(envelope.provenance)
        except Exception as e:
            logger.warning("实盘净值缺行情 %s: %s", symbol, e)
            market_quality[symbol] = {
                "status": "unavailable", "stale": False, "partial": True,
                "issues": [str(e)],
            }
            market_provenance[symbol] = []
    nav = daily_nav(ledger, pd.DataFrame(prices))
    if nav.empty:
        return {
            "dates": [], "twr": [], "benchmark": [], "excess_annual": 0.0,
            "assets": [], "pnl": [],
            "data_quality": {
                "status": "unavailable", "partial": True,
                "stale": any(bool(value.get("stale")) for value in market_quality.values()),
                "issues": ["没有足够行情构建实盘净值曲线"],
                "by_symbol": market_quality,
            },
            "market_provenance": market_provenance,
        }
    payload = {"dates": [], "twr": [], "benchmark": [], "excess_annual": 0.0}
    try:
        benchmark_envelope = read_history(benchmark, start, end, store=store)
        bench = benchmark_envelope.require_data()["close"]
        market_quality[benchmark] = benchmark_envelope.quality.to_dict()
        market_provenance[benchmark] = list(benchmark_envelope.provenance)
        payload = nav_with_benchmark(nav, bench)
    except Exception as e:
        logger.warning("基准 %s 加载失败: %s", benchmark, e)
        market_quality[benchmark] = {
            "status": "unavailable", "stale": False, "partial": True,
            "issues": [f"基准行情不可用：{e}"],
        }
        market_provenance[benchmark] = []
        payload["dates"] = [str(d.date()) for d in nav.index]
        payload["twr"] = [round(float(v), 6) for v in nav["twr_nav"]]
    payload["assets"] = _series_to_points(nav["total_assets"])
    payload["pnl"] = _series_to_points(nav["pnl"])
    payload["warnings"] = nav_warnings(nav)
    statuses = [str(value.get("status") or "unavailable") for value in market_quality.values()]
    issues = [
        f"{symbol}: {issue}"
        for symbol, quality in market_quality.items()
        for issue in (quality.get("issues") or [])
    ]
    missing_count = len(symbols) - len(prices)
    if missing_count:
        issues.append(f"{missing_count} 个持仓缺少可用行情")
    status = (
        "unavailable" if "unavailable" in statuses
        else "degraded" if "degraded" in statuses else "verified"
    )
    payload["data_quality"] = {
        "status": status,
        "partial": missing_count > 0 or any(bool(value.get("partial")) for value in market_quality.values()),
        "stale": any(bool(value.get("stale")) for value in market_quality.values()),
        "issues": list(dict.fromkeys(issues)),
        "by_symbol": market_quality,
    }
    payload["market_provenance"] = market_provenance
    return payload


def serve(*, reload: bool = False) -> None:  # pragma: no cover - 入口
    from quantmaster.server.lifecycle import run_uvicorn_foreground

    cfg = get_config().server
    run_uvicorn_foreground(
        app,
        host=cfg.host,
        port=cfg.port,
        log_level="warning",
        reload=reload,
    )
