"""FastAPI 本地服务：JSON API + Web 仪表盘。

启动：qm serve  （或 uvicorn quantmaster.server.app:app）
浏览器访问 http://127.0.0.1:8686
"""

from __future__ import annotations

import logging
import queue
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal, cast

import pandas as pd
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import Field

from quantmaster import __version__
from quantmaster.backtest.metrics import RISK_FREE, TRADING_DAYS
from quantmaster.backtest.quality import assess_panel_quality, assess_signal_quality
from quantmaster.config import get_config
from quantmaster.logging_config import redact_sensitive_text
from quantmaster.release import RELEASE_DATE, RELEASE_HISTORY_URL, RELEASES
from quantmaster.runtime.contracts import ContractModel
from quantmaster.runtime.json import StrictJSONResponse as JSONResponse
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.runtime.problems import OperationProblem, make_problem

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI):
    from quantmaster.analysis.stock_jobs import (
        get_stock_analysis_jobs,
        shutdown_stock_analysis_jobs,
    )
    from quantmaster.automation.runtime import get_runtime
    from quantmaster.backtest.paper_accounts import get_paper_automation_worker
    from quantmaster.backtest.workbench import get_backtest_worker
    from quantmaster.data.instruments import InstrumentStore
    from quantmaster.data.maintenance import data_refresh_manager
    from quantmaster.data.repair import get_data_repair_manager
    from quantmaster.lab.worker import get_worker
    from quantmaster.logging_config import current_log_path
    from quantmaster.research.jobs import get_research_job_manager
    from quantmaster.rotation.service import get_rotation_worker
    from quantmaster.runtime.maintenance import MaintenanceParticipant, maintenance_barrier
    from quantmaster.server.management import capture_runtime_baseline

    capture_runtime_baseline()
    # Startup must stay deterministic and bounded.  InstrumentStore installs the
    # bundled offline snapshot; external catalog refreshes are explicit maintenance
    # operations so a stopped app cannot leave network/database threads behind.
    InstrumentStore()
    runtime = get_runtime()
    runtime.start()
    worker = get_worker()
    backtest_worker = get_backtest_worker()
    research_worker = get_research_job_manager()
    rotation_worker = get_rotation_worker()
    repair_worker = get_data_repair_manager()
    stock_analysis_worker = get_stock_analysis_jobs()

    def drain_workers() -> None:
        # The data root can be hot-switched, which replaces this singleton.
        # Always resolve the current worker instead of stopping the startup copy.
        get_paper_automation_worker().stop()
        rotation_worker.stop()
        repair_worker.shutdown()
        data_refresh_manager.shutdown()
        research_worker.shutdown()
        backtest_worker.stop()
        worker.stop()
        runtime.stop()
        stock_analysis_worker.pause()

    def resume_workers() -> None:
        stock_analysis_worker.resume()
        runtime.start()
        research_worker.start()
        data_refresh_manager.start()
        repair_worker.start()
        backtest_worker.start()
        get_paper_automation_worker().start()
        rotation_worker.start()
        if get_config().lab.enabled:
            worker.start()

    unregister_maintenance = maintenance_barrier.register(MaintenanceParticipant(
        name=f"web-background-components:{uuid.uuid4().hex}",
        drain=drain_workers,
        resume=resume_workers,
        idle=lambda: (
            not data_refresh_manager.active
            and rotation_worker.idle
            and get_paper_automation_worker().idle
            and stock_analysis_worker.idle
        ),
    ))
    research_worker.start()
    stock_analysis_worker.start()
    data_refresh_manager.start()
    repair_worker.start()
    backtest_worker.start()
    get_paper_automation_worker().start()
    rotation_worker.start(bootstrap_local=True)
    if get_config().lab.enabled:
        worker.start()
    cfg = get_config()
    runtime_status = runtime.status()
    worker_status = worker.status()
    channels = ",".join(
        name for name, active in runtime_status.get("channels", {}).items() if active
    ) or "disabled"
    log_path = current_log_path()
    logger.info(
        "QuantMaster %s 已就绪 · http://%s:%s",
        __version__, cfg.server.host, cfg.server.port,
    )
    logger.info(
        "自动化 %s · Bot %s · Lab %s · 完整日志 %s",
        runtime_status.get("status", "unknown"), channels,
        worker_status.get("status", "unknown"),
        str(log_path) if log_path else "仅终端",
    )
    try:
        yield
    finally:
        drain_workers()
        unregister_maintenance()
        from quantmaster.ai.llm import close_llm_http_clients

        close_llm_http_clients()
        # 飞书 outbox 已在 drain_workers 中停止，不会在此处重新创建分析单例。
        shutdown_stock_analysis_jobs()
        logger.info("QuantMaster 已停止")


app = FastAPI(
    title="QuantMaster", version=__version__, lifespan=lifespan,
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


@app.exception_handler(RequestValidationError)
async def safe_validation_error(request: Request, exc: RequestValidationError):
    """设置请求的校验错误不回显输入值，防止替换中的密钥进入响应。"""
    content = {
        "error_id": _request_id(request),
        "problem": make_problem(
            "request_validation_failed",
            source="本地服务",
            title="提交内容需要修改",
            message="部分字段缺失或格式不正确。",
            action="按页面提示修改输入后重试。",
            blocking=True,
        ),
    }
    if (request.url.path.startswith("/api/v1/settings") or
            request.url.path.startswith("/api/v1/news/sources") or
            request.url.path.startswith("/api/v1/automation/channels/")):
        errors = [{key: value for key, value in item.items() if key not in {"input", "ctx"}}
                  for item in exc.errors()]
        content["detail"] = jsonable_encoder(errors)
        return JSONResponse(status_code=422, content=content)
    content["detail"] = jsonable_encoder(exc.errors())
    return JSONResponse(status_code=422, content=content)


@app.exception_handler(OperationProblem)
async def operation_problem(request: Request, exc: OperationProblem):
    """向普通请求和前端返回一致、可恢复的问题语义。"""
    return JSONResponse(
        status_code=exc.status_code,
        content=jsonable_encoder(exc.response(_request_id(request))),
    )


@app.middleware("http")
async def request_context_and_migration_lock(request: Request, call_next):
    """Apply the local security boundary, request context and migration lock."""
    from quantmaster.data.migration import migration_manager
    from quantmaster.runtime.maintenance import maintenance_barrier
    from quantmaster.server.security import (
        SecurityViolation,
        apply_security_headers,
        enforce_request_security,
    )

    request_id = _new_request_id()
    request.state.request_id = request_id
    path = request.url.path
    allowed = (path in {
                   "/api/v1/health/live", "/api/v1/health/ready",
                   "/api/v1/diagnostics", "/api/v1/release", "/api/v1/session", "/",
               } or
               path.startswith(("/static/", "/api/v1/data/migrations")) or
               (path == "/api/v1/settings" and request.method == "GET"))
    try:
        enforce_request_security(request)
        if ((migration_manager.active or maintenance_barrier.active)
                and path.startswith("/api/v1/") and not allowed):
            problem = make_problem(
                "data_migration_active",
                severity="warning",
                source="数据目录",
                title="数据目录正在迁移",
                message="迁移完成前，读取或写入数据的操作已暂停。",
                action="等待迁移完成后重试。",
                blocking=True,
            )
            response = JSONResponse(
                status_code=423,
                content={
                    "detail": problem["message"], "problem": problem,
                    "error_id": request_id,
                },
            )
        else:
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
        response = JSONResponse(
            status_code=exc.status_code,
            content={"detail": str(exc.detail), "problem": problem, "error_id": request_id},
        )
    except HTTPException as exc:
        problem = make_problem(
            "request_rejected",
            source="本地服务",
            title="请求未能执行",
            message=str(exc.detail),
            action="请检查请求内容后重试。",
            blocking=True,
        )
        response = JSONResponse(
            status_code=exc.status_code,
            content={"detail": str(exc.detail), "problem": problem, "error_id": request_id},
        )
    except Exception:
        logger.exception(
            "未处理的接口异常 request_id=%s method=%s path=%s",
            request_id, request.method, path,
        )
        problem = make_problem(
            "unhandled_server_error",
            source="本地服务",
            title="服务端处理失败",
            message="请求未能完成，详细原因已写入服务端日志。",
            action="稍后重试；如持续发生，请提供请求编号。",
            blocking=True,
        )
        response = JSONResponse(
            status_code=500,
            content={
                "detail": problem["message"],
                "problem": problem,
                "error_id": request_id,
            },
        )
    response.headers["X-Request-ID"] = request_id
    apply_security_headers(response)
    if response.status_code >= 400:
        logger.warning(
            "接口返回失败 request_id=%s method=%s path=%s status=%s",
            request_id, request.method, path, response.status_code,
        )
    return response

STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

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
    task: Callable[[ProgressEmitter], dict], request_id: str | None = None,
) -> StreamingResponse:
    """在线程中执行同步数据任务，以 NDJSON 持续发送真实阶段进度。"""
    request_id = request_id or _new_request_id()
    events: queue.Queue[dict | None] = queue.Queue()

    def emit(
        progress: int, phase: str, detail: str = "", partial: dict | None = None,
        level: str = "info",
    ) -> None:
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
            events.put({
                "type": "result", "data": task(emit), "request_id": request_id,
            })
        except OperationProblem as exc:
            logger.warning(
                "流式数据任务被业务门禁阻止 request_id=%s code=%s",
                request_id, exc.problem.get("code"),
            )
            event = {
                "type": "error", "message": exc.problem["message"],
                "problem": exc.problem,
                "error_id": request_id, "request_id": request_id,
            }
            if exc.data_quality is not None:
                event["data_quality"] = exc.data_quality
            events.put(event)
        except Exception as exc:
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
            events.put({
                "type": "error", "message": message,
                "problem": problem,
                "error_id": request_id, "request_id": request_id,
            })
        finally:
            events.put(None)

    threading.Thread(target=run, daemon=True).start()

    def generate() -> Iterator[str]:
        while True:
            event = events.get()
            if event is None:
                break
            yield strict_json_dumps(jsonable_encoder(event)) + "\n"

    return StreamingResponse(
        generate(), media_type="application/x-ndjson",
        headers={
            "Cache-Control": "no-cache", "X-Accel-Buffering": "no",
            "X-Request-ID": request_id,
        },
    )


# ---------- 页面 ----------

@app.get("/", include_in_schema=False)
def index(request: Request) -> HTMLResponse:
    from quantmaster.server.security import ensure_csrf_cookie

    template = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    page = (
        template.replace("%%QM_VERSION%%", __version__)
        .replace("%%QM_RELEASE_DATE%%", RELEASE_DATE)
        .replace("%%QM_TRADING_DAYS%%", str(TRADING_DAYS))
        .replace("%%QM_RISK_FREE%%", str(RISK_FREE))
    )
    csp = "; ".join((
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
    ))
    response = HTMLResponse(page, headers={
        "Cache-Control": "no-cache",
        "Content-Security-Policy": csp,
    })
    ensure_csrf_cookie(response, request)
    return response


@app.get("/api/v1/session")
def create_browser_session(request: Request, response: Response) -> dict:
    """Issue a stateless double-submit token for this local browser."""
    from quantmaster.server.security import attach_csrf_cookie, issue_csrf

    token = issue_csrf()
    attach_csrf_cookie(response, request, token)
    return {"csrf_token": token, "expires_in": 8 * 60 * 60, "local_only": True}


@app.get("/api/v1/health/live")
def liveness() -> dict:
    """Constant-time process liveness; deliberately performs no store access."""
    return {"status": "ok", "version": __version__, "release_date": RELEASE_DATE}


@app.get("/api/v1/health/ready")
def readiness() -> dict:
    """Check only the minimum local path needed to accept work."""
    root = get_config().data_root
    ready = root.is_dir()
    return {
        "status": "ready" if ready else "not_ready",
        "version": __version__,
        "data_root": str(root),
    }


@app.get("/api/v1/diagnostics")
def diagnostic_report() -> dict:
    from quantmaster.server.diagnostics import diagnostics

    return diagnostics()


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
        "releases": RELEASES,
        "history_url": RELEASE_HISTORY_URL,
    }


# ---------- 市场 ----------


PERSONAL_MARKET_GROUP = "我的股票"


def _personal_market_symbols() -> tuple[dict[str, str], dict[str, list[str]]]:
    """合并自选、关注与持有，保留来源分类并优先使用用户填写的名称。"""
    from quantmaster.data import load_stock_names
    from quantmaster.portfolio import AssetListStore, Ledger

    symbols: dict[str, str] = {}
    memberships: dict[str, list[str]] = {}

    def usable_name(value: object, symbol: str) -> str:
        name = str(value or "").strip()
        return "" if name.upper() == symbol else name

    lists = AssetListStore().all()
    for list_name in ("favorites", "following"):
        for item in lists.get(list_name, []):
            symbol = str(item["symbol"]).upper()
            name = usable_name(item.get("name"), symbol)
            symbols.setdefault(symbol, name)
            if name and not symbols[symbol]:
                symbols[symbol] = name
            memberships.setdefault(symbol, []).append(list_name)

    for position in Ledger().positions():
        if position.shares <= 0:
            continue
        symbol = str(position.symbol).upper()
        symbols.setdefault(symbol, "")
        memberships.setdefault(symbol, []).append("holdings")

    missing = [
        symbol for symbol, name in symbols.items()
        if not usable_name(name, symbol)
    ]
    if missing:
        cached_names = load_stock_names(missing)
        for symbol in missing:
            symbols[symbol] = usable_name(cached_names.get(symbol), symbol) or symbol
    return symbols, memberships


def _market_groups() -> dict[str, dict[str, str]]:
    from quantmaster.data.akshare_source import A_SHARE_INDEXES, FUTURES_MAIN
    from quantmaster.data.yfinance_source import GLOBAL_REFS

    return {
        "A股指数": dict(A_SHARE_INDEXES),
        "全球市场": {key: value[1] for key, value in GLOBAL_REFS.items()
                     if "=" not in key and "-" not in key},
        "商品与汇率": {
            **{key: value for key, value in FUTURES_MAIN.items() if not key.startswith("IF")},
            **{key: value[1] for key, value in GLOBAL_REFS.items()
               if "=" in key or "-" in key},
        },
    }


def _market_item(symbol: str, name: str, frame: pd.DataFrame, meta: dict | None) -> dict | None:
    if frame is None or frame.empty or "close" not in frame:
        return None
    close = frame["close"].dropna()
    if close.empty:
        return None
    checked_at = (meta or {}).get("checked_at")
    return {
        "symbol": symbol,
        "name": name,
        "last": round(float(close.iloc[-1]), 3),
        "change_pct": round(float(close.iloc[-1] / close.iloc[-2] - 1) * 100, 2)
        if len(close) > 1 else 0.0,
        "nav": _series_to_points(close / close.iloc[0]),
        "as_of": str(close.index[-1].date()),
        "checked_at": (
            pd.Timestamp.fromtimestamp(float(checked_at)).isoformat()
            if checked_at else ""
        ),
        "cache_status": str((meta or {}).get("last_status") or "ready"),
        "source": str((meta or {}).get("last_source") or "local-cache"),
        "freshness": (
            "stale" if str((meta or {}).get("last_status") or "ready")
            in {"stale", "refresh_failed"} else "ready"
        ),
    }


def _needs_market_sync(meta: dict | None, start: str, end: str, refresh: str) -> bool:
    if not meta:
        return True
    coverage_start = str(meta.get("coverage_start") or meta.get("start") or "")
    coverage_end = str(meta.get("coverage_end") or meta.get("end") or "")
    if not coverage_start or coverage_start > start or not coverage_end or coverage_end < end:
        return True
    if refresh == "incremental":
        return True
    checked_at = float(meta.get("checked_at") or 0)
    return time.time() - checked_at >= get_config().data.cache_days * 86400


def _sync_reference_market(
    symbols: list[str], start: str, end: str, refresh: str, store,
) -> tuple[dict[str, pd.DataFrame], dict[str, dict]]:
    """同步全球参考标的；每个标的独立选择兼容来源和错误状态。"""
    from quantmaster.data.reference_market import (
        ReferenceMarketUnavailable,
        fetch_reference,
    )
    from quantmaster.data.registry import _covers_requested_range
    from quantmaster.data.resilience import data_priority

    plans: dict[str, str] = {}
    for symbol in symbols:
        meta = store.metadata(symbol)
        if not _needs_market_sync(meta, start, end, refresh):
            continue
        cached = store.get(symbol)
        if cached is None or cached.empty:
            plans[symbol] = start
        elif str((meta or {}).get("coverage_start") or (meta or {}).get("start") or "") > start:
            plans[symbol] = start
        else:
            plans[symbol] = str(cached.index[max(0, len(cached) - 5)].date())

    failures: dict[str, dict] = {}

    def sync_one(symbol: str, fetch_start: str) -> None:
        try:
            with data_priority("interactive"):
                fetched = fetch_reference(symbol, fetch_start, end)
            frame = fetched.frame
            if frame is None or frame.empty or not _covers_requested_range(frame, fetch_start, end):
                raise ValueError("响应缺失有效交易日或内部过于稀疏")
            with store.lock(symbol):
                cached = store.get(symbol)
                merged = frame if cached is None or cached.empty else pd.concat([cached, frame])
                merged = merged[~merged.index.duplicated(keep="last")].sort_index()
                store.put(symbol, merged, replace=True)
                store.mark_checked(symbol, fetch_start, end, source=fetched.source)
        except ReferenceMarketUnavailable as exc:
            failures[symbol] = {
                "error_code": "all_sources_unavailable",
                "message": str(exc)[:500],
                "source_attempts": list(exc.attempts),
            }
            previous_source = str((store.metadata(symbol) or {}).get("last_source") or "")
            store.mark_status(symbol, "stale", source=previous_source)
        except Exception as exc:
            failures[symbol] = {
                "error_code": type(exc).__name__,
                "message": (str(exc).strip() or "同步失败")[:500],
                "source_attempts": [],
            }
            previous_source = str((store.metadata(symbol) or {}).get("last_source") or "")
            store.mark_status(symbol, "stale", source=previous_source)

    with ThreadPoolExecutor(
        max_workers=min(6, max(1, len(plans))),
        thread_name_prefix="reference-market",
    ) as executor:
        pending = [executor.submit(sync_one, symbol, fetch_start)
                   for symbol, fetch_start in plans.items()]
        for future in as_completed(pending):
            future.result()

    result: dict[str, pd.DataFrame] = {}
    for symbol in symbols:
        cached = store.get(symbol)
        if cached is not None and not cached.empty:
            sliced = cached.loc[start:end]
            if not sliced.empty:
                result[symbol] = sliced
    return result, failures


def _market_overview_data(
    start: str | None = None,
    progress: ProgressEmitter | None = None,
    refresh: Literal["auto", "incremental"] = "auto",
) -> dict:
    """个人股票与全球参考市场概览：返回近一年走势并优先发送本地缓存。"""
    from quantmaster.data import load_history
    from quantmaster.data.storage import BarStore
    from quantmaster.data.yfinance_source import GLOBAL_REFS

    end = pd.Timestamp.now().normalize()
    start_ts = pd.Timestamp(start) if start else end - pd.Timedelta(days=365)
    start_value, end_value = str(start_ts.date()), str(end.date())
    personal_symbols, personal_memberships = _personal_market_symbols()
    groups = {PERSONAL_MARKET_GROUP: personal_symbols, **_market_groups()}
    store = BarStore()
    items: dict[tuple[str, str], dict] = {}
    failures: dict[tuple[str, str], dict] = {}
    total = sum(len(symbols) for symbols in groups.values())
    completed = 0

    # 第一阶段只读本地缓存。即使所有上游均不可用，用户也能立即使用已有卡片。
    for group, symbols in groups.items():
        for symbol, name in symbols.items():
            cached = store.get(symbol, columns=["close"])
            if cached is None:
                continue
            item = _market_item(
                symbol, name, cached.loc[start_value:end_value], store.metadata(symbol))
            if item is None:
                continue
            if group == PERSONAL_MARKET_GROUP:
                item["memberships"] = personal_memberships.get(symbol, [])
            items[(group, symbol)] = item
            if progress:
                progress(
                    2, "读取本地市场缓存", f"{name} · 已显示本地数据",
                    {"kind": "market_item", "stage": "cache", "group": group, "item": item},
                )

    yahoo_symbols = set(GLOBAL_REFS)

    def one(group: str, symbol: str, name: str):
        frame = load_history(
            symbol, start_value, end_value, store=store,
            refresh=refresh, priority="interactive",
        )
        return group, symbol, name, frame

    futures = {}
    with ThreadPoolExecutor(max_workers=8, thread_name_prefix="market-sync") as executor:
        batch = [symbol for symbol in yahoo_symbols
                 if any(symbol in values for values in groups.values())]
        futures[executor.submit(
            _sync_reference_market, batch, start_value, end_value, refresh, store
        )] = ("__yahoo__", "", "")
        for group, symbols in groups.items():
            for symbol, name in symbols.items():
                if symbol in yahoo_symbols:
                    continue
                futures[executor.submit(one, group, symbol, name)] = (group, symbol, name)

        for future in as_completed(futures):
            group, symbol, name = futures[future]
            if group == "__yahoo__":
                try:
                    frames, reference_failures = future.result()
                except Exception as exc:
                    logger.debug("全球参考市场同步失败: %s", exc)
                    frames, reference_failures = {}, {}
                batch_lookup: dict[str, list[tuple[str, str]]] = {}
                for candidate_group, values in groups.items():
                    for candidate_symbol, candidate_name in values.items():
                        if candidate_symbol in yahoo_symbols:
                            batch_lookup.setdefault(candidate_symbol, []).append(
                                (candidate_group, candidate_name))
                for batch_symbol in batch:
                    frame = frames.get(batch_symbol)
                    for batch_group, batch_name in batch_lookup[batch_symbol]:
                        completed += 1
                        if batch_symbol in reference_failures:
                            failures[(batch_group, batch_symbol)] = reference_failures[batch_symbol]
                        item = _market_item(
                            batch_symbol, batch_name, frame, store.metadata(batch_symbol))
                        if item is None and batch_symbol in reference_failures:
                            item = items.get((batch_group, batch_symbol))
                            if item is not None:
                                item = {
                                    **item,
                                    "cache_status": "stale",
                                    "freshness": "stale",
                                }
                        if item is not None and batch_group == PERSONAL_MARKET_GROUP:
                            item["memberships"] = personal_memberships.get(batch_symbol, [])
                        if item is not None:
                            items[(batch_group, batch_symbol)] = item
                        if progress:
                            progress(
                                3 + round(94 * completed / max(1, total)), "同步市场行情",
                                f"{completed}/{total} · {batch_name} · "
                                f"{'已更新' if item else '沿用缓存或跳过'}",
                                {"kind": "market_item", "stage": "updated",
                                 "group": batch_group, "item": item} if item else None,
                                "info" if item else "warning",
                            )
                continue
            completed += 1
            try:
                market_result = cast(
                    tuple[str, str, str, pd.DataFrame],
                    future.result(),
                )
                frame = market_result[3]
                item = _market_item(symbol, name, frame, store.metadata(symbol))
            except Exception as exc:
                logger.debug("市场概览跳过 %s: %s", symbol, exc)
                item = items.get((group, symbol))
                failures[(group, symbol)] = {
                    "error_code": type(exc).__name__,
                    "message": (str(exc).strip() or "同步失败")[:500],
                    "source_attempts": [],
                }
            if item is not None and group == PERSONAL_MARKET_GROUP:
                item["memberships"] = personal_memberships.get(symbol, [])
            if item is not None:
                items[(group, symbol)] = item
            if progress:
                progress(
                    3 + round(94 * completed / max(1, total)), "同步市场行情",
                    f"{completed}/{total} · {name} · {'已更新' if item else '已跳过'}",
                    {"kind": "market_item", "stage": "updated", "group": group, "item": item}
                    if item is not None else None,
                    "info" if item else "warning",
                )

    result = {
        group: [items[(group, symbol)] for symbol in symbols if (group, symbol) in items]
        for group, symbols in groups.items()
    }
    unavailable = []
    group_statuses = {}
    for group, symbols in groups.items():
        stale = sum(
            str(items[(group, symbol)].get("freshness")) == "stale"
            for symbol in symbols if (group, symbol) in items
        )
        missing = [symbol for symbol in symbols if (group, symbol) not in items]
        for symbol in missing:
            missing_issue = failures.get((group, symbol), {})
            meta = store.metadata(symbol) or {}
            checked_at = float(meta.get("checked_at") or 0)
            unavailable.append({
                "group": group,
                "symbol": symbol,
                "name": symbols[symbol],
                "status": "unavailable",
                "error_code": missing_issue.get("error_code", "no_usable_data"),
                "message": missing_issue.get("message", "没有本地缓存，且数据源未返回可用行情"),
                "source_attempts": missing_issue.get("source_attempts", []),
                "last_success_at": (
                    pd.Timestamp.fromtimestamp(checked_at).isoformat() if checked_at else ""
                ),
            })
        group_statuses[group] = {
            "configured": len(symbols),
            "ready": len(result[group]) - stale,
            "stale": stale,
            "unavailable": len(missing),
            "issues": [
                {
                    "symbol": symbol,
                    "error_code": status_issue.get("error_code", "unavailable"),
                    "message": status_issue.get("message", "数据源未返回可用行情"),
                }
                for symbol in symbols
                if (status_issue := failures.get((group, symbol)))
            ],
        }
    return {
        "groups": result,
        "group_counts": {group: len(symbols) for group, symbols in groups.items()},
        "group_statuses": group_statuses,
        "unavailable_items": unavailable,
    }


@app.get("/api/v1/market/overview")
def market_overview(
    start: str | None = None, refresh: Literal["auto", "incremental"] = "auto",
) -> dict:
    """个人股票与全球参考市场概览。"""
    return _market_overview_data(start, refresh=refresh)


@app.get("/api/v1/market/overview/stream")
def market_overview_stream(
    request: Request,
    start: str | None = None,
    refresh: Literal["auto", "incremental"] = "auto",
) -> StreamingResponse:
    """市场概览流式进度；每完成一个标的就发送一行 NDJSON。"""

    def task(emit: ProgressEmitter) -> dict:
        emit(1, "准备市场清单", "检查本地缓存与数据源")
        result = _market_overview_data(start, emit, refresh)
        emit(100, "市场数据已就绪", "正在绘制行情卡片")
        return result

    return _progress_stream(task, _request_id(request))


@app.get("/api/v1/market/history/{symbol}")
def market_history(symbol: str, start: str = "2023-01-01", end: str | None = None,
                   frequency: str = "1d") -> dict:
    from quantmaster.data import load_bars
    from quantmaster.data.base import validate_frequency, validate_symbol

    end = end or str(pd.Timestamp.now().date())
    try:
        symbol = validate_symbol(symbol)
        frequency = validate_frequency(frequency)
    except ValueError:
        raise HTTPException(422, "标的代码或行情频率无效") from None
    try:
        df = load_bars(symbol, start, end, frequency=frequency)
    except Exception:
        logger.warning("行情历史读取失败 symbol=%s frequency=%s", symbol, frequency, exc_info=True)
        raise HTTPException(404, f"获取 {symbol} 失败，请查看本机日志") from None
    return {
        "symbol": symbol,
        "frequency": frequency,
        "kline": [
            [str(idx.date()) if frequency == "1d" else str(idx),
             round(row["open"], 3), round(row["close"], 3),
             round(row["low"], 3), round(row["high"], 3),
             round(row.get("volume", 0.0), 0)]
            for idx, row in df.iterrows()
        ],
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
    from quantmaster.data import load_panel
    from quantmaster.data.industry import load_industry_map
    from quantmaster.data.universe import load_universe
    from quantmaster.market import analyze_market, analyze_sectors

    end = req.end or str(pd.Timestamp.now().date())
    try:
        panel = load_panel(load_universe(req.universe), req.start, end)
        report = analyze_market(panel)
        past = report.pop("past").tail(req.history)
        report["past"] = [
            {"date": str(idx.date()), **{
                key: _json_scalar(value)
                for key, value in row.items()
            }} for idx, row in past.iterrows()
        ]
        report["sectors"] = []
        if req.sectors:
            sectors = analyze_sectors(panel, load_industry_map()).head(req.sector_top)
            report["sectors"] = sectors.to_dict(orient="records")
        return report
    except Exception as e:
        raise HTTPException(400, str(e)) from e


class SelectionRequest(ContractModel):
    universe: str = "demo"
    start: str = "2022-01-01"
    end: str | None = None
    top_n: int = 10
    horizon: Literal[1, 3, 5, 7] = 3
    profile: Literal["risk_adjusted", "short_term", "stable"] = "risk_adjusted"
    include_industry: bool = True
    save: bool = False


@app.post("/api/v1/research/selection/daily")
def selection_daily(req: SelectionRequest) -> dict:
    """收盘后生成适合次日执行的 1-7 日持有选股决策。"""
    from quantmaster.data import load_panel, load_stock_names
    from quantmaster.data.industry import load_industry_map
    from quantmaster.data.universe import load_universe
    from quantmaster.decision import hybrid_daily_selection

    end = req.end or str(pd.Timestamp.now().date())
    try:
        symbols = load_universe(req.universe)
        panel = load_panel(symbols, req.start, end)
        mapping = load_industry_map() if req.include_industry else {}
        names = load_stock_names(symbols)
        report = hybrid_daily_selection(
            panel, top_n=req.top_n, horizon=req.horizon, profile=req.profile,
            universe=req.universe, industry_map=mapping, name_map=names,
        )
        if req.save:
            from quantmaster.decision import DecisionStore

            DecisionStore().save(report, req.universe)
        return report
    except Exception as e:
        raise HTTPException(400, str(e)) from e


@app.get("/api/v1/research/selection/history")
def selection_history(
    universe: str | None = None, limit: int = 30, profile: str | None = None,
    horizon: Literal[1, 3, 5, 7] | None = None,
) -> dict:
    from quantmaster.data import load_stock_names
    from quantmaster.decision import DecisionStore

    snapshots = DecisionStore().history(
        universe, min(max(limit, 1), 200), profile=profile, horizon=horizon,
    )
    symbols = list(dict.fromkeys(
        pick.get("symbol", "")
        for snapshot in snapshots for pick in snapshot.get("picks", [])
        if pick.get("symbol")
    ))
    names = load_stock_names(symbols) if symbols else {}
    for snapshot in snapshots:
        for pick in snapshot.get("picks", []):
            if not pick.get("name") or pick.get("name") == "名称待同步":
                pick["name"] = names.get(pick.get("symbol"), "名称待同步")
    return {"snapshots": snapshots}


class DecisionDashboardRequest(ContractModel):
    universe: str = "demo"
    start: str = "2022-01-01"
    end: str | None = None
    top_n: int = Field(10, ge=1, le=50)
    horizon: Literal[1, 3, 5, 7] = 3
    profile: Literal["risk_adjusted", "short_term", "stable"] = "risk_adjusted"
    sector_top: int = Field(10, ge=1, le=50)
    history: int = Field(2600, ge=7, le=3000)
    save: bool = True


@app.post("/api/v1/research/decision/dashboard")
def decision_dashboard(req: DecisionDashboardRequest) -> dict:
    """决策工作台：只加载一次行情，同时生成市场、板块、选股和历史快照。"""
    try:
        return _decision_dashboard_data(req)
    except Exception as e:
        raise HTTPException(400, str(e)) from e


def _decision_dashboard_data(
    req: DecisionDashboardRequest, progress: ProgressEmitter | None = None,
) -> dict:
    from quantmaster.data import load_panel, load_stock_names
    from quantmaster.data.industry import load_industry_map
    from quantmaster.data.universe import load_universe
    from quantmaster.decision import DecisionStore, hybrid_daily_selection, resolve_policy
    from quantmaster.market import analyze_market, analyze_sectors

    end = req.end or str(pd.Timestamp.now().date())
    symbols = load_universe(req.universe)
    if progress:
        progress(3, "准备候选", f"共 {len(symbols)} 只标的")

    def on_symbol(completed: int, total: int, symbol: str, success: bool) -> None:
        if progress:
            progress(
                5 + round(58 * completed / max(1, total)),
                "同步候选行情",
                f"{completed}/{total} · {symbol} · {'已就绪' if success else '已跳过'}",
                {
                    "kind": "decision_symbol", "symbol": symbol,
                    "success": success, "completed": completed, "total": total,
                },
                "info" if success else "warning",
            )

    panel = load_panel(
        symbols, req.start, end, progress=on_symbol if progress else None)
    if progress:
        progress(67, "加载行业与名称", "优先复用本地缓存")
    mapping = load_industry_map()
    names = load_stock_names(symbols)
    if progress:
        progress(78, "计算牛熊与趋势", "汇总 MACD、资金量和市场宽度")
    market = analyze_market(panel)
    past = market.pop("past").tail(req.history)
    market["past"] = [
        {"date": str(idx.date()), **{
            key: _json_scalar(value) for key, value in row.items()
        }} for idx, row in past.iterrows()
    ]
    if progress:
        # 中间态先给最近约一年，足够默认 3M 视图且避免把最长 10Y 历史
        # 在 partial 与最终 result 中重复传输两遍；最终事件仍返回完整窗口。
        preview_market = {**market, "past": market["past"][-260:]}
        progress(
            84, "市场状态已就绪", "牛熊、宽度与未来概率可先查看",
            {"kind": "decision_market", "market": preview_market},
        )
        progress(86, "聚合板块强弱", "按行业计算趋势与上涨宽度")
    market["sectors"] = analyze_sectors(panel, mapping).head(
        req.sector_top).to_dict(orient="records")
    if progress:
        progress(
            90, "板块数据已就绪", f"已生成 {len(market['sectors'])} 个板块状态",
            {"kind": "decision_sectors", "sectors": market["sectors"]},
        )
        progress(91, "匹配 Quant Lab Champion", f"{req.profile} · {req.horizon} 日")
    policy = resolve_policy(
        req.universe, req.horizon, req.profile, symbols=list(panel["close"].columns),
    )
    if progress:
        progress(
            92, "决策模型已就绪", policy["profile_label"],
            {"kind": "decision_policy", "policy": policy},
        )
        progress(93, "生成每日候选", f"目标持有 {req.horizon} 日")
    selection = hybrid_daily_selection(
        panel, top_n=req.top_n, horizon=req.horizon, profile=req.profile,
        universe=req.universe, industry_map=mapping, name_map=names,
        policy_snapshot=policy,
    )
    if progress:
        progress(
            96, "每日候选已就绪", f"已生成 {len(selection.get('picks', []))} 只候选",
            {"kind": "decision_selection", "selection": selection},
        )
    store = DecisionStore()
    if req.save:
        if progress:
            progress(97, "保存决策快照", "写入本地 SQLite")
        store.save(selection, req.universe)
    history = store.history(req.universe, limit=10, profile=req.profile)
    # 旧版本快照没有 name 字段；响应时补齐，避免历史区继续只显示代码。
    for snapshot in history:
        for pick in snapshot.get("picks", []):
            if not pick.get("name") or pick.get("name") == "名称待同步":
                pick["name"] = names.get(pick.get("symbol"), "名称待同步")
    if progress:
        progress(
            99, "历史快照已就绪", f"已读取 {len(history)} 条本地记录",
            {"kind": "decision_history", "history": history},
        )
    result = {
        "market": market,
        "selection": selection,
        "history": history,
        "model_snapshot": selection.get("model_snapshot"),
        "data_quality": selection.get("data_quality"),
    }
    if progress:
        progress(100, "决策数据已就绪", f"生成 {len(selection.get('picks', []))} 只候选")
    return result


@app.post("/api/v1/research/decision/dashboard/stream")
def decision_dashboard_stream(
    req: DecisionDashboardRequest, request: Request,
) -> StreamingResponse:
    """决策工作台流式进度；最终 result 事件携带完整原接口响应。"""
    return _progress_stream(
        lambda emit: _decision_dashboard_data(req, emit), _request_id(request),
    )


# ---------- 因子 ----------

@app.get("/api/v1/research/factors")
def factors_list() -> dict:
    from quantmaster.ai.sentiment import list_news_factors
    from quantmaster.factors.fundamental import list_fundamental_factors
    from quantmaster.factors.library import list_factors
    from quantmaster.lab.models import factor_name_key
    from quantmaster.lab.store import LabStore

    factors = list_factors() + list_fundamental_factors() + list_news_factors()
    for item in factors:
        item.setdefault("source", "builtin")
    known_names = {factor_name_key(item["name"]) for item in factors}
    try:
        for item in LabStore().runtime_factors():
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
    neutralize: bool = False          # 行业中性化（行业内去均值）


@app.post("/api/v1/research/factors/test")
def factors_test(req: FactorTestRequest) -> dict:
    from quantmaster.data import load_panel
    from quantmaster.data.universe import load_universe
    from quantmaster.factors import analyze_factor, compute_factor
    from quantmaster.factors.fundamental import resolve_factor

    end = req.end or str(pd.Timestamp.now().date())
    try:
        symbols = load_universe(req.universe)
        factor = resolve_factor(req.expression, symbols, req.start, end)
        panel = load_panel(symbols, req.start, end)
        values = compute_factor(factor, panel)
        neutralized = False
        if req.neutralize:
            from quantmaster.data.industry import load_industry_map
            from quantmaster.factors.neutral import industry_neutralize

            mapping = load_industry_map()
            if mapping:
                values = industry_neutralize(values, mapping)
                neutralized = True
        report = analyze_factor(values, panel["close"], name=factor.name,
                                quantiles=req.quantiles)
    except Exception as e:
        raise HTTPException(400, str(e)) from e
    return {
        "summary": report.summary(),
        "neutralized": neutralized,
        "ic_series": _series_to_points(report.ic_series.rolling(20, min_periods=5).mean()),
        "quantile_nav": {col: _series_to_points(report.quantile_returns[col])
                         for col in report.quantile_returns.columns},
    }


# ---------- 回测 ----------

class BacktestRequest(ContractModel):
    strategy: Literal["factor", "swing"] = "factor"
    factor: str = "mom_20d"
    universe: str = "demo"
    start: str = "2022-01-01"
    end: str | None = None
    top_n: int = Field(5, ge=1, le=200)
    rebalance: Literal["D", "W", "M"] = "W"
    benchmark: str = "000300.SH"
    initial_capital: float = Field(1_000_000.0, ge=10_000)
    stop_loss: float | None = None
    take_profit: float | None = None
    weighting: Literal["equal", "ic"] = "equal"
    holding_days: int = Field(3, ge=1, le=7)
    allow_partial: bool = False


@app.post("/api/v1/backtest/run")
def backtest_run(req: BacktestRequest) -> dict:
    from quantmaster.backtest import BacktestConfig, FactorStrategy, full_report, run_backtest
    from quantmaster.backtest.strategy import MultiFactorStrategy
    from quantmaster.data import load_history, load_panel
    from quantmaster.data.universe import load_universe
    from quantmaster.factors.fundamental import resolve_factor

    end = req.end or str(pd.Timestamp.now().date())
    quality: dict = {}
    warnings: list[dict] = []
    try:
        symbols = load_universe(req.universe)
        panel = load_panel(symbols, req.start, end)
        quality, panel_warnings = assess_panel_quality(
            panel, symbols, minimum_symbols=req.top_n,
            allow_partial=req.allow_partial,
        )
        warnings.extend(panel_warnings)
        from quantmaster.backtest.spec import split_factor_references

        names = split_factor_references(req.factor)
        if req.strategy == "swing":
            from quantmaster.backtest import SwingStrategy

            strategy = SwingStrategy(top_n=req.top_n, holding_days=req.holding_days)
        elif len(names) > 1:
            strategy = MultiFactorStrategy(
                [resolve_factor(n, symbols, req.start, end) for n in names],
                top_n=req.top_n, rebalance=req.rebalance, weighting=req.weighting)
        else:
            if not names:
                raise ValueError("因子表达式不能为空")
            strategy = FactorStrategy(resolve_factor(names[0], symbols, req.start, end),
                                      top_n=req.top_n, rebalance=req.rebalance)
        weights = strategy.target_weights(panel)
        warnings.extend(assess_signal_quality(
            panel, weights, quality, allow_partial=req.allow_partial,
        ))
        benchmark = None
        if req.benchmark:
            try:
                benchmark = load_history(req.benchmark, req.start, end)["close"]
                if benchmark.empty:
                    raise ValueError("基准没有可用收盘价")
                quality["benchmark_status"] = "complete"
            except Exception as e:
                logger.warning("基准 %s 加载失败: %s", req.benchmark, e)
                quality["benchmark_status"] = "unavailable"
                quality["status"] = "partial"
                warnings.append(make_problem(
                    "benchmark_unavailable",
                    severity="warning",
                    source="策略回测",
                    title="基准数据不可用",
                    message=f"{req.benchmark} 未能加载，超额收益和信息比率将不可用。",
                    action="回测主体结果仍可使用；需要相对收益时请刷新基准行情后重试。",
                    problem_id=f"backtest:benchmark:{req.benchmark}",
                ))
        else:
            quality["benchmark_status"] = "not_requested"
        result = run_backtest(panel, weights,
                              BacktestConfig(initial_capital=req.initial_capital,
                                             stop_loss=req.stop_loss,
                                             take_profit=req.take_profit),
                              benchmark_close=benchmark)
        if not result.trades:
            problem = make_problem(
                "no_valid_trades",
                source="策略回测",
                title="回测没有产生有效成交",
                message="所有信号均未形成可验证成交，不能把空净值曲线当作有效回测结果。",
                action="检查成交日价格、涨跌停限制、资金规模和策略信号后重试。",
                blocking=True,
                problem_id="backtest:no-valid-trades",
            )
            quality["status"] = "blocked"
            raise OperationProblem(422, problem, data_quality=quality)
    except OperationProblem:
        raise
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, str(e)) from e

    drawdown = 1.0 - result.nav / result.nav.cummax()
    report = full_report(result)
    quality["warning_count"] = len(warnings)
    quality["trade_count"] = len(result.trades)
    return {
        "strategy": strategy.name,
        "metrics": result.metrics,
        "nav": _series_to_points(result.nav),
        "benchmark_nav": _series_to_points(result.benchmark_nav)
        if result.benchmark_nav is not None else [],
        "drawdown": _series_to_points(-drawdown),
        "trades": [t.__dict__ for t in result.trades[-200:]],
        "yearly": report["yearly"],
        "monthly": report["monthly"],
        "trade_stats": report["trade_stats"],
        "data_quality": quality,
        "warnings": warnings,
    }


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
    from quantmaster.data import load_panel
    from quantmaster.data.universe import load_universe
    from quantmaster.factors.fundamental import resolve_factor

    end = req.end or str(pd.Timestamp.now().date())
    try:
        symbols = load_universe(req.universe)
        factor = resolve_factor(req.expression, symbols, req.start, end)
        panel = load_panel(symbols, req.start, end)
        result = train_test_ic(factor, panel, split=req.split)
        segments = walk_forward_ic(factor, panel, n_splits=req.n_splits)
        result["segments"] = [
            {k: (None if pd.isna(v) else (float(v) if isinstance(v, (int, float)) else str(v)))
             for k, v in row.items()}
            for _, row in segments.iterrows()
        ]
    except Exception as e:
        raise HTTPException(400, str(e)) from e
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
    from quantmaster.data import load_panel
    from quantmaster.data.universe import load_universe
    from quantmaster.factors.mining import GeneticMiner

    end = req.end or str(pd.Timestamp.now().date())
    try:
        panel = load_panel(load_universe(req.universe), req.start, end)
        miner = GeneticMiner(population=req.population, generations=req.generations,
                             seed=req.seed)
        mined = miner.mine(panel, top_n=req.top_n, progress=False)
    except Exception as e:
        raise HTTPException(400, str(e)) from e
    return {"factors": [m.__dict__ for m in mined]}


class MineLLMRequest(ContractModel):
    universe: str = "demo"
    start: str = "2022-01-01"
    end: str | None = None
    n: int = 8
    rounds: int = 2


@app.post("/api/v1/research/mining/llm")
def mine_llm(req: MineLLMRequest) -> dict:
    from quantmaster.data import load_panel
    from quantmaster.data.universe import load_universe
    from quantmaster.factors.mining import LLMFactorMiner

    end = req.end or str(pd.Timestamp.now().date())
    try:
        panel = load_panel(load_universe(req.universe), req.start, end)
        miner = LLMFactorMiner()
        mined = miner.mine(panel, n=req.n, rounds=req.rounds)
    except Exception as e:
        raise HTTPException(400, str(e)) from e
    return {"factors": [m.__dict__ for m in mined]}


# ---------- 模拟盘 ----------

class PaperRunRequest(ContractModel):
    strategy: str = "factor"         # factor | swing
    factor: str = "mom_20d"
    universe: str = "demo"
    top_n: int = 5
    rebalance: str = "W"
    holding_days: int = Field(3, ge=1, le=7)
    initial_capital: float = 1_000_000.0


@app.post("/api/v1/paper/run")
def paper_run(req: PaperRunRequest, request: Request) -> dict:
    """兼容入口：只生成提案，不再按收盘价直接写入成交。"""
    from quantmaster.backtest.paper_accounts import get_paper_service
    from quantmaster.backtest.spec import PaperAccountSpec
    from quantmaster.server.management import _require_csrf

    _require_csrf(request)
    try:
        service = get_paper_service()
        account = next(
            (item for item in service.store.accounts() if item["name"] == "默认模拟盘"), None,
        )
        if req.strategy == "swing":
            strategy = {
                "kind": "swing", "top_n": req.top_n,
                "holding_days": req.holding_days, "cap_weight": 0.25,
            }
        elif req.strategy == "factor":
            strategy = {
                "kind": "factor", "factor": req.factor, "top_n": req.top_n,
                "rebalance": req.rebalance, "weighting": "equal", "cap_weight": 0.35,
            }
        else:
            raise ValueError("strategy 仅支持 factor/swing")
        if account is None:
            account = service.create_account(PaperAccountSpec.model_validate({
                "name": "默认模拟盘", "strategy": strategy, "universe": req.universe,
                "initial_capital": req.initial_capital, "mode": "manual",
            }))
        elif account["strategy"] != strategy or account["universe"] != req.universe:
            raise ValueError("默认模拟盘的策略快照不同；请在新版模拟盘中新建账户")
        result = service.propose(account["id"])
        return {
            **result,
            "deprecated": True,
            "notice": "此兼容入口只生成提案；请确认后等待下一交易日开盘撮合。",
        }
    except Exception as e:
        raise HTTPException(400, str(e)) from e


@app.get("/api/v1/paper/report")
def paper_report() -> dict:
    """模拟盘报告 + TWR 净值序列（行情只走本地缓存，不触网）。"""
    from quantmaster.data.storage import BarStore
    from quantmaster.portfolio import Ledger, daily_nav, ledger_report, nav_warnings

    ledger = Ledger(name="paper")
    trades = ledger.trades()
    store = BarStore()
    prices: dict[str, pd.Series] = {}
    price_map: dict[str, float] = {}
    for symbol in (sorted(trades["symbol"].unique()) if len(trades) else []):
        cached = store.get(symbol)
        if cached is not None and not cached.empty:
            prices[symbol] = cached["close"]
            price_map[symbol] = float(cached["close"].dropna().iloc[-1])
    report = ledger_report(ledger, prices=price_map)
    payload: dict = {"report": report, "dates": [], "twr": [], "warnings": []}
    if len(trades) and prices:
        nav = daily_nav(ledger, pd.DataFrame(prices))
        if not nav.empty:
            payload["dates"] = [str(d.date()) for d in nav.index]
            payload["twr"] = [round(float(v), 6) for v in nav["twr_nav"]]
            payload["warnings"] = nav_warnings(nav)
    return payload


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

    lists = AssetListStore().all()
    store = BarStore()
    quote_cache: dict[str, dict] = {}

    def quote(symbol: str) -> dict:
        if symbol not in quote_cache:
            quote_cache[symbol] = _cached_asset_quote(symbol, store)
        return quote_cache[symbol]

    payload: dict[str, list[dict]] = {}
    for list_name, items in lists.items():
        payload[list_name] = [{**item, **quote(item["symbol"])} for item in items]

    holdings = []
    for position in Ledger().positions():
        if position.shares <= 0:
            continue
        item = {"symbol": position.symbol, **quote(position.symbol)}
        last = item["last"] if item["last"] is not None else position.avg_cost
        item.update({
            "shares": round(position.shares, 2),
            "avg_cost": round(position.avg_cost, 4),
            "market_value": round(position.shares * last, 2),
            "unrealized_pnl": round(position.shares * (last - position.avg_cost), 2),
            "pnl_pct": round(last / position.avg_cost - 1, 4) if position.avg_cost else None,
            "realized_pnl": round(position.realized_pnl, 2),
        })
        holdings.append(item)
    payload["holdings"] = sorted(holdings, key=lambda item: item["market_value"], reverse=True)
    return payload


@app.get("/api/v1/portfolio/lists")
def asset_lists_get() -> dict:
    """自选、关注和实盘持有；报价仅复用本地缓存。"""
    return _asset_lists_payload()


@app.post("/api/v1/portfolio/lists/{list_name}")
def asset_lists_add(
    list_name: Literal["favorites", "following"], item: AssetListIn,
) -> dict:
    from quantmaster.portfolio import AssetListStore

    try:
        AssetListStore().add(list_name, item.symbol, item.name)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    return _asset_lists_payload()


@app.delete("/api/v1/portfolio/lists/{list_name}/{symbol}")
def asset_lists_remove(
    list_name: Literal["favorites", "following"], symbol: str,
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

    return ledger_report(Ledger())


@app.get("/api/v1/portfolio/ledger/trades")
def ledger_get_trades() -> dict:
    from quantmaster.portfolio import Ledger

    df = Ledger().trades()
    return {"trades": df.to_dict(orient="records")}


@app.get("/api/v1/portfolio/ledger/nav")
def ledger_get_nav(benchmark: str = "000300.SH") -> dict:
    """实盘每日净值（TWR）与基准对比。行情走本地缓存，缺失标的按最近成交价估值。"""
    from quantmaster.data import load_history
    from quantmaster.data.storage import BarStore
    from quantmaster.portfolio import Ledger, daily_nav, nav_warnings, nav_with_benchmark

    ledger = Ledger()
    trades = ledger.trades()
    if trades.empty:
        return {"dates": [], "twr": [], "benchmark": [], "excess_annual": 0.0,
                "assets": [], "pnl": []}
    symbols = sorted(trades["symbol"].unique())
    start = str(pd.to_datetime(trades["date"]).min().date())
    end = str(pd.Timestamp.now().date())
    store = BarStore()
    prices: dict[str, pd.Series] = {}
    for symbol in symbols:
        try:
            prices[symbol] = load_history(symbol, start, end, store=store)["close"]
        except Exception as e:
            logger.warning("实盘净值缺行情 %s: %s", symbol, e)
    nav = daily_nav(ledger, pd.DataFrame(prices))
    if nav.empty:
        return {"dates": [], "twr": [], "benchmark": [], "excess_annual": 0.0,
                "assets": [], "pnl": []}
    payload = {"dates": [], "twr": [], "benchmark": [], "excess_annual": 0.0}
    try:
        bench = load_history(benchmark, start, end, store=store)["close"]
        payload = nav_with_benchmark(nav, bench)
    except Exception as e:
        logger.warning("基准 %s 加载失败: %s", benchmark, e)
        payload["dates"] = [str(d.date()) for d in nav.index]
        payload["twr"] = [round(float(v), 6) for v in nav["twr_nav"]]
    payload["assets"] = _series_to_points(nav["total_assets"])
    payload["pnl"] = _series_to_points(nav["pnl"])
    payload["warnings"] = nav_warnings(nav)
    return payload


def serve() -> None:  # pragma: no cover - 入口
    from quantmaster.server.lifecycle import run_uvicorn_foreground

    cfg = get_config().server
    run_uvicorn_foreground(app, host=cfg.host, port=cfg.port, log_level="warning")
