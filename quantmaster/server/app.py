"""FastAPI 本地服务：JSON API + Web 仪表盘。

启动：qm serve  （或 uvicorn quantmaster.server.app:app）
浏览器访问 http://127.0.0.1:8686
"""

from __future__ import annotations

import json
import logging
import queue
import re
import threading
import uuid
from collections.abc import Callable, Iterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

import pandas as pd
from fastapi import FastAPI, HTTPException, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from quantmaster import __version__
from quantmaster.config import get_config
from quantmaster.release import RELEASE_DATE, RELEASES

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI):
    from quantmaster.automation.runtime import get_runtime
    from quantmaster.lab.worker import get_worker

    runtime = get_runtime()
    runtime.start()
    worker = get_worker()
    if get_config().lab.enabled:
        worker.start()
    try:
        yield
    finally:
        worker.stop()
        runtime.stop()


app = FastAPI(title="QuantMaster", version=__version__, lifespan=lifespan)


def _new_request_id() -> str:
    """生成可在前端提示和后端日志间关联的短请求编号。"""
    return uuid.uuid4().hex[:12]


def _request_id(request: Request) -> str:
    return getattr(request.state, "request_id", "") or _new_request_id()


def _safe_client_error(exc: Exception) -> str:
    """保留可操作信息，同时避免第三方异常把凭据回显到浏览器。"""
    message = str(exc).strip() or "数据任务未完成"
    message = re.sub(
        r"(?i)((?:api[_-]?key|token|authorization)\s*[=:]\s*)[^\s,;]+",
        r"\1***", message,
    )
    message = re.sub(r"(?i)(bearer\s+)[^\s,;]+", r"\1***", message)
    message = re.sub(r"\bsk-[A-Za-z0-9_-]{8,}\b", "sk-***", message)
    return message[:297] + "…" if len(message) > 300 else message


@app.exception_handler(RequestValidationError)
async def safe_validation_error(request: Request, exc: RequestValidationError):
    """设置请求的校验错误不回显输入值，防止替换中的密钥进入响应。"""
    content = {"error_id": _request_id(request)}
    if (request.url.path.startswith("/api/settings") or
            request.url.path.startswith("/api/news/sources") or
            request.url.path.startswith("/api/automation/channels/")):
        errors = [{key: value for key, value in item.items() if key not in {"input", "ctx"}}
                  for item in exc.errors()]
        content["detail"] = jsonable_encoder(errors)
        return JSONResponse(status_code=422, content=content)
    content["detail"] = jsonable_encoder(exc.errors())
    return JSONResponse(status_code=422, content=content)


@app.middleware("http")
async def request_context_and_migration_lock(request: Request, call_next):
    """关联前后端错误，并在迁移期间冻结会读写数据的接口。"""
    from quantmaster.data.migration import migration_manager

    request_id = _new_request_id()
    request.state.request_id = request_id
    path = request.url.path
    allowed = (path in {"/api/health", "/api/release", "/"} or
               path.startswith(("/static/", "/api/settings/migration")) or
               (path == "/api/settings" and request.method == "GET"))
    try:
        if migration_manager.active and path.startswith("/api/") and not allowed:
            response = JSONResponse(
                status_code=423,
                content={"detail": "数据目录正在迁移，请稍后重试", "error_id": request_id},
            )
        else:
            response = await call_next(request)
    except Exception:
        logger.exception(
            "未处理的接口异常 request_id=%s method=%s path=%s",
            request_id, request.method, path,
        )
        response = JSONResponse(
            status_code=500,
            content={
                "detail": "服务端处理失败，请稍后重试；如持续发生，请提供请求编号",
                "error_id": request_id,
            },
        )
    response.headers["X-Request-ID"] = request_id
    if response.status_code >= 400:
        logger.warning(
            "接口返回失败 request_id=%s method=%s path=%s status=%s",
            request_id, request.method, path, response.status_code,
        )
    return response

STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

from quantmaster.server.automation import router as automation_router  # noqa: E402
from quantmaster.server.lab import router as lab_router  # noqa: E402
from quantmaster.server.management import router as management_router  # noqa: E402
from quantmaster.server.news import router as news_router  # noqa: E402

app.include_router(management_router)
app.include_router(automation_router)
app.include_router(lab_router)
app.include_router(news_router)


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
        except Exception as exc:
            logger.exception("流式数据任务失败 request_id=%s", request_id)
            message = _safe_client_error(exc)
            events.put({
                "type": "error", "message": message,
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
            yield json.dumps(
                jsonable_encoder(event), ensure_ascii=False, allow_nan=False) + "\n"

    return StreamingResponse(
        generate(), media_type="application/x-ndjson",
        headers={
            "Cache-Control": "no-cache", "X-Accel-Buffering": "no",
            "X-Request-ID": request_id,
        },
    )


# ---------- 页面 ----------

@app.get("/", include_in_schema=False)
def index() -> HTMLResponse:
    template = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    page = template.replace("%%QM_VERSION%%", __version__).replace(
        "%%QM_RELEASE_DATE%%", RELEASE_DATE)
    return HTMLResponse(page, headers={"Cache-Control": "no-cache"})


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok", "version": __version__, "release_date": RELEASE_DATE}


@app.get("/api/release")
def release_info() -> dict:
    """前端版本入口使用的发布信息，与应用包版本保持一致。"""
    return {
        "version": __version__,
        "release_date": RELEASE_DATE,
        "releases": RELEASES,
    }


# ---------- 市场 ----------

def _market_overview_data(
    start: str | None = None, progress: ProgressEmitter | None = None,
) -> dict:
    """全球参考市场概览：A股/港股/美日韩指数 + 商品期货的近一年走势。"""
    from quantmaster.data import load_history
    from quantmaster.data.akshare_source import A_SHARE_INDEXES, FUTURES_MAIN
    from quantmaster.data.yfinance_source import GLOBAL_REFS

    end = pd.Timestamp.now().normalize()
    start_ts = pd.Timestamp(start) if start else end - pd.Timedelta(days=365)

    groups = {
        "A股指数": {k: v for k, v in A_SHARE_INDEXES.items()},
        "全球市场": {k: v[1] for k, v in GLOBAL_REFS.items() if "=" not in k and "-" not in k},
        "商品与汇率": {**{k: v for k, v in FUTURES_MAIN.items() if not k.startswith("IF")},
                     **{k: v[1] for k, v in GLOBAL_REFS.items() if "=" in k or "-" in k}},
    }
    result: dict[str, list] = {}
    total = sum(len(symbols) for symbols in groups.values())
    completed = 0
    for group, symbols in groups.items():
        rows = []
        for symbol, name in symbols.items():
            success = False
            item = None
            try:
                df = load_history(symbol, str(start_ts.date()), str(end.date()))
                if df.empty:
                    continue
                close = df["close"].dropna()
                item = {
                    "symbol": symbol,
                    "name": name,
                    "last": round(float(close.iloc[-1]), 3),
                    "change_pct": round(float(close.iloc[-1] / close.iloc[-2] - 1) * 100, 2)
                    if len(close) > 1 else 0.0,
                    "nav": _series_to_points(close / close.iloc[0]),
                }
                rows.append(item)
                success = True
            except Exception as e:
                logger.warning("市场概览跳过 %s: %s", symbol, e)
            finally:
                completed += 1
                if progress:
                    progress(
                        3 + round(94 * completed / max(1, total)),
                        "同步全球市场",
                        f"{completed}/{total} · {name} · {'已就绪' if success else '已跳过'}",
                        {"kind": "market_item", "group": group, "item": item}
                        if item is not None else None,
                        "info" if success else "warning",
                    )
        result[group] = rows
    return {"groups": result}


@app.get("/api/market/overview")
def market_overview(start: str | None = None) -> dict:
    """全球参考市场概览：A股/港股/美日韩指数 + 商品期货的近一年走势。"""
    return _market_overview_data(start)


@app.get("/api/market/overview/stream")
def market_overview_stream(request: Request, start: str | None = None) -> StreamingResponse:
    """市场概览流式进度；每完成一个标的就发送一行 NDJSON。"""

    def task(emit: ProgressEmitter) -> dict:
        emit(1, "准备市场清单", "检查本地缓存与数据源")
        result = _market_overview_data(start, emit)
        emit(100, "市场数据已就绪", "正在绘制行情卡片")
        return result

    return _progress_stream(task, _request_id(request))


@app.get("/api/market/history/{symbol}")
def market_history(symbol: str, start: str = "2023-01-01", end: str | None = None,
                   frequency: str = "1d") -> dict:
    from quantmaster.data import load_bars

    end = end or str(pd.Timestamp.now().date())
    try:
        df = load_bars(symbol, start, end, frequency=frequency)
    except Exception as e:
        raise HTTPException(404, f"获取 {symbol} 失败: {e}") from e
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


class RegimeRequest(BaseModel):
    universe: str = "demo"
    start: str = "2022-01-01"
    end: str | None = None
    sectors: bool = True
    sector_top: int = 10
    history: int = Field(60, ge=7, le=3000)


@app.post("/api/market/regime")
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


class SelectionRequest(BaseModel):
    universe: str = "demo"
    start: str = "2022-01-01"
    end: str | None = None
    top_n: int = 10
    horizon: int = Field(3, ge=1, le=7)
    include_industry: bool = True
    save: bool = False


@app.post("/api/selection/daily")
def selection_daily(req: SelectionRequest) -> dict:
    """收盘后生成适合次日执行的 1-7 日持有选股决策。"""
    from quantmaster.data import load_panel, load_stock_names
    from quantmaster.data.industry import load_industry_map
    from quantmaster.data.universe import load_universe
    from quantmaster.decision import daily_selection

    end = req.end or str(pd.Timestamp.now().date())
    try:
        symbols = load_universe(req.universe)
        panel = load_panel(symbols, req.start, end)
        mapping = load_industry_map() if req.include_industry else {}
        names = load_stock_names(symbols)
        report = daily_selection(panel, top_n=req.top_n, horizon=req.horizon,
                                 industry_map=mapping, name_map=names)
        if req.save:
            from quantmaster.decision import DecisionStore

            DecisionStore().save(report, req.universe)
        return report
    except Exception as e:
        raise HTTPException(400, str(e)) from e


@app.get("/api/selection/history")
def selection_history(universe: str | None = None, limit: int = 30) -> dict:
    from quantmaster.data import load_stock_names
    from quantmaster.decision import DecisionStore

    snapshots = DecisionStore().history(universe, min(max(limit, 1), 200))
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


class DecisionDashboardRequest(BaseModel):
    universe: str = "demo"
    start: str = "2022-01-01"
    end: str | None = None
    top_n: int = Field(10, ge=1, le=50)
    horizon: int = Field(3, ge=1, le=7)
    sector_top: int = Field(10, ge=1, le=50)
    history: int = Field(2600, ge=7, le=3000)
    save: bool = True


@app.post("/api/decision/dashboard")
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
    from quantmaster.decision import DecisionStore, daily_selection
    from quantmaster.market import analyze_market, analyze_sectors

    end = req.end or str(pd.Timestamp.now().date())
    symbols = load_universe(req.universe)
    if progress:
        progress(3, "准备股票池", f"共 {len(symbols)} 只标的")

    def on_symbol(completed: int, total: int, symbol: str, success: bool) -> None:
        if progress:
            progress(
                5 + round(58 * completed / max(1, total)),
                "同步股票池行情",
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
        progress(92, "生成每日候选", f"目标持有 {req.horizon} 日")
    selection = daily_selection(
        panel, top_n=req.top_n, horizon=req.horizon,
        industry_map=mapping, name_map=names)
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
    history = store.history(req.universe, limit=10)
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
    }
    if progress:
        progress(100, "决策数据已就绪", f"生成 {len(selection.get('picks', []))} 只候选")
    return result


@app.post("/api/decision/dashboard/stream")
def decision_dashboard_stream(
    req: DecisionDashboardRequest, request: Request,
) -> StreamingResponse:
    """决策工作台流式进度；最终 result 事件携带完整原接口响应。"""
    return _progress_stream(
        lambda emit: _decision_dashboard_data(req, emit), _request_id(request),
    )


# ---------- 因子 ----------

@app.get("/api/factors")
def factors_list() -> dict:
    from quantmaster.ai.sentiment import list_news_factors
    from quantmaster.factors.fundamental import list_fundamental_factors
    from quantmaster.factors.library import list_factors

    return {"factors": list_factors() + list_fundamental_factors() + list_news_factors()}


class FactorTestRequest(BaseModel):
    expression: str = Field(..., description="因子名或表达式，如 rank(-delta(close, 5))")
    universe: str = "demo"
    start: str = "2022-01-01"
    end: str | None = None
    quantiles: int = 5
    neutralize: bool = False          # 行业中性化（行业内去均值）


@app.post("/api/factors/test")
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

class BacktestRequest(BaseModel):
    strategy: str = "factor"         # factor | swing
    factor: str = "mom_20d"
    universe: str = "demo"
    start: str = "2022-01-01"
    end: str | None = None
    top_n: int = 5
    rebalance: str = "W"
    benchmark: str = "000300.SH"
    initial_capital: float = 1_000_000.0
    stop_loss: float | None = None
    take_profit: float | None = None
    weighting: str = "equal"          # 多因子合成方式：equal | ic
    holding_days: int = Field(3, ge=1, le=7)


@app.post("/api/backtest/run")
def backtest_run(req: BacktestRequest) -> dict:
    from quantmaster.backtest import BacktestConfig, FactorStrategy, full_report, run_backtest
    from quantmaster.backtest.strategy import MultiFactorStrategy
    from quantmaster.data import load_history, load_panel
    from quantmaster.data.universe import load_universe
    from quantmaster.factors.fundamental import resolve_factor

    end = req.end or str(pd.Timestamp.now().date())
    try:
        symbols = load_universe(req.universe)
        panel = load_panel(symbols, req.start, end)
        names = [n.strip() for n in req.factor.split(",") if n.strip()]
        if req.strategy == "swing":
            from quantmaster.backtest import SwingStrategy

            strategy = SwingStrategy(top_n=req.top_n, holding_days=req.holding_days)
        elif req.strategy != "factor":
            raise ValueError("strategy 仅支持 factor/swing")
        elif len(names) > 1:
            strategy = MultiFactorStrategy(
                [resolve_factor(n, symbols, req.start, end) for n in names],
                top_n=req.top_n, rebalance=req.rebalance, weighting=req.weighting)
        else:
            strategy = FactorStrategy(resolve_factor(names[0], symbols, req.start, end),
                                      top_n=req.top_n, rebalance=req.rebalance)
        weights = strategy.target_weights(panel)
        benchmark = None
        try:
            benchmark = load_history(req.benchmark, req.start, end)["close"]
        except Exception as e:
            logger.warning("基准 %s 加载失败: %s", req.benchmark, e)
        result = run_backtest(panel, weights,
                              BacktestConfig(initial_capital=req.initial_capital,
                                             stop_loss=req.stop_loss,
                                             take_profit=req.take_profit),
                              benchmark_close=benchmark)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, str(e)) from e

    drawdown = 1.0 - result.nav / result.nav.cummax()
    report = full_report(result)
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
    }


class ValidateRequest(BaseModel):
    expression: str
    universe: str = "demo"
    start: str = "2022-01-01"
    end: str | None = None
    split: str
    n_splits: int = 4


@app.post("/api/factors/validate")
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

class MineRequest(BaseModel):
    universe: str = "demo"
    start: str = "2022-01-01"
    end: str | None = None
    generations: int = 5
    population: int = 40
    top_n: int = 10
    seed: int = 42


@app.post("/api/mine/genetic")
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


class MineLLMRequest(BaseModel):
    universe: str = "demo"
    start: str = "2022-01-01"
    end: str | None = None
    n: int = 8
    rounds: int = 2


@app.post("/api/mine/llm")
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

class PaperRunRequest(BaseModel):
    strategy: str = "factor"         # factor | swing
    factor: str = "mom_20d"
    universe: str = "demo"
    top_n: int = 5
    rebalance: str = "W"
    holding_days: int = Field(3, ge=1, le=7)
    initial_capital: float = 1_000_000.0


@app.post("/api/paper/run")
def paper_run(req: PaperRunRequest) -> dict:
    """按策略最新信号执行一次模拟盘调仓（行情走缓存，缺失才触网）。"""
    from quantmaster.backtest.paper import PaperTrader
    from quantmaster.backtest.strategy import FactorStrategy, SwingStrategy
    from quantmaster.data.universe import load_universe
    from quantmaster.factors.fundamental import resolve_factor

    try:
        symbols = load_universe(req.universe)
        end = str(pd.Timestamp.now().date())
        start = str((pd.Timestamp.now() - pd.Timedelta(days=400)).date())
        trader = PaperTrader(initial_capital=req.initial_capital)
        if req.strategy == "swing":
            strategy = SwingStrategy(top_n=req.top_n, holding_days=req.holding_days)
        elif req.strategy == "factor":
            strategy = FactorStrategy(resolve_factor(req.factor, symbols, start, end),
                                      top_n=req.top_n, rebalance=req.rebalance)
        else:
            raise ValueError("strategy 仅支持 factor/swing")
        return trader.run_once(strategy, symbols)
    except Exception as e:
        raise HTTPException(400, str(e)) from e


@app.get("/api/paper/report")
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


class AssetListIn(BaseModel):
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


@app.get("/api/assets/lists")
def asset_lists_get() -> dict:
    """自选、关注和实盘持有；报价仅复用本地缓存。"""
    return _asset_lists_payload()


@app.post("/api/assets/lists/{list_name}")
def asset_lists_add(
    list_name: Literal["favorites", "following"], item: AssetListIn,
) -> dict:
    from quantmaster.portfolio import AssetListStore

    try:
        AssetListStore().add(list_name, item.symbol, item.name)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    return _asset_lists_payload()


@app.delete("/api/assets/lists/{list_name}/{symbol}")
def asset_lists_remove(
    list_name: Literal["favorites", "following"], symbol: str,
) -> dict:
    from quantmaster.portfolio import AssetListStore

    try:
        AssetListStore().remove(list_name, symbol)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    return _asset_lists_payload()

class TradeIn(BaseModel):
    date: str
    symbol: str
    side: str
    price: float
    shares: float
    fee: float = 0.0
    note: str = ""


@app.post("/api/ledger/trade")
def ledger_add_trade(trade: TradeIn) -> dict:
    from quantmaster.portfolio import Ledger, TradeRecord

    try:
        Ledger().add_trade(TradeRecord(**trade.model_dump()))
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    return {"status": "ok"}


class CashflowIn(BaseModel):
    date: str
    amount: float
    kind: str = "deposit"
    note: str = ""


@app.post("/api/ledger/cashflow")
def ledger_add_cashflow(flow: CashflowIn) -> dict:
    from quantmaster.portfolio import Ledger

    try:
        Ledger().add_cashflow(flow.date, flow.amount, flow.kind, flow.note)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    return {"status": "ok"}


@app.get("/api/ledger/report")
def ledger_get_report() -> dict:
    from quantmaster.portfolio import Ledger, ledger_report

    return ledger_report(Ledger())


@app.get("/api/ledger/trades")
def ledger_get_trades() -> dict:
    from quantmaster.portfolio import Ledger

    df = Ledger().trades()
    return {"trades": df.to_dict(orient="records")}


@app.get("/api/ledger/nav")
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
    run_uvicorn_foreground(app, host=cfg.host, port=cfg.port, log_level="info")
