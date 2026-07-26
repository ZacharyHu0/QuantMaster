"""FastAPI 本地服务：JSON API + Web 仪表盘。

启动：qm serve  （或 uvicorn quantmaster.server.app:app）
浏览器访问 http://127.0.0.1:8686
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from quantmaster import __version__
from quantmaster.config import get_config

logger = logging.getLogger(__name__)

app = FastAPI(title="QuantMaster", version=__version__)

STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def _series_to_points(s: pd.Series) -> list[list]:
    return [[str(k.date()), round(float(v), 6)] for k, v in s.dropna().items()]


# ---------- 页面 ----------

@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok", "version": __version__}


# ---------- 市场 ----------

@app.get("/api/market/overview")
def market_overview(start: str | None = None) -> dict:
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
    for group, symbols in groups.items():
        rows = []
        for symbol, name in symbols.items():
            try:
                df = load_history(symbol, str(start_ts.date()), str(end.date()))
                if df.empty:
                    continue
                close = df["close"].dropna()
                rows.append({
                    "symbol": symbol,
                    "name": name,
                    "last": round(float(close.iloc[-1]), 3),
                    "change_pct": round(float(close.iloc[-1] / close.iloc[-2] - 1) * 100, 2)
                    if len(close) > 1 else 0.0,
                    "nav": _series_to_points(close / close.iloc[0]),
                })
            except Exception as e:
                logger.warning("市场概览跳过 %s: %s", symbol, e)
        result[group] = rows
    return {"groups": result}


@app.get("/api/market/history/{symbol}")
def market_history(symbol: str, start: str = "2023-01-01", end: str | None = None) -> dict:
    from quantmaster.data import load_history

    end = end or str(pd.Timestamp.now().date())
    try:
        df = load_history(symbol, start, end)
    except Exception as e:
        raise HTTPException(404, f"获取 {symbol} 失败: {e}") from e
    return {
        "symbol": symbol,
        "kline": [
            [str(idx.date()), round(row["open"], 3), round(row["close"], 3),
             round(row["low"], 3), round(row["high"], 3),
             round(row.get("volume", 0.0), 0)]
            for idx, row in df.iterrows()
        ],
    }


# ---------- 因子 ----------

@app.get("/api/factors")
def factors_list() -> dict:
    from quantmaster.factors.library import list_factors

    return {"factors": list_factors()}


class FactorTestRequest(BaseModel):
    expression: str = Field(..., description="因子名或表达式，如 rank(-delta(close, 5))")
    universe: str = "demo"
    start: str = "2022-01-01"
    end: str | None = None
    quantiles: int = 5


@app.post("/api/factors/test")
def factors_test(req: FactorTestRequest) -> dict:
    from quantmaster.data import load_panel
    from quantmaster.data.universe import load_universe
    from quantmaster.factors import analyze_factor, compute_factor
    from quantmaster.factors.library import get_factor

    end = req.end or str(pd.Timestamp.now().date())
    try:
        factor = get_factor(req.expression)
        symbols = load_universe(req.universe)
        panel = load_panel(symbols, req.start, end)
        values = compute_factor(factor, panel)
        report = analyze_factor(values, panel["close"], name=factor.name,
                                quantiles=req.quantiles)
    except Exception as e:
        raise HTTPException(400, str(e)) from e
    return {
        "summary": report.summary(),
        "ic_series": _series_to_points(report.ic_series.rolling(20, min_periods=5).mean()),
        "quantile_nav": {col: _series_to_points(report.quantile_returns[col])
                         for col in report.quantile_returns.columns},
    }


# ---------- 回测 ----------

class BacktestRequest(BaseModel):
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


@app.post("/api/backtest/run")
def backtest_run(req: BacktestRequest) -> dict:
    from quantmaster.backtest import BacktestConfig, FactorStrategy, full_report, run_backtest
    from quantmaster.data import load_history, load_panel
    from quantmaster.data.universe import load_universe
    from quantmaster.factors.library import get_factor

    end = req.end or str(pd.Timestamp.now().date())
    try:
        symbols = load_universe(req.universe)
        panel = load_panel(symbols, req.start, end)
        strategy = FactorStrategy(get_factor(req.factor), top_n=req.top_n,
                                  rebalance=req.rebalance)
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
    from quantmaster.factors.library import get_factor

    end = req.end or str(pd.Timestamp.now().date())
    try:
        factor = get_factor(req.expression)
        panel = load_panel(load_universe(req.universe), req.start, end)
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


# ---------- AI 爬虫 / 新闻 ----------

@app.post("/api/news/crawl")
def news_crawl(skip_llm: bool = False) -> dict:
    from quantmaster.ai.crawler import AICrawler

    try:
        return AICrawler().run(skip_llm=skip_llm)
    except Exception as e:
        raise HTTPException(400, str(e)) from e


@app.get("/api/news")
def news_recent(limit: int = 50) -> dict:
    from quantmaster.ai.crawler import NewsStore

    return {"items": NewsStore().recent(limit=limit)}


# ---------- 实盘账本 ----------

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
    from quantmaster.portfolio import Ledger, daily_nav, nav_with_benchmark

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
    return payload


def serve() -> None:  # pragma: no cover - 入口
    import uvicorn

    cfg = get_config().server
    uvicorn.run(app, host=cfg.host, port=cfg.port, log_level="info")
