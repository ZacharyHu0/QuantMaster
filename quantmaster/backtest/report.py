"""回测分析报告：分年/分月收益拆解、成交统计与 JSON 友好的完整报告。

为什么需要「拆开看」（面向本科水平读者）：

- 一条总的年化收益/夏普会掩盖很多信息——策略可能全部收益来自某一年
  的牛市，其余年份都在亏钱。**分年统计**能暴露这种「靠天吃饭」。
- **月度收益表**（行=年份、列=月份）是业内标准展示方式：一眼看出策略
  在哪些月份/季节性行情下失灵（例如小市值因子在每年初的「春季躁动」
  与年末的风格切换）。
- **成交统计**回答「收益是不是被交易成本吃掉了」：笔数、总费用、单笔
  平均金额过小往往意味着换手过高或资金利用率低。

本模块所有输出都设计为 JSON 可序列化（浮点圆整、NaN 一律转 None），
方便 Web 前端与 CLI 直接展示。
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

from quantmaster.backtest.engine import BacktestResult, Trade
from quantmaster.backtest.metrics import RISK_FREE, TRADING_DAYS, max_drawdown

YEARLY_COLUMNS = ("return", "volatility", "max_drawdown", "sharpe", "days")
MONTH_COLUMNS = tuple(range(1, 13))


def yearly_returns(returns: pd.Series) -> pd.DataFrame:
    """按自然年统计绩效。

    返回 DataFrame：index=年份字符串，列 = 收益（当年累计，非年化）、
    年化波动率、当年最大回撤、夏普（用当年数据年化后计算）、交易日数。
    空输入或全 NaN 输入返回空表（不抛异常）。
    """
    clean = returns.dropna()
    if clean.empty:
        return pd.DataFrame(columns=list(YEARLY_COLUMNS))

    idx = pd.DatetimeIndex(clean.index)
    rows: dict[str, dict] = {}
    for year, r in clean.groupby(idx.year):
        nav = (1 + r).cumprod()
        base = float(nav.iloc[-1])
        n = len(r)
        # 波动率按 √244 年化；仅 1 个样本时标准差无定义，记 0
        vol = float(r.std() * np.sqrt(TRADING_DAYS)) if n > 1 else 0.0
        # 夏普用「年化后」的收益口径，与 performance_metrics 保持一致
        annual = (base ** (TRADING_DAYS / n) - 1) if base > 0 else -1.0
        sharpe = (annual - RISK_FREE) / vol if vol > 0 else 0.0
        mdd, _, _ = max_drawdown(nav)
        rows[str(year)] = {
            "return": base - 1.0,
            "volatility": vol,
            "max_drawdown": mdd,
            "sharpe": sharpe,
            "days": n,
        }
    return pd.DataFrame.from_dict(rows, orient="index")[list(YEARLY_COLUMNS)]


def monthly_return_table(returns: pd.Series) -> pd.DataFrame:
    """月度收益表：行=年份字符串，列=1..12 月，值=当月累计收益（小数）。

    没有数据的月份为 NaN。空输入返回空表（只有 12 列、无行）。
    """
    clean = returns.dropna()
    if clean.empty:
        return pd.DataFrame(columns=list(MONTH_COLUMNS))

    idx = pd.DatetimeIndex(clean.index)
    monthly = clean.groupby([idx.year, idx.month]).apply(lambda r: float((1 + r).prod() - 1))
    table = monthly.unstack(level=1).reindex(columns=list(MONTH_COLUMNS))
    table.index = table.index.astype(str)
    table.columns = list(MONTH_COLUMNS)
    return table


def trade_stats(trades: list[Trade]) -> dict:
    """成交统计：买卖笔数、总费用、平均单笔金额、按 symbol 聚合的成交额 top10。"""
    if not trades:
        return {
            "trade_count": 0, "buy_count": 0, "sell_count": 0,
            "total_cost": 0.0, "total_amount": 0.0, "avg_amount": 0.0,
            "top_symbols": [],
        }

    buy_count = sum(1 for t in trades if t.side == "buy")
    total_amount = float(sum(t.amount for t in trades))
    total_cost = float(sum(t.cost for t in trades))

    by_symbol: dict[str, dict] = {}
    for t in trades:
        agg = by_symbol.setdefault(t.symbol, {"symbol": t.symbol, "amount": 0.0, "count": 0})
        agg["amount"] += t.amount
        agg["count"] += 1
    top = sorted(by_symbol.values(), key=lambda x: x["amount"], reverse=True)[:10]

    return {
        "trade_count": len(trades),
        "buy_count": buy_count,
        "sell_count": len(trades) - buy_count,
        "total_cost": round(total_cost, 2),
        "total_amount": round(total_amount, 2),
        "avg_amount": round(total_amount / len(trades), 2),
        "top_symbols": [
            {"symbol": x["symbol"], "amount": round(x["amount"], 2), "count": x["count"]}
            for x in top
        ],
    }


def _to_jsonable(obj, ndigits: int = 6):
    """递归转成 JSON 可序列化对象：numpy 标量转 Python、float 圆整、NaN/Inf 转 None。"""
    if isinstance(obj, dict):
        return {str(k): _to_jsonable(v, ndigits) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_jsonable(v, ndigits) for v in obj]
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (float, np.floating)):
        f = float(obj)
        return None if not math.isfinite(f) else round(f, ndigits)
    if obj is pd.NaT or obj is None:
        return None
    return obj


def full_report(result: BacktestResult) -> dict:
    """聚合完整回测报告：metrics + 分年（records）+ 月度表（records）+ 成交统计。

    输出保证可直接 ``json.dumps``（NaN 已替换为 None，浮点已圆整），
    供 Web API / 前端 / LLM 摘要直接消费。
    """
    yearly = yearly_returns(result.returns)
    monthly = monthly_return_table(result.returns)

    yearly_records = [
        {"year": year, **row} for year, row in yearly.to_dict(orient="index").items()
    ]
    monthly_records = [
        {"year": year, **{str(m): row.get(m) for m in MONTH_COLUMNS}}
        for year, row in monthly.to_dict(orient="index").items()
    ]

    return _to_jsonable({
        "metrics": result.metrics,
        "yearly": yearly_records,
        "monthly": monthly_records,
        "trade_stats": trade_stats(result.trades),
        "risk_diagnostics": risk_diagnostics(result),
        "stress_tests": stress_tests(result),
        "trade_lifecycle": trade_lifecycle(result),
    })


def risk_diagnostics(result: BacktestResult) -> dict:
    """固定种子的区块 bootstrap 区间与基准市场状态拆解。"""
    returns = result.returns.dropna()
    if returns.empty:
        return {}
    rng = np.random.default_rng(42)
    block = min(20, len(returns))
    annualized = []
    values = returns.to_numpy(float)
    for _ in range(300):
        pieces = []
        while sum(len(item) for item in pieces) < len(values):
            start = int(rng.integers(0, max(1, len(values) - block + 1)))
            pieces.append(values[start:start + block])
        sampled = np.concatenate(pieces)[:len(values)]
        base = float(np.prod(1 + np.clip(sampled, -0.999, None)))
        annualized.append(base ** (TRADING_DAYS / len(sampled)) - 1 if base > 0 else -1.0)
    regimes: dict[str, dict] = {}
    if result.benchmark_nav is not None:
        benchmark = result.benchmark_nav.reindex(returns.index).ffill()
        trend = benchmark / benchmark.rolling(60, min_periods=20).mean() - 1
        volatility = benchmark.pct_change(fill_method=None).rolling(20).std()
        threshold = volatility.expanding(min_periods=20).median()
        labels = pd.Series(
            np.where(trend >= 0, "uptrend", "downtrend"), index=returns.index,
        ).str.cat(pd.Series(np.where(volatility > threshold, "high_vol", "normal_vol"),
                            index=returns.index), sep="/")
        for name, group in returns.groupby(labels):
            regimes[str(name)] = {
                "days": len(group), "return": float((1 + group).prod() - 1),
                "win_rate": float((group > 0).mean()),
            }
    return {
        "annual_return_confidence_95": [
            float(np.quantile(annualized, 0.025)), float(np.quantile(annualized, 0.975)),
        ],
        "market_regimes": regimes,
    }


def stress_tests(result: BacktestResult) -> list[dict]:
    """按实际成交费用提高后的组合压力结果。"""
    base_return = float(result.nav.iloc[-1] - 1) if not result.nav.empty else 0.0
    paid = sum(float(trade.cost) for trade in result.trades)
    rows = []
    for multiplier in (1.0, 1.5, 2.0, 3.0):
        extra = paid * (multiplier - 1) / max(result.initial_capital, 1.0)
        rows.append({
            "cost_multiplier": multiplier,
            "stressed_total_return": base_return - extra,
            "additional_cost": paid * (multiplier - 1),
        })
    return rows


def trade_lifecycle(result: BacktestResult) -> dict:
    """按完整开平仓周期估算持有期、MFE 与 MAE。"""
    if result.close_prices is None or result.close_prices.empty:
        return {"round_trips": 0, "items": []}
    opened: dict[str, dict] = {}
    items = []
    for trade in result.trades:
        if trade.side == "buy":
            state = opened.setdefault(trade.symbol, {
                "date": trade.date, "amount": 0.0, "shares": 0.0,
            })
            state["amount"] += trade.amount
            state["shares"] += trade.shares
        elif trade.symbol in opened:
            state = opened[trade.symbol]
            entry = state["amount"] / max(state["shares"], 1e-12)
            closed_shares = min(float(trade.shares), float(state["shares"]))
            path = result.close_prices.loc[state["date"]:trade.date, trade.symbol].dropna()
            returns = path / entry - 1 if not path.empty else pd.Series(dtype=float)
            items.append({
                "symbol": trade.symbol, "entry_date": state["date"], "exit_date": trade.date,
                "shares": closed_shares,
                "holding_days": max(0, len(path) - 1),
                "mfe": float(returns.max()) if not returns.empty else None,
                "mae": float(returns.min()) if not returns.empty else None,
            })
            state["shares"] -= closed_shares
            state["amount"] -= closed_shares * entry
            if state["shares"] <= 1e-9:
                opened.pop(trade.symbol)
    return {
        "round_trips": len(items),
        "average_holding_days": float(np.mean([item["holding_days"] for item in items])) if items else 0,
        "items": items[-200:],
    }
