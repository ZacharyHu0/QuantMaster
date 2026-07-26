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
    })
