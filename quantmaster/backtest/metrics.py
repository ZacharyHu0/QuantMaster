"""绩效指标。

指标速览（面向本科水平读者）：
- 年化收益：把整段收益折算成「每年平均」的复利口径。
- 最大回撤：净值从历史高点最多跌了多少，衡量最痛的一段。
- 夏普比率：超额收益 / 波动率，每承担一份波动换来多少收益，>1 算不错。
- 卡玛比率：年化收益 / 最大回撤，更贴近「亏钱的痛」。
- 信息比率(IR)：相对基准的超额收益 / 跟踪误差，衡量跑赢基准的稳定性。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

TRADING_DAYS = 244   # A 股年均交易日
RISK_FREE = 0.02     # 无风险利率假设（年化 2%）


def max_drawdown(nav: pd.Series) -> tuple[float, str, str]:
    """返回 (最大回撤, 高点日期, 低点日期)。回撤为正数，如 0.25 = 25%。"""
    running_max = nav.cummax()
    drawdown = 1.0 - nav / running_max
    if drawdown.empty or drawdown.max() <= 0:
        return 0.0, "", ""
    trough = drawdown.idxmax()
    peak = nav.loc[:trough].idxmax()
    return float(drawdown.max()), str(pd.Timestamp(peak).date()), str(pd.Timestamp(trough).date())


def performance_metrics(
    returns: pd.Series,
    benchmark_nav: pd.Series | None = None,
    trades: list | None = None,
) -> dict:
    clean = returns.dropna()
    if clean.empty:
        return {}
    nav = (1 + clean).cumprod()
    n = len(clean)
    total_return = float(nav.iloc[-1] - 1)
    base = float(nav.iloc[-1])
    annual_return = (base ** (TRADING_DAYS / n) - 1) if base > 0 else -1.0
    volatility = float(clean.std() * np.sqrt(TRADING_DAYS))
    mdd, peak, trough = max_drawdown(nav)

    sharpe = (annual_return - RISK_FREE) / volatility if volatility > 0 else 0.0
    calmar = annual_return / mdd if mdd > 0 else 0.0
    win_rate = float((clean > 0).mean())

    metrics = {
        "total_return": round(total_return, 4),
        "annual_return": round(annual_return, 4),
        "volatility": round(volatility, 4),
        "sharpe": round(sharpe, 3),
        "max_drawdown": round(mdd, 4),
        "max_drawdown_peak": peak,
        "max_drawdown_trough": trough,
        "calmar": round(calmar, 3),
        "daily_win_rate": round(win_rate, 4),
        "days": n,
    }

    if benchmark_nav is not None and len(benchmark_nav.dropna()) > 2:
        bench_returns = benchmark_nav.pct_change().reindex(clean.index).fillna(0.0)
        excess = clean - bench_returns
        tracking_error = float(excess.std() * np.sqrt(TRADING_DAYS))
        excess_annual = float(excess.mean() * TRADING_DAYS)
        bench_base = float((1 + bench_returns).prod())
        metrics.update({
            "benchmark_annual_return": round(
                (bench_base ** (TRADING_DAYS / n) - 1) if bench_base > 0 else -1.0, 4),
            "excess_annual_return": round(excess_annual, 4),
            "information_ratio": round(
                excess_annual / tracking_error if tracking_error > 0 else 0.0, 3),
        })

    if trades:
        total_cost = sum(t.cost for t in trades)
        metrics.update({
            "trade_count": len(trades),
            "total_trade_cost": round(total_cost, 2),
        })
    return metrics
