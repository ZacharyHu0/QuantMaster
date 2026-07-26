"""回测引擎：日线级、按目标权重调仓，内置 A 股交易规则。

时间约定（严格避免未来函数）：
    T 日收盘后产生信号（目标权重） -> T+1 日「开盘价」成交 -> 每日收盘结算市值。
    因为每天只在开盘交易一次，今天买入的股票最早明天才卖出，天然满足 T+1。

内置 A 股规则：
    - 涨跌停：开盘涨停(≥9.8%/19.6%)买不进，开盘跌停卖不出（简化为开盘价判定）。
      主板 ±10%，创业板(30xxxx)/科创板(688xxx) ±20%，北交所 ±30%。
    - 交易成本：佣金（默认万2.5、最低5元）+ 印花税（卖出 0.05%）+ 过户费 + 滑点。
    - 一手 100 股整数倍买入。
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from quantmaster.config import TradeConfig, get_config


@dataclass
class BacktestConfig:
    initial_capital: float = 1_000_000.0
    trade: TradeConfig = field(default_factory=lambda: get_config().trade)
    limit_check: bool = True         # 是否启用涨跌停约束
    allow_fractional: bool = False   # True 时忽略 100 股整手限制（研究用）
    stop_loss: float | None = None   # 止损线：开盘价较持仓成本跌幅 ≥ 该比例则清仓（如 0.08）
    take_profit: float | None = None # 止盈线：开盘价较持仓成本涨幅 ≥ 该比例则清仓（如 0.25）


@dataclass
class Trade:
    date: str
    symbol: str
    side: str          # buy / sell
    price: float
    shares: float
    amount: float
    cost: float        # 该笔交易的全部费用
    note: str = ""


@dataclass
class BacktestResult:
    nav: pd.Series                    # 组合净值（初始=1）
    returns: pd.Series                # 日收益率
    positions: pd.DataFrame           # 每日持仓市值（date × symbol）
    trades: list[Trade]
    metrics: dict
    benchmark_nav: pd.Series | None = None

    def to_dict(self) -> dict:
        return {
            "metrics": self.metrics,
            "nav": {str(k.date()): round(float(v), 6) for k, v in self.nav.items()},
            "benchmark_nav": (
                {str(k.date()): round(float(v), 6) for k, v in self.benchmark_nav.items()}
                if self.benchmark_nav is not None else None
            ),
            "trades": [t.__dict__ for t in self.trades[-500:]],
        }


def price_limit(symbol: str) -> float:
    """按代码推断涨跌停幅度。"""
    code = symbol.split(".")[0]
    if code.startswith(("688", "689", "300", "301")):
        return 0.20
    if code.startswith(("8", "4")) and symbol.endswith(".BJ"):
        return 0.30
    return 0.10


def _buy_cost(amount: float, t: TradeConfig) -> float:
    return max(amount * t.commission_rate, t.commission_min) + amount * t.transfer_fee_rate


def _sell_cost(amount: float, t: TradeConfig) -> float:
    return (
        max(amount * t.commission_rate, t.commission_min)
        + amount * t.stamp_tax_rate
        + amount * t.transfer_fee_rate
    )


def run_backtest(
    panel: dict[str, pd.DataFrame],
    target_weights: pd.DataFrame,
    config: BacktestConfig | None = None,
    benchmark_close: pd.Series | None = None,
) -> BacktestResult:
    """执行回测。

    参数
    ----
    panel:          {"open": df, "close": df, ...}，df 为 date × symbol
    target_weights: date × symbol 的目标权重，行 = 信号产生日（T 日收盘）。
                    仅在权重发生变化的日期调仓；行内权重和应 ≤ 1（余下为现金）。
    benchmark_close: 基准指数收盘价序列（如沪深300），用于对比与超额统计。
    """
    from quantmaster.backtest.metrics import performance_metrics

    config = config or BacktestConfig()
    tcfg = config.trade
    open_px = panel["open"]
    close_px = panel["close"]
    dates = close_px.index
    symbols = list(close_px.columns)

    target_weights = target_weights.reindex(index=dates, columns=symbols)
    prev_close = close_px.shift(1)

    cash = config.initial_capital
    shares: dict[str, float] = {s: 0.0 for s in symbols}
    entry_cost: dict[str, float] = {s: 0.0 for s in symbols}   # 持仓的加权平均成本
    trades: list[Trade] = []
    nav_values: list[float] = []
    position_rows: list[dict] = []

    pending: pd.Series | None = None       # T 日收盘信号，T+1 开盘执行

    for i, date in enumerate(dates):
        date_str = str(date.date())
        stopped_today: set[str] = set()

        # ---- 开盘：止损/止盈检查（先于调仓信号执行）----
        if config.stop_loss is not None or config.take_profit is not None:
            day_open_sl = open_px.loc[date]
            day_prev_close_sl = prev_close.loc[date]
            for symbol in symbols:
                if shares[symbol] <= 0 or entry_cost[symbol] <= 0:
                    continue
                px = day_open_sl.get(symbol, np.nan)
                if np.isnan(px) or px <= 0:
                    continue
                change = px / entry_cost[symbol] - 1.0
                reason = None
                if config.stop_loss is not None and change <= -config.stop_loss:
                    reason = "stop_loss"
                elif config.take_profit is not None and change >= config.take_profit:
                    reason = "take_profit"
                if reason is None:
                    continue
                # 跌停无法卖出：止损单也排不上队，只能顺延
                pc = day_prev_close_sl.get(symbol, np.nan)
                limit = price_limit(symbol)
                if config.limit_check and not np.isnan(pc) and px <= pc * (1 - limit) * 1.002:
                    continue
                exec_px = float(px) * (1 - tcfg.slippage)
                amount = shares[symbol] * exec_px
                sell_cost = _sell_cost(amount, tcfg)
                cash += amount - sell_cost
                trades.append(Trade(date_str, symbol, "sell", round(exec_px, 4),
                                    shares[symbol], round(amount, 2), round(sell_cost, 2),
                                    note=reason))
                shares[symbol] = 0.0
                entry_cost[symbol] = 0.0
                stopped_today.add(symbol)   # 当日不再重新买入，避免止损即回补

        # ---- 开盘：执行昨日信号 ----
        if pending is not None:
            day_open = open_px.loc[date]
            day_prev_close = prev_close.loc[date]
            portfolio_value = cash + sum(
                shares[s] * day_open.get(s, np.nan)
                for s in symbols
                if shares[s] > 0 and not np.isnan(day_open.get(s, np.nan))
            )

            weights = pending.fillna(0.0).clip(lower=0.0)
            total_w = float(weights.sum())
            if total_w > 1.0 + 1e-9:
                weights = weights / total_w

            # 先卖后买（腾出资金）
            orders: list[tuple[str, float]] = []
            for symbol in symbols:
                px = day_open.get(symbol, np.nan)
                if np.isnan(px) or px <= 0:
                    continue
                target_value = portfolio_value * float(weights.get(symbol, 0.0))
                diff_value = target_value - shares[symbol] * px
                orders.append((symbol, diff_value))
            orders.sort(key=lambda x: x[1])   # 卖单（负值）在前

            for symbol, diff_value in orders:
                px = float(day_open[symbol])
                pc = day_prev_close.get(symbol, np.nan)
                limit = price_limit(symbol)
                if abs(diff_value) < max(portfolio_value * 5e-4, 100 * px * 0.5):
                    continue   # 忽略过小的调整，抑制无谓换手

                if diff_value < 0 and shares[symbol] > 0:
                    # 跌停无法卖出
                    if config.limit_check and not np.isnan(pc) and px <= pc * (1 - limit) * 1.002:
                        continue
                    sell_shares = min(shares[symbol], -diff_value / px)
                    if not config.allow_fractional:
                        lot = tcfg.lot_size
                        # 不足一手的零股允许一次性清仓
                        if sell_shares < shares[symbol] - 1e-9:
                            sell_shares = int(sell_shares // lot) * lot
                    if sell_shares <= 0:
                        continue
                    exec_px = px * (1 - tcfg.slippage)
                    amount = sell_shares * exec_px
                    cost = _sell_cost(amount, tcfg)
                    cash += amount - cost
                    shares[symbol] -= sell_shares
                    if shares[symbol] <= 1e-9:
                        entry_cost[symbol] = 0.0   # 清仓后重置成本（部分卖出保持均价不变）
                    trades.append(Trade(date_str, symbol, "sell", round(exec_px, 4),
                                        sell_shares, round(amount, 2), round(cost, 2)))
                elif diff_value > 0:
                    if symbol in stopped_today:
                        continue   # 当日刚止损/止盈的股票不立即回补
                    # 涨停无法买入
                    if config.limit_check and not np.isnan(pc) and px >= pc * (1 + limit) * 0.998:
                        continue
                    exec_px = px * (1 + tcfg.slippage)
                    budget = min(diff_value, cash)
                    buy_shares = budget / (exec_px * (1 + tcfg.commission_rate + tcfg.transfer_fee_rate))
                    if not config.allow_fractional:
                        buy_shares = int(buy_shares // tcfg.lot_size) * tcfg.lot_size
                    if buy_shares <= 0:
                        continue
                    amount = buy_shares * exec_px
                    cost = _buy_cost(amount, tcfg)
                    if amount + cost > cash + 1e-6:
                        continue
                    cash -= amount + cost
                    prev_shares = shares[symbol]
                    shares[symbol] += buy_shares
                    # 加权平均持仓成本（不含费用，止损线以成交价为基准）
                    entry_cost[symbol] = (
                        (prev_shares * entry_cost[symbol] + buy_shares * exec_px) / shares[symbol]
                    )
                    trades.append(Trade(date_str, symbol, "buy", round(exec_px, 4),
                                        buy_shares, round(amount, 2), round(cost, 2)))
            pending = None

        # ---- 收盘：结算市值、读取新信号 ----
        day_close = close_px.loc[date]
        position_value = 0.0
        row = {"date": date}
        for symbol in symbols:
            if shares[symbol] > 0:
                px = day_close.get(symbol, np.nan)
                if np.isnan(px):
                    px = prev_close.loc[date].get(symbol, np.nan)
                value = shares[symbol] * (0.0 if np.isnan(px) else float(px))
                position_value += value
                row[symbol] = value
        nav_values.append(cash + position_value)
        position_rows.append(row)

        signal_row = target_weights.iloc[i]
        if signal_row.notna().any():
            pending = signal_row

    nav = pd.Series(nav_values, index=dates, name="nav") / config.initial_capital
    returns = nav.pct_change().fillna(0.0)
    positions = pd.DataFrame(position_rows).set_index("date").fillna(0.0)

    benchmark_nav = None
    if benchmark_close is not None and len(benchmark_close.dropna()) > 2:
        bench = benchmark_close.reindex(dates).ffill()
        benchmark_nav = bench / bench.iloc[0]

    metrics = performance_metrics(returns, benchmark_nav=benchmark_nav, trades=trades)
    return BacktestResult(
        nav=nav, returns=returns, positions=positions,
        trades=trades, metrics=metrics, benchmark_nav=benchmark_nav,
    )
