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

from quantmaster.backtest.execution import (
    buy_cost,
    limit_reason,
    sell_cost,
)
from quantmaster.backtest.execution import price_limit as price_limit
from quantmaster.config import TradeConfig, get_config


@dataclass
class BacktestConfig:
    initial_capital: float = 1_000_000.0
    trade: TradeConfig = field(default_factory=lambda: get_config().trade)
    limit_check: bool = True         # 是否启用涨跌停约束
    allow_fractional: bool = False   # True 时忽略 100 股整手限制（研究用）
    stop_loss: float | None = None   # 止损线：开盘价较持仓成本跌幅 ≥ 该比例则清仓（如 0.08）
    take_profit: float | None = None # 止盈线：开盘价较持仓成本涨幅 ≥ 该比例则清仓（如 0.25）
    research_tier: str = "sandbox"  # production 必须提供真实涨跌停/停牌/复权因子


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
class BlockedOrder:
    date: str
    symbol: str
    side: str
    reason: str
    note: str = ""


@dataclass
class BacktestResult:
    nav: pd.Series                    # 组合净值（初始=1）
    returns: pd.Series                # 日收益率
    positions: pd.DataFrame           # 每日持仓市值（date × symbol）
    trades: list[Trade]
    metrics: dict
    benchmark_nav: pd.Series | None = None
    blocked_orders: list[BlockedOrder] = field(default_factory=list)
    initial_capital: float = 1_000_000.0
    close_prices: pd.DataFrame | None = None

    def to_dict(self) -> dict:
        return {
            "metrics": self.metrics,
            "nav": {str(k.date()): round(float(v), 6) for k, v in self.nav.items()},
            "benchmark_nav": (
                {str(k.date()): round(float(v), 6) for k, v in self.benchmark_nav.items()}
                if self.benchmark_nav is not None else None
            ),
            "trades": [t.__dict__ for t in self.trades[-500:]],
            "blocked_orders": [order.__dict__ for order in self.blocked_orders[-500:]],
        }


def _buy_cost(amount: float, t: TradeConfig) -> float:
    return buy_cost(amount, t)


def _sell_cost(amount: float, t: TradeConfig) -> float:
    return sell_cost(amount, t)


def _execution_reason(
    symbol: str, side: str, date, price: float, previous_close: float | None,
    *, up_limit: pd.DataFrame | None, down_limit: pd.DataFrame | None,
    suspended: pd.DataFrame | None, config: BacktestConfig,
) -> str | None:
    if suspended is not None:
        value = suspended.reindex(index=[date], columns=[symbol]).iloc[0, 0]
        if pd.notna(value) and bool(value):
            return "suspended"
    frame = up_limit if side == "buy" else down_limit
    limit_value = np.nan
    if frame is not None:
        limit_value = frame.reindex(index=[date], columns=[symbol]).iloc[0, 0]
    if pd.notna(limit_value) and float(limit_value) > 0:
        tolerance = max(1e-6, abs(float(limit_value)) * 1e-6)
        if side == "buy" and price >= float(limit_value) - tolerance:
            return "limit_up"
        if side == "sell" and price <= float(limit_value) + tolerance:
            return "limit_down"
        return None
    if config.research_tier == "production":
        return "missing_actual_limit"
    return limit_reason(
        symbol, side, price, previous_close, enabled=config.limit_check,
    )


@dataclass(frozen=True)
class _MarketData:
    open_px: pd.DataFrame
    close_px: pd.DataFrame
    up_limit: pd.DataFrame | None
    down_limit: pd.DataFrame | None
    suspended: pd.DataFrame | None
    adj_factor: pd.DataFrame | None
    dates: pd.Index
    symbols: list[str]


@dataclass
class _BacktestState:
    cash: float
    shares: dict[str, float]
    entry_cost: dict[str, float]
    last_close: dict[str, float] = field(default_factory=dict)
    last_factor: dict[str, float] = field(default_factory=dict)
    trades: list[Trade] = field(default_factory=list)
    blocked_orders: list[BlockedOrder] = field(default_factory=list)
    nav_values: list[float] = field(default_factory=list)
    position_rows: list[dict] = field(default_factory=list)
    pending: pd.Series | None = None


def _prepare_market_data(panel: dict[str, pd.DataFrame]) -> _MarketData:
    signal_close = panel["close"]
    return _MarketData(
        open_px=panel.get("execution_open", panel["open"]).reindex_like(signal_close),
        close_px=panel.get("execution_close", signal_close).reindex_like(signal_close),
        up_limit=panel.get("up_limit"),
        down_limit=panel.get("down_limit"),
        suspended=panel.get("suspended"),
        adj_factor=panel.get("adj_factor"),
        dates=signal_close.index,
        symbols=list(signal_close.columns),
    )


def _missing_production_fields(panel: dict[str, pd.DataFrame]) -> list[str]:
    required = (
        "up_limit", "down_limit", "suspended", "adj_factor",
        "execution_open", "execution_close",
    )
    return [name for name in required if panel.get(name) is None]


def _validate_production_panel(
    panel: dict[str, pd.DataFrame],
    config: BacktestConfig,
) -> None:
    if config.research_tier != "production":
        return
    missing = _missing_production_fields(panel)
    if missing:
        raise ValueError("production 回测缺少真实成交字段：" + "、".join(missing))


def _initial_state(config: BacktestConfig, symbols: list[str]) -> _BacktestState:
    return _BacktestState(
        cash=config.initial_capital,
        shares={symbol: 0.0 for symbol in symbols},
        entry_cost={symbol: 0.0 for symbol in symbols},
    )


def _apply_adjustment_factors(
    state: _BacktestState,
    factors: pd.Series,
    symbols: list[str],
) -> None:
    for symbol in symbols:
        value = factors.get(symbol, np.nan)
        if pd.isna(value) or float(value) <= 0:
            continue
        previous = state.last_factor.get(symbol)
        if previous and state.shares[symbol] > 0:
            ratio = float(value) / previous
            state.shares[symbol] *= ratio
            state.entry_cost[symbol] /= ratio
        state.last_factor[symbol] = float(value)


def _risk_exit_reason(change: float, config: BacktestConfig) -> str | None:
    if config.stop_loss is not None and change <= -config.stop_loss:
        return "stop_loss"
    if config.take_profit is not None and change >= config.take_profit:
        return "take_profit"
    return None


def _previous_close_value(row: pd.Series, symbol: str) -> float | None:
    value = row.get(symbol, np.nan)
    return float(value) if not np.isnan(value) else None


def _try_risk_exit(
    state: _BacktestState,
    market: _MarketData,
    config: BacktestConfig,
    date,
    date_str: str,
    symbol: str,
    day_open: pd.Series,
    day_previous_close: pd.Series,
) -> bool:
    if state.shares[symbol] <= 0 or state.entry_cost[symbol] <= 0:
        return False
    price = day_open.get(symbol, np.nan)
    if np.isnan(price) or price <= 0:
        return False
    reason = _risk_exit_reason(price / state.entry_cost[symbol] - 1.0, config)
    if reason is None:
        return False
    blocked = _execution_reason(
        symbol, "sell", date, float(price),
        _previous_close_value(day_previous_close, symbol),
        up_limit=market.up_limit,
        down_limit=market.down_limit,
        suspended=market.suspended,
        config=config,
    )
    if blocked:
        state.blocked_orders.append(
            BlockedOrder(date_str, symbol, "sell", blocked, note=reason)
        )
        return True
    execution_price = float(price) * (1 - config.trade.slippage)
    amount = state.shares[symbol] * execution_price
    cost = _sell_cost(amount, config.trade)
    state.cash += amount - cost
    state.trades.append(Trade(
        date_str, symbol, "sell", round(execution_price, 4),
        state.shares[symbol], round(amount, 2), round(cost, 2), note=reason,
    ))
    state.shares[symbol] = 0.0
    state.entry_cost[symbol] = 0.0
    return True


def _execute_risk_exits(
    state: _BacktestState,
    market: _MarketData,
    config: BacktestConfig,
    previous_close: pd.DataFrame,
    date,
    date_str: str,
) -> set[str]:
    if config.stop_loss is None and config.take_profit is None:
        return set()
    day_open = market.open_px.loc[date]
    day_previous_close = previous_close.loc[date]
    return {
        symbol
        for symbol in market.symbols
        if _try_risk_exit(
            state, market, config, date, date_str, symbol,
            day_open, day_previous_close,
        )
    }


def _portfolio_value_at_open(
    state: _BacktestState,
    symbols: list[str],
    day_open: pd.Series,
) -> float:
    shares = np.array([state.shares[s] for s in symbols], dtype=np.float64)
    prices = np.array(
        [float(day_open.get(s, np.nan)) if pd.notna(day_open.get(s, np.nan)) else 0.0 for s in symbols],
        dtype=np.float64,
    )
    # Use last_close as fallback for missing/zero prices
    fallback = np.array(
        [state.last_close.get(s, 0.0) for s in symbols], dtype=np.float64,
    )
    prices = np.where((prices > 0), prices, fallback)
    position_value = float(np.sum(shares * prices))
    return state.cash + position_value


def _normalized_target_weights(pending: pd.Series) -> pd.Series:
    weights = pending.fillna(0.0).clip(lower=0.0)
    total_weight = float(weights.sum())
    return weights / total_weight if total_weight > 1.0 + 1e-9 else weights


def _build_orders(
    state: _BacktestState,
    symbols: list[str],
    weights: pd.Series,
    day_open: pd.Series,
    portfolio_value: float,
    date_str: str,
) -> tuple[list[tuple[str, float]], bool]:
    n = len(symbols)
    target_values = np.zeros(n, dtype=np.float64)
    current_values = np.zeros(n, dtype=np.float64)
    valid_mask = np.ones(n, dtype=bool)
    retry_pending = False

    for i, symbol in enumerate(symbols):
        price = day_open.get(symbol, np.nan)
        tw = float(weights.get(symbol, 0.0))
        if np.isnan(price) or price <= 0:
            valid_mask[i] = False
            if state.shares[symbol] > 0 or tw > 0:
                side = "sell" if tw <= 0 else "rebalance"
                state.blocked_orders.append(
                    BlockedOrder(date_str, symbol, side, "missing_open")
                )
                retry_pending = True
            continue
        target_values[i] = portfolio_value * tw
        current_values[i] = state.shares[symbol] * price

    differences = target_values - current_values
    valid_indices = np.where(valid_mask)[0]
    valid_diffs = differences[valid_indices]
    sorted_indices = valid_indices[np.argsort(valid_diffs, kind="stable")]

    orders = [(symbols[i], float(differences[i])) for i in sorted_indices]
    return orders, retry_pending


def _execute_sell_order(
    state: _BacktestState,
    market: _MarketData,
    config: BacktestConfig,
    date,
    date_str: str,
    symbol: str,
    price: float,
    previous_close: float | None,
    difference: float,
) -> bool:
    blocked = _execution_reason(
        symbol, "sell", date, price, previous_close,
        up_limit=market.up_limit,
        down_limit=market.down_limit,
        suspended=market.suspended,
        config=config,
    )
    if blocked:
        state.blocked_orders.append(BlockedOrder(date_str, symbol, "sell", blocked))
        return True
    sell_shares = min(state.shares[symbol], -difference / price)
    if not config.allow_fractional and sell_shares < state.shares[symbol] - 1e-9:
        sell_shares = int(sell_shares // config.trade.lot_size) * config.trade.lot_size
    if sell_shares <= 0:
        return False
    execution_price = price * (1 - config.trade.slippage)
    amount = sell_shares * execution_price
    cost = _sell_cost(amount, config.trade)
    state.cash += amount - cost
    state.shares[symbol] -= sell_shares
    if state.shares[symbol] <= 1e-9:
        state.entry_cost[symbol] = 0.0
    state.trades.append(Trade(
        date_str, symbol, "sell", round(execution_price, 4),
        sell_shares, round(amount, 2), round(cost, 2),
    ))
    return False


def _execute_buy_order(
    state: _BacktestState,
    market: _MarketData,
    config: BacktestConfig,
    date,
    date_str: str,
    symbol: str,
    price: float,
    previous_close: float | None,
    difference: float,
    stopped_today: set[str],
) -> bool:
    if symbol in stopped_today:
        return False
    blocked = _execution_reason(
        symbol, "buy", date, price, previous_close,
        up_limit=market.up_limit,
        down_limit=market.down_limit,
        suspended=market.suspended,
        config=config,
    )
    if blocked:
        state.blocked_orders.append(BlockedOrder(date_str, symbol, "buy", blocked))
        return True
    execution_price = price * (1 + config.trade.slippage)
    budget = min(difference, state.cash)
    buy_shares = budget / (
        execution_price
        * (1 + config.trade.commission_rate + config.trade.transfer_fee_rate)
    )
    if not config.allow_fractional:
        buy_shares = int(buy_shares // config.trade.lot_size) * config.trade.lot_size
    if buy_shares <= 0:
        return False
    amount = buy_shares * execution_price
    cost = _buy_cost(amount, config.trade)
    if amount + cost > state.cash + 1e-6:
        state.blocked_orders.append(
            BlockedOrder(date_str, symbol, "buy", "insufficient_cash")
        )
        return False
    state.cash -= amount + cost
    previous_shares = state.shares[symbol]
    state.shares[symbol] += buy_shares
    state.entry_cost[symbol] = (
        previous_shares * state.entry_cost[symbol] + buy_shares * execution_price
    ) / state.shares[symbol]
    state.trades.append(Trade(
        date_str, symbol, "buy", round(execution_price, 4),
        buy_shares, round(amount, 2), round(cost, 2),
    ))
    return False


def _execute_orders(
    state: _BacktestState,
    market: _MarketData,
    config: BacktestConfig,
    date,
    date_str: str,
    day_open: pd.Series,
    day_previous_close: pd.Series,
    portfolio_value: float,
    orders: list[tuple[str, float]],
    stopped_today: set[str],
) -> bool:
    retry_pending = False
    for symbol, difference in orders:
        price = float(day_open[symbol])
        threshold = max(portfolio_value * 5e-4, 100 * price * 0.5)
        if abs(difference) < threshold:
            continue
        previous = _previous_close_value(day_previous_close, symbol)
        if difference < 0 and state.shares[symbol] > 0:
            retry_pending = _execute_sell_order(
                state, market, config, date, date_str,
                symbol, price, previous, difference,
            ) or retry_pending
        elif difference > 0:
            retry_pending = _execute_buy_order(
                state, market, config, date, date_str,
                symbol, price, previous, difference, stopped_today,
            ) or retry_pending
    return retry_pending


def _execute_pending_signal(
    state: _BacktestState,
    market: _MarketData,
    config: BacktestConfig,
    previous_close: pd.DataFrame,
    date,
    date_str: str,
    stopped_today: set[str],
) -> None:
    if state.pending is None:
        return
    day_open = market.open_px.loc[date]
    day_previous_close = previous_close.loc[date]
    portfolio_value = _portfolio_value_at_open(state, market.symbols, day_open)
    weights = _normalized_target_weights(state.pending)
    orders, retry_pending = _build_orders(
        state, market.symbols, weights, day_open, portfolio_value, date_str,
    )
    retry_pending = _execute_orders(
        state, market, config, date, date_str, day_open, day_previous_close,
        portfolio_value, orders, stopped_today,
    ) or retry_pending
    state.pending = weights if retry_pending else None


def _settle_close(
    state: _BacktestState,
    symbols: list[str],
    date,
    day_close: pd.Series,
) -> None:
    position_value = 0.0
    row = {"date": date}
    for symbol in symbols:
        price = day_close.get(symbol, np.nan)
        if not np.isnan(price):
            state.last_close[symbol] = float(price)
        if state.shares[symbol] > 0:
            value = state.shares[symbol] * state.last_close.get(symbol, 0.0)
            position_value += value
            row[symbol] = value
    state.nav_values.append(state.cash + position_value)
    state.position_rows.append(row)


def _benchmark_nav(
    benchmark_close: pd.Series | None,
    dates: pd.Index,
) -> pd.Series | None:
    if benchmark_close is None or len(benchmark_close.dropna()) <= 2:
        return None
    benchmark = benchmark_close.reindex(dates).ffill()
    return benchmark / benchmark.iloc[0]


def _trade_participation(
    trade: Trade,
    average_daily_value: pd.DataFrame,
) -> float | None:
    timestamp = pd.Timestamp(trade.date)
    if (
        timestamp not in average_daily_value.index
        or trade.symbol not in average_daily_value.columns
    ):
        return None
    value = average_daily_value.at[timestamp, trade.symbol]
    if pd.isna(value) or float(value) <= 0:
        return None
    return float(trade.amount) / float(value)


def _capacity_metrics(
    volume: pd.DataFrame | None,
    close_px: pd.DataFrame,
    trades: list[Trade],
) -> dict[str, float]:
    if volume is None or volume.empty or not trades:
        return {}
    average_daily_value = (
        volume.reindex_like(close_px).mul(close_px).rolling(20, min_periods=5).mean()
    )
    participation = [
        value
        for trade in trades
        if (value := _trade_participation(trade, average_daily_value)) is not None
    ]
    if not participation:
        return {}
    return {
        "capacity_max_participation": round(max(participation), 6),
        "capacity_p95_participation": round(
            float(np.quantile(participation, 0.95)), 6
        ),
    }


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
    market = _prepare_market_data(panel)
    from quantmaster.market_capabilities import (
        MarketCapability,
        require_symbols_capability,
    )

    require_symbols_capability(market.symbols, MarketCapability.BACKTEST)
    _validate_production_panel(panel, config)
    target_weights = target_weights.reindex(
        index=market.dates,
        columns=market.symbols,
    )
    previous_close = market.close_px.shift(1)
    state = _initial_state(config, market.symbols)

    for index, date in enumerate(market.dates):
        date_str = str(date.date())
        if market.adj_factor is not None:
            factors = market.adj_factor.reindex(
                index=[date], columns=market.symbols
            ).iloc[0]
            _apply_adjustment_factors(state, factors, market.symbols)
        stopped_today = _execute_risk_exits(
            state, market, config, previous_close, date, date_str
        )
        _execute_pending_signal(
            state, market, config, previous_close, date, date_str, stopped_today,
        )
        _settle_close(state, market.symbols, date, market.close_px.loc[date])
        signal_row = target_weights.iloc[index]
        if signal_row.notna().any():
            state.pending = signal_row

    nav = pd.Series(
        state.nav_values, index=market.dates, name="nav"
    ) / config.initial_capital
    returns = nav.pct_change(fill_method=None).fillna(0.0)
    positions = pd.DataFrame(state.position_rows).set_index("date").fillna(0.0)
    benchmark_nav = _benchmark_nav(benchmark_close, market.dates)
    metrics = performance_metrics(
        returns, benchmark_nav=benchmark_nav, trades=state.trades
    )
    metrics["blocked_order_count"] = len(state.blocked_orders)
    metrics.update(_capacity_metrics(panel.get("volume"), market.close_px, state.trades))
    return BacktestResult(
        nav=nav, returns=returns, positions=positions,
        trades=state.trades, metrics=metrics, benchmark_nav=benchmark_nav,
        blocked_orders=state.blocked_orders, initial_capital=config.initial_capital,
        close_prices=market.close_px,
    )
