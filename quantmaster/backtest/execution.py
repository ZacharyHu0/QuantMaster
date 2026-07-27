"""回测与模拟盘共用的 A 股成交规则。

本模块只负责确定一笔订单能否在给定开盘价成交，以及精确计算费用。
账户状态、目标权重和订单生命周期由上层服务管理。
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from quantmaster.config import TradeConfig


def price_limit(symbol: str) -> float:
    """按证券代码推断普通涨跌停幅度。"""
    code = symbol.split(".")[0]
    if code.startswith(("688", "689", "300", "301")):
        return 0.20
    if code.startswith(("8", "4")) and symbol.endswith(".BJ"):
        return 0.30
    return 0.10


def buy_cost(amount: float, trade: TradeConfig) -> float:
    return max(amount * trade.commission_rate, trade.commission_min) + (
        amount * trade.transfer_fee_rate
    )


def sell_cost(amount: float, trade: TradeConfig) -> float:
    return (
        max(amount * trade.commission_rate, trade.commission_min)
        + amount * trade.stamp_tax_rate
        + amount * trade.transfer_fee_rate
    )


def limit_reason(
    symbol: str,
    side: str,
    open_price: float | None,
    previous_close: float | None,
    *,
    enabled: bool = True,
) -> str:
    """返回订单在开盘阶段不能成交的原因；空字符串表示可成交。"""
    if open_price is None or not math.isfinite(open_price) or open_price <= 0:
        return "missing_open"
    if not enabled or previous_close is None or not math.isfinite(previous_close):
        return ""
    limit = price_limit(symbol)
    if side == "buy" and open_price >= previous_close * (1 + limit) * 0.998:
        return "limit_up"
    if side == "sell" and open_price <= previous_close * (1 - limit) * 1.002:
        return "limit_down"
    return ""


def executable_buy_shares(
    cash: float,
    desired_value: float,
    open_price: float,
    trade: TradeConfig,
    *,
    allow_fractional: bool = False,
) -> float:
    """在完整费用约束下返回不会使现金为负的最大买入数量。"""
    if cash <= 0 or desired_value <= 0 or open_price <= 0:
        return 0.0
    execution_price = open_price * (1 + trade.slippage)
    budget = min(cash, desired_value)
    estimate = budget / (execution_price * (1 + trade.commission_rate + trade.transfer_fee_rate))
    if allow_fractional:
        shares = estimate
    else:
        shares = math.floor(estimate / trade.lot_size) * trade.lot_size
    while shares > 0:
        amount = shares * execution_price
        if amount + buy_cost(amount, trade) <= cash + 1e-8:
            return float(shares)
        shares = shares - (1 if allow_fractional else trade.lot_size)
    return 0.0


@dataclass(frozen=True)
class ExecutionQuote:
    symbol: str
    side: str
    open_price: float
    previous_close: float | None
    execution_price: float
    blocked_reason: str = ""


def quote_open(
    symbol: str,
    side: str,
    open_price: float | None,
    previous_close: float | None,
    trade: TradeConfig,
    *,
    limit_check: bool = True,
) -> ExecutionQuote:
    reason = limit_reason(
        symbol, side, open_price, previous_close, enabled=limit_check,
    )
    raw = float(open_price or 0.0)
    execution_price = raw * (1 + trade.slippage if side == "buy" else 1 - trade.slippage)
    return ExecutionQuote(
        symbol=symbol,
        side=side,
        open_price=raw,
        previous_close=previous_close,
        execution_price=execution_price,
        blocked_reason=reason,
    )
