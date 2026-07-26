"""模拟盘（Paper Trading）：用真实行情、虚拟资金演练策略。

与回测的关系：回测跑历史，模拟盘跑「现在」。每个交易日收盘后执行一次
`run_once`，按策略最新信号在模拟账本里调仓（成交价用最新收盘价+滑点），
积累一段时间后用 ledger_report 查看虚拟收益，再决定是否实盘。

模拟账本与实盘账本共用 Ledger 结构（存于 ledger_paper.sqlite）。
"""

from __future__ import annotations

import pandas as pd

from quantmaster.backtest.strategy import Strategy
from quantmaster.config import get_config
from quantmaster.portfolio.ledger import Ledger, TradeRecord
from quantmaster.portfolio.performance import ledger_report


class PaperTrader:
    def __init__(self, initial_capital: float = 1_000_000.0, name: str = "paper"):
        self.ledger = Ledger(name=name)
        cashflows = self.ledger.cashflows()
        if cashflows.empty:
            self.ledger.add_cashflow(
                str(pd.Timestamp.now().date()), initial_capital, "deposit", "模拟盘初始资金"
            )

    def rebalance_to(self, weights: dict[str, float], prices: dict[str, float]) -> list[TradeRecord]:
        """把模拟持仓调整到目标权重。返回生成的成交记录。"""
        tcfg = get_config().trade
        today = str(pd.Timestamp.now().date())
        report = ledger_report(self.ledger, prices=prices)
        total = report["total_assets"]
        current = {p["symbol"]: p["shares"] for p in report["positions"] if p["shares"] > 0}

        executed: list[TradeRecord] = []
        # 先卖后买
        for symbol in sorted(set(current) | set(weights),
                             key=lambda s: weights.get(s, 0.0)):
            price = prices.get(symbol)
            if not price or price <= 0:
                continue
            target_shares = int(total * weights.get(symbol, 0.0) / price // tcfg.lot_size) * tcfg.lot_size
            diff = target_shares - current.get(symbol, 0)
            if diff == 0:
                continue
            side = "buy" if diff > 0 else "sell"
            exec_price = price * (1 + tcfg.slippage) if diff > 0 else price * (1 - tcfg.slippage)
            amount = abs(diff) * exec_price
            fee = max(amount * tcfg.commission_rate, tcfg.commission_min)
            if side == "sell":
                fee += amount * tcfg.stamp_tax_rate
            trade = TradeRecord(date=today, symbol=symbol, side=side,
                                price=round(exec_price, 4), shares=abs(diff),
                                fee=round(fee, 2), note="paper")
            self.ledger.add_trade(trade)
            executed.append(trade)
        return executed

    def run_once(self, strategy: Strategy, universe: list[str],
                 lookback_days: int = 400) -> dict:  # pragma: no cover - 网络
        """取最新行情 → 计算策略目标权重 → 模拟调仓。"""
        from quantmaster.data import load_panel

        end = pd.Timestamp.now().normalize()
        start = end - pd.Timedelta(days=lookback_days)
        panel = load_panel(universe, str(start.date()), str(end.date()))
        weights_df = strategy.target_weights(panel)
        latest = weights_df.dropna(how="all").iloc[-1].fillna(0.0)
        weights = {s: float(w) for s, w in latest.items() if w > 0}

        close = panel["close"]
        prices = {s: float(close[s].dropna().iloc[-1]) for s in close.columns
                  if close[s].notna().any()}
        executed = self.rebalance_to(weights, prices)
        report = ledger_report(self.ledger, prices=prices)
        return {
            "signal_date": str(weights_df.dropna(how="all").index[-1].date()),
            "target_weights": weights,
            "executed": [t.__dict__ for t in executed],
            "report": report,
        }

    def report(self, prices: dict[str, float] | None = None) -> dict:
        return ledger_report(self.ledger, prices=prices)
