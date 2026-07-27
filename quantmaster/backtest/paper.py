"""模拟盘（Paper Trading）：用真实行情、虚拟资金演练策略。

与回测的关系：回测跑历史，模拟盘跑「现在」。旧 ``PaperTrader`` 仅保留
兼容读取和提案能力；多账户、确认与 T+1 开盘撮合由 ``PaperService`` 负责。

模拟账本与实盘账本共用 Ledger 结构（存于 ledger_paper.sqlite）。
"""

from __future__ import annotations

import pandas as pd

from quantmaster.backtest.strategy import Strategy
from quantmaster.config import get_config
from quantmaster.portfolio.ledger import Ledger, TradeRecord
from quantmaster.portfolio.performance import ledger_report


class PaperTrader:
    def __init__(self, initial_capital: float = 1_000_000.0, name: str = "paper",
                 initialize: bool = True):
        self.initial_capital = initial_capital
        self.ledger = Ledger(name=name)
        cashflows = self.ledger.cashflows()
        if initialize and cashflows.empty:
            self.ledger.add_cashflow(
                str(pd.Timestamp.now().date()), initial_capital, "deposit", "模拟盘初始资金"
            )

    def plan_rebalance(self, weights: dict[str, float], prices: dict[str, float]) -> list[TradeRecord]:
        """生成调仓成交预览，不写入模拟账本。"""
        tcfg = get_config().trade
        today = str(pd.Timestamp.now().date())
        report = ledger_report(self.ledger, prices=prices)
        total = report["total_assets"] or self.initial_capital
        current = {p["symbol"]: p["shares"] for p in report["positions"] if p["shares"] > 0}

        planned: list[TradeRecord] = []
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
            planned.append(trade)
        return planned

    def apply_rebalance(
        self, trades: list[TradeRecord], idempotency_prefix: str | None = None,
    ) -> list[TradeRecord]:
        """按预览写入账本；幂等前缀保证机器人重试不会重复成交。"""
        if self.ledger.cashflows().empty:
            self.ledger.add_cashflow(
                str(pd.Timestamp.now().date()), self.initial_capital, "deposit", "模拟盘初始资金",
                idempotency_key=f"{idempotency_prefix}:initial" if idempotency_prefix else None,
            )
        executed: list[TradeRecord] = []
        for index, trade in enumerate(trades):
            key = f"{idempotency_prefix}:trade:{index}" if idempotency_prefix else None
            if self.ledger.add_trade(trade, idempotency_key=key):
                executed.append(trade)
        return executed

    def rebalance_to(self, weights: dict[str, float], prices: dict[str, float]) -> list[TradeRecord]:
        """把模拟持仓调整到目标权重。返回生成的成交记录。"""
        return self.apply_rebalance(self.plan_rebalance(weights, prices))

    @staticmethod
    def _signal(
        strategy: Strategy, panel: dict[str, pd.DataFrame],
    ) -> tuple[str, dict[str, float], dict[str, float]]:
        weights_df = strategy.target_weights(panel)
        signals = weights_df.dropna(how="all")
        if signals.empty:
            raise ValueError("策略没有生成有效调仓信号")
        latest = pd.to_numeric(signals.iloc[-1], errors="coerce").fillna(0.0)
        weights = {s: float(w) for s, w in latest.items() if w > 0}
        close = panel["close"]
        prices = {s: float(close[s].dropna().iloc[-1]) for s in close.columns
                  if close[s].notna().any()}
        return str(signals.index[-1].date()), weights, prices

    def propose_once(
        self, strategy: Strategy, universe: list[str], lookback_days: int = 400,
        panel: dict[str, pd.DataFrame] | None = None,
    ) -> dict:
        """计算模拟调仓提案，整个过程不写入成交或现金流水。"""
        if panel is None:
            from quantmaster.data import load_panel

            end = pd.Timestamp.now().normalize()
            start = end - pd.Timedelta(days=lookback_days)
            panel = load_panel(universe, str(start.date()), str(end.date()))
        signal_date, weights, prices = self._signal(strategy, panel)
        planned = self.plan_rebalance(weights, prices)
        return {
            "signal_date": signal_date, "target_weights": weights, "prices": prices,
            "planned": [trade.__dict__ for trade in planned],
        }

    def run_once(
        self,
        strategy: Strategy,
        universe: list[str],
        lookback_days: int = 400,
        panel: dict[str, pd.DataFrame] | None = None,
    ) -> dict:  # pragma: no cover - 网络
        """兼容别名：取最新行情并生成提案，不再直接写入成交。

        调用方已经完成每日行情更新时可传 panel，避免重复加载与重复触网。
        """
        proposal = self.propose_once(
            strategy, universe, lookback_days=lookback_days, panel=panel,
        )
        return {
            **proposal,
            "executed": [],
            "deprecated": True,
            "notice": "run_once 只生成提案；请使用 PaperService 确认并在 T+1 开盘撮合。",
        }

    def report(self, prices: dict[str, float] | None = None) -> dict:
        return ledger_report(self.ledger, prices=prices)
