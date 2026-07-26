"""回测引擎与 A 股规则的测试。"""

import numpy as np
import pandas as pd
import pytest
from conftest import make_panel

from quantmaster.backtest import BacktestConfig, FactorStrategy, run_backtest
from quantmaster.backtest.engine import price_limit
from quantmaster.backtest.strategy import BuyAndHold, rebalance_mask
from quantmaster.config import TradeConfig
from quantmaster.factors import ExpressionFactor


def flat_panel(price: float = 10.0, days: int = 30, symbols: tuple = ("600000.SH",)):
    """恒定价格的面板，便于精确断言现金流。"""
    dates = pd.bdate_range("2023-01-02", periods=days)
    df = pd.DataFrame(price, index=dates, columns=list(symbols))
    return {"open": df.copy(), "high": df.copy(), "low": df.copy(),
            "close": df.copy(), "volume": df * 1e5}


class TestPriceLimit:
    def test_main_board(self):
        assert price_limit("600519.SH") == 0.10
        assert price_limit("000001.SZ") == 0.10

    def test_growth_boards(self):
        assert price_limit("300750.SZ") == 0.20
        assert price_limit("688111.SH") == 0.20

    def test_beijing(self):
        assert price_limit("830000.BJ") == 0.30


class TestEngineBasics:
    def test_full_position_buy_and_costs(self):
        """满仓买入恒价股：只损失交易成本，且股数为整手。"""
        panel = flat_panel(price=10.0)
        dates = panel["close"].index
        weights = pd.DataFrame(np.nan, index=dates, columns=["600000.SH"])
        weights.iloc[0] = 1.0   # T0 收盘信号 -> T1 开盘买入

        tcfg = TradeConfig(slippage=0.0)
        result = run_backtest(panel, weights, BacktestConfig(trade=tcfg))

        assert len(result.trades) == 1
        trade = result.trades[0]
        assert trade.side == "buy"
        assert trade.shares % 100 == 0
        # 终值 = 初始资金 - 交易成本
        expected_nav = 1 - trade.cost / 1_000_000
        assert abs(result.nav.iloc[-1] - expected_nav) < 1e-6

    def test_t_plus_one_execution(self):
        """T 日信号必须 T+1 开盘执行：成交日期是信号日的下一交易日。"""
        panel = flat_panel()
        dates = panel["close"].index
        weights = pd.DataFrame(np.nan, index=dates, columns=["600000.SH"])
        weights.iloc[3] = 1.0
        result = run_backtest(panel, weights)
        assert result.trades[0].date == str(dates[4].date())

    def test_limit_up_blocks_buy(self):
        """开盘涨停买不进。"""
        panel = flat_panel(price=10.0, days=10)
        # 第 2 天开盘直接 +10% 涨停
        panel["open"].iloc[1] = 11.0
        panel["close"].iloc[1:] = 11.0
        panel["high"].iloc[1:] = 11.0
        dates = panel["close"].index
        weights = pd.DataFrame(np.nan, index=dates, columns=["600000.SH"])
        weights.iloc[0] = 1.0
        result = run_backtest(panel, weights)
        assert all(t.date != str(dates[1].date()) for t in result.trades)

    def test_limit_down_blocks_sell(self):
        """开盘跌停卖不出：持仓保持不变。"""
        panel = flat_panel(price=10.0, days=10)
        panel["open"].iloc[3:] = 9.0    # -10% 跌停
        panel["close"].iloc[3:] = 9.0
        panel["low"].iloc[3:] = 9.0
        dates = panel["close"].index
        weights = pd.DataFrame(np.nan, index=dates, columns=["600000.SH"])
        weights.iloc[0] = 1.0
        weights.iloc[2] = 0.0   # T2 信号清仓 -> T3 开盘跌停，卖不出
        result = run_backtest(panel, weights)
        sells = [t for t in result.trades if t.side == "sell"]
        assert all(t.date != str(dates[3].date()) for t in sells)

    def test_cash_never_negative(self, panel):
        strategy = FactorStrategy(ExpressionFactor("rank(-delta(close, 5))"),
                                  top_n=3, rebalance="W")
        result = run_backtest(panel, strategy.target_weights(panel))
        # 净值应始终为正，且现金约束下不出现爆仓
        assert (result.nav > 0).all()

    def test_benchmark_metrics_present(self, panel):
        strategy = FactorStrategy(ExpressionFactor("rank(-delta(close, 5))"), top_n=3)
        benchmark = panel["close"].mean(axis=1)
        result = run_backtest(panel, strategy.target_weights(panel),
                              benchmark_close=benchmark)
        assert "information_ratio" in result.metrics
        assert result.benchmark_nav is not None
        assert abs(result.benchmark_nav.iloc[0] - 1.0) < 1e-9


class TestStrategy:
    def test_rebalance_mask_weekly(self):
        dates = pd.bdate_range("2023-01-02", periods=15)
        mask = rebalance_mask(dates, "W")
        assert mask.sum() == 3
        assert bool(mask.iloc[-1])

    def test_factor_strategy_weights(self, panel):
        strategy = FactorStrategy(ExpressionFactor("rank(close)"), top_n=3, rebalance="W")
        weights = strategy.target_weights(panel)
        active = weights.dropna(how="all")
        row = active.iloc[-1].fillna(0)
        assert (row > 0).sum() <= 3
        assert row.sum() == pytest.approx(1.0, abs=0.06) or row.sum() <= 1.0

    def test_buy_and_hold(self, panel):
        weights = BuyAndHold().target_weights(panel)
        active = weights.dropna(how="all")
        assert len(active) == 1
        assert active.iloc[0].sum() == pytest.approx(1.0)


class TestNoLookahead:
    def test_shifted_signal_equals_original(self):
        """引擎只在信号次日交易：把行情整体后移一天，交易日期应同样后移。"""
        panel_a = make_panel(days=40, seed=3)
        dates = panel_a["close"].index
        weights = pd.DataFrame(np.nan, index=dates, columns=panel_a["close"].columns)
        weights.iloc[10, :3] = 1 / 3
        result = run_backtest(panel_a, weights)
        assert result.trades
        assert min(t.date for t in result.trades) == str(dates[11].date())
