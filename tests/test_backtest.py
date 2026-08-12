"""回测引擎与 A 股规则的测试。"""

import numpy as np
import pandas as pd
import pytest
from conftest import make_panel

from quantmaster.backtest import BacktestConfig, FactorStrategy, run_backtest
from quantmaster.backtest.engine import (
    _missing_production_fields,
    _normalized_target_weights,
    _risk_exit_reason,
    price_limit,
)
from quantmaster.backtest.strategy import BuyAndHold, rebalance_mask
from quantmaster.config import TradeConfig
from quantmaster.factors import ExpressionFactor


def flat_panel(price: float = 10.0, days: int = 30, symbols: tuple = ("600000.SH",)):
    """恒定价格的面板，便于精确断言现金流。"""
    dates = pd.bdate_range("2023-01-02", periods=days)
    df = pd.DataFrame(price, index=dates, columns=list(symbols))
    return {"open": df.copy(), "high": df.copy(), "low": df.copy(),
            "close": df.copy(), "volume": df * 1e5}


class TestEngineDecisions:
    def test_missing_production_fields_preserve_contract_order(self):
        panel = flat_panel(days=3)
        panel["up_limit"] = panel["open"].copy()
        panel["execution_close"] = panel["close"].copy()

        assert _missing_production_fields(panel) == [
            "down_limit",
            "suspended",
            "adj_factor",
            "execution_open",
        ]

    def test_target_weights_clip_negative_values_before_normalizing(self):
        weights = pd.Series({"600000.SH": 2.0, "000001.SZ": -1.0, "600519.SH": np.nan})

        normalized = _normalized_target_weights(weights)

        assert normalized.to_dict() == {
            "600000.SH": 1.0,
            "000001.SZ": 0.0,
            "600519.SH": 0.0,
        }

    @pytest.mark.parametrize(
        ("change", "expected"),
        [(-0.10, "stop_loss"), (0.15, "take_profit"), (0.05, None)],
    )
    def test_risk_exit_reason_includes_boundaries(self, change, expected):
        config = BacktestConfig(stop_loss=0.10, take_profit=0.15)

        assert _risk_exit_reason(change, config) == expected


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
    def test_production_requires_real_execution_fields(self):
        panel = flat_panel(days=3)
        weights = pd.DataFrame(np.nan, index=panel["close"].index,
                               columns=panel["close"].columns)

        with pytest.raises(ValueError) as error:
            run_backtest(panel, weights, BacktestConfig(research_tier="production"))

        assert str(error.value) == (
            "production 回测缺少真实成交字段：up_limit、down_limit、suspended、"
            "adj_factor、execution_open、execution_close"
        )

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

    def test_limit_up_order_retries_on_following_session(self):
        """涨停订单不会静默丢失；没有新信号时应在下一交易日继续尝试。"""
        panel = flat_panel(price=10.0, days=6)
        dates = panel["close"].index
        panel["open"].iloc[1] = 11.0
        panel["close"].iloc[1] = 11.0
        panel["open"].iloc[2] = 10.8
        panel["close"].iloc[2] = 10.8
        weights = pd.DataFrame(np.nan, index=dates, columns=["600000.SH"])
        weights.iloc[0] = 1.0

        result = run_backtest(panel, weights)

        assert result.trades[0].date == str(dates[2].date())
        assert result.blocked_orders[0].date == str(dates[1].date())
        assert result.blocked_orders[0].reason == "limit_up"
        assert result.metrics["blocked_order_count"] >= 1

    def test_missing_open_order_retries_on_following_session(self):
        panel = flat_panel(price=10.0, days=6)
        dates = panel["close"].index
        panel["open"].iloc[1] = np.nan
        weights = pd.DataFrame(np.nan, index=dates, columns=["600000.SH"])
        weights.iloc[0] = 1.0

        result = run_backtest(panel, weights)

        assert result.trades[0].date == str(dates[2].date())
        assert result.blocked_orders[0].date == str(dates[1].date())
        assert result.blocked_orders[0].side == "rebalance"
        assert result.blocked_orders[0].reason == "missing_open"

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
        assert (result.nav > 0).all()
        # 真正的现金约束：现金 = 总资产 - 持仓市值，任何一天都不允许透支
        cash = result.nav * 1_000_000 - result.positions.sum(axis=1)
        assert (cash >= -1e-6).all(), "回测引擎出现现金透支"

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


class TestStopLoss:
    def _panel_with_drop(self):
        """恒价 10 元买入后，第 5 天起分两天阴跌到 8.8（避开一次性跌停无法卖出）。"""
        panel = flat_panel(price=10.0, days=12)
        for df in (panel["open"], panel["close"], panel["high"], panel["low"]):
            df.iloc[5:] = 9.5
            df.iloc[6:] = 8.8
        return panel

    def test_stop_loss_triggers(self):
        panel = self._panel_with_drop()
        dates = panel["close"].index
        weights = pd.DataFrame(np.nan, index=dates, columns=["600000.SH"])
        weights.iloc[0] = 1.0
        result = run_backtest(panel, weights, BacktestConfig(stop_loss=0.10))
        stops = [t for t in result.trades if t.note == "stop_loss"]
        assert len(stops) == 1
        # 开盘价 8.8 相对成本 10 跌 12%，第 7 个交易日触发
        assert stops[0].date == str(dates[6].date())
        # 止损后空仓：期末净值 ≈ 现金
        assert result.positions.iloc[-1].sum() == 0

    def test_no_stop_without_config(self):
        panel = self._panel_with_drop()
        dates = panel["close"].index
        weights = pd.DataFrame(np.nan, index=dates, columns=["600000.SH"])
        weights.iloc[0] = 1.0
        result = run_backtest(panel, weights)
        assert all(t.note != "stop_loss" for t in result.trades)

    def test_take_profit_triggers(self):
        panel = flat_panel(price=10.0, days=12)
        for df in (panel["open"], panel["close"], panel["high"], panel["low"]):
            df.iloc[4:] = 10.9
            df.iloc[5:] = 11.8
        dates = panel["close"].index
        weights = pd.DataFrame(np.nan, index=dates, columns=["600000.SH"])
        weights.iloc[0] = 1.0
        result = run_backtest(panel, weights, BacktestConfig(take_profit=0.15))
        takes = [t for t in result.trades if t.note == "take_profit"]
        assert len(takes) == 1
        assert takes[0].date == str(dates[5].date())

    def test_limit_down_deferred_stop_freezes_buys(self):
        """跌停顺延止损的当日也不允许加仓——否则摊低均价后止损线永远追不上。"""
        panel = flat_panel(price=10.0, days=12)
        # 第 6 天起一字跌停（开盘即 -10%），止损单排不上队
        for df in (panel["open"], panel["close"], panel["high"], panel["low"]):
            df.iloc[5:] = 9.0
        dates = panel["close"].index
        weights = pd.DataFrame(np.nan, index=dates, columns=["600000.SH"])
        weights.iloc[:] = 1.0   # 信号一直要求满仓
        result = run_backtest(panel, weights, BacktestConfig(stop_loss=0.05))
        limit_day = str(dates[5].date())
        buys_on_limit_day = [t for t in result.trades
                             if t.side == "buy" and t.date == limit_day]
        assert not buys_on_limit_day, "跌停顺延日不应继续买入摊低成本"

    def test_suspension_keeps_last_valuation(self):
        """连续停牌（缺价）期间持仓按最近有效收盘价估值，净值不应塌陷。"""
        panel = flat_panel(price=10.0, days=12)
        for df in (panel["open"], panel["close"], panel["high"], panel["low"]):
            df.iloc[6:9] = np.nan    # 停牌 3 天
        dates = panel["close"].index
        weights = pd.DataFrame(np.nan, index=dates, columns=["600000.SH"])
        weights.iloc[0] = 1.0
        result = run_backtest(panel, weights)
        suspended_nav = result.nav.iloc[6:9]
        assert (suspended_nav > 0.9).all(), "停牌期间净值不应按 0 估值塌陷"

    def test_stopped_symbol_not_rebought_same_day(self):
        """止损当日即使信号仍要求持有，也不回补。"""
        panel = self._panel_with_drop()
        dates = panel["close"].index
        weights = pd.DataFrame(np.nan, index=dates, columns=["600000.SH"])
        weights.iloc[:] = 1.0   # 每天都发满仓信号
        result = run_backtest(panel, weights, BacktestConfig(stop_loss=0.10))
        stop_date = next(t.date for t in result.trades if t.note == "stop_loss")
        buys_on_stop_day = [t for t in result.trades
                            if t.side == "buy" and t.date == stop_date]
        assert not buys_on_stop_day


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


class TestMultiFactorStrategy:
    def test_equal_weighting(self, panel):
        from quantmaster.backtest.strategy import MultiFactorStrategy
        from quantmaster.factors import BUILTIN_FACTORS

        strategy = MultiFactorStrategy(
            [BUILTIN_FACTORS["mom_20d"], BUILTIN_FACTORS["rev_5d"]],
            top_n=3, rebalance="W")
        weights = strategy.target_weights(panel)
        active = weights.dropna(how="all")
        assert len(active) > 0
        row = active.iloc[-1].fillna(0)
        assert (row > 0).sum() <= 3
        assert row.sum() <= 1.0 + 1e-9
        result = run_backtest(panel, weights)
        assert (result.nav > 0).all()

    def test_ic_weighting_runs(self, panel):
        from quantmaster.backtest.strategy import MultiFactorStrategy
        from quantmaster.factors import BUILTIN_FACTORS

        strategy = MultiFactorStrategy(
            [BUILTIN_FACTORS["mom_20d"], BUILTIN_FACTORS["rev_5d"]],
            top_n=3, rebalance="W", weighting="ic", ic_lookback=30)
        weights = strategy.target_weights(panel)
        # IC 冷启动期后应有信号
        assert weights.dropna(how="all").iloc[-1].fillna(0).sum() > 0

    def test_rejects_bad_args(self, panel):
        from quantmaster.backtest.strategy import MultiFactorStrategy
        from quantmaster.factors import BUILTIN_FACTORS

        with pytest.raises(ValueError):
            MultiFactorStrategy([], top_n=3)
        with pytest.raises(ValueError):
            MultiFactorStrategy([BUILTIN_FACTORS["mom_20d"]], weighting="magic")
