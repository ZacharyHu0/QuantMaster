"""回测分析报告（分年/月度/成交统计/完整 JSON 报告）的测试。"""

import json

import numpy as np
import pandas as pd
import pytest
from conftest import make_panel

from quantmaster.backtest import run_backtest
from quantmaster.backtest.engine import Trade
from quantmaster.backtest.report import (
    full_report,
    monthly_return_table,
    trade_stats,
    yearly_returns,
)


def _two_year_returns() -> pd.Series:
    """跨 2022/2023 两年的确定性日收益序列。"""
    dates = pd.bdate_range("2022-12-26", "2023-01-13")
    return pd.Series(0.01, index=dates)


class TestYearlyReturns:
    def test_splits_across_two_years(self):
        r = _two_year_returns()
        table = yearly_returns(r)
        assert list(table.index) == ["2022", "2023"]
        n_2022 = int((r.index.year == 2022).sum())
        n_2023 = int((r.index.year == 2023).sum())
        assert table.loc["2022", "days"] == n_2022
        assert table.loc["2023", "days"] == n_2023
        # 当年累计收益 = (1+r)^n - 1（按自然年切分，不能混年）
        assert table.loc["2022", "return"] == pytest.approx(1.01 ** n_2022 - 1)
        assert table.loc["2023", "return"] == pytest.approx(1.01 ** n_2023 - 1)

    def test_drawdown_and_sharpe_columns(self):
        dates = pd.bdate_range("2023-01-02", periods=10)
        r = pd.Series([0.05, -0.10] + [0.01] * 8, index=dates)
        table = yearly_returns(r)
        assert table.loc["2023", "max_drawdown"] == pytest.approx(0.10)
        assert set(table.columns) == {"return", "volatility", "max_drawdown", "sharpe", "days"}

    def test_empty_and_all_nan_do_not_crash(self):
        assert yearly_returns(pd.Series(dtype=float)).empty
        nan_series = pd.Series(np.nan, index=pd.bdate_range("2023-01-02", periods=20))
        assert yearly_returns(nan_series).empty


class TestMonthlyReturnTable:
    def test_known_month_value(self):
        dates = pd.bdate_range("2023-01-02", "2023-02-28")
        r = pd.Series(0.01, index=dates)
        table = monthly_return_table(r)
        n_jan = int((r.index.month == 1).sum())
        assert table.loc["2023", 1] == pytest.approx(1.01 ** n_jan - 1)

    def test_missing_months_are_nan(self):
        dates = pd.bdate_range("2023-03-01", "2023-03-31")
        table = monthly_return_table(pd.Series(0.002, index=dates))
        assert list(table.columns) == list(range(1, 13))
        assert np.isnan(table.loc["2023", 1])       # 没有 1 月数据
        assert not np.isnan(table.loc["2023", 3])

    def test_empty_and_all_nan_do_not_crash(self):
        assert monthly_return_table(pd.Series(dtype=float)).empty
        nan_series = pd.Series(np.nan, index=pd.bdate_range("2023-01-02", periods=20))
        assert monthly_return_table(nan_series).empty


class TestTradeStats:
    def _trades(self) -> list[Trade]:
        return [
            Trade("2023-01-03", "600000.SH", "buy", 10.0, 1000, 10000.0, 5.0),
            Trade("2023-01-10", "600000.SH", "sell", 11.0, 1000, 11000.0, 12.0),
            Trade("2023-01-03", "600001.SH", "buy", 20.0, 100, 2000.0, 5.0),
        ]

    def test_counts_costs_and_avg(self):
        stats = trade_stats(self._trades())
        assert stats["trade_count"] == 3
        assert stats["buy_count"] == 2
        assert stats["sell_count"] == 1
        assert stats["total_cost"] == pytest.approx(22.0)
        assert stats["avg_amount"] == pytest.approx(23000.0 / 3, abs=0.01)

    def test_top_symbols_aggregated_and_sorted(self):
        stats = trade_stats(self._trades())
        top = stats["top_symbols"]
        assert top[0]["symbol"] == "600000.SH"      # 10000+11000 > 2000
        assert top[0]["amount"] == pytest.approx(21000.0)
        assert top[0]["count"] == 2
        assert len(top) <= 10

    def test_empty_trades(self):
        stats = trade_stats([])
        assert stats["trade_count"] == 0
        assert stats["top_symbols"] == []


class TestFullReport:
    def test_json_serializable_without_nan(self):
        panel = make_panel(days=80, seed=11)
        dates = panel["close"].index
        weights = pd.DataFrame(np.nan, index=dates, columns=panel["close"].columns)
        weights.iloc[5, :3] = 1 / 3
        result = run_backtest(panel, weights)

        report = full_report(result)
        assert set(report) == {"metrics", "yearly", "monthly", "trade_stats"}
        # allow_nan=False：任何残留 NaN/Inf 都会让 dumps 直接报错
        text = json.dumps(report, allow_nan=False, ensure_ascii=False)
        assert "NaN" not in text

    def test_records_shapes(self):
        panel = make_panel(days=80, seed=11)
        dates = panel["close"].index
        weights = pd.DataFrame(np.nan, index=dates, columns=panel["close"].columns)
        weights.iloc[5, :3] = 1 / 3
        result = run_backtest(panel, weights)
        report = full_report(result)

        assert report["yearly"][0]["year"] == "2023"
        month_keys = {str(m) for m in range(1, 13)}
        assert month_keys <= set(report["monthly"][0])
        # 缺失月份必须是 None 而非 NaN
        missing = [k for k in month_keys if report["monthly"][0][k] is None]
        assert len(missing) >= 1                     # 80 个交易日覆盖不满全年
        assert report["trade_stats"]["trade_count"] == len(result.trades)
