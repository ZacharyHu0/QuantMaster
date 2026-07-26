"""绩效指标测试。"""

import numpy as np
import pandas as pd
import pytest

from quantmaster.backtest.metrics import TRADING_DAYS, max_drawdown, performance_metrics
from quantmaster.factors.analysis import annualize


class TestMaxDrawdown:
    def test_known_case(self):
        nav = pd.Series([1.0, 1.2, 0.9, 1.1, 1.3],
                        index=pd.bdate_range("2023-01-02", periods=5))
        mdd, peak, trough = max_drawdown(nav)
        assert mdd == pytest.approx(0.25)   # 1.2 -> 0.9
        assert peak < trough

    def test_monotonic_up_has_zero_drawdown(self):
        nav = pd.Series(np.linspace(1, 2, 50),
                        index=pd.bdate_range("2023-01-02", periods=50))
        mdd, _, _ = max_drawdown(nav)
        assert mdd == 0.0


class TestPerformance:
    def test_constant_positive_returns(self):
        r = pd.Series([0.001] * TRADING_DAYS,
                      index=pd.bdate_range("2023-01-02", periods=TRADING_DAYS))
        m = performance_metrics(r)
        # metrics 输出保留 4 位小数
        assert m["annual_return"] == pytest.approx((1.001) ** TRADING_DAYS - 1, abs=1e-4)
        assert m["max_drawdown"] == 0.0
        assert m["daily_win_rate"] == 1.0

    def test_benchmark_excess(self):
        idx = pd.bdate_range("2023-01-02", periods=100)
        r = pd.Series(0.002, index=idx)
        bench_nav = pd.Series((1.001) ** np.arange(100), index=idx)
        m = performance_metrics(r, benchmark_nav=bench_nav)
        assert m["excess_annual_return"] > 0
        assert "information_ratio" in m

    def test_annualize_total_loss_capped(self):
        r = pd.Series([-0.5, -0.9], index=pd.bdate_range("2023-01-02", periods=2))
        assert annualize(r) >= -1.0
