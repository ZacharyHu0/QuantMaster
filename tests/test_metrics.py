"""绩效指标测试。"""

import pandas as pd
import pytest

from quantmaster.backtest.metrics import TRADING_DAYS, max_drawdown, performance_metrics


class TestMaxDrawdown:
    def test_known_case(self):
        nav = pd.Series([1.0, 1.2, 0.9, 1.1, 1.3],
                        index=pd.bdate_range("2023-01-02", periods=5))
        mdd, peak, trough = max_drawdown(nav)
        assert mdd == pytest.approx(0.25)   # 1.2 -> 0.9
        assert peak < trough

class TestPerformance:
    def test_constant_positive_returns(self):
        r = pd.Series([0.001] * TRADING_DAYS,
                      index=pd.bdate_range("2023-01-02", periods=TRADING_DAYS))
        m = performance_metrics(r)
        # metrics 输出保留 4 位小数
        assert m["annual_return"] == pytest.approx((1.001) ** TRADING_DAYS - 1, abs=1e-4)
        assert m["max_drawdown"] == 0.0
        assert m["daily_win_rate"] == 1.0
