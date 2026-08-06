"""样本外验证工具（train/test IC、walk-forward、网格搜索）的测试。"""

import pytest

from quantmaster.backtest.validation import grid_search, train_test_ic, walk_forward_ic
from quantmaster.factors.analysis import forward_returns
from quantmaster.factors.base import FuncFactor

SPLIT = "2023-04-15"    # make_panel(150) 覆盖 2023-01 ~ 2023-07，split 大致居中


def cheat_factor() -> FuncFactor:
    """作弊因子：直接拿未来收益当因子值（参照 test_factors 的做法）。

    它在任何时段 RankIC 都应接近 1——用来验证验证工具本身算得对，
    真实研究中当然绝不允许这么构造因子（未来函数）。
    """
    return FuncFactor("cheat", lambda p: forward_returns(p["close"], 1))


class TestTrainTestIC:
    def test_cheat_factor_is_robust(self, panel):
        out = train_test_ic(cheat_factor(), panel, split=SPLIT)
        assert out["is_ic"] > 0.99
        assert out["oos_ic"] > 0.99
        assert out["degradation"] == pytest.approx(0.0, abs=0.01)
        assert out["verdict"] == "稳健"
        assert out["is_days"] > 0 and out["oos_days"] > 0

class TestWalkForwardIC:
    def test_cheat_factor_stable_across_segments(self, panel):
        table = walk_forward_ic(cheat_factor(), panel, n_splits=4)
        assert (table["ic_mean"] > 0.99).all()
        assert (table["days"] > 0).all()


class TestGridSearch:
    def test_small_grid_complete_and_sorted(self, panel):
        df = grid_search(panel, ["mom_20d", "rev_5d"], top_ns=[2, 3], rebalances=["W"])
        assert len(df) == 4                          # 2 因子 × 2 top_n × 1 调仓
        combos = set(zip(df["factor"], df["top_n"], df["rebalance"], strict=True))
        assert combos == {("mom_20d", 2, "W"), ("mom_20d", 3, "W"),
                          ("rev_5d", 2, "W"), ("rev_5d", 3, "W")}
        sharpe = df["sharpe"].dropna()
        assert list(sharpe) == sorted(sharpe, reverse=True)   # 按 metric 降序
        assert {"annual_return", "sharpe", "max_drawdown", "calmar"} <= set(df.columns)
