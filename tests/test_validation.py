"""样本外验证工具（train/test IC、walk-forward、网格搜索）的测试。"""

import numpy as np
import pandas as pd
import pytest
from conftest import make_panel

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


def noise_factor(seed: int = 42) -> FuncFactor:
    """纯噪声因子：与收益完全独立的随机数，IC 应接近 0。"""
    def compute(p):
        close = p["close"]
        rng = np.random.default_rng(seed)
        return pd.DataFrame(rng.normal(size=close.shape),
                            index=close.index, columns=close.columns)
    return FuncFactor("noise", compute)


class TestTrainTestIC:
    def test_cheat_factor_is_robust(self, panel):
        out = train_test_ic(cheat_factor(), panel, split=SPLIT)
        assert out["is_ic"] > 0.99
        assert out["oos_ic"] > 0.99
        assert out["degradation"] == pytest.approx(0.0, abs=0.01)
        assert out["verdict"] == "稳健"
        assert out["is_days"] > 0 and out["oos_days"] > 0

    def test_noise_factor_has_low_ic(self, panel):
        out = train_test_ic(noise_factor(), panel, split=SPLIT)
        assert abs(out["is_ic"]) < 0.1
        assert abs(out["oos_ic"]) < 0.1

    def test_sign_flip_is_invalid(self, panel):
        """样本内有效、样本外符号反转的因子必须判「失效」。"""
        split_ts = pd.Timestamp(SPLIT)

        def compute(p):
            fwd = forward_returns(p["close"], 1)
            flipped = fwd.copy()
            flipped.loc[flipped.index >= split_ts] *= -1.0
            return flipped

        out = train_test_ic(FuncFactor("flip", compute), panel, split=SPLIT)
        assert out["is_ic"] > 0.9
        assert out["oos_ic"] < -0.9
        assert out["verdict"] == "失效"

    def test_bad_split_raises(self, panel):
        with pytest.raises(ValueError):
            train_test_ic(cheat_factor(), panel, split="2030-01-01")


class TestWalkForwardIC:
    def test_row_count_equals_n_splits(self, panel):
        for n in (3, 4):
            table = walk_forward_ic(cheat_factor(), panel, n_splits=n)
            assert len(table) == n
            assert {"start", "end", "days", "ic_mean", "icir"} <= set(table.columns)

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

    def test_failed_combo_recorded_as_nan(self):
        panel = make_panel(days=60, seed=5)
        # "not_a_factor(" 既非内置名也非法表达式 -> 该组合应记 NaN 而非抛异常
        df = grid_search(panel, ["mom_20d", "not_a_factor("], top_ns=[2], rebalances=["W"])
        assert len(df) == 2
        bad = df[df["factor"] == "not_a_factor("]
        assert bad["sharpe"].isna().all()
        assert df[df["factor"] == "mom_20d"]["sharpe"].notna().all()

    def test_unknown_metric_raises(self, panel):
        with pytest.raises(ValueError):
            grid_search(panel, ["mom_20d"], top_ns=[2], rebalances=["W"], metric="alpha")
