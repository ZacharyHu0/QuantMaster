"""多因子合成与正交化（composite 模块）的测试。

核心验证点：
- factor_correlation 的对角线 / 反号因子的相关性；
- ic_weighted_combine 的权重确实防未来函数（shift 过），
  作弊因子权重远大于噪声、负 IC 因子拿到负权重；
- orthogonalize 的残差与 base 截面不相关；
- greedy_select 跳过与已选因子高度相关的冗余因子。
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from conftest import make_panel

from quantmaster.factors.analysis import forward_returns
from quantmaster.factors.composite import (
    factor_correlation,
    greedy_select,
    ic_weighted_combine,
    orthogonalize,
)


def _noise_like(close: pd.DataFrame, seed: int) -> pd.DataFrame:
    """与 close 同形状的独立高斯噪声面板（无预测能力的"假因子"）。"""
    rng = np.random.default_rng(seed)
    return pd.DataFrame(rng.normal(size=close.shape), index=close.index, columns=close.columns)


class TestFactorCorrelation:
    def test_diagonal_is_one(self, panel):
        close = panel["close"]
        values = {"a": _noise_like(close, 1), "b": _noise_like(close, 2)}
        corr = factor_correlation(values)
        assert list(corr.index) == ["a", "b"]
        assert np.allclose(np.diag(corr.to_numpy()), 1.0)

    def test_negation_is_minus_one(self, panel):
        """因子与其相反数：秩序完全反转，逐日截面秩相关恒为 -1。"""
        f = _noise_like(panel["close"], 3)
        corr = factor_correlation({"f": f, "neg": -f})
        assert corr.loc["f", "neg"] < -0.999
        assert corr.loc["f", "neg"] == corr.loc["neg", "f"]  # 对称


class TestIcWeightedCombine:
    def test_cheat_factor_dominates_noise(self, panel):
        """作弊因子（直接用未来收益）IC 恒为 1，应拿走绝大部分权重。"""
        close = panel["close"]
        cheat = forward_returns(close, 1)
        noise = _noise_like(close, 4)
        combined, weights = ic_weighted_combine(
            {"cheat": cheat, "noise": noise}, close, lookback=20, min_periods=10
        )
        assert combined.shape == close.shape
        valid = weights.dropna()
        # 归一化：每日权重绝对值之和为 1
        assert np.allclose(valid.abs().sum(axis=1), 1.0)
        assert valid["cheat"].abs().mean() > 0.7
        assert valid["cheat"].abs().mean() > 3 * valid["noise"].abs().mean()

    def test_negative_ic_gets_negative_weight(self, panel):
        """反向作弊因子 IC 恒为 -1，应自动获得负权重（等于自动反向）。"""
        close = panel["close"]
        anti = -forward_returns(close, 1)
        noise = _noise_like(close, 5)
        _, weights = ic_weighted_combine(
            {"anti": anti, "noise": noise}, close, lookback=20, min_periods=10
        )
        anti_w = weights["anti"].dropna()
        assert not anti_w.empty
        assert (anti_w < 0).all()

    def test_weights_cold_start_is_shifted(self, panel):
        """首个有效权重出现在位置 min_periods（不 shift 的话会早一天出现）。"""
        close = panel["close"]
        dates = close.index
        min_periods = 10
        cheat = forward_returns(close, 1)
        noise = _noise_like(close, 6)
        _, weights = ic_weighted_combine(
            {"cheat": cheat, "noise": noise}, close, lookback=30, min_periods=min_periods
        )
        # IC 序列从第 0 天起有值，滚动窗口在位置 min_periods-1 首次满足样本数，
        # 再经 shift(1) 后，首个有效权重落在位置 min_periods
        assert weights["cheat"].first_valid_index() == dates[min_periods]
        assert weights.loc[dates[min_periods - 1]].isna().all()

    def test_weight_only_depends_on_past_ic(self, panel):
        """篡改 T 日之后的行情，T 日及之前的权重必须一字不差——无未来函数。"""
        close = panel["close"]
        dates = close.index
        t = 80
        values = {"a": _noise_like(close, 7), "b": _noise_like(close, 8)}
        _, w1 = ic_weighted_combine(values, close, lookback=20, min_periods=10)

        rng = np.random.default_rng(9)
        close2 = close.copy()
        close2.iloc[t + 1 :] = close2.iloc[t + 1 :].to_numpy() * rng.uniform(
            0.8, 1.2, close2.iloc[t + 1 :].shape
        )
        _, w2 = ic_weighted_combine(values, close2, lookback=20, min_periods=10)

        # T 日权重最多用到 T-1 日的 IC（依赖 T 日收盘价），不受 T+1 起的篡改影响
        pd.testing.assert_frame_equal(w1.loc[: dates[t]], w2.loc[: dates[t]])
        # 而 T 日之后的权重确实被篡改影响了（证明扰动本身有效）
        diff = (w1.loc[dates[t + 1] :] - w2.loc[dates[t + 1] :]).abs()
        assert np.nanmax(diff.to_numpy()) > 1e-8

    def test_icir_method(self, panel):
        """icir 加权下，IC 恒为 1（标准差为 0）的作弊因子权重应趋近 1。"""
        close = panel["close"]
        cheat = forward_returns(close, 1)
        noise = _noise_like(close, 10)
        combined, weights = ic_weighted_combine(
            {"cheat": cheat, "noise": noise}, close, lookback=20, method="icir", min_periods=10
        )
        assert combined.shape == close.shape
        assert list(weights.columns) == ["cheat", "noise"]
        assert weights["cheat"].dropna().abs().mean() > 0.9

    def test_invalid_method_raises(self, panel):
        close = panel["close"]
        with pytest.raises(ValueError, match="method"):
            ic_weighted_combine({"x": _noise_like(close, 11)}, close, method="magic")


class TestOrthogonalize:
    def test_self_orthogonalization_is_zero(self, panel):
        """x 对自身回归：beta=1，残差应为（数值意义上的）0。"""
        x = _noise_like(panel["close"], 12)
        resid = orthogonalize(x, x)
        assert float(resid.abs().max().max()) < 1e-8

    def test_residual_uncorrelated_with_base(self, panel):
        """OLS 残差与自变量正交：逐日截面相关应为 0。"""
        base = _noise_like(panel["close"], 13)
        target = 0.7 * base + _noise_like(panel["close"], 14)
        resid = orthogonalize(target, base)
        assert resid.shape == base.shape
        daily_corr = resid.corrwith(base, axis=1)
        assert daily_corr.abs().max() < 1e-6


class TestGreedySelect:
    def _three_factors(self, close: pd.DataFrame) -> dict[str, pd.DataFrame]:
        """A 强因子；B ≈ 0.9*A + 噪声（与 A 高度相关）；C 独立弱因子。"""
        fwd = forward_returns(close, 1)
        a = fwd
        b = 0.9 * a + 0.01 * _noise_like(close, 15)
        c = _noise_like(close, 16)
        return {"A": a, "B": b, "C": c}

    def test_skips_correlated_factor(self):
        close = make_panel(days=150, n=30, seed=21)["close"]
        values = self._three_factors(close)
        # 前置校验：B 确实与 A 高度相关，C 与 A 基本不相关
        corr = factor_correlation(values)
        assert abs(corr.loc["A", "B"]) > 0.6
        assert abs(corr.loc["A", "C"]) < 0.3
        # |IC| 排序为 A > B > C；B 因与已选的 A 相关性超限被跳过
        assert greedy_select(values, close, max_corr=0.6, top_k=5) == ["A", "C"]

    def test_top_k_limits_selection(self):
        close = make_panel(days=150, n=30, seed=21)["close"]
        values = self._three_factors(close)
        assert greedy_select(values, close, max_corr=0.6, top_k=1) == ["A"]


class TestLastDayWeights:
    def test_combined_last_row_not_all_nan(self, panel):
        """最后一个交易日（实盘出信号那天）必须有合成值：权重来自 T-1 及更早的 IC。"""
        from quantmaster.factors import BUILTIN_FACTORS, compute_factors
        from quantmaster.factors.composite import ic_weighted_combine

        values = compute_factors(
            [BUILTIN_FACTORS["mom_20d"], BUILTIN_FACTORS["rev_5d"]], panel)
        combined, weights = ic_weighted_combine(values, panel["close"], lookback=20)
        assert combined.iloc[-1].notna().any(), "末日合成因子全 NaN——实盘拿不到信号"
        assert weights.iloc[-1].notna().any(), "末日权重缺失"

    def test_partial_coverage_renormalized(self, panel):
        """某股票缺一个因子值时，按可得因子的 |权重| 重归一，而不是当 0。"""
        import numpy as np

        from quantmaster.factors import BUILTIN_FACTORS, compute_factors
        from quantmaster.factors.composite import ic_weighted_combine

        values = compute_factors(
            [BUILTIN_FACTORS["mom_20d"], BUILTIN_FACTORS["rev_5d"]], panel)
        sym = panel["close"].columns[0]
        a, b = list(values)
        _full, _ = ic_weighted_combine(values, panel["close"], lookback=20)
        # 人为挖掉一个因子在末段的值
        values[a].iloc[-30:, values[a].columns.get_loc(sym)] = np.nan
        partial, weights = ic_weighted_combine(values, panel["close"], lookback=20)
        last = partial.index[-1]
        wb = weights.loc[last, b]
        if not np.isnan(wb) and abs(wb) > 1e-12:
            expected = values[b].loc[last, sym] * np.sign(wb)
            assert partial.loc[last, sym] == pytest.approx(expected, rel=1e-6)
