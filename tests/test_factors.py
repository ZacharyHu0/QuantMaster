"""因子表达式引擎、算子与分析的测试。"""

import pandas as pd
import pytest

from quantmaster.factors import (
    BUILTIN_FACTORS,
    ExpressionFactor,
    analyze_factor,
    compute_factor,
)
from quantmaster.factors.analysis import forward_returns, information_coefficient
from quantmaster.factors.base import ExpressionError
from quantmaster.factors.engine import combine_factors, compute_factors


class TestExpressionSafety:
    def test_valid_expression(self):
        ExpressionFactor("rank(-delta(close, 5))")

    def test_rejects_import(self):
        with pytest.raises(ExpressionError):
            ExpressionFactor("__import__('os').system('id')")

    def test_rejects_attribute_access(self):
        with pytest.raises(ExpressionError):
            ExpressionFactor("close.values")

    def test_rejects_unknown_function(self):
        with pytest.raises(ExpressionError):
            ExpressionFactor("eval(close)")

    def test_rejects_unknown_field(self):
        with pytest.raises(ExpressionError):
            ExpressionFactor("rank(pe_ratio)")

    def test_rejects_wrong_arity(self):
        with pytest.raises(ExpressionError):
            ExpressionFactor("delta(close)")

    def test_rejects_string_constant(self):
        with pytest.raises(ExpressionError):
            ExpressionFactor("delay(close, 'a')")

    def test_rejects_comparison(self):
        with pytest.raises(ExpressionError):
            ExpressionFactor("close > 10")


class TestOps:
    def test_delta(self, panel):
        result = ExpressionFactor("delta(close, 5)").compute(panel)
        close = panel["close"]
        expected = close.iloc[10] - close.iloc[5]
        pd.testing.assert_series_equal(result.iloc[10], expected, check_names=False)

    def test_rank_range(self, panel):
        result = ExpressionFactor("rank(close)").compute(panel)
        assert result.max().max() <= 1.0
        assert result.min().min() > 0.0

    def test_arithmetic(self, panel):
        result = ExpressionFactor("close / delay(close, 1) - 1").compute(panel)
        expected = panel["close"].pct_change()
        pd.testing.assert_frame_equal(result, expected)

    def test_derived_vwap_and_returns(self, panel):
        result = ExpressionFactor("ts_mean(returns, 5) + rank(vwap)").compute(panel)
        assert result.shape == panel["close"].shape


class TestBuiltinFactors:
    def test_all_builtin_compute(self, panel):
        values = compute_factors(list(BUILTIN_FACTORS.values()), panel)
        assert len(values) == len(BUILTIN_FACTORS)
        for name, df in values.items():
            assert df.shape == panel["close"].shape, name
            # 标准化后截面均值应接近 0
            tail_mean = df.iloc[-20:].mean(axis=1).abs().mean()
            assert tail_mean < 0.2, name

    def test_combine_factors(self, panel):
        values = compute_factors(
            [BUILTIN_FACTORS["mom_20d"], BUILTIN_FACTORS["rev_5d"]], panel
        )
        combined = combine_factors(values, {"mom_20d": 0.6, "rev_5d": 0.4})
        assert combined.shape == panel["close"].shape


class TestAnalysis:
    def test_perfect_factor_has_ic_one(self, panel):
        """把「未来收益」当因子，RankIC 应为 1（验证 IC 计算本身正确）。"""
        close = panel["close"]
        cheat = forward_returns(close, 1)
        ic = information_coefficient(cheat, forward_returns(close, 1))
        assert ic.mean() > 0.999

    def test_analyze_factor_report(self, panel):
        values = compute_factor(BUILTIN_FACTORS["rev_5d"], panel)
        report = analyze_factor(values, panel["close"], name="rev_5d")
        summary = report.summary()
        assert set(summary) >= {"ic_mean", "icir", "monotonicity", "quantile_annual"}
        assert len(report.quantile_returns.columns) == 5
        assert -1 <= summary["ic_mean"] <= 1

    def test_quantile_navs_start_at_one(self, panel):
        values = compute_factor(BUILTIN_FACTORS["mom_20d"], panel)
        report = analyze_factor(values, panel["close"])
        first = report.quantile_returns.iloc[0]
        assert ((first - 1).abs() < 0.1).all()
