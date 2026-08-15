"""因子表达式引擎、算子与分析的测试。"""

from typing import ClassVar

import numpy as np
import pandas as pd
import pytest

from quantmaster.factors import (
    BUILTIN_FACTORS,
    ExpressionFactor,
    analyze_factor,
    compute_factor,
    ops,
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

    def test_rejects_future_and_invalid_periods(self):
        with pytest.raises(ExpressionError, match="禁止未来数据"):
            ExpressionFactor("delay(close, -1)")
        with pytest.raises(ExpressionError, match="正整数"):
            ExpressionFactor("ts_mean(close, 0)")

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
        expected = panel["close"].pct_change(fill_method=None)
        pd.testing.assert_frame_equal(result, expected)

    def test_derived_vwap_and_returns(self, panel):
        result = ExpressionFactor("ts_mean(returns, 5) + rank(vwap)").compute(panel)
        assert result.shape == panel["close"].shape

    def test_derived_returns_preserve_missing_prices(self, panel):
        close = panel["close"].copy()
        close.iloc[10, 0] = float("nan")
        panel_with_gap = dict(panel)
        panel_with_gap["close"] = close

        result = ExpressionFactor("returns").compute(panel_with_gap)
        expected = close.pct_change(fill_method=None)

        pd.testing.assert_frame_equal(result, expected)
        assert pd.isna(result.iloc[10, 0])
        assert pd.isna(result.iloc[11, 0])

    def test_cross_sectional_ops_treat_infinities_as_missing(self, panel):
        values = panel["close"].iloc[:2].copy()
        values.iloc[0, 0] = np.inf
        values.iloc[1, 1] = -np.inf

        for result in (
            ops.rank(values), ops.zscore(values), ops.demean(values), ops.winsorize(values),
        ):
            assert pd.isna(result.iloc[0, 0])
            assert pd.isna(result.iloc[1, 1])


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


class TestCacheReplaceSemantics:
    def test_increment_calibrates_mixed_adjustment_bases(self, tmp_path, monkeypatch):
        """无语义旧缓存通过同源完整替换，不猜测重叠复权比例。"""
        import pandas as pd

        from quantmaster.data import registry
        from quantmaster.data.base import DataSource, Market
        from quantmaster.data.storage import BarStore

        store = BarStore(root=tmp_path / "bars")
        old_dates = pd.bdate_range("2024-01-02", "2024-06-28")
        # 旧缓存：旧复权基准，恒价 100
        old = pd.DataFrame({c: 100.0 for c in ["open", "high", "low", "close"]},
                           index=old_dates)
        old["volume"] = 1e6
        store.put("600000.SH", old)

        # 模拟除权后的新基准：全体历史价 ×0.8（qfq 语义）
        class FakeSource(DataSource):
            name = "fake"
            markets = (Market.CN,)
            calls: ClassVar[list[tuple[str, str]]] = []

            def daily(self, symbol, start, end):
                FakeSource.calls.append((start, end))
                dates = pd.bdate_range(start, "2024-12-31")
                df = pd.DataFrame({c: 80.0 for c in ["open", "high", "low", "close"]},
                                  index=dates)
                df["volume"] = 1e6
                return df

        monkeypatch.setattr(registry, "_factories", lambda: {Market.CN: [FakeSource]})
        # 让缓存「过期」以强制触网
        with store._conn() as conn:
            conn.execute("UPDATE bar_meta SET updated_at = updated_at - 999999")

        registry.refresh_history("600000.SH", "2024-07-01", "2024-12-31", store=store)
        assert FakeSource.calls[0][0] == "2024-01-02"
        # 价格列整体校准成新基准：不存在 100 与 80 的接缝跳空。
        cached = store.get("600000.SH")
        assert cached.loc["2024-02-01", "close"] == 80.0
        returns = cached["close"].pct_change(fill_method=None).dropna()
        assert float(returns.abs().max()) < 1e-9, "缓存中出现复权基准接缝跳变"

    def test_fresh_cache_must_cover_start(self, tmp_path, monkeypatch):
        """『新鲜但不覆盖 start』的缓存不得截断长区间请求。"""
        import pandas as pd

        from quantmaster.data import registry
        from quantmaster.data.base import DataSource, Market
        from quantmaster.data.storage import BarStore

        store = BarStore(root=tmp_path / "bars")
        short_dates = pd.bdate_range("2024-06-03", "2024-06-28")
        short = pd.DataFrame({c: 10.0 for c in ["open", "high", "low", "close"]},
                             index=short_dates)
        short["volume"] = 1e6
        store.put("600000.SH", short)   # 新鲜（刚写入）但只覆盖 6 月

        class FullSource(DataSource):
            name = "full"
            markets = (Market.CN,)

            def daily(self, symbol, start, end):
                dates = pd.bdate_range(start, end)
                df = pd.DataFrame({c: 10.0 for c in ["open", "high", "low", "close"]},
                                  index=dates)
                df["volume"] = 1e6
                return df

        monkeypatch.setattr(registry, "_factories", lambda: {Market.CN: [FullSource]})
        envelope = registry.refresh_history(
            "600000.SH", "2024-01-02", "2024-06-28", store=store,
        )
        df = envelope.data
        assert envelope.quality.status != "unavailable"
        assert str(df.index.min().date()) <= "2024-01-03", "长区间请求被新鲜短缓存截断"

    def test_partial_refetch_calibrates_prices_and_preserves_old_volume(self, tmp_path, monkeypatch):
        """部分 provider 响应不能与无语义旧缓存拼成正式序列。"""
        import pandas as pd

        from quantmaster.data import registry
        from quantmaster.data.base import DataSource, Market
        from quantmaster.data.storage import BarStore

        store = BarStore(root=tmp_path / "bars")
        old_dates = pd.bdate_range("2024-01-02", "2024-06-28")
        old = pd.DataFrame({c: 100.0 for c in ["open", "high", "low", "close"]},
                           index=old_dates)
        old["volume"] = 1e6
        store.put("600000.SH", old)

        class PartialSource(DataSource):
            name = "partial"
            markets = (Market.CN,)

            def daily(self, symbol, start, end):
                # 返回区间有用，但异常缺失了 1-3 月。
                dates = pd.bdate_range("2024-04-01", "2024-12-31")
                df = pd.DataFrame({c: 80.0 for c in ["open", "high", "low", "close"]},
                                  index=dates)
                df["volume"] = 2e6
                return df

        monkeypatch.setattr(registry, "_factories", lambda: {Market.CN: [PartialSource]})
        with store._conn() as conn:
            conn.execute("UPDATE bar_meta SET updated_at = updated_at - 999999")

        result = registry.refresh_history(
            "600000.SH", "2024-01-02", "2024-12-31", store=store)
        data = result.data
        cached = store.get("600000.SH")
        assert result.quality.status == "unavailable"
        assert str(data.index.min().date()) == "2024-01-02"
        assert str(data.index.max().date()) == "2024-06-28"
        assert cached.loc["2024-02-01", "close"] == 100.0
        assert cached.loc["2024-05-02", "close"] == 100.0
        assert cached.loc["2024-02-01", "volume"] == 1e6

    def test_sparse_response_is_rejected_when_no_fallback_succeeds(self, tmp_path, monkeypatch):
        """内部大面积缺行的响应不得污染本地数据库。"""
        import pandas as pd

        from quantmaster.data import registry
        from quantmaster.data.base import DataSource, Market
        from quantmaster.data.storage import BarStore

        store = BarStore(root=tmp_path / "bars")

        class SparseSource(DataSource):
            name = "sparse"
            markets = (Market.CN,)

            def daily(self, symbol, start, end):
                dates = pd.bdate_range(start, end)
                sparse = dates[::3].union(pd.DatetimeIndex([dates[-1]]))
                frame = pd.DataFrame({
                    c: 10.0 for c in ["open", "high", "low", "close", "volume"]
                }, index=sparse)
                return frame

        class BrokenFallback(DataSource):
            name = "broken"
            markets = (Market.CN,)

            def daily(self, symbol, start, end):
                raise ConnectionError("offline")

        monkeypatch.setattr(
            registry, "_factories", lambda: {Market.CN: [SparseSource, BrokenFallback]})
        with pytest.raises(RuntimeError, match="响应内部过于稀疏"):
            registry.refresh_history(
                "600000.SH", "2024-01-02", "2024-06-28", store=store)
        assert store.get("600000.SH") is None


class TestQuantilePeriods:
    def test_periods_rebalance_semantics(self, panel):
        """periods=3 表示每 3 天调仓：非调仓日沿用旧分组，逐日 1 日收益复利。"""
        from quantmaster.factors import BUILTIN_FACTORS, compute_factor
        from quantmaster.factors.analysis import quantile_backtest

        values = compute_factor(BUILTIN_FACTORS["rev_5d"], panel)
        daily3, nav3 = quantile_backtest(values, panel["close"], periods=3)
        assert nav3.shape[1] == 5
        assert nav3.notna().all().all()
        # 调仓更慢 -> 分组序列变化更少，但两种口径都必须是逐日 1 日收益
        # （旧实现把 3 日重叠收益 /3 逐日复利，会低估波动 ~sqrt(3) 倍）
        assert daily3.abs().max().max() < 0.25   # 单日收益量级合理

    def test_constant_factor_invariant_to_periods(self, panel):
        """因子恒定时分组不随时间变化，periods 取值不应影响净值。"""
        import pandas as pd

        from quantmaster.factors.analysis import quantile_backtest

        close = panel["close"]
        const = pd.DataFrame(
            {s: i for i, s in enumerate(close.columns)}, index=close.index, dtype=float)
        _, nav1 = quantile_backtest(const, close, periods=1)
        _, nav5 = quantile_backtest(const, close, periods=5)
        pd.testing.assert_frame_equal(nav1, nav5)
