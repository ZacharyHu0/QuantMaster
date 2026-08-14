"""基本面数据层与价值/质量因子的测试（全部离线，合成数据，不触网）。"""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd
import pytest

from quantmaster.data import fundamentals
from quantmaster.data.fundamentals import (
    extract_roe,
    fundamental_panel,
    fundamental_store,
    quarterly_to_daily,
)
from quantmaster.factors.analysis import analyze_factor
from quantmaster.factors.engine import compute_factor
from quantmaster.factors.fundamental import make_fundamental_factors, resolve_factor
from quantmaster.temporal import TemporalContractError

DATES = pd.bdate_range("2023-01-02", "2023-12-29")


def make_quarterly() -> pd.DataFrame:
    """三个报告期的合成 ROE：Q1=10, Q2=12, Q3=8。"""
    idx = pd.to_datetime(["2023-03-31", "2023-06-30", "2023-09-30"])
    return pd.DataFrame({"roe": [10.0, 12.0, 8.0]}, index=idx)


def test_current_akshare_valuation_api_is_normalized(monkeypatch):
    """AKShare 1.18.81 的分指标估值接口应拼成稳定字段并统一市值单位。"""
    calls = []
    values = {
        "市盈率(静)": [18.0, 19.0],
        "市盈率(TTM)": [17.0, 18.0],
        "市净率": [1.8, 1.9],
        "总市值": [100.0, 101.0],
    }

    class CurrentAkshare:
        @staticmethod
        def stock_zh_valuation_baidu(**params):
            calls.append(params)
            return pd.DataFrame({
                "date": ["2026-07-29", "2026-07-30"],
                "value": values[params["indicator"]],
            })

    monkeypatch.setattr(fundamentals, "_require_akshare", lambda: CurrentAkshare())
    monkeypatch.setattr(
        "quantmaster.data.tushare_source.TushareSource.cached_daily_indicators",
        lambda self, symbol, start=None, end=None: None,
    )

    result = fundamentals.fetch_daily_indicators(
        "600000.SH", start="2026-07-29", end="2026-07-30",
    )

    assert list(result.columns) == list(fundamentals.DAILY_FIELDS)
    assert result.loc["2026-07-30", "pe"] == 19.0
    assert result.loc["2026-07-30", "pe_ttm"] == 18.0
    assert result.loc["2026-07-30", "pb"] == 1.9
    assert result.loc["2026-07-30", "total_mv"] == 1_010_000.0
    assert result["dv_ratio"].isna().all()
    assert {item["indicator"] for item in calls} == set(values)
    assert {item["period"] for item in calls} == {"近一年"}


def make_fund_panel(close: pd.DataFrame, seed: int = 3) -> dict[str, pd.DataFrame]:
    """构造与行情面板同形状的合成基本面面板。

    total_mv 按列递增（第 0 列市值最小），方便测小市值因子的方向。
    """
    rng = np.random.default_rng(seed)
    shape = close.shape

    def df(arr) -> pd.DataFrame:
        return pd.DataFrame(arr, index=close.index, columns=close.columns)

    base_mv = np.linspace(20e8, 2000e8, shape[1])
    return {
        "pe_ttm": df(rng.uniform(5.0, 60.0, shape)),
        "pb": df(rng.uniform(0.5, 8.0, shape)),
        "dv_ratio": df(rng.uniform(0.0, 6.0, shape)),
        "total_mv": df(np.tile(base_mv, (shape[0], 1)) * (1 + rng.normal(0, 0.01, shape))),
        "roe": df(rng.uniform(-5.0, 30.0, shape)),
    }


class TestQuarterlyToDaily:
    def test_report_date_not_visible(self):
        """报告期当天财报尚未公布：只能看到上一期的值（防未来函数）。"""
        daily = quarterly_to_daily(make_quarterly(), DATES, lag_days=45)
        # Q1 报告期当天（3-31）：Q1 要到 5-15 才可见，此前一无所有
        assert pd.isna(daily.loc["2023-03-31", "roe"])
        # Q2 报告期当天（6-30）：可见的仍是 Q1 的 10，而不是 Q2 的 12
        assert daily.loc["2023-06-30", "roe"] == 10.0

    def test_visible_only_after_lag(self):
        """日期级可见证据不能冒充 00:00，最早从下一 session 使用。"""
        daily = quarterly_to_daily(make_quarterly(), DATES, lag_days=45)
        assert pd.isna(daily.loc["2023-05-15", "roe"])
        assert daily.loc["2023-05-16", "roe"] == 10.0

    def test_ffill_between_publications(self):
        """两次披露之间 ffill 保持旧值，披露日切换为新值。"""
        daily = quarterly_to_daily(make_quarterly(), DATES, lag_days=45)
        assert daily.loc["2023-07-03", "roe"] == 10.0          # Q2 尚未披露
        assert daily.loc["2023-08-14", "roe"] == 10.0          # 日期证据尚未跨 session
        assert daily.loc["2023-08-15", "roe"] == 12.0
        assert daily.loc["2023-11-13", "roe"] == 12.0          # Q3 尚未披露
        assert daily.loc["2023-11-14", "roe"] == 12.0
        assert daily.loc["2023-11-15", "roe"] == 8.0

    def test_weekend_publication_not_lost(self):
        """可见日落在周末（3-31 + 43 = 周六 5-13）时，下个交易日起生效而非丢失。"""
        daily = quarterly_to_daily(make_quarterly(), DATES, lag_days=43)
        assert pd.isna(daily.loc["2023-05-12", "roe"])
        assert daily.loc["2023-05-15", "roe"] == 10.0

    def test_zero_lag_still_waits_until_next_session(self):
        """即使研究显式设零滞后，date-only 报告期也不能冒充当日 00:00。"""
        daily = quarterly_to_daily(make_quarterly(), DATES, lag_days=0)
        assert pd.isna(daily.loc["2023-03-31", "roe"])
        assert daily.loc["2023-04-03", "roe"] == 10.0

    def test_ann_date_uses_next_real_session_and_keeps_report_period_separate(self):
        """ann_date 只控制披露可见性，report_date 只说明报告期。"""
        quarterly = pd.DataFrame(
            {
                "report_date": pd.to_datetime(["2022-12-31"]),
                "roe": [9.5],
                "update_flag": ["0"],
            },
            index=pd.DatetimeIndex(["2023-04-28"], name="ann_date"),
        )
        # 调用方的真实 session 索引跳过周末和五一休市；实现不能用自然日/BDay
        # 猜出 5 月 1 日或把 4 月 28 日当成午夜已知。
        sessions = pd.DatetimeIndex(["2023-04-28", "2023-05-04", "2023-05-05"])
        daily = quarterly_to_daily(quarterly, sessions)

        assert pd.isna(daily.loc["2023-04-28", "roe"])
        assert daily.loc["2023-05-04", "roe"] == 9.5

    def test_precise_published_at_uses_aware_cutoff(self):
        quarterly = pd.DataFrame(
            {
                "published_at": ["2023-04-28T10:00:00+08:00"],
                "report_period": pd.to_datetime(["2022-12-31"]),
                "roe": [9.5],
            }
        )
        cutoffs = pd.DatetimeIndex([
            "2023-04-28T09:00:00+08:00",
            "2023-04-28T11:00:00+08:00",
        ])
        daily = quarterly_to_daily(quarterly, cutoffs)

        assert pd.isna(daily.iloc[0]["roe"])
        assert daily.iloc[1]["roe"] == 9.5

    def test_precise_published_at_rejects_date_only_cutoff(self):
        quarterly = pd.DataFrame({
            "published_at": ["2023-04-28T10:00:00+08:00"],
            "roe": [9.5],
        })
        with pytest.raises(TemporalContractError, match="cutoff"):
            quarterly_to_daily(quarterly, pd.DatetimeIndex(["2023-04-28"]))

    def test_precise_published_at_rejects_missing_timezone(self):
        quarterly = pd.DataFrame({
            "published_at": ["2023-04-28T10:00:00"],
            "roe": [9.5],
        })
        cutoffs = pd.DatetimeIndex(["2023-04-28T11:00:00+08:00"])
        with pytest.raises(TemporalContractError, match="时区"):
            quarterly_to_daily(quarterly, cutoffs)

    def test_empty_input(self):
        """空季度数据返回同形状的全 NaN 面板，不报错。"""
        empty = pd.DataFrame(columns=["roe"])
        daily = quarterly_to_daily(empty, DATES)
        assert list(daily.columns) == ["roe"]
        assert len(daily) == len(DATES)
        assert daily["roe"].isna().all()


class TestExtractRoe:
    def test_extract_from_chinese_table(self):
        """从中文财务指标表（日期列 + 净资产收益率(%)列）中标准化抽取。"""
        raw = pd.DataFrame(
            {
                "日期": ["2023-06-30", "2023-03-31"],
                "净资产收益率(%)": ["12.5", "10.0"],
                "总资产周转率(次)": [0.3, 0.2],
            }
        )
        out = extract_roe(raw)
        assert list(out.columns) == ["roe"]
        assert isinstance(out.index, pd.DatetimeIndex)
        assert out.index.is_monotonic_increasing
        assert out.loc["2023-03-31", "roe"] == 10.0
        assert out.loc["2023-06-30", "roe"] == 12.5

    def test_extract_missing_column_raises(self):
        raw = pd.DataFrame({"日期": ["2023-03-31"], "毛利率(%)": [30.0]})
        with pytest.raises(ValueError):
            extract_roe(raw)


class TestFundamentalFactors:
    def test_factor_shapes_align_with_quote_panel(self, panel):
        """各因子输出与行情面板 close 完全同形状、同索引。"""
        factors = make_fundamental_factors(make_fund_panel(panel["close"]))
        assert set(factors) == {"ep", "bp", "dividend_yield", "small_cap", "roe"}
        close = panel["close"]
        for name, factor in factors.items():
            values = factor.compute(panel)
            assert values.shape == close.shape, name
            assert values.index.equals(close.index), name
            assert values.columns.equals(close.columns), name

    def test_ep_negative_pe_is_nan(self, panel):
        """PE<=0（亏损）处 EP 为 NaN，正常处等于 1/PE。"""
        fund = make_fund_panel(panel["close"])
        fund["pe_ttm"].iloc[0, 0] = -8.0
        fund["pe_ttm"].iloc[1, 1] = 0.0
        ep = make_fundamental_factors(fund)["ep"].compute(panel)
        assert pd.isna(ep.iloc[0, 0])
        assert pd.isna(ep.iloc[1, 1])
        assert ep.iloc[2, 2] == pytest.approx(1.0 / fund["pe_ttm"].iloc[2, 2])

    def test_bp_nonpositive_pb_is_nan(self, panel):
        fund = make_fund_panel(panel["close"])
        fund["pb"].iloc[0, 0] = -1.5
        bp = make_fundamental_factors(fund)["bp"].compute(panel)
        assert pd.isna(bp.iloc[0, 0])
        assert bp.iloc[0, 1] == pytest.approx(1.0 / fund["pb"].iloc[0, 1])

    def test_small_cap_direction(self, panel):
        """市值越小因子值越大：第 0 列（最小市值）恒大于最后一列（最大市值）。"""
        sc = make_fundamental_factors(make_fund_panel(panel["close"]))["small_cap"].compute(panel)
        assert (sc.iloc[:, 0] > sc.iloc[:, -1]).all()

    def test_missing_field_skips_factor(self, panel):
        """fund_panel 缺哪个字段就不产出对应因子。"""
        fund = make_fund_panel(panel["close"])
        factors = make_fundamental_factors({"pe_ttm": fund["pe_ttm"], "pb": fund["pb"]})
        assert set(factors) == {"ep", "bp"}

    def test_reindex_fills_missing_symbol_with_nan(self, panel):
        """基本面数据缺失的股票在因子输出里为 NaN，但形状仍与行情面板一致。"""
        fund = make_fund_panel(panel["close"])
        partial_pe = fund["pe_ttm"].iloc[:, :-1]  # 去掉最后一只股票
        ep = make_fundamental_factors({"pe_ttm": partial_pe})["ep"].compute(panel)
        assert ep.shape == panel["close"].shape
        assert ep.iloc[:, -1].isna().all()
        assert ep.iloc[:, 0].notna().all()

    def test_compute_factor_pipeline(self, panel):
        """与 compute_factor 标准化流水线（缩尾 + 截面标准分）兼容。"""
        factors = make_fundamental_factors(make_fund_panel(panel["close"]))
        values = compute_factor(factors["ep"], panel)
        assert values.shape == panel["close"].shape
        # 截面标准分后每日均值应接近 0
        assert values.mean(axis=1).abs().max() < 1e-6

    def test_analyze_factor_full_chain(self, panel):
        """全链路：基本面面板 -> 因子 -> 标准化 -> IC / 分层回测报告。"""
        factors = make_fundamental_factors(make_fund_panel(panel["close"]))
        values = compute_factor(factors["small_cap"], panel)
        report = analyze_factor(values, panel["close"], name="small_cap")
        summary = report.summary()
        assert set(summary) >= {"ic_mean", "icir", "monotonicity", "quantile_annual"}
        assert len(report.quantile_returns.columns) == 5
        assert -1 <= summary["ic_mean"] <= 1


class TestFundamentalPanel:
    START, END = "2023-01-02", "2023-06-30"
    SYMBOLS = ("600000.SH", "000001.SZ")

    def _seed_cache(self, symbols) -> None:
        """把合成的每日指标与季度 ROE 写入本地缓存（isolated_config 隔离目录）。"""
        store = fundamental_store()
        dates = pd.bdate_range(self.START, self.END)
        for i, s in enumerate(symbols):
            indicators = pd.DataFrame(
                {
                    "pe": 15.0 + i, "pe_ttm": 10.0 + i, "pb": 2.0 + i,
                    "dv_ratio": 3.0 + i, "total_mv": (100.0 + i) * 1e8,
                },
                index=dates,
            )
            store.put(s, indicators)
            quarterly = pd.DataFrame(
                {"roe": [10.0 + i, 12.0 + i]},
                index=pd.to_datetime(["2022-12-31", "2023-03-31"]),
            )
            store.put(fundamentals._roe_key(s), quarterly)

    def _forbid_network(self, monkeypatch) -> list:
        calls: list = []

        def spy(*args, **kwargs):
            calls.append(args)
            raise RuntimeError("离线测试：禁止触网")

        monkeypatch.setattr(fundamentals, "fetch_daily_indicators", spy)
        monkeypatch.setattr(fundamentals, "fetch_quarterly_roe", spy)
        return calls

    def test_panel_from_cache_offline(self, monkeypatch):
        """缓存命中时完全不触网，输出 {字段: DataFrame(date × symbol)}。"""
        symbols = list(self.SYMBOLS)
        self._seed_cache(symbols)
        calls = self._forbid_network(monkeypatch)

        result = fundamental_panel(symbols, self.START, self.END)
        assert not calls, "缓存命中时不应触网"
        assert set(result) == {"pe_ttm", "pb", "dv_ratio", "total_mv", "roe"}
        for field, df in result.items():
            assert list(df.columns) == symbols, field
            assert isinstance(df.index, pd.DatetimeIndex), field
        assert result["pe_ttm"].loc["2023-03-01", "600000.SH"] == 10.0
        assert result["pe_ttm"].loc["2023-03-01", "000001.SZ"] == 11.0

    def test_panel_roe_respects_publication_lag(self, monkeypatch):
        """面板中的 ROE 按披露截止日滞后：年报 +120 天、一季报 +31 天。"""
        symbols = list(self.SYMBOLS)
        self._seed_cache(symbols)
        self._forbid_network(monkeypatch)

        roe = fundamental_panel(symbols, self.START, self.END)["roe"]
        # 2022 年报（12-31）+120 天 = 2023-04-30 可见；此前 NaN
        assert pd.isna(roe.loc["2023-02-14", "600000.SH"])
        assert pd.isna(roe.loc["2023-04-28", "600000.SH"])
        # 2023 一季报（3-31）+31 天 = 5-01 可见——注意一季报比上年年报先「可见」，
        # 这正是 A 股的真实披露节奏（年报最晚 4-30，一季报也是 4 月底前后）
        first_visible = roe["600000.SH"].first_valid_index()
        assert str(first_visible.date()) >= "2023-05-01"
        assert roe.loc["2023-05-04", "600000.SH"] == 12.0

    def test_panel_skips_failed_symbol(self, monkeypatch, caplog):
        """无缓存且获取失败的标的被跳过并告警，其余标的正常返回。"""
        self._seed_cache(["600000.SH"])
        self._forbid_network(monkeypatch)

        with caplog.at_level(logging.WARNING, logger="quantmaster.data.fundamentals"):
            result = fundamental_panel(["600000.SH", "000002.SZ"], self.START, self.END)
        assert list(result["pe_ttm"].columns) == ["600000.SH"]
        assert list(result["roe"].columns) == ["600000.SH"]
        assert "000002.SZ" in caplog.text

    def test_panel_unknown_field_ignored(self, monkeypatch):
        """未知字段被忽略（告警），不影响其他字段。"""
        self._seed_cache(["600000.SH"])
        self._forbid_network(monkeypatch)

        result = fundamental_panel(["600000.SH"], self.START, self.END, fields=["pe_ttm", "nonexist"])
        assert set(result) == {"pe_ttm"}


class TestResolveFactor:
    """resolve_factor 统一入口：表达式/内置/基本面三类都能解析（离线）。"""

    START, END = "2023-01-02", "2023-06-30"

    def _seed(self):
        store = fundamental_store()
        dates = pd.bdate_range(self.START, self.END)
        indicators = pd.DataFrame(
            {"pe": 15.0, "pe_ttm": 10.0, "pb": 2.0, "dv_ratio": 3.0, "total_mv": 1e10},
            index=dates)
        store.put("600000.SH", indicators)

    def test_resolves_builtin_and_expression(self):
        from quantmaster.factors.fundamental import resolve_factor

        f1 = resolve_factor("mom_20d", ["600000.SH"], self.START, self.END)
        assert f1.name == "mom_20d"
        f2 = resolve_factor("rank(-delta(close, 5))", ["600000.SH"], self.START, self.END)
        assert "delta" in f2.name

    def test_resolves_fundamental_from_cache(self, monkeypatch):
        from quantmaster.data import fundamentals as mod
        from quantmaster.factors.fundamental import resolve_factor

        self._seed()
        monkeypatch.setattr(mod, "fetch_daily_indicators",
                            lambda *a, **k: (_ for _ in ()).throw(AssertionError("不应触网")))
        monkeypatch.setattr(mod, "fetch_quarterly_roe",
                            lambda *a, **k: (_ for _ in ()).throw(AssertionError("不应触网")))
        factor = resolve_factor("ep", ["600000.SH"], self.START, self.END)
        dates = pd.bdate_range(self.START, self.END)
        close = pd.DataFrame({"600000.SH": 10.0}, index=dates)
        values = factor.compute({"close": close})
        # ep = 1 / pe_ttm = 0.1
        assert values["600000.SH"].dropna().iloc[-1] == pytest.approx(0.1)

    def test_daily_factors_share_cache_without_requesting_roe(self, monkeypatch):
        """日频基本面因子只拉一次每日指标，且绝不能顺带请求季度 ROE。"""
        dates = pd.bdate_range(self.START, self.END)
        calls = {"daily": 0, "roe": 0}

        def daily(symbol, start=None, end=None):
            calls["daily"] += 1
            return pd.DataFrame({
                "pe": 15.0,
                "pe_ttm": 10.0,
                "pb": 2.0,
                "dv_ratio": 3.0,
                "total_mv": 1e10,
            }, index=dates)

        def roe(*args, **kwargs):
            calls["roe"] += 1
            raise AssertionError("股息率/EP 不应请求季度 ROE")

        monkeypatch.setattr(fundamentals, "fetch_daily_indicators", daily)
        monkeypatch.setattr(fundamentals, "fetch_quarterly_roe", roe)

        resolve_factor("dividend_yield", ["600000.SH"], self.START, self.END)
        resolve_factor("ep", ["600000.SH"], self.START, self.END)

        assert calls == {"daily": 1, "roe": 0}

    def test_covered_parquet_without_metadata_still_stays_offline(self, monkeypatch):
        """旧缓存元信息即使遗失，覆盖目标日期的 parquet 仍优先于 API。"""
        self._seed()
        store = fundamental_store()
        with store._conn() as conn:
            conn.execute("DELETE FROM bar_meta WHERE symbol=?", ("600000.SH",))
        calls = []

        def forbidden(*args, **kwargs):
            calls.append(args)
            raise AssertionError("已覆盖的本地 parquet 不应重新触网")

        monkeypatch.setattr(fundamentals, "fetch_daily_indicators", forbidden)
        result = fundamental_panel(
            ["600000.SH"], self.START, self.END,
            fields=["dv_ratio"], store=store,
        )

        assert not calls
        assert result["dv_ratio"].iloc[-1, 0] == pytest.approx(3.0)

    def test_parallel_factors_coalesce_the_same_api_request(self, monkeypatch):
        """并行任务对同一标的的首次加载只允许一个请求进入提供商。"""
        dates = pd.bdate_range(self.START, self.END)
        entered = threading.Event()
        release = threading.Event()
        calls = 0

        def daily(symbol, start=None, end=None):
            nonlocal calls
            calls += 1
            entered.set()
            assert release.wait(timeout=3)
            return pd.DataFrame({
                "pe_ttm": 10.0, "pb": 2.0, "dv_ratio": 3.0, "total_mv": 1e10,
            }, index=dates)

        monkeypatch.setattr(fundamentals, "fetch_daily_indicators", daily)
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(
                    resolve_factor, name, ["600000.SH"], self.START, self.END,
                )
                for name in ("dividend_yield", "ep")
            ]
            assert entered.wait(timeout=3)
            release.set()
            [future.result(timeout=3) for future in futures]

        assert calls == 1

    def test_names_listing(self):
        from quantmaster.factors.fundamental import (
            FUNDAMENTAL_FACTOR_NAMES,
            list_fundamental_factors,
        )

        assert set(FUNDAMENTAL_FACTOR_NAMES) == {
            "ep", "bp", "dividend_yield", "small_cap", "roe"}
        listing = list_fundamental_factors()
        assert all(item["description"].startswith("[基本面]") for item in listing)


def test_bulk_fundamental_failures_are_aggregated_once(monkeypatch, caplog):
    monkeypatch.setattr(
        fundamentals, "_load_cached_or_fetch", lambda *args, **kwargs: None,
    )
    symbols = [f"{number:06d}.SZ" for number in range(1000)]

    with caplog.at_level(logging.WARNING, logger="quantmaster.data.fundamentals"):
        result = fundamental_panel(
            symbols, "2026-01-01", "2026-01-31",
            fields=["pe_ttm", "roe"], store=object(),
        )

    warnings = [record for record in caplog.records if record.levelno >= logging.WARNING]
    assert result == {}
    assert len(warnings) == 1
    assert "1000/1000" in warnings[0].getMessage()
