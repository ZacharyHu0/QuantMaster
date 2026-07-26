"""每日净值重建（daily_nav / nav_with_benchmark）测试：全部合成数据，手工核算断言。"""

from __future__ import annotations

import json

import pandas as pd
import pytest

from quantmaster.portfolio.ledger import Ledger, TradeRecord
from quantmaster.portfolio.nav import daily_nav, nav_with_benchmark

SYM = "600519.SH"


def _trade(date, side, price, shares, fee=0.0, symbol=SYM):
    return TradeRecord(date=date, symbol=symbol, side=side, price=price, shares=shares, fee=fee)


def _prices(dates, values, symbol=SYM) -> pd.DataFrame:
    return pd.DataFrame({symbol: values}, index=pd.to_datetime(list(dates)))


class TestDailyNav:
    def test_deposit_buy_price_up(self, tmp_path):
        """单笔入金 + 单笔买入 + 价格上涨：逐日 total_assets/pnl 手工核算到分。"""
        ledger = Ledger(path=tmp_path / "l.sqlite")
        ledger.add_cashflow("2024-01-02", 100_000, "deposit")
        ledger.add_trade(_trade("2024-01-03", "buy", 100.0, 100, fee=5.0))
        prices = _prices(["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"],
                         [100.0, 101.0, 105.0, 103.0])
        nav = daily_nav(ledger, prices)

        assert list(nav.columns) == [
            "cash", "position_value", "total_assets", "net_invested", "pnl", "twr_nav"]
        assert len(nav) == 4
        d1, d2, d3, d4 = nav.index

        # 首日：只有入金，昨日资产为 0 -> 当日收益 0，twr 起点 1.0
        assert nav.loc[d1, "cash"] == pytest.approx(100_000.0, abs=0.01)
        assert nav.loc[d1, "total_assets"] == pytest.approx(100_000.0, abs=0.01)
        assert nav.loc[d1, "pnl"] == pytest.approx(0.0, abs=0.01)
        assert nav.loc[d1, "twr_nav"] == pytest.approx(1.0)

        # 买入日：cash = 100000 - 100*100 - 5 = 89995，持仓按当日收盘 101 估值
        assert nav.loc[d2, "cash"] == pytest.approx(89_995.0, abs=0.01)
        assert nav.loc[d2, "position_value"] == pytest.approx(10_100.0, abs=0.01)
        assert nav.loc[d2, "total_assets"] == pytest.approx(100_095.0, abs=0.01)
        assert nav.loc[d2, "pnl"] == pytest.approx(95.0, abs=0.01)

        # 之后无出入金，twr 相对首日资产telescoping：twr_t = assets_t / 100000
        assert nav.loc[d3, "total_assets"] == pytest.approx(100_495.0, abs=0.01)
        assert nav.loc[d3, "pnl"] == pytest.approx(495.0, abs=0.01)
        assert nav.loc[d3, "twr_nav"] == pytest.approx(100_495.0 / 100_000.0)
        assert nav.loc[d4, "total_assets"] == pytest.approx(100_295.0, abs=0.01)
        assert nav.loc[d4, "twr_nav"] == pytest.approx(100_295.0 / 100_000.0)
        assert nav["net_invested"].iloc[-1] == pytest.approx(100_000.0)

    def test_second_deposit_no_twr_jump(self, tmp_path):
        """关键测试：入金日总资产跳涨 50%，twr 却不因入金本身产生收益。

        入金按期初流量口径进分母（GIPS 常用惯例）：当日盈亏 200 除以
        「昨日资产 + 当日入金」150000，日收益 ≈ 0.1333%，而不是资产
        跳涨的 50.2%。（旧的期末流量口径在「入金当日建仓」场景会把
        收益按前日小基数放大出百倍失真，见 test_deposit_invested_same_day。）
        """
        ledger = Ledger(path=tmp_path / "l.sqlite")
        ledger.add_cashflow("2024-01-02", 100_000, "deposit")
        ledger.add_trade(_trade("2024-01-03", "buy", 100.0, 100))
        ledger.add_cashflow("2024-01-04", 50_000, "deposit")   # 二次入金
        prices = _prices(["2024-01-02", "2024-01-03", "2024-01-04"],
                         [100.0, 100.0, 102.0])
        nav = daily_nav(ledger, prices)
        d2, d3 = nav.index[1], nav.index[2]

        # 入金日：assets 100000 -> 150200（跳涨 50.2%），但持仓只赚了 200
        assert nav.loc[d3, "total_assets"] == pytest.approx(150_200.0, abs=0.01)
        assert nav.loc[d3, "net_invested"] == pytest.approx(150_000.0, abs=0.01)
        day_return = nav.loc[d3, "twr_nav"] / nav.loc[d2, "twr_nav"] - 1.0
        assert day_return == pytest.approx(200.0 / 150_000.0, abs=1e-9)

    def test_deposit_invested_same_day(self, tmp_path):
        """回归测试：大额入金当日建仓，收益必须按含入金的本金基数计算。

        旧口径 r=(assets-flow)/prev 会算出 (1020100-1000000)/10000-1 ≈ +101%
        的荒谬日收益（真实约 +1%），跌 2% 时甚至把净值打成负数。
        """
        ledger = Ledger(path=tmp_path / "l.sqlite")
        ledger.add_cashflow("2024-01-02", 10_000, "deposit")
        ledger.add_cashflow("2024-01-03", 1_000_000, "deposit")
        ledger.add_trade(_trade("2024-01-03", "buy", 100.0, 10_000))   # 当日全额买入
        prices = _prices(["2024-01-02", "2024-01-03"], [100.0, 101.0])
        nav = daily_nav(ledger, prices)
        d1, d2 = nav.index[0], nav.index[1]

        day_return = nav.loc[d2, "twr_nav"] / nav.loc[d1, "twr_nav"] - 1.0
        # 持仓市值 101万，现金 1万，总资产 1,020,000；基数 1,010,000
        assert day_return == pytest.approx(10_000.0 / 1_010_000.0, abs=1e-9)
        assert 0 < day_return < 0.02   # 绝不允许出现百分之百量级的失真
        assert (nav["twr_nav"] > 0).all()

    def test_withdraw_no_twr_drop(self, tmp_path):
        """出金日价格不变：总资产下降，但 twr 不动。"""
        ledger = Ledger(path=tmp_path / "l.sqlite")
        ledger.add_cashflow("2024-01-02", 100_000, "deposit")
        ledger.add_trade(_trade("2024-01-03", "buy", 100.0, 100))
        ledger.add_cashflow("2024-01-04", 30_000, "withdraw")
        prices = _prices(["2024-01-02", "2024-01-03", "2024-01-04"],
                         [100.0, 100.0, 100.0])
        nav = daily_nav(ledger, prices)
        last = nav.index[-1]

        assert nav.loc[last, "cash"] == pytest.approx(60_000.0, abs=0.01)
        assert nav.loc[last, "total_assets"] == pytest.approx(70_000.0, abs=0.01)
        assert nav.loc[last, "net_invested"] == pytest.approx(70_000.0, abs=0.01)
        assert nav.loc[last, "pnl"] == pytest.approx(0.0, abs=0.01)
        # 价格全程不变 -> 各日 twr 均为 1.0（出金被剔除，不算亏损）
        assert nav["twr_nav"].iloc[-1] == pytest.approx(1.0, abs=1e-9)

    def test_dividend_in_cash_pnl_and_twr(self, tmp_path):
        """分红计入 cash 与 pnl，且不当作外部流入 -> 体现为 twr 的正收益。"""
        ledger = Ledger(path=tmp_path / "l.sqlite")
        ledger.add_cashflow("2024-01-02", 100_000, "deposit")
        ledger.add_trade(_trade("2024-01-03", "buy", 100.0, 100))
        ledger.add_cashflow("2024-01-04", 500, "dividend")
        prices = _prices(["2024-01-02", "2024-01-03", "2024-01-04"],
                         [100.0, 100.0, 100.0])
        nav = daily_nav(ledger, prices)
        last = nav.index[-1]

        assert nav.loc[last, "cash"] == pytest.approx(90_500.0, abs=0.01)
        assert nav.loc[last, "pnl"] == pytest.approx(500.0, abs=0.01)
        assert nav.loc[last, "net_invested"] == pytest.approx(100_000.0, abs=0.01)
        # 分红日 twr 收益 = 500 / 100000 = 0.5%
        assert nav.loc[last, "twr_nav"] == pytest.approx(1.005, abs=1e-9)

    def test_sell_updates_cash(self, tmp_path):
        """卖出后：cash = 入金 - 买入 - 买费 + 卖出净额 - 卖费，持仓清零。"""
        ledger = Ledger(path=tmp_path / "l.sqlite")
        ledger.add_cashflow("2024-01-02", 100_000, "deposit")
        ledger.add_trade(_trade("2024-01-03", "buy", 100.0, 100, fee=5.0))
        ledger.add_trade(_trade("2024-01-04", "sell", 110.0, 100, fee=6.0))
        prices = _prices(["2024-01-02", "2024-01-03", "2024-01-04"],
                         [100.0, 100.0, 110.0])
        nav = daily_nav(ledger, prices)
        last = nav.index[-1]

        assert nav.loc[last, "cash"] == pytest.approx(100_000 - 10_000 - 5 + 11_000 - 6, abs=0.01)
        assert nav.loc[last, "position_value"] == pytest.approx(0.0, abs=0.01)
        assert nav.loc[last, "pnl"] == pytest.approx(989.0, abs=0.01)

    def test_missing_price_ffill(self, tmp_path):
        """停牌/缺价日：持仓用最近可得价估值（ffill，不用未来价格）。"""
        ledger = Ledger(path=tmp_path / "l.sqlite")
        ledger.add_cashflow("2024-01-02", 100_000, "deposit")
        ledger.add_trade(_trade("2024-01-02", "buy", 100.0, 100))
        prices = _prices(["2024-01-02", "2024-01-03", "2024-01-04"],
                         [100.0, float("nan"), 104.0])
        nav = daily_nav(ledger, prices)

        # 缺价日按前一日收盘 100 估值，而不是未来的 104
        assert nav["position_value"].iloc[1] == pytest.approx(10_000.0, abs=0.01)
        assert nav["position_value"].iloc[2] == pytest.approx(10_400.0, abs=0.01)

    def test_trade_before_first_price_uses_trade_price(self, tmp_path):
        """成交日早于价格面板首日：退回用成交价估值（仍是过去信息，无未来函数）。"""
        ledger = Ledger(path=tmp_path / "l.sqlite")
        ledger.add_cashflow("2024-01-01", 100_000, "deposit")
        ledger.add_trade(_trade("2024-01-01", "buy", 98.0, 100))
        prices = _prices(["2024-01-02", "2024-01-03"], [100.0, 101.0])
        nav = daily_nav(ledger, prices)

        assert nav.index[0] == pd.Timestamp("2024-01-01")
        assert nav["position_value"].iloc[0] == pytest.approx(9_800.0, abs=0.01)
        assert nav["position_value"].iloc[1] == pytest.approx(10_000.0, abs=0.01)

    def test_end_truncates(self, tmp_path):
        ledger = Ledger(path=tmp_path / "l.sqlite")
        ledger.add_cashflow("2024-01-02", 100_000, "deposit")
        prices = _prices(["2024-01-02", "2024-01-03", "2024-01-04"], [100.0, 101.0, 102.0])
        nav = daily_nav(ledger, prices, end="2024-01-03")
        assert len(nav) == 2
        assert nav.index[-1] == pd.Timestamp("2024-01-03")

    def test_empty_ledger(self, tmp_path):
        """空账本：返回空 DataFrame（保留列结构），不崩。"""
        ledger = Ledger(path=tmp_path / "l.sqlite")
        prices = _prices(["2024-01-02"], [100.0])
        nav = daily_nav(ledger, prices)
        assert nav.empty
        assert list(nav.columns) == [
            "cash", "position_value", "total_assets", "net_invested", "pnl", "twr_nav"]


class TestNavWithBenchmark:
    def _nav(self, tmp_path) -> pd.DataFrame:
        ledger = Ledger(path=tmp_path / "l.sqlite")
        ledger.add_cashflow("2024-01-02", 100_000, "deposit")
        ledger.add_trade(_trade("2024-01-03", "buy", 100.0, 100))
        prices = _prices(["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"],
                         [100.0, 101.0, 105.0, 103.0])
        return daily_nav(ledger, prices)

    def test_json_serializable(self, tmp_path):
        nav = self._nav(tmp_path)
        bench = pd.Series([3000.0, 3030.0, 3015.0, 3060.0],
                          index=pd.to_datetime(
                              ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]))
        result = nav_with_benchmark(nav, bench)

        text = json.dumps(result)   # numpy 类型会在这里炸，必须是原生 float/str
        assert isinstance(text, str)
        assert set(result) == {"dates", "twr", "benchmark", "excess_annual"}
        assert len(result["dates"]) == len(result["twr"]) == len(result["benchmark"]) == 4
        # 两条曲线都在共同区间首日归一到 1.0
        assert result["twr"][0] == pytest.approx(1.0)
        assert result["benchmark"][0] == pytest.approx(1.0)
        assert result["benchmark"][-1] == pytest.approx(3060.0 / 3000.0, abs=1e-4)
        assert isinstance(result["excess_annual"], float)

    def test_partial_overlap_alignment(self, tmp_path):
        """基准只覆盖部分日期：按交集对齐并在交集首日重新归一。"""
        nav = self._nav(tmp_path)
        bench = pd.Series([3030.0, 3060.0],
                          index=pd.to_datetime(["2024-01-03", "2024-01-04"]))
        result = nav_with_benchmark(nav, bench)
        assert result["dates"] == ["2024-01-03", "2024-01-04"]
        assert result["twr"][0] == pytest.approx(1.0)
        # 交集区间内组合收益 100495/100095，基准收益 3060/3030
        assert result["twr"][-1] == pytest.approx(100_495.0 / 100_095.0, abs=1e-4)
        assert result["benchmark"][-1] == pytest.approx(3060.0 / 3030.0, abs=1e-4)

    def test_empty_inputs(self, tmp_path):
        ledger = Ledger(path=tmp_path / "l.sqlite")
        empty_nav = daily_nav(ledger, _prices(["2024-01-02"], [100.0]))
        bench = pd.Series([3000.0], index=pd.to_datetime(["2024-01-02"]))
        result = nav_with_benchmark(empty_nav, bench)
        assert result == {"dates": [], "twr": [], "benchmark": [], "excess_annual": 0.0}
        json.dumps(result)


class TestNavWarnings:
    def test_negative_cash_warns(self, tmp_path):
        """漏记入金（只有买入没有入金）必须给出明确警告，而不是静默输出荒谬收益。"""
        from quantmaster.portfolio import nav_warnings

        ledger = Ledger(path=tmp_path / "l.sqlite")
        ledger.add_trade(_trade("2024-01-03", "buy", 100.0, 100))   # 没有入金记录
        prices = _prices(["2024-01-03", "2024-01-04"], [100.0, 105.0])
        nav = daily_nav(ledger, prices)
        warnings = nav_warnings(nav)
        assert warnings, "负现金/零入金必须有警告"
        assert any("入金" in w for w in warnings)
        # 且 TWR 链条不被负基数打坏
        assert nav["twr_nav"].notna().all()

    def test_healthy_ledger_no_warnings(self, tmp_path):
        from quantmaster.portfolio import nav_warnings

        ledger = Ledger(path=tmp_path / "l.sqlite")
        ledger.add_cashflow("2024-01-02", 100_000, "deposit")
        ledger.add_trade(_trade("2024-01-03", "buy", 100.0, 100))
        prices = _prices(["2024-01-02", "2024-01-03"], [100.0, 101.0])
        assert nav_warnings(daily_nav(ledger, prices)) == []
