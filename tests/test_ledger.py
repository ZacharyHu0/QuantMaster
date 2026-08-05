"""实盘账本与收益统计测试。"""

import math

import pytest

from quantmaster.portfolio import Ledger, TradeRecord, ledger_report
from quantmaster.portfolio.performance import xirr


def _trade(date, symbol, side, price, shares, fee=0.0):
    return TradeRecord(date=date, symbol=symbol, side=side,
                       price=price, shares=shares, fee=fee)


class TestFIFO:
    def test_avg_cost_includes_fee(self, tmp_path):
        ledger = Ledger(path=tmp_path / "l.sqlite")
        ledger.add_trade(_trade("2024-01-02", "600519.SH", "buy", 100.0, 100, fee=10.0))
        pos = ledger.positions()[0]
        assert pos.shares == 100
        assert pos.avg_cost == pytest.approx(100.1)   # 100 + 10/100

    def test_fifo_realized_pnl(self, tmp_path):
        ledger = Ledger(path=tmp_path / "l.sqlite")
        ledger.add_trade(_trade("2024-01-02", "600519.SH", "buy", 10.0, 100))
        ledger.add_trade(_trade("2024-01-03", "600519.SH", "buy", 20.0, 100))
        ledger.add_trade(_trade("2024-01-04", "600519.SH", "sell", 30.0, 150))
        pos = ledger.positions()[0]
        # FIFO: 卖 100@成本10 + 50@成本20 -> (30-10)*100 + (30-20)*50 = 2500
        assert pos.realized_pnl == pytest.approx(2500.0)
        assert pos.shares == 50
        assert pos.avg_cost == pytest.approx(20.0)

    def test_rejects_bad_side(self, tmp_path):
        ledger = Ledger(path=tmp_path / "l.sqlite")
        with pytest.raises(ValueError):
            ledger.add_trade(_trade("2024-01-02", "600519.SH", "short", 10.0, 100))

    def test_rejects_nonfinite_trade_numbers(self, tmp_path):
        for index, (field, value) in enumerate((
            ("price", math.nan), ("price", math.inf),
            ("shares", math.nan), ("shares", math.inf),
            ("fee", math.nan), ("fee", math.inf),
        )):
            ledger = Ledger(path=tmp_path / f"invalid-{index}.sqlite")
            trade = _trade("2024-01-02", "600519.SH", "buy", 10.0, 100)
            setattr(trade, field, value)
            with pytest.raises(ValueError, match="有限数字"):
                ledger.add_trade(trade)
            assert ledger.trades().empty

    def test_sell_cannot_exceed_chronological_inventory(self, tmp_path):
        ledger = Ledger(path=tmp_path / "l.sqlite")
        ledger.add_trade(_trade("2024-01-02", "600519.SH", "buy", 10.0, 100))
        with pytest.raises(ValueError, match="卖出超过可用持仓"):
            ledger.add_trade(_trade("2024-01-03", "600519.SH", "sell", 12.0, 101))
        with pytest.raises(ValueError, match="卖出超过可用持仓"):
            ledger.add_trade(_trade("2024-01-01", "600519.SH", "sell", 12.0, 1))
        assert len(ledger.trades()) == 1

    def test_duplicate_idempotent_sell_does_not_trigger_false_oversell(self, tmp_path):
        ledger = Ledger(path=tmp_path / "l.sqlite")
        ledger.add_trade(_trade("2024-01-02", "600519.SH", "buy", 10.0, 100))
        sell = _trade("2024-01-03", "600519.SH", "sell", 12.0, 100)
        assert ledger.add_trade(sell, idempotency_key="sell-1") is True
        assert ledger.add_trade(sell, idempotency_key="sell-1") is False

    def test_rejects_nonfinite_cashflows(self, tmp_path):
        for index, amount in enumerate((math.nan, math.inf, -math.inf)):
            ledger = Ledger(path=tmp_path / f"invalid-cash-{index}.sqlite")
            with pytest.raises(ValueError, match="有限数字"):
                ledger.add_cashflow("2024-01-02", amount)
            assert ledger.cashflows().empty


class TestCSVImport:
    def test_import(self, tmp_path):
        csv = tmp_path / "trades.csv"
        csv.write_text(
            "date,symbol,side,price,shares,fee\n"
            "2024-01-08,600519.SH,buy,1620.0,100,8.1\n"
            "2024-02-01,600519.SH,sell,1700.0,100,8.5\n",
            encoding="utf-8",
        )
        ledger = Ledger(path=tmp_path / "l.sqlite")
        assert ledger.import_csv(csv) == 2
        pos = ledger.positions()[0]
        assert pos.shares == 0
        assert pos.realized_pnl == pytest.approx((1700 - 1620) * 100 - 8.1 - 8.5)

    def test_missing_columns(self, tmp_path):
        csv = tmp_path / "bad.csv"
        csv.write_text("date,symbol\n2024-01-08,600519.SH\n", encoding="utf-8")
        ledger = Ledger(path=tmp_path / "l.sqlite")
        with pytest.raises(ValueError, match="缺少列"):
            ledger.import_csv(csv)

    def test_batch_oversell_rolls_back_all_rows(self, tmp_path):
        ledger = Ledger(path=tmp_path / "l.sqlite")
        records = [
            {"date": "2024-01-02", "symbol": "600519.SH", "side": "buy",
             "price": 10, "shares": 100, "fee": 0},
            {"date": "2024-01-03", "symbol": "600519.SH", "side": "sell",
             "price": 12, "shares": 101, "fee": 0},
        ]
        with pytest.raises(ValueError, match="卖出超过可用持仓"):
            ledger.import_records(records, "batch", "trades.csv", "utf-8")
        assert ledger.trades().empty
        assert not ledger.has_import_hash("batch")


class TestReport:
    def test_report_totals(self, tmp_path):
        ledger = Ledger(path=tmp_path / "l.sqlite")
        ledger.add_cashflow("2024-01-01", 100_000, "deposit")
        ledger.add_trade(_trade("2024-01-02", "600519.SH", "buy", 100.0, 100, fee=5.0))
        report = ledger_report(ledger, prices={"600519.SH": 110.0})
        assert report["market_value"] == pytest.approx(11_000)
        assert report["cash"] == pytest.approx(100_000 - 10_000 - 5)
        assert report["unrealized_pnl"] == pytest.approx((110 - 100.05) * 100)
        assert report["total_pnl"] == pytest.approx(
            report["total_assets"] - 100_000)

    def test_missing_price_flagged(self, tmp_path):
        ledger = Ledger(path=tmp_path / "l.sqlite")
        ledger.add_cashflow("2024-01-01", 50_000, "deposit")
        ledger.add_trade(_trade("2024-01-02", "000001.SZ", "buy", 10.0, 100))
        report = ledger_report(ledger, prices={})
        assert report["missing_price"] == ["000001.SZ"]


class TestXIRR:
    def test_simple_doubling(self):
        # 一年翻倍 -> 年化 100%
        rate = xirr([("2023-01-01", -100.0), ("2024-01-01", 200.0)])
        assert rate == pytest.approx(1.0, abs=0.02)

    def test_no_solution(self):
        assert xirr([("2023-01-01", -100.0)]) is None
        assert xirr([("2023-01-01", -100.0), ("2024-01-01", -50.0)]) is None
