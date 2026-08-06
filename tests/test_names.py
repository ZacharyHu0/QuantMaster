"""证券主数据名称测试（离线）。"""

from quantmaster.data import names as mod
from quantmaster.data.universe import DEMO_STOCK_NAMES


def test_demo_names_are_available_without_network(monkeypatch):
    monkeypatch.setattr(
        mod, "fetch_stock_names",
        lambda symbols: (_ for _ in ()).throw(AssertionError("不应触网")),
    )
    symbols = list(DEMO_STOCK_NAMES)[:3]
    assert mod.load_stock_names(symbols) == {
        symbol: DEMO_STOCK_NAMES[symbol] for symbol in symbols
    }


def test_partial_name_refresh_merges_local_master_data(monkeypatch):
    mod.save_stock_names({"000001.SZ": "平安银行"})
    monkeypatch.setattr(
        mod, "fetch_stock_names", lambda symbols: {"600000.SH": "浦发银行"})

    result = mod.load_stock_names(["000001.SZ", "600000.SH"])
    assert result == {"000001.SZ": "平安银行", "600000.SH": "浦发银行"}
    assert mod.cached_stock_names(list(result)) == result
