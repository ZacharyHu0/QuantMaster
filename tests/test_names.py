"""证券主数据名称测试（离线）。"""

from quantmaster.data import names as mod
from quantmaster.data.instruments import InstrumentStore
from quantmaster.data.universe import DEMO_STOCK_NAMES


def test_demo_names_are_available_without_network(monkeypatch):
    monkeypatch.setattr(
        mod, "refresh_stock_names",
        lambda symbols: (_ for _ in ()).throw(AssertionError("不应触网")),
    )
    # The writer publishes the bundled master once; the page-facing reader
    # must then simply consume that local snapshot.
    InstrumentStore()
    symbols = list(DEMO_STOCK_NAMES)[:3]
    assert mod.read_stock_names(symbols) == {
        symbol: DEMO_STOCK_NAMES[symbol] for symbol in symbols
    }


def test_partial_name_refresh_merges_local_master_data(monkeypatch):
    mod.save_stock_names({"000001.SZ": "平安银行"})
    monkeypatch.setattr(
        mod, "refresh_stock_names", lambda symbols: {"600000.SH": "浦发银行"})

    result = mod.refresh_stock_names_if_needed(["000001.SZ", "600000.SH"])
    assert result == {"000001.SZ": "平安银行", "600000.SH": "浦发银行"}
    assert mod.read_stock_names(list(result)) == result
