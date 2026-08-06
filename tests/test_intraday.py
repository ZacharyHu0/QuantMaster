"""分钟线标准化、按频率持久化与缓存复用。"""

from typing import ClassVar

import pandas as pd
import pytest

from quantmaster.data import registry
from quantmaster.data.base import DataSource, Market, normalize_bars
from quantmaster.data.storage import IntradayBarStore


def test_normalize_chinese_intraday_columns():
    raw = pd.DataFrame({
        "时间": ["2024-01-02 09:30:00", "2024-01-02 09:35:00"],
        "开盘": [10.0, 10.1], "收盘": [10.1, 10.2],
        "最高": [10.2, 10.3], "最低": [9.9, 10.0],
        "成交量": [1000, 1200], "成交额": [10100, 12240],
    })
    bars = normalize_bars(raw)
    assert isinstance(bars.index, pd.DatetimeIndex)
    assert list(bars.columns) == ["open", "high", "low", "close", "volume", "amount"]
    assert bars.index[1].minute == 35


def test_intraday_cache_is_reused_and_frequency_isolated(tmp_path, monkeypatch):
    class FakeSource(DataSource):
        name = "fake"
        markets = (Market.CN,)
        calls: ClassVar[list[str]] = []

        def daily(self, symbol, start, end):
            raise NotImplementedError

        def intraday(self, symbol, start, end, frequency="5m"):
            self.calls.append(frequency)
            index = pd.date_range("2024-01-02 09:30", periods=4, freq="5min")
            return pd.DataFrame({
                "open": [10.0] * 4, "high": [10.2] * 4, "low": [9.9] * 4,
                "close": [10.1] * 4, "volume": [1000.0] * 4,
            }, index=index)

    monkeypatch.setattr(registry, "_factories", lambda: {Market.CN: [FakeSource]})
    store = IntradayBarStore("5m", root=tmp_path / "intraday")
    first = registry.load_intraday(
        "600000.SH", "2024-01-02 09:30", "2024-01-02 09:45", "5m", store=store)
    second = registry.load_intraday(
        "600000.SH", "2024-01-02 09:30", "2024-01-02 09:45", "5m", store=store)
    whole_day = registry.load_intraday(
        "600000.SH", "2024-01-02", "2024-01-02", "5m", store=store)
    assert len(first) == len(second) == 4
    assert len(whole_day) == 4
    assert FakeSource.calls == ["5m"]
    assert not list(store.root.glob("*.tmp"))
    assert IntradayBarStore("15m", root=tmp_path / "intraday").root != store.root
    with pytest.raises(ValueError, match="非法字符"):
        store.path_for_repair("../escape")


def test_multisymbol_intraday_panel(monkeypatch):
    class FakeSource(DataSource):
        name = "fake"
        markets = (Market.CN,)

        def daily(self, symbol, start, end):
            raise NotImplementedError

        def intraday(self, symbol, start, end, frequency="5m"):
            index = pd.date_range("2024-01-02 09:30", periods=3, freq="5min")
            offset = 1.0 if symbol.startswith("600") else 2.0
            return pd.DataFrame({
                "open": offset, "high": offset + 0.2, "low": offset - 0.1,
                "close": offset + 0.1, "volume": 1000.0,
            }, index=index)

    monkeypatch.setattr(registry, "_factories", lambda: {Market.CN: [FakeSource]})
    updates = []
    panel = registry.load_bar_panel(
        ["600000.SH", "000001.SZ"], "2024-01-02 09:30", "2024-01-02 09:40", "5m",
        progress=lambda *args: updates.append(args),
    )
    assert set(panel) >= {"open", "high", "low", "close", "volume"}
    assert panel["close"].shape == (3, 2)
    # 并发加载按实际完成顺序回调，但进度编号连续且每只标的只报告一次。
    assert [item[:2] for item in updates] == [(1, 2), (2, 2)]
    assert {(item[2], item[3]) for item in updates} == {
        ("600000.SH", True), ("000001.SZ", True),
    }
    close = registry.load_bar_panel(
        ["600000.SH", "000001.SZ"], "2024-01-02 09:30", "2024-01-02 09:40",
        "5m", field="close")
    pd.testing.assert_frame_equal(close, panel["close"])
