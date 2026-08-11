"""分钟线标准化、按频率持久化与缓存复用。"""

from typing import ClassVar

import pandas as pd
import pytest

from quantmaster.data import registry
from quantmaster.data.base import (
    DataCapability,
    DataSource,
    Market,
    normalize_bars,
    normalize_daily,
)
from quantmaster.data.storage import BarStore, IntradayBarStore


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


def test_daily_bars_and_cache_drop_provider_timezone(tmp_path):
    index = pd.date_range("2026-08-05", periods=2, tz="America/New_York")
    raw = pd.DataFrame({
        "Open": [10.0, 10.1], "High": [10.2, 10.3], "Low": [9.9, 10.0],
        "Close": [10.1, 10.2], "Volume": [1000.0, 1200.0],
    }, index=index)

    normalized = normalize_daily(raw)
    assert normalized.index.tz is None
    assert normalized.index[0] == pd.Timestamp("2026-08-05")

    store = BarStore(root=tmp_path / "bars")
    store.put("DX-Y.NYB.US", raw, replace=True)
    cached = store.get("DX-Y.NYB.US")
    assert cached is not None
    assert cached.index.tz is None
    assert cached.loc["2026-08-05":"2026-08-06"].shape[0] == 2


def test_intraday_cache_preserves_timezone(tmp_path):
    index = pd.date_range("2026-08-05 09:30", periods=2, freq="5min", tz="Asia/Shanghai")
    frame = pd.DataFrame({"close": [10.0, 10.1]}, index=index)
    store = IntradayBarStore("5m", root=tmp_path / "intraday")

    store.put("600000.SH", frame, replace=True)

    cached = store.get("600000.SH")
    assert cached is not None
    assert str(cached.index.tz) == "Asia/Shanghai"


def test_registry_selects_only_sources_with_the_required_capability(monkeypatch):
    class DailyOnly(DataSource):
        def daily(self, symbol, start, end):
            return pd.DataFrame()

    class MinuteSource(DailyOnly):
        def intraday(self, symbol, start, end, frequency="5m"):
            return pd.DataFrame()

    monkeypatch.setattr(
        registry,
        "_factories",
        lambda: {Market.CN: [DailyOnly, MinuteSource]},
    )

    assert isinstance(
        registry.get_source(Market.CN, DataCapability.INTRADAY),
        MinuteSource,
    )


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
    first = registry.refresh_intraday(
        "600000.SH", "2024-01-02 09:30", "2024-01-02 09:45", "5m", store=store)
    second = registry.refresh_intraday(
        "600000.SH", "2024-01-02 09:30", "2024-01-02 09:45", "5m", store=store)
    whole_day = registry.refresh_intraday(
        "600000.SH", "2024-01-02", "2024-01-02", "5m", store=store)
    assert len(first.data) == len(second.data) == 4
    assert len(whole_day.data) == 4
    assert first.quality.status != "unavailable"
    assert second.quality.status != "unavailable"
    assert whole_day.quality.status != "unavailable"
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
    panel_envelope = registry.refresh_bar_panel(
        ["600000.SH", "000001.SZ"], "2024-01-02 09:30", "2024-01-02 09:40", "5m",
        progress=lambda *args: updates.append(args),
    )
    panel = panel_envelope.data
    assert panel_envelope.quality.status != "unavailable"
    assert set(panel) >= {"open", "high", "low", "close", "volume"}
    assert panel["close"].shape == (3, 2)
    # 并发加载按实际完成顺序回调，但进度编号连续且每只标的只报告一次。
    assert [item[:2] for item in updates] == [(1, 2), (2, 2)]
    assert {(item[2], item[3]) for item in updates} == {
        ("600000.SH", True), ("000001.SZ", True),
    }
    close_envelope = registry.refresh_bar_panel(
        ["600000.SH", "000001.SZ"], "2024-01-02 09:30", "2024-01-02 09:40",
        "5m", field="close")
    close = close_envelope.data
    assert close_envelope.quality.status != "unavailable"
    pd.testing.assert_frame_equal(close, panel["close"])
