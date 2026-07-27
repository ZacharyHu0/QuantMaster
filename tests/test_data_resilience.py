"""AKShare 重试与 Tushare 2000 积分档缓存/限流测试。"""

from __future__ import annotations

import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import ClassVar

import pandas as pd
import pytest

from quantmaster.data import registry
from quantmaster.data.base import DataSource, Market
from quantmaster.data.resilience import (
    PROVIDER_HEALTH,
    EndpointFrameCache,
    TushareRateLimiter,
    akshare_call,
    provider_call,
)
from quantmaster.data.storage import BarStore
from quantmaster.data.tushare_source import TushareSource


def test_akshare_exponential_retry(isolated_config, monkeypatch):
    isolated_config.data.akshare_retries = 3
    isolated_config.data.akshare_retry_backoff = 0.25
    sleeps: list[float] = []
    attempts = 0

    def flaky():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ConnectionError("temporary")
        return "ok"

    monkeypatch.setattr("quantmaster.data.resilience.time.sleep", sleeps.append)
    assert akshare_call("test", flaky) == "ok"
    assert attempts == 3
    assert sleeps == [0.25, 0.5]


def test_tushare_rate_limit_is_shared_in_data_root(isolated_config, monkeypatch):
    isolated_config.data.tushare_calls_per_minute = 600
    sleeps: list[float] = []
    monkeypatch.setattr("quantmaster.data.resilience.time.time", lambda: 1000.0)
    monkeypatch.setattr("quantmaster.data.resilience.time.sleep", sleeps.append)
    limiter = TushareRateLimiter()
    limiter.wait()
    limiter.wait()
    assert sleeps == [pytest.approx(0.1)]
    assert (isolated_config.data_root / "tushare_rate.sqlite").exists()


def test_tushare_qfq_units_and_disk_cache(tmp_path, isolated_config, monkeypatch):
    isolated_config.data.tushare_token = "test-token"
    monkeypatch.setattr("quantmaster.data.tushare_source.TUSHARE_LIMITER.wait", lambda: None)

    class FakePro:
        def __init__(self):
            self.calls: list[str] = []

        def daily(self, **params):
            self.calls.append("daily")
            assert params["ts_code"] == "600000.SH"
            return pd.DataFrame({
                "ts_code": ["600000.SH", "600000.SH"],
                "trade_date": ["20240103", "20240102"],
                "open": [12.0, 10.0], "high": [13.0, 11.0],
                "low": [11.0, 9.0], "close": [12.0, 10.0],
                "vol": [20.0, 10.0], "amount": [2.0, 1.0],
            })

        def adj_factor(self, **params):
            self.calls.append("adj_factor")
            return pd.DataFrame({
                "ts_code": ["600000.SH", "600000.SH"],
                "trade_date": ["20240103", "20240102"],
                "adj_factor": [2.0, 1.0],
            })

    api = FakePro()
    cache = EndpointFrameCache("tushare", root=tmp_path / "api-cache")
    source = TushareSource(cache=cache)
    source._api = api
    first = source.daily("600000.SH", "2024-01-02", "2024-01-03")

    # 以前一日因子/结束日因子计算 qfq；Tushare 手/千元统一成股/元。
    assert first.loc["2024-01-02", "close"] == pytest.approx(5.0)
    assert first.loc["2024-01-02", "volume"] == pytest.approx(1000.0)
    assert first.loc["2024-01-02", "amount"] == pytest.approx(1000.0)
    assert api.calls == ["daily", "adj_factor"]

    # 新实例模拟服务重启；相同接口参数应直接读取 Parquet，不再调用 Tushare。
    second = TushareSource(cache=cache)
    second._api = api
    again = second.daily("600000.SH", "2024-01-02", "2024-01-03")
    pd.testing.assert_frame_equal(first, again)
    assert api.calls == ["daily", "adj_factor"]


def test_tushare_index_uses_index_daily_only(tmp_path, isolated_config, monkeypatch):
    isolated_config.data.tushare_token = "test-token"
    monkeypatch.setattr("quantmaster.data.tushare_source.TUSHARE_LIMITER.wait", lambda: None)

    class FakePro:
        def __init__(self):
            self.calls: list[str] = []

        def index_daily(self, **params):
            self.calls.append("index_daily")
            return pd.DataFrame({
                "ts_code": ["000300.SH"], "trade_date": ["20240102"],
                "open": [3000.0], "high": [3010.0], "low": [2990.0],
                "close": [3005.0], "vol": [100.0], "amount": [20.0],
            })

    api = FakePro()
    source = TushareSource(EndpointFrameCache("tushare", root=tmp_path / "cache"))
    source._api = api
    frame = source.daily("000300.SH", "2024-01-02", "2024-01-02")
    assert not frame.empty
    assert api.calls == ["index_daily"]


def test_tushare_industry_is_batched_and_cached(tmp_path, isolated_config, monkeypatch):
    isolated_config.data.tushare_token = "test-token"
    monkeypatch.setattr("quantmaster.data.tushare_source.TUSHARE_LIMITER.wait", lambda: None)

    class FakePro:
        def __init__(self):
            self.calls: list[tuple[str, str]] = []

        def index_classify(self, **params):
            self.calls.append(("index_classify", ""))
            return pd.DataFrame({
                "index_code": ["801010.SI", "801020.SI"],
                "industry_name": ["农林牧渔", "煤炭"], "level": ["L1", "L1"],
            })

        def index_member_all(self, **params):
            code = params["l1_code"]
            self.calls.append(("index_member_all", code))
            symbol = "000001.SZ" if code == "801010.SI" else "600000.SH"
            return pd.DataFrame({
                "l1_code": [code], "l1_name": ["x"],
                "ts_code": [symbol], "is_new": ["Y"],
            })

    api = FakePro()
    cache = EndpointFrameCache("tushare", root=tmp_path / "cache")
    source = TushareSource(cache)
    source._api = api
    assert source.industry_map() == {"000001.SZ": "农林牧渔", "600000.SH": "煤炭"}
    assert len(api.calls) == 3

    restarted = TushareSource(cache)
    restarted._api = api
    assert restarted.industry_map()["600000.SH"] == "煤炭"
    assert len(api.calls) == 3


def test_fundamentals_fall_back_to_tushare(isolated_config, monkeypatch):
    from quantmaster.data import fundamentals

    isolated_config.data.akshare_retries = 1
    isolated_config.data.tushare_token = "test-token"

    class BrokenAkshare:
        @staticmethod
        def stock_a_indicator_lg(**params):
            raise ConnectionError("akshare down")

    expected = pd.DataFrame(
        {"pe_ttm": [12.0]}, index=pd.DatetimeIndex(["2024-01-02"], name="date"))
    monkeypatch.setattr(fundamentals, "_require_akshare", lambda: BrokenAkshare())
    monkeypatch.setattr(TushareSource, "daily_indicators", lambda self, symbol: expected)
    pd.testing.assert_frame_equal(fundamentals.fetch_daily_indicators("600000.SH"), expected)


def test_fresh_cache_does_not_hide_missing_end(tmp_path, isolated_config, monkeypatch):
    """刚写入但只覆盖旧区间的缓存，不能阻止后续区间增量拉取。"""
    store = BarStore(root=tmp_path / "bars")
    dates = pd.bdate_range("2024-01-02", "2024-01-31")
    cached = pd.DataFrame({
        "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0,
        "volume": 1000.0,
    }, index=dates)
    store.put("600000.SH", cached)

    class FakeSource(DataSource):
        name = "fake"
        markets = (Market.CN,)
        calls = 0

        def daily(self, symbol, start, end):
            FakeSource.calls += 1
            full_dates = pd.bdate_range(start, end)
            return pd.DataFrame({
                "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0,
                "volume": 1000.0,
            }, index=full_dates)

    monkeypatch.setattr(registry, "_factories", lambda: {Market.CN: [FakeSource]})
    result = registry.load_history(
        "600000.SH", "2024-01-02", "2024-06-28", store=store)
    assert FakeSource.calls == 1
    assert str(result.index.max().date()) == "2024-06-28"


def test_old_bar_metadata_is_migrated_without_network(tmp_path):
    root = tmp_path / "bars"
    root.mkdir()
    dates = pd.bdate_range("2024-01-02", periods=5)
    pd.DataFrame({"close": range(5)}, index=dates).to_parquet(root / "600000.SH.parquet")
    with sqlite3.connect(root / "meta.sqlite") as conn:
        conn.execute(
            "CREATE TABLE bar_meta (symbol TEXT PRIMARY KEY,start TEXT,end TEXT,updated_at REAL)"
        )
        conn.execute(
            "INSERT INTO bar_meta VALUES ('600000.SH','2024-01-02','2024-01-08',1234)"
        )

    meta = BarStore(root=root).metadata("600000.SH")
    assert meta["coverage_start"] == "2024-01-02"
    assert meta["coverage_end"] == "2024-01-08"
    assert meta["checked_at"] == 1234
    assert meta["last_status"] == "ready"


def test_historical_coverage_is_immutable_even_when_ttl_expired(tmp_path, monkeypatch):
    store = BarStore(root=tmp_path / "bars")
    dates = pd.bdate_range("2024-01-02", "2024-03-29")
    frame = pd.DataFrame({
        "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "volume": 1.0,
    }, index=dates)
    store.put("600000.SH", frame)
    with store._conn() as conn:
        conn.execute("UPDATE bar_meta SET checked_at=0")

    class MustNotRun(DataSource):
        name = "offline"
        markets = (Market.CN,)

        def daily(self, symbol, start, end):
            raise AssertionError("历史覆盖完整时不应触网")

    monkeypatch.setattr(registry, "_factories", lambda: {Market.CN: [MustNotRun]})
    result = registry.load_history("600000.SH", "2024-01-02", "2024-03-29", store=store)
    pd.testing.assert_frame_equal(result, frame, check_freq=False)


def test_current_auto_refresh_only_fetches_tail_overlap(tmp_path, monkeypatch):
    store = BarStore(root=tmp_path / "bars")
    end = pd.Timestamp.now().normalize()
    dates = pd.bdate_range(end=end - pd.Timedelta(days=10), periods=40)
    # bdate_range(end=..., periods=...) keeps the last cached day before the requested end.
    cached = pd.DataFrame({
        "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "volume": 1.0,
    }, index=dates)
    store.put("600000.SH", cached)
    with store._conn() as conn:
        conn.execute("UPDATE bar_meta SET checked_at=0")

    class TailSource(DataSource):
        name = "tail"
        markets = (Market.CN,)
        calls: ClassVar[list[tuple[str, str]]] = []

        def daily(self, symbol, start, requested_end):
            self.calls.append((start, requested_end))
            index = pd.bdate_range(start, requested_end)
            return pd.DataFrame({
                "open": 10.0, "high": 10.0, "low": 10.0,
                "close": 10.0, "volume": 2.0,
            }, index=index)

    monkeypatch.setattr(registry, "_factories", lambda: {Market.CN: [TailSource]})
    start = str(dates[0].date())
    end_value = str(end.date())
    registry.load_history("600000.SH", start, end_value, store=store)
    assert TailSource.calls == [(str(dates[-5].date()), end_value)]

    # 同一 TTL 内自动加载完全本地命中；显式“同步最新行情”仍会检查尾部。
    registry.load_history("600000.SH", start, end_value, store=store)
    assert len(TailSource.calls) == 1
    registry.load_history(
        "600000.SH", start, end_value, store=store, refresh="incremental")
    assert len(TailSource.calls) == 2


def test_concurrent_same_symbol_history_load_is_single_flight(tmp_path, monkeypatch):
    store = BarStore(root=tmp_path / "bars")

    class SlowSource(DataSource):
        name = "slow"
        markets = (Market.CN,)
        calls = 0

        def daily(self, symbol, start, end):
            type(self).calls += 1
            time.sleep(0.05)
            index = pd.bdate_range(start, end)
            return pd.DataFrame({
                "open": 10.0, "high": 10.0, "low": 10.0,
                "close": 10.0, "volume": 1.0,
            }, index=index)

    monkeypatch.setattr(registry, "_factories", lambda: {Market.CN: [SlowSource]})

    def load(_):
        return registry.load_history(
            "600000.SH", "2024-01-02", "2024-03-29", store=store)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(load, range(2)))
    assert SlowSource.calls == 1
    pd.testing.assert_frame_equal(results[0], results[1])


def test_failed_full_refresh_keeps_previous_cache(tmp_path, monkeypatch):
    store = BarStore(root=tmp_path / "bars")
    dates = pd.bdate_range("2024-01-02", "2024-03-29")
    cached = pd.DataFrame({
        "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "volume": 1.0,
    }, index=dates)
    store.put("600000.SH", cached)

    class Broken(DataSource):
        name = "broken"
        markets = (Market.CN,)

        def daily(self, symbol, start, end):
            raise ConnectionError("offline")

    monkeypatch.setattr(registry, "_factories", lambda: {Market.CN: [Broken]})
    result = registry.load_history(
        "600000.SH", "2024-01-02", "2024-03-29", store=store, refresh="full")
    pd.testing.assert_frame_equal(result, cached, check_freq=False)
    pd.testing.assert_frame_equal(store.get("600000.SH"), cached, check_freq=False)
    assert store.metadata("600000.SH")["last_status"] == "refresh_failed"


def test_successful_fallback_is_recorded_as_ready(tmp_path, monkeypatch):
    store = BarStore(root=tmp_path / "bars")

    class Broken(DataSource):
        name = "broken"
        markets = (Market.CN,)

        def daily(self, symbol, start, end):
            raise ConnectionError("offline")

    class Fallback(DataSource):
        name = "fallback"
        markets = (Market.CN,)

        def daily(self, symbol, start, end):
            index = pd.bdate_range(start, end)
            return pd.DataFrame({
                "open": 10.0, "high": 10.0, "low": 10.0,
                "close": 10.0, "volume": 1.0,
            }, index=index)

    monkeypatch.setattr(
        registry, "_factories", lambda: {Market.CN: [Broken, Fallback]})
    result = registry.load_history(
        "600000.SH", "2024-01-02", "2024-03-29", store=store)
    assert not result.empty
    meta = store.metadata("600000.SH")
    assert meta["last_status"] == "ready"
    assert meta["last_source"] == "fallback"


def test_different_provider_lanes_execute_in_parallel():
    barrier = threading.Barrier(2, timeout=2)

    def call(lane):
        return provider_call(lane, f"parallel-{lane}", lambda: barrier.wait() or lane)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(call, ["akshare:eastmoney", "yahoo"]))
    assert len(results) == 2


def test_akshare_proxy_failure_opens_circuit_before_queued_calls(isolated_config):
    isolated_config.data.akshare_retries = 1
    attempts = 0
    lock = threading.Lock()

    def broken():
        nonlocal attempts
        with lock:
            attempts += 1
        raise RuntimeError("ProxyError: proxy unavailable")

    def call(index):
        with pytest.raises(RuntimeError):
            akshare_call(f"proxy-{index}", broken)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(call, range(8)))

    assert attempts == 1
    health = PROVIDER_HEALTH.status("akshare:eastmoney")["akshare:eastmoney"]
    assert health["state"] == "open"
    assert health["suppressed"] >= 1


def test_yahoo_daily_many_uses_one_batch_and_restores_symbol_mapping(monkeypatch):
    from quantmaster.data.yfinance_source import YFinanceSource

    index = pd.bdate_range("2024-01-02", periods=3)
    columns = pd.MultiIndex.from_product([
        ["^GSPC", "^N225"], ["Open", "High", "Low", "Close", "Volume"],
    ])
    raw = pd.DataFrame(1.0, index=index, columns=columns)
    raw[("^GSPC", "Close")] = [100.0, 101.0, 102.0]
    raw[("^N225", "Close")] = [200.0, 202.0, 204.0]

    class FakeYF:
        calls: ClassVar[list[tuple[list[str], dict]]] = []

        @classmethod
        def download(cls, symbols, **kwargs):
            cls.calls.append((symbols, kwargs))
            return raw

    monkeypatch.setattr("quantmaster.data.yfinance_source._require_yfinance", lambda: FakeYF)
    monkeypatch.setattr(
        "quantmaster.data.yfinance_source.provider_call",
        lambda lane, key, func, **kwargs: func(),
    )
    result = YFinanceSource().daily_many(
        ["^GSPC.US", "^N225.JP"], "2024-01-02", "2024-01-04")

    assert len(FakeYF.calls) == 1
    assert set(FakeYF.calls[0][0]) == {"^GSPC", "^N225"}
    assert result["^GSPC.US"].iloc[-1]["close"] == 102.0
    assert result["^N225.JP"].iloc[-1]["close"] == 204.0
