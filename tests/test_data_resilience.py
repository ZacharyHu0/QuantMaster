"""AKShare 重试与 Tushare 2000 积分档缓存/限流测试。"""

from __future__ import annotations

import multiprocessing
import os
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import ClassVar

import httpx
import pandas as pd
import pytest

from quantmaster.data import registry
from quantmaster.data.base import DataSource, Market
from quantmaster.data.resilience import (
    PROVIDER_HEALTH,
    PROVIDER_SCHEDULER,
    CircuitOpenError,
    EndpointFrameCache,
    LocalOnlyDataAccessError,
    ProviderScheduler,
    ProviderTimeoutError,
    TushareRateLimiter,
    akshare_call,
    classify_provider_failure,
    local_only_data_access,
    provider_call,
)
from quantmaster.data.storage import BarStore
from quantmaster.data.tushare_source import TushareSource, _current_session_cache_floor


def test_default_daily_bar_store_is_reused_per_root(tmp_path, monkeypatch):
    created = []

    def factory(*, root, read_only):
        store = object()
        created.append((root, read_only, store))
        return store

    config = type("Config", (), {"data_root": tmp_path})()
    registry._DEFAULT_BAR_STORES.clear()
    monkeypatch.setattr(registry, "BarStore", factory)
    monkeypatch.setattr(registry, "get_config", lambda: config)

    first = registry._default_bar_store()
    second = registry._default_bar_store()

    assert first is second
    assert created == [((tmp_path / "bars").resolve(), False, first)]
    registry._DEFAULT_BAR_STORES.clear()


def test_local_only_bar_store_does_not_initialize_a_cold_cache(tmp_path):
    root = tmp_path / "cold-bars"

    with local_only_data_access():
        store = BarStore(root=root)
        result = store.read("600000.SH")

    assert store.read_only is True
    assert result.status == "missing"
    assert not root.exists()


def test_local_only_reader_does_not_repair_or_mutate_a_bad_cache(tmp_path):
    from quantmaster.data.registry import read_history

    store = BarStore(root=tmp_path / "bars")
    dates = pd.bdate_range("2026-08-03", periods=3)
    store.put("600000.SH", pd.DataFrame({"close": [10.0, 11.0, 12.0]}, index=dates))
    path = store.path_for_repair("600000.SH")
    path.write_bytes(b"not a parquet file")
    before = store.metadata("600000.SH")

    with local_only_data_access():
        result = read_history("600000.SH", "2026-08-03", "2026-08-07", store=store)

    assert result.quality.status == "unavailable"
    assert store.metadata("600000.SH") == before


def test_local_only_context_rejects_provider_call():
    with local_only_data_access(), pytest.raises(LocalOnlyDataAccessError):
        provider_call("akshare:local-only-test", "blocked", lambda: "unexpected")


def test_local_only_context_rejects_akshare_retry_entrypoint():
    called = False

    def unexpected():
        nonlocal called
        called = True
        return "remote"

    with local_only_data_access(), pytest.raises(LocalOnlyDataAccessError):
        akshare_call("local-only-test", unexpected)

    assert called is False


def test_public_history_reader_enforces_local_only_without_http_middleware(tmp_path, monkeypatch):
    """A future helper inside the reader cannot silently restore provider I/O."""

    store = BarStore(root=tmp_path / "bars")

    def accidental_provider(*_args, **_kwargs):
        return provider_call("akshare:reader-contract", "unexpected", lambda: "remote")

    monkeypatch.setattr(registry, "_bar_envelope", accidental_provider)

    with pytest.raises(LocalOnlyDataAccessError):
        registry.read_history("600000.SH", "2026-08-03", "2026-08-07", store=store)


def test_read_panel_uses_one_bounded_batch_read_and_preserves_input_order(tmp_path, monkeypatch):
    store = BarStore(root=tmp_path / "bars")
    dates = pd.DatetimeIndex(["2026-08-03", "2026-08-04"])
    bars = pd.DataFrame({
        "open": [10.0, 11.0], "high": [11.0, 12.0],
        "low": [9.0, 10.0], "close": [10.5, 11.5],
        "volume": [100.0, 120.0],
    }, index=dates)
    store.put("000001.SZ", bars)
    store.put("600000.SH", bars * 2)
    expected = {
        symbol: registry.read_history(symbol, "2026-08-03", "2026-08-04", store=store)
        for symbol in ("000001.SZ", "600000.SH", "MISSING.SH")
    }
    store.read_only = True

    calls: list[tuple[list[str], dict]] = []
    original_read_many = store.read_many

    def read_many(symbols, *args, **kwargs):
        calls.append((list(symbols), kwargs))
        return original_read_many(symbols, *args, **kwargs)

    monkeypatch.setattr(store, "read_many", read_many)
    monkeypatch.setattr(
        store,
        "get",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("不应逐标的读取")),
    )
    progress: list[tuple[int, int, str, bool]] = []

    panel = registry.read_panel(
        ["000001.SZ", "600000.SH", "000001.SZ", "MISSING.SH"],
        "2026-08-03",
        "2026-08-04",
        field="close",
        store=store,
        progress=lambda *item: progress.append(item),
    )

    assert calls == [(
        ["000001.SZ", "600000.SH", "MISSING.SH"],
        {
            "start": "2026-08-03", "end": "2026-08-04",
            "max_workers": 8, "enqueue_repair": False,
        },
    )]
    assert list(panel.data.columns) == ["000001.SZ", "600000.SH"]
    assert [item["symbol"] for item in panel.provenance] == [
        "000001.SZ", "600000.SH", "MISSING.SH",
    ]
    assert [item["quality"] for item in panel.provenance] == [
        expected[symbol].quality.to_dict()
        for symbol in ("000001.SZ", "600000.SH", "MISSING.SH")
    ]
    assert progress == [
        (1, 3, "000001.SZ", True),
        (2, 3, "600000.SH", True),
        (3, 3, "MISSING.SH", False),
    ]


def _hold_cross_process_bar_lock(root: str, start, events) -> None:
    store = BarStore(Path(root))
    start.wait(10)
    with store.lock("600000.SH"):
        events.put(("enter", os.getpid(), time.monotonic()))
        time.sleep(0.25)
        events.put(("exit", os.getpid(), time.monotonic()))


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


def test_online_provider_switches_block_direct_calls(isolated_config):
    isolated_config.data.akshare_enabled = False
    isolated_config.data.tushare_enabled = False
    isolated_config.data.yfinance_enabled = False
    isolated_config.data.free_stockdb_online_enabled = False

    with pytest.raises(LocalOnlyDataAccessError, match="akshare"):
        akshare_call("disabled", lambda: "unexpected")
    for lane in ("tushare:daily", "yahoo:daily", "free-stockdb-online"):
        with pytest.raises(LocalOnlyDataAccessError, match="已在设置中关闭"):
            provider_call(lane, "disabled", lambda: "unexpected")


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


def test_tushare_permission_gated_endpoint_can_use_an_isolated_health_lane(
    tmp_path, isolated_config, monkeypatch,
):
    isolated_config.data.tushare_token = "test-token"
    monkeypatch.setattr("quantmaster.data.tushare_source.TUSHARE_LIMITER.wait", lambda: None)
    lanes = []

    def direct_call(lane, key, fetch):
        lanes.append(lane)
        return fetch()

    monkeypatch.setattr("quantmaster.data.tushare_source.provider_call", direct_call)

    class FakePro:
        def dc_index(self, **params):
            return pd.DataFrame([{
                "ts_code": "BK1184.DC", "trade_date": params["trade_date"],
                "name": "人形机器人",
            }])

    source = TushareSource(EndpointFrameCache("tushare", root=tmp_path / "cache"))
    source._api = FakePro()
    frame = source._call(
        "dc_index",
        1,
        provider_lane="tushare:dc-concept",
        trade_date="20260730",
    )

    assert len(frame) == 1
    assert lanes == ["tushare:dc-concept"]


def test_tushare_default_health_lane_is_isolated_per_endpoint(
    tmp_path, isolated_config, monkeypatch,
):
    isolated_config.data.tushare_token = "test-token"
    monkeypatch.setattr("quantmaster.data.tushare_source.TUSHARE_LIMITER.wait", lambda: None)
    lanes = []

    def direct_call(lane, key, fetch, **kwargs):
        lanes.append(lane)
        return fetch()

    monkeypatch.setattr("quantmaster.data.tushare_source.provider_call", direct_call)

    class FakePro:
        def fund_basic(self, **params):
            return pd.DataFrame([{"ts_code": "510300.SH", "name": "沪深300ETF"}])

    source = TushareSource(EndpointFrameCache("tushare", root=tmp_path / "cache"))
    source._api = FakePro()

    frame = source._call("fund_basic", 1, market="E")

    assert len(frame) == 1
    assert lanes == ["tushare:fund_basic"]


def test_tushare_required_nonempty_ignores_poisoned_empty_cache(
    tmp_path, isolated_config, monkeypatch,
):
    isolated_config.data.tushare_token = "test-token"
    monkeypatch.setattr("quantmaster.data.tushare_source.TUSHARE_LIMITER.wait", lambda: None)
    monkeypatch.setattr(
        "quantmaster.data.tushare_source.provider_call",
        lambda lane, key, fetch, **kwargs: fetch(),
    )
    cache = EndpointFrameCache("tushare", root=tmp_path / "cache")
    params = {
        "trade_date": "20250529",
        "fields": "ts_code,trade_date,open,high,low,close,vol,amount",
    }
    cache.put("daily", params, pd.DataFrame())

    class FakePro:
        calls = 0

        def daily(self, **kwargs):
            self.calls += 1
            return pd.DataFrame([{
                "ts_code": "600000.SH", "trade_date": kwargs["trade_date"],
                "open": 10.0, "high": 10.2, "low": 9.9, "close": 10.1,
                "vol": 100, "amount": 1000,
            }])

    source = TushareSource(cache)
    source._api = FakePro()
    result = source._call(
        "daily", 1, required_nonempty=True,
        required_columns=("ts_code", "trade_date"), **params,
    )

    assert len(result) == 1
    assert source._api.calls == 1
    assert len(pd.read_parquet(cache.path_for("daily", params))) == 1


def test_current_session_rejects_endpoint_cache_written_before_close():
    before_close = pd.Timestamp("2026-07-28 14:00", tz="Asia/Shanghai").to_pydatetime()
    after_close = pd.Timestamp("2026-07-28 16:00", tz="Asia/Shanghai").to_pydatetime()
    assert _current_session_cache_floor("20260728", now=before_close) is None
    assert _current_session_cache_floor(
        "20260728", now=after_close,
    ) == pd.Timestamp("2026-07-28 15:30", tz="Asia/Shanghai").timestamp()


def test_incremental_refresh_bypasses_cached_tushare_tail(
    tmp_path, isolated_config, monkeypatch,
):
    isolated_config.data.tushare_token = "test-token"
    monkeypatch.setattr("quantmaster.data.tushare_source.TUSHARE_LIMITER.wait", lambda: None)

    class FakePro:
        include_latest = False
        calls: ClassVar[list[dict]] = []

        def index_daily(self, **params):
            self.calls.append(params)
            dates = ["20240102", "20240103"] if self.include_latest else ["20240102"]
            return pd.DataFrame({
                "ts_code": ["000300.SH"] * len(dates), "trade_date": dates,
                "open": [3000.0] * len(dates), "high": [3010.0] * len(dates),
                "low": [2990.0] * len(dates), "close": [3005.0] * len(dates),
                "vol": [100.0] * len(dates), "amount": [20.0] * len(dates),
            })

    api = FakePro()
    cache = EndpointFrameCache("tushare", root=tmp_path / "api-cache")
    warm = TushareSource(cache)
    warm._api = api
    old = warm.daily("000300.SH", "2024-01-02", "2024-01-03")
    assert str(old.index.max().date()) == "2024-01-02"

    store = BarStore(root=tmp_path / "bars")
    store.put("000300.SH", old)
    api.include_latest = True

    class FakeTushare(TushareSource):
        def __init__(self):
            super().__init__(cache)
            self._api = api

    monkeypatch.setattr(registry, "_factories", lambda: {Market.CN: [FakeTushare]})
    refreshed = registry.refresh_history(
        "000300.SH", "2024-01-02", "2024-01-03", store=store,
        mode="incremental",
    )

    assert str(refreshed.data.index.max().date()) == "2024-01-03"
    assert len(api.calls) == 2
    assert api.calls[-1]["start_date"] == "20240102"
    assert api.calls[-1]["end_date"] == "20240103"


def test_incremental_tail_tries_fallback_when_primary_has_no_new_date(
    tmp_path, monkeypatch,
):
    store = BarStore(root=tmp_path / "bars")
    old_date = pd.DatetimeIndex(["2024-01-02"])
    old = pd.DataFrame({
        "open": [10.0], "high": [10.0], "low": [10.0], "close": [10.0],
        "volume": [1.0],
    }, index=old_date)
    store.put("600000.SH", old)

    class Lagging(DataSource):
        name = "lagging"
        markets = (Market.CN,)
        calls = 0

        def daily(self, symbol, start, end):
            type(self).calls += 1
            return old

    class Current(DataSource):
        name = "current"
        markets = (Market.CN,)
        calls = 0

        def daily(self, symbol, start, end):
            type(self).calls += 1
            frame = pd.concat([old, old.rename(index={old.index[0]: pd.Timestamp("2024-01-03")})])
            return frame

    monkeypatch.setattr(
        registry, "_factories", lambda: {Market.CN: [Lagging, Current]})
    refreshed = registry.refresh_history(
        "600000.SH", "2024-01-02", "2024-01-03", store=store,
        mode="incremental",
    )

    assert str(refreshed.data.index.max().date()) == "2024-01-03"
    assert Lagging.calls == Current.calls == 1
    assert store.metadata("600000.SH")["last_source"] == "current"


def test_incremental_alignment_accepts_one_revised_overlap_day():
    dates = pd.bdate_range("2026-07-21", periods=5)
    cached = pd.DataFrame({
        "open": [100.0] * 5, "high": [100.0] * 5, "low": [100.0] * 5,
        "close": [100.0, 100.0, 100.0, 100.0, 99.0], "volume": [1.0] * 5,
    }, index=dates)
    fresh_dates = dates.append(pd.DatetimeIndex(["2026-07-28"]))
    fresh = pd.DataFrame({
        "open": [100.0] * 6, "high": [100.0] * 6, "low": [100.0] * 6,
        "close": [100.0] * 6, "volume": [2.0] * 6,
    }, index=fresh_dates)

    merged = registry._align_increment(cached, fresh, "right")
    assert merged.loc[dates[-1], "close"] == pytest.approx(100.0)
    assert merged.loc["2026-07-28", "close"] == pytest.approx(100.0)


def test_cn_early_check_becomes_stale_after_close():
    cached = pd.DataFrame(
        {"close": [10.0]}, index=pd.DatetimeIndex(["2026-07-27"]))
    checked_at = pd.Timestamp("2026-07-28 10:00", tz="Asia/Shanghai").timestamp()
    assert registry._session_refresh_due(
        "600000.SH", pd.Timestamp("2026-07-28"), cached, checked_at,
        now=pd.Timestamp("2026-07-28 16:00", tz="Asia/Shanghai"),
    )
    assert not registry._session_refresh_due(
        "600000.SH", pd.Timestamp("2026-07-28"), cached, checked_at,
        now=pd.Timestamp("2026-07-28 14:00", tz="Asia/Shanghai"),
    )


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
        def stock_zh_valuation_baidu(**params):
            raise ConnectionError("akshare down")

    expected = pd.DataFrame(
        {"pe_ttm": [12.0]}, index=pd.DatetimeIndex(["2024-01-02"], name="date"))
    monkeypatch.setattr(fundamentals, "_require_akshare", lambda: BrokenAkshare())
    monkeypatch.setattr(TushareSource, "daily_indicators", lambda self, symbol: expected)
    pd.testing.assert_frame_equal(fundamentals.fetch_daily_indicators("600000.SH"), expected)


def test_fundamentals_reuse_tushare_disk_cache_before_any_api(isolated_config, monkeypatch):
    """规范化基本面缓存缺失时，已有 Tushare 接口缓存仍优先于 AKShare 请求。"""
    from quantmaster.data import fundamentals

    expected = pd.DataFrame(
        {"dv_ratio": [3.2]}, index=pd.DatetimeIndex(["2024-01-02"], name="date"))
    monkeypatch.setattr(
        TushareSource,
        "cached_daily_indicators",
        lambda self, symbol, start=None, end=None: expected,
    )
    monkeypatch.setattr(
        fundamentals,
        "_require_akshare",
        lambda: (_ for _ in ()).throw(AssertionError("命中本地接口缓存后不应访问 AKShare")),
    )

    result = fundamentals.fetch_daily_indicators(
        "600000.SH", start="2024-01-01", end="2024-01-31",
    )
    pd.testing.assert_frame_equal(result, expected)


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
    result = registry.refresh_history(
        "600000.SH", "2024-01-02", "2024-06-28", store=store)
    assert FakeSource.calls == 1
    assert str(result.data.index.max().date()) == "2024-06-28"


def test_bar_store_recovers_file_after_metadata_commit_failure(tmp_path, monkeypatch):
    root = tmp_path / "bars"
    store = BarStore(root=root)
    old = pd.DataFrame({"close": [10.0]}, index=pd.to_datetime(["2024-01-02"]))
    new = pd.DataFrame({"close": [11.0]}, index=pd.to_datetime(["2024-01-03"]))
    store.put("600000.SH", old)
    original_commit = store._commit_metadata

    def fail_commit(connection, metadata, *, clear_intent):
        if clear_intent and metadata["end"] == "2024-01-03":
            raise sqlite3.OperationalError("injected catalog failure")
        return original_commit(connection, metadata, clear_intent=clear_intent)

    monkeypatch.setattr(store, "_commit_metadata", fail_commit)
    with pytest.raises(sqlite3.OperationalError, match="injected"):
        store.put("600000.SH", new, replace=True)

    recovered = BarStore(root=root)
    pd.testing.assert_frame_equal(recovered.get("600000.SH"), new, check_freq=False)
    assert recovered.metadata("600000.SH")["content_sha256"]
    with recovered._conn() as connection:
        assert connection.execute("SELECT COUNT(*) FROM bar_write_intents").fetchone()[0] == 0
    assert not list(root.glob("*.bak"))


def test_bar_store_rejects_readable_content_that_changed_outside_commit(tmp_path):
    store = BarStore(root=tmp_path / "bars")
    original = pd.DataFrame({"close": [10.0]}, index=pd.to_datetime(["2024-01-02"]))
    store.put("600000.SH", original)
    pd.DataFrame(
        {"close": [99.0]}, index=pd.to_datetime(["2024-01-02"]),
    ).to_parquet(store._path("600000.SH"))

    assert store.get("600000.SH") is None
    assert store.metadata("600000.SH")["last_status"] == "corrupt"


def test_bar_store_suppresses_repeated_missing_file_error_logs(tmp_path, caplog):
    store = BarStore(root=tmp_path / "bars")
    store.put(
        "HG=F.US",
        pd.DataFrame({"close": [4.2]}, index=pd.to_datetime(["2026-08-07"])),
    )
    store._path("HG=F.US").unlink()

    with caplog.at_level("DEBUG", logger="quantmaster.data.storage"):
        store.read("HG=F.US", enqueue_repair=False)
        store.read("HG=F.US", enqueue_repair=False)
        store.read("HG=F.US", enqueue_repair=False)

    errors = [record for record in caplog.records if record.levelname == "ERROR"]
    suppressed = [record for record in caplog.records if "suppressed" in record.message]
    assert len(errors) == 1
    assert len(suppressed) == 2


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
    result = registry.refresh_history("600000.SH", "2024-01-02", "2024-03-29", store=store)
    pd.testing.assert_frame_equal(result.data, frame, check_freq=False)


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
    registry.refresh_history("600000.SH", start, end_value, store=store)
    assert TailSource.calls == [(str(dates[-5].date()), end_value)]

    # 同一 TTL 内自动加载完全本地命中；显式“同步最新行情”仍会检查尾部。
    registry.refresh_history("600000.SH", start, end_value, store=store)
    assert len(TailSource.calls) == 1
    registry.refresh_history(
        "600000.SH", start, end_value, store=store, mode="incremental")
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
        return registry.refresh_history(
            "600000.SH", "2024-01-02", "2024-03-29", store=store)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(load, range(2)))
    assert SlowSource.calls == 1
    pd.testing.assert_frame_equal(results[0].data, results[1].data)


def test_same_symbol_lock_serializes_spawned_processes(tmp_path):
    context = multiprocessing.get_context("spawn")
    start = context.Event()
    events = context.Queue()
    processes = [
        context.Process(
            target=_hold_cross_process_bar_lock,
            args=(str(tmp_path / "bars"), start, events),
        )
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    start.set()
    records = [events.get(timeout=15) for _ in range(4)]
    for process in processes:
        process.join(timeout=15)
        assert process.exitcode == 0

    by_process = {}
    for kind, process_id, timestamp in records:
        by_process.setdefault(process_id, {})[kind] = timestamp
    assert len(by_process) == 2
    intervals = sorted(
        (value["enter"], value["exit"]) for value in by_process.values()
    )
    assert intervals[1][0] >= intervals[0][1] - 0.02


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
    result = registry.refresh_history(
        "600000.SH", "2024-01-02", "2024-03-29", store=store, mode="full")
    pd.testing.assert_frame_equal(result.data, cached, check_freq=False)
    pd.testing.assert_frame_equal(store.get("600000.SH"), cached, check_freq=False)
    assert store.metadata("600000.SH")["last_status"] == "refresh_failed"


@pytest.mark.parametrize(
    "failure",
    [
        httpx.ConnectError("refused"),
        httpx.ReadTimeout("timed out"),
        httpx.HTTPStatusError(
            "bad gateway",
            request=httpx.Request("GET", "http://stockdb.invalid"),
            response=httpx.Response(502),
        ),
    ],
)
def test_full_refresh_httpx_failures_continue_to_fallback(
    tmp_path, monkeypatch, failure,
):
    store = BarStore(root=tmp_path / "bars")

    class Broken(DataSource):
        name = "broken"
        markets = (Market.CN,)

        def daily(self, symbol, start, end):
            raise failure

    class Fallback(DataSource):
        name = "fallback"
        markets = (Market.CN,)

        def daily(self, symbol, start, end):
            index = pd.bdate_range(start, end)
            return pd.DataFrame({
                "open": 10.0, "high": 10.0, "low": 10.0,
                "close": 10.0, "volume": 1.0,
            }, index=index)

    monkeypatch.setattr(registry, "_factories", lambda: {
        Market.CN: [Broken, Fallback],
    })
    result = registry.refresh_history(
        "600000.SH", "2024-01-02", "2024-01-12",
        store=store, mode="full",
    )

    assert not result.data.empty
    assert store.metadata("600000.SH")["last_source"] == "fallback"


def test_daily_panel_primes_uncached_symbols_with_one_local_batch(
    tmp_path, monkeypatch,
):
    from quantmaster.data.free_stockdb_source import FreeStockDBSource

    store = BarStore(root=tmp_path / "bars")
    calls: list[list[str]] = []

    def bars(start: str, end: str) -> pd.DataFrame:
        index = pd.bdate_range(start, end)
        return pd.DataFrame({
            "open": 10.0, "high": 10.0, "low": 10.0,
            "close": 10.0, "volume": 1.0,
        }, index=index)

    monkeypatch.setattr(registry, "BarStore", lambda *, root, read_only=False: store)
    monkeypatch.setattr(FreeStockDBSource, "native_batch_available", lambda _self: True)

    def daily_many(_self, symbols, start, end):
        calls.append(list(symbols))
        return {symbols[0]: bars(start, end)}

    monkeypatch.setattr(FreeStockDBSource, "daily_many", daily_many)
    monkeypatch.setattr(
        registry,
        "_local_sessions",
        lambda start, end: (pd.bdate_range(start, end), "tushare:SSE"),
    )

    class Fallback(DataSource):
        name = "fallback"
        markets = (Market.CN,)

        def daily(self, symbol, start, end):
            return bars(start, end)

    monkeypatch.setattr(registry, "_factories", lambda: {Market.CN: [Fallback]})
    panel = registry.refresh_bar_panel(
        ["600000.SH", "000001.SZ"], "2024-01-02", "2024-01-12",
        field="close",
    )

    assert calls == [["600000.SH", "000001.SZ"]]
    assert list(panel.data.columns) == ["600000.SH", "000001.SZ"]
    # A complete native StockDB batch is published as local evidence.  The
    # panel path must not contact Tushare merely to decorate its lineage.
    assert store.metadata("600000.SH")["last_source"] == "free-stockdb"
    assert store.metadata("000001.SZ")["last_source"] == "fallback"


def test_successful_fallback_without_truth_contract_is_recorded_as_degraded(
    tmp_path, monkeypatch,
):
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
    result = registry.refresh_history(
        "600000.SH", "2024-01-02", "2024-03-29", store=store)
    assert not result.data.empty
    meta = store.metadata("600000.SH")
    assert result.quality.status == "degraded"
    assert meta["last_status"] == "degraded"
    assert meta["last_source"] == "fallback"


def test_different_provider_lanes_execute_in_parallel():
    barrier = threading.Barrier(2, timeout=2)

    def call(lane):
        return provider_call(lane, f"parallel-{lane}", lambda: barrier.wait() or lane)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(call, ["akshare:eastmoney", "yahoo"]))
    assert len(results) == 2


def test_provider_scheduler_enforces_global_network_concurrency_ceiling():
    scheduler = ProviderScheduler()
    release = threading.Event()
    eight_entered = threading.Event()
    active = 0
    peak = 0
    lock = threading.Lock()

    def blocked(index: int) -> int:
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
            if active == scheduler.MAX_NETWORK_CONCURRENCY:
                eight_entered.set()
        try:
            assert release.wait(2.0)
            return index
        finally:
            with lock:
                active -= 1

    with ThreadPoolExecutor(max_workers=scheduler.MAX_NETWORK_CONCURRENCY + 1) as pool:
        lanes = [
            "tushare:global-limit-0",
            "tushare:global-limit-1",
            "akshare:global-limit-0",
            "yahoo:global-limit-1",
            "free-stockdb:global-limit-0",
            "free-stockdb:global-limit-1",
            "free-stockdb:global-limit-2",
            "free-stockdb:global-limit-3",
            "free-stockdb:global-limit-4",
        ]
        pending = [
            pool.submit(
                scheduler.call,
                lanes[index],
                str(index),
                lambda index=index: blocked(index),
                timeout=2.0,
            )
            for index in range(scheduler.MAX_NETWORK_CONCURRENCY + 1)
        ]
        assert eight_entered.wait(1.0)
        time.sleep(0.05)
        assert peak == scheduler.MAX_NETWORK_CONCURRENCY
        assert scheduler.status()["network_active"] == scheduler.MAX_NETWORK_CONCURRENCY
        release.set()
        assert sorted(item.result(timeout=2.0) for item in pending) == list(
            range(scheduler.MAX_NETWORK_CONCURRENCY + 1)
        )


def test_provider_scheduler_uses_fixed_provider_family_pools():
    scheduler = ProviderScheduler()
    lanes = ["tushare:daily", "akshare:daily", "free-stockdb:daily", "yahoo:daily"]

    for index, lane in enumerate(lanes):
        assert scheduler.call(lane, str(index), lambda index=index: index) == index

    assert set(scheduler._queues) == {"tushare", "external", "stockdb"}
    assert sum(scheduler.FAMILY_WORKERS.values()) == scheduler.MAX_NETWORK_CONCURRENCY
    assert scheduler._lanes == set(lanes)


def test_provider_timeout_opens_circuit_and_fences_late_success(isolated_config):
    isolated_config.data.provider_timeout = 0.2
    lane = "akshare:timeout-fence-test"
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()

    def slow_provider():
        started.set()
        try:
            release.wait(2)
            return "late-success"
        finally:
            finished.set()

    began = time.monotonic()
    with pytest.raises(ProviderTimeoutError, match=r"0\.2 秒"):
        provider_call(lane, "slow", slow_provider)
    assert started.is_set()
    assert time.monotonic() - began < 1
    assert PROVIDER_HEALTH.status(lane)[lane]["state"] == "open"
    scheduler = PROVIDER_SCHEDULER.status()
    assert scheduler["lanes"][lane]["timeout_count"] == 1
    assert scheduler["lanes"][lane]["expired"] == 1

    with pytest.raises(CircuitOpenError):
        provider_call(lane, "must-fail-fast", lambda: pytest.fail("熔断期间不得再次触网"))

    release.set()
    assert finished.wait(1)
    for _ in range(20):
        if PROVIDER_SCHEDULER.status()["lanes"][lane]["active"] == 0:
            break
        time.sleep(0.01)
    assert PROVIDER_HEALTH.status(lane)[lane]["state"] == "open"


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


def test_permanent_provider_failure_stays_disabled_until_config_changes(isolated_config):
    isolated_config.data.tushare_token = "old-token"
    lane = "tushare:dc-permission-test"
    calls = 0

    def denied():
        nonlocal calls
        calls += 1
        raise RuntimeError("抱歉，您没有权限访问该接口")

    with pytest.raises(RuntimeError):
        provider_call(lane, "permission", denied)
    health = PROVIDER_HEALTH.status(lane)[lane]
    assert health["state"] == "disabled"
    assert health["failure_class"] == "permission"

    with pytest.raises(CircuitOpenError):
        provider_call(lane, "blocked", denied)
    assert calls == 1

    isolated_config.data.tushare_token = "new-token"
    assert provider_call(lane, "reconfigured", lambda: "ok") == "ok"
    assert PROVIDER_HEALTH.status(lane)[lane]["state"] == "closed"


def test_disabled_status_is_read_only_and_allows_a_changed_credential(isolated_config):
    isolated_config.data.tushare_token = "old-token"
    lane = "tushare:optional-permission-test"
    PROVIDER_HEALTH.failure(lane, RuntimeError("permission denied"), immediate=True)

    before = PROVIDER_HEALTH.status(lane)[lane]
    disabled = PROVIDER_HEALTH.disabled_status(lane)
    after = PROVIDER_HEALTH.status(lane)[lane]

    assert disabled is not None
    assert disabled["failure_class"] == "permission"
    assert after["suppressed"] == before["suppressed"] == 0

    isolated_config.data.tushare_token = "new-token"
    assert PROVIDER_HEALTH.disabled_status(lane) is None


def test_source_health_removes_obsolete_tushare_ths_lane():
    lane = "tushare:ths-concept"
    with PROVIDER_HEALTH._conn() as connection:
        connection.execute(
            "INSERT OR REPLACE INTO source_health"
            "(lane,state,last_error,failure_class,config_revision) "
            "VALUES (?,'disabled','ths_index permission denied','permission','old')",
            (lane,),
        )
        connection.execute("PRAGMA user_version=3")

    assert lane not in PROVIDER_HEALTH.status()
    with PROVIDER_HEALTH._conn() as connection:
        assert int(connection.execute("PRAGMA user_version").fetchone()[0]) == 4


def test_rate_limit_opens_recoverable_circuit(isolated_config):
    lane = "yahoo:rate-limit-test"
    response = httpx.Response(
        429, request=httpx.Request("GET", "https://example.test/chart"),
    )

    def rate_limited():
        response.raise_for_status()

    with pytest.raises(httpx.HTTPStatusError):
        provider_call(lane, "rate-limit", rate_limited)

    health = PROVIDER_HEALTH.status(lane)[lane]
    assert health["state"] == "open"
    assert health["failure_class"] == "rate_limit"
    assert health["permanent"] is False


def test_retry_after_is_persisted_as_the_provider_recovery_deadline(isolated_config):
    lane = "yahoo:retry-after-test"
    response = httpx.Response(
        429, headers={"Retry-After": "75"},
        request=httpx.Request("GET", "https://example.test/chart"),
    )
    before = time.time()

    with pytest.raises(httpx.HTTPStatusError):
        provider_call(lane, "retry-after", response.raise_for_status)

    health = PROVIDER_HEALTH.status(lane)[lane]
    assert health["failure_class"] == "rate_limit"
    assert health["diagnostic_code"] == "http_429"
    assert health["retry_after_at"] >= before + 74
    assert health["next_probe_at"] >= health["retry_after_at"]


def test_http_401_disables_without_retry_and_explains_remediation(isolated_config):
    lane = "tushare:http-auth-test"
    calls = 0
    response = httpx.Response(
        401, request=httpx.Request("GET", "https://example.test/pro"),
    )

    def unauthorized():
        nonlocal calls
        calls += 1
        response.raise_for_status()

    with pytest.raises(httpx.HTTPStatusError):
        provider_call(lane, "unauthorized", unauthorized)
    with pytest.raises(CircuitOpenError, match=r"HTTP 401.*令牌"):
        provider_call(lane, "blocked", unauthorized)

    assert calls == 1
    assert PROVIDER_HEALTH.status(lane)[lane]["failure_class"] == "http_401_authentication"


def test_transient_5xx_uses_bounded_shared_provider_backoff(isolated_config, monkeypatch):
    isolated_config.data.provider_retry_attempts = 3
    isolated_config.data.provider_retry_backoff = 0.25
    isolated_config.data.provider_retry_max_backoff = 0.3
    sleeps: list[float] = []
    calls = 0
    response = httpx.Response(
        503, request=httpx.Request("GET", "https://example.test/upstream"),
    )

    def flaky():
        nonlocal calls
        calls += 1
        if calls < 3:
            response.raise_for_status()
        return "ok"

    monkeypatch.setattr("quantmaster.data.resilience.time.sleep", sleeps.append)
    assert provider_call("yahoo:5xx-backoff", "retry", flaky) == "ok"
    assert calls == 3
    assert sleeps == [0.25, 0.3]


def test_windows_socket_access_error_is_transient_network(isolated_config):
    error = OSError(10013, "以一种访问权限不允许的方式做了一个访问套接字的尝试。")
    error.winerror = 10013
    assert classify_provider_failure(error) == "transient_network"

    lane = "akshare:windows-socket-test"
    with pytest.raises(OSError):
        provider_call(lane, "socket-access", lambda: (_ for _ in ()).throw(error))

    health = PROVIDER_HEALTH.status(lane)[lane]
    assert health["state"] == "closed"
    assert health["failure_class"] == "transient_network"
    assert health["permanent"] is False


def test_expired_half_open_probe_is_reclaimable(isolated_config):
    lane = "akshare:stale-probe-test"
    with PROVIDER_HEALTH._conn() as connection:
        connection.execute(
            "INSERT OR REPLACE INTO source_health"
            "(lane,state,failures,open_count,open_until,last_failure,last_success,last_error,"
            "suppressed,failure_class,config_revision,probe_started) "
            "VALUES (?,'half_open',2,1,0,0,0,'offline',0,'transient_network','',0)",
            (lane,),
        )
    assert PROVIDER_HEALTH.status(lane)[lane]["state"] == "open"
    assert provider_call(lane, "recovery", lambda: "restored") == "restored"


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
