"""Opt-in contract checks against the user-managed real free-stockdb runtime."""

from __future__ import annotations

import asyncio
import os

import pandas as pd
import pytest

from quantmaster.data.free_stockdb_ingest import StockDBIngestService
from quantmaster.data.free_stockdb_source import FreeStockDBSource

pytestmark = pytest.mark.skipif(
    os.getenv("QM_RUN_STOCKDB_CONTRACT") != "1",
    reason="set QM_RUN_STOCKDB_CONTRACT=1 to validate the real free-stockdb runtime",
)


@pytest.fixture(scope="module")
def source() -> FreeStockDBSource:
    value = FreeStockDBSource()
    if not value.native_batch_available():
        pytest.skip("free-stockdb SDK/runtime is not installed or connected")
    return value


def test_real_runtime_daily_shapes_order_and_adjustment_boundary(source: FreeStockDBSource) -> None:
    symbols = ["600000.SH", "000001.SZ"]
    frame = source.daily_cross_section(symbols, "2025-01-01", "2026-08-08")
    assert not frame.empty
    assert {"symbol", "date", "open", "high", "low", "close", "volume"}.issubset(frame)
    assert frame.equals(frame.sort_values(["symbol", "date"]).reset_index(drop=True))
    assert not frame.duplicated(["symbol", "date"]).any()
    factors = source.adjustment_factors(symbols, "2025-01-01", "2026-08-08")
    assert factors.empty or {"symbol", "date", "adj_factor"}.issubset(factors)


def test_real_runtime_board_levels_and_chinese_http_contract(source: FreeStockDBSource) -> None:
    boards = source.board_hierarchy()
    assert boards
    assert "L1" in {item.get("level") for item in boards}
    assert all(isinstance(item.get("members"), list) for item in boards)


def test_real_runtime_cn_minutes_do_not_cross_lunch(source: FreeStockDBSource) -> None:
    minute = source.intraday("510300.SH", "2026-08-07 09:30", "2026-08-07 15:00", "1m")
    if minute.empty:
        pytest.skip("selected completed session has no ETF minute coverage")
    aggregated = source.intraday("510300.SH", "2026-08-07 09:30", "2026-08-07 15:00", "60m")
    assert all(not (stamp.hour == 12) for stamp in pd.DatetimeIndex(aggregated.index))
    assert len(aggregated) <= 4


def test_real_runtime_public_native_indicator_api_and_large_batch(source: FreeStockDBSource) -> None:
    catalog = source.security_catalog()
    symbols = [str(item.get("symbol") or item.get("ts_code") or "") for item in catalog]
    symbols = [item for item in symbols if item][:300]
    if symbols:
        frame = source.daily_cross_section(symbols, "2026-07-01", "2026-08-08")
        assert isinstance(frame, pd.DataFrame)
    payload = source.native_indicators(
        ["MA", "EMA", "MACD", "RSI", "ATR", "BOLL"],
        ["600000.SH"],
        "2025-01-01",
        "2026-08-08",
    )
    assert payload is not None


def test_real_runtime_week_month_and_async_shapes(source: FreeStockDBSource) -> None:
    client = source._sdk_client()
    arguments = {
        "code": "600000",
        "start": "20260101",
        "end": "20260808",
        "fq": "qfq",
    }
    weekly = client.get_data(**arguments, frequency="1w")
    monthly = client.get_data(**arguments, frequency="1M")
    weekly_async = asyncio.run(client.get_data_async(**arguments, frequency="1w"))
    assert isinstance(weekly, list) and isinstance(monthly, list)
    assert weekly_async == weekly
    assert all(weekly[index]["date"] < weekly[index + 1]["date"] for index in range(len(weekly) - 1))
    assert all(monthly[index]["date"] < monthly[index + 1]["date"] for index in range(len(monthly) - 1))


def test_real_runtime_large_batch_can_cancel_between_fragments(source: FreeStockDBSource) -> None:
    service = StockDBIngestService(source)
    checks = iter((False, True))
    with pytest.raises(InterruptedError, match="已取消"):
        service._read_batches(
            ["600000.SH"] * 301,
            "2026-07-01",
            "2026-08-08",
            progress=lambda *_args: None,
            cancelled=lambda: next(checks, True),
        )
