from __future__ import annotations

import pandas as pd

import quantmaster.data.free_stockdb_source as free_stockdb
from quantmaster.data.free_stockdb_source import FreeStockDBSource


class _FakeReader:
    def get(self, pattern: str):
        assert pattern == "板块*"
        return self

    def do(self):
        return [
            ["板块:801120.SL", {
                "code": "801120.SL", "name": "食品饮料",
                "category": "申万一级", "symbols": ["600519", "000858"],
            }],
            ["板块:BK_AI", {
                "code": "BK_AI", "name": "人工智能",
                "category": "概念", "symbols": ["300750", "600519"],
            }],
        ]


class _FakeClient:
    rd = _FakeReader()

    def __init__(self):
        self.calls: list[dict] = []

    def get_data(self, **kwargs):
        self.calls.append(kwargs)
        stamp = "20260805100500" if kwargs["frequency"] == "5m" else "20260805"
        return [{
            "date": stamp,
            "open": 10,
            "high": 11,
            "low": 9,
            "close": 10.5,
            "volume": 100,
        }]


def _source(monkeypatch) -> tuple[FreeStockDBSource, _FakeClient]:
    monkeypatch.setattr(
        free_stockdb,
        "provider_call",
        lambda _lane, _key, function, **_kwargs: function(),
    )
    source = FreeStockDBSource()
    client = _FakeClient()
    source._sdk_checked = True
    source._client = client
    return source, client


def test_free_stockdb_sdk_supplies_daily_and_minute_bars(monkeypatch) -> None:
    source, client = _source(monkeypatch)

    daily = source.daily("600519.SH", "2026-08-05", "2026-08-05")
    minute = source.intraday(
        "600519.SH", "2026-08-05 10:00", "2026-08-05 10:10", "5m",
    )

    assert daily.loc[pd.Timestamp("2026-08-05"), "close"] == 10.5
    assert minute.loc[pd.Timestamp("2026-08-05 10:05"), "volume"] == 100
    assert client.calls[0]["fq"] == "qfq"
    assert client.calls[1]["frequency"] == "5m"
    assert client.calls[1]["fq"] is None


def test_free_stockdb_board_data_feeds_industry_and_concepts(monkeypatch) -> None:
    source, _client = _source(monkeypatch)

    assert source.industry_map()["600519.SH"] == "食品饮料"
    assert source.themes() == [{
        "code": "BK_AI",
        "name": "人工智能",
        "members": ["300750.SZ", "600519.SH"],
        "aliases": [],
        "source": "free-stockdb:concept",
    }]
