from __future__ import annotations

import pandas as pd

import quantmaster.data.free_stockdb_source as free_stockdb
from quantmaster.data.free_stockdb_source import FreeStockDBOnlineSource, FreeStockDBSource


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


def test_free_stockdb_cross_section_discloses_optional_field_coverage(monkeypatch) -> None:
    source, client = _source(monkeypatch)

    frame = source.daily_cross_section(
        ["600519.SH"], "2026-08-05", "2026-08-05",
    )

    assert frame.loc[0, "symbol"] == "600519.SH"
    assert pd.isna(frame.loc[0, "pe_ttm"])
    assert "pe_ttm" in client.calls[0]["fields"]
    assert source.board_hierarchy()[0]["level"] == "L1"


def test_free_stockdb_cross_section_decodes_positional_sdk_rows(monkeypatch) -> None:
    source, client = _source(monkeypatch)

    def positional(**kwargs):
        client.calls.append(kwargs)
        fields = kwargs["fields"].split(",")
        values = {
            "date": 20260806, "open": 10, "high": 11, "low": 9,
            "close": 10.5, "volume": 100, "amount": 1_000_000,
            "float_mv": 2_000_000, "total_mv": 3_000_000,
            "pe_ttm": 20, "pb": 2, "is_st": False,
        }
        return {
            code: [[values[field] for field in fields]]
            for code in kwargs["code"]
        }

    client.get_data = positional
    frame = source.daily_cross_section(
        ["600519.SH", "000001.SZ"], "2026-08-06", "2026-08-06",
    )

    assert frame.shape == (2, 13)
    assert frame["symbol"].tolist() == ["000001.SZ", "600519.SH"]
    assert frame["amount"].tolist() == [1_000_000, 1_000_000]
    assert frame["is_st"].tolist() == [False, False]


def test_online_fallback_has_an_independent_single_worker_lane(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        free_stockdb,
        "provider_call",
        lambda lane, _key, function, **_kwargs: calls.append(lane) or function(),
    )
    source = FreeStockDBOnlineSource()
    source._sdk_checked = True
    source._client = _FakeClient()

    result = source.daily("600519.SH", "2026-08-05", "2026-08-05")

    assert not result.empty
    assert calls == ["free-stockdb-online"]


def test_cn_source_order_uses_online_only_after_local(monkeypatch) -> None:
    from quantmaster.config import get_config
    from quantmaster.data.base import Market
    from quantmaster.data.registry import _factories

    monkeypatch.setattr(get_config().data, "free_stockdb_online_enabled", True)
    names = [source.name for source in _factories()[Market.CN]]

    assert names[:4] == [
        "free-stockdb", "free-stockdb-online", "akshare", "tushare",
    ]


def test_sdk_path_auto_discovers_managed_pybao_and_explicit_path_wins(
    tmp_path, monkeypatch,
) -> None:
    from quantmaster.config import get_config

    managed = tmp_path / "managed"
    managed_sdk = managed / "pybao" / "stock_sdk.py"
    managed_sdk.parent.mkdir(parents=True)
    managed_sdk.write_text("# managed", encoding="utf-8")
    explicit = tmp_path / "custom" / "stock_sdk.py"
    explicit.parent.mkdir()
    explicit.write_text("# explicit", encoding="utf-8")
    monkeypatch.setattr(get_config().data, "free_stockdb_root", str(managed))
    monkeypatch.setattr(get_config().data, "free_stockdb_sdk_path", "")

    assert free_stockdb.resolve_free_stockdb_sdk_path() == managed_sdk
    assert free_stockdb.resolve_free_stockdb_sdk_path(str(explicit)) == explicit


def test_free_stockdb_daily_many_uses_one_native_batch_call(monkeypatch) -> None:
    source, client = _source(monkeypatch)

    def batch(**kwargs):
        client.calls.append(kwargs)
        return {
            code: [{
                "date": "20260805", "open": 10, "high": 11, "low": 9,
                "close": 10.5, "volume": 100,
            }]
            for code in kwargs["code"]
            if code != "000858"
        }

    client.get_data = batch
    result = source.daily_many(
        ["600519.SH", "000858.SZ"], "2026-08-05", "2026-08-05",
    )

    assert list(result) == ["600519.SH"]
    assert client.calls[0]["code"] == ["600519", "000858"]


def test_online_source_is_filtered_outside_interactive_requests(monkeypatch) -> None:
    from quantmaster.config import get_config
    from quantmaster.data.base import Market
    from quantmaster.data.registry import _request_factories

    monkeypatch.setattr(get_config().data, "free_stockdb_online_enabled", True)
    normal = [source.name for source in _request_factories(
        priority="normal", allow_online=True,
    )[Market.CN]]
    interactive = [source.name for source in _request_factories(
        priority="interactive", allow_online=True,
    )[Market.CN]]
    full = [source.name for source in _request_factories(
        priority="interactive", allow_online=False,
    )[Market.CN]]

    assert "free-stockdb-online" not in normal
    assert "free-stockdb-online" in interactive
    assert "free-stockdb-online" not in full
