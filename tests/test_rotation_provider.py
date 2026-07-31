from __future__ import annotations

import sys

import pandas as pd
import pytest

from quantmaster.rotation.analytics import estimate_etf_flows
from quantmaster.rotation.provider import RotationProvider, _broad_etf_category
from quantmaster.rotation.store import RotationStore


class FakeTushare:
    def _call(self, endpoint, ttl, **params):
        if endpoint == "index_classify":
            return pd.DataFrame([
                {"index_code": "801080.SI", "industry_name": "电子", "level": "L1"},
                {"index_code": "999999.SI", "industry_name": "伪造行业", "level": "L1"},
            ])
        if endpoint == "index_member_all":
            return pd.DataFrame([
                {
                    "l1_code": "801080.SI", "l1_name": "电子",
                    "l2_code": "801081.SI", "l2_name": "半导体",
                    "ts_code": "600001.SH", "is_new": "Y",
                },
                {
                    "l1_code": "801080.SI", "l1_name": "电子",
                    "l2_code": "801082.SI", "l2_name": "元件",
                    "ts_code": "000001.SZ", "is_new": "Y",
                },
            ])
        if endpoint == "fund_basic":
            return pd.DataFrame([
                {"ts_code": "510300.SH", "name": "沪深300ETF", "fund_type": "股票型ETF"},
                {"ts_code": "000001.OF", "name": "普通基金", "fund_type": "混合型"},
            ])
        if endpoint == "fund_share":
            trade_date = params["trade_date"]
            share = 100 if trade_date == "20260729" else 110
            return pd.DataFrame([
                {"ts_code": "510300.SH", "trade_date": trade_date, "fd_share": share},
            ])
        if endpoint == "fund_daily":
            trade_date = params["trade_date"]
            close = 4.0 if trade_date == "20260729" else 4.1
            return pd.DataFrame([
                {"ts_code": "510300.SH", "trade_date": trade_date, "close": close},
            ])
        if endpoint == "fund_nav":
            nav_date = params["nav_date"]
            nav = 4.05 if nav_date == "20260729" else 4.15
            return pd.DataFrame([
                {"ts_code": "510300.SH", "nav_date": nav_date, "unit_nav": nav},
            ])
        if endpoint == "dc_index":
            return pd.DataFrame([
                {
                    "ts_code": "BK1184.DC", "trade_date": params["trade_date"],
                    "name": "人形机器人", "idx_type": "概念板块",
                },
                {
                    "ts_code": "BK0816.DC", "trade_date": params["trade_date"],
                    "name": "人工智能", "idx_type": "概念板块",
                },
            ])
        if endpoint == "dc_member":
            symbols = (
                ["002117.SZ", "603662.SH", "688165.SH"]
                if params["ts_code"] == "BK1184.DC"
                else ["000001.SZ", "600001.SH", "830001.BJ"]
            )
            return pd.DataFrame([
                {
                    "trade_date": params["trade_date"],
                    "ts_code": params["ts_code"],
                    "con_code": symbol,
                    "name": f"成分{index}",
                }
                for index, symbol in enumerate(symbols)
            ])
        raise AssertionError(endpoint)

    def trade_calendar(self, start, end):
        return pd.DatetimeIndex(["2026-07-29", "2026-07-30"])


def test_provider_builds_strict_l1_l2_taxonomy(tmp_path):
    store = RotationStore(tmp_path / "rotation")
    provider = RotationProvider(store, FakeTushare())
    updates = []
    result = provider.sync_industry_taxonomy(
        lambda value, phase, detail: updates.append((value, phase)),
        lambda: False,
    )

    assert result == {"l1": 1, "l2": 2}
    l1 = store.taxonomy_nodes("L1")[0]
    assert l1["members"] == ["000001.SZ", "600001.SH"]
    assert {item["parent_code"] for item in store.taxonomy_nodes("L2")} == {"801080.SI"}
    assert updates[-1][0] == 39


def test_broad_etf_classifier_rejects_sector_and_offshore_products():
    assert _broad_etf_category("沪深300ETF") == "核心宽基"
    assert _broad_etf_category("央企红利低波ETF") == "策略宽基"
    assert _broad_etf_category("中证800医药ETF") == ""
    assert _broad_etf_category("纳指ETF") == ""
    assert _broad_etf_category("沪深300ETF", pd.NA) == "核心宽基"


def test_provider_merges_recent_etf_share_nav_and_close_snapshots(tmp_path, monkeypatch):
    store = RotationStore(tmp_path / "rotation")
    provider = RotationProvider(store, FakeTushare())
    monkeypatch.setattr("quantmaster.rotation.provider.date", type(
        "FixedDate", (), {"today": staticmethod(lambda: pd.Timestamp("2026-07-30").date())}
    ))
    result = provider.sync_etf_observations(lambda *args: None, lambda: False)
    observations = store.etf_observations()

    assert result["symbols"] == 1
    assert len(observations) == 2
    assert observations.iloc[-1]["shares"] == 1_100_000
    assert observations.iloc[-1]["nav"] == 4.15
    assert observations.iloc[-1]["close"] == 4.1


def test_provider_marks_close_fallback_when_fund_nav_is_unavailable(tmp_path, monkeypatch):
    class FakeNoNav(FakeTushare):
        def _call(self, endpoint, ttl, **params):
            if endpoint == "fund_nav":
                raise RuntimeError("fund_nav permission unavailable")
            return super()._call(endpoint, ttl, **params)

    store = RotationStore(tmp_path / "rotation")
    provider = RotationProvider(store, FakeNoNav())
    monkeypatch.setattr("quantmaster.rotation.provider.date", type(
        "FixedDate", (), {"today": staticmethod(lambda: pd.Timestamp("2026-07-30").date())}
    ))
    provider.sync_etf_observations(lambda *args: None, lambda: False)
    result = estimate_etf_flows(store.etf_observations())

    assert result["items"][0]["price_source"] == "close"
    assert result["summary"]["close_fallback_count"] == 1


def test_provider_uses_akshare_concept_code_for_member_lookup(tmp_path, monkeypatch):
    """新版目录返回 BK 代码后直接查询成分，避免再次按名称解析目录。"""
    member_symbols = []

    class FakeAkshare:
        @staticmethod
        def stock_board_concept_name_em():
            return pd.DataFrame([{"板块代码": "BK0816", "板块名称": "人工智能"}])

        @staticmethod
        def stock_board_concept_cons_em(symbol):
            member_symbols.append(symbol)
            return pd.DataFrame({"代码": ["000001", "600001"]})

    def direct_call(endpoint, function, **kwargs):
        kwargs.pop("lane", None)
        return function(**kwargs)

    monkeypatch.setitem(sys.modules, "akshare", FakeAkshare())
    monkeypatch.setattr("quantmaster.rotation.provider.akshare_call", direct_call)
    store = RotationStore(tmp_path / "rotation")
    provider = RotationProvider(store, FakeTushare())

    result = provider.sync_themes(lambda *args: None, lambda: False)

    assert result["source"] == "eastmoney-concept"
    assert member_symbols == ["BK0816"]
    assert store.themes()[0]["members"] == ["000001.SZ", "600001.SH"]


def test_provider_falls_back_to_tushare_dc_concepts_as_one_taxonomy(
    tmp_path, monkeypatch,
):
    monkeypatch.setitem(sys.modules, "akshare", None)
    store = RotationStore(tmp_path / "rotation")
    store.replace_themes([{
        "code": "EM_OLD", "name": "东方财富旧目录", "members": ["600000.SH"],
        "aliases": [], "source": "eastmoney-concept",
    }])
    monkeypatch.setattr("quantmaster.rotation.provider.date", type(
        "FixedDate", (), {"today": staticmethod(lambda: pd.Timestamp("2026-07-30").date())}
    ))
    provider = RotationProvider(store, FakeTushare())
    monkeypatch.setattr(
        "quantmaster.rotation.provider.akshare_call",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("connection closed")),
    )

    result = provider.sync_themes(lambda *args: None, lambda: False)

    assert result["source"] == "tushare:dc-concept"
    assert result["trade_date"] == "20260730"
    assert result["issues"] == [
        "东方财富概念接口不可用，已自动切换为 Tushare DC 概念目录。"
    ]
    themes = store.themes()
    assert {item["code"] for item in themes} == {"BK0816.DC", "BK1184.DC"}
    assert {item["source"] for item in themes} == {"tushare:dc-concept"}


def test_provider_keeps_previous_theme_catalog_when_both_sources_fail(
    tmp_path, monkeypatch,
):
    monkeypatch.setitem(sys.modules, "akshare", None)

    class UnavailableTushare(FakeTushare):
        def _call(self, endpoint, ttl, **params):
            if endpoint == "dc_index":
                raise RuntimeError("permission unavailable")
            return super()._call(endpoint, ttl, **params)

    store = RotationStore(tmp_path / "rotation")
    previous = {
        "code": "EM_OLD", "name": "可恢复旧目录", "members": ["600000.SH"],
        "aliases": [], "source": "eastmoney-concept",
    }
    store.replace_themes([previous])
    provider = RotationProvider(store, UnavailableTushare())
    monkeypatch.setattr(
        "quantmaster.rotation.provider.akshare_call",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("connection closed")),
    )

    with pytest.raises(RuntimeError, match="题材目录双源不可用"):
        provider.sync_themes(lambda *args: None, lambda: False)

    assert store.themes() == [previous]
