from __future__ import annotations

import sys

import pandas as pd
import pytest

from quantmaster.rotation.analytics import estimate_etf_flows
from quantmaster.rotation.provider import RotationProvider, _broad_etf_category
from quantmaster.rotation.store import RotationStore


class FakeTushare:
    def __init__(self):
        self.calls = []

    def _call(self, endpoint, ttl, **params):
        self.calls.append((endpoint, params))
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
            dates = ([params["trade_date"]] if "trade_date" in params else
                     ["20230710", "20260729", "20260730"])
            shares = {"20230710": 90, "20260729": 100, "20260730": 110}
            return pd.DataFrame([
                {"ts_code": "510300.SH", "trade_date": value,
                 "fd_share": shares[value]}
                for value in dates
                if params.get("start_date", value) <= value <= params.get("end_date", value)
            ])
        if endpoint == "fund_daily":
            dates = ([params["trade_date"]] if "trade_date" in params else
                     ["20230710", "20260729", "20260730"])
            closes = {"20230710": 3.8, "20260729": 4.0, "20260730": 4.1}
            return pd.DataFrame([
                {"ts_code": "510300.SH", "trade_date": value,
                 "close": closes[value]}
                for value in dates
                if params.get("start_date", value) <= value <= params.get("end_date", value)
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


def test_market_history_stops_at_latest_completed_session(tmp_path, monkeypatch):
    observed: dict[str, str] = {}

    class FakeCatalog:
        def partitions(self, **kwargs):
            observed["catalog_end"] = kwargs["end"]
            return []

    class FakeLake:
        catalog = FakeCatalog()

    class FakePlan:
        tasks = ()
        target_dates = ("2026-08-03",)

    class FakeEngine:
        def __init__(self, *, lake):
            assert isinstance(lake, FakeLake)

        def plan(self, start, end, **kwargs):
            observed["plan_end"] = end
            return FakePlan()

    monkeypatch.setattr(
        "quantmaster.rotation.service._expected_market_session",
        lambda: "2026-08-03",
    )
    monkeypatch.setattr("quantmaster.research.lake.ResearchLake", FakeLake)
    monkeypatch.setattr("quantmaster.research.engine.ResearchEngine", FakeEngine)

    result = RotationProvider(
        RotationStore(tmp_path / "rotation"), FakeTushare(),
    ).sync_market_history(lambda *_: None, lambda: False)

    assert observed == {"catalog_end": "2026-08-03", "plan_end": "2026-08-03"}
    assert result["expected_as_of"] == "2026-08-03"


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
    assert len(observations) == 3
    assert observations.iloc[0]["trade_date"] == pd.Timestamp("2023-07-10")
    assert observations.iloc[-1]["shares"] == 1_100_000
    assert observations.iloc[-1]["nav"] == 4.15
    assert observations.iloc[-1]["close"] == 4.1
    assert any(
        endpoint == "fund_share" and params.get("start_date") == "20230710"
        for endpoint, params in provider.source.calls
    )


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
    tmp_path, monkeypatch, caplog,
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
    fallback_records = [
        record for record in caplog.records
        if "尝试 Tushare DC 后备源" in record.getMessage()
    ]
    assert len(fallback_records) == 1
    assert fallback_records[0].exc_info is None
    assert fallback_records[0].getMessage().partition("：")[2]
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

    with pytest.raises(RuntimeError, match="题材目录全部不可用"):
        provider.sync_themes(lambda *args: None, lambda: False)

    assert store.themes() == [previous]


def test_ths_page_parser_reads_members_and_page_count():
    html = """
    <table class="m-table"><thead><tr><th>序号</th><th>代码</th><th>名称</th></tr></thead>
    <tbody><tr><td>1</td><td>920130</td><td>北交样本</td></tr>
    <tr><td>2</td><td>300364</td><td>深市样本</td></tr></tbody></table>
    <span class="page_info">1/35</span>
    """

    members, pages = RotationProvider._parse_ths_page(html)

    assert members == ["920130.BJ", "300364.SZ"]
    assert pages == 35


def test_ths_catalog_publishes_as_one_source_after_quality_gate(tmp_path, monkeypatch):
    class FakeAkshare:
        @staticmethod
        def stock_board_concept_name_ths():
            return pd.DataFrame([
                {"name": "题材一", "code": "301001"},
                {"name": "题材二", "code": "301002"},
            ])

    html = """
    <table class="m-table"><thead><tr><th>序号</th><th>代码</th><th>名称</th></tr></thead>
    <tbody><tr><td>1</td><td>600000</td><td>样本</td></tr></tbody></table>
    <span class="page_info">1/1</span>
    """
    monkeypatch.setitem(sys.modules, "akshare", FakeAkshare())
    monkeypatch.setattr(
        "quantmaster.rotation.provider.akshare_call",
        lambda label, function, *args, **kwargs: function(*args),
    )
    client_options = {}

    class CapturingClient:
        def __init__(self, **kwargs):
            client_options.update(kwargs)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

    monkeypatch.setattr("quantmaster.rotation.provider.httpx.Client", CapturingClient)
    store = RotationStore(tmp_path / "rotation")
    provider = RotationProvider(store, FakeTushare())
    monkeypatch.setattr(provider, "_ths_page", lambda client, code, page: html)

    result = provider._sync_ths_themes(lambda *args: None, lambda: False, [])

    assert result["coverage"] == 1.0
    assert result["source"] == "ths:concept"
    assert client_options["trust_env"] is False
    assert {item["source"] for item in store.themes()} == {"ths:concept"}


def test_ths_cold_start_can_publish_only_individually_complete_partial_catalog(
    tmp_path, monkeypatch,
):
    class FakeAkshare:
        @staticmethod
        def stock_board_concept_name_ths():
            return pd.DataFrame([
                {"name": f"题材{index}", "code": f"30{index:04d}"}
                for index in range(100)
            ])

    html = """
    <table class="m-table"><thead><tr><th>序号</th><th>代码</th></tr></thead>
    <tbody><tr><td>1</td><td>600000</td></tr></tbody></table>
    <span class="page_info">1/1</span>
    """
    monkeypatch.setitem(sys.modules, "akshare", FakeAkshare())
    monkeypatch.setattr(
        "quantmaster.rotation.provider.akshare_call",
        lambda label, function, *args, **kwargs: function(*args),
    )
    store = RotationStore(tmp_path / "rotation")
    provider = RotationProvider(store, FakeTushare())

    def page(_client, code, _page):
        if int(code) < 300075:
            return html
        raise RuntimeError("高页码需要登录")

    monkeypatch.setattr(provider, "_ths_page", page)
    result = provider._sync_ths_themes(lambda *args: None, lambda: False, [])

    assert result["quality_status"] == "partial"
    assert result["available"] == 75
    assert result["coverage"] == 0.75
    assert len(store.themes()) == 75
    assert {item["code"] for item in store.themes()} == {
        f"30{index:04d}" for index in range(75)
    }
    assert any("受限目录" in issue for issue in result["issues"])


def test_ths_refresh_reuses_fully_traversed_published_partial_catalog(
    tmp_path, monkeypatch,
):
    class FakeAkshare:
        @staticmethod
        def stock_board_concept_name_ths():
            return pd.DataFrame([
                {"name": f"题材{index}", "code": f"30{index:04d}"}
                for index in range(100)
            ])

    html = """
    <table class="m-table"><thead><tr><th>序号</th><th>代码</th></tr></thead>
    <tbody><tr><td>1</td><td>600000</td></tr></tbody></table>
    <span class="page_info">1/1</span>
    """
    monkeypatch.setitem(sys.modules, "akshare", FakeAkshare())
    monkeypatch.setattr(
        "quantmaster.rotation.provider.akshare_call",
        lambda label, function, *args, **kwargs: function(*args),
    )
    store = RotationStore(tmp_path / "rotation")
    provider = RotationProvider(store, FakeTushare())

    def page(_client, code, _page):
        if int(code) < 300075:
            return html
        raise RuntimeError("高页码需要登录")

    monkeypatch.setattr(provider, "_ths_page", page)
    first = provider._sync_ths_themes(lambda *args: None, lambda: False, [])
    assert first["quality_status"] == "partial"

    def unexpected_client(*args, **kwargs):
        raise AssertionError("已完整遍历的受限目录不应再次访问题材详情页")

    monkeypatch.setattr("quantmaster.rotation.provider.httpx.Client", unexpected_client)
    second = provider._sync_ths_themes(
        lambda *args: None, lambda: False, store.themes(),
    )

    assert second["quality_status"] == "partial"
    assert second["catalog"] == 100
    assert second["available"] == 75
    assert second["coverage"] == 0.75
    assert len(store.themes()) == 75
    assert any("继续使用已验证的受限目录" in issue for issue in second["issues"])
