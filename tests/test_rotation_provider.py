from __future__ import annotations

import pandas as pd

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
