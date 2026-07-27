"""证券主数据、跨市场解析与品种路由回归。"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient

from quantmaster.data.instruments import InstrumentStore, resolve_instruments
from quantmaster.server.app import app


def test_bundled_master_is_offline_and_covers_core_markets(isolated_config):
    store = InstrumentStore()
    diagnostics = store.diagnostics()
    batch = store.get_many(["600519.SH", "00700.HK", "missing.US"])

    assert diagnostics["record_count"] > 30_000
    assert set(batch) == {"600519.SH", "00700.HK"}
    assert store.get("600519.SH").name == "贵州茅台"
    assert store.get("589160.SH").asset_type == "etf"
    assert store.get("931743.CSI").asset_type == "index"
    assert store.get("00700.HK").name == "腾讯控股"
    assert store.get("AAPL.US").exchange == "NASDAQ"


def test_parser_handles_qualified_codes_names_and_pinyin():
    store = InstrumentStore()
    expected = {
        "sh600519": "600519.SH",
        "600519.ss": "600519.SH",
        "SSE:600519": "600519.SH",
        "贵州茅台": "600519.SH",
        "GZMT": "600519.SH",
        "700.hk": "00700.HK",
        "香港交易所:00700": "00700.HK",
        "NASDAQ:AAPL": "AAPL.US",
        "AAPL.US": "AAPL.US",
        "BRK.B.US": "BRK.B.US",
    }
    # 香港交易所的英文限定写法是快照生成的正式别名。
    expected["HKEX:00700"] = expected.pop("香港交易所:00700")
    for query, symbol in expected.items():
        result = store.resolve(query)
        assert result["status"] == "resolved", query
        assert result["instrument"]["symbol"] == symbol


def test_short_numeric_code_requires_explicit_market_choice():
    store = InstrumentStore()
    result = store.resolve("700")
    symbols = [item["symbol"] for item in result["candidates"]]

    assert result["status"] == "ambiguous"
    assert symbols[:2] == ["000700.SZ", "00700.HK"]
    store.remember("700", "00700.HK")
    assert store.resolve("700")["status"] == "ambiguous"

    selected = resolve_instruments(["700"], selections={"700": "00700.HK"})
    assert selected["status"] == "ok"
    assert selected["resolved"][0]["instrument"]["symbol"] == "00700.HK"


def test_legacy_name_file_is_migrated_without_losing_alias(isolated_config):
    root = isolated_config.data_root
    (root / "stock_names.json").write_text(
        json.dumps({"updated_at": 1, "names": {"600519.SH": "茅台旧称"}}, ensure_ascii=False),
        encoding="utf-8",
    )
    store = InstrumentStore(root / "fresh.sqlite")

    assert store.get("600519.SH").name == "贵州茅台"
    assert store.resolve("茅台旧称")["instrument"]["symbol"] == "600519.SH"


def test_tushare_routes_etf_and_csi_index(monkeypatch):
    from quantmaster.data.tushare_source import TushareSource

    calls = []
    source = TushareSource()

    def fake_call(endpoint, ttl_days, **params):
        calls.append((endpoint, params["ts_code"]))
        return pd.DataFrame({
            "ts_code": [params["ts_code"]], "trade_date": ["20260727"],
            "open": [1], "high": [2], "low": [1], "close": [2],
            "vol": [10], "amount": [20],
        })

    monkeypatch.setattr(source, "_call", fake_call)
    assert not source.daily("589160.SH", "2026-07-27", "2026-07-27").empty
    assert not source.daily("931743.CSI", "2026-07-27", "2026-07-27").empty
    assert calls == [("fund_daily", "589160.SH"), ("index_daily", "931743.CSI")]


def test_snapshot_is_declared_as_package_data():
    project = Path("pyproject.toml").read_text(encoding="utf-8")
    spec = Path("packaging/quantmaster.spec").read_text(encoding="utf-8")
    assert '"quantmaster.data" = ["security_master.json.gz"]' in project
    assert "security_master.json.gz" in spec


def test_instrument_http_api_exposes_and_resolves_ambiguity():
    client = TestClient(app)
    search = client.get("/api/instruments/search?q=GZMT&online=false").json()
    assert search["items"][0]["symbol"] == "600519.SH"

    settings = client.get("/api/settings").json()
    headers = {"X-CSRF-Token": settings["csrf_token"]}
    ambiguous = client.post(
        "/api/instruments/resolve", json={"queries": ["700"]}, headers=headers,
    ).json()
    assert ambiguous["status"] == "needs_confirmation"
    assert len(ambiguous["ambiguous"][0]["candidates"]) >= 2

    resolved = client.post(
        "/api/instruments/resolve",
        json={"queries": ["700"], "selections": {"700": "00700.HK"}},
        headers=headers,
    ).json()
    assert resolved["resolved"][0]["instrument"]["symbol"] == "00700.HK"
