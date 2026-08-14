from __future__ import annotations

import json
import os
from types import SimpleNamespace
from typing import ClassVar

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from quantmaster.data.base import DataCapability
from quantmaster.data.free_stockdb_compatibility import (
    StockDBCompatibilityStore,
    compare_indicator_frames,
    publish_validation,
    quantmaster_indicators,
)
from quantmaster.data.free_stockdb_contracts import (
    StockDBArtifactIdentity,
    StockDBIngestSnapshot,
)
from quantmaster.data.free_stockdb_ingest import StockDBIngestService, StockDBIngestStore
from quantmaster.data.free_stockdb_source import FreeStockDBSource
from quantmaster.data.instrument_snapshots import (
    TUSHARE_CATALOG_QUERY,
    freeze_instrument_catalog,
)
from quantmaster.data.instruments import Instrument
from quantmaster.research.adapters import StockDBResearchAdapter
from quantmaster.research.catalog import ResearchCatalog
from quantmaster.rotation.etf_research import (
    EtfResearchService,
    EtfResearchStore,
    classify_etf,
    etf_directory_master_hash,
    is_exchange_etf,
)
from quantmaster.server.app import app
from tests.catalog_evidence_helpers import bound_tushare_catalog


def test_local_stockdb_advertises_eod_but_not_realtime_spot():
    assert DataCapability.EOD_SNAPSHOT in FreeStockDBSource.capabilities
    assert DataCapability.REALTIME_TICK not in FreeStockDBSource.capabilities
    assert DataCapability.SPOT not in FreeStockDBSource.capabilities


def test_etf_sandbox_candidate_filter_rejects_lof_and_deduplicates_evidence():
    candidates: dict[str, dict] = {}
    evidence: dict[str, list[dict[str, str]]] = {}
    target = pd.Timestamp("2026-08-08")
    observed_at = pd.Timestamp("2026-08-08T06:00:00+00:00")

    EtfResearchService._add_sandbox_candidate(
        candidates,
        evidence,
        target=target,
        symbol="510300.SH",
        row={"name": "沪深300ETF", "exchange": "SH", "list_date": "2012-05-28"},
        kind="instrument_store",
        source="InstrumentStore",
        observed_at=observed_at,
    )
    EtfResearchService._add_sandbox_candidate(
        candidates,
        evidence,
        target=target,
        symbol="510300.SH",
        row={"name": "沪深300ETF", "exchange": "SH", "list_date": "2012-05-28"},
        kind="instrument_store",
        source="InstrumentStore",
        observed_at=observed_at,
    )
    EtfResearchService._add_sandbox_candidate(
        candidates,
        evidence,
        target=target,
        symbol="160000.SZ",
        row={"name": "示例LOF", "exchange": "SZ"},
        kind="etf_metadata",
        source="RotationStore",
        observed_at=observed_at,
    )
    EtfResearchService._add_sandbox_candidate(
        candidates,
        evidence,
        target=target,
        symbol="510500.SH",
        row={"name": "未来ETF", "exchange": "SH", "list_date": "2026-08-09"},
        kind="etf_metadata",
        source="RotationStore",
        observed_at=observed_at,
    )

    assert list(candidates) == ["510300.SH"]
    assert candidates["510300.SH"]["asset_type"] == "etf"
    assert evidence["510300.SH"] == [{
        "kind": "instrument_store",
        "source": "InstrumentStore",
        "observed_at": "2026-08-08T06:00:00+00:00",
    }]


def test_native_acceleration_requires_current_artifact_profile(isolated_config, monkeypatch):
    close = np.linspace(10, 14, 80) + np.sin(np.arange(80))
    bars = pd.DataFrame(
        {
            "date": pd.bdate_range("2026-01-01", periods=80),
            "close": close,
            "high": close + 0.2,
            "low": close - 0.2,
        }
    )
    expected = quantmaster_indicators(bars)
    comparisons = compare_indicator_frames(expected, expected.copy())
    store = StockDBCompatibilityStore()
    profile = publish_validation("artifact-a", [comparisons], [{"symbol": "600000.SH"}], store=store)

    assert profile.status == "compatible"
    assert store.admitted("artifact-a", "MACD") is True
    assert store.admitted("artifact-b", "MACD") is False

    source = FreeStockDBSource()
    isolated_config.data.free_stockdb_native_acceleration_enabled = True
    monkeypatch.setattr(
        source, "artifact_identity", lambda **_kwargs: SimpleNamespace(artifact_id="artifact-b")
    )
    # The current artifact has no profile, so vendor code must not execute.
    monkeypatch.setattr(source, "native_indicators", lambda *_args: pytest.fail("must fall back"))
    assert (
        source.accelerated_indicators(["MACD"], ["600000.SH"], "2026-01-01", "2026-04-30")["path"]
        == "quantmaster"
    )


def test_sdk_module_cache_key_changes_when_file_changes(tmp_path):
    sdk = tmp_path / "stock_sdk.py"
    sdk.write_text("VALUE = 1\n", encoding="utf-8")
    first = FreeStockDBSource(sdk_path=str(sdk))._load_sdk_module()
    sdk.write_text("VALUE = 2\n", encoding="utf-8")
    second = FreeStockDBSource(sdk_path=str(sdk))._load_sdk_module()

    assert first.VALUE == 1
    assert second.VALUE == 2
    assert first.__name__ != second.__name__


def test_cn_minute_aggregation_never_crosses_lunch():
    index = pd.to_datetime(
        [
            "2026-08-07 11:26",
            "2026-08-07 11:29",
            "2026-08-07 13:00",
            "2026-08-07 13:04",
        ]
    )
    frame = pd.DataFrame(
        {
            "open": [1, 2, 3, 4],
            "high": [2, 3, 4, 5],
            "low": [1, 1, 3, 3],
            "close": [2, 3, 4, 5],
            "volume": [1, 1, 1, 1],
            "amount": [2, 3, 4, 5],
        },
        index=index,
    )
    result = FreeStockDBSource._aggregate_cn_minutes(
        frame,
        "5m",
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
            "amount": "sum",
        },
    )

    assert len(result) == 2
    assert result.index.tolist() == [pd.Timestamp("2026-08-07 11:26"), pd.Timestamp("2026-08-07 13:00")]
    assert result["volume"].tolist() == [2, 2]


def test_ingest_store_is_immutable_content_addressed_and_reusable(tmp_path, isolated_config):
    isolated_config.data.free_stockdb_ingest_retain = 30
    store = StockDBIngestStore(tmp_path / "ingest")
    frame = pd.DataFrame(
        {
            "symbol": ["600000.SH"],
            "date": [pd.Timestamp("2026-08-07")],
            "open": [10.0],
            "high": [11.0],
            "low": [9.0],
            "close": [10.5],
            "volume": [100.0],
        }
    )
    kwargs = dict(
        boards=[{"code": "L1", "members": ["600000.SH"]}],
        catalog=[],
        delisted=[],
        as_of_date="2026-08-07",
        artifact_id="artifact",
        master_snapshot_id="master",
        start_date="2026-01-01",
        end_date="2026-08-08",
        coverage={"status": "complete"},
        provenance={
            "cache_key": "same",
            "upstream": "vendor-declared-unverified",
            "upstream_evidence": "not_provided",
            "distribution": "free-stockdb",
        },
    )
    first = store.publish(frame=frame, **kwargs)
    again = store.publish(frame=frame, **kwargs)
    revised = store.publish(
        frame=frame.assign(close=10.6),
        **{**kwargs, "provenance": {**kwargs["provenance"], "cache_key": "revised"}},
    )

    assert first.ingest_id == again.ingest_id
    assert revised.ingest_id != first.ingest_id
    assert store.find("same").ingest_id == first.ingest_id
    assert store.load_frame(first).loc[0, "close"] == 10.5


def test_stockdb_cross_validation_sample_is_content_addressed_and_stratified():
    dates = pd.bdate_range("2026-07-01", periods=8)
    rows = []
    for exchange, base in (("SH", 600000), ("SZ", 1), ("BJ", 830000)):
        for offset in range(16):
            symbol = f"{base + offset:06d}.{exchange}"
            for day, stamp in enumerate(dates):
                rows.append({
                    "symbol": symbol,
                    "date": stamp,
                    "amount": float((offset + 1) * (day + 1) * 10_000),
                })
    frame = pd.DataFrame(rows)

    first = StockDBIngestService.cross_validation_sample(frame)
    second = StockDBIngestService.cross_validation_sample(
        frame.sample(frac=1.0, random_state=7).reset_index(drop=True),
    )

    assert first == second
    assert len(first["symbols"]) == 32
    assert len(first["trade_dates"]) == 5
    assert {item["exchange"] for item in first["strata"]} == {"SH", "SZ", "BJ"}


def test_stockdb_cross_validation_reuses_local_tushare_cache_before_network(
    tmp_path, isolated_config, monkeypatch,
):
    isolated_config.data.tushare_token = "fixture-token"
    dates = pd.bdate_range("2026-07-01", periods=5)
    rows = []
    for symbol, price in (("600000.SH", 10.0), ("000001.SZ", 20.0)):
        for stamp in dates:
            rows.append({
                "symbol": symbol,
                "date": stamp,
                "open": price,
                "high": price + 1,
                "low": price - 1,
                "close": price,
                "volume": 1_000.0,
                "amount": 10_000.0,
            })
    frame = pd.DataFrame(rows)

    def cached_daily(_self, symbol, _start, _end):
        return (
            frame.loc[frame["symbol"] == symbol]
            .drop(columns="symbol")
            .set_index("date")
        )

    monkeypatch.setattr(
        "quantmaster.data.tushare_source.TushareSource.cached_daily", cached_daily,
    )
    monkeypatch.setattr(
        "quantmaster.data.tushare_source.TushareSource.daily",
        lambda *_args, **_kwargs: pytest.fail("缓存命中时不应联网"),
    )
    service = StockDBIngestService(
        source=SimpleNamespace(),
        store=StockDBIngestStore(tmp_path / "ingest"),
    )

    evidence = service._cross_source_validation(frame)

    assert evidence["status"] == "verified"
    assert evidence["cache_hits"] == 2
    assert evidence["remote_fetches"] == 0


def test_stockdb_cross_validation_keeps_completed_remote_items_on_batch_failure(
    tmp_path, isolated_config, monkeypatch,
):
    isolated_config.data.tushare_token = "fixture-token"
    dates = pd.bdate_range("2026-07-01", periods=5)
    frame = pd.concat([
        pd.DataFrame({
            "symbol": symbol, "date": dates, "open": price, "high": price + 1,
            "low": price - 1, "close": price, "volume": 1_000.0, "amount": 10_000.0,
        })
        for symbol, price in (("600000.SH", 10.0), ("000001.SZ", 20.0))
    ], ignore_index=True)
    calls: list[str] = []

    monkeypatch.setattr(
        "quantmaster.data.tushare_source.TushareSource.cached_daily",
        lambda *_args, **_kwargs: None,
    )

    def remote(_self, symbol, _start, _end):
        calls.append(symbol)
        if symbol == "000001.SZ":
            raise RuntimeError("tail failure")
        return frame.loc[frame["symbol"] == symbol].drop(columns="symbol").set_index("date")

    monkeypatch.setattr("quantmaster.data.tushare_source.TushareSource.daily", remote)
    store = StockDBIngestStore(tmp_path / "ingest")
    service = StockDBIngestService(source=SimpleNamespace(), store=store)

    first = service._cross_source_validation(frame)
    request_id = "dates-2026-07-01-to-2026-07-07"
    assert first["status"] == "locally_validated"
    assert first["completed_items"] == 1
    assert store.cross_validation_item(request_id, "600000.SH")["status"] == "complete"

    def finish(_self, symbol, _start, _end):
        calls.append(symbol)
        return frame.loc[frame["symbol"] == symbol].drop(columns="symbol").set_index("date")

    monkeypatch.setattr("quantmaster.data.tushare_source.TushareSource.daily", finish)
    second = service._cross_source_validation(frame)
    assert second["status"] == "verified"
    assert second["reused_items"] == 1
    assert calls.count("600000.SH") == 1
    assert calls.count("000001.SZ") == 2


def test_stockdb_field_contracts_expose_confirmed_local_schema_semantics():
    frame = pd.DataFrame({
        "symbol": ["600000.SH"], "date": [pd.Timestamp("2026-08-07")],
        "open": [10.0], "high": [11.0], "low": [9.0], "close": [10.5],
        "volume": [100.0], "amount": [1_000.0],
    })

    contracts = {
        item["field"]: item for item in StockDBIngestService.field_contracts(
            frame, "2026-08-07", asset_class="stock", source="free-stockdb",
        )
    }

    assert contracts["amount"]["unit"] == "CNY"
    assert contracts["volume"]["unit"] == "share"
    assert contracts["turnover"]["validation"]["semantic"] == "percent of float shares"


def test_ingest_prune_uses_publish_time_not_content_id(tmp_path, isolated_config):
    isolated_config.data.free_stockdb_ingest_retain = 10
    store = StockDBIngestStore(tmp_path / "ingest", retain=10)
    manifests = []
    for index in range(3):
        snapshot = store.publish(
            frame=pd.DataFrame({"symbol": [f"60000{index}.SH"], "date": [pd.Timestamp("2026-08-07")]}),
            boards=[{"code": "L1"}],
            catalog=[],
            delisted=[],
            as_of_date="2026-08-07",
            artifact_id=f"artifact-{index}",
            master_snapshot_id="master",
            start_date="2026-01-01",
            end_date="2026-08-08",
            coverage={},
            provenance={"cache_key": str(index)},
        )
        path = store.manifests / f"{snapshot.ingest_id}.json"
        os.utime(path, (100 + index, 100 + index))
        manifests.append(path)
    store.retain = 2
    store.pin(manifests[0].stem, "test", "frozen-snapshot")
    store.prune()

    assert all(path.exists() for path in manifests)
    assert store.references(manifests[0].stem)[0]["reference_id"] == "frozen-snapshot"


def test_frozen_factor_derives_research_price_without_mutating_raw_frame():
    raw = pd.DataFrame(
        {
            "symbol": ["600000.SH", "600000.SH"],
            "date": pd.to_datetime(["2026-07-15", "2026-07-16"]),
            "open": [10.0, 5.0],
            "high": [10.0, 5.0],
            "low": [10.0, 5.0],
            "close": [10.0, 5.0],
            "pre_close": [10.0, 10.0],
        }
    )
    factors = pd.DataFrame(
        {
            "symbol": ["600000.SH", "600000.SH"],
            "date": pd.to_datetime(["2025-01-01", "2026-07-16"]),
            "adj_factor": [1.0, 2.0],
        }
    )
    research = StockDBIngestService._research_prices(raw, factors)

    assert raw["close"].tolist() == [10.0, 5.0]
    assert research["close"].tolist() == [5.0, 5.0]
    assert research["price_adjustment"].unique().tolist() == [
        "forward_adjusted_from_frozen_factor_v1"
    ]


class _NoReadSource:
    sdk_path = "installed"

    def daily_cross_section(self, *_args, **_kwargs):
        raise AssertionError("research adapter should reuse the immutable ingest")


def test_research_adapter_reuses_ingest_and_exposes_factor_lineage(tmp_path, isolated_config):
    isolated_config.data.free_stockdb_ingest_retain = 30
    ingest_store = StockDBIngestStore(tmp_path / "ingest")
    raw = pd.DataFrame(
        {
            "symbol": ["600000.SH"],
            "date": [pd.Timestamp("2026-08-07")],
            "open": [10.0],
            "high": [11.0],
            "low": [9.0],
            "close": [10.5],
            "volume": [100.0],
            "amount": [1_000.0],
            "turnover": [1.0],
        }
    )
    factors = pd.DataFrame(
        {
            "symbol": ["600000.SH"],
            "date": [pd.Timestamp("2026-07-01")],
            "adj_factor": [2.0],
        }
    )
    snapshot = ingest_store.publish(
        frame=raw,
        adjustment=factors,
        boards=[{"code": "L1"}],
        catalog=[],
        delisted=[],
        as_of_date="2026-08-07",
        artifact_id="artifact",
        master_snapshot_id="master",
        start_date="2026-01-01",
        end_date="2026-08-08",
        coverage={},
        provenance={},
    )
    adapter = StockDBResearchAdapter(
        ResearchCatalog(tmp_path / "research.sqlite"),
        source=_NoReadSource(),
        instruments=SimpleNamespace(list=lambda **_kwargs: []),
        ingest_store=ingest_store,
    )

    bars = adapter.fetch_date("stock_bars", "2026-08-07")
    adjustment = adapter.fetch_date("stock_adj_factor", "2026-08-07")

    assert bars.loc[0, "ingest_id"] == snapshot.ingest_id
    assert bars.loc[0, "distribution"] == "free-stockdb"
    assert adjustment.loc[0, "adj_factor"] == 2.0
    assert adjustment.loc[0, "factor_observed_date"] == pd.Timestamp("2026-07-01")


@pytest.mark.parametrize(
    ("name", "category"),
    [
        ("沪深300ETF", "境内宽基"),
        ("恒生科技ETF(QDII)", "海外权益"),
        ("国债ETF", "债券"),
        ("黄金ETF", "商品"),
        ("货币ETF", "货币"),
        ("红利低波ETF", "策略"),
        ("机器人ETF", "行业主题"),
    ],
)
def test_etf_classification_is_cross_asset_and_category_local(name, category):
    assert classify_etf(name)[0] == category


def test_lof_is_not_admitted_as_etf():
    lof = Instrument(
        "501001.SH",
        "501001",
        "某某LOF",
        "CN",
        "SH",
        "fund",
        status="listed",
    )
    assert is_exchange_etf(lof) is False


class _EtfInstruments:
    items: ClassVar[list[Instrument]] = [
        Instrument(
            "510300.SH", "510300", "沪深300ETF", "CN", "SH", "fund",
            status="listed", list_date="20120528",
        ),
        Instrument(
            "159920.SZ", "159920", "恒生ETF(QDII)", "CN", "SZ", "fund",
            status="listed", list_date="20121022",
        ),
        Instrument(
            "511010.SH", "511010", "国债ETF", "CN", "SH", "etf",
            status="listed", list_date="20130805",
        ),
        Instrument(
            "501001.SH", "501001", "测试LOF", "CN", "SH", "fund",
            status="listed", list_date="20200101",
        ),
    ]

    def list(self, **_kwargs):
        return list(self.items)


class _EtfSource:
    name = "free-stockdb"

    def __init__(self):
        self.intraday_calls = 0
        self.daily_calls = 0
        dates = pd.bdate_range(end="2026-08-07", periods=65)
        rows = []
        for index, symbol in enumerate(("510300.SH", "159920.SZ", "511010.SH"), 1):
            for offset, stamp in enumerate(dates):
                close = 1 + index * 0.1 + offset * 0.001
                rows.append(
                    {
                        "symbol": symbol,
                        "date": stamp,
                        "open": close * 0.99,
                        "high": close * 1.01,
                        "low": close * 0.98,
                        "close": close,
                        "volume": 1_000_000,
                        "amount": 80_000_000 + index,
                        "total_share": 1_000_000_000 + offset * 1_000_000,
                        "float_share": 1_000_000_000 + offset * 1_000_000,
                        "total_mv": close * (1_000_000_000 + offset * 1_000_000),
                        "float_mv": close * (1_000_000_000 + offset * 1_000_000),
                        "pe_ttm": np.nan,
                        "pb": np.nan,
                        "is_st": False,
                        "pre_close": close - 0.001,
                        "pct_chg": 0.1,
                        "amplitude": 1,
                        "turnover": 0.1,
                        "vol_ratio": 1,
                        "name": symbol,
                    }
                )
        self.frame = pd.DataFrame(rows)

    def artifact_identity(self, **kwargs):
        return StockDBArtifactIdentity.discover(None, None, data_session=kwargs.get("data_session", ""))

    def daily_cross_section(self, symbols, start, end):
        self.daily_calls += 1
        return self.frame[self.frame["symbol"].isin(symbols)].copy()

    def intraday_many(self, symbols, start, end, frequency):
        self.intraday_calls += 1
        if "510300.SH" not in symbols:
            return pd.DataFrame()
        stamps = pd.date_range("2026-08-07 09:30", periods=241, freq="min")
        return pd.DataFrame(
            {
                "symbol": "510300.SH",
                "date": stamps,
                "open": 1.0,
                "high": 1.01,
                "low": 0.99,
                "close": np.linspace(1, 1.01, len(stamps)),
                "volume": 1000,
                "amount": 1000,
            }
        )


def test_etf_compatible_ingest_respects_historical_knowledge_cutoff():
    identity = StockDBArtifactIdentity(
        artifact_id="etf-artifact",
        sdk={"available": True, "sha256": "1" * 64},
    )
    context = SimpleNamespace(
        identity=identity,
        start=pd.Timestamp("2023-07-01"),
        end=pd.Timestamp("2026-08-08"),
    )
    candidate = StockDBIngestSnapshot(
        ingest_id="sdi-history",
        as_of_date="2026-08-08",
        artifact_id=identity.artifact_id,
        master_snapshot_id="etf-master",
        start_date="2023-01-01",
        end_date="2026-08-08",
        assets={"etf": {}},
        coverage={"status": "complete"},
        content_hashes={},
        provenance={},
        created_at="2026-08-08T07:00:00+00:00",
    )

    assert EtfResearchService._compatible_scan_candidate(
        candidate,
        context,
        pd.Timestamp("2026-08-08T07:00:00+00:00"),
    )
    assert not EtfResearchService._compatible_scan_candidate(
        candidate,
        context,
        pd.Timestamp("2026-08-08T06:59:59+00:00"),
    )


def test_etf_daily_batch_cancellation_stops_before_source_read():
    source = _EtfSource()
    service = object.__new__(EtfResearchService)
    service.source = source
    context = SimpleNamespace(
        symbols=["510300.SH"],
        start=pd.Timestamp("2023-07-01"),
        end=pd.Timestamp("2026-08-08"),
        cancelled=lambda: True,
        progress=lambda *_args: None,
    )

    with pytest.raises(InterruptedError, match="ETF 研究扫描已取消"):
        service._read_scan_daily_batches(context, report_progress=True)

    assert source.daily_calls == 0


def test_etf_scan_builds_v3_sector_radar_and_loads_minutes_only_on_demand(
    tmp_path,
    isolated_config,
    monkeypatch,
):
    monkeypatch.setattr(
        EtfResearchService, "_direct_share_observations", staticmethod(lambda: pd.DataFrame())
    )
    instruments = [
        item
        for item in _EtfInstruments().list(market="CN")
        if is_exchange_etf(item)
    ]
    etf_catalog = [
        {
            "symbol": instrument.symbol,
            "name": instrument.name,
            "market": "CN",
            "exchange": instrument.exchange,
            "asset_type": "etf",
            "status": instrument.status,
            "list_date": instrument.list_date,
            "delist_date": instrument.delist_date,
        }
        for instrument in instruments
    ]
    monkeypatch.setattr(
        "quantmaster.data.instrument_snapshots.TUSHARE_MINIMUM_ASSET_COUNTS",
        {"CN:stock": 3000, "CN:etf": len(etf_catalog)},
    )
    stock_catalog = [
        {
            "symbol": f"{600000 + index:06d}.SH",
            "name": f"目录股票{index}",
            "market": "CN",
            "exchange": "SH",
            "asset_type": "stock",
            "status": "L",
            "list_date": "2020-01-01",
            "delist_date": "",
        }
        for index in range(3_000)
    ]
    catalog_records, catalog_outcomes = bound_tushare_catalog(
        [*stock_catalog, *etf_catalog],
    )
    catalog_snapshot = freeze_instrument_catalog(
        catalog_records,
        source="tushare:catalog",
        query=TUSHARE_CATALOG_QUERY,
        request_outcomes=catalog_outcomes,
        acquired_at=pd.Timestamp("2026-08-08T15:01:00+08:00").to_pydatetime(),
    )
    catalog_evidence = catalog_snapshot.evidence(
        market="CN", asset_type="etf", as_of="2026-08-08"
    )
    directory_rows = []
    for instrument in instruments:
        directory_rows.append(
            {
                "symbol": instrument.symbol,
                "name": instrument.name,
                "exchange": instrument.exchange,
                "asset_type": "etf",
                "status": "L",
                "list_date": pd.Timestamp(instrument.list_date).date().isoformat(),
                "delist_date": instrument.delist_date,
                "metadata_source": "free-stockdb:security-master",
                "effective_date": "2026-08-08",
                "updated_at": "2026-08-08",
                "observed_at": "2026-08-08T07:02:00+00:00",
                "directory_snapshot_id": "etf-directory-test",
                "directory_complete": True,
                "directory_expected_symbols": 3,
                "directory_observed_symbols": 3,
                "directory_member_source": "tushare:catalog",
                "directory_member_observed_at": catalog_snapshot.acquired_at,
                "directory_source": "tushare:catalog",
                "directory_acquired_at": catalog_snapshot.acquired_at,
                "directory_cutoff_at": "2026-08-08T15:00:00+08:00",
                "directory_freshness": "fresh",
                "directory_master_record_count": len(catalog_snapshot.records),
                "directory_master_batch_record_count": len(catalog_snapshot.records),
                "directory_master_snapshot_sha256": catalog_snapshot.snapshot_id,
                "directory_catalog_snapshot_id": catalog_snapshot.snapshot_id,
                "directory_catalog_records_sha256": catalog_evidence["records_sha256"],
                "directory_catalog_file_sha256": catalog_snapshot.file_sha256,
                "directory_catalog_file_size": str(catalog_snapshot.file_size),
                "directory_catalog_file_mtime_ns": str(
                    catalog_snapshot.file_mtime_ns
                ),
                "directory_catalog_relative_path": catalog_evidence["relative_path"],
                "directory_catalog_as_of": "2026-08-08",
                "directory_catalog_expected_count": 3,
            }
        )
    directory = pd.DataFrame(directory_rows)
    directory_hash = etf_directory_master_hash(directory)
    directory["directory_attestation_sha256"] = directory_hash
    directory["directory_snapshot_id"] = "etf_directory_" + directory_hash[:24]

    class HistoricalRotationStore:
        @staticmethod
        def etf_metadata_history():
            return directory.copy()

        @staticmethod
        def etf_metadata():
            return directory.copy()

        @staticmethod
        def etf_observations():
            return pd.DataFrame()

    monkeypatch.setattr("quantmaster.rotation.store.RotationStore", HistoricalRotationStore)
    source = _EtfSource()
    service = EtfResearchService(
        source=source,
        instruments=_EtfInstruments(),
        ingest_store=StockDBIngestStore(tmp_path / "ingest"),
        store=EtfResearchStore(tmp_path / "research"),
    )
    obsolete = service.store.root / "snapshots" / "obsolete_v2.json"
    obsolete.parent.mkdir(parents=True)
    obsolete.write_text(
        '{"schema_version":"2.0","research_model_version":"QM_ETF_SECTOR_RADAR_V2.4"}',
        encoding="utf-8",
    )
    snapshot = service.scan(as_of="2026-08-08")
    repeated = service.scan(as_of="2026-08-08")
    assert source.intraday_calls == 0
    assert source.daily_calls == 1

    assert len(snapshot.items) == 3
    assert {item.category for item in snapshot.items} == {"境内宽基", "海外权益", "债券"}
    assert snapshot.schema_version == "3.0"
    assert snapshot.research_model_version == "QM_ETF_SECTOR_RADAR_V3.5"
    assert len(snapshot.sectors) == 3
    assert all(item.funds["status"] == "missing" for item in snapshot.items)
    assert all("score" not in item.to_dict() for item in snapshot.items)
    assert all("minute_evidence" not in item.to_dict() for item in snapshot.items)
    assert "分钟" not in snapshot.evidence_hashes
    assert snapshot.capabilities["intraday"]["status"] == "on_demand"
    assert snapshot.freshness["metadata"]["coverage"] == 1.0
    assert snapshot.freshness["metadata"]["official_coverage"] == 0.0
    assert snapshot.provenance["calculation"] == "QuantMaster ETF Sector Radar V3"
    assert repeated.snapshot_id == snapshot.snapshot_id
    assert repeated.generated_at == snapshot.generated_at
    assert service.store.get(snapshot.snapshot_id) is service.store.get(snapshot.snapshot_id)
    assert not obsolete.exists()
    assert service.ingest_store.references(snapshot.ingest_id)[0]["namespace"] == "etf_research"

    monkeypatch.setattr(
        "quantmaster.rotation.etf_research.get_etf_research_service",
        lambda **_kwargs: service,
    )
    client = TestClient(app)
    listing = client.get("/api/v1/rotation/etfs?category=境内宽基")
    equity_listing = client.get("/api/v1/rotation/etfs?asset=equity")
    overview = client.get("/api/v1/rotation/etfs/overview?asset=equity")
    sector_id = next(item.sector_id for item in snapshot.items if item.symbol == "510300.SH")
    sector = client.get(f"/api/v1/rotation/etfs/sectors/{sector_id}")
    detail = client.get("/api/v1/rotation/etfs/510300.SH")
    history = client.get("/api/v1/rotation/etfs/snapshots")
    exported = client.get(f"/api/v1/rotation/etfs/export/{snapshot.snapshot_id}?format=csv")
    historical = client.get(f"/api/v1/rotation/etfs?snapshot_id={snapshot.snapshot_id}&category=境内宽基")
    coverage = client.get(f"/api/v1/rotation/etfs/snapshots/{snapshot.snapshot_id}/coverage")
    intraday = client.get("/api/v1/rotation/etfs/510300.SH/intraday")
    legacy_sort = client.get("/api/v1/rotation/etfs?sort=score")

    assert listing.status_code == 200
    assert [item["symbol"] for item in listing.json()["data"]["items"]] == ["510300.SH"]
    assert equity_listing.json()["data"]["categories"] == ["境内宽基"]
    assert overview.status_code == 200
    assert len(overview.json()["data"]["sectors"]) == 1
    assert "items" not in overview.json()["data"]
    assert "fields" not in overview.json()["meta"]["quality"]
    assert overview.json()["data"]["map"]["position_metric"] == "position_60d"
    assert overview.json()["data"]["map"]["sector_ids"]
    assert overview.json()["meta"]["refresh"]["recommended"] is False
    monkeypatch.setattr(
        service,
        "_direct_share_observations",
        lambda: pd.DataFrame(
            {
                "symbol": ["510300.SH"],
                "trade_date": [snapshot.as_of_date],
                "shares": [1_000_000_000],
                "share_source": ["tushare:etf_share_size"],
            }
        ),
    )
    monkeypatch.setattr(
        service,
        "_direct_metadata",
        lambda: pd.DataFrame(
            {
                "symbol": ["510300.SH"],
                "name": ["沪深300ETF"],
                "benchmark": ["沪深300指数"],
                "metadata_source": ["tushare:fund_basic"],
            }
        ),
    )
    changed_evidence = client.get("/api/v1/rotation/etfs/overview?asset=equity")
    assert changed_evidence.json()["meta"]["refresh"]["recommended"] is True
    assert "份额" in changed_evidence.json()["meta"]["refresh"]["reason"]
    assert "元数据" in changed_evidence.json()["meta"]["refresh"]["reason"]
    monkeypatch.setattr(service, "_direct_share_observations", lambda: pd.DataFrame())
    monkeypatch.setattr(service, "_direct_metadata", lambda: pd.DataFrame())
    assert sector.status_code == 200 and sector.json()["data"]["members"][0]["symbol"] == "510300.SH"
    assert detail.status_code == 200 and detail.json()["data"]["category"] == "境内宽基"
    assert history.status_code == 200 and history.json()["items"][0]["ingest_id"] == snapshot.ingest_id
    assert exported.status_code == 200 and exported.content.startswith(b"\xef\xbb\xbf")
    assert historical.status_code == 200 and historical.json()["meta"]["snapshot_id"] == snapshot.snapshot_id
    assert coverage.status_code == 200
    assert coverage.json()["data"]["share_semantic_counts"]["missing"] == 3
    assert coverage.json()["data"]["intraday_mode"] == "on_demand"
    assert intraday.status_code == 200
    # Detail reads are snapshot-only.  A missing minute artifact is an
    # explicit degraded result, never a hidden StockDB request.
    assert source.intraday_calls == 0
    assert intraday.json()["data"]["status"] == "missing"
    assert intraday.json()["data"]["issue"] == "snapshot_unavailable"
    assert intraday.json()["data"]["series"] == []
    assert len(overview.content) <= 180_000
    assert len(listing.content) <= 60_000
    assert len(sector.content) <= 100_000
    assert legacy_sort.status_code == 422

    (service.store.root / "latest.json").write_text(
        json.dumps({"snapshot_id": "obsolete_v1"}),
        encoding="utf-8",
    )
    cold = client.get("/api/v1/rotation/etfs/overview?asset=equity")
    assert cold.json()["meta"]["quality"]["status"] == "cold"
    assert cold.json()["meta"]["refresh"]["recommended"] is True
    assert cold.json()["meta"]["refresh"]["input_id"]
    assert cold.json()["meta"]["refresh"]["input_as_of"] == snapshot.as_of_date
    assert "本地证据已变化" in cold.json()["meta"]["refresh"]["reason"]

    frozen_factor = (
        service.store.frozen_adjustments / f"{snapshot.evidence_hashes['复权']}.parquet"
    )
    frozen_factor.unlink()
    service._detail_history_cache.clear()
    refused = client.get(
        "/api/v1/rotation/etfs/510300.SH",
        params={"snapshot_id": snapshot.snapshot_id},
    )
    assert refused.status_code == 409
    assert "ETF 历史证据不可复现" in refused.json()["detail"]


def test_etf_sandbox_preview_uses_local_pit_denominator_without_publishing(
    tmp_path,
    isolated_config,
    monkeypatch,
):
    monkeypatch.setattr(
        "quantmaster.rotation.etf_research.market_date",
        lambda: pd.Timestamp("2026-08-09").date(),
    )
    metadata = pd.DataFrame(
        [
            {
                "symbol": "510300.SH",
                "name": "沪深300ETF截止前",
                "benchmark": "沪深300指数",
                "asset_type": "etf",
                "list_date": "2012-05-28",
                "effective_date": "2026-08-07",
                "observed_at": "2026-08-07T06:30:00+00:00",
                "metadata_source": "free-stockdb:security-master",
            },
            {
                "symbol": "510300.SH",
                "name": "收盘后才知道的未来名称ETF",
                "benchmark": "未来指数",
                "asset_type": "etf",
                "list_date": "2012-05-28",
                "effective_date": "2026-08-07",
                "observed_at": "2026-08-07T08:00:00+00:00",
                "metadata_source": "free-stockdb:security-master",
            },
            *[
                {
                    "symbol": symbol,
                    "name": name,
                    "asset_type": "etf",
                    "list_date": listed,
                    "effective_date": "2026-08-07",
                    "observed_at": "2026-08-07T06:30:00+00:00",
                    "metadata_source": "free-stockdb:security-master",
                }
                for symbol, name, listed in (
                    ("159920.SZ", "恒生ETF(QDII)", "2012-10-22"),
                    ("511010.SH", "国债ETF", "2013-08-05"),
                )
            ],
            {
                "symbol": "512999.SH",
                "name": "已退市测试ETF",
                "asset_type": "etf",
                "status": "listed",
                "list_date": "2020-01-01",
                "effective_date": "2026-08-06",
                "observed_at": "2026-08-06T06:00:00+00:00",
                "metadata_source": "free-stockdb:security-master",
            },
            {
                "symbol": "512999.SH",
                "name": "已退市测试ETF",
                "asset_type": "etf",
                "status": "delisted",
                "list_date": "2020-01-01",
                "effective_date": "2026-08-07",
                "observed_at": "2026-08-07T06:00:00+00:00",
                "metadata_source": "free-stockdb:security-master",
            },
        ]
    )
    observations = pd.DataFrame(
        [
            {
                "symbol": "510300.SH",
                "trade_date": "2026-08-07",
                "shares": 1_000_000_000,
                "share_source": "free-stockdb:etf-share",
                "acquired_at": "2026-08-07T06:40:00+00:00",
            }
        ]
    )

    class LocalEvidenceStore:
        @staticmethod
        def etf_metadata_history():
            return metadata.copy()

        @staticmethod
        def etf_metadata():
            return metadata.copy()

        @staticmethod
        def etf_observations():
            return observations.copy()

    class LocalInstruments(_EtfInstruments):
        items: ClassVar[list[Instrument]] = [
            *[
                Instrument(
                    **{
                        **item.to_dict(),
                        "observed_at": pd.Timestamp(
                            "2026-08-07T06:00:00+00:00"
                        ).timestamp(),
                    }
                )
                for item in _EtfInstruments.items
            ],
            Instrument(
                "588999.SH",
                "588999",
                "未来上市ETF",
                "CN",
                "SH",
                "etf",
                status="listed",
                list_date="2026-08-09",
                observed_at=pd.Timestamp("2026-08-07T06:00:00+00:00").timestamp(),
            ),
        ]

    monkeypatch.setattr("quantmaster.rotation.store.RotationStore", LocalEvidenceStore)
    source = _EtfSource()
    source.frame = pd.concat(
        [
            source.frame,
            pd.DataFrame(
                [
                    {
                        "symbol": "510300.SH",
                        "date": "2026-08-10",
                        "open": 9.9,
                        "high": 10.1,
                        "low": 9.8,
                        "close": 10.0,
                        "volume": 1_000_000,
                        "amount": 10_000_000,
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    ingest_store = StockDBIngestStore(tmp_path / "ingest")
    research_store = EtfResearchStore(tmp_path / "research")
    service = EtfResearchService(
        source=source,
        instruments=LocalInstruments(),
        ingest_store=ingest_store,
        store=research_store,
    )

    with pytest.raises(RuntimeError, match="没有完整、可复验"):
        service.profiles(as_of="2026-08-08")
    with pytest.raises(RuntimeError, match="没有完整、可复验"):
        service.scan(as_of="2026-08-08")

    profiles = service.profiles(as_of="2026-08-08", tier="sandbox")
    assert [item.symbol for item in profiles] == ["159920.SZ", "510300.SH", "511010.SH"]
    assert (
        next(item for item in profiles if item.symbol == "510300.SH").name
        == "收盘后才知道的未来名称ETF"
    )

    preview = service.scan(as_of="2026-08-08", tier="sandbox")

    assert preview.tier == "sandbox"
    assert preview.formal_eligible is False
    assert preview.snapshot_id.startswith("etf_preview_")
    assert preview.as_of_date == "2026-08-07"
    assert preview.coverage["status"] == "degraded"
    assert preview.coverage["denominator"]["complete_market_denominator"] is False
    assert preview.coverage["denominator"]["as_of"] == "2026-08-07"
    assert preview.coverage["denominator"]["expected_symbols"] == 3
    assert next(item for item in preview.items if item.symbol == "510300.SH").name == "沪深300ETF截止前"
    assert all(
        member["sources"] and member["observed_at"]
        for member in preview.coverage["denominator"]["members"]
    )
    assert preview.capabilities["publication"] == {
        "tier": "sandbox",
        "formal_eligible": False,
        "status": "blocked",
        "reason": "sandbox 使用本地非完整母集，禁止发布为 production 快照",
    }
    assert research_store.latest() is None
    assert research_store.history() == []
    assert ingest_store.history() == []
    with pytest.raises(RuntimeError, match="仅接受正式 production"):
        research_store.publish(preview)
    with pytest.raises(RuntimeError, match="不存在或契约已淘汰"):
        service.product_history("510300.SH", snapshot_id=preview.snapshot_id)
    history = service.product_history(
        "510300.SH",
        snapshot_id=preview.snapshot_id,
        tier="sandbox",
    )
    assert history
    assert max(pd.Timestamp(item["date"]) for item in history) <= pd.Timestamp(
        preview.as_of_date
    )

    monkeypatch.setattr(
        "quantmaster.rotation.etf_research.get_etf_research_service",
        lambda **_kwargs: service,
    )
    client = TestClient(app)
    default_listing = client.get("/api/v1/rotation/etfs")
    production_detail = client.get(
        f"/api/v1/rotation/etfs/510300.SH?snapshot_id={preview.snapshot_id}"
    )
    sandbox_listing = client.get(
        f"/api/v1/rotation/etfs?tier=sandbox&snapshot_id={preview.snapshot_id}"
    )
    sandbox_overview = client.get(
        f"/api/v1/rotation/etfs/overview?tier=sandbox&snapshot_id={preview.snapshot_id}"
    )
    sandbox_detail = client.get(
        f"/api/v1/rotation/etfs/510300.SH?tier=sandbox&snapshot_id={preview.snapshot_id}"
    )

    assert default_listing.json()["data"]["items"] == []
    assert production_detail.status_code == 404
    assert sandbox_listing.status_code == 200
    assert sandbox_listing.json()["meta"]["formal_eligible"] is False
    assert len(sandbox_listing.json()["data"]["items"]) == 3
    assert sandbox_overview.json()["meta"]["tier"] == "sandbox"
    assert sandbox_detail.status_code == 200
    assert sandbox_detail.json()["meta"]["formal_eligible"] is False


def test_etf_sandbox_rejects_future_as_of_without_crossing_time(
    tmp_path,
    isolated_config,
    monkeypatch,
):
    monkeypatch.setattr(
        "quantmaster.rotation.etf_research.market_date",
        lambda: pd.Timestamp("2026-08-09").date(),
    )
    service = EtfResearchService(
        source=_EtfSource(),
        instruments=_EtfInstruments(),
        ingest_store=StockDBIngestStore(tmp_path / "ingest"),
        store=EtfResearchStore(tmp_path / "research"),
    )

    with pytest.raises(RuntimeError, match="晚于当前市场日"):
        service.profiles(as_of="2026-08-10", tier="sandbox")

    class EmptyLocalEvidenceStore:
        @staticmethod
        def etf_metadata_history():
            return pd.DataFrame()

        @staticmethod
        def etf_metadata():
            return pd.DataFrame()

        @staticmethod
        def etf_observations():
            return pd.DataFrame()

    monkeypatch.setattr(
        "quantmaster.rotation.store.RotationStore",
        EmptyLocalEvidenceStore,
    )
    with pytest.raises(RuntimeError, match="没有可用的沪深场内 ETF"):
        service.profiles(as_of="2026-08-08", tier="sandbox")

    monkeypatch.setattr(
        service.source,
        "intraday_many",
        lambda *_args: pd.DataFrame(
            {
                "symbol": ["510300.SH", "510300.SH", "510300.SH"],
                "date": [
                    "2026-08-08 09:29:00",
                    "2026-08-08 10:00:00",
                    "2026-08-09 10:00:00",
                ],
                "close": [1.0, 1.1, 9.9],
                "volume": [100, 200, 999],
                "amount": [100, 220, 9_890],
            }
        ),
    )
    intraday = service.intraday("510300.SH", as_of_date="2026-08-08")
    assert [item["time"] for item in intraday["series"]] == ["2026-08-08T10:00"]
    assert (
        service.store.root / "evidence" / "intraday" / "510300_SH_2026-08-08.parquet"
    ).is_file()
    with pytest.raises(ValueError, match="ETF 代码格式无效"):
        service.intraday("../../outside", as_of_date="2026-08-08")


def test_etf_metadata_only_change_reuses_compatible_local_daily_ingest(
    tmp_path,
    isolated_config,
    monkeypatch,
):
    monkeypatch.setattr(
        EtfResearchService, "_direct_share_observations", staticmethod(lambda: pd.DataFrame())
    )
    source = _EtfSource()
    instruments = _EtfInstruments()
    instruments.items = list(instruments.items)
    service = EtfResearchService(
        source=source,
        instruments=instruments,
        ingest_store=StockDBIngestStore(tmp_path / "ingest"),
        store=EtfResearchStore(tmp_path / "research"),
    )
    profiles = service.profiles(tier="sandbox")
    assert len(profiles) == 3
    updated_profiles = list(profiles)
    updated_profiles[0] = type(profiles[0])(
        **{**profiles[0].to_dict(), "name": "沪深300增强标注ETF"}
    )
    monkeypatch.setattr(
        service,
        "profiles",
        lambda as_of="", tier="production": updated_profiles,
    )
    service._profile_capabilities = {
        "status": "ready",
        "source": "test:immutable-catalog",
        "covered_symbols": len(updated_profiles),
    }
    identity = StockDBArtifactIdentity(
        artifact_id="test-etf-artifact",
        sdk={"available": True, "sha256": "1" * 64},
    )
    monkeypatch.setattr(source, "artifact_identity", lambda **_kwargs: identity)
    monkeypatch.setattr(
        "quantmaster.rotation.etf_research.market_date",
        lambda: pd.Timestamp("2026-08-08").date(),
    )
    future = source.frame.groupby("symbol", as_index=False).tail(1).copy()
    future["date"] = pd.Timestamp("2026-08-09")
    cached_daily = pd.concat([source.frame, future], ignore_index=True)
    first = service.ingest_store.publish_etf(
        daily=cached_daily,
        minutes=pd.DataFrame(),
        profiles=[item.to_dict() for item in profiles],
        as_of_date="2026-08-09",
        artifact_id=identity.artifact_id,
        master_snapshot_id="etf-master-before-metadata-refresh",
        start_date="2023-01-01",
        end_date="2026-08-09",
        coverage={"status": "complete", "symbol_ratio": 1.0},
        provenance={"source": "free-stockdb"},
        session_dates=sorted(
            cached_daily["date"].dt.strftime("%Y-%m-%d").unique().tolist()
        ),
    )

    second = service.scan()

    assert source.daily_calls == 0
    assert second.ingest_id != first.ingest_id
    republished = service.ingest_store.get(second.ingest_id)
    assert republished is not None
    assert republished.provenance["profile_refresh_from"] == first.ingest_id
    republished_daily = service.ingest_store.load_frame(republished, "etf_daily")
    assert pd.to_datetime(republished_daily["date"]).max() <= pd.Timestamp("2026-08-08")
    assert republished.end_date == second.as_of_date == "2026-08-07"
    assert republished.content_hashes["etf_daily"] != first.content_hashes["etf_daily"]


def test_etf_cli_contract_supports_cancel_and_resume():
    from quantmaster.cli import build_parser

    cancel = build_parser().parse_args(["etf-research", "cancel", "job-1"])
    resume = build_parser().parse_args(["etf-research", "resume", "job-1"])

    assert cancel.etf_research_cmd == "cancel" and cancel.job_id == "job-1"
    assert resume.etf_research_cmd == "resume" and resume.job_id == "job-1"


def test_etf_job_syncs_optional_evidence_before_research_and_keeps_warnings(monkeypatch):
    from quantmaster.rotation.etf_jobs import EtfResearchJobs

    calls: list[tuple[str, object]] = []

    class FakeProvider:
        def __init__(self, _store):
            pass

        def sync_etf_observations(self, _progress, _cancelled):
            calls.append(("sync", None))
            return {"issues": ["份额接口降级 fund_share"]}

    snapshot = SimpleNamespace(
        schema_version="3.0",
        snapshot_id="etf_test",
        ingest_id="ingest_test",
        artifact_id="upstream_test",
        input_hash="hash_test",
        to_dict=lambda: {"schema_version": "3.0", "snapshot_id": "etf_test"},
    )

    class FakeService:
        store = SimpleNamespace(record_failure=lambda _reason: None)

        @staticmethod
        def scan(**kwargs):
            calls.append(("scan", kwargs.get("refresh_warnings")))
            return snapshot

    context = SimpleNamespace(
        progress=lambda *_args: None,
        cancelled=lambda: False,
        write_artifact=lambda *_args: {"id": "artifact_test"},
    )
    monkeypatch.setattr("quantmaster.rotation.provider.RotationProvider", FakeProvider)
    monkeypatch.setattr("quantmaster.rotation.store.RotationStore", lambda: object())
    monkeypatch.setattr(
        "quantmaster.rotation.etf_jobs.get_etf_research_service",
        lambda: FakeService(),
    )

    outcome = EtfResearchJobs._handle(context, {"as_of": "2026-08-07"})

    assert calls == [("sync", None), ("scan", ["份额接口降级 fund_share"])]
    assert outcome.result_artifact_id == "artifact_test"
    assert "1 项证据已降级" in outcome.detail


def test_etf_sandbox_job_skips_remote_sync_and_emits_preview_artifact(monkeypatch):
    from quantmaster.rotation.etf_jobs import EtfResearchJobs

    calls: list[tuple[str, object]] = []
    writes: list[tuple[str, dict[str, object], dict[str, object]]] = []

    class FailProvider:
        def __init__(self, _store):
            raise AssertionError("sandbox 不得触发远端 ETF 证据同步")

    snapshot = SimpleNamespace(
        schema_version="3.0",
        snapshot_id="etf_preview_test",
        ingest_id="preview_sdi_test",
        artifact_id="upstream_test",
        input_hash="hash_test",
        to_dict=lambda: {
            "schema_version": "3.0",
            "snapshot_id": "etf_preview_test",
            "formal_eligible": False,
        },
    )

    class FakeService:
        store = SimpleNamespace(
            record_failure=lambda _reason: calls.append(("record_failure", _reason))
        )

        @staticmethod
        def scan(**kwargs):
            calls.append(("scan", (kwargs.get("tier"), kwargs.get("refresh_warnings"))))
            return snapshot

    def write_artifact(kind, payload, metadata):
        writes.append((kind, payload, metadata))
        return {"id": "artifact_preview"}

    context = SimpleNamespace(
        progress=lambda *_args: None,
        cancelled=lambda: False,
        write_artifact=write_artifact,
    )
    monkeypatch.setattr("quantmaster.rotation.provider.RotationProvider", FailProvider)
    monkeypatch.setattr(
        "quantmaster.rotation.etf_jobs.get_etf_research_service",
        lambda: FakeService(),
    )

    outcome = EtfResearchJobs._handle(
        context,
        {"as_of": "2026-08-07", "tier": "sandbox"},
    )

    assert calls == [("scan", ("sandbox", []))]
    assert writes[0][0] == "rotation.etf.preview"
    assert writes[0][2]["lineage"]["formal_eligible"] is False
    assert outcome.result_artifact_id == "artifact_preview"
    assert "不可发布" in outcome.detail


def test_etf_sandbox_job_public_result_exposes_preview_id():
    from quantmaster.rotation.etf_jobs import EtfResearchJobs

    jobs = object.__new__(EtfResearchJobs)
    jobs.runtime = SimpleNamespace(
        public=lambda value: {"id": value["id"], "status": value["status"]},
        store=SimpleNamespace(
            artifact=lambda artifact_id: {
                "id": artifact_id,
                "payload": {
                    "snapshot_id": "etf_preview_public",
                    "formal_eligible": False,
                },
            }
        ),
    )

    result = jobs.public(
        {
            "id": "job_preview",
            "status": "completed",
            "detail": "ETF 本地降级预览已生成（不可发布）",
            "spec": {"tier": "sandbox"},
            "result_artifact_id": "artifact_preview",
        }
    )

    assert result["tier"] == "sandbox"
    assert result["formal_eligible"] is False
    assert result["result"] == {
        "snapshot_id": "etf_preview_public",
        "preview_id": "etf_preview_public",
        "tier": "sandbox",
        "formal_eligible": False,
        "artifact_id": "artifact_preview",
    }


def test_experimental_online_endpoints_are_disabled_by_default(isolated_config):
    from quantmaster.data.free_stockdb_experimental import StockDBExperimentalOnline

    isolated_config.data.free_stockdb_experimental_tick_enabled = False
    isolated_config.data.free_stockdb_experimental_fundamentals_enabled = False
    service = StockDBExperimentalOnline(root=isolated_config.data_root / "experimental")
    with pytest.raises(PermissionError):
        service.tick("600000.SH")
    with pytest.raises(PermissionError):
        service.fundamentals("600000.SH", dataset="income", stat_date="2025q4")


def test_experimental_tick_is_quotad_cached_and_audited(tmp_path, isolated_config):
    from quantmaster.data.free_stockdb_experimental import StockDBExperimentalOnline

    calls = []
    module = SimpleNamespace(get_last_tick=lambda code, count: calls.append((code, count)) or [{"p": 1}])
    source = SimpleNamespace(_load_sdk_module=lambda: module)
    isolated_config.data.free_stockdb_experimental_tick_enabled = True
    isolated_config.data.free_stockdb_experimental_daily_quota = 2
    service = StockDBExperimentalOnline(source=source, root=tmp_path / "experimental")

    first = service.tick("600000.SH", count=2)
    second = service.tick("600000.SH", count=2)
    audits = [
        json.loads(line)
        for line in (tmp_path / "experimental" / "audit.jsonl").read_text(encoding="utf-8").splitlines()
    ]

    assert first["cached"] is False and second["cached"] is True
    assert calls == [("600000", 2)]
    assert service.status()["quota"]["count"] == 1
    assert [item["outcome"] for item in audits] == ["success", "success"]
    assert audits[-1]["cached"] is True


def test_stockdb_audit_api_discloses_same_upstream_and_experimental_state(
    monkeypatch,
    isolated_config,
):
    def must_not_run(*_args, **_kwargs):
        pytest.fail("audit GET must not call the StockDB provider")

    monkeypatch.setattr(FreeStockDBSource, "probe", must_not_run)
    monkeypatch.setattr(FreeStockDBSource, "board_hierarchy", must_not_run)
    monkeypatch.setattr(FreeStockDBSource, "security_catalog", must_not_run)
    monkeypatch.setattr(FreeStockDBSource, "delisted_catalog", must_not_run)

    response = TestClient(app).get("/api/v1/data-sources/free-stockdb/audit")

    assert response.status_code == 200
    payload = response.json()
    assert payload["upstream"] == "vendor-declared-unverified"
    assert payload["upstream_evidence"] == "not_provided"
    assert payload["distribution"] == "free-stockdb"
    assert payload["independent_cross_validation"] is False
    assert payload["capabilities"]["realtime_tick"]["state"] == "disabled"
    assert payload["capabilities"]["daily_bars"]["asset_classes"] == ["stock", "etf"]
    assert payload["capabilities"]["daily_bars"]["state"] == "unavailable"
    assert payload["capabilities"]["daily_bars"]["verified"] is False
    assert payload["capabilities"]["security_catalog"]["state"] == "unavailable"
    assert payload["capabilities"]["security_catalog"]["verified"] is False
    assert payload["experimental_online"]["max_concurrency"] == 2
    assert payload["probe"]["status"] == "not_run_in_request"
    assert payload["status"] == "unavailable"


def test_stockdb_audit_api_projects_latest_manifest_without_live_catalog(
    monkeypatch,
    isolated_config,
):
    store = StockDBIngestStore()
    store.publish(
        frame=pd.DataFrame({"symbol": ["600000.SH"], "date": ["2026-08-10"], "close": [10.0]}),
        boards=[{"code": "801000.SI", "level": "L1", "members": ["600000.SH"]}],
        catalog=[{"code": "600000"}],
        delisted=[{"code": "000001"}],
        as_of_date="2026-08-10",
        artifact_id="published-artifact",
        master_snapshot_id="master",
        start_date="2026-08-10",
        end_date="2026-08-10",
        coverage={"status": "locally_validated"},
        provenance={"source": "test"},
    )

    def must_not_run(*_args, **_kwargs):
        pytest.fail("published audit must not reload a live catalog")

    monkeypatch.setattr(FreeStockDBSource, "probe", must_not_run)
    monkeypatch.setattr(FreeStockDBSource, "security_catalog", must_not_run)
    monkeypatch.setattr(FreeStockDBSource, "board_hierarchy", must_not_run)
    response = TestClient(app).get("/api/v1/data-sources/free-stockdb/audit")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "degraded"
    assert payload["capabilities"]["daily_bars"]["state"] == "locally_validated"
    assert payload["catalog"] == {"securities": 1, "delisted_records": 1}
    assert payload["boards"]["total"] == 1
