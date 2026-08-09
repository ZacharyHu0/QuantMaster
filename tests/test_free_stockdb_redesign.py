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
from quantmaster.data.free_stockdb_contracts import StockDBArtifactIdentity
from quantmaster.data.free_stockdb_ingest import StockDBIngestService, StockDBIngestStore
from quantmaster.data.free_stockdb_source import FreeStockDBSource
from quantmaster.data.instruments import Instrument
from quantmaster.research.adapters import StockDBResearchAdapter
from quantmaster.research.catalog import ResearchCatalog
from quantmaster.rotation.etf_research import (
    EtfResearchService,
    EtfResearchStore,
    classify_etf,
    is_exchange_etf,
)
from quantmaster.server.app import app


def test_local_stockdb_advertises_eod_but_not_realtime_spot():
    assert DataCapability.EOD_SNAPSHOT in FreeStockDBSource.capabilities
    assert DataCapability.REALTIME_TICK not in FreeStockDBSource.capabilities
    assert DataCapability.SPOT not in FreeStockDBSource.capabilities


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
        provenance={"cache_key": "same", "upstream": "tushare", "distribution": "free-stockdb"},
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
    assert research["price_adjustment"].unique().tolist() == ["qfq_from_frozen_factor_v1"]


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
        Instrument("510300.SH", "510300", "沪深300ETF", "CN", "SH", "fund", status="listed"),
        Instrument("159920.SZ", "159920", "恒生ETF(QDII)", "CN", "SZ", "fund", status="listed"),
        Instrument("511010.SH", "511010", "国债ETF", "CN", "SH", "etf", status="listed"),
        Instrument("501001.SH", "501001", "测试LOF", "CN", "SH", "fund", status="listed"),
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


def test_etf_scan_builds_v3_sector_radar_and_loads_minutes_only_on_demand(
    tmp_path,
    isolated_config,
    monkeypatch,
):
    monkeypatch.setattr(
        EtfResearchService, "_direct_share_observations", staticmethod(lambda: pd.DataFrame())
    )
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
    assert snapshot.research_model_version == "QM_ETF_SECTOR_RADAR_V3.4"
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
        lambda: service,
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
    assert source.intraday_calls == 1
    assert intraday.json()["data"]["metrics"]["complete_session"] is True
    assert len(intraday.json()["data"]["series"]) == 241
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

    first = service.scan(as_of="2026-08-08")
    original = instruments.items[0]
    instruments.items[0] = Instrument(
        symbol=original.symbol,
        code=original.code,
        name="沪深300增强标注ETF",
        market=original.market,
        exchange=original.exchange,
        asset_type=original.asset_type,
        status=original.status,
    )
    second = service.scan(as_of="2026-08-08")

    assert source.daily_calls == 1
    assert second.snapshot_id != first.snapshot_id
    assert second.ingest_id != first.ingest_id
    republished = service.ingest_store.get(second.ingest_id)
    assert republished is not None
    assert republished.provenance["profile_refresh_from"] == first.ingest_id
    assert republished.content_hashes["etf_daily"] == service.ingest_store.get(
        first.ingest_id
    ).content_hashes["etf_daily"]


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
    monkeypatch.setattr(FreeStockDBSource, "probe", lambda self: {"status": "ok"})
    monkeypatch.setattr(
        FreeStockDBSource,
        "board_hierarchy",
        lambda self: [
            {"code": "801000.SI", "level": "L1", "members": ["600000.SH"]},
        ],
    )
    monkeypatch.setattr(FreeStockDBSource, "security_catalog", lambda self: [{"code": "600000"}])
    monkeypatch.setattr(FreeStockDBSource, "delisted_catalog", lambda self: [])
    monkeypatch.setattr(
        FreeStockDBSource,
        "artifact_identity",
        lambda self, **kwargs: StockDBArtifactIdentity.discover(
            None,
            None,
            data_session=kwargs.get("data_session", ""),
        ),
    )

    response = TestClient(app).get("/api/v1/data-sources/free-stockdb/audit")

    assert response.status_code == 200
    payload = response.json()
    assert payload["upstream"] == "tushare"
    assert payload["distribution"] == "free-stockdb"
    assert payload["independent_cross_validation"] is False
    assert payload["capabilities"]["realtime_tick"]["state"] == "disabled"
    assert payload["capabilities"]["daily_bars"]["asset_classes"] == ["stock", "etf"]
    assert payload["experimental_online"]["max_concurrency"] == 2


def test_stockdb_audit_api_redacts_probe_exception_details(monkeypatch, isolated_config):
    internal = r"C:\private\stockdb.sqlite Bearer secret-value"

    def fail_probe(_self):
        raise RuntimeError(internal)

    monkeypatch.setattr(FreeStockDBSource, "probe", fail_probe)
    response = TestClient(app).get("/api/v1/data-sources/free-stockdb/audit")

    assert response.status_code == 200
    assert response.json()["issues"] == ["连接探测失败；详细信息已写入本机日志"]
    assert "private" not in response.text
    assert "secret-value" not in response.text
