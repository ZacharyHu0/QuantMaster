from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from quantmaster.data.free_stockdb_ingest import StockDBIngestStore
from quantmaster.data.instrument_snapshots import (
    TUSHARE_CATALOG_QUERY,
    freeze_instrument_catalog,
)
from quantmaster.data.instruments import Instrument
from quantmaster.data.resilience import PROVIDER_HEALTH
from quantmaster.research.contracts import content_hash
from quantmaster.rotation.etf_models import (
    ETF_RESEARCH_MODEL_VERSION,
    EtfProfile,
    EtfResearchItem,
    EtfResearchSnapshot,
)
from quantmaster.rotation.etf_research import (
    _ADJUSTMENT_COLUMNS,
    EtfResearchService,
    EtfResearchStore,
    _frame_hash,
)
from quantmaster.rotation.etf_v2 import (
    adjusted_daily_metrics,
    build_sector_research,
    classify_etf_profile,
    fund_evidence,
    normalize_index_name,
)
from quantmaster.server.rotation import _etf_overview_payload, _etf_refresh_hint
from tests.catalog_evidence_helpers import bound_tushare_catalog


def _profile(
    symbol: str,
    name: str,
    *,
    benchmark_code: str,
) -> EtfProfile:
    taxonomy = classify_etf_profile(
        name,
        benchmark_code=benchmark_code,
        index_name=name.replace("ETF", "指数"),
        metadata_source="etf_basic",
    )
    return EtfProfile(
        symbol=symbol,
        name=name,
        category=taxonomy["category"],
        asset_class=taxonomy["asset_class"],
        sector_id=taxonomy["sector_id"],
        sector_name=taxonomy["sector_name"],
        benchmark_code=benchmark_code,
        normalized_index=taxonomy["normalized_index"],
        metadata_source="etf_basic",
        classification_source=taxonomy["classification_source"],
        classification_confidence=taxonomy["classification_confidence"],
        classification_evidence=taxonomy["classification_evidence"],
    )


def _metrics(**updates) -> dict:
    value = {
        "return_5d": 0.0,
        "return_20d": 0.0,
        "return_60d": 0.0,
        "ma20_slope": 0.0,
        "position_20d": 50.0,
        "position_60d": 50.0,
        "position_250d": 50.0,
        "drawdown_250d": -0.1,
        "avg_amount_20d": 50_000_000.0,
        "amount_ratio_5v20": 1.0,
        "volatility_20d": 0.01,
        "above_ma20": False,
        "ma20_above_ma60": False,
        "adjustment_status": "official",
        "history": [],
    }
    value.update(updates)
    return value


def _row(profile: EtfProfile, **metric_updates) -> dict:
    return {
        "profile": profile,
        "metrics": _metrics(**metric_updates),
        "funds": {
            "status": "missing",
            "share": None,
            "share_delta": None,
            "share_change_pct": None,
            "estimated_flow": None,
            "effective_date": "",
            "source": "",
        },
        "total_size": None,
    }


class _MutableReplaySource:
    def __init__(self, frame: pd.DataFrame):
        self.frame = frame
        self.calls = 0

    def daily_cross_section(self, *_args):
        self.calls += 1
        return self.frame.copy()


def _published_replay_snapshot(tmp_path, *, freeze_factors: bool = True):
    dates = pd.bdate_range(end="2026-08-07", periods=80)
    daily = pd.DataFrame(
        {
            "symbol": "510300.SH",
            "date": dates,
            "open": np.linspace(3.5, 4.0, len(dates)),
            "high": np.linspace(3.6, 4.1, len(dates)),
            "low": np.linspace(3.4, 3.9, len(dates)),
            "close": np.linspace(3.5, 4.0, len(dates)),
            "volume": 1_000_000.0,
            "amount": 4_000_000.0,
            "pct_chg": 0.1,
        }
    )
    factors = pd.DataFrame(
        {
            "symbol": "510300.SH",
            "date": dates,
            "adj_factor": np.where(np.arange(len(dates)) < 40, 1.0, 2.0),
            "source": "tushare:fund_adj",
            "acquired_at": pd.Timestamp("2026-08-07T15:30:00+08:00"),
        }
    )
    ingest_store = StockDBIngestStore(tmp_path / "ingest")
    ingest = ingest_store.publish_etf(
        daily=daily,
        minutes=pd.DataFrame(),
        profiles=[{"symbol": "510300.SH"}],
        as_of_date="2026-08-07",
        artifact_id="stockdb-artifact-a",
        master_snapshot_id="etf-master-a",
        start_date=dates.min().date().isoformat(),
        end_date="2026-08-07",
        coverage={"symbol_ratio": 1.0},
        provenance={"source": "test-stockdb"},
    )
    store = EtfResearchStore(tmp_path / "research")
    adjustment_hash = _frame_hash(factors, _ADJUSTMENT_COLUMNS)
    if freeze_factors:
        store.freeze_adjustments(factors, adjustment_hash)
    evidence_hashes = {
        "行情": content_hash(ingest.content_hashes),
        "复权": adjustment_hash,
    }
    input_hash = content_hash(
        {
            "ingest_id": ingest.ingest_id,
            "research_model_version": ETF_RESEARCH_MODEL_VERSION,
            "evidence_hashes": evidence_hashes,
        }
    )
    snapshot_id = "etf_" + hashlib.sha256(
        f"2026-08-07:{ETF_RESEARCH_MODEL_VERSION}:{input_hash}".encode()
    ).hexdigest()[:24]
    item = EtfResearchItem(
        symbol="510300.SH",
        name="沪深300ETF",
        category="境内宽基",
        asset_class="equity",
        sector_id="equity:hs300",
        sector_name="沪深300",
        normalized_index="沪深300",
        benchmark_code="000300.SH",
        is_representative=True,
        representative_symbol="510300.SH",
        metrics={},
        funds={},
        metadata={},
        coverage={},
        provenance={},
        as_of_date="2026-08-07",
        snapshot_id=snapshot_id,
        ingest_id=ingest.ingest_id,
        artifact_id=ingest.artifact_id,
    )
    snapshot = store.publish(
        EtfResearchSnapshot(
            snapshot_id=snapshot_id,
            ingest_id=ingest.ingest_id,
            artifact_id=ingest.artifact_id,
            as_of_date="2026-08-07",
            coverage={},
            provenance={},
            items=(item,),
            sectors=(),
            queues={},
            candidate_queues={},
            summaries=(),
            freshness={},
            capabilities={},
            evidence_hashes=evidence_hashes,
            categories=("境内宽基",),
            input_hash=input_hash,
        )
    )
    return store, ingest_store, snapshot, daily, factors


def test_industry_classification_precedes_broad_full_index_token():
    result = classify_etf_profile(
        "广发中证全指医药卫生ETF",
        index_name="中证全指医药卫生指数",
        benchmark_code="000991.CSI",
        metadata_source="etf_basic",
    )

    assert result["category"] == "行业主题"
    assert result["sector_name"] == "医药"
    assert result["classification_confidence"] == 1.0


def test_official_benchmark_type_and_normalized_index_prevent_misc_bucket_collapse():
    broad = classify_etf_profile(
        "样本ETF",
        index_name="上证180指数",
        benchmark_code="000010.SH",
        benchmark_type="宽基",
        index_type="规模类指数",
        metadata_source="etf_basic",
    )
    satellite = classify_etf_profile(
        "卫星ETF",
        index_name="中证卫星产业指数",
        benchmark="中证卫星产业指数",
    )
    aerospace = classify_etf_profile(
        "航天ETF",
        index_name="中证商业航天指数",
        benchmark="中证商业航天指数",
    )
    industrial_a = classify_etf_profile("大成中证工业互联网主题ETF")
    industrial_b = classify_etf_profile("华夏中证工业互联网主题ETF")
    domestic_broad = classify_etf_profile("建信中证A股ETF")

    assert broad["category"] == "境内宽基"
    assert broad["sector_name"] == "上证180"
    assert "mkt_idx_bmk" in broad["classification_source"]
    assert satellite["sector_name"] == "商业航天"
    assert aerospace["sector_name"] == "商业航天"
    assert satellite["sector_id"] == aerospace["sector_id"]
    assert industrial_a["sector_name"] == "工业互联网"
    assert industrial_a["sector_id"] == industrial_b["sector_id"]
    assert domestic_broad["category"] == "境内宽基"
    assert domestic_broad["sector_name"] == "中证A股"


@pytest.mark.parametrize(
    ("name", "sector"),
    (
        ("华安创业板50ETF", "创业板50"),
        ("易方达深证50ETF", "深证50"),
        ("华夏科创创业50ETF", "科创创业50"),
    ),
)
def test_ambiguous_50_etf_names_do_not_collapse_into_shanghai_50(name, sector):
    result = classify_etf_profile(name)

    assert result["sector_name"] == sector
    assert result["normalized_index"] == sector
    assert result["sector_name"] != "上证50"


def test_index_formula_suffix_is_removed_without_erasing_economic_name():
    assert normalize_index_name("中证全指医药卫生指数×100%收益率公式") == "中证全指医药卫生"
    assert normalize_index_name("恒生科技指数（全收益）") == "恒生科技"


def test_bilingual_index_alias_order_collapses_to_one_readable_name():
    canonical = "MSCI中国A股国际通"
    assert normalize_index_name("MSCIChinaAInclusionRMBIndex(MSCI中国A股国际通指数)") == canonical
    assert normalize_index_name("MSCI中国A股国际通指数(MSCIChinaAInclusionRMBIndex)") == canonical
    assert normalize_index_name("MSCIChinaAInclusionRMBIndex(MSCI中国A股国际通指数)×100%") == canonical
    assert (
        normalize_index_name("MSCI中国A股国际通指数(MSCIChinaAInclusionRMBIndex)收益率×100%")
        == canonical
    )


@pytest.mark.parametrize(
    ("name", "sector"),
    (
        ("港股通汽车ETF", "港股汽车"),
        ("恒生科技ETF", "港股科技"),
        ("港股金融ETF", "港股金融"),
        ("港股红利低波ETF", "港股红利"),
    ),
)
def test_overseas_region_is_resolved_before_subtheme(name, sector):
    result = classify_etf_profile(name)

    assert result["asset_class"] == "overseas_equity"
    assert result["sector_name"] == sector


def test_adjustment_evidence_removes_split_jump_and_guards_long_position():
    dates = pd.bdate_range("2025-07-01", periods=260)
    economic = np.linspace(100.0, 130.0, len(dates))
    raw = economic.copy()
    raw[130:] = raw[130:] / 2
    pct = pd.Series(economic).pct_change().fillna(0) * 100
    daily = pd.DataFrame(
        {
            "date": dates,
            "close": raw,
            "pct_chg": pct,
            "amount": 100_000_000.0,
        }
    )
    factors = pd.DataFrame(
        {
            "date": dates,
            "adj_factor": np.where(np.arange(len(dates)) < 130, 1.0, 2.0),
        }
    )

    official = adjusted_daily_metrics(daily, factors)
    chained = adjusted_daily_metrics(daily)

    assert official["adjustment_status"] == "verified_local"
    assert 0 < official["return_20d"] < 0.05
    assert official["position_250d"] is not None
    assert chained["adjustment_status"] == "return_chain"
    assert 0 < chained["return_20d"] < 0.05
    assert chained["position_250d"] is None

    sparse = daily.copy()
    sparse.loc[: len(sparse) - 29, "pct_chg"] = np.nan
    guarded = adjusted_daily_metrics(sparse)
    assert guarded["adjustment_status"] == "raw_short_fallback"
    assert 0 < guarded["return_60d"] < 0.07
    assert guarded["position_250d"] is None

    suspicious = daily.copy()
    suspicious["pct_chg"] = np.nan
    suspicious.loc[len(suspicious) - 10 :, "close"] /= 2
    rejected = adjusted_daily_metrics(suspicious)
    assert rejected["adjustment_status"] == "unavailable"
    assert rejected["return_20d"] is None


def test_prepared_daily_metrics_match_the_normalized_public_path():
    dates = pd.bdate_range("2025-07-01", periods=260)
    daily = pd.DataFrame(
        {
            "date": dates,
            "close": np.linspace(4.0, 4.8, len(dates)),
            "pct_chg": pd.Series(np.linspace(4.0, 4.8, len(dates))).pct_change().fillna(0) * 100,
            "amount": np.linspace(80_000_000, 120_000_000, len(dates)),
        }
    )
    factors = pd.DataFrame(
        {
            "date": dates,
            "adj_factor": np.where(np.arange(len(dates)) < 130, 1.0, 1.2),
            "source": "free-stockdb:cum-factor-events",
        }
    )

    normalized = adjusted_daily_metrics(daily, factors)
    prepared = adjusted_daily_metrics(daily, factors, prepared=True)

    assert prepared == normalized


def test_stockdb_sparse_adjustment_events_expand_to_verified_daily_factors(tmp_path):
    dates = pd.bdate_range("2025-07-01", periods=260)
    economic = np.linspace(100.0, 130.0, len(dates))
    raw = economic.copy()
    raw[130:] /= 2
    daily = pd.DataFrame(
        {
            "symbol": "510300.SH",
            "date": dates,
            "close": raw,
            "amount": 100_000_000.0,
        }
    )

    class LocalFactors:
        calls = 0

        def adjustment_factors(self, symbols, start, end):
            self.calls += 1
            assert symbols == ["510300.SH"]
            return pd.DataFrame(
                {
                    "symbol": ["510300.SH"],
                    "date": [dates[130]],
                    "adj_factor": [2.0],
                }
            )

    source = LocalFactors()
    service = EtfResearchService(
        source=source,
        instruments=object(),
        ingest_store=object(),
        store=EtfResearchStore(tmp_path / "research"),
    )
    factors, capability = service._adjustment_factors(
        daily,
        progress=lambda *_: None,
        cancelled=lambda: False,
    )
    metrics = adjusted_daily_metrics(daily, factors)

    assert source.calls == 1
    assert len(factors) == len(dates)
    assert factors["source"].eq("free-stockdb:cum-factor-events").all()
    assert capability["coverage"] == 1.0
    assert capability["source"] == "free-stockdb:cum-factor-events"
    assert metrics["adjustment_status"] == "verified_local"
    assert metrics["position_250d"] is not None
    assert 0 < metrics["return_20d"] < 0.05


def test_stockdb_partial_adjustment_events_do_not_certify_other_products(
    tmp_path, monkeypatch,
):
    dates = pd.bdate_range("2026-07-01", periods=20)
    daily = pd.concat(
        [
            pd.DataFrame(
                {
                    "symbol": symbol,
                    "date": dates,
                    "close": np.linspace(4.0, 4.2, len(dates)),
                }
            )
            for symbol in ("510300.SH", "510500.SH")
        ],
        ignore_index=True,
    )

    class PartialEvents:
        def adjustment_factors(self, *_args):
            return pd.DataFrame(
                {
                    "symbol": ["510300.SH"],
                    "date": [dates[5]],
                    "adj_factor": [1.2],
                }
            )

    class Config:
        data = type("Data", (), {"tushare_token": ""})()

    monkeypatch.setattr("quantmaster.rotation.etf_research.get_config", lambda: Config())
    service = EtfResearchService(
        source=PartialEvents(),
        instruments=object(),
        ingest_store=object(),
        store=EtfResearchStore(tmp_path / "research"),
    )

    factors, capability = service._adjustment_factors(
        daily,
        progress=lambda *_: None,
        cancelled=lambda: False,
    )

    assert set(factors["symbol"]) == {"510300.SH"}
    assert capability["status"] != "ready"
    assert capability["coverage"] == 0.5


def test_stockdb_empty_adjustment_table_cannot_be_authoritative(tmp_path, monkeypatch):
    dates = pd.bdate_range("2026-07-01", periods=20)
    daily = pd.DataFrame(
        {"symbol": "510300.SH", "date": dates, "close": np.linspace(4.0, 4.2, 20)}
    )

    class EmptyEvents:
        def adjustment_factors(self, *_args):
            result = pd.DataFrame(columns=["symbol", "date", "adj_factor"])
            result.attrs["authoritative"] = True
            return result

    class Config:
        data = type("Data", (), {"tushare_token": ""})()

    monkeypatch.setattr("quantmaster.rotation.etf_research.get_config", lambda: Config())
    service = EtfResearchService(
        source=EmptyEvents(),
        instruments=object(),
        ingest_store=object(),
        store=EtfResearchStore(tmp_path / "research"),
    )

    factors, capability = service._adjustment_factors(
        daily,
        progress=lambda *_: None,
        cancelled=lambda: False,
    )

    assert factors.empty
    assert capability["status"] == "unavailable"
    assert capability["coverage"] == 0.0


def test_adjustment_refresh_does_not_wait_for_remote_fund_adj(
    tmp_path, monkeypatch, isolated_config,
):
    dates = pd.bdate_range("2026-07-01", periods=20)
    daily = pd.DataFrame(
        {"symbol": "510300.SH", "date": dates, "close": np.linspace(4.0, 4.2, 20)}
    )

    class EmptyLocalFactors:
        def adjustment_factors(self, *_args):
            return pd.DataFrame(columns=["symbol", "date", "adj_factor"])

    remote_calls = 0

    def fail_remote(*_args, **_kwargs):
        nonlocal remote_calls
        remote_calls += 1
        raise AssertionError("foreground ETF research must not call remote fund_adj")

    from quantmaster.data.tushare_source import TushareSource

    isolated_config.data.tushare_token = "configured-token"
    monkeypatch.setattr(TushareSource, "_call", fail_remote)
    service = EtfResearchService(
        source=EmptyLocalFactors(),
        instruments=object(),
        ingest_store=object(),
        store=EtfResearchStore(tmp_path / "research"),
    )

    factors, capability = service._adjustment_factors(
        daily,
        progress=lambda *_: None,
        cancelled=lambda: False,
    )

    assert factors.empty
    assert remote_calls == 0
    assert capability["status"] == "unavailable"
    assert "不串行等待远程 fund_adj" in capability["reason"]


def test_historical_adjustment_rejects_late_or_invalid_factor_evidence(
    tmp_path, monkeypatch,
):
    dates = pd.bdate_range("2026-07-01", periods=20)
    daily = pd.DataFrame(
        {
            "symbol": "510300.SH",
            "date": dates,
            "close": np.linspace(4.0, 4.2, len(dates)),
        }
    )

    class NoFactors:
        def adjustment_factors(self, *_args):
            raise RuntimeError("factor source offline")

    class Config:
        data = type("Data", (), {"tushare_token": ""})()

    store = EtfResearchStore(tmp_path / "research")
    target = store.root / "evidence" / "adjustment_factors.parquet"
    target.parent.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "symbol": "510300.SH",
                "date": value,
                "adj_factor": 1.0,
                "source": "tushare:fund_adj",
                "acquired_at": "2026-08-09T01:00:00+00:00",
            }
            for value in dates
        ]
        + [
            {
                "symbol": "510300.SH",
                "date": value,
                "adj_factor": -1.0,
                "source": "tushare:fund_adj",
                "acquired_at": "2026-08-08T06:00:00+00:00",
            }
            for value in dates
        ]
    ).to_parquet(target, index=False)
    monkeypatch.setattr("quantmaster.rotation.etf_research.get_config", lambda: Config())
    service = EtfResearchService(
        source=NoFactors(),
        instruments=object(),
        ingest_store=object(),
        store=store,
    )

    factors, capability = service._adjustment_factors(
        daily,
        progress=lambda *_: None,
        cancelled=lambda: False,
        as_of="2026-08-08",
    )

    assert factors.empty
    assert capability["status"] != "ready"
    assert capability["coverage"] == 0.0


def test_tushare_factor_source_remains_distinct_from_verified_local():
    dates = pd.bdate_range("2025-07-01", periods=260)
    daily = pd.DataFrame(
        {"date": dates, "close": np.linspace(100, 130, len(dates)), "amount": 1_000_000}
    )
    factors = pd.DataFrame(
        {
            "date": dates,
            "adj_factor": 1.0,
            "source": "tushare:fund_adj",
        }
    )

    result = adjusted_daily_metrics(daily, factors)

    assert result["adjustment_status"] == "official"
    assert result["adjustment_source"] == "tushare:fund_adj"


def test_official_metadata_skips_known_denied_etf_basic(isolated_config):
    isolated_config.data.tushare_token = "test-token"
    PROVIDER_HEALTH.failure(
        "tushare:etf_basic", RuntimeError("etf_basic permission denied"), immediate=True,
    )

    metadata, capability = EtfResearchService._official_metadata()

    assert metadata == {}
    assert capability["status"] == "fallback"
    assert "已按当前凭据跳过" in capability["reason"]
    health = PROVIDER_HEALTH.status("tushare:etf_basic")["tushare:etf_basic"]
    assert health["suppressed"] == 0


def test_fund_basic_is_official_directory_without_claiming_etf_basic_enhancement(
    tmp_path, monkeypatch,
):
    catalog = [
        {
            "symbol": "510300.SH",
            "name": "沪深300ETF",
            "market": "CN",
            "exchange": "SH",
            "asset_type": "etf",
            "status": "L",
            "list_date": "2012-05-28",
            "delist_date": "",
        },
        *[
            {
                "symbol": f"{560000 + index:06d}.SH",
                "name": f"未来占位{index}ETF",
                "market": "CN",
                "exchange": "SH",
                "asset_type": "etf",
                "status": "P",
                "list_date": "2099-01-01",
                "delist_date": "",
            }
            for index in range(99)
        ],
        *[
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
        ],
    ]
    monkeypatch.setattr(
        "quantmaster.data.instrument_snapshots.TUSHARE_MINIMUM_ASSET_COUNTS",
        {"CN:stock": 3000, "CN:etf": 1},
    )
    catalog = [item for item in catalog if item["status"] != "P"]
    catalog, catalog_outcomes = bound_tushare_catalog(catalog)
    freeze_instrument_catalog(
        catalog,
        source="tushare:catalog",
        query=TUSHARE_CATALOG_QUERY,
        request_outcomes=catalog_outcomes,
        acquired_at=pd.Timestamp("2026-08-09T15:01:00+08:00").to_pydatetime(),
    )
    monkeypatch.setattr(
        "quantmaster.rotation.etf_research.market_date",
        lambda: pd.Timestamp("2026-08-09").date(),
    )
    metadata = pd.DataFrame(
        [
            {
                "symbol": "510300.SH",
                "name": "沪深300ETF",
                "benchmark": np.nan,
                "normalized_index": "沪深300",
                "mgt_fee": "",
                "metadata_source": "tushare:fund_basic",
                "updated_at": "2026-08-09",
            }
        ]
    )

    class FakeRotationStore:
        def etf_metadata(self):
            return metadata

        def etf_observations(self):
            return pd.DataFrame(
                {
                    "symbol": ["510300.SH"],
                    "trade_date": ["2026-08-07"],
                    "benchmark": [pd.NA],
                    "fund_type": [pd.NA],
                    "invest_type": [pd.NA],
                }
            )

    class OneInstrument:
        def list(self, *, market=""):
            return [
                Instrument(
                    symbol="510300.SH",
                    code="510300",
                    name="沪深300ETF",
                    market="CN",
                    exchange="SH",
                    asset_type="etf",
                )
            ]

    monkeypatch.setattr("quantmaster.rotation.store.RotationStore", FakeRotationStore)
    service = EtfResearchService(
        source=object(),
        instruments=OneInstrument(),
        ingest_store=object(),
        store=EtfResearchStore(tmp_path / "research"),
    )

    profiles = service.profiles()

    assert profiles[0].management_fee is None
    assert profiles[0].metadata_source == "fund_basic"
    assert profiles[0].normalized_index == "沪深300"
    assert profiles[0].benchmark == ""
    assert service._profile_capabilities["official_covered_symbols"] == 1
    assert service._profile_capabilities["enhanced_covered_symbols"] == 0


def test_current_profiles_degrade_to_local_master_without_catalog_artifact(
    tmp_path, monkeypatch,
):
    class EmptyRotationStore:
        @staticmethod
        def etf_metadata_history():
            return pd.DataFrame()

        @staticmethod
        def etf_observations():
            return pd.DataFrame()

    class OneMutableInstrument:
        @staticmethod
        def list(*, market=""):
            return [
                Instrument(
                    "510300.SH",
                    "510300",
                    "沪深300ETF",
                    "CN",
                    "SH",
                    "etf",
                    list_date="2012-05-28",
                )
            ]

    monkeypatch.setattr("quantmaster.rotation.store.RotationStore", EmptyRotationStore)
    monkeypatch.setattr(
        "quantmaster.rotation.etf_research.market_date",
        lambda: pd.Timestamp("2026-08-09").date(),
    )
    service = EtfResearchService(
        source=object(),
        instruments=OneMutableInstrument(),
        ingest_store=object(),
        store=EtfResearchStore(tmp_path / "research"),
    )

    profiles = service.profiles()

    assert [item.symbol for item in profiles] == ["510300.SH"]
    assert service._profile_capabilities["status"] == "degraded"
    assert service._profile_capabilities["publication_allowed"] is True
    assert service._profile_capabilities["denominator"]["complete_market_denominator"] is False
    assert service._profile_capabilities["denominator"]["formal_eligible"] is True
    assert "stockdb 与本地缓存" in service._profile_capabilities["reason"]

    with pytest.raises(RuntimeError, match="没有完整、可复验"):
        service.profiles(as_of="2026-08-09")


def test_default_research_target_prefers_validated_stockdb_session(monkeypatch):
    monkeypatch.setattr(
        "quantmaster.rotation.etf_research.market_date",
        lambda: pd.Timestamp("2026-08-11").date(),
    )
    monkeypatch.setattr(
        "quantmaster.data.free_stockdb_runtime.free_stockdb_runtime.status",
        lambda: {"validated_session": "2026-08-10"},
    )

    target, source = EtfResearchService._research_target()

    assert target == pd.Timestamp("2026-08-10")
    assert source == "free-stockdb:validated-session"


def test_etf_overview_compacts_local_denominator_members():
    snapshot = SimpleNamespace(
        sectors=(),
        queues={},
        candidate_queues={},
        freshness={},
        capabilities={
            "metadata": {
                "status": "degraded",
                "denominator": {
                    "observed_symbols": 2,
                    "coverage": 1.0,
                    "members": [{"symbol": "510300.SH"}, {"symbol": "159919.SZ"}],
                },
            }
        },
    )

    payload = _etf_overview_payload(snapshot, "equity")

    denominator = payload["capabilities"]["metadata"]["denominator"]
    assert denominator["member_count"] == 2
    assert "members" not in denominator
    assert len(snapshot.capabilities["metadata"]["denominator"]["members"]) == 2


def test_etf_refresh_hint_reuses_unchanged_file_hashes_without_loading_frames(
    tmp_path,
    monkeypatch,
):
    factor_path = tmp_path / "research" / "evidence" / "adjustment_factors.parquet"
    share_path = tmp_path / "rotation" / "etf_observations.parquet"
    metadata_path = tmp_path / "rotation" / "etf_metadata.parquet"
    for path in (factor_path, share_path, metadata_path):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"immutable-local-evidence")

    class LocalRotationStore:
        etf_path = share_path
        etf_metadata_path = metadata_path

        def __init__(self, *, read_only: bool = False):
            assert read_only is True

    monkeypatch.setattr("quantmaster.rotation.store.RotationStore", LocalRotationStore)
    latest_input = SimpleNamespace(
        assets=("etf",),
        content_hashes={"etf_daily": "daily-hash"},
        ingest_id="sdi_latest",
        as_of_date="2026-08-10",
    )
    service = SimpleNamespace(
        ingest_store=SimpleNamespace(history=lambda _limit: [latest_input]),
        store=SimpleNamespace(root=tmp_path / "research"),
        _direct_share_observations=lambda: pytest.fail("不应读取份额明细"),
        _direct_metadata=lambda: pytest.fail("不应读取元数据明细"),
    )
    snapshot = SimpleNamespace(
        generated_at="2030-01-01T00:00:00+00:00",
        evidence_hashes={
            "行情": content_hash(latest_input.content_hashes),
            "份额": "share-hash",
            "复权": "factor-hash",
            "元数据源": "metadata-source-hash",
        },
    )

    hint = _etf_refresh_hint(service, snapshot)

    assert hint["recommended"] is False
    assert hint["input_as_of"] == "2026-08-10"
    assert "已使用最新" in hint["reason"]


def test_duplicate_same_index_product_does_not_change_sector_trend_input():
    liquid = _profile("512480.SH", "半导体ETF", benchmark_code="H30184.CSI")
    duplicate = _profile("159999.SZ", "半导体ETF", benchmark_code="H30184.CSI")
    second_index = _profile("588200.SH", "半导体ETF", benchmark_code="931865.CSI")
    base = [
        _row(liquid, return_20d=0.10, avg_amount_20d=200_000_000),
        _row(second_index, return_20d=0.20, avg_amount_20d=80_000_000),
    ]
    with_duplicate = [
        *base,
        _row(duplicate, return_20d=-0.80, avg_amount_20d=1_000_000),
    ]

    sectors_base, representatives_base, *_ = build_sector_research(base)
    sectors_duplicate, representatives_duplicate, *_ = build_sector_research(with_duplicate)

    assert sectors_base[0]["metrics"]["return_20d"] == pytest.approx(0.15)
    assert sectors_duplicate[0]["metrics"]["return_20d"] == pytest.approx(0.15)
    assert representatives_base[liquid.symbol] == liquid.symbol
    assert representatives_duplicate[duplicate.symbol] == liquid.symbol


def test_same_index_label_in_different_sectors_keeps_one_representative_per_sector():
    first = _profile("512010.SH", "医药ETF", benchmark_code="")
    second = replace(
        _profile("512480.SH", "半导体ETF", benchmark_code=""),
        normalized_index=first.normalized_index,
    )

    sectors, representatives, *_ = build_sector_research([_row(first), _row(second)])

    assert {item["sector_id"] for item in sectors} == {first.sector_id, second.sector_id}
    assert representatives[first.symbol] == first.symbol
    assert representatives[second.symbol] == second.symbol


def test_verified_local_adjustment_counts_as_sector_evidence_and_is_labeled_truthfully():
    profile = _profile("510300.SH", "沪深300ETF", benchmark_code="000300.SH")

    sectors, *_ = build_sector_research(
        [_row(profile, adjustment_status="verified_local", position_250d=62.0)]
    )

    assert sectors[0]["adjustment_coverage"] == 1.0
    assert sectors[0]["long_position_source"] == "verified_local_adjusted"
    assert sectors[0]["position_source"] == "verified_local_adjusted"


def test_absolute_state_gates_allow_low_turn_leader_risk_and_cold_market():
    low = _row(
        _profile("512010.SH", "医药ETF", benchmark_code="000933.CSI"),
        return_5d=0.04,
        return_20d=0.08,
        return_60d=0.10,
        position_250d=30,
        above_ma20=True,
        ma20_slope=0.02,
        amount_ratio_5v20=1.25,
        avg_amount_20d=200_000_000,
    )
    leader = _row(
        _profile("512480.SH", "半导体ETF", benchmark_code="H30184.CSI"),
        return_5d=0.05,
        return_20d=0.12,
        return_60d=0.25,
        position_250d=92,
        above_ma20=True,
        ma20_above_ma60=True,
        ma20_slope=0.03,
        amount_ratio_5v20=1.5,
        avg_amount_20d=1_000_000_000,
        volatility_20d=0.03,
    )
    cold = _row(_profile("510300.SH", "沪深300ETF", benchmark_code="000300.SH"))

    sectors, _, queues, _, _ = build_sector_research([low, leader, cold])
    by_name = {item["sector_name"]: item for item in sectors}

    assert by_name["医药"]["state"] == "low_turn"
    assert by_name["半导体"]["state"] == "leading"
    assert any(item["code"] == "crowded_high" for item in by_name["半导体"]["risk_badges"])
    assert by_name["沪深300"]["state"] == "watch"
    assert queues["leading"] == (by_name["半导体"]["sector_id"],)

    cold_only, _, cold_queues, _, summaries = build_sector_research([cold])
    assert cold_only[0]["state"] == "watch"
    assert cold_queues["leading"] == ()
    assert summaries[0]["sector_id"] == cold_only[0]["sector_id"]
    assert summaries[0]["title"] == "趋势最强"


def test_stage_candidates_and_dynamic_position_survive_zero_official_adjustment_coverage():
    low = _row(
        _profile("512010.SH", "医药ETF", benchmark_code="000933.CSI"),
        return_5d=0.04,
        return_20d=0.06,
        return_60d=0.03,
        position_60d=25,
        position_250d=None,
        above_ma20=True,
        ma20_slope=-0.01,
        amount_ratio_5v20=1.2,
        avg_amount_20d=200_000_000,
        adjustment_status="raw_short_fallback",
    )
    high = _row(
        _profile("512480.SH", "半导体ETF", benchmark_code="H30184.CSI"),
        return_5d=0.03,
        return_20d=0.08,
        return_60d=0.12,
        position_60d=95,
        position_250d=None,
        above_ma20=True,
        amount_ratio_5v20=1.4,
        avg_amount_20d=900_000_000,
        volatility_20d=0.03,
        adjustment_status="raw_short_fallback",
    )

    sectors, _, _, candidates, summaries = build_sector_research([low, high])
    by_name = {item["sector_name"]: item for item in sectors}

    assert all(item["position_metric"] == "position_60d" for item in sectors)
    assert all(item["position_label"] == "60 日阶段位置" for item in sectors)
    assert by_name["医药"]["display_position"] == 25
    assert by_name["医药"]["sector_id"] in candidates["stage_low_rebound"]
    low_candidate = by_name["医药"]["candidates"]["stage_low_rebound"]
    assert low_candidate["eligible"] is True
    assert "250 日复权位置不高于 40" in low_candidate["unmet_conditions"]
    assert summaries[1]["evaluation_status"] == "candidate"
    assert summaries[1]["sector_id"] == by_name["医药"]["sector_id"]
    assert by_name["半导体"]["sector_id"] in candidates["stage_high_activity"]
    assert summaries[2]["evaluation_status"] == "candidate"


def test_share_evidence_distinguishes_zero_change_nonzero_stale_and_missing():
    sessions = ["2026-08-05", "2026-08-06", "2026-08-07"]
    zero_frame = pd.DataFrame(
        {
            "trade_date": sessions,
            "shares": [1_000_000_000, 1_000_000_000, 1_000_000_000],
            "close": [2.0, 2.0, 2.0],
            "share_source": "tushare:etf_share_size",
        }
    )
    changed_frame = zero_frame.copy()
    changed_frame.loc[2, "shares"] = 1_100_000_000

    zero = fund_evidence(zero_frame, as_of_date=sessions[-1], session_dates=sessions, fallback_price=2.0)
    changed = fund_evidence(
        changed_frame, as_of_date=sessions[-1], session_dates=sessions, fallback_price=2.0
    )
    stale = fund_evidence(
        zero_frame.iloc[:2], as_of_date=sessions[-1], session_dates=sessions, fallback_price=2.0
    )
    missing = fund_evidence(
        pd.DataFrame(), as_of_date=sessions[-1], session_dates=sessions, fallback_price=2.0
    )

    assert zero["status"] == "confirmed_zero"
    assert zero["share_delta"] == 0.0 and zero["share_change_pct"] == 0.0
    assert zero["unchanged_sessions"] == 2
    assert changed["status"] == "confirmed_change"
    assert changed["share_delta"] == 100_000_000
    assert changed["share_change_pct"] == pytest.approx(0.10)
    assert changed["estimated_flow"] == 200_000_000
    assert stale["status"] == "stale" and stale["share_delta"] is None
    assert missing["status"] == "missing" and missing["share_delta"] is None


def test_prepared_share_evidence_reuses_one_session_index():
    sessions = ["2026-08-05", "2026-08-06", "2026-08-07"]
    frame = pd.DataFrame(
        {
            "trade_date": pd.to_datetime(sessions),
            "shares": [1_000_000_000.0, 1_000_000_000.0, 1_100_000_000.0],
            "close": [2.0, 2.0, 2.0],
            "share_source": "tushare:etf_share_size",
        }
    )

    class ExplodingSessions:
        def __iter__(self):
            raise AssertionError("prepared path must not parse the shared calendar again")

    result = fund_evidence(
        frame,
        as_of_date=sessions[-1],
        session_dates=ExplodingSessions(),
        session_index={value: index for index, value in enumerate(sessions)},
        fallback_price=2.0,
        prepared=True,
    )

    assert result["status"] == "confirmed_change"
    assert result["share_delta"] == 100_000_000


def test_share_gap_is_labeled_as_multi_session_total_and_breaks_unchanged_streak():
    sessions = ["2026-08-03", "2026-08-04", "2026-08-05", "2026-08-06", "2026-08-07"]
    frame = pd.DataFrame(
        {
            "trade_date": [sessions[0], sessions[-1]],
            "shares": [1_000_000_000, 1_100_000_000],
            "close": [2.0, 2.0],
            "share_source": "tushare:etf_share_size",
        }
    )

    result = fund_evidence(
        frame,
        as_of_date=sessions[-1],
        session_dates=sessions,
        fallback_price=2.0,
    )

    assert result["status"] == "confirmed_change"
    assert result["period_kind"] == "interval"
    assert result["period_sessions"] == 4
    assert result["period_label"] == "近 4 个交易日累计变化"
    assert result["consecutive"] is False
    assert result["unchanged_sessions"] == 0
    assert "不可解释为当日申赎" in result["message"]


def test_sector_funds_keep_stale_provenance_and_confirmed_zero_streak():
    profile = _profile("512010.SH", "医药ETF", benchmark_code="000933.CSI")
    stale_row = _row(profile)
    stale_row["funds"] = {
        "status": "stale",
        "effective_date": "2026-08-06",
        "source": "tushare:fund_share",
        "share_delta": None,
        "share_change_pct": None,
        "estimated_flow": None,
    }
    stale_sector = build_sector_research([stale_row])[0][0]
    assert stale_sector["funds"]["status"] == "stale"
    assert stale_sector["funds"]["effective_date"] == "2026-08-06"
    assert stale_sector["funds"]["source"] == "tushare:fund_share"

    zero_row = _row(profile)
    zero_row["funds"] = {
        "status": "confirmed_zero",
        "effective_date": "2026-08-07",
        "source": "tushare:etf_share_size",
        "share": 1_000_000_000,
        "prior_share": 1_000_000_000,
        "share_delta": 0.0,
        "share_change_pct": 0.0,
        "estimated_flow": 0.0,
        "unchanged_sessions": 4,
    }
    zero_sector = build_sector_research([zero_row])[0][0]
    assert zero_sector["funds"]["status"] == "confirmed"
    assert zero_sector["funds"]["share_change_pct"] == 0.0
    assert zero_sector["funds"]["unchanged_sessions"] == 4


def test_low_fund_coverage_and_watch_state_never_create_divergence_badge():
    watched = _row(_profile("512010.SH", "医药ETF", benchmark_code="000933.CSI"))
    watched["funds"] = {
        "status": "confirmed_change",
        "effective_date": "2026-08-07",
        "source": "tushare:etf_share_size",
        "prior_share": 1_000_000_000,
        "share_delta": 100_000_000,
        "share_change_pct": 0.1,
        "estimated_flow": 200_000_000,
        "period_sessions": 1,
        "consecutive": True,
    }
    missing = _row(_profile("159929.SZ", "医药ETF", benchmark_code="000933.CSI-ALT"))
    missing_two = _row(_profile("560600.SH", "医药ETF", benchmark_code="000933.CSI-ALT2"))

    sector = build_sector_research([watched, missing, missing_two])[0][0]

    assert sector["state"] == "watch"
    assert sector["funds"]["coverage"] == pytest.approx(1 / 3)
    assert sector["funds"]["coverage_level"] == "low"
    assert not any(badge["code"].startswith("fund_") for badge in sector["risk_badges"])
    assert "覆盖低于 50%" in sector["funds"]["interpretation_note"]

    medium_sector = build_sector_research([watched, missing])[0][0]
    assert medium_sector["funds"]["coverage_level"] == "medium"
    assert not any(badge["code"].startswith("fund_") for badge in medium_sector["risk_badges"])
    assert "中性展示" in medium_sector["funds"]["interpretation_note"]


def test_obsolete_snapshot_contracts_are_intentionally_removed():
    with pytest.raises(ValueError, match="已淘汰"):
        EtfResearchSnapshot.from_dict({"schema_version": "1.0"})
    with pytest.raises(ValueError, match=r"研究模型.*已淘汰"):
        EtfResearchSnapshot.from_dict(
            {"schema_version": "3.0", "research_model_version": "QM_ETF_SECTOR_RADAR_V2"}
        )


def test_snapshot_history_omits_obsolete_research_models(tmp_path):
    store = EtfResearchStore(tmp_path / "research")
    snapshots = store.root / "snapshots"
    snapshots.mkdir(parents=True)
    (snapshots / "obsolete.json").write_text(
        '{"schema_version":"2.0","research_model_version":"QM_ETF_SECTOR_RADAR_V2",'
        '"snapshot_id":"obsolete","as_of_date":"2026-08-07","generated_at":"2026-08-07T16:00:00Z"}',
        encoding="utf-8",
    )

    assert store.history() == []


def test_snapshot_paths_require_content_addressed_ids_below_store_root(tmp_path):
    store = EtfResearchStore(tmp_path / "research")
    snapshot_id = "etf_" + "a" * 24

    assert store._snapshot_path(snapshot_id) == (
        store.root / "snapshots" / f"{snapshot_id}.json"
    ).resolve()
    for malicious in (
        f"../{snapshot_id}",
        f"..\\{snapshot_id}",
        f"C:\\outside\\{snapshot_id}",
        "etf_preview_" + "a" * 24,
    ):
        with pytest.raises(ValueError, match="快照标识无效"):
            store._snapshot_path(malicious)
        assert store.get(malicious) is None


def test_product_history_replays_frozen_ingest_and_factors_after_restart(tmp_path):
    store, ingest_store, snapshot, daily, factors = _published_replay_snapshot(tmp_path)
    changing_source = _MutableReplaySource(daily.assign(close=999.0))
    first = EtfResearchService(
        source=changing_source,
        ingest_store=ingest_store,
        store=store,
    ).product_history("510300.SH", snapshot_id=snapshot.snapshot_id)

    current_factor_cache = store.root / "evidence" / "adjustment_factors.parquet"
    current_factor_cache.parent.mkdir(parents=True, exist_ok=True)
    factors.assign(adj_factor=77.0).to_parquet(current_factor_cache, index=False)
    restarted_source = _MutableReplaySource(daily.assign(close=0.01))
    restarted = EtfResearchService(
        source=restarted_source,
        ingest_store=StockDBIngestStore(ingest_store.root),
        store=EtfResearchStore(store.root),
    )

    second = restarted.product_history("510300.SH", snapshot_id=snapshot.snapshot_id)

    assert second == first
    assert first[-1]["price"] == pytest.approx(4.0)
    assert changing_source.calls == 0
    assert restarted_source.calls == 0


def test_old_snapshot_without_frozen_factors_fails_closed_after_restart(tmp_path):
    store, ingest_store, snapshot, _daily, factors = _published_replay_snapshot(
        tmp_path,
        freeze_factors=False,
    )
    current_factor_cache = store.root / "evidence" / "adjustment_factors.parquet"
    current_factor_cache.parent.mkdir(parents=True, exist_ok=True)
    factors.to_parquet(current_factor_cache, index=False)
    restarted = EtfResearchService(
        source=_MutableReplaySource(pd.DataFrame()),
        ingest_store=StockDBIngestStore(ingest_store.root),
        store=EtfResearchStore(store.root),
    )

    with pytest.raises(RuntimeError, match=r"冻结复权证据缺失.*旧快照不可用当前因子回填"):
        restarted.product_history("510300.SH", snapshot_id=snapshot.snapshot_id)


def test_product_history_rejects_frozen_factor_hash_mismatch(tmp_path):
    store, ingest_store, snapshot, _daily, factors = _published_replay_snapshot(tmp_path)
    frozen = store.frozen_adjustments / f"{snapshot.evidence_hashes['复权']}.parquet"
    factors.assign(adj_factor=9.0).to_parquet(frozen, index=False)
    restarted = EtfResearchService(
        source=_MutableReplaySource(pd.DataFrame()),
        ingest_store=StockDBIngestStore(ingest_store.root),
        store=EtfResearchStore(store.root),
    )

    with pytest.raises(RuntimeError, match="冻结复权证据哈希不匹配"):
        restarted.product_history("510300.SH", snapshot_id=snapshot.snapshot_id)


def test_product_history_rejects_frozen_market_hash_mismatch(tmp_path):
    store, ingest_store, snapshot, daily, _factors = _published_replay_snapshot(tmp_path)
    ingest = ingest_store.get(snapshot.ingest_id)
    assert ingest is not None
    frozen = ingest_store.content / f"{ingest.content_hashes['etf_daily']}.parquet"
    daily.assign(close=999.0).to_parquet(frozen, index=False)
    restarted = EtfResearchService(
        source=_MutableReplaySource(pd.DataFrame()),
        ingest_store=StockDBIngestStore(ingest_store.root),
        store=EtfResearchStore(store.root),
    )

    with pytest.raises(RuntimeError, match="冻结行情证据哈希不匹配"):
        restarted.product_history("510300.SH", snapshot_id=snapshot.snapshot_id)


def test_product_history_rejects_snapshot_path_identity_mismatch(tmp_path):
    store, ingest_store, snapshot, _daily, _factors = _published_replay_snapshot(tmp_path)
    snapshot_path = store.root / "snapshots" / f"{snapshot.snapshot_id}.json"
    payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    payload["snapshot_id"] = "etf_000000000000000000000000"
    snapshot_path.write_text(json.dumps(payload), encoding="utf-8")
    restarted = EtfResearchService(
        source=_MutableReplaySource(pd.DataFrame()),
        ingest_store=StockDBIngestStore(ingest_store.root),
        store=EtfResearchStore(store.root),
    )

    with pytest.raises(RuntimeError, match="快照路径与内部标识不匹配"):
        restarted.product_history("510300.SH", snapshot_id=snapshot.snapshot_id)


def test_product_history_rejects_ingest_manifest_content_mismatch(tmp_path):
    store, ingest_store, snapshot, _daily, _factors = _published_replay_snapshot(tmp_path)
    manifest_path = ingest_store.manifests / f"{snapshot.ingest_id}.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["coverage"] = {"symbol_ratio": 0.5}
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    restarted = EtfResearchService(
        source=_MutableReplaySource(pd.DataFrame()),
        ingest_store=StockDBIngestStore(ingest_store.root),
        store=EtfResearchStore(store.root),
    )

    with pytest.raises(RuntimeError, match="摄取清单内容哈希不匹配"):
        restarted.product_history("510300.SH", snapshot_id=snapshot.snapshot_id)
