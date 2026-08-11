"""Research contracts, date lake, cross-asset providers, planner and management API."""

from __future__ import annotations

from typing import ClassVar

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from quantmaster.factors.artifact import ArtifactFactor, parse_artifact_reference
from quantmaster.research import (
    ArtifactKind,
    ArtifactRef,
    AssetClass,
    DataRequest,
    ExecutionPlan,
    Frequency,
    KernelBackend,
    PlanTask,
    ResearchSpec,
)
from quantmaster.research.adapters import (
    DATASETS,
    CompositeResearchAdapter,
    ResearchCrossSectionIncomplete,
    TushareResearchAdapter,
)
from quantmaster.research.diagnostics import factor_diagnostics
from quantmaster.research.engine import ResearchEngine
from quantmaster.research.jobs import ResearchJobManager
from quantmaster.research.kernel import Kernel
from quantmaster.research.lake import (
    FeatureBatchProvider,
    ResearchDataIntegrityError,
    ResearchLake,
)
from quantmaster.research.providers import (
    build_future_continuous,
    compute_core_factors,
    compute_forward_labels,
    compute_qm_style_v1,
)
from quantmaster.research.registry import built_in_registry
from quantmaster.server.app import app


@pytest.fixture(autouse=True)
def _verified_empty_suspension_snapshot(monkeypatch):
    monkeypatch.setattr(
        "quantmaster.data.instrument_snapshots.load_or_fetch_suspension_snapshot",
        lambda _source, trade_date: {
            "trade_date": trade_date,
            "acquired_at": f"{trade_date}T07:00:00+00:00",
            "content_hash": "s" * 64,
            "symbols": [],
            "source": "tushare:suspend_d",
            "file_sha256": "f" * 64,
        },
    )


def synthetic_bars(days: int = 80, symbols: int = 4) -> pd.DataFrame:
    dates = pd.bdate_range("2024-01-02", periods=days)
    rows = []
    for symbol_index in range(symbols):
        symbol = f"6000{symbol_index:02d}.SH"
        for index, trade_date in enumerate(dates):
            close = 10 + symbol_index + index * (0.01 + symbol_index * 0.001)
            volume = 10_000 + index * 10 + symbol_index * 100
            rows.append({
                "trade_date": trade_date, "symbol": symbol,
                "open": close - 0.03, "high": close + 0.1, "low": close - 0.1,
                "close": close, "volume": volume, "amount": close * volume,
                "research_price": close,
            })
    return pd.DataFrame(rows)


def test_contracts_are_versioned_and_reject_lookahead_factor():
    request = DataRequest("stock_bars", ("close",), lookback_sessions=20)
    spec = ResearchSpec(
        id="demo_factor", version="1.2.0", kind=ArtifactKind.FACTOR,
        asset_classes=(AssetClass.STOCK,), provider_id="demo_provider",
        output="score", dependencies=(request,), lookback_sessions=20,
    )
    assert spec.storage_column == "demo_factor__v1_2_0"
    assert ResearchSpec.from_dict(spec.to_dict()) == spec
    with pytest.raises(ValueError, match="前看"):
        ResearchSpec(
            id="bad_factor", version="1.0.0", kind=ArtifactKind.FACTOR,
            asset_classes=(AssetClass.STOCK,), lookahead_sessions=1,
        )


def test_partial_stockdb_cross_section_with_empty_remote_is_not_published(
    tmp_path, monkeypatch,
):
    target = pd.Timestamp.now(tz="Asia/Shanghai").normalize() - pd.Timedelta(days=1)
    acquired_at = (target + pd.Timedelta(hours=15, minutes=1)).timestamp()
    trade_date = str(target.date())

    class Local:
        LOCAL_DATASETS = frozenset({"stock_bars"})

        def __init__(self):
            self.instruments = self

        def list(self, **_kwargs):
            return [
                type("Instrument", (), {
                    "symbol": f"600{index:03d}.SH",
                    "asset_type": "stock",
                    "list_date": "20200101",
                    "delist_date": "",
                    "status": "L",
                    "source": "tushare:catalog",
                    "observed_at": acquired_at,
                })()
                for index in range(100)
            ]

        def diagnostics(self):
            return {
                "coverage": [{"market": "CN", "asset_type": "stock", "count": 100}],
                "sources": [{
                    "source": "tushare:catalog",
                    "status": "success",
                    "last_success": acquired_at,
                    "record_count": 100,
                }],
            }

        def fetch_date(self, _dataset_id, trade_date):
            return pd.DataFrame({
                "trade_date": [pd.Timestamp(trade_date)],
                "symbol": ["600000.SH"],
                "close": [10.0],
            })

        def capabilities(self):
            return []

    class Direct:
        def fetch_date(self, _dataset_id, _trade_date):
            return pd.DataFrame()

        def capabilities(self):
            return []

    expected = {f"600{index:03d}.SH" for index in range(100)}
    monkeypatch.setattr(
        "quantmaster.data.instrument_snapshots.load_instrument_catalog_snapshot",
        lambda **_kwargs: (None, expected, {
            "snapshot_sha256": "catalog-a",
            "snapshot_id": "catalog-a",
            "acquired_at": pd.Timestamp(acquired_at, unit="s", tz="UTC").isoformat(),
            "expected_count": 100,
            "source": "tushare:catalog",
        }),
    )

    lake = ResearchLake(tmp_path / "lake")
    adapter = CompositeResearchAdapter(lake.catalog, local=Local(), direct=Direct())
    engine = ResearchEngine(lake=lake, adapter=adapter)
    task = PlanTask(
        "sync", "stock_bars", AssetClass.STOCK, Frequency.DAILY, trade_date,
    )
    plan = ExecutionPlan(
        id="partial-cross-section",
        start=trade_date,
        end=trade_date,
        target_dates=(trade_date,),
        asset_classes=(AssetClass.STOCK,),
        frequency=Frequency.DAILY,
        datasets=("stock_bars",),
        selected_specs=(),
        tasks=(task,),
    )

    with pytest.raises(ResearchCrossSectionIncomplete, match="expected=100，observed=1"):
        engine.execute_task(plan, task, run_id="partial")
    assert lake.catalog.partition(
        ArtifactKind.RAW,
        AssetClass.STOCK,
        Frequency.DAILY,
        "stock_bars",
        trade_date,
    ) is None


def _degraded_preview_adapter(tmp_path, monkeypatch, local, direct=None):
    target = pd.Timestamp.now(tz="Asia/Shanghai").normalize() - pd.Timedelta(days=1)
    acquired_at = (target + pd.Timedelta(hours=15, minutes=1)).timestamp()
    trade_date = str(target.date())
    expected = {f"600{index:03d}.SH" for index in range(100)}
    local_frame = local.copy()
    direct_frame = direct.copy() if direct is not None else pd.DataFrame()

    class Local:
        LOCAL_DATASETS = frozenset({"stock_bars"})

        def fetch_date(self, _dataset_id, _trade_date):
            return local_frame.copy()

    class Direct:
        def fetch_date(self, _dataset_id, _trade_date):
            return direct_frame.copy()

    monkeypatch.setattr(
        "quantmaster.data.instrument_snapshots.load_instrument_catalog_snapshot",
        lambda **_kwargs: (None, expected, {
            "snapshot_sha256": "catalog-preview",
            "snapshot_id": "catalog-preview",
            "acquired_at": pd.Timestamp(acquired_at, unit="s", tz="UTC").isoformat(),
            "expected_count": len(expected),
            "source": "tushare:catalog",
        }),
    )
    adapter = CompositeResearchAdapter(
        ResearchLake(tmp_path / "lake").catalog,
        local=Local(),
        direct=Direct(),
    )
    return adapter, expected, trade_date


@pytest.mark.parametrize("observed_count", [1, 99])
def test_degraded_preview_discloses_partial_cross_section_without_formal_publishability(
    tmp_path, monkeypatch, observed_count,
):
    trade_date = str(
        (pd.Timestamp.now(tz="Asia/Shanghai").normalize() - pd.Timedelta(days=1)).date()
    )
    local = pd.DataFrame({
        "trade_date": [pd.Timestamp(trade_date)] * observed_count,
        "symbol": [f"600{index:03d}.SH" for index in range(observed_count)],
        "close": [10.0 + index for index in range(observed_count)],
    })
    adapter, expected, trade_date = _degraded_preview_adapter(
        tmp_path,
        monkeypatch,
        local,
    )

    preview = adapter.preview_date("stock_bars", trade_date)
    quality = preview.attrs["research_partition_quality"]
    observed = sorted(local["symbol"])
    missing = sorted(expected - set(observed))

    assert list(preview["symbol"]) == observed
    assert quality["status"] == "degraded_preview"
    assert quality["formal_eligible"] is False
    assert quality["expected_count"] == 100
    assert quality["observed_count"] == observed_count
    assert quality["missing_symbols"] == missing
    assert quality["coverage_ratio"] == observed_count / 100
    assert preview.attrs["formal_eligible"] is False
    assert preview.attrs["denominator_status"] == "verified_catalog_and_suspension"
    assert preview.attrs["expected"] == sorted(expected)
    assert preview.attrs["observed"] == observed
    assert preview.attrs["missing"] == missing
    assert preview.attrs["field_provenance"]["close"] == [
        "free-stockdb:vendor-upstream-unverified"
    ]
    with pytest.raises(
        ResearchCrossSectionIncomplete,
        match=rf"expected=100，observed={observed_count}",
    ):
        adapter.fetch_date("stock_bars", trade_date)


def test_degraded_preview_quarantines_unexpected_and_every_duplicate_symbol(
    tmp_path, monkeypatch,
):
    trade_date = str(
        (pd.Timestamp.now(tz="Asia/Shanghai").normalize() - pd.Timedelta(days=1)).date()
    )
    local = pd.DataFrame({
        "trade_date": [pd.Timestamp(trade_date)] * 4,
        "symbol": ["600000.SH", "600001.SH", "600001.SH", "999999.SH"],
        "close": [10.0, 11.0, 12.0, 99.0],
    })
    direct = pd.DataFrame({
        "trade_date": [pd.Timestamp(trade_date)] * 5,
        "symbol": [
            "600000.SH", "600002.SH", "600003.SH", "600003.SH", "888888.SH",
        ],
        "close": [10.5, 12.0, 13.0, 14.0, 88.0],
    })
    adapter, expected, trade_date = _degraded_preview_adapter(
        tmp_path,
        monkeypatch,
        local,
        direct,
    )

    preview = adapter.preview_date("stock_bars", trade_date)
    quality = preview.attrs["research_partition_quality"]

    assert list(preview["symbol"]) == ["600000.SH", "600002.SH"]
    assert quality["observed_count"] == 2
    assert quality["duplicate_symbols"] == ["600001.SH", "600003.SH"]
    assert quality["unexpected_symbols"] == ["888888.SH", "999999.SH"]
    assert "600000.SH" not in quality["unexpected_symbols"]
    assert quality["missing_symbols"] == sorted(
        expected - {"600000.SH", "600002.SH"}
    )
    assert preview.attrs["field_provenance"]["close"] == [
        "free-stockdb:vendor-upstream-unverified",
        "tushare:direct",
    ]


def test_stale_or_unproven_master_snapshot_never_verifies_local_cross_section(tmp_path):
    acquired_at = (pd.Timestamp.now(tz="Asia/Shanghai") - pd.Timedelta(days=8)).timestamp()
    trade_date = str(pd.Timestamp.now(tz="Asia/Shanghai").date())
    instrument = type("Instrument", (), {
        "symbol": "600000.SH",
        "asset_type": "stock",
        "list_date": "20200101",
        "delist_date": "",
        "status": "L",
        "source": "bundled",
        "observed_at": acquired_at,
    })()

    class Local:
        LOCAL_DATASETS = frozenset({"stock_bars"})
        instruments = None

        def __init__(self):
            self.instruments = self

        def list(self, **_kwargs):
            return [instrument]

        def diagnostics(self):
            return {
                "coverage": [{"market": "CN", "asset_type": "stock", "count": 1}],
                "sources": [{
                    "source": "tushare:catalog",
                    "status": "bundled",
                    "last_success": acquired_at,
                    "record_count": 1,
                }],
            }

        def fetch_date(self, _dataset_id, value_date):
            return pd.DataFrame({
                "trade_date": [pd.Timestamp(value_date)],
                "symbol": [instrument.symbol],
                "close": [10.0],
            })

    adapter = CompositeResearchAdapter(
        ResearchLake(tmp_path / "lake").catalog,
        local=Local(),
        direct=type("Direct", (), {"fetch_date": lambda *_args: pd.DataFrame()})(),
    )
    with pytest.raises(ResearchCrossSectionIncomplete, match="不可变证券目录"):
        adapter.fetch_date("stock_bars", trade_date)


def test_future_listing_in_historical_cross_section_is_rejected(tmp_path, monkeypatch):
    target = pd.Timestamp.now(tz="Asia/Shanghai").normalize() - pd.Timedelta(days=1)
    acquired_at = (target + pd.Timedelta(hours=15, minutes=1)).timestamp()
    trade_date = str(target.date())
    instruments = [
        type("Instrument", (), {
            "symbol": symbol,
            "asset_type": "stock",
            "list_date": list_date,
            "delist_date": "",
            "status": "L",
            "source": "tushare:catalog",
            "observed_at": acquired_at,
        })()
        for symbol, list_date in (
            ("600000.SH", "20200101"),
            ("600001.SH", str((target + pd.Timedelta(days=1)).date())),
        )
    ]

    class Local:
        LOCAL_DATASETS = frozenset({"stock_bars"})

        def __init__(self):
            self.instruments = self

        def list(self, **_kwargs):
            return instruments

        def diagnostics(self):
            return {
                "coverage": [{"market": "CN", "asset_type": "stock", "count": 2}],
                "sources": [{
                    "source": "tushare:catalog",
                    "status": "success",
                    "last_success": acquired_at,
                    "record_count": 2,
                }],
            }

        def fetch_date(self, _dataset_id, value_date):
            return pd.DataFrame({
                "trade_date": [pd.Timestamp(value_date)] * 2,
                "symbol": [item.symbol for item in instruments],
                "close": [10.0, 20.0],
            })

    adapter = CompositeResearchAdapter(
        ResearchLake(tmp_path / "lake").catalog,
        local=Local(),
        direct=type("Direct", (), {"fetch_date": lambda *_args: pd.DataFrame()})(),
    )
    monkeypatch.setattr(
        "quantmaster.data.instrument_snapshots.load_instrument_catalog_snapshot",
        lambda **_kwargs: (None, {"600000.SH"}, {
            "snapshot_sha256": "catalog-a",
            "snapshot_id": "catalog-a",
            "acquired_at": pd.Timestamp(acquired_at, unit="s", tz="UTC").isoformat(),
            "expected_count": 1,
            "source": "tushare:catalog",
        }),
    )
    with pytest.raises(ResearchCrossSectionIncomplete, match="未来成员穿越"):
        adapter.fetch_date("stock_bars", trade_date)


def test_latest_partial_catalog_cannot_relabel_old_stock_row_as_same_snapshot(
    tmp_path, monkeypatch,
):
    target = pd.Timestamp.now(tz="Asia/Shanghai").normalize() - pd.Timedelta(days=1)
    acquired_at = (target + pd.Timedelta(hours=15, minutes=1)).timestamp()
    old_observed_at = (target - pd.Timedelta(days=1) + pd.Timedelta(hours=15)).timestamp()
    rows = [
        type("Instrument", (), {
            "symbol": "600000.SH",
            "asset_type": "stock",
            "list_date": "20200101",
            "delist_date": "",
            "status": "L",
            "source": "tushare:catalog",
            "observed_at": acquired_at,
        })(),
        type("Instrument", (), {
            "symbol": "600001.SH",
            "asset_type": "stock",
            "list_date": "20200101",
            "delist_date": "",
            "status": "L",
            "source": "tushare:catalog",
            "observed_at": old_observed_at,
        })(),
        type("Instrument", (), {
            "symbol": "510300.SH",
            "asset_type": "fund",
            "list_date": "20200101",
            "delist_date": "",
            "status": "L",
            "source": "tushare:catalog",
            "observed_at": acquired_at,
        })(),
    ]

    class Local:
        LOCAL_DATASETS = frozenset({"stock_bars"})

        def __init__(self):
            self.instruments = self

        def list(self, **_kwargs):
            return rows

        def diagnostics(self):
            return {
                "coverage": [
                    {"market": "CN", "asset_type": "stock", "count": 2},
                    {"market": "CN", "asset_type": "fund", "count": 1},
                ],
                "sources": [{
                    "source": "tushare:catalog",
                    "status": "success",
                    "last_success": acquired_at,
                    # Today's partial response was A + one fund. B is yesterday's residue.
                    "record_count": 2,
                }],
            }

        def fetch_date(self, _dataset_id, value_date):
            return pd.DataFrame({
                "trade_date": [pd.Timestamp(value_date)] * 2,
                "symbol": ["600000.SH", "600001.SH"],
                "close": [10.0, 20.0],
            })

    adapter = CompositeResearchAdapter(
        ResearchLake(tmp_path / "lake").catalog,
        local=Local(),
        direct=type("Direct", (), {"fetch_date": lambda *_args: pd.DataFrame()})(),
    )
    from quantmaster.data.instrument_snapshots import InstrumentCatalogEvidenceError

    def reject_partial(**_kwargs):
        raise InstrumentCatalogEvidenceError("partial catalog snapshot")

    monkeypatch.setattr(
        "quantmaster.data.instrument_snapshots.load_instrument_catalog_snapshot",
        reject_partial,
    )
    with pytest.raises(ResearchCrossSectionIncomplete, match="partial catalog"):
        adapter.fetch_date("stock_bars", str(target.date()))


def test_old_delisted_bundled_row_does_not_block_current_catalog_snapshot(
    tmp_path, monkeypatch,
):
    target = pd.Timestamp.now(tz="Asia/Shanghai").normalize() - pd.Timedelta(days=1)
    acquired_at = (target + pd.Timedelta(hours=15, minutes=1)).timestamp()
    instruments = [
        type("Instrument", (), {
            "symbol": "600000.SH",
            "asset_type": "stock",
            "list_date": "20200101",
            "delist_date": "",
            "status": "L",
            "source": "tushare:catalog",
            "observed_at": acquired_at,
        })(),
        type("Instrument", (), {
            "symbol": "600001.SH",
            "asset_type": "stock",
            "list_date": "20200101",
            "delist_date": str((target - pd.Timedelta(days=1)).date()),
            "status": "D",
            "source": "bundled",
            "observed_at": (target - pd.Timedelta(days=10)).timestamp(),
        })(),
    ]

    class Local:
        LOCAL_DATASETS = frozenset({"stock_bars"})

        def __init__(self):
            self.instruments = self

        def list(self, **_kwargs):
            return instruments

        def diagnostics(self):
            return {
                "coverage": [{"market": "CN", "asset_type": "stock", "count": 2}],
                "sources": [{
                    "source": "tushare:catalog",
                    "status": "success",
                    "last_success": acquired_at,
                    "record_count": 1,
                }],
            }

        def fetch_date(self, _dataset_id, value_date):
            return pd.DataFrame({
                "trade_date": [pd.Timestamp(value_date)],
                "symbol": ["600000.SH"],
                "close": [10.0],
            })

    adapter = CompositeResearchAdapter(
        ResearchLake(tmp_path / "lake").catalog,
        local=Local(),
        direct=type("Direct", (), {"fetch_date": lambda *_args: pd.DataFrame()})(),
    )
    monkeypatch.setattr(
        "quantmaster.data.instrument_snapshots.load_instrument_catalog_snapshot",
        lambda **_kwargs: (None, {"600000.SH"}, {
            "snapshot_sha256": "catalog-a",
            "snapshot_id": "catalog-a",
            "acquired_at": pd.Timestamp(acquired_at, unit="s", tz="UTC").isoformat(),
            "expected_count": 1,
            "source": "tushare:catalog",
        }),
    )
    value = adapter.fetch_date("stock_bars", str(target.date()))
    assert value["symbol"].tolist() == ["600000.SH"]
    assert value.loc[0, "universe_expected_count"] == 1


@pytest.mark.parametrize(
    ("acquired_hour", "accepted"),
    [(9, False), (15, True)],
)
def test_daily_cross_section_requires_post_close_catalog_snapshot(
    tmp_path, acquired_hour, accepted, monkeypatch,
):
    target = pd.Timestamp.now(tz="Asia/Shanghai").normalize() - pd.Timedelta(days=1)
    acquired = target + pd.Timedelta(hours=acquired_hour, minutes=1)
    instrument = type("Instrument", (), {
        "symbol": "600000.SH",
        "asset_type": "stock",
        "list_date": "20200101",
        "delist_date": "",
        "status": "L",
        "source": "tushare:catalog",
        "observed_at": acquired.timestamp(),
    })()

    class Local:
        LOCAL_DATASETS = frozenset({"stock_bars"})

        def __init__(self):
            self.instruments = self

        def list(self, **_kwargs):
            return [instrument]

        def diagnostics(self):
            return {
                "coverage": [{"market": "CN", "asset_type": "stock", "count": 1}],
                "sources": [{
                    "source": "tushare:catalog",
                    "status": "success",
                    "last_success": acquired.timestamp(),
                    "record_count": 1,
                }],
            }

        def fetch_date(self, _dataset_id, value_date):
            return pd.DataFrame({
                "trade_date": [pd.Timestamp(value_date)],
                "symbol": [instrument.symbol],
                "close": [10.0],
            })

    adapter = CompositeResearchAdapter(
        ResearchLake(tmp_path / f"lake-{acquired_hour}").catalog,
        local=Local(),
        direct=type("Direct", (), {"fetch_date": lambda *_args: pd.DataFrame()})(),
    )
    from quantmaster.data.instrument_snapshots import InstrumentCatalogEvidenceError

    def load_catalog(**_kwargs):
        if not accepted:
            raise InstrumentCatalogEvidenceError("早于上海 15:00")
        return None, {"600000.SH"}, {
            "snapshot_sha256": "catalog-a",
            "snapshot_id": "catalog-a",
            "acquired_at": acquired.isoformat(),
            "expected_count": 1,
            "source": "tushare:catalog",
        }

    monkeypatch.setattr(
        "quantmaster.data.instrument_snapshots.load_instrument_catalog_snapshot",
        load_catalog,
    )
    if not accepted:
        with pytest.raises(ResearchCrossSectionIncomplete, match="上海 15:00"):
            adapter.fetch_date("stock_bars", str(target.date()))
        return
    value = adapter.fetch_date("stock_bars", str(target.date()))
    assert value.attrs["research_partition_quality"]["status"] == "verified_complete"
    assert value.loc[0, "universe_expected_count"] == 1


def test_engine_rejects_nonempty_cross_section_without_quality_evidence(tmp_path):
    class UnverifiedAdapter:
        def fetch_date(self, _dataset_id, trade_date):
            return pd.DataFrame({
                "trade_date": [pd.Timestamp(trade_date)],
                "symbol": ["600000.SH"],
                "close": [10.0],
            })

    lake = ResearchLake(tmp_path / "lake")
    engine = ResearchEngine(lake=lake, adapter=UnverifiedAdapter())
    task = PlanTask(
        "sync", "stock_bars", AssetClass.STOCK, Frequency.DAILY, "2024-01-02",
    )
    plan = ExecutionPlan(
        id="unverified-cross-section",
        start="2024-01-02",
        end="2024-01-02",
        target_dates=("2024-01-02",),
        asset_classes=(AssetClass.STOCK,),
        frequency=Frequency.DAILY,
        datasets=("stock_bars",),
        selected_specs=(),
        tasks=(task,),
    )

    with pytest.raises(RuntimeError, match="缺少完整横截面质量证明"):
        engine.execute_task(plan, task, run_id="unverified")
    assert lake.catalog.partition(
        ArtifactKind.RAW,
        AssetClass.STOCK,
        Frequency.DAILY,
        "stock_bars",
        "2024-01-02",
    ) is None


def test_official_suspension_evidence_reduces_daily_trading_denominator(
    tmp_path, monkeypatch,
):
    trade_date = "2026-08-08"

    class Local:
        LOCAL_DATASETS = frozenset({"stock_bars"})

        def fetch_date(self, _dataset_id, value_date):
            return pd.DataFrame({
                "trade_date": [pd.Timestamp(value_date)],
                "symbol": ["600000.SH"],
                "close": [10.0],
            })

    class Direct:
        source = object()

    monkeypatch.setattr(
        "quantmaster.data.instrument_snapshots.load_instrument_catalog_snapshot",
        lambda **_kwargs: (None, {"600000.SH", "600001.SH"}, {
            "snapshot_sha256": "catalog-a",
            "snapshot_id": "catalog-a",
            "acquired_at": "2026-08-08T07:00:00+00:00",
            "expected_count": 2,
            "source": "tushare:catalog",
        }),
    )
    monkeypatch.setattr(
        "quantmaster.data.instrument_snapshots.load_or_fetch_suspension_snapshot",
        lambda _source, _date: {
            "trade_date": trade_date,
            "acquired_at": "2026-08-08T07:00:00+00:00",
            "content_hash": "s" * 64,
            "symbols": ["600001.SH"],
            "source": "tushare:suspend_d",
            "file_sha256": "f" * 64,
        },
    )
    adapter = CompositeResearchAdapter(
        ResearchLake(tmp_path / "lake").catalog, local=Local(), direct=Direct(),
    )
    value = adapter.fetch_date("stock_bars", trade_date)
    quality = value.attrs["research_partition_quality"]
    assert quality["status"] == "verified_complete"
    assert quality["expected_universe_evidence"]["catalog_expected_count"] == 2
    assert quality["expected_universe_evidence"]["suspended_count"] == 1
    assert value.loc[0, "suspension_snapshot_sha256"] == "s" * 64


def test_missing_suspension_evidence_rejects_daily_cross_section(tmp_path, monkeypatch):
    from quantmaster.data.instrument_snapshots import InstrumentCatalogEvidenceError

    monkeypatch.setattr(
        "quantmaster.data.instrument_snapshots.load_instrument_catalog_snapshot",
        lambda **_kwargs: (None, {"600000.SH"}, {
            "snapshot_sha256": "catalog-a",
            "snapshot_id": "catalog-a",
            "acquired_at": "2026-08-08T07:00:00+00:00",
            "expected_count": 1,
            "source": "tushare:catalog",
        }),
    )

    def unavailable(_source, _date):
        raise InstrumentCatalogEvidenceError("missing suspend_d")

    monkeypatch.setattr(
        "quantmaster.data.instrument_snapshots.load_or_fetch_suspension_snapshot",
        unavailable,
    )
    local = type("Local", (), {
        "LOCAL_DATASETS": frozenset({"stock_bars"}),
        "fetch_date": lambda *_args: pd.DataFrame({
            "trade_date": [pd.Timestamp("2026-08-08")],
            "symbol": ["600000.SH"],
            "close": [10.0],
        }),
    })()
    adapter = CompositeResearchAdapter(
        ResearchLake(tmp_path / "lake").catalog,
        local=local,
        direct=type("Direct", (), {"source": object()})(),
    )
    with pytest.raises(ResearchCrossSectionIncomplete, match="missing suspend_d"):
        adapter.fetch_date("stock_bars", "2026-08-08")


def test_adj_factor_local_first_remote_missing_completion_is_exact(tmp_path, monkeypatch):
    trade_date = "2026-08-08"
    monkeypatch.setattr(
        "quantmaster.data.instrument_snapshots.load_instrument_catalog_snapshot",
        lambda **_kwargs: (None, {"600000.SH", "600001.SH"}, {
            "snapshot_sha256": "catalog-a",
            "snapshot_id": "catalog-a",
            "acquired_at": "2026-08-08T07:00:00+00:00",
            "expected_count": 2,
            "source": "tushare:catalog",
        }),
    )

    class Local:
        LOCAL_DATASETS = frozenset({"stock_adj_factor"})

        def fetch_date(self, _dataset_id, value_date):
            return pd.DataFrame({
                "trade_date": [pd.Timestamp(value_date)],
                "symbol": ["600000.SH"],
                "adj_factor": [1.5],
            })

    class Direct:
        source = object()

        def fetch_date(self, _dataset_id, value_date):
            return pd.DataFrame({
                "trade_date": [pd.Timestamp(value_date)],
                "symbol": ["600001.SH"],
                "adj_factor": [2.0],
            })

    adapter = CompositeResearchAdapter(
        ResearchLake(tmp_path / "lake").catalog, local=Local(), direct=Direct(),
    )
    value = adapter.fetch_date("stock_adj_factor", trade_date)
    assert value.set_index("symbol")["adj_factor"].to_dict() == {
        "600000.SH": 1.5,
        "600001.SH": 2.0,
    }
    assert value.attrs["research_partition_quality"]["status"] == "verified_complete"


def test_provider_inputs_reject_partial_adj_factor_before_research_price(tmp_path):
    lake = ResearchLake(tmp_path / "lake")
    trade_date = "2024-01-02"
    bars = pd.DataFrame({
        "trade_date": [trade_date, trade_date],
        "symbol": ["600000.SH", "600001.SH"],
        "close": [10.0, 20.0],
    })
    lake.write_partition(
        ArtifactKind.RAW, AssetClass.STOCK, Frequency.DAILY,
        "stock_bars", trade_date, bars,
    )
    lake.write_partition(
        ArtifactKind.RAW, AssetClass.STOCK, Frequency.DAILY,
        "stock_adj_factor", trade_date,
        pd.DataFrame({
            "trade_date": [trade_date],
            "symbol": ["600000.SH"],
            "adj_factor": [1.0],
        }),
    )
    engine = ResearchEngine(lake=lake, adapter=FakePlanningAdapter())
    with pytest.raises(RuntimeError, match="不是一对一完整集合"):
        engine._provider_inputs(
            AssetClass.STOCK, "cross_asset_core", trade_date, trade_date,
        )


def test_builtin_registry_groups_outputs_and_exposes_curated_48():
    registry = built_in_registry()
    catalog = registry.catalog()
    assert len(catalog) == 66
    assert len(registry.provider("cross_asset_core").outputs) == 6
    assert len(registry.select(("cross_asset_core",))) == 6
    assert len([item for item in catalog if "curated-48" in item["tags"]]) == 48
    assert registry.resolve("cross_momentum_20d").lookback_sessions == 20


def test_version_pinned_news_sentiment_is_never_silently_reinterpreted():
    registry = built_in_registry()

    assert registry.resolve("news_sentiment").version == "2.0.0"
    assert registry.resolve("news_sentiment", version="2.0.0").version == "2.0.0"
    with pytest.raises(KeyError, match="news_sentiment@1.0.0"):
        registry.resolve("news_sentiment", version="1.0.0")


def test_lake_merges_versioned_wide_partitions_and_builds_tensor():
    lake = ResearchLake()
    registry = built_in_registry()
    bars = synthetic_bars(days=30, symbols=3)
    factors = compute_core_factors(bars, Kernel(KernelBackend.PYTHON))
    first = registry.resolve("cross_momentum_20d")
    second = registry.resolve("cross_realized_vol_20d")
    lake.write_artifact_values(
        first, factors, asset_class=AssetClass.STOCK, run_id="wide-test",
    )
    lake.write_artifact_values(
        second, factors, asset_class=AssetClass.STOCK, run_id="wide-test",
    )
    trade_date = str(pd.to_datetime(factors["trade_date"]).max().date())
    frame = lake.read_partition(
        ArtifactKind.FACTOR, AssetClass.STOCK, Frequency.DAILY, "wide", trade_date,
    )
    assert {first.storage_column, second.storage_column}.issubset(frame)
    assert not frame.duplicated(["trade_date", "symbol"]).any()
    metadata = lake.catalog.partition(
        ArtifactKind.FACTOR, AssetClass.STOCK, Frequency.DAILY, "wide", trade_date,
    )
    assert metadata and len(metadata["content_sha256"]) == 64
    ref = ArtifactRef(
        ArtifactKind.FACTOR, first.id, first.version, AssetClass.STOCK,
    )
    tensor = FeatureBatchProvider(lake).tensor(
        [ref], str(pd.to_datetime(factors["trade_date"]).min().date()), trade_date, 5,
    )
    assert tensor["values"].shape[1:] == (5, 1)
    assert tensor["mask"].shape == tensor["values"].shape


def test_artifact_panel_preserves_verified_trading_date_gaps_as_nan(tmp_path):
    lake = ResearchLake(tmp_path / "lake")
    ref = ArtifactRef(
        ArtifactKind.FACTOR, "gap_factor", "1.0.0", AssetClass.STOCK,
    )
    for trade_date in ("2024-01-02", "2024-01-03", "2024-01-04"):
        lake.write_partition(
            ArtifactKind.RAW, AssetClass.STOCK, Frequency.DAILY,
            "stock_bars", trade_date,
            pd.DataFrame({
                "trade_date": [trade_date], "symbol": ["600000.SH"], "close": [10.0],
            }),
        )
    for trade_date, value in (("2024-01-02", 1.0), ("2024-01-04", 3.0)):
        lake.write_partition(
            ArtifactKind.FACTOR, AssetClass.STOCK, Frequency.DAILY,
            ref.id, trade_date,
            pd.DataFrame({
                "trade_date": [trade_date], "symbol": ["600000.SH"],
                ref.storage_column: [value],
            }),
            merge_columns=True,
            spec_versions={ref.id: ref.version},
        )

    panel = lake.artifact_panel(ref, "2024-01-02", "2024-01-04")
    tensor = FeatureBatchProvider(lake).tensor(
        [ref], "2024-01-02", "2024-01-04", lookback=2,
    )

    assert [str(value.date()) for value in panel.index] == [
        "2024-01-02", "2024-01-03", "2024-01-04",
    ]
    assert pd.isna(panel.loc["2024-01-03", "600000.SH"])
    assert tensor["keys"][-1] == {"trade_date": "2024-01-04", "symbol": "600000.SH"}
    assert tensor["mask"][-1, :, 0].tolist() == [False, True]


def test_lake_rejects_duplicate_keys_without_replacing_good_partition():
    lake = ResearchLake()
    good = pd.DataFrame({
        "trade_date": ["2024-01-02"], "symbol": ["600000.SH"], "close": [10.0],
    })
    lake.write_partition(
        ArtifactKind.RAW, AssetClass.STOCK, Frequency.DAILY,
        "stock_bars", "2024-01-02", good,
    )
    duplicate = pd.concat([good, good], ignore_index=True)
    with pytest.raises(ValueError, match="主键重复"):
        lake.write_partition(
            ArtifactKind.RAW, AssetClass.STOCK, Frequency.DAILY,
            "stock_bars", "2024-01-02", duplicate,
        )
    assert len(lake.read_partition(
        ArtifactKind.RAW, AssetClass.STOCK, Frequency.DAILY,
        "stock_bars", "2024-01-02",
    )) == 1


def test_lake_recovers_replaced_partition_after_catalog_commit_failure(
    tmp_path, monkeypatch,
):
    root = tmp_path / "lake"
    lake = ResearchLake(root)
    old = pd.DataFrame({
        "trade_date": ["2024-01-02"], "symbol": ["600000.SH"], "close": [10.0],
    })
    new = old.assign(close=11.0)
    lake.write_partition(
        ArtifactKind.RAW, AssetClass.STOCK, Frequency.DAILY,
        "stock_bars", "2024-01-02", old,
    )

    def fail_commit(*args, **kwargs):
        raise RuntimeError("injected catalog failure")

    monkeypatch.setattr(lake.catalog, "commit_partition_write", fail_commit)
    with pytest.raises(RuntimeError, match="injected"):
        lake.write_partition(
            ArtifactKind.RAW, AssetClass.STOCK, Frequency.DAILY,
            "stock_bars", "2024-01-02", new,
        )

    recovered = ResearchLake(root)
    frame = recovered.read_partition(
        ArtifactKind.RAW, AssetClass.STOCK, Frequency.DAILY,
        "stock_bars", "2024-01-02",
    )
    assert frame.iloc[0]["close"] == 11.0
    assert recovered.catalog.partition_intents() == []


def test_lake_detects_valid_parquet_tampering_and_run_artifacts_are_immutable(tmp_path):
    lake = ResearchLake(tmp_path / "lake")
    frame = pd.DataFrame({
        "trade_date": ["2024-01-02"], "symbol": ["600000.SH"], "close": [10.0],
    })
    lake.write_partition(
        ArtifactKind.RAW, AssetClass.STOCK, Frequency.DAILY,
        "stock_bars", "2024-01-02", frame,
    )
    path = lake.partition_path(
        ArtifactKind.RAW, AssetClass.STOCK, Frequency.DAILY,
        "stock_bars", "2024-01-02",
    )
    frame.assign(close=99.0).to_parquet(path, index=False)
    with pytest.raises(ResearchDataIntegrityError, match="内容校验失败"):
        lake.read_partition(
            ArtifactKind.RAW, AssetClass.STOCK, Frequency.DAILY,
            "stock_bars", "2024-01-02",
        )

    lake.write_run_files("run-1", {"run_id": "run-1", "status": "completed"})
    with pytest.raises(ResearchDataIntegrityError, match="运行工件不可变"):
        lake.write_run_files("run-1", {"run_id": "run-1", "status": "failed"})


def test_core_factors_labels_and_style_model_have_expected_contracts():
    bars = synthetic_bars(days=300, symbols=4)
    factors = compute_core_factors(bars, Kernel(KernelBackend.PYTHON))
    labels = compute_forward_labels(bars)
    basic = bars[["trade_date", "symbol"]].copy()
    symbol_number = basic["symbol"].str.extract(r"(\d+)")[0].astype(int)
    basic["total_mv"] = 100_000 + symbol_number
    basic["pb"] = 1.0 + symbol_number.mod(10) / 10
    basic["turnover_rate_f"] = 1.0 + symbol_number.mod(3) / 10
    basic["industry"] = np.where(symbol_number.mod(2).eq(0), "bank", "industry")
    risk = compute_qm_style_v1(bars, basic)
    assert {
        "cross_momentum_20d", "cross_reversal_5d", "cross_realized_vol_20d",
        "cross_volume_ratio_20d", "cross_price_volume_corr_20d", "cross_amihud_20d",
    }.issubset(factors)
    assert {"fwd_return_1d", "fwd_return_3d", "fwd_return_5d", "fwd_return_7d"}.issubset(labels)
    assert {
        "SIZE_raw", "SIZE", "VALUE_raw", "VALUE", "MOMENTUM_raw", "MOMENTUM",
        "VOLATILITY_raw", "VOLATILITY", "LIQUIDITY_raw", "LIQUIDITY",
    }.issubset(risk)
    latest = risk.loc[risk["trade_date"].eq(risk["trade_date"].max())]
    assert latest[["SIZE", "VALUE", "MOMENTUM", "VOLATILITY", "LIQUIDITY"]].notna().all().all()


def test_future_continuous_uses_previous_overlap_ratio_at_roll():
    dates = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"])
    bars = pd.DataFrame([
        {"trade_date": day, "symbol": symbol, "close": close, "settle": settle, "volume": 10}
        for day, old_close, old_settle, new_close, new_settle in zip(
            dates, [100, 102, 104], [100, 102, 104], [200, 204, 208], [200, 204, 208],
            strict=True,
        )
        for symbol, close, settle in (
            ("CU2401.SHF", old_close, old_settle), ("CU2402.SHF", new_close, new_settle),
        )
    ])
    mapping = pd.DataFrame({
        "trade_date": dates,
        "symbol": ["CU.SHF"] * 3,
        "mapping_ts_code": ["CU2401.SHF", "CU2401.SHF", "CU2402.SHF"],
    })
    result = build_future_continuous(bars, mapping)
    rolled = result.iloc[-1]
    assert bool(rolled["roll_flag"])
    assert rolled["continuous_adj_factor"] == pytest.approx(102 / 204)
    assert rolled["settle_adj"] == pytest.approx(104)
    assert rolled["mapping_ts_code"] == "CU2402.SHF"


class FakePlanningAdapter:
    calls: ClassVar[list[tuple[str, str, str]]] = []

    def capabilities(self):
        return [{
            "dataset_id": item.id, "endpoint": item.endpoint, "state": "available",
            "detail": "fixture", "min_points": item.min_points, "premium": item.premium,
        } for item in DATASETS]

    def official_calendar(self, asset_class, start, end):
        self.calls.append((asset_class.value, start, end))
        return pd.bdate_range(start, end), "fixture"


def test_planner_merges_provider_work_and_expands_warmup():
    engine = ResearchEngine(adapter=FakePlanningAdapter())
    plan = engine.plan(
        "2024-03-01", "2024-03-05", asset_classes=(AssetClass.STOCK,),
        datasets=("stock_bars",),
        spec_ids=("cross_asset_core",),
    )
    compute = [task for task in plan.tasks if task.kind == "compute"]
    sync = [task for task in plan.tasks if task.kind == "sync"]
    assert len(compute) == 1
    assert compute[0].dataset_id == "cross_asset_core"
    assert sync[0].trade_date < plan.start
    assert len(plan.selected_specs) == 6
    assert not plan.capability_blocks


def test_planner_retries_temporary_provider_failure_instead_of_permanently_blocking():
    class TemporaryFailureAdapter(FakePlanningAdapter):
        def capabilities(self):
            values = super().capabilities()
            for value in values:
                if value["dataset_id"] == "stock_bars":
                    value.update({
                        "state": "temporary_failure",
                        "detail": "tushare 暂停请求，约 1800 秒后探测",
                    })
            return values

    engine = ResearchEngine(adapter=TemporaryFailureAdapter())
    plan = engine.plan(
        "2024-03-01", "2024-03-05", asset_classes=(AssetClass.STOCK,),
        datasets=("stock_bars",), mode="incremental",
    )

    assert not plan.capability_blocks
    assert any("重新探测数据源" in warning for warning in plan.warnings)


def test_engine_executes_offline_from_local_trading_dates_and_publishes_diagnostics():
    lake = ResearchLake()
    bars = synthetic_bars(days=32, symbols=4)
    for trade_date, group in bars.groupby(pd.to_datetime(bars["trade_date"]).dt.date):
        lake.write_partition(
            ArtifactKind.RAW, AssetClass.STOCK, Frequency.DAILY,
            "stock_bars", str(trade_date), group, run_id="raw-fixture",
        )
        lake.write_partition(
            ArtifactKind.RAW, AssetClass.STOCK, Frequency.DAILY,
            "stock_adj_factor", str(trade_date),
            group[["trade_date", "symbol"]].assign(adj_factor=1.0),
            run_id="raw-fixture",
        )

    class OfflineAdapter(FakePlanningAdapter):
        def official_calendar(self, asset_class, start, end):
            return pd.bdate_range(start, end), "fallback:offline fixture"

    engine = ResearchEngine(lake=lake, adapter=OfflineAdapter())
    dates = pd.DatetimeIndex(bars["trade_date"].unique()).sort_values()
    plan = engine.plan(
        str(dates[20].date()), str(dates[24].date()),
        asset_classes=(AssetClass.STOCK,), datasets=("stock_bars",),
        spec_ids=("cross_asset_core", "forward_returns"),
    )
    assert not [task for task in plan.tasks if task.kind == "sync"]
    assert any("本地已落盘交易日" in warning for warning in plan.warnings)
    manifest = engine.execute(plan)
    assert manifest["status"] == "completed"
    assert len(manifest["diagnostics"]) == 6
    reference = ArtifactRef(
        ArtifactKind.FACTOR, "cross_momentum_20d", "1.0.0", AssetClass.STOCK,
    )
    assert not lake.artifact_panel(reference, plan.start, plan.end).empty


def test_planner_refuses_unverified_business_day_fallback_without_local_calendar():
    class OfflineAdapter(FakePlanningAdapter):
        def official_calendar(self, asset_class, start, end):
            return pd.DatetimeIndex([]), "fallback:unavailable (offline)"

    engine = ResearchEngine(adapter=OfflineAdapter())
    with pytest.raises(RuntimeError, match="拒绝生成研究计划"):
        engine.plan(
            "2024-10-01", "2024-10-07",
            asset_classes=(AssetClass.STOCK,), datasets=("stock_bars",),
            spec_ids=("cross_asset_core",),
        )


def test_tushare_adapter_normalizes_cross_section_units():
    class Source:
        def _call(self, endpoint, _ttl, **_params):
            assert endpoint == "fut_daily"
            return pd.DataFrame({
                "ts_code": ["CU2401.SHF"], "trade_date": ["20240102"],
                "open": [10], "high": [11], "low": [9], "close": [10.5],
                "settle": [10.4], "pre_settle": [10.0], "vol": [3],
                "amount": [2.5], "oi": [8],
            })

    adapter = TushareResearchAdapter(ResearchLake().catalog, Source())
    value = adapter.fetch_date("future_bars", "2024-01-02")
    assert value.iloc[0]["symbol"] == "CU2401.SHF"
    assert value.iloc[0]["amount"] == 25_000
    assert value.iloc[0]["open_interest"] == 8


def test_tushare_adapter_fetches_etf_basic_as_snapshot():
    class Source:
        def _call(self, endpoint, _ttl, **params):
            assert endpoint == "fund_basic"
            assert params["market"] == "E"
            return pd.DataFrame({
                "ts_code": ["510300.SH"], "name": ["沪深300ETF"],
                "management": ["示例基金"], "custodian": ["示例银行"],
                "fund_type": ["股票型"], "found_date": ["20120504"],
                "list_date": ["20120528"], "delist_date": [None],
                "status": ["L"], "invest_type": ["被动指数型"], "market": ["E"],
            })

    value = TushareResearchAdapter(ResearchLake().catalog, Source()).fetch_date(
        "etf_basic", "2024-01-02",
    )
    assert value.iloc[0]["symbol"] == "510300.SH"
    assert value.iloc[0]["trade_date"] == pd.Timestamp("2024-01-02")


def test_diagnostics_include_ic_decay_years_and_turnover_efficiency():
    bars = synthetic_bars(days=40, symbols=8)
    factors = compute_core_factors(bars, Kernel(KernelBackend.PYTHON))
    labels = compute_forward_labels(bars)
    report = factor_diagnostics(factors, labels, "cross_momentum_20d")
    assert report["summary"]["ic_decay"]
    assert not report["ic_by_year"].empty
    assert set(report["coverage"]) >= {"trade_date", "coverage"}
    assert "turnover_efficiency" in report["summary"]


def test_rust_kernel_matches_python_when_extension_is_available():
    native = Kernel(KernelBackend.AUTO)
    if native.backend_used != KernelBackend.RUST:
        pytest.skip(native.fallback_reason)
    python = Kernel(KernelBackend.PYTHON)
    values = np.array([
        [1.0, 2.0, np.nan, 100.0],
        [4.0, 4.0, 2.0, 8.0],
        [np.nan, 3.0, 6.0, 9.0],
        [2.0, 4.0, 8.0, 16.0],
    ])
    weights = np.array([
        [1.0, 2.0, np.nan, 4.0],
        [1.0, 2.0, 3.0, 4.0],
        [np.nan, 2.0, 3.0, 4.0],
        [1.0, 2.0, 3.0, 4.0],
    ])
    calls = (
        (native.cross_section_rank(values), python.cross_section_rank(values)),
        (native.robust_standardize(values), python.robust_standardize(values)),
        (native.weighted_zscore(values, weights), python.weighted_zscore(values, weights)),
        (native.rolling_mean(values, 3), python.rolling_mean(values, 3)),
        (native.rolling_std(values, 3), python.rolling_std(values, 3)),
        (
            native.rolling_corr(values, values * 2, 3),
            python.rolling_corr(values, values * 2, 3),
        ),
    )
    for actual, expected in calls:
        assert np.array_equal(np.isnan(actual), np.isnan(expected))
        assert np.allclose(actual, expected, atol=1e-6, rtol=1e-6, equal_nan=True)


@pytest.mark.parametrize("k", [0, -1, float("nan"), float("inf")])
def test_kernel_rejects_invalid_robust_clipping_limit(k):
    for backend in (KernelBackend.PYTHON, KernelBackend.AUTO):
        with pytest.raises(ValueError, match="k 必须是有限正数"):
            Kernel(backend).robust_standardize([[1.0, 2.0]], k=k)


def test_artifact_factor_parser_and_missing_signal_error(panel):
    reference = parse_artifact_reference(
        "artifact:factor:stock:cross_momentum_20d@1.0.0"
    )
    assert reference and reference.kind == ArtifactKind.FACTOR
    factor = ArtifactFactor(reference)
    with pytest.raises(ValueError, match="研究产物不存在"):
        factor.compute(panel)
    assert parse_artifact_reference("artifact:factor:stock:bad") is None


def test_model_predictions_are_published_and_consumed_as_artifact_factor(panel):
    lake = ResearchLake()
    engine = ResearchEngine(lake=lake, adapter=FakePlanningAdapter())
    close = panel["close"]
    rows = close.stack().rename("value").reset_index()
    rows.columns = ["trade_date", "symbol", "value"]
    records = engine.publish_model_predictions(
        "demo_model", "1.0.0", AssetClass.STOCK, rows,
        run_id="model-publish-test",
    )
    reference = ArtifactRef(
        ArtifactKind.MODEL, "demo_model", "1.0.0", AssetClass.STOCK,
    )
    signal = ArtifactFactor(reference, lake).compute(panel)
    assert len(records) == len(close.index)
    assert np.allclose(signal.to_numpy(), close.to_numpy(), rtol=1e-6)


def test_empty_persistent_job_completes_and_keeps_manifest():
    manager = ResearchJobManager(ResearchEngine(adapter=FakePlanningAdapter()))
    plan = ExecutionPlan(
        id="empty-plan", start="2024-01-02", end="2024-01-02",
        target_dates=("2024-01-02",), asset_classes=(AssetClass.STOCK,),
        frequency=Frequency.DAILY, datasets=(), selected_specs=(), tasks=(),
    )
    job = manager.create(plan)
    job = manager.wait(job["id"], poll_seconds=0.01)
    assert job["status"] == "completed"
    assert job["manifest"]["plan_hash"]


def test_execution_plan_hash_excludes_runtime_identity():
    common = dict(
        start="2024-01-02", end="2024-01-03", target_dates=("2024-01-02",),
        asset_classes=(AssetClass.STOCK,), frequency=Frequency.DAILY,
        datasets=("stock_bars",), selected_specs=(), tasks=(),
    )
    first = ExecutionPlan(id="first", created_at="2026-01-01T00:00:00+00:00", **common)
    second = ExecutionPlan(id="second", created_at="2026-07-30T00:00:00+00:00", **common)
    assert first.plan_hash == second.plan_hash
    assert first.to_dict()["plan_hash"] == second.to_dict()["plan_hash"]


def test_research_job_lease_is_atomic_and_only_expired_owner_is_recovered(tmp_path):
    from quantmaster.research.catalog import ResearchCatalog

    catalog = ResearchCatalog(tmp_path / "catalog.sqlite")
    payload = ExecutionPlan(
        id="leased", start="2024-01-02", end="2024-01-02",
        target_dates=("2024-01-02",), asset_classes=(AssetClass.STOCK,),
        frequency=Frequency.DAILY, datasets=(), selected_specs=(), tasks=(),
    ).to_dict()
    catalog.create_job("job-one", "historical", payload)
    assert catalog.claim_job("job-one", "worker-a", lease_seconds=60)
    assert not catalog.claim_job("job-one", "worker-b", lease_seconds=60)
    assert catalog.recover_interrupted_jobs() == 0
    assert catalog.job("job-one")["status"] == "running"

    with catalog._connect() as connection:
        connection.execute(
            "UPDATE research_jobs SET lease_expires=0 WHERE id='job-one'"
        )
    assert catalog.recover_interrupted_jobs() == 1
    assert catalog.job("job-one")["status"] == "interrupted"


def test_research_retry_preserves_original_plan_and_records_attempt(tmp_path):
    from quantmaster.research.catalog import ResearchCatalog

    catalog = ResearchCatalog(tmp_path / "catalog.sqlite")
    plan = ExecutionPlan(
        id="immutable", start="2024-01-02", end="2024-01-02",
        target_dates=("2024-01-02",), asset_classes=(AssetClass.STOCK,),
        frequency=Frequency.DAILY, datasets=("stock_bars",), selected_specs=(),
        tasks=(PlanTask(
            "sync", "stock_bars", AssetClass.STOCK, Frequency.DAILY, "2024-01-02",
        ),),
    )
    original = plan.to_dict()
    catalog.create_job("retry-job", "historical", original)
    catalog.update_job(
        "retry-job", status="completed_with_errors", next_index=1, failed=1,
        failures_json=[{"task_index": 0, "task": plan.tasks[0].to_dict(), "error": "x"}],
    )
    resumed = catalog.resume_job("retry-job")
    assert resumed["plan"] == original
    assert resumed["attempt"] == 2
    assert resumed["task_indexes"] == [0]
    assert [event["type"] for event in catalog.job_events("retry-job")] == [
        "queued", "resumed",
    ]


def test_research_management_api_is_local_and_csrf_protected(monkeypatch):
    monkeypatch.setattr(
        TushareResearchAdapter,
        "official_calendar",
        lambda self, asset_class, start, end: (
            pd.date_range("2024-01-02", "2024-01-03"), "fixture:official",
        ),
    )
    client = TestClient(app)
    settings = client.get("/api/v1/settings")
    token = settings.json()["csrf_token"]
    catalog = client.get("/api/v1/research/data/catalog")
    assert catalog.status_code == 200
    assert len(catalog.json()["specs"]) == 66
    preview_frame = pd.DataFrame({
        "symbol": ["600519.SH"], "trade_date": [pd.Timestamp("2024-01-02")],
        "close": [1.0],
    })
    preview_frame.attrs["research_partition_quality"] = {
        "status": "degraded_preview", "formal_eligible": False,
        "expected_count": 2, "observed_count": 1, "coverage_ratio": 0.5,
        "missing_symbols": ["000001.SZ"],
    }
    monkeypatch.setattr(
        ResearchEngine,
        "preview_date",
        lambda _self, _dataset, _date: preview_frame,
    )
    assert client.post("/api/v1/research/data/preview", json={}).status_code == 403
    preview = client.post(
        "/api/v1/research/data/preview",
        headers={"X-CSRF-Token": token},
        json={"dataset_id": "stock_bars", "trade_date": "2024-01-02"},
    )
    assert preview.status_code == 200, preview.text
    assert preview.json()["tier"] == "sandbox"
    assert preview.json()["quality"]["formal_eligible"] is False
    assert preview.json()["rows"][0]["symbol"] == "600519.SH"
    assert client.post("/api/v1/research/data/plans", json={}).status_code == 403
    planned = client.post(
        "/api/v1/research/data/plans",
        headers={"X-CSRF-Token": token},
        json={
            "start": "2024-01-02", "end": "2024-01-03", "assets": ["stock"],
            "datasets": ["stock_bars"], "specs": [], "mode": "historical",
        },
    )
    assert planned.status_code == 200, planned.text
    assert planned.json()["capability_blocks"][0]["state"] == "unconfigured"
