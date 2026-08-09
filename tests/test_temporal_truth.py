from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd
import pytest

from quantmaster.data.industry import load_industry_map, save_industry_map
from quantmaster.data.instrument_snapshots import (
    TUSHARE_CATALOG_QUERY,
    freeze_instrument_catalog,
    snapshot_symbols,
)
from quantmaster.data.instruments import Instrument
from quantmaster.decision import resolve_policy
from quantmaster.decision.storage import DecisionStore
from quantmaster.lab.dataset import (
    create_snapshot,
    load_snapshot_evidence,
    verify_snapshot_evidence,
)
from quantmaster.lab.errors import LabError
from quantmaster.lab.models import FactorSpec
from quantmaster.lab.store import LabStore
from quantmaster.rotation.etf_research import (
    EtfResearchService,
    EtfResearchStore,
    etf_directory_master_hash,
)
from quantmaster.rotation.store import RotationStore as EtfMetadataStore
from quantmaster.trading_sessions import daily_signal_cutoff, market_date, market_now
from tests.catalog_evidence_helpers import bound_tushare_catalog


def _decision_panel() -> dict[str, pd.DataFrame]:
    return {
        "close": pd.DataFrame(
            [[10.0]], index=pd.to_datetime(["2026-08-09"]), columns=["600000.SH"],
        ),
    }


def _deployed_store(tmp_path, monkeypatch, created_at: str) -> tuple[LabStore, str]:
    monkeypatch.setattr("quantmaster.lab.store.utc_now", lambda: created_at)
    store = LabStore(tmp_path / "lab.sqlite")
    _factor, version, _created = store.create_factor(
        FactorSpec(slug="pit_factor", name="PIT 因子", expression="rank(close)")
    )
    store.save_validation(version["id"], "dataset", {
        "gates": {"hard_failures": [], "soft_failures": []},
        "candidate_score": 80,
    })
    store.approve(version["id"], actor="tester", reason="PIT cutoff test")
    deployment = store.deploy(
        version["id"], universe="demo", horizon=3, actor="tester",
    )
    return store, deployment["deployment_id"]


def test_market_clock_is_always_shanghai() -> None:
    instant = datetime(2026, 8, 8, 16, 30, tzinfo=UTC)
    assert market_now(instant).isoformat().startswith("2026-08-09T00:30")
    assert market_date(instant).isoformat() == "2026-08-09"


@pytest.mark.parametrize(
    ("created_at", "available"),
    [
        ("2026-08-09T06:59:00+00:00", True),   # 14:59 Shanghai
        ("2026-08-09T07:01:00+00:00", False),  # 15:01 Shanghai
        ("2026-08-09T15:00:00+00:00", False),  # 23:00 Shanghai
    ],
)
def test_deployment_as_of_uses_signal_cutoff(
    tmp_path, monkeypatch, created_at, available,
) -> None:
    store, deployment_id = _deployed_store(tmp_path, monkeypatch, created_at)

    ids = {item["id"] for item in store.deployments_as_of("2026-08-09")}
    assert (deployment_id in ids) is available


def test_historical_policy_uses_deployment_time_version_and_validation(
    tmp_path, monkeypatch,
) -> None:
    store, deployment_id = _deployed_store(
        tmp_path, monkeypatch, "2026-08-09T06:59:00+00:00",
    )
    deployment = next(
        item for item in store.deployments_as_of("2026-08-09")
        if item["id"] == deployment_id
    )
    version_id = deployment["version_id"]

    monkeypatch.setattr(
        "quantmaster.lab.store.utc_now", lambda: "2026-08-09T07:01:00+00:00",
    )
    store.save_validation(version_id, "future-dataset", {
        "gates": {"hard_failures": [], "soft_failures": []},
        "candidate_score": 999,
    })
    with store._conn() as connection:
        connection.execute(
            "UPDATE factor_versions SET spec_json=?,status='archived',updated_at=? WHERE id=?",
            (
                '{"name":"未来因子","kind":"expression","expression":"rank(open)"}',
                "2026-08-09T07:01:00+00:00",
                version_id,
            ),
        )

    policy = resolve_policy(
        "demo", 3, "stable", store=store,
        as_of="2026-08-09", mode="historical_replay",
    )
    factor = next(item for item in policy["components"] if item["role"] == "factor")
    assert factor["status"] == "production"
    assert factor["spec"]["expression"] == "rank(close)"
    assert factor["validation"]["candidate_score"] == 80


def test_historical_policy_rejects_deployment_without_frozen_evidence(
    tmp_path, monkeypatch,
) -> None:
    store, deployment_id = _deployed_store(
        tmp_path, monkeypatch, "2026-08-09T06:59:00+00:00",
    )
    with store._conn() as connection:
        connection.execute(
            "DELETE FROM deployment_evidence WHERE deployment_id=?", (deployment_id,),
        )
    with pytest.raises(RuntimeError, match="冻结版本/验证快照"):
        resolve_policy(
            "demo", 3, "stable", store=store,
            as_of="2026-08-09", mode="historical_replay",
        )


def test_historical_policy_rejects_missing_ledger_evidence() -> None:
    class EmptyStore:
        @staticmethod
        def active_deployments():
            return []

    with pytest.raises(RuntimeError, match="历史模型策略无法重建"):
        resolve_policy(
            "demo", 3, "stable", store=EmptyStore(),
            as_of="2026-08-09", mode="historical_replay",
        )


def _paper_candidate(
    store: LabStore, monkeypatch, *, name: str, day: str, information_ratio: float,
) -> dict:
    monkeypatch.setattr(
        "quantmaster.lab.store.utc_now", lambda: f"{day}T01:00:00+00:00",
    )
    cycle = store.create_research_cycle(snapshot_id=f"snapshot-{name}", protocol={})
    candidate = store.save_strategy_candidate(
        cycle_id=cycle["id"], horizon=3, name=name,
        components=[
            {"version_id": f"{name}-a", "weight": 0.34},
            {"version_id": f"{name}-b", "weight": 0.33},
            {"version_id": f"{name}-c", "weight": 0.33},
        ],
        development={},
        sealed_evidence={
            "gates": {"passed": True},
            "metrics": {
                "net_information_ratio": information_ratio,
                "max_drawdown": 0.20,
                "net_annual_excess_return": 0.10,
            },
        },
    )
    monkeypatch.setattr(
        "quantmaster.lab.store.utc_now", lambda: f"{day}T02:00:00+00:00",
    )
    store.update_strategy_tracking(candidate["id"], shadow={
        "matured_signal_days": 20,
        "net_excess_return": 0.01,
        "drawdown_within_stress": True,
        "coverage_degraded": False,
    })
    store.promote_strategy(
        candidate["id"], target="paper", actor="tester", reason="shadow passed",
    )
    store.update_strategy_tracking(candidate["id"], paper={
        "trading_days": 20, "net_return": 0.01, "persistent_anomalies": 0,
    })
    return store.strategy(candidate["id"]) or {}


@pytest.mark.parametrize(
    ("promoted_at", "available"),
    [
        ("2026-08-09T06:59:00+00:00", True),
        ("2026-08-09T07:01:00+00:00", False),
        ("2026-08-09T15:00:00+00:00", False),
    ],
)
def test_champion_events_use_same_signal_cutoff(
    tmp_path, monkeypatch, promoted_at, available,
) -> None:
    store = LabStore(tmp_path / "lab.sqlite")
    candidate = _paper_candidate(
        store, monkeypatch, name="Cutoff Champion", day="2026-08-08",
        information_ratio=0.50,
    )
    monkeypatch.setattr("quantmaster.lab.store.utc_now", lambda: promoted_at)
    store.promote_strategy(
        candidate["id"], target="champion", actor="tester", reason="cutoff test",
    )

    ids = {
        item["id"] for item in store.champion_strategies_as_of("2026-08-09", horizon=3)
    }
    assert (candidate["id"] in ids) is available


def test_champion_as_of_rejects_legacy_event_without_frozen_payload(
    tmp_path, monkeypatch,
) -> None:
    store = LabStore(tmp_path / "lab.sqlite")
    candidate = _paper_candidate(
        store, monkeypatch, name="Legacy Champion", day="2026-08-08",
        information_ratio=0.50,
    )
    monkeypatch.setattr(
        "quantmaster.lab.store.utc_now", lambda: "2026-08-09T06:59:00+00:00",
    )
    store.promote_strategy(
        candidate["id"], target="champion", actor="tester", reason="legacy test",
    )
    with store._conn() as connection:
        connection.execute(
            "UPDATE promotion_events SET evidence_json='{}' "
            "WHERE strategy_id=? AND to_status='champion'",
            (candidate["id"],),
        )

    with pytest.raises(RuntimeError, match="缺少冻结证据"):
        store.champion_strategies_as_of("2026-08-09", horizon=3)


def test_real_champion_replacement_freezes_history_and_never_returns_two(
    tmp_path, monkeypatch,
) -> None:
    store = LabStore(tmp_path / "lab.sqlite")
    first = _paper_candidate(
        store, monkeypatch, name="旧 Champion", day="2026-08-07", information_ratio=0.50,
    )
    monkeypatch.setattr(
        "quantmaster.lab.store.utc_now", lambda: "2026-08-07T03:00:00+00:00",
    )
    store.promote_strategy(
        first["id"], target="champion", actor="tester", reason="first champion",
    )
    second = _paper_candidate(
        store, monkeypatch, name="新 Champion", day="2026-08-08", information_ratio=0.70,
    )
    original_components = second["components"]
    monkeypatch.setattr(
        "quantmaster.lab.store.utc_now", lambda: "2026-08-08T03:00:00+00:00",
    )
    store.promote_strategy(
        second["id"], target="champion", actor="tester", reason="superior challenger",
    )

    old = store.strategy(first["id"]) or {}
    assert old["status"] == "degraded"
    assert any(
        event["from_status"] == "champion" and event["to_status"] == "degraded"
        for event in old["promotion_events"]
    )
    assert [item["id"] for item in store.champion_strategies_as_of(
        "2026-08-07", horizon=3,
    )] == [first["id"]]
    assert [item["id"] for item in store.champion_strategies_as_of(
        "2026-08-08", horizon=3,
    )] == [second["id"]]

    # Mutating the current projection later must not rewrite the event-time payload.
    monkeypatch.setattr(
        "quantmaster.lab.store.utc_now", lambda: "2026-08-09T12:00:00+00:00",
    )
    store.save_strategy_candidate(
        cycle_id=second["cycle_id"], horizon=3, name="未来修订名称",
        components=[
            {"version_id": "future-a", "weight": 0.34},
            {"version_id": "future-b", "weight": 0.33},
            {"version_id": "future-c", "weight": 0.33},
        ],
        development={"future": True},
        sealed_evidence={"gates": {"passed": True}, "future": True},
    )
    rebuilt = store.champion_strategies_as_of("2026-08-08", horizon=3)
    assert rebuilt[0]["components"] == original_components
    assert rebuilt[0]["sealed_evidence"]["metrics"]["net_information_ratio"] == 0.70

    # A damaged legacy ledger with the demotion missing is rejected, not sorted
    # into an arbitrary winner.
    with store._conn() as connection:
        connection.execute(
            "DELETE FROM promotion_events WHERE strategy_id=? "
            "AND from_status='champion' AND to_status='degraded'",
            (first["id"],),
        )
    with pytest.raises(RuntimeError, match="多个 Champion"):
        store.champion_strategies_as_of("2026-08-08", horizon=3)


def test_retrospective_decision_cannot_enter_formal_history(tmp_path) -> None:
    with pytest.raises(ValueError, match="retrospective"):
        DecisionStore(tmp_path / "decisions.sqlite").save(
            {"policy_mode": "retrospective"}, "demo",
        )


@pytest.mark.parametrize(
    ("deployed_at", "accepted"),
    [
        ("2026-08-09T06:59:00+00:00", True),
        ("2026-08-09T07:01:00+00:00", False),
    ],
)
def test_decision_store_checks_same_day_deployment_instant(
    tmp_path, deployed_at, accepted,
) -> None:
    report = {
        "policy_mode": "historical_replay",
        "signal_date": "2026-08-09",
        "generated_at": "2026-08-10T01:00:00+00:00",
        "holding_horizon_days": 3,
        "profile": "stable",
        "policy_hash": "policy",
        "model_version": "model",
        "model_snapshot": {
            "components": [{"name": "PIT model", "deployed_at": deployed_at}],
        },
    }
    store = DecisionStore(tmp_path / "decisions.sqlite")
    if accepted:
        store.save(report, "demo", panel=_decision_panel())
        assert store.latest("demo") == report
    else:
        with pytest.raises(ValueError, match="上海 15:00 后"):
            store.save(report, "demo", panel=_decision_panel())


def test_decision_store_rejects_nonbuiltin_component_without_deployment_time(tmp_path) -> None:
    report = {
        "policy_mode": "historical_replay",
        "signal_date": "2026-08-09",
        "generated_at": "2026-08-10T01:00:00+00:00",
        "holding_horizon_days": 3,
        "profile": "stable",
        "policy_hash": "policy",
        "model_version": "model",
        "model_snapshot": {
            "components": [{"name": "future", "version_id": "x"}],
        },
    }

    with pytest.raises(ValueError, match="缺少 deployed_at"):
        DecisionStore(tmp_path / "decisions.sqlite").save(
            report, "demo", panel=_decision_panel(),
        )


@pytest.mark.parametrize("future_field", ["close", "feature::news_sentiment"])
def test_decision_store_rejects_future_market_or_feature_rows(
    tmp_path, future_field,
) -> None:
    report = {
        "policy_mode": "historical_replay",
        "signal_date": "2026-08-09",
        "generated_at": "2026-08-10T01:00:00+00:00",
        "holding_horizon_days": 3,
        "profile": "stable",
        "policy_hash": "policy",
        "model_version": "model",
        "model_snapshot": {"components": []},
    }
    panel = _decision_panel()
    if future_field == "close":
        panel["close"].index = pd.to_datetime(["2026-08-10"])
    else:
        panel[future_field] = pd.DataFrame(
            [[0.5]],
            index=pd.to_datetime(["2026-08-10"]),
            columns=["600000.SH"],
        )

    with pytest.raises(ValueError, match="15:00 后"):
        DecisionStore(tmp_path / "decisions.sqlite").save(
            report,
            "demo",
            panel=panel,
        )


def test_lab_snapshot_evidence_is_recoverable_and_tamper_evident(tmp_path) -> None:
    dates = pd.to_datetime(["2026-08-07", "2026-08-08"])
    membership = pd.DataFrame(True, index=dates, columns=["600000.SH"])
    panel = {"close": pd.DataFrame([10.0, 10.1], index=dates, columns=["600000.SH"])}
    snapshot = create_snapshot(
        "csi800", "2026-08-07", "2026-08-08", panel=panel, membership=membership,
    ).to_dict()

    restored, restored_membership = load_snapshot_evidence(snapshot)
    pd.testing.assert_frame_equal(restored["close"], panel["close"])
    pd.testing.assert_frame_equal(restored_membership, membership)

    evidence = snapshot["manifest"]["evidence"]
    root = tmp_path / "data" / evidence["relative_root"]
    damaged = root / evidence["files"][0]["file"]
    damaged.write_bytes(damaged.read_bytes() + b"tampered")
    with pytest.raises(LabError, match="缺失或损坏"):
        verify_snapshot_evidence(snapshot)


def test_industry_history_keeps_preclose_observation_when_current_is_postclose(
    isolated_config, monkeypatch,
) -> None:
    from quantmaster.data import industry as industry_module
    from quantmaster.data import instrument_snapshots

    evidence = {
        "snapshot_id": "fixture-catalog",
        "snapshot_sha256": "fixture-catalog",
        "expected_count": 1,
        "as_of": "2026-08-09",
        "acquired_at": "2026-08-09T07:00:00+00:00",
        "source": "tushare:catalog",
    }
    monkeypatch.setattr(
        industry_module,
        "_active_cn_universe",
        lambda **_kwargs: ({"600000.SH"}, evidence),
    )
    monkeypatch.setattr(
        instrument_snapshots,
        "verify_instrument_catalog_evidence",
        lambda *_args, **_kwargs: (None, {"600000.SH"}),
    )
    monkeypatch.setattr(
        "quantmaster.data.industry._active_cn_symbols", lambda: {"600000.SH"},
    )
    save_industry_map(
        {"600000.SH": "银行-收盘前"},
        effective_as_of="2026-08-09",
        observed_at="2026-08-09T07:00:00+00:00",
        expected_symbols=1,
    )
    save_industry_map(
        {"600000.SH": "银行-盘后修订"},
        effective_as_of="2026-08-09",
        observed_at="2026-08-09T07:30:00+00:00",
        expected_symbols=1,
    )

    assert load_industry_map(as_of="2026-08-09") == {"600000.SH": "银行-收盘前"}
    assert load_industry_map() == {"600000.SH": "银行-盘后修订"}
    snapshots = list((isolated_config.data_root / "industry_map_history").glob("*.json"))
    assert len(snapshots) == 2


def _etf_directory(
    rows: list[dict],
    *,
    snapshot_id: str,
    effective_date: str,
    observed_at: str,
    monkeypatch,
) -> pd.DataFrame:
    target = pd.Timestamp(effective_date).normalize()
    requested_acquired = pd.Timestamp(observed_at)
    if requested_acquired.tzinfo is None:
        requested_acquired = requested_acquired.tz_localize("UTC")
    acquired = requested_acquired.tz_convert("Asia/Shanghai")
    cutoff = pd.Timestamp(daily_signal_cutoff(target.date()))
    if acquired.date() == target.date() and acquired < cutoff:
        acquired = cutoff + pd.Timedelta(seconds=1)
    etf_records = [
        {
            "symbol": str(row["symbol"]).upper(),
            "name": str(row.get("name") or row["symbol"]),
            "market": "CN",
            "exchange": str(row["symbol"]).rsplit(".", 1)[-1],
            "asset_type": "etf",
            "status": {
                "listed": "L", "active": "L", "delisted": "D",
            }.get(str(row.get("status") or "L").lower(), str(row.get("status") or "L")),
            "list_date": str(row.get("list_date") or "2020-01-01"),
            "delist_date": str(row.get("delist_date") or ""),
        }
        for row in rows
    ]
    for item in etf_records:
        delisted = pd.Timestamp(item["delist_date"]) if item["delist_date"] else None
        if item["status"] == "D" and delisted is not None and delisted >= target:
            item["status"] = "L"
            item["delist_date"] = ""
    monkeypatch.setattr(
        "quantmaster.data.instrument_snapshots.TUSHARE_MINIMUM_ASSET_COUNTS",
        {"CN:stock": 3000, "CN:etf": 1},
    )
    stock_records = [
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
        [*stock_records, *etf_records],
    )
    snapshot = freeze_instrument_catalog(
        catalog_records,
        source="tushare:catalog",
        query=TUSHARE_CATALOG_QUERY,
        request_outcomes=catalog_outcomes,
        acquired_at=acquired.to_pydatetime(),
    )
    expected_symbols = snapshot_symbols(
        snapshot,
        market="CN",
        asset_type="etf",
        as_of=effective_date,
    )
    expected = len(expected_symbols)
    evidence = snapshot.evidence(
        market="CN", asset_type="etf", as_of=effective_date
    )
    lifecycle = {item["symbol"]: item for item in etf_records}
    metadata_observed = max(requested_acquired.tz_convert("UTC"), acquired.tz_convert("UTC"))
    frame = pd.DataFrame(
        [
            {
                **row,
                "exchange": str(row["symbol"]).rsplit(".", 1)[-1],
                "asset_type": "etf",
                "status": lifecycle[str(row["symbol"]).upper()]["status"],
                "list_date": lifecycle[str(row["symbol"]).upper()]["list_date"],
                "delist_date": lifecycle[str(row["symbol"]).upper()]["delist_date"],
                "metadata_source": "free-stockdb:security-master",
                "effective_date": effective_date,
                "updated_at": effective_date,
                "observed_at": metadata_observed.isoformat(),
                "directory_snapshot_id": snapshot_id,
                "directory_complete": True,
                "directory_expected_symbols": expected,
                "directory_observed_symbols": expected,
                "directory_member_source": "tushare:catalog",
                "directory_member_observed_at": snapshot.acquired_at,
                "directory_source": "tushare:catalog",
                "directory_acquired_at": snapshot.acquired_at,
                "directory_cutoff_at": cutoff.isoformat(),
                "directory_freshness": "fresh",
                "directory_master_record_count": len(snapshot.records),
                "directory_master_batch_record_count": len(snapshot.records),
                "directory_master_snapshot_sha256": snapshot.snapshot_id,
                "directory_catalog_snapshot_id": snapshot.snapshot_id,
                "directory_catalog_records_sha256": evidence["records_sha256"],
                "directory_catalog_file_sha256": snapshot.file_sha256,
                "directory_catalog_file_size": str(snapshot.file_size),
                "directory_catalog_file_mtime_ns": str(snapshot.file_mtime_ns),
                "directory_catalog_relative_path": evidence["relative_path"],
                "directory_catalog_as_of": effective_date,
                "directory_catalog_expected_count": expected,
                "directory_test_label": snapshot_id,
            }
            for row in rows
            if str(row["symbol"]).upper() in expected_symbols
        ]
    )
    master_hash = etf_directory_master_hash(frame)
    frame["directory_attestation_sha256"] = master_hash
    frame["directory_snapshot_id"] = "etf_directory_" + master_hash[:24]
    return frame


def test_etf_historical_profiles_fail_closed_without_verified_directory(
    tmp_path, monkeypatch,
) -> None:
    metadata = pd.DataFrame([{
        "symbol": "510300.SH", "name": "未来名称ETF", "benchmark": "未来指数",
        "metadata_source": "tushare:fund_basic", "updated_at": "2026-08-10",
        "list_date": "2020-01-01",
    }])

    class RotationStore:
        @staticmethod
        def etf_metadata():
            return metadata

        @staticmethod
        def etf_observations():
            return pd.DataFrame()

    class Instruments:
        @staticmethod
        def list(*, market=""):
            return [
                Instrument(
                    "510300.SH", "510300", "当前名称ETF", "CN", "SH", "etf",
                    list_date="2020-01-01",
                ),
                Instrument(
                    "588888.SH", "588888", "未来上市ETF", "CN", "SH", "etf",
                    list_date="2026-08-10",
                ),
            ]

    monkeypatch.setattr("quantmaster.rotation.store.RotationStore", RotationStore)
    service = EtfResearchService(
        source=object(), instruments=Instruments(), ingest_store=object(),
        store=EtfResearchStore(tmp_path / "etf-research"),
    )
    with pytest.raises(RuntimeError, match="没有完整、可复验"):
        service.profiles(as_of="2026-08-09")


def test_etf_same_day_metadata_uses_latest_verified_closing_catalog(
    tmp_path, monkeypatch,
) -> None:
    metadata = pd.concat(
        [
            _etf_directory(
                [
                    {"symbol": "510300.SH", "name": "收盘前名称", "benchmark": "收盘前指数"},
                    {"symbol": "510500.SH", "name": "收盘前500", "benchmark": "收盘前500指数"},
                ],
                snapshot_id="directory-preclose",
                effective_date="2026-08-09",
                observed_at="2026-08-09T06:59:00+00:00",
                monkeypatch=monkeypatch,
            ),
            _etf_directory(
                [
                    {"symbol": "510300.SH", "name": "盘后名称", "benchmark": "盘后指数"},
                    {"symbol": "510500.SH", "name": "盘后500", "benchmark": "盘后500指数"},
                ],
                snapshot_id="directory-postclose",
                effective_date="2026-08-09",
                observed_at="2026-08-09T07:01:00+00:00",
                monkeypatch=monkeypatch,
            ),
        ],
        ignore_index=True,
    )

    class RotationStore:
        @staticmethod
        def etf_metadata():
            return metadata

        @staticmethod
        def etf_observations():
            return pd.DataFrame()

    class Instruments:
        @staticmethod
        def list(*, market=""):
            return [
                Instrument(
                    "510300.SH", "510300", "当前300ETF", "CN", "SH", "etf",
                    list_date="2020-01-01",
                ),
                Instrument(
                    "510500.SH", "510500", "当前500ETF", "CN", "SH", "etf",
                    list_date="2020-01-01",
                ),
            ]

    monkeypatch.setattr("quantmaster.rotation.store.RotationStore", RotationStore)
    service = EtfResearchService(
        source=object(), instruments=Instruments(), ingest_store=object(),
        store=EtfResearchStore(tmp_path / "etf-research"),
    )
    profiles = {item.symbol: item for item in service.profiles(as_of="2026-08-09")}

    assert profiles["510300.SH"].name == "盘后名称"
    assert profiles["510300.SH"].benchmark == "盘后指数"
    assert profiles["510500.SH"].name == "盘后500"
    assert profiles["510500.SH"].benchmark == "盘后500指数"


def test_etf_store_preserves_target_day_catalog_and_rejects_late_backfill(
    tmp_path, monkeypatch,
) -> None:
    metadata_store = EtfMetadataStore(tmp_path / "rotation")
    metadata_store.save_etf_metadata(
        _etf_directory(
            [{"symbol": "510300.SH", "name": "收盘前名称", "benchmark": "收盘前指数"}],
            snapshot_id="directory-preclose",
            effective_date="2026-08-09",
            observed_at="2026-08-09T06:59:00+00:00",
            monkeypatch=monkeypatch,
        )
    )
    metadata_store.save_etf_metadata(
        _etf_directory(
            [
                {"symbol": "510300.SH", "name": "盘后名称", "benchmark": "盘后指数"},
                {"symbol": "510500.SH", "name": "晚采集旧日期", "benchmark": "未来所知指数"},
            ],
            snapshot_id="directory-late",
            effective_date="2026-08-08",
            observed_at="2026-08-10T01:00:00+00:00",
            monkeypatch=monkeypatch,
        )
    )

    class Instruments:
        @staticmethod
        def list(*, market=""):
            return [
                Instrument(
                    "510300.SH", "510300", "当前300ETF", "CN", "SH", "etf",
                    list_date="2020-01-01",
                ),
                Instrument(
                    "510500.SH", "510500", "当前500ETF", "CN", "SH", "etf",
                    list_date="2020-01-01",
                ),
            ]

    monkeypatch.setattr("quantmaster.rotation.store.RotationStore", lambda: metadata_store)
    service = EtfResearchService(
        source=object(), instruments=Instruments(), ingest_store=object(),
        store=EtfResearchStore(tmp_path / "etf-research"),
    )
    profiles = {item.symbol: item for item in service.profiles(as_of="2026-08-09")}

    assert len(metadata_store.etf_metadata_history()) == 3
    assert profiles["510300.SH"].name == "收盘前名称"
    assert "510500.SH" not in profiles


def test_etf_share_observation_rejects_late_acquisition_of_old_trade_date(
    tmp_path, monkeypatch,
) -> None:
    observations = pd.DataFrame([
        {
            "symbol": "510300.SH", "trade_date": "2026-08-07",
            "benchmark": "晚到未来指数", "fund_type": "未来类型",
            "invest_type": "未来风格", "acquired_at": "2026-08-09T01:00:00+00:00",
        },
        {
            "symbol": "510500.SH", "trade_date": "2026-08-07",
            "benchmark": "截止前指数", "fund_type": "股票型",
            "invest_type": "被动", "acquired_at": "2026-08-08T06:59:00+00:00",
        },
    ])
    metadata = _etf_directory(
        [
            {"symbol": "510300.SH", "name": "300ETF"},
            {"symbol": "510500.SH", "name": "500ETF"},
        ],
        snapshot_id="directory-share-cutoff",
        effective_date="2026-08-08",
        observed_at="2026-08-08T06:00:00+00:00",
        monkeypatch=monkeypatch,
    )

    class RotationStore:
        @staticmethod
        def etf_metadata_history():
            return metadata

        @staticmethod
        def etf_observations():
            return observations

    class Instruments:
        @staticmethod
        def list(*, market=""):
            return [
                Instrument(
                    "510300.SH", "510300", "当前300ETF", "CN", "SH", "etf",
                    list_date="2020-01-01",
                ),
                Instrument(
                    "510500.SH", "510500", "当前500ETF", "CN", "SH", "etf",
                    list_date="2020-01-01",
                ),
            ]

    monkeypatch.setattr("quantmaster.rotation.store.RotationStore", RotationStore)
    service = EtfResearchService(
        source=object(), instruments=Instruments(), ingest_store=object(),
        store=EtfResearchStore(tmp_path / "etf-research"),
    )
    profiles = {item.symbol: item for item in service.profiles(as_of="2026-08-08")}

    assert profiles["510300.SH"].benchmark == ""
    assert profiles["510500.SH"].benchmark == "截止前指数"


def test_etf_historical_directory_keeps_products_delisted_after_replay_date(
    tmp_path, monkeypatch,
) -> None:
    metadata = _etf_directory(
        [
            {"symbol": "510300.SH", "name": "当前ETF"},
            {
                "symbol": "510500.SH",
                "name": "后来退市ETF",
                "status": "delisted",
                "delist_date": "2026-07-31",
            },
            {
                "symbol": "510880.SH",
                "name": "此前退市ETF",
                "status": "delisted",
                "delist_date": "2026-06-30",
            },
        ],
        snapshot_id="directory-with-delisted",
        effective_date="2026-07-01",
        observed_at="2026-07-01T06:59:00+00:00",
        monkeypatch=monkeypatch,
    )

    class RotationStore:
        @staticmethod
        def etf_metadata_history():
            return metadata

        @staticmethod
        def etf_observations():
            return pd.DataFrame()

    class CurrentInstruments:
        @staticmethod
        def list(*, market=""):
            return [
                Instrument("510300.SH", "510300", "当前ETF", "CN", "SH", "etf"),
                Instrument(
                    "510500.SH", "510500", "后来退市ETF", "CN", "SH", "etf",
                    status="delisted", delist_date="2026-07-31",
                ),
            ]

    monkeypatch.setattr("quantmaster.rotation.store.RotationStore", RotationStore)
    service = EtfResearchService(
        source=object(), instruments=CurrentInstruments(), ingest_store=object(),
        store=EtfResearchStore(tmp_path / "etf-research"),
    )

    profiles = service.profiles(as_of="2026-07-01")

    assert [item.symbol for item in profiles] == ["510300.SH", "510500.SH"]
