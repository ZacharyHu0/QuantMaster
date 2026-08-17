from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from quantmaster.after_close.service import AfterCloseService, DataGateRejected
from quantmaster.after_close.store import AfterCloseStore
from quantmaster.data.instruments import Instrument
from quantmaster.trading_sessions import SessionExpectation


class _Instruments:
    def __init__(self) -> None:
        self.items = [
            Instrument("600001.SH", "600001", "甲公司", "CN", "SH", "stock"),
            Instrument("000001.SZ", "000001", "乙公司", "CN", "SZ", "stock"),
            Instrument("300001.SZ", "300001", "ST丙公司", "CN", "SZ", "stock"),
            Instrument("830001.BJ", "830001", "丁公司", "CN", "BJ", "stock"),
        ]

    def list(self, **_filters):
        return list(self.items)


class _Source:
    name = "free-stockdb"
    sdk_path = "C:/local/stock_sdk.py"

    def __init__(self, frame: pd.DataFrame):
        self.frame = frame

    def daily_cross_section(self, symbols, start, end):
        dates = pd.to_datetime(self.frame["date"])
        return self.frame.loc[
            self.frame["symbol"].isin(symbols) & (dates >= pd.Timestamp(start)) & (dates <= pd.Timestamp(end))
        ].copy()

    def adjustment_factors(self, symbols, start, end):
        dates = pd.to_datetime(self.frame["date"])
        value = self.frame.loc[
            self.frame["symbol"].isin(symbols)
            & (dates >= pd.Timestamp(start))
            & (dates <= pd.Timestamp(end)),
            ["symbol", "date"],
        ].drop_duplicates()
        return value.assign(adj_factor=1.0)

    def board_hierarchy(self):
        symbols = sorted(self.frame["symbol"].unique())
        return [
            {
                "code": "801010.SL",
                "name": "一级甲",
                "category": "申万一级",
                "level": "L1",
                "members": symbols,
            },
            {
                "code": "801011.SL",
                "name": "二级甲",
                "category": "申万二级",
                "level": "L2",
                "members": symbols[:2],
            },
            {
                "code": "BK_TEST",
                "name": "测试概念",
                "category": "概念",
                "level": "CONCEPT",
                "members": symbols[1:],
            },
        ]

    def native_batch_available(self):
        return True

    def sdk_version(self):
        return "test-sdk"


def _frame(days: int = 190) -> pd.DataFrame:
    dates = pd.bdate_range(end="2026-08-05", periods=days)
    rows = []
    specs = {
        "600001.SH": (10.0, 1.20, 80_000_000.0, False),
        "000001.SZ": (12.0, 1.10, 55_000_000.0, False),
        "300001.SZ": (8.0, 1.30, 90_000_000.0, True),
        "830001.BJ": (6.0, 1.05, 8_000_000.0, False),
    }
    for symbol, (base, growth, amount, is_st) in specs.items():
        closes = np.linspace(base, base * growth, len(dates))
        for stamp, close in zip(dates, closes, strict=True):
            rows.append(
                {
                    "symbol": symbol,
                    "date": stamp,
                    "open": close * 0.99,
                    "high": close * 1.01,
                    "low": close * 0.98,
                    "close": close,
                    "volume": 1_000_000,
                    "amount": amount,
                    "float_mv": 1e10,
                    "total_mv": 1.2e10,
                    "pe_ttm": 20.0,
                    "pb": 2.0,
                    "is_st": is_st,
                }
            )
    return pd.DataFrame(rows)


@pytest.fixture
def service(tmp_path, isolated_config, monkeypatch):
    isolated_config.data.after_close_min_listing_sessions = 60
    isolated_config.data.after_close_min_avg_amount = 30_000_000
    isolated_config.data.after_close_candidate_limit = 30
    monkeypatch.setattr(
        "quantmaster.after_close.service.expected_session",
        lambda: SessionExpectation(ready=False, reason="test"),
    )
    monkeypatch.setattr(
        "quantmaster.data.index_membership.load_cached_csi800_members_as_of",
        lambda as_of, **_kwargs: {
            "as_of": as_of,
            "symbols": ["000001.SZ", "600001.SH"],
            "snapshot_dates": {"000300.SH": as_of, "000905.SH": as_of},
            "dataset": "csi800_membership",
            "source": "research_lake:tushare:index_weight",
            "content_hash": "test-membership-hash",
        },
    )
    value = AfterCloseService(
        source=_Source(_frame()),
        instruments=_Instruments(),
        store=AfterCloseStore(tmp_path / "after_close.sqlite"),
    )
    monkeypatch.setattr(value, "_write_research_lake", lambda *_args: None)
    return value


def test_after_close_scan_is_immutable_auditable_and_filters_stock_pool(service) -> None:
    first = service.scan()
    replay = service.scan(as_of="2026-08-05")

    assert replay.snapshot_id == first.snapshot_id
    assert replay.input_hash == first.input_hash
    assert [item.symbol for item in first.candidates] == ["600001.SH", "000001.SZ"]
    assert first.excluded_counts == {"liquidity": 1, "st": 1}
    assert first.coverage["field_coverage"]["pe_ttm"]["latest_ratio"] == 1
    assert {item.level for item in first.sectors} == {"L1", "L2", "CONCEPT"}
    assert first.provenance["sdk_version"] == "test-sdk"
    assert first.validation["promotion"] == "research_observation_only"
    assert first.validation["baselines"]["csi800"]["status"] == "available"
    assert all(item.snapshot_id == first.snapshot_id for item in first.sectors)
    assert all(item.snapshot_id == first.snapshot_id for item in first.candidates)
    assert all(item.score_version == first.score_version for item in first.sectors)
    assert first.score_version == "QM_AFTER_CLOSE_V1"
    assert first.shadow_candidates
    assert all(item.score_version == "QM_AFTER_CLOSE_V2_SHADOW" for item in first.shadow_candidates)
    assert first.validation["shadow_comparison"]["formal_score_version"] == "QM_AFTER_CLOSE_V1"
    assert all(item.sensitivity.get("amount_weighted") for item in first.sectors)
    ingest = service.ingest.store.get(first.ingest_id)
    assert ingest is not None
    assert len(ingest.session_dates) == 180
    assert ingest.session_source == "stockdb_broad_coverage"
    assert ingest.catalog_id.startswith("sdc_")
    assert ingest.coverage["observed_history_sessions"] == 180
    assert any(item["field"] == "amount" and item["unit"] == "CNY" for item in ingest.coverage["fields"])
    assert service.ingest.store.references(first.ingest_id)[0]["namespace"] == "after_close"

    revised = service.source.frame.copy()
    mask = (revised["symbol"] == "600001.SH") & (
        revised["date"] == revised.loc[revised["symbol"] == "600001.SH", "date"].min()
    )
    revised.loc[mask, "close"] *= 1.01
    assert service._frame_hash(revised) != service._frame_hash(service.source.frame)


def test_gate_failure_keeps_previous_snapshot_and_marks_it_stale(service) -> None:
    published = service.scan()
    service.source.frame = service.source.frame.loc[service.source.frame["symbol"] == "600001.SH"].copy()

    with pytest.raises(DataGateRejected):
        service.scan(force=True)

    latest = service.store.public_latest()
    assert latest["snapshot_id"] == published.snapshot_id
    assert latest["staleness"]["stale"] is True
    assert "覆盖" in latest["staleness"]["reason"]


def test_gate_rejects_first_snapshot_with_stable_twenty_percent_symbol_gap(service) -> None:
    with pytest.raises(DataGateRejected, match="没有停牌/退市证据"):
        service._gate(service.source.frame, service.source.board_hierarchy(), expected_count=5)


def test_gate_accepts_missing_symbol_only_with_explicit_suspension_evidence(
    service, isolated_config, monkeypatch,
) -> None:
    from quantmaster.data import instrument_snapshots

    isolated_config.data.tushare_token = "test-token"
    symbols = [*sorted(service.source.frame["symbol"].unique()), "000002.SZ"]
    monkeypatch.setattr(
        instrument_snapshots,
        "load_suspension_snapshot",
        lambda _date: {
            "source": "tushare:suspend_d",
            "contract": "tushare-suspend_d-trade-date-v1",
            "symbols": ["000002.SZ"],
            "content_hash": "suspension-proof",
        },
    )

    _as_of, coverage = service._gate(
        service.source.frame,
        service.source.board_hierarchy(),
        expected_count=len(symbols),
        expected_symbols=symbols,
    )

    assert coverage["status"] == "complete"
    assert coverage["catalog_symbols"] == 5
    assert coverage["expected_symbols"] == 4
    assert coverage["excused_suspended_symbols"] == ["000002.SZ"]
    assert coverage["suspension_evidence"]["content_hash"] == "suspension-proof"


def test_gate_rejects_nonfinite_and_impossible_ohlcv(service) -> None:
    frame = service.source.frame.copy()
    latest = pd.to_datetime(frame["date"]).eq(pd.to_datetime(frame["date"]).max())
    frame.loc[latest, "close"] = np.inf
    frame.loc[latest, "volume"] = -1

    with pytest.raises(DataGateRejected) as caught:
        service._gate(frame, service.source.board_hierarchy(), expected_count=4)

    assert caught.value.coverage["required_ohlcv_ratio"] == 0.0
    assert caught.value.coverage["invalid_ohlcv"]["nonfinite_rows"] == 4


def test_historical_force_rejects_current_board_taxonomy(service, monkeypatch) -> None:
    monkeypatch.setattr(
        "quantmaster.after_close.service.resolve_session_target",
        lambda as_of: SessionExpectation(
            as_of, "fixture", True, "verified", "previous_session_complete",
        ),
    )
    with pytest.raises(DataGateRejected, match="当前分类"):
        service.scan(as_of="2026-08-05", force=True)


def test_historical_force_rejects_future_dated_board_taxonomy(service, monkeypatch) -> None:
    monkeypatch.setattr(
        "quantmaster.after_close.service.resolve_session_target",
        lambda as_of: SessionExpectation(
            as_of, "fixture", True, "verified", "previous_session_complete",
        ),
    )
    original = service.source.board_hierarchy

    def future_boards():
        return [
            {**item, "effective_date": "2026-08-06"}
            for item in original()
        ]

    service.source.board_hierarchy = future_boards
    with pytest.raises(DataGateRejected, match="晚于历史目标日"):
        service.scan(as_of="2026-08-05", force=True)


def test_future_labels_use_only_realized_sessions_and_market_baseline(service) -> None:
    snapshot = service.scan()
    future_dates = pd.bdate_range("2026-08-06", periods=7)
    extension = []
    for _symbol, group in service.source.frame.groupby("symbol"):
        latest = group.sort_values("date").iloc[-1]
        for offset, stamp in enumerate(future_dates, 1):
            row = latest.to_dict()
            row.update(date=stamp, close=float(latest["close"]) * (1 + offset * 0.01))
            extension.append(row)
    realized = pd.concat((service.source.frame, pd.DataFrame(extension)), ignore_index=True)

    service.evaluate_pending(realized)
    labels = service.store.labels(snapshot.snapshot_id)

    # Seven realized sessions mature only the 1/3/5/7-day labels.
    assert [item["horizon"] for item in labels] == [1, 3, 5, 7]
    assert all(item["baseline"] == "all_market" for item in labels)
    assert all(item["market_mean_return"] is not None for item in labels)
    assert all(item["csi800_mean_return"] is not None for item in labels)
    assert all(item["excess_vs_csi800"] is not None for item in labels)
    assert all(item["mean_max_drawdown"] <= 0 for item in labels)
    assert all("QM_AFTER_CLOSE_V2_SHADOW" in item["score_versions"] for item in labels)


def test_strategy_health_never_promotes_research_to_trading(service) -> None:
    service.scan()
    health = service.store.health()

    assert health["candidate_promotion_allowed"] is False
    assert health["status"] == "observation"


def test_board_membership_lake_uses_relationship_key(service) -> None:
    from quantmaster.research.contracts import ArtifactKind, AssetClass, Frequency
    from quantmaster.research.lake import ResearchLake

    snapshot = service.scan()
    boards = service.source.board_hierarchy()
    AfterCloseService._write_research_lake(
        service,
        snapshot,
        service.source.frame,
        boards,
    )
    frame = ResearchLake().read_partition(
        ArtifactKind.RAW,
        AssetClass.STOCK,
        Frequency.DAILY,
        "after_close_board_membership",
        snapshot.as_of_date,
    )

    assert not frame.empty
    assert "component_symbol" in frame
    assert frame["symbol"].is_unique
    assert frame["component_symbol"].duplicated().any()


def test_after_close_api_and_web_share_the_same_snapshot(service, monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from quantmaster.server import after_close as routes
    from quantmaster.server.app import app

    snapshot = service.scan()
    monkeypatch.setattr(routes, "get_after_close_service", lambda **_kwargs: service)
    client = TestClient(app)

    latest = client.get("/api/v1/after-close/snapshots/latest")
    diagnostics = client.get("/api/v1/after-close/diagnostics")
    old_health = client.get("/api/v1/after-close/health")
    exported = client.get(f"/api/v1/after-close/export/{snapshot.snapshot_id}?format=csv")
    page = client.get("/")

    assert latest.status_code == 200
    assert diagnostics.status_code == 200
    assert old_health.status_code == 200
    assert latest.json()["snapshot"]["snapshot_id"] == snapshot.snapshot_id
    assert exported.status_code == 200
    assert "600001.SH" in exported.content.decode("utf-8-sig")
    assert 'id="tab-after-close"' in page.text
    assert "/static/after-close.js" not in page.text
    assert "Tushare 和其他页面的行情缓存不参与本扫描" in page.text
    assert "data-after-close-open-stockdb" in page.text
    assert 'id="after-close-update-data"' in page.text
    assert "更新扫描数据" in page.text
    assert "按当前数据重新计算" in page.text
    assert "强制重跑" not in page.text

    script = client.get("/static/after-close.js")
    assert script.status_code == 200
    assert "盘后扫描专用的 free-stockdb" in script.text
    assert "quantmaster:navigate" in script.text
    assert "section:'local-data'" in script.text
    assert "/api/v1/settings/free-stockdb/update" in script.text
    assert "/api/v1/after-close/diagnostics?limit=500" in script.text
    assert "/api/v1/after-close/health" not in script.text
    assert "扫描数据已更新至" in script.text


def test_after_close_invalid_snapshot_path_returns_404(
    service, monkeypatch,
) -> None:
    from fastapi.testclient import TestClient

    from quantmaster.server import after_close as routes
    from quantmaster.server.app import app

    service.scan()
    monkeypatch.setattr(routes, "get_after_close_service", lambda **_kwargs: service)
    client = TestClient(app)

    # The literal path "after-close" must NOT match the
    # {snapshot_id} route — snapshot IDs always start with "ac_".
    bad = client.get("/api/v1/after-close/snapshots/after-close")
    assert bad.status_code == 404

    # A path that looks plausible but is not a valid snapshot ID
    # should also be rejected at the router layer.
    not_a_snapshot = client.get(
        "/api/v1/after-close/snapshots/2026-08-15"
    )
    assert not_a_snapshot.status_code == 404

    # The legitimately-generated snapshot ID must still resolve.
    good = client.get(f"/api/v1/after-close/snapshots/{service.store.latest().snapshot_id}")
    assert good.status_code == 200


def test_after_close_empty_snapshot_returns_problem_503(
    monkeypatch,
) -> None:
    import tempfile
    from pathlib import Path as _Path

    from fastapi.testclient import TestClient

    from quantmaster.after_close.store import AfterCloseStore
    from quantmaster.server import after_close as routes
    from quantmaster.server.app import app

    # Use read_only=True so the store doesn't create the DB file.
    # _published_service checks if the file exists; when it doesn't,
    # it raises 503 before latest() even runs.
    with tempfile.TemporaryDirectory() as td:
        db_path = _Path(td) / "nonexistent.sqlite"
        fake_store = AfterCloseStore(path=db_path, read_only=True)
        fake_service = type("FakeService", (), {"store": fake_store})()

        monkeypatch.setattr(
            routes, "get_after_close_service",
            lambda **_kwargs: fake_service,
        )

        client = TestClient(app)
        resp = client.get("/api/v1/after-close/snapshots/latest")

    assert resp.status_code == 503
    body = resp.json()
    assert body["problem"]["id"].endswith("snapshot_unavailable")


def test_after_close_cli_contract_supports_csv_export() -> None:
    from quantmaster.cli import build_parser

    args = build_parser().parse_args(
        [
            "after-close",
            "export",
            "scan.csv",
            "--format",
            "csv",
        ]
    )

    assert args.after_close_cmd == "export"
    assert args.format == "csv"

    score = build_parser().parse_args(["after-close", "score-version", "status"])
    assert score.score_version_cmd == "status"


def test_after_close_current_decoder_never_guesses_old_or_damaged_payload(service) -> None:
    from quantmaster.after_close.models import AfterCloseSnapshot

    payload = service.scan().to_dict()
    old = dict(payload)
    old["schema_version"] = "1.1"
    with pytest.raises(ValueError, match="当前 schema"):
        AfterCloseSnapshot.from_dict(old)
    damaged = dict(payload)
    damaged.pop("validation")
    with pytest.raises(ValueError, match="字段不匹配"):
        AfterCloseSnapshot.from_dict(damaged)


def test_after_close_one_shot_migration_uses_schema_label_and_leaves_new_facts_empty(service) -> None:
    from quantmaster.after_close.migration import (
        inspect_after_close_snapshots,
        migrate_after_close_batch,
    )
    from quantmaster.runtime.sqlite import connect_sqlite

    snapshot = service.scan()
    payload = snapshot.to_dict()
    payload["schema_version"] = "1.0"
    payload.pop("ingest_id")
    payload.pop("artifact_id")
    payload.pop("shadow_candidates")
    for sector in payload["sectors"]:
        sector.pop("sensitivity")
    for candidate in payload["candidates"]:
        candidate.pop("shadow")
    with connect_sqlite(service.store.path) as connection:
        connection.execute("ALTER TABLE snapshots ADD COLUMN payload_hash TEXT NOT NULL DEFAULT ''")
        connection.execute(
            "UPDATE snapshots SET payload_json=?,payload_hash='retired' WHERE snapshot_id=?",
            (json.dumps(payload, ensure_ascii=False), snapshot.snapshot_id),
        )

    planned = inspect_after_close_snapshots(service.store.path.parent)
    assert planned[0]["diagnostic_code"] == "after_close_optional_fields_empty"
    assert "ingest_id" in planned[0]["unknown_fields"]
    migrate_after_close_batch(service.store.path.parent)

    with connect_sqlite(service.store.path, read_only=True) as connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(snapshots)")}
    assert "payload_hash" not in columns
    migrated = service.store.get(snapshot.snapshot_id)
    assert migrated.schema_version == "1.2"
    assert migrated.ingest_id == ""
    assert migrated.artifact_id == ""
    assert migrated.shadow_candidates == ()


def test_after_close_damaged_current_is_not_misclassified_as_old(service) -> None:
    from quantmaster.after_close.migration import inspect_after_close_snapshots
    from quantmaster.runtime.sqlite import connect_sqlite

    snapshot = service.scan()
    payload = snapshot.to_dict()
    payload.pop("validation")
    with connect_sqlite(service.store.path) as connection:
        connection.execute(
            "UPDATE snapshots SET payload_json=? WHERE snapshot_id=?",
            (json.dumps(payload, ensure_ascii=False), snapshot.snapshot_id),
        )
    planned = inspect_after_close_snapshots(service.store.path.parent)
    assert planned[0]["diagnostic_code"] == "after_close_schema_invalid"
    assert planned[0]["outcome"] == "review"


def test_after_close_submit_uses_versioned_singleflight_key(monkeypatch) -> None:
    from quantmaster.after_close.jobs import TASK_TYPE, AfterCloseJobs

    class _Runtime:
        def __init__(self) -> None:
            self.store = self
            self.items = [
                {
                    "id": "old-failure",
                    "type": TASK_TYPE,
                    "status": "failed",
                    "spec": {"as_of": "", "force": False},
                }
            ]
            self.submissions = []

        def register(self, *_args, **_kwargs) -> None:
            return None

        def submit(self, job_type, spec, **options):
            for item in self.items:
                if (
                    item["type"] == job_type
                    and item["status"] in {"queued", "running", "cancelling", "interrupted"}
                    and item["spec"] == spec
                    and item.get("input_fingerprint") == options.get("input_fingerprint")
                    and item.get("algorithm_version") == options.get("algorithm_version")
                ):
                    return item, False
            self.submissions.append((job_type, spec, options))
            created = {
                "id": f"new-{len(self.submissions)}",
                "type": job_type,
                "status": "queued",
                "spec": spec,
                "input_fingerprint": options.get("input_fingerprint"),
                "algorithm_version": options.get("algorithm_version"),
            }
            self.items.insert(0, created)
            return created, True

    runtime = _Runtime()
    jobs = AfterCloseJobs(runtime)  # type: ignore[arg-type]
    monkeypatch.setattr(
        AfterCloseJobs,
        "input_fingerprint",
        staticmethod(lambda **_kwargs: ("after-close-generation-v1", "score-v1")),
    )

    replacement, created = jobs.submit()
    assert created is True
    assert replacement["id"] == "new-1"
    assert runtime.submissions[0][2]["input_fingerprint"] == "after-close-generation-v1"
    assert runtime.submissions[0][2]["algorithm_version"] == "score-v1"
    assert runtime.submissions[0][2]["max_attempts"] == 2

    duplicate, duplicate_created = jobs.submit()
    assert duplicate_created is False
    assert duplicate["id"] == replacement["id"]
    assert len(runtime.submissions) == 1
