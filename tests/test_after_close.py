from __future__ import annotations

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
        store=AfterCloseStore(tmp_path / "after-close.sqlite"),
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


def test_historical_force_rejects_current_board_taxonomy(service) -> None:
    with pytest.raises(DataGateRejected, match="当前分类"):
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
    monkeypatch.setattr(routes, "get_after_close_service", lambda: service)
    client = TestClient(app)

    latest = client.get("/api/v1/after-close/snapshots/latest")
    exported = client.get(f"/api/v1/after-close/export/{snapshot.snapshot_id}?format=csv")
    page = client.get("/")

    assert latest.status_code == 200
    assert latest.json()["snapshot"]["snapshot_id"] == snapshot.snapshot_id
    assert exported.status_code == 200
    assert "600001.SH" in exported.content.decode("utf-8-sig")
    assert 'id="tab-after-close"' in page.text
    assert "/static/after-close.js?rev=" in page.text
    assert "Tushare 和其他页面的行情缓存不参与本扫描" in page.text
    assert "data-after-close-open-stockdb" in page.text
    assert 'id="after-close-update-data"' in page.text
    assert "更新扫描数据" in page.text
    assert "按当前数据重新计算" in page.text
    assert "强制重跑" not in page.text

    script = client.get("/static/after-close.js")
    assert script.status_code == 200
    assert "盘后扫描专用的 free-stockdb" in script.text
    assert "QuantMasterManagement?.open('local-data')" in script.text
    assert "/api/v1/settings/free-stockdb/update" in script.text
    assert "扫描数据已更新至" in script.text


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


def test_after_close_submit_only_coalesces_active_jobs() -> None:
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

        def register(self, *_args) -> None:
            return None

        def list(self, _limit, *, job_type=""):
            return [item for item in self.items if item["type"] == job_type]

        def submit(self, job_type, spec, **options):
            self.submissions.append((job_type, spec, options))
            created = {
                "id": f"new-{len(self.submissions)}",
                "type": job_type,
                "status": "queued",
                "spec": spec,
            }
            self.items.insert(0, created)
            return created, True

    runtime = _Runtime()
    jobs = AfterCloseJobs(runtime)  # type: ignore[arg-type]

    replacement, created = jobs.submit()
    assert created is True
    assert replacement["id"] == "new-1"
    assert runtime.submissions[0][2]["idempotency_key"] == ""

    duplicate, duplicate_created = jobs.submit()
    assert duplicate_created is False
    assert duplicate["id"] == replacement["id"]
    assert len(runtime.submissions) == 1
