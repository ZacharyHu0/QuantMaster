from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from quantmaster.rotation.contracts import RotationJobSpec
from quantmaster.rotation.service import RotationService, RotationWorker
from quantmaster.rotation.store import (
    RotationIntegrityError,
    RotationJobStore,
    RotationStore,
)
from quantmaster.runtime.sqlite import connect_sqlite


def _market(days: int = 100, symbols: int = 40):
    dates = pd.bdate_range("2025-03-03", periods=days)
    rng = np.random.default_rng(11)
    returns = rng.normal(0.0004, 0.012, (days, symbols))
    close = pd.DataFrame(
        30 * np.exp(np.cumsum(returns, axis=0)),
        index=dates,
        columns=[f"{600000 + index:06d}.SH" for index in range(symbols)],
    )
    return close, close * 800_000


def test_rotation_store_round_trips_snapshots_preferences_and_auxiliary_data(tmp_path):
    store = RotationStore(tmp_path / "rotation")
    payload = {
        "meta": {
            "snapshot_id": "sample", "as_of": "2026-07-30",
            "generated_at": "2026-07-30T10:00:00+00:00",
        },
        "data": {"current": {"temperature": 42.5}},
    }
    store.save_snapshots({"temperature": payload})

    assert store.snapshot("temperature") == payload
    assert store.snapshots()[0]["snapshot_id"] == "sample"
    saved = store.save_preferences({"l2_codes": ["801081.SI", "801081.SI"], "theme_limit": 20})
    assert saved["l2_codes"] == ["801081.SI"]
    assert store.preferences()["theme_limit"] == 20

    store.replace_taxonomy_nodes([
        {"code": "801081.SI", "name": "半导体", "level": "L2", "parent_code": "801080.SI"}
    ])
    assert store.taxonomy_nodes("L2")[0]["name"] == "半导体"
    store.replace_themes([
        {"code": "BK1001", "name": "机器人", "members": ["600000.SH"]}
    ])
    assert store.themes()[0]["code"] == "BK1001"
    store.set_runtime_state("scheduled_close", "2026-07-30")
    assert store.runtime_state("scheduled_close") == "2026-07-30"


def test_rotation_overview_cache_invalidates_on_snapshot_id(tmp_path, monkeypatch):
    store = RotationStore(tmp_path / "rotation")
    service = RotationService(store, RotationJobStore(tmp_path / "jobs.sqlite"))

    def payload(kind, snapshot_id):
        data = {"as_of": "2026-08-04", "items": []}
        if kind == "temperature":
            data.update({"current": {}, "history": []})
        elif kind == "structure":
            data["current"] = {}
        elif kind == "etf_flows":
            data.update({"summary": {}, "benchmarks": []})
        return {
            "meta": {
                "snapshot_id": snapshot_id, "as_of": "2026-08-04",
                "generated_at": "2026-08-04T10:00:00+00:00",
                "algorithm_version": "QM_ROTATION_V2", "sources": ["unit"],
                "quality": {"status": "complete", "issues": []},
            },
            "data": data,
        }

    store.save_snapshots({kind: payload(kind, "one") for kind in (
        "temperature", "structure", "industries", "themes", "etf_flows",
    )})
    from quantmaster.rotation import service as service_module

    calls = []
    original = service_module._rank_window
    monkeypatch.setattr(
        service_module, "_rank_window",
        lambda *args, **kwargs: calls.append(1) or original(*args, **kwargs),
    )
    service.overview()
    first_count = len(calls)
    assert service.snapshot("themes")["meta"]["quality"]["upgrade_pending"] is True
    service.overview()
    assert len(calls) == first_count
    store.save_snapshots({"themes": payload("themes", "two")})
    service.overview()
    assert len(calls) > first_count


def test_theme_staging_keeps_old_catalog_until_atomic_quality_commit(tmp_path):
    store = RotationStore(tmp_path / "rotation")
    old = {
        "code": "OLD", "name": "旧目录", "members": ["600000.SH"],
        "source": "eastmoney-concept",
    }
    store.replace_themes([old])
    staging = store.begin_theme_sync("ths:concept", "directory-hash", 1)
    assert staging["attempted_count"] == 0
    fresh = {
        "code": "301558", "name": "新目录", "members": ["920130.BJ"],
        "source": "ths:concept",
    }
    store.save_theme_sync_item(
        staging["run_id"], fresh["code"], fresh["name"], payload=fresh, pages=2,
    )

    assert store.themes() == [old]

    store.commit_theme_sync(staging["run_id"], [fresh], [])
    assert store.themes() == [fresh]
    resumed = store.begin_theme_sync("ths:concept", "directory-hash", 1)
    assert resumed["attempted_count"] == 1


def test_rotation_charts_use_adaptive_axes_and_two_axis_zoom():
    script = (
        Path(__file__).parents[1] / "quantmaster" / "server" / "static" / "rotation.js"
    ).read_text(encoding="utf-8")

    assert "Math.ceil(maximum * 1.12 / 5) * 5" in script
    assert "Math.max(40" not in script
    assert "Math.max(60" not in script
    assert script.count('id="rotation-industry-scatter"') == 1
    assert "rotation-radar-scatter" not in script
    assert "周期坐标与 ${activeWindow} 日轨迹" in script
    assert "dataZoom:chartZoom(history.length)" in script
    assert "dataZoom:chartZoom(items.length)" in script
    assert "dataZoom:chartZoom(daily.length,{yAxisIndex:[0,1],initialPoints:260})" in script
    assert "orient:'vertical',yAxisIndex" in script


def test_rotation_jobs_keep_specs_immutable_and_recover_only_expired_leases(tmp_path):
    jobs = RotationJobStore(tmp_path / "jobs.sqlite")
    created = jobs.create({"scope": "all", "mode": "incremental", "source": "local"})
    duplicate = jobs.create({"scope": "all", "mode": "incremental", "source": "local"})
    assert duplicate["id"] == created["id"]

    claimed = jobs.claim("worker-one", lease_seconds=5)
    assert claimed and claimed["status"] == "running"
    assert jobs.claim("worker-two") is None
    jobs.progress(claimed["id"], "worker-one", 50, "计算中", "一半")
    jobs.complete(claimed["id"], "worker-one", {"snapshot_id": "done"})
    completed = jobs.get(claimed["id"])
    assert completed and completed["spec"] == created["spec"]
    assert completed["result"] == {"snapshot_id": "done"}

    retried = jobs.retry(claimed["id"])
    assert retried["id"] != claimed["id"]
    assert retried["spec"] == claimed["spec"]
    assert any(event["type"] == "retry_of" for event in jobs.events(retried["id"]))


def test_rotation_worker_shutdown_releases_lease_without_cancelling_job(
    tmp_path, monkeypatch,
):
    jobs = RotationJobStore(tmp_path / "jobs.sqlite")
    service = RotationService(RotationStore(tmp_path / "rotation"), jobs)
    worker = RotationWorker(service)
    created = jobs.create({"scope": "themes", "mode": "incremental", "source": "auto"})
    claimed = jobs.claim(worker.identity.value)
    assert claimed and claimed["id"] == created["id"]

    def interrupted_build(_spec, *, progress, cancelled):
        del progress
        assert cancelled()
        raise InterruptedError("worker stopping")

    monkeypatch.setattr(service, "build", interrupted_build)
    worker._stop.set()
    worker._run_job(claimed)

    handed_off = jobs.get(created["id"])
    assert handed_off and handed_off["status"] == "running"
    assert handed_off["cancel_requested"] is False
    assert handed_off["lease_expires_at"] == 0
    assert jobs.events(created["id"])[-1]["type"] == "lease_released"
    reclaimed = jobs.claim("worker-next")
    assert reclaimed and reclaimed["id"] == created["id"]
    assert reclaimed["attempt"] == 2


def test_rotation_schedule_marks_success_only_after_fresh_completion(tmp_path):
    store = RotationStore(tmp_path / "rotation")
    service = RotationService(store, RotationJobStore(tmp_path / "jobs.sqlite"))
    worker = RotationWorker(service)
    spec = RotationJobSpec(scope="close", source="auto")
    date_key = str(pd.Timestamp.now(tz="Asia/Shanghai").date())

    worker._record_scheduled_result(spec, succeeded=False)

    retry = store.runtime_state("scheduled_close_retry").split("|")
    assert retry[:2] == [date_key, "1"]
    assert store.runtime_state("scheduled_close") == ""
    assert worker._scheduled_retry_due("close", date_key) is False

    worker._record_scheduled_result(spec, succeeded=True)
    assert store.runtime_state("scheduled_close") == date_key
    assert store.runtime_state("scheduled_close_retry") == ""


def test_rotation_service_builds_coherent_views_from_local_matrices(tmp_path, monkeypatch):
    store = RotationStore(tmp_path / "rotation")
    jobs = RotationJobStore(tmp_path / "rotation-jobs.sqlite")
    service = RotationService(store, jobs)
    close, amount = _market()
    names = {symbol: f"股票{index}" for index, symbol in enumerate(close.columns)}
    from quantmaster.rotation import service as service_module

    trend_calls = []
    real_compute_trend = service_module.compute_trend_matrices

    def counted_trend(values):
        trend_calls.append(len(values))
        return real_compute_trend(values)

    monkeypatch.setattr(service_module, "compute_trend_matrices", counted_trend)

    def load_values(*, progress, cancelled):
        assert not cancelled()
        progress(20, "测试行情", "已准备")
        return close, amount, names, len(close.columns), ["test:local"]

    service.loader.market_matrices = load_values
    industry_names = ("电子", "计算机", "机械设备", "医药生物")
    industry_map = {
        symbol: industry_names[index // 10]
        for index, symbol in enumerate(close.columns)
    }
    monkeypatch.setattr(
        "quantmaster.rotation.service.load_cached_industry_map",
        lambda: industry_map,
    )
    store.replace_themes([
        {
            "code": "BK1001", "name": "主题一",
            "members": list(close.columns[:16]),
        },
        {
            "code": "BK1002", "name": "主题一别名",
            "members": list(close.columns[:15]),
        },
    ])
    etf = pd.DataFrame([
        {"trade_date": "2026-07-29", "symbol": "510300.SH", "shares": 100, "nav": 4.0},
        {"trade_date": "2026-07-30", "symbol": "510300.SH", "shares": 105, "nav": 4.1},
    ])
    store.save_etf_observations(etf)
    updates: list[tuple[int, str]] = []
    result = service.build(
        RotationJobSpec(source="local"),
        progress=lambda value, phase, detail: updates.append((value, phase)),
        cancelled=lambda: False,
    )

    assert result["tracked_count"] == 40
    assert set(result["updated"]) == {
        "etf_flows", "industries", "structure", "taxonomy", "temperature", "themes",
    }
    temperature = service.snapshot("temperature")
    industries = service.snapshot("industries")
    themes = service.snapshot("themes")
    etf_flows = service.snapshot("etf_flows")
    assert temperature["meta"]["batch_id"] == industries["meta"]["batch_id"]
    assert len(industries["data"]["items"]) == 4
    assert len(themes["data"]["items"]) == 1
    assert set(industries["data"]["items"][0]["signals"]) == {"1", "3", "5", "20"}
    assert themes["data"]["items"][0]["primary_industry"] is not None
    assert "tushare:fund_nav" in etf_flows["meta"]["sources"]
    assert etf_flows["meta"]["quality"]["status"] == "complete"
    overview = service.overview()
    assert overview["data"]["temperature"] is not None
    assert overview["data"]["rankings"]["5"]["industries"]["available"] == 4
    assert len(overview["data"]["resonance"]["5"]) == 4
    assert overview["data"]["etf_context"]["summary"]["windows"]["1"]["net_flow"] == 20.5
    assert updates[-1][0] == 96
    assert trend_calls == [len(close)]

    close_result = service.build(
        RotationJobSpec(scope="close", source="local"),
        progress=lambda *_: None,
        cancelled=lambda: False,
    )
    assert close_result["updated"] == []
    assert close_result["outcome"] == "unchanged"
    assert trend_calls == [len(close), len(close)]


def test_partial_theme_provider_uses_catalog_denominator_and_deduplicates_issues(
    tmp_path, monkeypatch,
):
    store = RotationStore(tmp_path / "rotation")
    service = RotationService(store, RotationJobStore(tmp_path / "jobs.sqlite"))
    close, amount = _market()
    names = {symbol: f"股票{index}" for index, symbol in enumerate(close.columns)}
    monkeypatch.setattr(
        "quantmaster.rotation.service._expected_market_session",
        lambda: str(close.index[-1].date()),
    )
    service.loader.market_matrices = lambda **_: (
        close, amount, names, len(close.columns), ["test:local"],
    )
    store.replace_themes([{
        "code": "THS_SAMPLE",
        "name": "受限题材",
        "members": list(close.columns[:16]),
        "source": "ths:concept",
    }])

    class PartialProvider:
        def __init__(self, _store):
            pass

        @staticmethod
        def sync_market_history(progress, cancelled, *, rebuild):
            return {"expected_as_of": str(close.index[-1].date()), "issues": []}

        @staticmethod
        def sync_themes(progress, cancelled):
            return {
                "catalog": 100,
                "available": 75,
                "quality_status": "partial",
                "issues": ["受限目录", "受限目录"],
            }

    monkeypatch.setattr("quantmaster.rotation.provider.RotationProvider", PartialProvider)
    service.build(
        RotationJobSpec(scope="themes", source="auto"),
        progress=lambda *_: None,
        cancelled=lambda: False,
    )

    quality = service.snapshot("themes")["meta"]["quality"]
    assert quality["status"] == "partial"
    assert quality["eligible_count"] == 1
    assert quality["expected_count"] == 100
    assert quality["coverage"] == 0.01
    assert quality["issues"].count("受限目录") == 1


def test_rotation_job_cancel_queued_is_terminal(tmp_path):
    jobs = RotationJobStore(tmp_path / "jobs.sqlite")
    created = jobs.create({"scope": "etf", "mode": "incremental", "source": "local"})
    cancelled = jobs.cancel(created["id"])
    assert cancelled["status"] == "cancelled"
    assert jobs.claim("worker") is None
    assert jobs.get(created["id"])["cancel_requested"] is True
    assert jobs.events(created["id"])[-1]["type"] == "cancel_requested"
    assert time.time() >= cancelled["updated_at"]


def test_rotation_overview_reports_dimensions_without_fabricating_zero_coverage(
    tmp_path,
):
    store = RotationStore(tmp_path / "rotation")
    service = RotationService(store, RotationJobStore(tmp_path / "jobs.sqlite"))
    meta = {
        "snapshot_id": "sample",
        "as_of": "2026-07-30",
        "generated_at": "2026-07-30T10:00:00+00:00",
        "sources": ["test:local"],
    }
    store.save_snapshots({
        "temperature": {
            "meta": {**meta, "quality": {"status": "complete", "issues": []}},
            "data": {"current": {"temperature": 42.0}},
        },
        "industries": {
            "meta": {**meta, "quality": {"status": "partial", "issues": []}},
            "data": {"items": []},
        },
        "etf_flows": {
            "meta": {**meta, "quality": {"status": "complete", "issues": []}},
            "data": {"summary": {"status": "ready"}, "items": []},
        },
    })

    quality = service.overview()["meta"]["quality"]

    assert quality["status"] == "partial"
    assert quality["coverage"] is None
    assert quality["eligible_count"] is None
    assert quality["expected_count"] is None
    assert quality["available_dimensions"] == 3
    assert quality["total_dimensions"] == 4
    assert quality["dimension_statuses"]["细分题材"] == "cold"
    assert "3/4 个维度可用" in quality["issues"][0]


def test_rotation_snapshot_hash_failure_is_exposed_as_corrupt(tmp_path):
    store = RotationStore(tmp_path / "rotation")
    store.save_snapshots({
        "temperature": {
            "meta": {"snapshot_id": "ok", "as_of": "2026-07-30", "generated_at": "now"},
            "data": {"current": {"temperature": 30}},
        }
    })
    with connect_sqlite(store.cache_path, policy="cache") as connection:
        connection.execute(
            "UPDATE snapshots SET payload_json=? WHERE kind='temperature'",
            ('{"data":{"current":{"temperature":99}}}',),
        )

    with pytest.raises(RotationIntegrityError, match="哈希不匹配"):
        store.snapshot("temperature")
    public = RotationService(store, RotationJobStore(tmp_path / "jobs.sqlite")).snapshot(
        "temperature"
    )
    assert public["meta"]["quality"]["status"] == "corrupt"
    assert "损坏内容不会参与计算" in public["meta"]["quality"]["issues"][-1]
    assert "哈希不匹配" not in str(public)


def test_rotation_etf_file_corruption_is_not_treated_as_empty(tmp_path):
    store = RotationStore(tmp_path / "rotation")
    store.etf_path.write_bytes(b"not-a-parquet-file")

    with pytest.raises(RotationIntegrityError, match="ETF 观察文件损坏"):
        store.etf_observations()


def test_rotation_worker_bootstrap_is_explicit_and_close_scoped(tmp_path, monkeypatch):
    service = RotationService(
        RotationStore(tmp_path / "rotation"),
        RotationJobStore(tmp_path / "jobs.sqlite"),
    )

    class Morning:
        @staticmethod
        def now(_timezone):
            return pd.Timestamp("2026-07-30 10:00:00", tz="Asia/Shanghai")

    monkeypatch.setattr("quantmaster.rotation.service.datetime", Morning)
    ordinary = RotationWorker(service)
    monkeypatch.setattr(ordinary, "_run", lambda: ordinary._stop.wait())
    ordinary.start()
    assert service.jobs.list() == []
    ordinary.stop()

    bootstrap = RotationWorker(service)
    monkeypatch.setattr(bootstrap, "_run", lambda: bootstrap._stop.wait())
    bootstrap.start(bootstrap_local=True)
    specs = [item["spec"] for item in service.jobs.list()]
    assert {
        "scope": "close", "mode": "incremental", "source": "local",
    } in specs
    assert {
        "scope": "themes", "mode": "incremental", "source": "auto",
    } in specs
    bootstrap.stop()

