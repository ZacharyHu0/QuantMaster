"""Unified v1 job API contracts."""

from fastapi.testclient import TestClient

from quantmaster.backtest.jobs import get_backtest_job_manager
from quantmaster.backtest.spec import BacktestSpec
from quantmaster.data.repair import get_data_repair_manager
from quantmaster.server.app import app


def test_rotation_refresh_job_exposes_partial_result(monkeypatch):
    from quantmaster.server import jobs

    result = {"outcome": "partial", "as_of": "2026-08-20", "warnings": ["份额尚未推进"]}
    monkeypatch.setattr(jobs, "_artifact_payload", lambda _: result)
    public = jobs._public_job("rotation", {
        "id": "rotation-partial", "type": "rotation.refresh", "status": "completed",
        "result_artifact_id": "result-partial",
    })
    assert public["result"] == result
    assert public["outcome"] == "partial"


def _spec(name: str = "统一任务") -> BacktestSpec:
    return BacktestSpec.model_validate({
        "name": name,
        "strategy": {"kind": "factor", "factor": "mom_20d", "top_n": 3},
        "universe": "demo",
        "start": "2023-01-01",
        "end": "2023-12-31",
        "benchmark": None,
        "initial_capital": 100_000,
    })


def test_unified_jobs_lists_gets_cancels_and_retries_backtests(monkeypatch):
    manager = get_backtest_job_manager()
    monkeypatch.setattr(manager, "_owns_runtime", lambda: False)
    monkeypatch.setattr(manager, "start", lambda: None)
    created = manager.enqueue(_spec())
    client = TestClient(app)

    listed = client.get("/api/v1/jobs", params={"domain": "backtests"})
    assert listed.status_code == 200
    item = next(value for value in listed.json()["items"] if value["id"] == created["id"])
    assert item["domain"] == "backtests"
    assert item["can_cancel"] is True
    assert item["links"]["self"] == f"/api/v1/jobs/{created['id']}"

    token = client.get("/api/v1/session").json()["csrf_token"]
    headers = {"X-CSRF-Token": token}
    cancelled = client.post(
        f"/api/v1/jobs/{created['id']}/cancel", headers=headers,
    )
    assert cancelled.status_code == 200
    assert cancelled.json()["status"] == "cancelled"

    retried = client.post(
        f"/api/v1/jobs/{created['id']}/retry", headers=headers,
    )
    assert retried.status_code == 202
    assert retried.json()["id"] == created["id"]
    assert retried.json()["attempt"] == 2
    events = client.get(
        f"/api/v1/jobs/{created['id']}/events",
    ).json()["items"]
    assert any(event["type"] == "job_retried" for event in events)


def test_unified_jobs_exposes_repair_events_cancel_and_retry():
    manager = get_data_repair_manager()
    created = manager.enqueue(
        "bar", "bars::600000.SH", reason="hash mismatch",
        spec={"root": "bars", "symbol": "600000.SH"}, source="market",
    )
    client = TestClient(app)

    listed = client.get("/api/v1/jobs", params={"domain": "repairs"})
    assert listed.status_code == 200
    assert next(item for item in listed.json()["items"] if item["id"] == created["id"])[
        "can_cancel"
    ]
    token = client.get("/api/v1/session").json()["csrf_token"]
    headers = {"X-CSRF-Token": token}
    cancelled = client.post(
        f"/api/v1/jobs/{created['id']}/cancel", headers=headers,
    )
    assert cancelled.status_code == 200
    assert cancelled.json()["status"] == "cancelled"
    retried = client.post(
        f"/api/v1/jobs/{created['id']}/retry", headers=headers,
    )
    assert retried.status_code == 202
    assert retried.json()["id"] == created["id"]
    events = client.get(
        f"/api/v1/jobs/{created['id']}/events",
    ).json()["items"]
    assert [item["type"] for item in events] == [
        "job_queued", "data_repair_evidence", "job_cancel_requested", "job_retried",
    ]


def test_refresh_routes_preserve_durable_planning_through_cancel_retry_and_completion(
    isolated_config, monkeypatch,
):
    isolated_config.data.free_stockdb_managed = False
    import threading

    from quantmaster.data.maintenance import DataRefreshManager

    manager = DataRefreshManager()
    manager.initialize()
    reader = DataRefreshManager()
    monkeypatch.setattr(manager, "_start", lambda _: None)
    monkeypatch.setattr("quantmaster.server.jobs.data_refresh_manager", reader)
    planning, resolve, syncing, finish = (threading.Event() for _ in range(4))

    def resolve_symbols(*_args):
        planning.set()
        assert resolve.wait(10)
        return ["600000.SH", "000001.SZ"]

    def refresh_one(*_args):
        syncing.set()
        assert finish.wait(10)
        return None

    def worker_command(operation, payload, **_kwargs):
        if operation == "data.refresh.create":
            return manager.create(payload["scope"], payload["universe"], payload["start"])
        if operation == "data.refresh.cancel":
            return manager.cancel(payload["job_id"])
        if operation == "data.refresh.retry":
            return manager.resume(payload["job_id"])
        raise AssertionError(operation)

    monkeypatch.setattr(manager, "_resolve_symbols", resolve_symbols)
    monkeypatch.setattr(manager, "_refresh_one", refresh_one)
    monkeypatch.setattr(manager, "_publish_market_snapshot", lambda: None)
    monkeypatch.setattr("quantmaster.runtime.worker_ipc.call_worker_command", worker_command)
    monkeypatch.setattr(
        "quantmaster.runtime.worker.runtime_worker_status",
        lambda: {"available": True, "status": "running", "age_seconds": 0.0},
    )
    client = TestClient(app)
    headers = {"X-CSRF-Token": client.get("/api/v1/session").json()["csrf_token"]}

    def assert_state(value, status, known, is_planning, total):
        assert value["status"] == status
        assert value["total_known"] is known
        assert value["planning"] is is_planning
        assert value["total"] == total

    def poll(status, known, is_planning, total):
        detail = client.get(f"/api/v1/jobs/{job_id}")
        assert detail.status_code == 200
        listed = client.get("/api/v1/jobs", params={"domain": "data"})
        assert listed.status_code == 200
        item = next(item for item in listed.json()["items"] if item["id"] == job_id)
        for value in (detail.json(), item, reader.get(job_id)):
            assert_state(value, status, known, is_planning, total)

    try:
        created = client.post(
            "/api/v1/data/refresh", json={"scope": "universe", "universe": "csi800"},
            headers=headers,
        )
        assert created.status_code == 202
        job_id = created.json()["id"]
        assert_state(created.json(), "queued", False, True, 0)
        poll("queued", False, True, 0)
        runtime = manager._ensure_runtime()
        runtime.dispatch_job(job_id)
        assert planning.wait(3)
        poll("running", False, True, 0)
        cancelled = client.post(f"/api/v1/jobs/{job_id}/cancel", headers=headers)
        assert cancelled.status_code == 200
        assert_state(cancelled.json(), "cancelling", False, False, 0)
        resolve.set()
        runtime.wait(job_id, timeout=5)
        poll("cancelled", False, False, 0)
        assert not syncing.is_set()

        planning.clear()
        resolve.clear()
        retried = client.post(f"/api/v1/jobs/{job_id}/retry", headers=headers)
        assert retried.status_code == 202
        assert_state(retried.json(), "queued", False, True, 0)
        assert retried.json()["attempt"] == 2
        assert planning.wait(3)
        poll("running", False, True, 0)
        resolve.set()
        assert syncing.wait(3)
        poll("running", True, False, 2)
        finish.set()
        runtime.wait(job_id, timeout=5)
        poll("completed", True, False, 2)
        completed = client.get(f"/api/v1/jobs/{job_id}").json()
        assert completed["next_index"] == completed["succeeded"] == 2
        assert completed["can_cancel"] is False
        assert completed["can_retry"] is False
    finally:
        resolve.set()
        finish.set()
        manager.shutdown()
