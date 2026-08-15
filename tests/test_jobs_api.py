"""Unified v1 job API contracts."""

from fastapi.testclient import TestClient

from quantmaster.backtest.jobs import get_backtest_job_manager
from quantmaster.backtest.spec import BacktestSpec
from quantmaster.data.repair import get_data_repair_manager
from quantmaster.server.app import app


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
