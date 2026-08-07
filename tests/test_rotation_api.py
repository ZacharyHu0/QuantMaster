from __future__ import annotations

from fastapi.testclient import TestClient

from quantmaster.rotation.contracts import RotationJobSpec
from quantmaster.rotation.service import get_rotation_service
from quantmaster.server.app import app


def _client() -> TestClient:
    client = TestClient(app)
    token = client.get("/api/v1/session").json()["csrf_token"]
    client.headers["X-CSRF-Token"] = token
    return client


def test_rotation_cold_state_and_static_taxonomy_are_explicit():
    client = _client()
    temperature = client.get("/api/v1/market/temperature")
    assert temperature.status_code == 200
    assert temperature.json()["meta"]["quality"]["status"] == "cold"
    assert temperature.json()["meta"]["quality"]["coverage"] is None
    assert temperature.json()["meta"]["algorithm_version"] == "QM_ROTATION_V3"

    overview = client.get("/api/v1/rotation/overview").json()
    assert overview["meta"]["quality"]["coverage"] is None
    assert overview["meta"]["quality"]["available_dimensions"] == 0
    assert overview["meta"]["quality"]["total_dimensions"] == 4
    assert overview["data"]["windows"] == [1, 3, 5, 20]
    assert set(overview["data"]["dimensions"]) == {
        "market", "industries", "themes", "etf",
    }

    taxonomy = client.get("/api/v1/rotation/taxonomy/industries")
    assert taxonomy.status_code == 200
    assert len(taxonomy.json()["data"]["l1"]) == 31
    assert taxonomy.json()["data"]["version"] == "SW2021"

    page = client.get("/")
    assert page.status_code == 200
    assert 'href="/static/rotation.css"' in page.text
    assert 'data-tab="rotation"' in page.text
    assert 'id="market-temperature-view"' in page.text
    assert 'id="market-style-view"' in page.text
    assert 'id="tab-rotation"' in page.text
    assert 'data-rotation-page="overview"' in page.text
    assert 'id="rotation-overview-view"' in page.text
    assert 'data-rotation-page="themes"' in page.text
    assert 'id="rotation-radar-view"' not in page.text
    assert 'src="/static/rotation.js"' in page.text


def test_manual_rotation_refresh_allows_one_tushare_recovery_probe(monkeypatch):
    from quantmaster.rotation.service import get_rotation_worker

    client = _client()
    calls = []
    worker = get_rotation_worker()
    monkeypatch.setattr(
        "quantmaster.data.resilience.PROVIDER_HEALTH.reset",
        lambda lane: calls.append(lane) or {},
    )
    monkeypatch.setattr(worker, "start", lambda: None)
    monkeypatch.setattr(
        worker, "submit",
        lambda spec: {
            "id": "rotation-probe", "status": "queued", "progress": 0,
            "spec": spec.model_dump(mode="json"), "attempt": 1,
        },
    )
    response = client.post(
        "/api/v1/market/analytics/refresh",
        json={"scope": "market", "mode": "incremental", "source": "auto"},
    )

    assert response.status_code == 202
    assert calls == ["tushare"]


def test_rotation_preferences_validate_known_l2_codes():
    service = get_rotation_service()
    service.store.replace_taxonomy_nodes([
        {
            "code": "801081.SI", "name": "半导体", "level": "L2",
            "parent_code": "801080.SI", "members": [],
        }
    ])
    service.store.save_snapshots({
        "industries": {
            "meta": {
                "snapshot_id": "industry-sample", "as_of": "2026-07-30",
                "generated_at": "2026-07-30T10:00:00+00:00",
                "quality": {"status": "complete", "issues": []},
            },
            "data": {"items": [
                {"code": "801080.SI", "name": "电子", "level": "L1"},
                {"code": "801081.SI", "name": "半导体", "level": "L2"},
            ]},
        }
    })
    client = _client()

    before = client.get("/api/v1/rotation/industries").json()["data"]["items"]
    assert [item["code"] for item in before] == ["801080.SI"]

    saved = client.put(
        "/api/v1/rotation/preferences",
        json={"l2_codes": ["801081.SI"], "theme_limit": 20},
    )
    assert saved.status_code == 200
    assert saved.json()["data"]["l2_codes"] == ["801081.SI"]
    after = client.get("/api/v1/rotation/industries").json()["data"]["items"]
    assert [item["code"] for item in after] == ["801080.SI", "801081.SI"]
    unknown = client.put(
        "/api/v1/rotation/preferences",
        json={"l2_codes": ["999999.SI"], "theme_limit": 16},
    )
    assert unknown.status_code == 422


def test_rotation_refresh_returns_unified_job_contract(monkeypatch):
    service = get_rotation_service()
    created = service.jobs.create(RotationJobSpec().model_dump(mode="json"))
    scopes = []

    class Worker:
        def start(self):
            return None

        def submit(self, spec):
            scopes.append(spec.scope)
            return created

    monkeypatch.setattr("quantmaster.server.rotation.get_rotation_worker", lambda: Worker())
    response = _client().post(
        "/api/v1/market/analytics/refresh",
        json={"scope": "all", "mode": "incremental", "source": "local"},
    )
    assert response.status_code == 202
    assert response.json()["domain"] == "rotation"
    assert response.json()["links"]["self"].startswith("/api/v1/jobs/rotation/")

    close = _client().post(
        "/api/v1/market/analytics/refresh",
        json={"scope": "close", "mode": "incremental", "source": "local"},
    )
    assert close.status_code == 202
    assert scopes == ["all", "close"]


def test_unified_jobs_support_rotation_cancel_and_retry(monkeypatch):
    service = get_rotation_service()
    created = service.jobs.create(RotationJobSpec(scope="etf").model_dump(mode="json"))
    client = _client()

    listed = client.get("/api/v1/jobs", params={"domain": "rotation"})
    assert listed.status_code == 200
    assert listed.json()["domains"][-1] == "rotation"
    assert listed.json()["items"][0]["id"] == created["id"]

    cancelled = client.post(f"/api/v1/jobs/rotation/{created['id']}/cancel")
    assert cancelled.status_code == 200
    assert cancelled.json()["status"] == "cancelled"

    class Worker:
        def start(self):
            return None

    monkeypatch.setattr("quantmaster.server.rotation.get_rotation_worker", lambda: Worker())
    retried = client.post(f"/api/v1/jobs/rotation/{created['id']}/retry")
    assert retried.status_code == 202
    assert retried.json()["id"] != created["id"]
    events = client.get(
        f"/api/v1/jobs/rotation/{created['id']}/events",
    ).json()["items"]
    assert any(event["type"] == "retried_as" for event in events)


def test_rotation_detail_returns_not_found_until_group_passes_quality_gate():
    response = _client().get("/api/v1/rotation/industries/801080.SI")
    assert response.status_code == 404


def test_rotation_theme_pagination_is_complete_stable_and_keeps_legacy_limit():
    service = get_rotation_service()
    items = [
        {
            "code": f"BK{index:04d}", "name": f"题材 {index:04d}", "stage": "repair_spread",
            "grade": "A" if index % 2 else "B", "rotation_score": 50.0,
            "coverage": 1.0, "primary_industry": {"name": "电子"},
            "signals": {"5": {"rotation_change_pp": 3.0, "excess_return": 0.01, "amount_activity": 0.02}},
        }
        for index in range(698)
    ]
    service.store.save_snapshots({
        "themes": {
            "meta": {"snapshot_id": "themes-698", "quality": {"status": "complete", "issues": []}},
            "data": {"items": items, "summary": {}},
        },
    })
    client = _client()

    legacy = client.get("/api/v1/rotation/themes", params={"limit": 7}).json()["data"]
    assert legacy["limit"] == 7
    assert len(legacy["items"]) == 7
    assert "pagination" not in legacy

    first = client.get(
        "/api/v1/rotation/themes", params={"page": 1, "page_size": 25, "limit": 2},
    ).json()["data"]
    assert first["pagination"] == {
        "page": 1, "page_size": 25, "total": 698, "pages": 28,
        "has_previous": False, "has_next": True,
    }
    again = client.get("/api/v1/rotation/themes", params={"page": 1, "page_size": 25}).json()["data"]
    assert [item["code"] for item in first["items"]] == [item["code"] for item in again["items"]]
    all_codes = []
    for page in range(1, 29):
        data = client.get("/api/v1/rotation/themes", params={"page": page, "page_size": 25}).json()["data"]
        all_codes.extend(item["code"] for item in data["items"])
    assert len(all_codes) == len(set(all_codes)) == 698
    assert set(all_codes) == {item["code"] for item in items}


def test_rotation_etf_summary_and_paginated_items_stay_consistent():
    service = get_rotation_service()
    items = [
        {
            "symbol": f"51{index:04d}.SH", "name": f"宽基 ETF {index}", "category": "宽基",
            "benchmark": "沪深300", "flow": float(index - 2), "flow_streak_sessions": index - 2,
            "flows": {"5": float(index - 2)},
        }
        for index in range(5)
    ]
    service.store.save_snapshots({
        "etf_flows": {
            "meta": {"snapshot_id": "etf-page", "quality": {"status": "complete", "issues": []}},
            "data": {
                "items": items, "summary": {"windows": {"5": {"net_flow": 0.0}}},
                "daily": [], "benchmarks": [],
            },
        },
    })
    client = _client()
    summary = client.get("/api/v1/rotation/etf-flows", params={"include_items": "false"}).json()["data"]
    assert "items" not in summary
    assert summary["item_total"] == 5
    paged = client.get("/api/v1/rotation/etf-flows/items", params={"page": 1, "page_size": 25}).json()["data"]
    assert paged["pagination"]["total"] == summary["item_total"]
    assert paged["categories"] == ["宽基"]
    assert [item["symbol"] for item in paged["items"]] == [item["symbol"] for item in reversed(items)]
