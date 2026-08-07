from __future__ import annotations

import pytest
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
    assert temperature.json()["meta"]["algorithm_version"] == "QM_ROTATION_V6"

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
    assert 'href="/static/rotation.css?rev=' in page.text
    assert "%%QM_ROTATION_CSS_REV%%" not in page.text
    rotation_css = client.get("/static/rotation.css")
    assert rotation_css.headers["cache-control"] == "no-cache, max-age=0, must-revalidate"
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


def test_group_apis_materialize_selected_window_scores_and_grade_filters():
    service = get_rotation_service()

    def group_item(code: str, name: str, *, level: str) -> dict:
        scores = {
            str(window): {
                "window": window,
                "score": 72.0 if window == 1 else 38.0,
                "grade": "A" if window == 1 else "D",
                "available_weight": 100,
                "minimum_weight": 60,
                "items": [{
                    "id": "trend", "label": "趋势向上", "score": 50.0,
                    "weight": 40, "note": "测试证据", "available": True,
                }],
            }
            for window in (1, 3, 5, 20)
        }
        return {
            "code": code, "name": name, "level": level, "stage": "unclear",
            "scores": scores, "score": scores["5"], "rotation_score": 38.0,
            "grade": "D", "signals": {str(window): {} for window in (1, 3, 5, 20)},
        }

    industry = group_item("801080.SI", "电子", level="L1")
    theme = group_item("BK1001", "机器人", level="concept")
    meta = {
        "snapshot_id": "window-scores", "as_of": "2026-08-07",
        "quality": {"status": "complete", "issues": []},
    }
    service.store.save_snapshots({
        "industries": {
            "meta": meta,
            "data": {"items": [industry], "details": {industry["code"]: industry}},
        },
        "themes": {
            "meta": meta,
            "data": {"items": [theme], "details": {theme["code"]: theme}},
        },
    })
    client = _client()

    one_day = client.get("/api/v1/rotation/industries", params={"window": 1}).json()["data"]
    five_day = client.get("/api/v1/rotation/industries", params={"window": 5}).json()["data"]
    assert one_day["window"] == 1
    assert one_day["items"][0]["rotation_score"] == 72.0
    assert one_day["items"][0]["score"]["grade"] == "A"
    assert one_day["items"][0]["score_available_weight"] == 100
    assert "scores" not in one_day["items"][0]
    assert five_day["items"][0]["rotation_score"] == 38.0
    assert five_day["items"][0]["grade"] == "D"

    detail = client.get(
        "/api/v1/rotation/industries/801080.SI", params={"window": 1},
    ).json()["data"]
    assert detail["score"]["window"] == 1
    assert detail["score"]["items"][0]["id"] == "trend"

    themes_one = client.get(
        "/api/v1/rotation/themes",
        params={"window": 1, "grade": "A", "page": 1, "page_size": 25},
    ).json()["data"]
    themes_five = client.get(
        "/api/v1/rotation/themes",
        params={"window": 5, "grade": "A", "page": 1, "page_size": 25},
    ).json()["data"]
    assert themes_one["pagination"]["total"] == 1
    assert themes_one["items"][0]["rotation_score"] == 72.0
    assert themes_five["pagination"]["total"] == 0
    assert client.get("/api/v1/rotation/industries", params={"window": 2}).status_code == 422
    assert client.get("/api/v1/rotation/themes/BK1001", params={"window": 20}).status_code == 200


def test_rotation_theme_pagination_is_complete_stable_and_keeps_legacy_limit():
    service = get_rotation_service()
    items = [
        {
            "code": f"BK{index:04d}", "name": f"题材 {index:04d}", "stage": "repair_spread",
            "grade": "A" if index % 2 else "B", "rotation_score": 50.0,
            "coverage": 1.0, "primary_industry": {"name": "电子"},
            "signals": {
                str(window): {
                    "rotation_change_pp": float(index if window in {1, 5} else -index),
                    "excess_return": 0.01,
                    "amount_activity": 0.02,
                }
                for window in (1, 3, 5, 20)
            },
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
    for window, expected_code in ((1, "BK0697"), (3, "BK0000"), (5, "BK0697"), (20, "BK0000")):
        response = client.get(
            "/api/v1/rotation/themes",
            params={
                "page": 1, "page_size": 25, "window": window,
                "sort": "change", "order": "desc",
            },
        )
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["pagination"]["total"] == 698
        assert data["items"][0]["code"] == expected_code
    all_codes = []
    for page in range(1, 29):
        data = client.get("/api/v1/rotation/themes", params={"page": page, "page_size": 25}).json()["data"]
        all_codes.extend(item["code"] for item in data["items"])
    assert len(all_codes) == len(set(all_codes)) == 698
    assert set(all_codes) == {item["code"] for item in items}


def test_rotation_theme_focus_queue_is_auditable_and_ignores_catalog_filters():
    service = get_rotation_service()

    def theme(
        code: str,
        name: str,
        *,
        score: float,
        grade: str,
        change: float,
        excess: float,
        breadth: float,
        amount: float,
    ) -> dict:
        return {
            "code": code,
            "name": name,
            "stage": "repair_spread" if change > 0 else "retreat_watch",
            "stage_label": "修复扩散" if change > 0 else "退潮观察",
            "rotation_score": score,
            "grade": grade,
            "coverage": 0.9,
            "signals": {
                str(window): {
                    "rotation_change_pp": change,
                    "excess_return": excess,
                    "advance_ratio": breadth,
                    "amount_activity": amount,
                }
                for window in (1, 3, 5, 20)
            },
        }

    items = [
        theme("BK1", "机器人", score=82, grade="A", change=8, excess=.02, breadth=.7, amount=.1),
        theme("BK2", "光模块", score=76, grade="B", change=6, excess=.01, breadth=.6, amount=.08),
        theme("BK3", "高分题材", score=95, grade="C", change=5, excess=.03, breadth=.8, amount=.2),
        theme("BK4", "宽度题材", score=66, grade="C", change=3, excess=.01, breadth=.55, amount=-.1),
        theme("BK5", "弱证据题材", score=64, grade="B", change=-2, excess=-.01, breadth=.4, amount=-.1),
    ]
    service.store.save_snapshots({
        "themes": {
            "meta": {"snapshot_id": "focus-themes", "quality": {"status": "complete", "issues": []}},
            "data": {"items": items, "summary": {"group_count": len(items)}},
        },
    })

    data = _client().get(
        "/api/v1/rotation/themes",
        params={"page": 1, "page_size": 25, "query": "没有匹配"},
    ).json()["data"]

    assert data["pagination"]["total"] == 0
    assert data["items"] == []
    assert [item["code"] for item in data["focus_items"]] == ["BK1", "BK2", "BK3", "BK4"]
    assert data["focus_items"][0]["focus"] == {
        "evidence_count": 5,
        "evidence_total": 5,
        "reasons": [
            {"id": "rotation", "label": "轮动改善"},
            {"id": "excess", "label": "相对收益为正"},
            {"id": "breadth", "label": "上涨宽度过半"},
            {"id": "amount", "label": "量能活跃"},
            {"id": "grade", "label": "周期结构 A/B"},
        ],
    }
    assert data["focus_definition"] == {
        "criteria": [
            {"id": "rotation", "label": "轮动改善"},
            {"id": "excess", "label": "相对收益为正"},
            {"id": "breadth", "label": "上涨宽度过半"},
            {"id": "amount", "label": "量能活跃"},
            {"id": "grade", "label": "周期结构 A/B"},
        ],
        "limit": 4,
        "window": 5,
    }


def test_rotation_etf_summary_and_paginated_items_stay_consistent():
    service = get_rotation_service()
    items = [
        {
            "symbol": f"51{index:04d}.SH", "name": f"宽基 ETF {index}", "category": "宽基",
            "benchmark": "沪深300", "flow": float(index - 2), "flow_streak_sessions": index - 2,
            "flows": {
                str(window): float((index - 2) * (1 if window in {1, 5} else -1))
                for window in (1, 3, 5, 20)
            },
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
    for window, expected_symbol in (
        (1, "510004.SH"), (3, "510000.SH"),
        (5, "510004.SH"), (20, "510000.SH"),
    ):
        response = client.get(
            "/api/v1/rotation/etf-flows/items",
            params={
                "page": 1, "page_size": 25, "window": window,
                "sort": "flow", "order": "desc",
            },
        )
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["pagination"]["total"] == summary["item_total"]
        assert data["items"][0]["symbol"] == expected_symbol


@pytest.mark.parametrize(
    "path",
    ("/api/v1/rotation/themes", "/api/v1/rotation/etf-flows/items"),
)
@pytest.mark.parametrize("window", (2, "invalid"))
def test_rotation_paginated_windows_reject_unsupported_values(path, window):
    response = _client().get(path, params={"page": 1, "page_size": 25, "window": window})

    assert response.status_code == 422
