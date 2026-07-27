from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from quantmaster.config import Config, set_config
from quantmaster.lab.catalog import curated_catalog
from quantmaster.lab.dataset import build_membership_mask, create_snapshot
from quantmaster.lab.ml import engineer_features, make_samples, train
from quantmaster.lab.models import FactorSpec
from quantmaster.lab.store import LabStore
from quantmaster.lab.validation import benjamini_hochberg, validate_factor_values


@pytest.fixture(autouse=True)
def reset_config():
    yield
    set_config(None)


def _config(tmp_path, *, enabled=False):
    cfg = Config()
    cfg.data.root = str(tmp_path)
    cfg.lab.enabled = enabled
    set_config(cfg)
    return cfg


def _panel(days=190, symbols=5):
    rng = np.random.default_rng(7)
    index = pd.bdate_range("2023-01-02", periods=days)
    columns = [f"S{number}" for number in range(symbols)]
    close = pd.DataFrame(
        100 * np.exp(np.cumsum(rng.normal(0, 0.012, (days, symbols)), axis=0)),
        index=index,
        columns=columns,
    )
    return {
        "close": close,
        "open": close * (1 + rng.normal(0, 0.002, close.shape)),
        "high": close * 1.012,
        "low": close * 0.988,
        "volume": pd.DataFrame(rng.lognormal(10, 0.8, close.shape), index, columns),
    }


def test_curated_catalog_has_48_unique_specs():
    specs = curated_catalog()
    assert len(specs) == 48
    assert len({spec.slug for spec in specs}) == 48
    assert {"expression"} <= {spec.kind for spec in specs}
    assert any(spec.slug == "news_sentiment" for spec in specs)


def test_point_in_time_membership_updates_indexes_independently():
    records = pd.DataFrame([
        {"trade_date": "2024-01-02", "index_code": "A", "symbol": "AAA"},
        {"trade_date": "2024-01-03", "index_code": "B", "symbol": "BBB"},
        {"trade_date": "2024-01-05", "index_code": "A", "symbol": "CCC"},
    ])
    calendar = pd.bdate_range("2024-01-02", "2024-01-08")
    mask = build_membership_mask(records, calendar)
    assert mask.loc["2024-01-03", ["AAA", "BBB"]].all()
    assert not mask.loc["2024-01-05", "AAA"]
    assert mask.loc["2024-01-05", ["BBB", "CCC"]].all()


def test_tushare_index_weights_are_loaded_month_by_month(tmp_path, monkeypatch):
    _config(tmp_path)
    from quantmaster.data.tushare_source import TushareSource

    source = TushareSource()
    calls = []

    def fake_call(endpoint, ttl, **params):
        calls.append((endpoint, ttl, params))
        if params["index_code"] == "000300.SH":
            return pd.DataFrame()
        return pd.DataFrame([{
            "index_code": params["index_code"], "con_code": "000001.SZ",
            "trade_date": params["start_date"], "weight": 1.0,
        }])

    monkeypatch.setattr(source, "_call", fake_call)
    result = source.index_weights("000300.SH", "2024-01-15", "2024-03-02")
    assert len(calls) == 6
    assert [calls[index][2]["start_date"] for index in (0, 2, 4)] == [
        "20240115", "20240201", "20240301",
    ]
    assert [calls[index][2]["end_date"] for index in (0, 2, 4)] == [
        "20240131", "20240229", "20240302",
    ]
    assert len(result) == 3


def test_snapshot_membership_hash_is_stable_and_serializable(tmp_path):
    _config(tmp_path)
    membership = pd.DataFrame(
        [[True, False], [True, True]],
        index=pd.to_datetime(["2024-01-02", "2024-01-03"]),
        columns=["A", "B"],
    )
    first = create_snapshot("csi800", "2024-01-01", "2024-01-31", membership=membership)
    second = create_snapshot("csi800", "2024-01-01", "2024-01-31", membership=membership)
    assert first.manifest["membership_hash"] == second.manifest["membership_hash"]
    json.dumps(first.to_dict())


def test_store_versions_validation_approval_deployment_and_jobs(tmp_path):
    _config(tmp_path)
    store = LabStore(tmp_path / "lab.sqlite")
    spec = FactorSpec(slug="manual_test", name="测试", expression="rank(close)")
    _factor, version, created = store.create_factor(spec)
    assert created and version["version"] == 1
    with pytest.raises(ValueError, match="统一验证"):
        store.approve(version["id"], actor="tester")
    report = {
        "gates": {"hard_failures": [], "soft_failures": ["IC 偏低"]},
        "candidate_score": 60,
    }
    updated = store.save_validation(version["id"], "dataset", report)
    assert updated["status"] == "candidate"
    with pytest.raises(ValueError, match="研究理由"):
        store.approve(version["id"], actor="tester")
    approved = store.approve(version["id"], actor="tester", reason="保留作正交候选")
    assert approved["status"] == "approved"
    deployed = store.deploy(version["id"], universe="demo", horizon=3, actor="tester")
    assert deployed["version"]["status"] == "production"

    queued = store.enqueue("prepare_data", {"universe": "demo"})
    claimed = store.claim_next("test-worker")
    assert claimed["id"] == queued["id"] and claimed["status"] == "running"
    store.update_job(queued["id"], 50, "处理中")
    store.finish_job(queued["id"], result={"ok": True})
    assert store.job(queued["id"])["status"] == "completed"
    assert len(store.events(queued["id"])) >= 3


def test_feature_engineering_and_ridge_training(tmp_path):
    _config(tmp_path)
    panel = _panel()
    assert len(engineer_features(panel)) == 48
    samples, targets, metadata, names = make_samples(panel, sequence_length=10)
    result = train(
        "ridge", samples, targets, metadata, artifact_dir=tmp_path / "model",
        config={"alpha": 2.0},
    )
    assert len(names) == 48
    assert result["validation_samples"] > 0
    assert (tmp_path / "model" / "ridge.npz").is_file()


def test_validation_report_contains_walk_forward_and_fdr(tmp_path):
    _config(tmp_path)
    panel = _panel(days=620, symbols=25)
    close = panel["close"]
    values = close.pct_change(5, fill_method=None)
    report = validate_factor_values(values, close, name="momentum", research_quality="production")
    assert report["best_horizon"] in {1, 3, 5, 7}
    assert len(report["horizons"]) == 4
    assert all(len(item["folds"]) == 4 for item in report["horizons"].values())
    assert report["gates"]["override_allowed"]
    assert benjamini_hochberg([0.01, 0.04, 0.03]) == pytest.approx([0.03, 0.04, 0.04])


def test_lab_api_catalog_create_and_queue(tmp_path):
    _config(tmp_path, enabled=False)
    from quantmaster.server.app import app

    with TestClient(app) as client:
        overview = client.get("/api/lab/overview")
        assert overview.status_code == 200
        assert overview.json()["capabilities"]["catalog_size"] == 48
        listing = client.get("/api/lab/factors?limit=60").json()
        assert listing["total"] == 48
        created = client.post("/api/lab/factors", json={
            "name": "人工反转", "expression": "rank(-pct_change(close, 5))",
        })
        assert created.status_code == 200
        version_id = created.json()["id"]
        queued = client.post("/api/lab/jobs", json={
            "kind": "validate",
            "params": {"version_id": version_id, "universe": "demo",
                       "start": "2023-01-01", "end": "2024-01-01"},
        })
        assert queued.status_code == 202
        assert queued.json()["status"] == "queued"


def test_lab_ui_alignment_dialog_and_ml_setup_contract(tmp_path):
    _config(tmp_path, enabled=False)
    from quantmaster.server.app import app

    with TestClient(app) as client:
        page = client.get("/").text
        styles = client.get("/static/lab.css").text
        script = client.get("/static/lab.js").text

    assert '<span class="nav-lab-label">Quant Lab</span>' in page
    assert 'id="lab-ml-setup"' in page
    assert "#nav .nav-lab-label" in styles
    assert "margin:auto" in styles
    assert "--dialog-x" in styles
    assert "setupDraggableDialog" in script
    assert "clampDialogOffset" in script
    assert 'python -m pip install -e ".[data,ml]"' in script
    assert "qm lab doctor" in script
    assert "qm lab worker" in script
    assert "aria-disabled" in script
