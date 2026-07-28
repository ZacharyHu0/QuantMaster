from __future__ import annotations

import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from quantmaster.ai.llm import LLMError
from quantmaster.config import Config, set_config
from quantmaster.factors.mining.llm_miner import LLMFactorMiner
from quantmaster.lab.catalog import curated_catalog
from quantmaster.lab.dataset import (
    build_membership_mask,
    create_snapshot,
    load_csi800_members_as_of,
)
from quantmaster.lab.ml import (
    artifact_sha256,
    engineer_features,
    make_samples,
    predict_panel,
    train,
)
from quantmaster.lab.models import FactorSpec
from quantmaster.lab.service import LabService
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


class _SequenceLLMClient:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = []
        self.config = SimpleNamespace(
            timeout=60, provider="openai-compatible", model="research-model",
        )

    def chat_json(self, prompt, system=None, *, timeout=None):
        self.calls.append(timeout)
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


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


def test_csi800_as_of_uses_latest_known_snapshot_without_lookahead():
    class Source:
        def index_weights(self, index_code, start, end):
            symbol = "600000.SH" if index_code == "000300.SH" else "000001.SZ"
            return pd.DataFrame([
                {"index_code": index_code, "symbol": symbol,
                 "trade_date": "2024-01-31", "weight": 1},
                {"index_code": index_code, "symbol": "999999.SH",
                 "trade_date": "2024-03-01", "weight": 1},
            ])

    result = load_csi800_members_as_of("2024-02-15", source=Source())
    assert result["symbols"] == ["000001.SZ", "600000.SH"]
    assert set(result["snapshot_dates"].values()) == {"2024-01-31"}


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


def test_llm_miner_retries_with_longer_read_windows():
    def retryable():
        return LLMError("模型暂未响应", code="read_timeout", retryable=True)

    client = _SequenceLLMClient([
        retryable(), retryable(), retryable(),
        [{"expression": "rank(close)", "rationale": "价格截面强度"}],
    ])
    events = []
    report = LLMFactorMiner(client=client).mine_report(
        _panel(days=80), rounds=1, retry_backoff=(0, 0, 0), on_event=events.append,
    )

    assert client.calls == [180, 240, 360, 480]
    assert report.attempts == 4
    assert report.rounds_completed == 1
    assert report.warnings == []
    assert any(item["type"] == "llm_retry_scheduled" for item in events)
    assert events[-1]["type"] == "llm_round_completed"


def test_llm_miner_keeps_first_round_when_later_round_exhausts_retries():
    def failure():
        return LLMError("模型服务排队超时", code="read_timeout", retryable=True)

    client = _SequenceLLMClient([
        [{"expression": "rank(close)", "rationale": "价格截面强度"}],
        failure(), failure(), failure(), failure(),
    ])
    report = LLMFactorMiner(client=client).mine_report(
        _panel(days=80), rounds=2, retry_backoff=(0, 0, 0),
    )

    assert report.rounds_completed == 1
    assert report.rounds_requested == 2
    assert report.attempts == 5
    assert report.factors[0].expression == "rank(close)"
    assert report.warnings[0]["code"] == "llm_round_incomplete"


def test_job_partial_completion_and_retry_are_auditable(tmp_path):
    _config(tmp_path)
    store = LabStore(tmp_path / "lab.sqlite")
    queued = store.enqueue("discover_llm", {"universe": "demo", "rounds": 2})
    store.claim_next("test-worker")
    store.update_job(queued["id"], 76, "AI 第 2/2 轮准备重试", "模型响应超时")
    store.finish_job(queued["id"], result={
        "candidates": [{"id": "candidate-v1"}],
        "warnings": [{"code": "llm_round_incomplete", "message": "第 2 轮未完成"}],
    })

    partial = store.job(queued["id"])
    assert partial["status"] == "completed_with_warnings"
    assert partial["progress"] == 100
    assert partial["phase"] == "部分完成"
    assert partial["detail"] == "第 2 轮未完成"

    retried = store.retry_job(queued["id"])
    assert retried["status"] == "queued"
    assert retried["params"] == {"universe": "demo", "rounds": 2}
    assert any(item["type"] == "retried_as" for item in store.events(queued["id"]))
    assert any(item["type"] == "retry_of" for item in store.events(retried["id"]))


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


def test_ridge_artifact_inference_and_integrity_check(tmp_path):
    _config(tmp_path)
    panel = _panel(days=240, symbols=6)
    samples, targets, metadata, names = make_samples(panel, sequence_length=10)
    model_dir = tmp_path / "lab_artifacts" / "test"
    result = train(
        "ridge", samples, targets, metadata, artifact_dir=model_dir,
        config={"alpha": 1.0},
    )
    result.pop("_predicted")
    result.pop("_actual")
    result.pop("_validation_metadata")
    artifact = model_dir / "ridge.npz"
    manifest = {
        "schema_version": 1,
        "kind": "ridge",
        "features": names,
        "sequence_length": 10,
        "minimum_feature_coverage": 0.80,
        "artifact": artifact.relative_to(tmp_path).as_posix(),
        "artifact_sha256": artifact_sha256(artifact),
    }
    manifest_path = model_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    predicted = predict_panel(panel, {
        "manifest": manifest_path.relative_to(tmp_path).as_posix(),
    })
    assert predicted.shape == panel["close"].shape
    assert predicted.iloc[-1].notna().any()

    artifact.write_bytes(artifact.read_bytes() + b"tampered")
    with pytest.raises(ValueError, match="完整性"):
        predict_panel(panel, {"manifest": manifest_path.relative_to(tmp_path).as_posix()})


def test_learned_model_is_shadow_candidate_until_manual_champion_promotion(
    tmp_path, monkeypatch,
):
    _config(tmp_path)
    panel = _panel(days=260, symbols=8)
    store = LabStore(tmp_path / "lab.sqlite")
    service = LabService(store)
    snapshot = store.save_snapshot({
        "snapshot_hash": "training-snapshot",
        "research_quality": "sandbox",
        "universe": "demo",
    })
    monkeypatch.setattr(
        service, "_context",
        lambda universe, start, end, progress=None: (panel, None, snapshot),
    )
    report = {
        "coverage": 0.96,
        "best_horizon": 3,
        "candidate_score": 78.0,
        "max_existing_correlation": 0.31,
        "horizons": {"3": {
            "horizon": 3, "oos_rank_ic": 0.052, "oos_icir": 0.61,
            "q_value": 0.03, "folds": [],
        }},
        "gates": {
            "passed": True, "hard_failures": [], "soft_failures": [],
            "override_allowed": True,
        },
    }
    monkeypatch.setattr(
        "quantmaster.lab.validation.validate_factor_values",
        lambda *args, **kwargs: json.loads(json.dumps(report)),
    )

    result = service.train_model(
        model="ridge", universe="demo", start="2023-01-01", end="2024-01-01",
        horizon=3, sequence_length=10, config={"alpha": 1.0},
    )
    version = store.version(result["version_id"])
    assert version["spec"]["kind"] == "learned"
    assert version["status"] == "candidate"
    assert (tmp_path / result["manifest"]).is_file()
    manifest = json.loads((tmp_path / result["manifest"]).read_text(encoding="utf-8"))
    assert manifest["fit_through"] < manifest["validation_start"]
    assert store.active_deployments() == []

    store.approve(version["id"], actor="tester")
    deployed = store.deploy(
        version["id"], universe="demo", horizon=3, actor="tester",
        profile="stable", scope="exact",
    )
    assert deployed["role"] == "ml"
    from quantmaster.decision import hybrid_daily_selection, resolve_policy

    policy = resolve_policy("demo", 3, "stable", symbols=list(panel["close"]), store=store)
    assert any(item["role"] == "ml" for item in policy["components"])
    selection = hybrid_daily_selection(
        panel, top_n=3, horizon=3, profile="stable", universe="demo",
        policy_snapshot=policy,
    )
    assert selection["model_snapshot"]["effective_weights"]["ml"] <= 0.15
    assert selection["shadow_model"] is None


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
        from quantmaster.server import lab as lab_api

        lab_api.get_lab_service().store.finish_job(
            queued.json()["id"], error="测试失败",
        )
        retried = client.post(f"/api/lab/jobs/{queued.json()['id']}/retry")
        assert retried.status_code == 202
        assert retried.json()["status"] == "queued"
        events = client.get(
            f"/api/lab/jobs/{retried.json()['id']}/events?after=0",
        ).json()["items"]
        assert any(item["type"] == "retry_of" for item in events)


def test_lab_ui_alignment_dialog_and_ml_setup_contract(tmp_path):
    _config(tmp_path, enabled=False)
    from quantmaster.server.app import app

    with TestClient(app) as client:
        page = client.get("/").text
        styles = client.get("/static/lab.css").text
        script = client.get("/static/lab.js").text

    assert '<span class="nav-lab-label">Quant Lab</span>' in page
    assert 'id="lab-ml-setup"' in page
    assert 'id="lab-job-drawer"' in page
    assert 'aria-labelledby="lab-job-drawer-title"' in page
    assert 'data-settings-panel="lab"' in page
    assert 'name="lab.horizons" data-list-checkbox' in page
    assert "#nav .nav-lab-label" in styles
    assert "margin:auto" in styles
    assert "--dialog-x" in styles
    assert "setupDraggableDialog" in script
    assert "refreshJobDetail" in script
    assert "data-retry-job" in script
    assert "completed_with_warnings" in script
    assert "180 / 240 / 360 / 480 秒" in script
    assert ".lab-job-drawer.is-open" in styles
    assert "clampDialogOffset" in script
    assert 'python -m pip install -e ".[data,ml]"' in script
    assert "qm lab doctor" in script
    assert "qm lab worker" in script
    assert "aria-disabled" in script
    assert "data-deploy-profile" in script
    assert "data-deploy-scope" in script
    assert "产出版本" in script
    assert "quantmaster:settings-applied" in script
