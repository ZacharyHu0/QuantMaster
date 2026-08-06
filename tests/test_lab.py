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
from quantmaster.factors.mining.python_miner import PythonFactorMiner
from quantmaster.factors.python_artifact import (
    PythonFactorPolicyError,
    RestrictedPythonRunner,
    validate_python_factor,
    write_python_factor_artifact,
)
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
from quantmaster.lab.research import sealed_three_way_split
from quantmaster.lab.robustness import (
    expression_parameter_variants,
    monte_carlo_block_bootstrap,
)
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


def test_factor_registry_enforces_unique_names_and_resolves_runtime_aliases(tmp_path):
    _config(tmp_path)
    store = LabStore(tmp_path / "lab.sqlite")
    first_spec = FactorSpec(
        slug="manual_first", name="人工反转", expression="rank(-close)",
    )
    first_factor, first_version, _created = store.create_factor(first_spec)

    with pytest.raises(ValueError, match=r"名称.*已存在"):
        store.create_factor(FactorSpec(
            slug="manual_second", name=" 人工反转 ", expression="rank(close)",
        ))
    with pytest.raises(ValueError, match="已登记为因子"):
        store.create_factor(FactorSpec(
            slug="manual_first", name="另一个名称", expression="rank(-close)",
        ))
    with pytest.raises(ValueError, match="英文逗号"):
        store.create_factor(FactorSpec(
            slug="manual_comma", name="反转,低波", expression="rank(close)",
        ))

    generated_factor, generated_version, _created = store.create_factor(
        FactorSpec(
            slug="gp_generated", name="人工反转", expression="rank(close)",
        ),
        source="genetic", actor="worker",
    )
    assert generated_factor["name"].startswith("人工反转 · ")
    assert generated_version["spec_json"]["name"] == generated_factor["name"]
    assert store.factor_reference("人工反转")["version_id"] == first_version["id"]
    assert store.factor_reference("manual_first")["name"] == first_factor["name"]
    assert {item["name"] for item in store.runtime_factors()} == {
        "人工反转", generated_factor["name"],
    }

    from quantmaster.factors.fundamental import resolve_factor

    resolved = resolve_factor("人工反转", ["600000.SH"], "2024-01-01", "2024-12-31")
    assert resolved.name == "人工反转"
    assert resolved.expression == "rank(-close)"

    _factor, ai_first, _created = store.create_factor(
        FactorSpec(slug="llm_first", name="AI 候选 1", expression="rank(volume)"),
        source="llm", actor="worker",
    )
    ai_second_factor, ai_second, _created = store.create_factor(
        FactorSpec(slug="llm_second", name="AI 候选 1", expression="rank(amount)"),
        source="llm", actor="worker",
    )
    assert ai_first["spec_json"]["name"] == "AI 候选 1"
    assert ai_second_factor["name"] == "AI 候选 2"
    assert ai_second["spec_json"]["name"] == "AI 候选 2"


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


def test_model_publication_outbox_is_immutable_leased_and_idempotent(
    tmp_path, monkeypatch,
):
    _config(tmp_path)
    store = LabStore(tmp_path / "lab.sqlite")
    service = LabService(store)
    rows = pd.DataFrame({
        "trade_date": pd.to_datetime(["2024-01-02"]),
        "symbol": ["600000.SH"],
        "value": [0.1],
    })
    artifact_dir = tmp_path / "lab_artifacts" / "experiment-1"
    publication = service._stage_model_publication(
        version_id="version-1", experiment_id="experiment-1",
        artifact_dir=artifact_dir, slug="model_one", prediction_rows=rows,
    )
    repeated = store.enqueue_publication(
        "model_predictions", "version-1", "experiment-1", publication["payload"],
    )
    assert repeated["id"] == publication["id"]

    calls = []

    def publish(*args, **kwargs):
        calls.append((args, kwargs))
        return [{"partition_key": "model:2024-01-02"}]

    monkeypatch.setattr(
        "quantmaster.research.engine.ResearchEngine.publish_model_predictions", publish,
    )
    completed = service.publish_model_outbox(publication["id"])
    repeated = service.publish_model_outbox(publication["id"])

    assert completed["status"] == "published"
    assert repeated["status"] == "published"
    assert completed["result"]["partitions"] == 1
    assert len(calls) == 1
    assert [event["type"] for event in store.publication_events(publication["id"])] == [
        "pending", "publishing", "published",
    ]

    with pytest.raises(ValueError, match="不可改写"):
        store.enqueue_publication(
            "model_predictions", "version-1", "experiment-1",
            {**publication["payload"], "rows": 999},
        )


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
    monkeypatch.setattr(
        "quantmaster.research.engine.ResearchEngine.publish_model_predictions",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("lake offline")),
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
    assert result["warnings"][0]["code"] == "model_publication_pending"
    assert store.experiment(result["experiment_id"])["status"] == "completed_with_warnings"
    publication = store.publication(result["publication"]["id"])
    assert publication["status"] == "pending"
    assert "OSError: lake offline" in publication["last_error"]

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
    assert set(report["robustness"]["failed_tests"]).issubset({
        "monte_carlo", "parameter_sensitivity", "walk_forward", "penetration",
    })
    assert report["robustness"]["schema_version"] == 1
    assert report["robustness"]["parameter_sensitivity"]["passed"]
    assert not report["robustness"]["parameter_sensitivity"]["applicable"]
    assert all(
        "train_rank_ic" in fold and "retention" in fold
        for item in report["horizons"].values() for fold in item["folds"]
    )
    assert report["gates"]["override_allowed"]
    assert benjamini_hochberg([0.01, 0.04, 0.03]) == pytest.approx([0.03, 0.04, 0.04])


def test_robustness_bootstrap_is_deterministic_and_expression_variants_are_safe():
    dates = pd.bdate_range("2022-01-01", periods=260)
    rng = np.random.default_rng(17)
    daily_ic = pd.Series(rng.normal(0.035, 0.08, len(dates)), index=dates)
    net = pd.Series(rng.normal(0.0008, 0.006, len(dates)), index=dates)
    first = monte_carlo_block_bootstrap(daily_ic, net, horizon=3, paths=300, seed=91)
    second = monte_carlo_block_bootstrap(daily_ic, net, horizon=3, paths=300, seed=91)
    assert first == second
    assert first["method"] == "circular_moving_block_bootstrap"
    assert first["probability_positive_ic"] > 0.95

    expression = "rank(-ts_mean(pct_change(close, 5), 20) * 0.5)"
    variants = expression_parameter_variants(expression)
    assert len(variants) == 4
    assert all("0.5" in item for item in variants.values())
    assert any("pct_change(close, 4)" in item for item in variants.values())
    assert any("ts_mean(pct_change(close, 5), 24)" in item for item in variants.values())


def test_validation_executes_parameter_and_penetration_layers(tmp_path):
    _config(tmp_path)
    panel = _panel(days=620, symbols=25)
    close = panel["close"]
    report = validate_factor_values(
        close.pct_change(5, fill_method=None),
        close,
        name="momentum-neighborhood",
        horizons=(3,),
        panel=panel,
        parameter_variants={
            "window=4": close.pct_change(4, fill_method=None),
            "window=6": close.pct_change(6, fill_method=None),
        },
    )
    robustness = report["robustness"]
    assert robustness["parameter_sensitivity"]["applicable"]
    assert robustness["parameter_sensitivity"]["tested_variants"] == 2
    assert robustness["penetration"]["liquidity"]["available"]
    assert {item["bucket"] for item in robustness["penetration"]["liquidity"]["buckets"]} == {
        "high", "low",
    }


def test_expression_version_validation_builds_safe_parameter_neighborhood(tmp_path, monkeypatch):
    _config(tmp_path)
    panel = _panel(days=620, symbols=25)
    store = LabStore(tmp_path / "lab.sqlite")
    service = LabService(store)
    snapshot = store.save_snapshot({
        "snapshot_hash": "robustness-snapshot",
        "research_quality": "production",
        "universe": "demo",
    })
    monkeypatch.setattr(
        service,
        "_context",
        lambda universe, start, end, progress=None: (panel, None, snapshot),
    )
    spec = FactorSpec(
        slug="safe_window_test",
        name="窗口鲁棒性",
        expression="rank(-pct_change(close, 5))",
        horizons=(3,),
    )
    _factor, version, _created = store.create_factor(spec)
    result = service.validate_version(
        version["id"], universe="demo", start="2020-01-01", end="2025-01-01",
    )
    sensitivity = result["report"]["robustness"]["parameter_sensitivity"]
    assert sensitivity["applicable"]
    assert sensitivity["tested_variants"] == 2
    assert {item["variant"] for item in sensitivity["variants"]} == {
        "pct_change#1:5→4", "pct_change#1:5→6",
    }


def test_lab_api_catalog_create_and_queue(tmp_path):
    _config(tmp_path, enabled=False)
    from quantmaster.server.app import app

    with TestClient(app) as client:
        client.headers["X-CSRF-Token"] = client.get(
            "/api/v1/session",
        ).json()["csrf_token"]
        overview = client.get("/api/v1/lab/overview")
        assert overview.status_code == 200
        assert overview.json()["capabilities"]["catalog_size"] == 48
        listing = client.get("/api/v1/lab/factors?limit=60").json()
        assert listing["total"] == 48
        created = client.post("/api/v1/lab/factors", json={
            "name": "人工反转", "expression": "rank(-pct_change(close, 5))",
        })
        assert created.status_code == 200
        version_id = created.json()["id"]
        duplicate = client.post("/api/v1/lab/factors", json={
            "name": "人工反转", "expression": "rank(close)",
        })
        assert duplicate.status_code == 400
        assert "名称" in duplicate.json()["detail"] and "已存在" in duplicate.json()["detail"]
        factor_catalog = client.get("/api/v1/research/factors").json()["factors"]
        catalog_item = next(item for item in factor_catalog if item["name"] == "人工反转")
        assert catalog_item["source"] == "quant_lab"
        assert catalog_item["slug"].startswith("manual_")
        queued = client.post("/api/v1/lab/jobs", json={
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
        retried = client.post(f"/api/v1/lab/jobs/{queued.json()['id']}/retry")
        assert retried.status_code == 202
        assert retried.json()["status"] == "queued"
        events = client.get(
            f"/api/v1/lab/jobs/{retried.json()['id']}/events?after=0",
        ).json()["items"]
        assert any(item["type"] == "retry_of" for item in events)


def test_restricted_python_policy_and_subprocess_contract():
    panel = _panel(days=80, symbols=5)
    source = (
        "def compute(features, params):\n"
        "    return features['close'].pct_change(params['window'])\n"
    )
    result = RestrictedPythonRunner(timeout_seconds=15).execute(
        source, {"close": panel["close"]}, {"window": 5},
    )
    assert result.index.equals(panel["close"].index)
    assert result.columns.equals(panel["close"].columns)
    for unsafe in (
        "import os\ndef compute(features, params):\n    return features['close']",
        "def compute(features, params):\n    return features['close'].shift(-1)",
        "def compute(features, params):\n    open('leak')\n    return features['close']",
    ):
        with pytest.raises(PythonFactorPolicyError):
            validate_python_factor(unsafe)


def test_python_artifact_and_mining_ledger_are_content_addressed(tmp_path):
    _config(tmp_path)
    source = "def compute(features, params):\n    return features['close'].pct_change(5)\n"
    artifact = write_python_factor_artifact(
        tmp_path, source=source, params={}, manifest={"split": {"test": "sealed"}},
    )
    repeated = write_python_factor_artifact(
        tmp_path, source=source, params={}, manifest={"split": {"test": "sealed"}},
    )
    assert repeated == artifact
    spec = FactorSpec(slug="python_demo", name="代码因子", kind="python", artifact=artifact)
    assert spec.to_dict()["artifact"]["hash"] == artifact["hash"]

    store = LabStore(tmp_path / "ledger.sqlite")
    run = store.create_mining_run({"candidate_limit": 24})
    store.save_mining_candidate(run["id"], {
        "id": "candidate-1", "name": "动量", "status": "validated",
        "train_metrics": {"rank_ic": 0.03}, "valid_metrics": {"rank_ic": 0.02},
    })
    loaded = store.mining_run(run["id"])
    assert loaded["candidates"][0]["proposal"]["name"] == "动量"
    assert loaded["candidates"][0]["metrics"]["valid_metrics"]["rank_ic"] == 0.02


def test_sealed_three_way_split_has_purges_and_minimum_holdouts():
    dates = pd.bdate_range("2018-01-01", periods=1200)
    split = sealed_three_way_split(dates, purge_gap=7)
    assert split["train"]["days"] >= 504
    assert split["valid"]["days"] >= 252
    assert split["test"]["days"] >= 252
    assert pd.Timestamp(split["train"]["end"]) < pd.Timestamp(split["valid"]["start"])
    assert pd.Timestamp(split["valid"]["end"]) < pd.Timestamp(split["test"]["start"])


def test_python_miner_freezes_order_before_sealed_test():
    panel = _panel(days=1100, symbols=6)
    source = "def compute(features, params):\n    return features['close'].pct_change(5)\n"
    client = _SequenceLLMClient([{"candidates": [{
        "name": "五日动量", "hypothesis": "短期趋势延续", "objective": "稳定 RankIC",
        "required_features": ["close"], "warmup": 20, "parameters": [], "code": source,
    }]}])
    catalog = [{
        "name": "close", "group": "price_volume_v2", "description": "收盘价",
        "pit_grade": "derived", "coverage": 1.0, "available": True,
    }]
    report = PythonFactorMiner(client=client).mine_report(
        {"close": panel["close"]}, catalog, rounds=1, candidate_limit=1, finalists=1,
    )
    assert report.llm_calls == 1
    assert len(report.finalists) == 1
    assert report.finalists[0].pareto_rank == 1
    assert report.finalists[0].test_metrics["days"] > 200


def test_python_mining_api_is_opt_in_and_exposes_preview(tmp_path):
    cfg = _config(tmp_path, enabled=False)
    from quantmaster.server.app import app

    with TestClient(app) as client:
        client.headers["X-CSRF-Token"] = client.get(
            "/api/v1/session",
        ).json()["csrf_token"]
        preview = client.post("/api/v1/lab/mining/preview", json={
            "start": "2018-01-01", "end": "2026-01-01", "horizon": 3,
        })
        assert preview.status_code == 200
        assert preview.json()["test_policy"] == "sealed_until_finalist_order_frozen"
        disabled = client.post("/api/v1/lab/jobs", json={
            "kind": "discover_python", "params": {
                "universe": "demo", "start": "2018-01-01", "end": "2026-01-01",
            },
        })
        assert disabled.status_code == 400
        cfg.lab.ai_python_mining_enabled = True
        queued = client.post("/api/v1/lab/jobs", json={
            "kind": "discover_python", "params": {
                "universe": "demo", "start": "2018-01-01", "end": "2026-01-01",
            },
        })
        assert queued.status_code == 202
        runs = client.get("/api/v1/lab/mining/runs").json()["items"]
        assert runs and runs[0]["job_id"] == queued.json()["id"]
