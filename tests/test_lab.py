from __future__ import annotations

import json
import time
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from quantmaster.ai.llm import LLMError
from quantmaster.config import Config, set_config
from quantmaster.data.research import ResearchDataBundle
from quantmaster.data.research_features import registered_features
from quantmaster.factors.mining.llm_miner import LLMFactorMiner
from quantmaster.factors.mining.python_miner import PythonFactorMiner
from quantmaster.factors.python_artifact import (
    PythonFactorPolicyError,
    RestrictedPythonRunner,
    execute_python_factor_artifact,
    validate_python_factor,
    write_python_factor_artifact,
)
from quantmaster.lab.catalog import curated_catalog
from quantmaster.lab.dataset import (
    build_membership_mask,
    clear_local_dataset_caches,
    create_snapshot,
    inspect_local_dataset,
    load_csi800_members_as_of,
    load_local_dataset,
)
from quantmaster.lab.errors import LabError, classify_lab_error
from quantmaster.lab.ml import (
    artifact_sha256,
    engineer_features,
    make_indexed_samples,
    make_samples,
    predict_panel,
    train,
    train_indexed,
)
from quantmaster.lab.models import FactorSpec
from quantmaster.lab.research import sealed_three_way_split
from quantmaster.lab.robustness import (
    expression_parameter_variants,
    monte_carlo_block_bootstrap,
)
from quantmaster.lab.service import LabService, get_lab_service
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
    cfg.lab.walk_forward_train_days = 120
    cfg.lab.walk_forward_test_days = 84
    cfg.lab.walk_forward_step_days = 84
    cfg.lab.walk_forward_purge_days = 30
    cfg.lab.walk_forward_folds = 3
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


def test_lab_service_owner_reuses_readers_and_writers_per_data_root(tmp_path):
    _config(tmp_path)
    writer = get_lab_service()
    reader = get_lab_service(read_only=True)

    assert get_lab_service() is writer
    assert get_lab_service(read_only=True) is reader
    assert reader is not writer
    assert reader.store.read_only is True

    _config(tmp_path / "next")
    assert get_lab_service() is not writer
    assert get_lab_service(read_only=True) is not reader


def test_lab_transport_does_not_export_service_lifecycle_owner():
    from quantmaster.server import lab as lab_api

    assert not hasattr(lab_api, "get_lab_service")


def test_cloud_suggestion_job_uses_lab_service_owner_and_publishes_artifact(
    tmp_path, monkeypatch,
):
    _config(tmp_path)
    from quantmaster.lab.llm_jobs import LabLLMJobs

    class Service:
        @staticmethod
        def suggest_revision(*_args, **_kwargs):
            return {"id": "suggestion-1", "status": "pending"}

    monkeypatch.setattr("quantmaster.lab.service.get_lab_service", lambda: Service())
    jobs = LabLLMJobs()
    try:
        job, created = jobs.submit("factor-1", True, {"rows": 3})
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            completed = jobs.runtime.store.get(job["id"])
            if completed["status"] in {"completed", "failed"}:
                break
            time.sleep(0.01)

        assert created is True
        assert completed["status"] == "completed"
        artifact = jobs.runtime.store.artifact(completed["result_artifact_id"])
        assert artifact["kind"] == "lab.cloud_suggestion"
        assert artifact["payload"] == {
            "schema_version": "1.0",
            "result": {"id": "suggestion-1", "status": "pending"},
        }
        assert artifact["lineage"] == {"version_id": "factor-1"}
    finally:
        assert jobs.runtime.stop()["status"] == "stopped"


def test_cloud_suggestion_shutdown_releases_the_worker_plan_runtime(tmp_path):
    _config(tmp_path)
    from quantmaster.lab.llm_jobs import get_lab_llm_jobs, shutdown_lab_llm_jobs

    shutdown_lab_llm_jobs()
    first = get_lab_llm_jobs()
    shutdown_lab_llm_jobs()

    assert first.runtime.snapshot()["status"] == "stopped"
    second = get_lab_llm_jobs()
    assert second is not first
    shutdown_lab_llm_jobs()


def test_cloud_suggestion_confirmation_follows_auto_send_setting(tmp_path, monkeypatch):
    cfg = _config(tmp_path)
    from starlette.responses import Response

    from quantmaster.runtime.jobs import UnifiedJobRuntime
    from quantmaster.server import lab as lab_api

    response = lab_api.suggest(
        "factor-1", lab_api.SuggestionRequest(use_cloud=True), Response(),
    )
    assert response.status_code == 409
    assert json.loads(response.body)["error"]["code"] == "OUTBOUND_CONFIRMATION_REQUIRED"

    submitted = []
    jobs = SimpleNamespace(submit=lambda *args: (submitted.append(args) or ({"id": "job-1"}, True)))
    monkeypatch.setattr("quantmaster.lab.llm_jobs.get_lab_llm_jobs", lambda: jobs)
    monkeypatch.setattr(UnifiedJobRuntime, "public", staticmethod(lambda job: job))

    accepted = lab_api.suggest(
        "factor-1",
        lab_api.SuggestionRequest(use_cloud=True, outbound_confirmed=True),
        Response(),
    )
    assert accepted == {"id": "job-1"}
    assert submitted

    cfg.lab.allow_cloud_sample = True
    accepted = lab_api.suggest(
        "factor-2", lab_api.SuggestionRequest(use_cloud=True), Response(),
    )
    assert accepted == {"id": "job-1"}


def test_cloud_sample_ui_describes_and_enforces_two_state_contract():
    root = __import__("pathlib").Path(__file__).parents[1]
    index = (root / "quantmaster/server/static/index.html").read_text(encoding="utf-8")
    settings_script = (root / "quantmaster/server/static/settings.js").read_text(encoding="utf-8")
    lab_script = (root / "quantmaster/server/static/lab.js").read_text(encoding="utf-8")

    assert "关闭时每次发送前单独确认；打开后直接发送" in index
    assert "不再逐次询问" in settings_script
    assert "research.allow_cloud_sample" in lab_script
    assert "outbound_confirmed:outboundConfirmed" in lab_script


def test_discovery_form_uses_the_queue_preflight_once():
    root = __import__("pathlib").Path(__file__).parents[1]
    lab_script = (root / "quantmaster/server/static/lab.js").read_text(encoding="utf-8")

    assert "const job = await enqueue(operation, params);" in lab_script
    assert "confirmPreflight(operation, params, kindLabel[operation]" not in lab_script


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


def test_lab_sandbox_feature_entry_preserves_news_preview_eligibility(tmp_path, monkeypatch):
    _config(tmp_path)
    panel = _panel(days=10, symbols=2)
    store = LabStore(tmp_path / "lab.sqlite")
    service = LabService(store)
    snapshot = {
        "snapshot_hash": "sandbox-news-snapshot",
        "payload": {"research_quality": "sandbox"},
    }
    monkeypatch.setattr(
        service,
        "_context",
        lambda universe, start, end, progress=None: (panel, None, snapshot),
    )
    monkeypatch.setattr(service, "_pit_fundamentals", lambda *args, **kwargs: {})
    observed = {}

    def preview(index, symbols, *, tier):
        observed["tier"] = tier
        result = pd.DataFrame(0.1, index=index, columns=symbols)
        result.attrs["news_factor"] = {
            "tier": "sandbox",
            "sample_start": "2023-01-02",
            "sample_end": "2023-01-13",
            "sessions": 10,
            "coverage": 1.0,
            "event_count": 12,
            "sources": [{
                "source_id": "sina_live",
                "formal_eligible": False,
                "reasons": ["non_official_source"],
            }],
            "formal_eligible": False,
            "reasons": ["sandbox_tier", "history_sessions_below_1038"],
        }
        return result

    monkeypatch.setattr("quantmaster.ai.sentiment.quality_sentiment_panel", preview)
    features, catalog, _snapshot, _bundle_hash = service._python_mining_context(
        "demo", "2023-01-02", "2023-01-13",
    )

    assert observed["tier"] == "sandbox"
    assert features["news_sentiment"].attrs["news_factor"]["formal_eligible"] is False
    news = next(item for item in catalog if item["name"] == "news_sentiment")
    assert news["tier"] == "sandbox"
    assert news["pit_grade"] == "research_only"
    assert news["formal_eligible"] is False
    assert news["evidence"]["sources"][0]["source_id"] == "sina_live"


def test_legacy_membership_requires_explicit_current_sandbox_and_is_not_backdated():
    records = pd.DataFrame([
        {"trade_date": "2024-01-02", "index_code": "A", "symbol": "AAA"},
        {"trade_date": "2024-01-03", "index_code": "B", "symbol": "BBB"},
        {"trade_date": "2024-01-05", "index_code": "A", "symbol": "CCC"},
    ])
    calendar = pd.bdate_range("2024-01-02", "2024-01-08")
    with pytest.raises(RuntimeError, match="sandbox_current"):
        build_membership_mask(records, calendar)

    mask = build_membership_mask(records, calendar, mode="sandbox_current")
    assert not mask.iloc[:-1].to_numpy().any()
    assert mask.loc["2024-01-08", ["BBB", "CCC"]].all()
    assert not mask.loc["2024-01-08", "AAA"]


def test_formal_membership_uses_first_observed_cutoff_and_effective_session():
    records = pd.DataFrame([
        {
            "effective_session_date": "2024-01-02", "index_code": "A",
            "symbol": "AAA", "published_at": "2024-01-01T08:00:00Z",
            "first_observed_at": "2024-01-02T06:59:00Z",
        },
        {
            "effective_session_date": "2024-01-03", "index_code": "A",
            "symbol": "BBB", "published_at": "2024-01-02T08:00:00Z",
            "first_observed_at": "2024-01-04T07:01:00Z",
        },
    ])
    calendar = pd.DatetimeIndex(["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"])

    strict = build_membership_mask(records, calendar)
    assert strict.loc["2024-01-03", "AAA"]
    assert not strict.loc["2024-01-04", "BBB"]
    assert strict.loc["2024-01-05", "BBB"]
    assert not strict.loc["2024-01-05", "AAA"]

    published = build_membership_mask(
        records, calendar, knowledge_mode="trusted_published",
    )
    assert published.loc["2024-01-03", "BBB"]
    assert not published.loc["2024-01-03", "AAA"]


def test_production_fundamentals_use_observed_sessions_for_date_only_announcements(
    tmp_path, monkeypatch,
):
    """五一休市不能被 BDay 猜成 session，ann_date 当天也不能盘中可见。"""
    _config(tmp_path)
    sessions = pd.DatetimeIndex(["2023-04-28", "2023-05-04", "2023-05-05"])
    indicators = pd.DataFrame(
        {
            "pe_ttm": 10.0,
            "pb": 2.0,
            "dv_ratio": 3.0,
            "total_mv": 1e8,
        },
        index=sessions,
    )
    quarterly = pd.DataFrame(
        {
            "report_date": pd.to_datetime(["2022-12-31"]),
            "roe": [9.5],
            "update_flag": ["0"],
        },
        index=pd.DatetimeIndex(["2023-04-28"], name="ann_date"),
    )
    monkeypatch.setattr(
        "quantmaster.data.tushare_source.TushareSource.daily_indicators",
        lambda self, symbol, start, end: indicators,
    )
    monkeypatch.setattr(
        "quantmaster.data.tushare_source.TushareSource.quarterly_roe",
        lambda self, symbol, start_year: quarterly,
    )

    result = LabService._pit_fundamentals(
        ["600000.SH"], "2023-04-28", "2023-05-05", production=True,
    )

    assert result["roe"].index.equals(sessions)
    assert pd.isna(result["roe"].loc["2023-04-28", "600000.SH"])
    assert result["roe"].loc["2023-05-04", "600000.SH"] == 9.5


def test_csi800_as_of_uses_latest_known_snapshot_without_lookahead():
    class Source:
        def index_weights(self, index_code, start, end):
            count = 300 if index_code == "000300.SH" else 500
            current = [
                f"6{position:05d}.SH" if count == 300 else f"0{position:05d}.SZ"
                for position in range(count)
            ]
            future = [
                f"7{position:05d}.SH" if count == 300 else f"2{position:05d}.SZ"
                for position in range(count)
            ]
            return pd.DataFrame([
                {
                    "index_code": index_code,
                    "symbol": symbol,
                    "trade_date": trade_date,
                    "weight": 1,
                    "acquired_at": "2024-02-15T06:59:00+00:00",
                    "snapshot_expected_count": count,
                }
                for trade_date, symbols in (
                    ("2024-01-31", current), ("2024-03-01", future),
                )
                for symbol in symbols
            ])

    result = load_csi800_members_as_of("2024-02-15", source=Source())
    assert len(result["symbols"]) == 800
    assert "600000.SH" in result["symbols"]
    assert "000000.SZ" in result["symbols"]
    assert "700000.SH" not in result["symbols"]
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
    panel = {"close": pd.DataFrame(
        [[1.0, 2.0], [1.1, 2.1]], index=membership.index, columns=membership.columns,
    )}
    first = create_snapshot(
        "csi800", "2024-01-01", "2024-01-31", panel=panel, membership=membership,
    )
    second = create_snapshot(
        "csi800", "2024-01-01", "2024-01-31", panel=panel, membership=membership,
    )
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


def test_lab_summary_lists_never_decode_large_job_or_study_artifacts(tmp_path, monkeypatch):
    _config(tmp_path)
    store = LabStore(tmp_path / "lab.sqlite")
    marker = "x" * 200_000
    queued = store.enqueue(
        "prepare_data", {"universe": "demo"},
        preflight={"resource_class": "cpu", "coverage": marker},
    )
    experiment = store.create_experiment("摘要实验", "ridge", {
        "universe": "demo", "start": "2024-01-01", "horizon": 3,
    })
    store.update_experiment(experiment["id"], status="completed", result={
        "metrics": {"correlation": 0.12}, "artifact": marker,
        "telemetry": {"effective_device": "cpu"},
    })
    study = store.create_study({
        "universe": "demo", "start": "2024-01-01", "budget_hours": 1,
        "protocol": {"test_window": 244},
    })
    store.update_study(study["id"], status="completed", result={
        "trials": [{"number": index, "artifact": marker} for index in range(2)],
        "sealed_metrics": {"rank_ic": 0.03},
    })

    monkeypatch.setattr(
        LabStore, "_decode",
        staticmethod(lambda *_args, **_kwargs: pytest.fail("summary decoded an artifact blob")),
    )
    jobs = store.jobs(summary=True)
    experiments = store.list_experiments(summary=True)
    studies = store.studies(summary=True)

    assert jobs[0]["id"] == queued["id"]
    assert jobs[0]["preflight"] == {}
    assert jobs[0]["params"] == {}
    assert experiments[0]["result_json"]["metrics"]["correlation"] == 0.12
    assert experiments[0]["result_json"]["telemetry"]["effective_device"] == "cpu"
    assert studies[0]["id"] == study["id"]
    assert studies[0]["result"] == {"trial_count": 2, "sealed": True}


def test_lab_capabilities_are_published_by_worker_and_read_without_hardware_probe(
    tmp_path, monkeypatch,
):
    _config(tmp_path)
    from quantmaster.lab import capabilities as capability_snapshot

    expected = capability_snapshot._fallback_capabilities()
    expected["models"]["torch"] = True
    expected["models"]["available_models"] = ["ridge", "transformer"]
    expected["local_data"]["catalogued_symbols"] = 800
    monkeypatch.setattr(capability_snapshot, "build_capabilities", lambda: expected)

    published = capability_snapshot.publish_capabilities()
    monkeypatch.setattr(
        "quantmaster.lab.dataset.readiness",
        lambda: pytest.fail("Web read ran local data readiness inspection"),
    )
    monkeypatch.setattr(
        "quantmaster.lab.ml.capabilities",
        lambda: pytest.fail("Web read ran hardware capability probe"),
    )

    value = LabService(read_only=True).capabilities()

    assert value["models"]["available_models"] == ["ridge", "transformer"]
    assert value["local_data"]["catalogued_symbols"] == 800
    assert value["snapshot"]["id"] == published["id"]
    assert value["snapshot"]["state"] == "fresh"


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


def test_prepare_data_keeps_partition_checkpoints_and_partial_result(tmp_path, monkeypatch):
    _config(tmp_path)
    service = LabService(LabStore(tmp_path / "lab.sqlite"))
    before = {
        "membership_missing": False,
        "providers": [
            {"id": "free-stockdb", "available": True},
            {"id": "tushare", "available": False},
        ],
        "gaps": [
            {
                "symbol": "A.SH",
                "segments": [{
                    "start": "2024-01-01", "end": "2024-01-31", "kind": "critical",
                }],
            },
            {
                "symbol": "B.SH",
                "segments": [{
                    "start": "2024-01-01", "end": "2024-01-31", "kind": "critical",
                }],
            },
        ],
        "repair_symbol_count": 2,
        "critical_repair_symbol_count": 2,
        "research_eligible": False,
    }
    after = {
        **before, "gaps": [before["gaps"][1]], "repair_symbol_count": 1,
        "critical_repair_symbol_count": 1,
    }
    plans = iter([before, after])
    monkeypatch.setattr("quantmaster.lab.service.dataset_repair_plan", lambda *_: next(plans))
    monkeypatch.setattr("quantmaster.lab.dataset.clear_local_dataset_caches", lambda: None)

    class Envelope:
        def __init__(self, symbol):
            self.symbol = symbol
            self.quality = SimpleNamespace(to_dict=lambda: {
                "status": "verified", "symbol": symbol,
            })

        def require_data(self):
            if self.symbol == "B.SH":
                raise OSError("disk I/O error")

    monkeypatch.setattr(
        "quantmaster.data.refresh_history", lambda symbol, *_args, **_kwargs: Envelope(symbol),
    )
    events = []

    def progress(value, phase, detail="", **metadata):
        events.append({"progress": value, "phase": phase, "detail": detail, **metadata})

    result = service.prepare_data(
        universe="demo", start="2024-01-01", end="2024-01-31",
        provider="free-stockdb", progress=progress,
    )

    assert result["partitions"] == {
        "total": 2, "persisted": 1, "failed": 1, "remaining": 1,
        "persisted_items": ["A.SH"], "failed_items": ["B.SH"],
    }
    assert result["warnings"][0]["code"] == "DATA_PARTITION_INCOMPLETE"
    assert result["safe_retry_point"] == "bars"
    checkpoints = [item for item in events if item.get("event_type") == "partition_checkpoint"]
    assert {item["metadata"]["partition"] for item in checkpoints} >= {"universe", "A.SH", "B.SH"}
    assert result["stages"]["bars"]["status"] == "completed_with_warnings"


def test_prepare_data_preflight_estimates_atomic_rewrite_peak(tmp_path, monkeypatch):
    _config(tmp_path)
    service = LabService(LabStore(tmp_path / "lab.sqlite"))
    monkeypatch.setattr("quantmaster.lab.service.run_preflight", lambda *_: {
        "runnable": True, "state": "ready", "blockers": [], "warnings": [],
        "estimate": {}, "dataset": {},
    })
    monkeypatch.setattr("quantmaster.lab.service.dataset_repair_plan", lambda *_: {
        "repair_symbol_count": 2, "membership_missing": False,
        "missing_session_count": 10,
        "gaps": [{"existing_bytes": 1000}, {"existing_bytes": 2000}],
    })
    monkeypatch.setattr(
        "quantmaster.lab.service.shutil.disk_usage",
        lambda _path: SimpleNamespace(free=1024),
    )

    report = service.preflight("prepare_data", {
        "universe": "demo", "start": "2024-01-01", "end": "2024-01-31",
    })

    estimate = report["estimate"]
    assert estimate["space_purpose"] == "bars_atomic_rewrite"
    assert estimate["repair_temporary_bytes"] > 3000
    assert estimate["repair_output_bytes"] > 0
    assert estimate["disk_bytes"] == (
        estimate["required_peak_bytes"] + estimate["reserve_bytes"]
    )
    assert estimate["available_after_reserve_bytes"] == 0
    assert report["runnable"] is False
    assert report["blockers"][-1]["code"] == "STORAGE_SPACE_INSUFFICIENT"


def test_prepare_data_space_guard_persists_blocked_safe_retry(tmp_path, monkeypatch):
    _config(tmp_path)
    service = LabService(LabStore(tmp_path / "lab.sqlite"))
    plan = {
        "membership_missing": False,
        "providers": [{"id": "free-stockdb", "available": True}],
        "gaps": [{
            "symbol": "A.SH", "existing_bytes": 1024, "missing_sessions": 1,
            "segments": [{
                "start": "2024-01-01", "end": "2024-01-31", "kind": "critical",
            }],
        }],
        "repair_symbol_count": 1, "critical_repair_symbol_count": 1,
        "research_eligible": False,
    }
    monkeypatch.setattr("quantmaster.lab.service.dataset_repair_plan", lambda *_: plan)
    monkeypatch.setattr(
        "quantmaster.lab.service.shutil.disk_usage",
        lambda _path: SimpleNamespace(free=1024),
    )
    events = []

    def progress(value, phase, detail="", **metadata):
        events.append({"progress": value, "phase": phase, "detail": detail, **metadata})

    with pytest.raises(LabError) as caught:
        service.prepare_data(
            universe="demo", start="2024-01-01", end="2024-01-31",
            provider="free-stockdb", progress=progress,
        )

    assert caught.value.code == "STORAGE_SPACE_INSUFFICIENT"
    blocked = [
        item for item in events
        if item.get("metadata", {}).get("status") == "blocked"
    ]
    assert blocked[-1]["event_type"] == "partition_checkpoint"
    assert blocked[-1]["metadata"]["safe_retry_point"] == "bars"
    assert blocked[-1]["metadata"]["diagnostic_code"] == "STORAGE_SPACE_INSUFFICIENT"
    assert blocked[-1]["metadata"]["reserve_bytes"] > 0


@pytest.mark.parametrize(
    ("failure", "code"),
    [
        (OSError(28, "No space left on device"), "STORAGE_SPACE_INSUFFICIENT"),
        (RuntimeError("database or disk is full"), "STORAGE_SPACE_INSUFFICIENT"),
        (RuntimeError("disk I/O error"), "STORAGE_IO_ERROR"),
    ],
)
def test_classify_lab_storage_failures(failure, code):
    assert classify_lab_error(failure).code == code


def test_partition_checkpoint_is_projected_and_warnings_finish_partial(tmp_path):
    _config(tmp_path)
    store = LabStore(tmp_path / "lab.sqlite")
    queued = store.enqueue("prepare_data", {"universe": "demo"})
    store.claim_next("worker")
    store.update_job(
        queued["id"], 40, "数据准备 · bars", "A.SH 已持久化",
        event_type="partition_checkpoint",
        metadata={
            "stage": "bars", "status": "completed", "partition": "A.SH",
            "persisted": 1, "total": 2,
        },
    )
    store.finish_job(queued["id"], result={
        "warnings": [{"code": "DATA_PARTITION_INCOMPLETE", "message": "1 个分区未完成"}],
        "partitions": {"persisted": 1, "failed": 1, "remaining": 1},
    })

    job = store.job(queued["id"])

    assert job["status"] == "completed_with_warnings"
    assert job["checkpoint"]["type"] == "partition_checkpoint"
    assert job["checkpoint"]["partition"] == "A.SH"
    assert job["checkpoint"]["persisted"] == 1


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


def test_local_snapshot_plans_actual_membership_ranges_and_invalidates(tmp_path, monkeypatch):
    _config(tmp_path)
    from quantmaster.data.storage import BarStore

    records = pd.DataFrame([
        {
            "index_code": "000300.SH", "trade_date": "2023-01-02", "symbol": "A.SH",
            "published_at": "2023-01-01T08:00:00Z",
            "acquired_at": "2023-01-02T06:59:00Z",
        },
        {
            "index_code": "000300.SH", "trade_date": "2023-01-02", "symbol": "B.SH",
            "published_at": "2023-01-01T08:00:00Z",
            "acquired_at": "2023-01-02T06:59:00Z",
        },
        {
            "index_code": "000300.SH", "trade_date": "2023-07-03", "symbol": "A.SH",
            "published_at": "2023-07-02T08:00:00Z",
            "acquired_at": "2023-07-03T06:59:00Z",
        },
    ])
    monkeypatch.setattr(
        "quantmaster.lab.dataset._cached_membership_records",
        lambda _start, _end: records.copy(),
    )
    dates = pd.bdate_range("2022-06-01", "2023-12-29")

    def bars(index):
        base = np.linspace(10.0, 20.0, len(index))
        return pd.DataFrame({
            "open": base, "high": base + 0.2, "low": base - 0.2,
            "close": base + 0.1, "volume": 1_000.0, "amount": 10_000.0,
        }, index=index)

    store = BarStore()
    verified = {
        "status": "verified", "stale": False, "partial": False,
        "issues": [], "sources": ["test:bars"],
    }
    store.put("A.SH", bars(dates), source="test:bars", quality=verified)
    store.put(
        "B.SH", bars(dates[dates <= "2023-06-30"]),
        source="test:bars", quality=verified,
    )

    inspected = inspect_local_dataset("csi800", "2023-01-02", "2023-12-29")
    assert inspected["state"] == "ready"
    assert inspected["required_ranges"]["B.SH"]["end"] == "2023-06-30"
    assert inspected["required_ranges"]["A.SH"]["end"] == "2023-12-29"
    panel, membership, snapshot = load_local_dataset(
        "csi800", "2023-01-02", "2023-12-29",
    )
    assert snapshot["production_eligible"] is True
    assert membership is not None and not membership.loc["2023-07-03":, "B.SH"].any()
    assert panel["close"].index.min() < pd.Timestamp("2023-01-02")

    _panel2, _membership2, repeated = load_local_dataset(
        "csi800", "2023-01-02", "2023-12-29",
    )
    assert repeated["cache_hit"] is True
    clear_local_dataset_caches()
    _panel2, _membership2, persistent = load_local_dataset(
        "csi800", "2023-01-02", "2023-12-29",
    )
    assert persistent["cache_hit"] is True
    assert persistent["load_profile"]["source"] == "persistent_evidence"
    assert persistent["snapshot_hash"] == snapshot["snapshot_hash"]
    changed = bars(dates)
    changed.iloc[-1, changed.columns.get_loc("close")] += 1
    store.put("A.SH", changed, replace=True, source="test:bars", quality=verified)
    _panel3, _membership3, refreshed = load_local_dataset(
        "csi800", "2023-01-02", "2023-12-29",
    )
    assert refreshed["cache_hit"] is False
    assert refreshed["snapshot_hash"] != snapshot["snapshot_hash"]


def test_local_snapshot_blocks_production_when_bar_quality_is_degraded(
    tmp_path, monkeypatch,
) -> None:
    _config(tmp_path)
    from quantmaster.data.storage import BarStore

    records = pd.DataFrame([{
        "index_code": "000300.SH", "trade_date": "2023-01-02", "symbol": "A.SH",
        "published_at": "2023-01-01T08:00:00Z",
        "acquired_at": "2023-01-02T06:59:00Z",
    }])
    monkeypatch.setattr(
        "quantmaster.lab.dataset._cached_membership_records",
        lambda _start, _end: records.copy(),
    )
    dates = pd.bdate_range("2022-06-01", "2023-12-29")
    base = np.linspace(10.0, 20.0, len(dates))
    frame = pd.DataFrame({
        "open": base, "high": base + 0.2, "low": base - 0.2,
        "close": base + 0.1, "volume": 1_000.0, "amount": 10_000.0,
    }, index=dates)
    BarStore().put(
        "A.SH",
        frame,
        source="free-stockdb",
        quality={
            "status": "degraded", "stale": False, "partial": False,
            "issues": ["前复权因子链未验证"], "sources": ["free-stockdb"],
        },
    )

    inspected = inspect_local_dataset("csi800", "2023-01-02", "2023-12-29")
    _panel_value, _membership, snapshot = load_local_dataset(
        "csi800", "2023-01-02", "2023-12-29",
    )

    assert inspected["production_eligible"] is False
    assert inspected["quality_gaps"][0]["status"] == "degraded"
    assert snapshot["production_eligible"] is False
    assert snapshot["research_quality"] == "sandbox"
    assert snapshot["manifest"]["bar_quality"][0]["quality"]["status"] == "degraded"


def test_verified_tail_does_not_upgrade_unverified_legacy_history(
    tmp_path, monkeypatch,
) -> None:
    _config(tmp_path)
    from quantmaster.data.storage import BarStore

    records = pd.DataFrame([{
        "index_code": "000300.SH", "trade_date": "2023-01-02", "symbol": "A.SH",
        "published_at": "2023-01-01T08:00:00Z",
        "acquired_at": "2023-01-02T06:59:00Z",
    }])
    monkeypatch.setattr(
        "quantmaster.lab.dataset._cached_membership_records",
        lambda _start, _end: records.copy(),
    )
    dates = pd.bdate_range("2022-06-01", "2023-12-29")
    base = np.linspace(10.0, 20.0, len(dates))
    store = BarStore()
    store.put("A.SH", pd.DataFrame({
        "open": base, "high": base + 0.2, "low": base - 0.2,
        "close": base + 0.1, "volume": 1_000.0, "amount": 10_000.0,
    }, index=dates))
    store.mark_checked(
        "A.SH",
        "2023-12-29",
        "2023-12-29",
        source="tushare",
        quality={
            "status": "verified", "stale": False, "partial": False,
            "issues": [], "sources": ["tushare"],
        },
    )

    inspected = inspect_local_dataset("csi800", "2023-01-02", "2023-12-29")

    assert inspected["production_eligible"] is False
    evidence = inspected["bars"][0]["quality"]
    assert evidence["lineage_complete"] is False
    assert "已验证来源链没有覆盖完整请求区间" in evidence["issues"]


def test_indexed_samples_match_legacy_ridge_and_reuse_cube(tmp_path):
    _config(tmp_path)
    panel = _panel(days=240, symbols=6)
    legacy_x, legacy_y, legacy_metadata, _names = make_samples(
        panel, sequence_length=10,
    )
    cache = tmp_path / "feature-cache" / "snapshot-unit"
    indexed = make_indexed_samples(
        panel, sequence_length=10, storage_dir=cache,
    )

    assert isinstance(indexed.cube, np.memmap)
    assert indexed.cube.dtype == np.float32
    assert len(indexed) == len(legacy_x)
    assert np.allclose(indexed.window(0), legacy_x[0], atol=1e-6)
    assert np.allclose(indexed.targets, legacy_y, atol=1e-6)
    assert indexed.metadata_frame(slice(0, 1)).iloc[0]["symbol"] == legacy_metadata[0]["symbol"]

    legacy = train(
        "ridge", legacy_x, legacy_y, legacy_metadata,
        artifact_dir=tmp_path / "legacy-ridge", config={"alpha": 2.0},
    )
    compact = train_indexed(
        "ridge", indexed, artifact_dir=tmp_path / "indexed-ridge",
        config={"alpha": 2.0},
    )
    assert np.allclose(compact["_predicted"], legacy["_predicted"], atol=2e-5)
    assert compact["telemetry"]["effective_device"] == "cpu"

    reused = make_indexed_samples(
        panel, sequence_length=10, storage_dir=cache,
    )
    assert reused.cache_hit is True
    assert not (cache / "sample-windows.npy").exists()


def test_indexed_torch_reports_real_cuda_telemetry(tmp_path):
    torch = pytest.importorskip("torch")
    if not torch.cuda.is_available():
        pytest.skip("CUDA runtime is not available")
    _config(tmp_path)
    panel = _panel(days=180, symbols=8)
    samples = make_indexed_samples(
        panel, sequence_length=10, storage_dir=tmp_path / "gpu-cache",
    )
    result = train_indexed(
        "mlp", samples, artifact_dir=tmp_path / "gpu-model",
        config={"device": "cuda", "epochs": 1, "batch_size": 128, "patience": 2},
    )

    telemetry = result["telemetry"]
    assert telemetry["effective_device"] == "cuda:0"
    assert telemetry["gpu_name"]
    assert telemetry["amp"] in {"bf16", "fp16"}
    assert telemetry["peak_gpu_memory_mb"] > 0
    assert telemetry["samples_per_second"] > 0
    assert (tmp_path / "gpu-model" / "mlp.pt").is_file()


def test_retired_ridge_artifact_requires_explicit_migration(tmp_path):
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
    with pytest.raises(ValueError, match="一次性迁移"):
        predict_panel(panel, {"manifest": manifest_path.relative_to(tmp_path).as_posix()})


def test_learned_model_is_shadow_candidate_until_manual_champion_promotion(
    tmp_path, monkeypatch,
):
    pytest.skip("单周期 schema v1 训练入口已退役；由共享多周期 schema v2 覆盖")
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
            "q_value": 0.03, "folds": [], "gates": {"passed": True},
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
    assert report["best_horizon"] in {1, 3, 5, 7, 10, 20, 30}
    assert len(report["horizons"]) == 7
    assert all(len(item["folds"]) == 3 for item in report["horizons"].values())
    assert report["protocol"] == {
        "train_window": 120,
        "test_window": 84,
        "step_days": 84,
        "purge_gap": 30,
        "development_folds": 3,
        "horizons": [1, 3, 5, 7, 10, 20, 30],
        "seed": 42,
    }
    assert all(item["sealed"]["sealed"] for item in report["horizons"].values())
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


def test_lab_api_catalog_create_and_queue(tmp_path, monkeypatch):
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
        assert queued.status_code == 409
        assert queued.json()["error"]["code"] == "DATA_COVERAGE_INSUFFICIENT"
        service = get_lab_service()
        monkeypatch.setattr(service, "preflight", lambda *_args, **_kwargs: {
            "runnable": True, "state": "ready", "resource_class": "cpu",
            "blockers": [], "warnings": [], "dataset": {},
        })
        queued = client.post("/api/v1/lab/jobs", json={
            "kind": "validate",
            "params": {"version_id": version_id, "universe": "demo",
                       "start": "2023-01-01", "end": "2024-01-01"},
        })
        assert queued.status_code == 202
        assert queued.json()["status"] == "queued"
        get_lab_service().store.finish_job(
            queued.json()["id"], error="测试失败",
        )
        retried = client.post(f"/api/v1/jobs/{queued.json()['id']}/retry")
        assert retried.status_code == 202
        assert retried.json()["status"] == "queued"
        events = client.get(
            f"/api/v1/jobs/{retried.json()['id']}/events?after=0",
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
        "def compute(features, params):\n    return features['close'].rolling(5, center=True).mean()",
        "def compute(features, params):\n    open('leak')\n    return features['close']",
    ):
        with pytest.raises(PythonFactorPolicyError):
            validate_python_factor(unsafe)

    parameterized_shift = (
        "def compute(features, params):\n"
        "    return features['close'].shift(params['lag'])\n"
    )
    assert validate_python_factor(parameterized_shift)["shift_params"] == ["lag"]
    with pytest.raises(PythonFactorPolicyError, match="非负整数"):
        RestrictedPythonRunner(timeout_seconds=15).execute(
            parameterized_shift, {"close": panel["close"]}, {"lag": -1},
        )
    causal = RestrictedPythonRunner(timeout_seconds=15).execute(
        parameterized_shift, {"close": panel["close"]}, {"lag": 1},
    )
    assert causal.index.equals(panel["close"].index)
    with pytest.raises(PythonFactorPolicyError, match="diff"):
        validate_python_factor(
            "def compute(features, params):\n"
            "    return features['close'].diff(-1)\n"
        )
    parameterized_change = (
        "def compute(features, params):\n"
        "    return features['close'].pct_change(params['lag'])\n"
    )
    with pytest.raises(PythonFactorPolicyError, match=r"pct_change.*非负整数"):
        RestrictedPythonRunner(timeout_seconds=15).execute(
            parameterized_change, {"close": panel["close"]}, {"lag": -1},
        )


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

    incomplete = {key: value for key, value in artifact.items() if key != "source_sha256"}
    with pytest.raises(PythonFactorPolicyError, match="source_sha256"):
        execute_python_factor_artifact(tmp_path, incomplete, {"close": _panel(days=20)["close"]})


def test_registered_membership_is_not_claimed_as_runtime_compatible():
    bundle = ResearchDataBundle.from_legacy_panel(_panel(days=20, symbols=3))
    bundle.membership = pd.DataFrame(
        True, index=bundle.signal["close"].index, columns=bundle.signal["close"].columns,
    )
    _features, catalog = registered_features(bundle)
    descriptor = next(item for item in catalog if item.name == "membership")
    assert descriptor.runtime_compatible is False


def test_python_decision_refuses_missing_membership_instead_of_using_all_true(tmp_path):
    _config(tmp_path)
    source = (
        "def compute(features, params):\n"
        "    return features['membership']\n"
    )
    artifact = write_python_factor_artifact(
        tmp_path, source=source, params={}, manifest={"required_features": ["membership"]},
    )
    from quantmaster.decision.hybrid import _python_component

    with pytest.raises(ValueError, match="缺少 PIT membership"):
        _python_component(
            {"close": _panel(days=20, symbols=3)["close"]},
            {"spec": {"required_features": ["membership"], "artifact": artifact}},
        )


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


def test_python_miner_supplies_declared_warmup_history_but_scores_only_sealed_rows():
    panel = _panel(days=1100, symbols=6)
    source = "def compute(features, params):\n    return features['close'].pct_change(5)\n"
    split = sealed_three_way_split(panel["close"].index, purge_gap=7)

    class _CaptureRunner:
        def __init__(self):
            self.windows = []

        def execute(self, _source, features, _params):
            self.windows.append((features["close"].index[0], features["close"].index[-1]))
            return features["close"].pct_change(5)

    runner = _CaptureRunner()
    client = _SequenceLLMClient([{"candidates": [{
        "name": "五日动量", "hypothesis": "短期趋势延续", "objective": "稳定 RankIC",
        "required_features": ["close"], "warmup": 20, "parameters": [], "code": source,
    }]}])
    report = PythonFactorMiner(client=client, runner=runner).mine_report(
        {"close": panel["close"]}, [{
            "name": "close", "group": "price_volume_v2", "description": "收盘价",
            "pit_grade": "derived", "coverage": 1.0, "available": True,
        }], rounds=1, candidate_limit=1, finalists=1,
    )
    valid_start = pd.Timestamp(split["valid"]["start"])
    test_start = pd.Timestamp(split["test"]["start"])
    valid_end = pd.Timestamp(split["valid"]["end"])
    test_end = pd.Timestamp(split["test"]["end"])
    assert any(first < valid_start and last == valid_end for first, last in runner.windows)
    assert any(first < test_start and last == test_end for first, last in runner.windows)
    assert report.finalists[0].test_metrics["days"] > 200


def test_python_miner_excludes_candidates_that_fail_sealed_test():
    panel = _panel(days=1100, symbols=6)
    source = "def compute(features, params):\n    return features['close'].pct_change(5)\n"
    split = sealed_three_way_split(panel["close"].index, purge_gap=7)
    test_end = pd.Timestamp(split["test"]["end"])

    class _FailOnTestRunner:
        def execute(self, _source, features, _params):
            if pd.Timestamp(features["close"].index.max()) == test_end:
                raise PythonFactorPolicyError("sealed TEST 不可执行")
            return features["close"].pct_change(5)

    client = _SequenceLLMClient([{"candidates": [{
        "name": "五日动量", "hypothesis": "短期趋势延续", "objective": "稳定 RankIC",
        "required_features": ["close"], "warmup": 20, "parameters": [], "code": source,
    }]}])
    report = PythonFactorMiner(client=client, runner=_FailOnTestRunner()).mine_report(
        {"close": panel["close"]}, [{
            "name": "close", "group": "price_volume_v2", "description": "收盘价",
            "pit_grade": "derived", "coverage": 1.0, "available": True,
        }], rounds=1, candidate_limit=1, finalists=1,
    )
    assert report.finalists == []
    assert report.candidates[0].status == "test_failed"
    assert any(item["code"] == "sealed_test_failed" for item in report.warnings)


def test_python_miner_rejects_malformed_llm_fields_without_aborting_round():
    panel = _panel(days=1100, symbols=6)
    source = "def compute(features, params):\n    return features['close'].pct_change(5)\n"
    source_two = "def compute(features, params):\n    return features['close'].pct_change(6)\n"
    client = _SequenceLLMClient([{"candidates": [
        {"name": "坏候选", "required_features": "close", "warmup": "not-a-number", "code": source},
        {"name": "好候选", "required_features": ["close"], "warmup": 20, "code": source_two},
    ]}])
    report = PythonFactorMiner(client=client).mine_report(
        {"close": panel["close"]}, [{
            "name": "close", "group": "price_volume_v2", "description": "收盘价",
            "pit_grade": "derived", "coverage": 1.0, "available": True,
        }], rounds=1, candidate_limit=2, finalists=1,
    )
    assert report.candidates[0].status == "rejected"
    assert "候选字段无效" in report.candidates[0].error
    assert report.finalists and report.finalists[0].name == "好候选"


def test_python_mining_api_is_opt_in_and_exposes_preview(tmp_path, monkeypatch):
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
        assert disabled.status_code == 409
        blockers = disabled.json()["error"]["context"]["preflight"]["blockers"]
        assert any(
            item.get("context", {}).get("dependency") == "ai_python_mining"
            for item in blockers
        )
        cfg.lab.ai_python_mining_enabled = True
        monkeypatch.setattr(get_lab_service(), "preflight", lambda *_args, **_kwargs: {
            "runnable": True, "state": "ready", "resource_class": "external",
            "blockers": [], "warnings": [], "dataset": {},
        })
        queued = client.post("/api/v1/lab/jobs", json={
            "kind": "discover_python", "params": {
                "universe": "demo", "start": "2018-01-01", "end": "2026-01-01",
            },
        })
        assert queued.status_code == 202
        runs = client.get("/api/v1/lab/mining/runs").json()["items"]
        assert runs and runs[0]["job_id"] == queued.json()["id"]
