"""牛熊/趋势状态与 1-7 日选股决策。"""

import json

import numpy as np
import pandas as pd
import pytest

from quantmaster.decision import (
    HybridDecisionStrategy,
    adaptive_rule_score_panel,
    hybrid_daily_selection,
    resolve_policy,
)
from quantmaster.decision.storage import DecisionStore
from quantmaster.market import analyze_bars, analyze_market, analyze_sectors


def test_uptrend_regime_has_macd_and_probabilistic_future():
    dates = pd.bdate_range("2024-01-02", periods=120)
    close = pd.Series(
        100 * np.exp(np.linspace(0, 0.35, len(dates)) + 0.01 * np.sin(np.arange(len(dates)))),
        index=dates,
    )
    bars = pd.DataFrame(
        {
            "close": close,
            "volume": 1e6 * (1 + 0.1 * np.sin(np.arange(len(dates)) / 3)),
            "amount": close * 1e6,
        }
    )
    report = analyze_bars(bars)
    assert report["current"]["state"] in {"up", "strong_up"}
    assert report["current"]["macd"] is not None
    assert [row["horizon_days"] for row in report["future"]] == [1, 3, 5, 7, 10, 20, 30]
    assert all(0 <= row["probability_up"] <= 1 for row in report["future"])
    validation = {row["horizon_days"]: row for row in report["forecast_validation"]}
    assert all(validation[horizon]["samples"] > 80 for horizon in (1, 3, 5, 7))
    assert all(row["samples"] > 0 for row in report["forecast_validation"])
    assert all(0 <= row["direction_accuracy"] <= 1 for row in report["forecast_validation"])
    assert all(0 <= row["brier_score"] <= 1 for row in report["forecast_validation"])
    assert "确定" in report["forecast_note"]


def test_market_and_sector_views(panel):
    report = analyze_market(panel)
    assert 0 <= report["current"]["bull_score"] <= 100
    assert 0 <= report["current"]["advance_ratio"] <= 1
    assert 0 <= report["current"]["rsi_14"] <= 100
    symbols = list(panel["close"].columns)
    mapping = {symbol: "行业A" if i < 4 else "行业B" for i, symbol in enumerate(symbols)}
    sectors = analyze_sectors(panel, mapping)
    assert set(sectors["sector"]) == {"行业A", "行业B"}
    assert sectors["bull_score"].between(0, 100).all()
    assert sectors["rsi_14"].between(0, 100).all()
    assert sectors["rsi_history"].map(len).min() > 0


def test_hybrid_adaptive_scores_have_no_forward_dependency(panel):
    cutoff = panel["close"].index[110]
    original = adaptive_rule_score_panel(panel, horizon=5).loc[cutoff]
    changed = {name: values.copy() for name, values in panel.items()}
    for values in changed.values():
        values.loc[values.index > cutoff] *= 25
    mutated = adaptive_rule_score_panel(changed, horizon=5).loc[cutoff]
    pd.testing.assert_series_equal(original, mutated)


def test_hybrid_profiles_snapshot_and_storage_are_reproducible(panel, tmp_path):
    from quantmaster.lab.store import LabStore

    lab = LabStore(tmp_path / "lab.sqlite")
    symbols = list(panel["close"].columns)
    industries = {symbol: f"行业{index % 3}" for index, symbol in enumerate(symbols)}
    risk_policy = resolve_policy(
        "demo",
        3,
        "risk_adjusted",
        symbols=symbols,
        store=lab,
    )
    stable_policy = resolve_policy(
        "demo",
        3,
        "stable",
        symbols=symbols,
        store=lab,
    )
    risk = hybrid_daily_selection(
        panel,
        top_n=4,
        horizon=3,
        profile="risk_adjusted",
        universe="demo",
        industry_map=industries,
        policy_snapshot=risk_policy,
    )
    stable = hybrid_daily_selection(
        panel,
        top_n=4,
        horizon=3,
        profile="stable",
        universe="demo",
        industry_map=industries,
        policy_snapshot=stable_policy,
    )
    assert risk["model_version"].startswith("hybrid-v3:risk_adjusted:")
    assert stable["model_version"].startswith("hybrid-v3:stable:")
    assert stable["recommended_exposure"] <= risk["recommended_exposure"]
    assert all(0 <= item["probability_up"] <= 1 for item in risk["picks"])
    assert all("component_scores" in item for item in risk["picks"])

    store = DecisionStore(tmp_path / "hybrid.sqlite")
    store.save(risk, "demo", panel=panel)
    store.save(stable, "demo", panel=panel)
    assert len(store.history("demo")) == 2
    assert len(store.history("demo", profile="stable")) == 1


def test_decision_store_refuses_conflicting_same_identity_rerun(tmp_path):
    market_panel = {
        "close": pd.DataFrame(
            [[10.0]], index=pd.to_datetime(["2026-08-07"]), columns=["600000.SH"],
        ),
    }
    report = {
        "signal_date": "2026-08-07",
        "holding_horizon_days": 3,
        "profile": "risk_adjusted",
        "policy_hash": "policy-v1",
        "model_version": "hybrid-v3:test",
        "position_state": "invested",
        "picks": [{"symbol": "600000.SH", "rank": 1}],
        "universe_evidence": {"content_hash": "universe-a"},
        "industry_evidence": {"content_hash": "industry-a"},
        "data_quality": {"status": "verified"},
        "market_provenance": [{"content_hash": "bars-a"}],
    }
    store = DecisionStore(tmp_path / "append-only.sqlite")
    store.save(report, "demo", panel=market_panel)

    revised = {**report, "picks": [{"symbol": "000001.SZ", "rank": 1}]}
    with pytest.raises(RuntimeError, match="拒绝覆盖"):
        store.save(revised, "demo", panel=market_panel)

    assert store.history("demo")[0]["picks"] == report["picks"]


def test_decision_market_input_is_frozen_and_tamper_evident(panel, tmp_path):
    store = DecisionStore(tmp_path / "decisions.sqlite")
    original = {name: frame.copy() for name, frame in panel.items()}
    report = {
        "signal_date": "2026-08-07",
        "holding_horizon_days": 3,
        "profile": "risk_adjusted",
        "policy_hash": "frozen-input-v1",
        "model_version": "hybrid-v3:test",
        "position_state": "flat",
        "picks": [],
    }
    store.save(report, "demo", panel=panel)
    evidence = report["market_input_evidence"]

    for frame in panel.values():
        frame.iloc[:, :] = -999.0
    restored = store.load_market_input(evidence)
    for name, frame in original.items():
        pd.testing.assert_frame_equal(restored[name], frame, check_freq=False)

    artifact = store.evidence_root / evidence["content_hash"] / "00.parquet"
    artifact.write_bytes(artifact.read_bytes() + b"tampered")
    with pytest.raises(RuntimeError, match="已改写"):
        store.load_market_input(evidence)
    assert store.history("demo")[0]["signal_date"] == "2026-08-07"


def test_decision_history_rejects_malformed_current_row_without_legacy_fallback(tmp_path):
    panel = {
        "close": pd.DataFrame(
            [[10.0]],
            index=pd.to_datetime(["2026-08-07"]),
            columns=["600000.SH"],
        ),
    }
    report = {
        "signal_date": "2026-08-07",
        "holding_horizon_days": 3,
        "profile": "risk_adjusted",
        "policy_hash": "policy-v1",
        "model_version": "hybrid-v3:test",
        "position_state": "flat",
        "picks": [],
    }
    store = DecisionStore(tmp_path / "decision-payload.sqlite")
    store.save(report, "demo", panel=panel)
    with store._conn() as connection:
        connection.execute(
            "UPDATE selection_snapshots SET payload=?",
            (json.dumps({"picks": [{"symbol": "600000.SH"}]}),),
        )

    with pytest.raises(RuntimeError, match="一次性迁移") as raised:
        store.history("demo")
    assert raised.value.diagnostic_code == "decision_payload_migration_required"


def test_decision_history_rejects_invalid_json_as_current_damage(tmp_path):
    store = DecisionStore(tmp_path / "minimal-history.sqlite")
    with store._conn() as connection:
        connection.execute(
            "INSERT INTO selection_snapshots "
            "(signal_date,universe,horizon,profile,policy_hash,model_version,payload,created_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            ("2026-08-07", "demo", 3, "risk_adjusted", "legacy", "hybrid-v3:test",
             "{broken", 0.0),
        )

    with pytest.raises(RuntimeError, match="不会尝试旧格式") as raised:
        store.history("demo")
    assert raised.value.diagnostic_code == "decision_payload_invalid_json"


def test_python_component_exposes_external_feature_input_for_freezing(panel, monkeypatch):
    from quantmaster.decision.hybrid import _python_component

    sentiment = pd.DataFrame(
        0.25,
        index=panel["close"].index,
        columns=panel["close"].columns,
    )
    monkeypatch.setattr(
        "quantmaster.ai.sentiment.quality_sentiment_panel",
        lambda *_args, **_kwargs: sentiment.copy(),
    )
    monkeypatch.setattr(
        "quantmaster.factors.python_artifact.execute_python_factor_artifact",
        lambda _root, _artifact, features: features["news_sentiment"],
    )
    evidence: dict[str, pd.DataFrame] = {}

    _python_component(
        panel,
        {
            "spec": {
                "kind": "python",
                "required_features": ["news_sentiment"],
                "artifact": {"content_hash": "test"},
            },
        },
        evidence_sink=evidence,
    )

    pd.testing.assert_frame_equal(evidence["feature::news_sentiment"], sentiment)


def test_hybrid_strategy_uses_profile_risk_limits(panel, tmp_path):
    from quantmaster.lab.store import LabStore

    policy = resolve_policy(
        "demo",
        5,
        "stable",
        symbols=list(panel["close"].columns),
        store=LabStore(tmp_path / "lab.sqlite"),
    )
    weights = HybridDecisionStrategy(
        top_n=4,
        holding_days=5,
        profile="stable",
        universe="demo",
        policy_snapshot=policy,
    ).target_weights(panel)
    signals = weights.dropna(how="all")
    assert len(signals) > 10
    assert (signals.sum(axis=1) <= 0.65 + 1e-9).all()


def test_legacy_hybrid_snapshot_requires_explicit_migration(panel, tmp_path):
    import json

    from quantmaster.backtest.spec import content_hash
    from quantmaster.lab.store import LabStore

    policy = resolve_policy(
        "demo", 3, "risk_adjusted", symbols=list(panel["close"].columns),
        store=LabStore(tmp_path / "legacy-lab.sqlite"),
    )
    legacy = json.loads(json.dumps(policy))
    legacy.pop("position_control")
    legacy["schema_version"] = 2
    legacy["engine_version"] = "hybrid-v2"
    legacy.pop("policy_hash")
    legacy.pop("model_version")
    legacy["policy_hash"] = content_hash(legacy)
    legacy["model_version"] = f"hybrid-v2:risk_adjusted:{legacy['policy_hash'][:12]}"
    strategy = HybridDecisionStrategy(
        top_n=4,
        holding_days=3,
        profile="risk_adjusted",
        universe="demo",
        policy_snapshot=legacy,
        cap_weight=0.25,
    )
    with pytest.raises(RuntimeError, match="一次性迁移") as raised:
        strategy.target_weights(panel)
    assert raised.value.diagnostic_code == "decision_policy_version_unsupported"


def test_profile_constraints_survive_missing_factor_component():
    class OnlyMlStore:
        @staticmethod
        def active_deployments():
            return [
                {
                    "id": "deployment",
                    "version_id": "learned",
                    "universe": "demo",
                    "horizon": 3,
                    "profile": "all",
                    "scope": "exact",
                    "role": "ml",
                    "created_at": "2026-07-27T00:00:00+00:00",
                }
            ]

        @staticmethod
        def version(version_id):
            return {
                "id": version_id,
                "name": "ML Champion",
                "status": "approved",
                "content_hash": "hash",
                "validation": {},
                "spec": {"kind": "learned", "model": {"manifest": "unused.json"}},
            }

    risk = resolve_policy("demo", 3, "risk_adjusted", store=OnlyMlStore())
    short = resolve_policy("demo", 3, "short_term", store=OnlyMlStore())
    assert risk["components"][1]["weight"] <= 0.30
    assert short["components"][1]["weight"] <= 0.45
    assert risk["components"][0]["weight"] >= 0.35
    assert short["components"][0]["weight"] >= 0.25
