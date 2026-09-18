"""Historical facts stay readable; only current evidence can authorize decisions."""

from copy import deepcopy

import pytest

from quantmaster.decision.hybrid import hybrid_score_bundle, resolve_policy
from quantmaster.lab.models import FactorSpec
from quantmaster.lab.store import LabStore
from quantmaster.lab.strategy import EXECUTION_CONTRACT


def _report(contract=EXECUTION_CONTRACT):
    return {
        "gates": {"passed": True, "hard_failures": [], "soft_failures": []},
        "horizons": {"3": {
            "execution": {} if contract is None else {"execution_contract": contract},
            "gates": {"passed": True},
        }},
    }


def _historical_factor(tmp_path, monkeypatch, contract):
    store = LabStore(tmp_path / "lab.sqlite")
    monkeypatch.setattr("quantmaster.lab.store.utc_now", lambda: "2026-08-08T01:00:00+00:00")
    _, version, _ = store.create_factor(FactorSpec(
        slug="historical", name="Historical factor", expression="rank(close)",
    ))
    report = _report(contract)
    store.save_validation(version["id"], "original-dataset", report)
    # Record a deployment under its original executor contract, then upgrade the reader.
    with monkeypatch.context() as old_engine:
        old_engine.setattr("quantmaster.execution_evidence.EXECUTION_CONTRACT", contract)
        store.approve(version["id"], actor="test")
        store.deploy(version["id"], universe="demo", horizon=3, actor="test")
    return store, version["id"], report


@pytest.mark.parametrize("contract", [None, "obsolete_open_v0"])
def test_stale_factor_cannot_be_approved_overridden_or_redeployed(
    tmp_path, monkeypatch, contract,
):
    store, version_id, report = _historical_factor(tmp_path, monkeypatch, contract)
    before = store.version(version_id)
    with pytest.raises(ValueError, match="LAB_EXECUTION_REVALIDATION_REQUIRED"):
        store.approve(version_id, actor="test", reason="manual override")
    with pytest.raises(ValueError, match="LAB_EXECUTION_REVALIDATION_REQUIRED"):
        store.deploy(version_id, universe="demo", horizon=3, actor="test")
    assert store.version(version_id) == before
    assert store.version(version_id)["validation"] == report


def test_current_other_horizon_does_not_certify_stale_deployment(tmp_path):
    store = LabStore(tmp_path / "lab.sqlite")
    _, version, _ = store.create_factor(FactorSpec(
        slug="mixed", name="Mixed evidence", expression="rank(close)",
    ))
    report = _report()
    report["horizons"]["5"] = _report("obsolete")["horizons"]["3"]
    store.save_validation(version["id"], "dataset", report)
    store.approve(version["id"], actor="test")
    with pytest.raises(ValueError, match="LAB_EXECUTION_REVALIDATION_REQUIRED"):
        store.deploy(version["id"], universe="demo", horizon=5, actor="test")
    assert store.deploy(version["id"], universe="demo", horizon=3, actor="test")


@pytest.mark.parametrize("contract", [None, "obsolete_open_v0", EXECUTION_CONTRACT])
@pytest.mark.parametrize("mode", ["live", "retrospective", "historical_replay"])
def test_factor_decision_boundaries_require_current_exact_evidence(
    tmp_path, monkeypatch, contract, mode,
):
    store, _, report = _historical_factor(tmp_path, monkeypatch, contract)
    history = store.deployments_as_of("2026-08-08")
    assert history[0]["version_snapshot"]["validation"] == report
    kwargs = dict(store=store, mode=mode, as_of="2026-08-08")
    if contract != EXECUTION_CONTRACT and mode == "historical_replay":
        with pytest.raises(RuntimeError, match="LAB_EXECUTION_REVALIDATION_REQUIRED"):
            resolve_policy("demo", 3, **kwargs)
    else:
        policy = resolve_policy("demo", 3, **kwargs)
        factors = [item for item in policy["components"] if item["role"] == "factor"]
        assert bool(factors) == (contract == EXECUTION_CONTRACT)
        assert any("LAB_EXECUTION_REVALIDATION_REQUIRED" in item for item in policy["warnings"]) == (
            contract != EXECUTION_CONTRACT
        )
    assert store.deployments_as_of("2026-08-08") == history


def test_factor_revalidation_cannot_recertify_an_old_frozen_snapshot(tmp_path, monkeypatch):
    store, version_id, _ = _historical_factor(tmp_path, monkeypatch, "obsolete_open_v0")
    history = store.deployments_as_of("2026-08-08")
    monkeypatch.setattr("quantmaster.lab.store.utc_now", lambda: "2026-08-09T01:00:00+00:00")
    store.save_validation(version_id, "recomputed-local-dataset", _report())
    assert len(resolve_policy("demo", 3, store=store)["components"]) == 2
    with pytest.raises(RuntimeError, match="LAB_EXECUTION_REVALIDATION_REQUIRED"):
        resolve_policy("demo", 3, store=store, mode="historical_replay", as_of="2026-08-08")
    assert store.deployments_as_of("2026-08-08") == history


@pytest.mark.parametrize("contract,passed", [
    (None, True), ("obsolete_open_v0", True), (EXECUTION_CONTRACT, False),
    (EXECUTION_CONTRACT, True),
])
@pytest.mark.parametrize("mode", ["live", "retrospective", "historical_replay"])
def test_champion_decisions_require_current_and_passed_sealed_evidence(contract, passed, mode):
    version = {"id": "factor", "spec": {"kind": "expression", "expression": "rank(close)"}}
    champion = {
        "id": "champion", "name": "Recorded Champion",
        "components": [{"version_id": "factor", "weight": 1.0}],
        "component_versions": {"factor": version},
        "sealed_evidence": {
            "metrics": {} if contract is None else {"execution_contract": contract},
            "gates": {"passed": passed},
        },
    }
    original = deepcopy(champion)

    class Store:
        def active_deployments(self):
            return []

        def deployments_as_of(self, as_of):
            return []

        def strategies(self, **kwargs):
            return [champion]

        def champion_strategies_as_of(self, as_of, **kwargs):
            return [champion]

        def version(self, version_id):
            return version

    valid = contract == EXECUTION_CONTRACT and passed
    if not valid and mode == "historical_replay":
        with pytest.raises(RuntimeError, match="LAB_EXECUTION_REVALIDATION_REQUIRED"):
            resolve_policy("demo", 3, store=Store(), mode=mode, as_of="2026-08-08")
    else:
        policy = resolve_policy("demo", 3, store=Store(), mode=mode, as_of="2026-08-08")
        assert (len(policy["components"]) == 2) is valid
        assert any("LAB_EXECUTION_REVALIDATION_REQUIRED" in item for item in policy["warnings"]) is not valid
    assert champion == original


@pytest.mark.parametrize("composite", [False, True])
def test_supplied_policy_requires_evidence_only_when_computing_new_scores(
    tmp_path, monkeypatch, panel, composite,
):
    store, _, _ = _historical_factor(tmp_path, monkeypatch, EXECUTION_CONTRACT)
    policy = resolve_policy("demo", 3, store=store)
    component = policy["components"][1]
    if composite:
        component["kind"] = "composite"
        component["spec"] = {
            "kind": "composite", "components": [{
                "version_id": "factor", "spec": component["spec"], "weight": 1.0,
            }],
        }
        component["validation"] = {
            "metrics": {"execution_contract": EXECUTION_CONTRACT}, "gates": {"passed": True},
        }
    bundle = hybrid_score_bundle(panel, horizon=3, policy_snapshot=policy)
    assert "factor" in bundle["components"]
    component["validation"] = {"gates": {"passed": True}}
    original = deepcopy(policy)
    # Schema validation remains suitable for inspection of an immutable recorded fact.
    from quantmaster.decision.schema import validate_current_policy

    validate_current_policy(policy)
    with pytest.raises(RuntimeError, match="LAB_EXECUTION_REVALIDATION_REQUIRED"):
        hybrid_score_bundle(panel, horizon=3, policy_snapshot=policy)
    assert policy == original


@pytest.mark.parametrize("raw_available", [False, True])
def test_python_revalidation_uses_raw_execution_prices(tmp_path, monkeypatch, panel, raw_available):
    from quantmaster.lab.service import LabService
    from quantmaster.lab.strategy import execute_daily_targets

    store = LabStore(tmp_path / "lab.sqlite")
    service = LabService(store)
    _, version, _ = store.create_factor(FactorSpec(
        slug="python_evidence", name="Python evidence", kind="python", artifact={"manifest": "test.json"},
    ))
    features = {"close": panel["close"], "open": panel["open"] / 2}
    if raw_available:
        features.update({"raw_open": panel["open"], "up_limit": panel["open"]})
    snapshot = store.save_snapshot({"snapshot_hash": "local", "research_quality": "production"})
    monkeypatch.setattr(service, "_python_mining_context", lambda *args: (features, [], snapshot, "bundle"))
    monkeypatch.setattr(service, "_version_values", lambda *args: (panel["close"], {}))

    def validate(values, close, **kwargs):
        assert close is features["close"]
        assert kwargs["open_prices"] is features["raw_open"]
        execution = execute_daily_targets(
            values, {**kwargs["panel"], "open": kwargs["open_prices"]}, horizon=3,
        )
        assert execution["weights"].sum().sum() == 0  # raw opens at the buy limit
        report = _report()
        report["horizons"]["3"]["execution"] = execution["metrics"]
        return report

    monkeypatch.setattr("quantmaster.lab.validation.validate_factor_values", validate)
    if not raw_available:
        with pytest.raises(ValueError, match="LAB_EXECUTION_REVALIDATION_REQUIRED"):
            service.validate_version(version["id"], universe="demo", start="2024-01-01", end="2025-01-01")
        assert store.version(version["id"])["validation"] is None
    else:
        result = service.validate_version(
            version["id"], universe="demo", start="2024-01-01", end="2025-01-01",
        )
        assert result["report"]["horizons"]["3"]["execution"]["execution_contract"] == EXECUTION_CONTRACT
