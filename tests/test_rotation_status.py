"""Industry/concept status, taxonomy, and temporal governance contracts."""

from types import SimpleNamespace

import pytest

from quantmaster.data.resilience import (
    ProviderCapabilityMissing,
    ProviderContractChanged,
    classify_provider_failure,
)
from quantmaster.rotation.contracts import RotationJobSpec
from quantmaster.rotation.status import (
    DataStatus,
    canonical_provider_status,
    data_status_payload,
    provider_status_payload,
    purpose_contract,
    taxonomy_identity,
)
from quantmaster.rotation.taxonomy import strict_l1_groups


def test_local_complete_remains_healthy_when_provider_is_rate_limited():
    data = data_status_payload(
        quality={"status": "complete", "eligible_count": 31, "expected_count": 31,
                 "coverage": 1.0},
        sources=["local:rotation_cache"],
        as_of="2026-08-12",
        expected_as_of="2026-08-12",
    )
    providers = provider_status_payload({
        "tushare:dc-concept": {
            "failure_class": "rate_limit", "retry_after": 100,
            "last_failure": 90, "suppressed": 12,
        },
    })

    assert data["state"] == "complete"
    assert data["resolution"] == DataStatus.LOCAL_HIT.value
    assert providers[0]["state"] == "rate_limited"
    assert providers[0]["suppressed_count"] == 12


def test_current_analysis_can_accept_a_dated_local_snapshot_without_provider_failure():
    data = data_status_payload(
        quality={"status": "stale", "eligible_count": 31, "expected_count": 31},
        sources=["local:rotation_cache"], as_of="2026-08-11",
        expected_as_of="2026-08-12", purpose="current_analysis",
    )

    assert data["state"] == "complete"
    assert data["resolution"] == "local_stale_accepted"
    assert data["freshness"]["state"] == "stale_accepted"


def test_partial_and_unavailable_data_statuses_preserve_exact_counts():
    partial = data_status_payload(
        quality={
            "status": "partial", "eligible_count": 75, "expected_count": 100,
            "coverage": .75, "missing_partitions": ["BK0003", "BK0004"],
        },
        sources=["local:rotation_cache"], as_of="2026-08-12",
    )
    unavailable = data_status_payload(
        quality={"status": "unavailable", "eligible_count": 0, "expected_count": 100},
        sources=[], as_of="",
    )

    assert partial["resolution"] == "partial_coverage"
    assert partial["coverage"] == {
        "complete": 75, "total": 100, "ratio": .75,
        "missing_partitions": ["BK0003", "BK0004"],
    }
    assert unavailable["state"] == "unavailable"


@pytest.mark.parametrize(
    ("error", "failure", "public"),
    [
        (RuntimeError("TOKEN无效"), "authentication", "auth_invalid"),
        (RuntimeError("permission denied"), "permission", "permission_missing"),
        (AttributeError("SDK has no attribute dc_member"), "capability_missing",
         "capability_missing"),
        (ProviderCapabilityMissing("provider does not support dc_member"),
         "capability_missing", "capability_missing"),
        (ProviderContractChanged("missing columns: code"), "contract_changed",
         "contract_changed"),
        (RuntimeError("HTTP 503 upstream"), "upstream_5xx", "5xx"),
        (RuntimeError("getaddrinfo failed"), "transient_network", "network"),
    ],
)
def test_provider_failure_taxonomy_is_canonical(error, failure, public):
    assert classify_provider_failure(error) == failure
    assert canonical_provider_status(failure) == public


def test_taxonomy_names_never_promote_an_unresolved_mapping_to_sw2021():
    unresolved = strict_l1_groups(
        {"600000.SH": "银行", "000001.SZ": "电子"}, taxonomy_id="eastmoney:industry:live",
    )
    verified = strict_l1_groups(
        {"600000.SH": "银行"}, taxonomy_id="sws:industry:2021",
    )

    assert all(not node["members"] for node in unresolved.values())
    assert verified["801780.SI"]["members"] == ["600000.SH"]
    assert taxonomy_identity("eastmoney-concept", kind="concept")["temporal_mode"] == "current_only"


def test_historical_and_formal_purposes_require_pit_membership():
    replay = purpose_contract("historical_replay", as_of="2024-01-02")
    formal = purpose_contract("formal_research", as_of="2024-01-02")

    assert replay["requires_effective_membership"] is True
    assert replay["accepts_current_only_taxonomy"] is False
    assert formal["requires_complete_denominator"] is True

    with pytest.raises(ValueError, match="knowledge_cutoff"):
        RotationJobSpec(
            purpose="historical_replay", as_of="2024-01-02",
            taxonomy_id="sws:industry:2021",
        )
    spec = RotationJobSpec(
        purpose="historical_replay", as_of="2024-01-02",
        knowledge_cutoff="2024-01-03T00:00:00+08:00",
        taxonomy_id="sws:industry:2021",
    )
    assert spec.taxonomy_id == "sws:industry:2021"


def test_http_429_maps_to_rate_limited():
    error = RuntimeError("limited")
    error.response = SimpleNamespace(status_code=429, headers={"Retry-After": "60"})
    failure = classify_provider_failure(error)
    assert failure == "rate_limit"
    assert canonical_provider_status(failure) == "rate_limited"


def test_capability_reason_is_exposed_without_changing_public_state():
    providers = provider_status_payload({
        "tushare:dc_member": {
            "failure_class": "capability_missing",
            "diagnostic_code": "capability_missing:sdk_method_missing",
            "state": "disabled",
        },
    })

    assert providers[0]["state"] == "capability_missing"
    assert providers[0]["capability_reason"] == "sdk_method_missing"
