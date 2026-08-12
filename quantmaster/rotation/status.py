"""Public status and temporal contracts for industry/concept data."""

from __future__ import annotations

from enum import StrEnum
from typing import Any


class DataStatus(StrEnum):
    LOCAL_HIT = "local_hit"
    LOCAL_STALE_ACCEPTED = "local_stale_accepted"
    LOCAL_PLUS_REMOTE_COMPLETE = "local_plus_remote_complete"
    PARTIAL_COVERAGE = "partial_coverage"
    REMOTE_COMPLETE = "remote_complete"
    UNAVAILABLE = "unavailable"


class ProviderStatus(StrEnum):
    AVAILABLE = "available"
    RATE_LIMITED = "rate_limited"
    AUTH_INVALID = "auth_invalid"
    PERMISSION_MISSING = "permission_missing"
    CAPABILITY_MISSING = "capability_missing"
    NETWORK = "network"
    UPSTREAM_5XX = "5xx"
    CONTRACT_CHANGED = "contract_changed"


class DataPurpose(StrEnum):
    DISPLAY = "display"
    CURRENT_ANALYSIS = "current_analysis"
    HISTORICAL_REPLAY = "historical_replay"
    FORMAL_RESEARCH = "formal_research"


CURRENT_ONLY_TAXONOMIES = frozenset({
    "eastmoney:industry:live",
    "eastmoney:concept:live",
    "ths:concept:live",
})


def purpose_contract(purpose: str, *, as_of: str = "") -> dict[str, Any]:
    """Return the explicit freshness/temporal contract for one consumer."""

    selected = DataPurpose(purpose)
    historical = selected in {DataPurpose.HISTORICAL_REPLAY, DataPurpose.FORMAL_RESEARCH}
    return {
        "purpose": selected.value,
        "as_of": str(as_of or ""),
        "requires_effective_membership": historical,
        "requires_knowledge_cutoff": historical,
        "requires_complete_denominator": selected is DataPurpose.FORMAL_RESEARCH,
        "accepts_stale": selected is DataPurpose.DISPLAY,
        "accepts_current_only_taxonomy": not historical,
    }


def taxonomy_identity(source: str, *, kind: str) -> dict[str, str]:
    """Map only documented provider identities; never infer by matching names."""

    value = str(source or "").strip().casefold()
    identities = {
        "sw2021": ("sws:industry:2021", "申万", "2021", "historical_intervals"),
        "eastmoney-concept": (
            "eastmoney:concept:live", "东方财富", "live", "current_only",
        ),
        "tushare:dc-concept": (
            "eastmoney:concept:live", "东方财富", "live", "dated_snapshot",
        ),
        "ths:concept": ("ths:concept:live", "同花顺", "live", "current_only"),
        "free-stockdb:concept": (
            "stockdb:concept:declared", "StockDB", "declared", "dated_snapshot",
        ),
    }
    identity = identities.get(value)
    if identity is None:
        return {
            "taxonomy_id": f"unresolved:{kind}:{value or 'unknown'}",
            "authority": "unresolved",
            "version": "",
            "temporal_mode": "unresolved",
        }
    taxonomy_id, authority, version, temporal_mode = identity
    return {
        "taxonomy_id": taxonomy_id,
        "authority": authority,
        "version": version,
        "temporal_mode": temporal_mode,
    }


def canonical_provider_status(value: str) -> str:
    """Translate legacy provider failure classes to the public status enum."""

    raw = str(value or "").casefold()
    if raw in {"rate_limit", "rate_limited"}:
        return ProviderStatus.RATE_LIMITED.value
    if raw in {"authentication", "http_401_authentication", "auth_invalid"}:
        return ProviderStatus.AUTH_INVALID.value
    if raw in {"permission", "http_403_permission", "permission_missing"}:
        return ProviderStatus.PERMISSION_MISSING.value
    if raw == "capability_missing":
        return ProviderStatus.CAPABILITY_MISSING.value
    if raw in {"transient_network", "network"}:
        return ProviderStatus.NETWORK.value
    if raw in {"upstream_5xx", "5xx"} or (
        raw.startswith("http_") and raw.endswith("_upstream")
    ):
        return ProviderStatus.UPSTREAM_5XX.value
    if raw in {"contract_changed", "empty_response"}:
        return ProviderStatus.CONTRACT_CHANGED.value
    return ProviderStatus.AVAILABLE.value if not raw else ProviderStatus.NETWORK.value


def data_status_payload(
    *,
    quality: dict[str, Any],
    sources: list[str],
    as_of: str,
    expected_as_of: str = "",
    purpose: str = "display",
    remote_fills: int = 0,
    pending: dict[str, Any] | None = None,
    taxonomy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Project data usability independently from provider health."""

    raw = str(quality.get("status") or "cold")
    complete = int(quality.get("eligible_count") or 0)
    total = int(quality.get("expected_count") or 0)
    ratio = quality.get("coverage")
    local_hits = sum(
        source.startswith(("local", "research_lake", "free-stockdb"))
        for source in sources
    )
    if raw in {"cold", "empty", "corrupt", "unavailable"}:
        resolution = DataStatus.UNAVAILABLE
        state = "unavailable"
    elif raw in {"partial", "limited"}:
        resolution = DataStatus.PARTIAL_COVERAGE
        state = "partial_coverage"
    elif local_hits and remote_fills:
        resolution = DataStatus.LOCAL_PLUS_REMOTE_COMPLETE
        state = "complete"
    elif local_hits and expected_as_of and as_of and as_of < expected_as_of:
        resolution = DataStatus.LOCAL_STALE_ACCEPTED
        state = (
            "complete"
            if purpose in {
                DataPurpose.DISPLAY.value, DataPurpose.CURRENT_ANALYSIS.value,
            }
            else "partial_coverage"
        )
    elif local_hits:
        resolution = DataStatus.LOCAL_HIT
        state = "complete"
    else:
        resolution = DataStatus.REMOTE_COMPLETE
        state = "complete"
    return {
        "state": state,
        "resolution": resolution.value,
        "purpose": purpose,
        "as_of": str(as_of or ""),
        "expected_as_of": str(expected_as_of or ""),
        "freshness": {
            "state": (
                "stale_accepted"
                if resolution is DataStatus.LOCAL_STALE_ACCEPTED else
                "fresh" if state == "complete" else "unknown"
            ),
        },
        "coverage": {
            "complete": complete,
            "total": total,
            "ratio": ratio,
            "missing_partitions": list(quality.get("missing_partitions") or []),
        },
        "provenance": {
            "sources": list(sources),
            "local_hits": local_hits,
            "remote_fills": max(0, int(remote_fills)),
            "taxonomy": dict(taxonomy or {}),
        },
        "pending": dict(pending or {"total": 0, "completed": 0, "retryable": 0}),
        "affected_views": list(quality.get("affected_views") or []),
        "problems": list(quality.get("problem_ids") or []),
    }


def provider_status_payload(lanes: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Expose redacted capability health without changing data usability."""

    result = []
    for lane, raw in sorted(lanes.items()):
        failure = str(raw.get("failure_class") or "")
        status = canonical_provider_status(failure)
        diagnostic = str(raw.get("diagnostic_code") or failure)
        capability_reason = ""
        if status == ProviderStatus.CAPABILITY_MISSING.value:
            capability_reason = (
                diagnostic.partition(":")[2]
                or str(raw.get("capability_reason") or "provider_unsupported")
            )
        result.append({
            "lane": lane,
            "provider": lane.partition(":")[0],
            "capability": lane.partition(":")[2] or lane,
            "capability_reason": capability_reason,
            "state": status,
            "available": status == ProviderStatus.AVAILABLE.value,
            "retry_after_at": float(raw.get("retry_after_at") or 0),
            "next_probe_at": float(raw.get("next_probe_at") or 0),
            "last_success_at": float(raw.get("last_success") or 0),
            "last_failure_at": float(raw.get("last_failure") or 0),
            "diagnostic_code": diagnostic,
            "problem_id": f"provider:{lane}",
            "suppressed_count": int(raw.get("suppressed") or 0),
            "permanent": bool(raw.get("permanent")),
        })
    return result
