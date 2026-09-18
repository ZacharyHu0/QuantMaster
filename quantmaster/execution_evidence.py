"""Shared execution evidence contract for Lab and decision consumers."""

from __future__ import annotations

from typing import Any

EXECUTION_CONTRACT = "self_financing_open_v1"


def execution_evidence_gate(metrics: dict[str, Any], gate: dict[str, Any]) -> dict[str, Any]:
    """Keep old evidence readable without granting it the current execution contract."""
    if metrics.get("execution_contract") == EXECUTION_CONTRACT:
        return dict(gate)
    reason = "LAB_EXECUTION_REVALIDATION_REQUIRED: 执行证据已过期，需要重新运行研究"
    failures = list(gate.get("failures") or [])
    if reason not in failures:
        failures.append(reason)
    return {
        **gate, "passed": False, "override_allowed": False,
        "revalidation_required": True, "failures": failures,
    }


def factor_execution_gate(report: dict[str, Any], horizon: int | None = None) -> dict[str, Any]:
    """Certify only recorded execution evidence, without modifying the report.

    Approval needs a current horizon; deployment needs the selected exact horizon.
    Neither a report-wide pass nor another horizon can supply its contract.
    """
    horizons = report.get("horizons") or {}
    evidence = horizons.get(str(horizon), {}) if horizon is not None else next(
        (item for item in horizons.values()
         if (item.get("execution") or {}).get("execution_contract") == EXECUTION_CONTRACT),
        {},
    )
    gate = dict((evidence if horizon is not None else report).get("gates") or {})
    if (report.get("gates") or {}).get("hard_failures"):
        gate["passed"] = False
    return execution_evidence_gate(evidence.get("execution") or {}, gate)


