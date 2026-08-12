"""Strict contracts for current decision payloads and Hybrid policies."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

DECISION_PAYLOAD_SCHEMA_VERSION = 1
HYBRID_POLICY_SCHEMA_VERSION = 3
HYBRID_POLICY_ENGINE = "hybrid-v3-position-control"


class DecisionSchemaError(RuntimeError):
    """A stored or supplied decision does not satisfy the current contract."""

    def __init__(self, diagnostic_code: str, message: str):
        super().__init__(message)
        self.diagnostic_code = diagnostic_code


def validate_current_policy(snapshot: Mapping[str, Any]) -> None:
    """Reject non-current policies; historical conversion belongs to migration."""
    if not isinstance(snapshot, Mapping):
        raise DecisionSchemaError("decision_policy_not_object", "Hybrid policy 必须是对象")
    if snapshot.get("schema_version") != HYBRID_POLICY_SCHEMA_VERSION:
        raise DecisionSchemaError(
            "decision_policy_version_unsupported",
            "Hybrid policy 不是当前 schema 3；请先执行一次性迁移",
        )
    if snapshot.get("engine_version") != HYBRID_POLICY_ENGINE:
        raise DecisionSchemaError(
            "decision_policy_engine_unsupported",
            "Hybrid policy engine_version 不符合当前契约",
        )
    position_control = snapshot.get("position_control")
    if not isinstance(position_control, Mapping) or not position_control:
        raise DecisionSchemaError(
            "decision_policy_position_control_missing",
            "Hybrid policy 缺少当前 position_control 契约",
        )


def validate_current_payload(
    payload: Mapping[str, Any],
    *,
    row_identity: Mapping[str, Any] | None = None,
) -> None:
    """Validate a current stored decision without repairing or defaulting it."""
    if not isinstance(payload, Mapping):
        raise DecisionSchemaError("decision_payload_not_object", "决策 payload 必须是对象")
    if payload.get("decision_schema_version") != DECISION_PAYLOAD_SCHEMA_VERSION:
        raise DecisionSchemaError(
            "decision_payload_migration_required",
            "决策 payload 不是当前 schema 1；请先执行一次性迁移",
        )
    _validate_payload_fields(payload)
    if payload["holding_horizon_days"] not in {1, 3, 5, 7, 10, 20, 30}:
        raise DecisionSchemaError(
            "decision_payload_horizon_invalid", "决策 payload 的 holding_horizon_days 无效",
        )
    snapshot = payload.get("model_snapshot")
    if snapshot is not None:
        validate_current_policy(snapshot)
    if row_identity:
        # Only columns proven to be historical facts are authoritative for old
        # rows.  profile/policy_hash were also used as legacy key sentinels;
        # migration may therefore leave their payload values empty.
        comparisons = {
            "signal_date": payload["signal_date"],
            "universe": payload["universe"],
            "horizon": payload["holding_horizon_days"],
            "model_version": payload.get("model_version"),
            "created_at": payload["created_at"],
        }
        mismatch = next(
            (field for field, actual in comparisons.items() if actual != row_identity.get(field)),
            None,
        )
        if mismatch:
            raise DecisionSchemaError(
                "decision_payload_row_identity_mismatch",
                f"决策 payload 与行列 {mismatch} 不一致",
            )


def _validate_payload_fields(payload: Mapping[str, Any]) -> None:
    required = {
        "signal_date": str,
        "universe": str,
        "holding_horizon_days": int,
        "created_at": str,
        "picks": list,
    }
    invalid = next(
        (
            field for field, expected in required.items()
            if not isinstance(payload.get(field), expected)
            or (expected is str and not payload.get(field))
        ),
        None,
    )
    if invalid:
        raise DecisionSchemaError(
            f"decision_payload_{invalid}_invalid",
            f"决策 payload 字段 {invalid} 缺失或类型错误",
        )
    optional_invalid = next(
        (
            field for field in ("profile", "policy_hash", "model_version")
            if payload.get(field) is not None and not isinstance(payload.get(field), str)
        ),
        None,
    )
    if optional_invalid:
        raise DecisionSchemaError(
            f"decision_payload_{optional_invalid}_invalid",
            f"决策 payload 字段 {optional_invalid} 类型错误",
        )
