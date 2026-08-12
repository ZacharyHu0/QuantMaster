"""One-time migration of decision rows; runtime readers never infer versions."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from quantmaster.data.legacy_migration import MigrationRecord
from quantmaster.decision.schema import (
    DECISION_PAYLOAD_SCHEMA_VERSION,
    DecisionSchemaError,
    validate_current_payload,
    validate_current_policy,
)
from quantmaster.runtime.sqlite import connect_sqlite

_IDENTITY_FIELDS = {
    "signal_date": "signal_date",
    "universe": "universe",
    "holding_horizon_days": "horizon",
    "profile": "profile",
    "policy_hash": "policy_hash",
    "model_version": "model_version",
}
_CURRENT_FIELDS = {
    "decision_schema_version", "signal_date", "universe", "holding_horizon_days",
    "created_at", "profile", "profile_label", "policy_hash", "model_version",
    "policy_mode", "policy_effective_at", "generated_at", "market_regime",
    "market_base_exposure", "opportunity_scale", "recommended_exposure",
    "cash_weight", "qualified_count", "position_state", "position_reasons",
    "model_snapshot", "validation_summary", "shadow_model", "data_quality",
    "rule_weights", "warnings", "picks", "risk_note", "market_input_evidence",
    "universe_evidence", "industry_evidence", "market_provenance", "persistence",
}


def _iso_timestamp(value: Any) -> str:
    return datetime.fromtimestamp(float(value), tz=UTC).isoformat()


def _identity(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "signal_date": str(row["signal_date"]),
        "universe": str(row["universe"]),
        "horizon": int(row["horizon"]),
        "profile": str(row["profile"]) or None,
        "policy_hash": str(row["policy_hash"]) or None,
        "model_version": str(row["model_version"]) or None,
        "created_at": _iso_timestamp(row["created_at"]),
    }


def _record_key(identity: dict[str, Any]) -> str:
    return json.dumps(identity, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _outcome(
    status: str, code: str, detail: str, identity: dict[str, Any],
    *, payload: dict[str, Any] | None = None, unknown: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "status": status, "diagnostic_code": code, "detail": detail,
        "identity": identity, "payload": payload, "unknown": unknown or {},
    }


def _parse_old_payload(row: sqlite3.Row, identity: dict[str, Any]) -> tuple[dict, dict] | dict:
    try:
        raw = json.loads(str(row["payload"]))
    except (TypeError, ValueError, json.JSONDecodeError):
        return _outcome(
            "unclassified", "decision_payload_invalid_json",
            "payload 不是可解析 JSON，无法可靠区分历史格式与当前损坏", identity,
        )
    if not isinstance(raw, dict):
        return _outcome(
            "unclassified", "decision_payload_not_object", "payload 不是对象",
            identity, unknown={"original_payload": raw},
        )
    return raw, identity


def _identity_conflicts(raw: dict[str, Any], identity: dict[str, Any]) -> list[str]:
    conflicts: list[str] = []
    for payload_field, row_field in _IDENTITY_FIELDS.items():
        if payload_field not in raw or raw[payload_field] in (None, ""):
            continue
        payload_value = raw[payload_field]
        if payload_field == "holding_horizon_days":
            try:
                payload_value = int(payload_value)
            except (TypeError, ValueError):
                conflicts.append(payload_field)
                continue
        if payload_value != identity[row_field]:
            conflicts.append(payload_field)
    return conflicts


def _classify(row: sqlite3.Row) -> dict[str, Any]:
    identity = _identity(row)
    parsed = _parse_old_payload(row, identity)
    if isinstance(parsed, dict):
        return parsed
    raw, _identity_value = parsed
    if "decision_schema_version" in raw:
        try:
            validate_current_payload(raw, row_identity=identity)
        except DecisionSchemaError as exc:
            return _outcome("conflict", exc.diagnostic_code, str(exc), identity)
        return _outcome(
            "unchanged", "decision_payload_current", "已是当前格式", identity,
            payload=raw,
        )

    conflicts = _identity_conflicts(raw, identity)
    if conflicts:
        return _outcome(
            "conflict", "decision_payload_identity_conflict",
            "payload 与原行列冲突: " + ", ".join(conflicts), identity,
        )

    migrated = {key: value for key, value in raw.items() if key in _CURRENT_FIELDS}
    unknown = {key: value for key, value in raw.items() if key not in _CURRENT_FIELDS}
    snapshot = migrated.get("model_snapshot")
    if snapshot is not None:
        try:
            validate_current_policy(snapshot)
        except DecisionSchemaError:
            unknown["model_snapshot"] = migrated.pop("model_snapshot")
    migrated.update({
        "decision_schema_version": DECISION_PAYLOAD_SCHEMA_VERSION,
        "signal_date": identity["signal_date"],
        "universe": identity["universe"],
        "holding_horizon_days": identity["horizon"],
        "created_at": identity["created_at"],
        "profile": migrated.get("profile") or None,
        "policy_hash": migrated.get("policy_hash") or None,
        "model_version": identity["model_version"],
    })
    migrated.setdefault("picks", [])
    try:
        validate_current_payload(migrated, row_identity=identity)
    except DecisionSchemaError as exc:
        return _outcome(
            "unclassified", exc.diagnostic_code, str(exc), identity, unknown=unknown,
        )
    optional_empty = [
        field for field in ("profile", "policy_hash", "model_version", "model_snapshot")
        if migrated.get(field) in (None, "")
    ]
    return _outcome(
        "migrated", (
            "decision_payload_migrated_with_optional_empty"
            if optional_empty else "decision_payload_migrated"
        ),
        (
            "可选字段留空: " + ", ".join(optional_empty)
            if optional_empty else "已从原行列回填当前身份字段"
        ),
        identity, payload=migrated, unknown=unknown,
    )


def _apply_outcomes(
    database: Path, outcomes: list[dict[str, Any]], batch_size: int,
) -> None:
    with connect_sqlite(database) as connection:
        connection.execute(
            "CREATE TABLE IF NOT EXISTS decision_migration_audit ("
            "record_key TEXT PRIMARY KEY,status TEXT NOT NULL,diagnostic_code TEXT NOT NULL,"
            "detail TEXT NOT NULL,unknown_fields_json TEXT NOT NULL,migrated_at TEXT NOT NULL)"
        )
    for start in range(0, len(outcomes), batch_size):
        with connect_sqlite(database) as connection:
            for outcome in outcomes[start:start + batch_size]:
                identity = outcome["identity"]
                if outcome["status"] == "migrated":
                    connection.execute(
                        "UPDATE selection_snapshots SET payload=? WHERE signal_date=? AND "
                        "universe=? AND horizon=? AND profile=? AND policy_hash=?",
                        (
                            json.dumps(outcome["payload"], ensure_ascii=False, allow_nan=False),
                            identity["signal_date"], identity["universe"], identity["horizon"],
                            identity["profile"] or "", identity["policy_hash"] or "",
                        ),
                    )
                connection.execute(
                    "INSERT INTO decision_migration_audit "
                    "(record_key,status,diagnostic_code,detail,unknown_fields_json,migrated_at) "
                    "VALUES (?,?,?,?,?,?) ON CONFLICT(record_key) DO UPDATE SET "
                    "status=excluded.status,diagnostic_code=excluded.diagnostic_code,"
                    "detail=excluded.detail,unknown_fields_json=excluded.unknown_fields_json,"
                    "migrated_at=excluded.migrated_at",
                    (
                        _record_key(identity), outcome["status"], outcome["diagnostic_code"],
                        outcome["detail"], json.dumps(outcome["unknown"], ensure_ascii=False),
                        datetime.now(UTC).isoformat(),
                    ),
                )


def migrate_decision_snapshots(
    path: Path,
    *,
    dry_run: bool = True,
    backup_path: Path | None = None,
    batch_size: int = 200,
) -> dict[str, Any]:
    """Plan or apply an idempotent, resumable decision migration.

    Applying requires an explicit, non-existing backup path.  The caller owns
    maintenance-mode coordination; this function never discovers a live DB.
    """
    database = Path(path)
    if batch_size < 1:
        raise ValueError("batch_size 必须为正数")
    if not dry_run:
        if backup_path is None:
            raise ValueError("应用迁移前必须指定 backup_path")
        backup = Path(backup_path)
        if backup.exists():
            raise FileExistsError(f"备份目标已存在: {backup}")
        backup.parent.mkdir(parents=True, exist_ok=True)
        with connect_sqlite(database, read_only=True) as source:
            with connect_sqlite(backup) as target:
                source.backup(target)
        if not backup.is_file():
            raise RuntimeError("决策库备份失败")

    with connect_sqlite(database, read_only=dry_run) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            "SELECT signal_date,universe,horizon,profile,policy_hash,model_version,"
            "payload,created_at FROM selection_snapshots ORDER BY created_at,signal_date"
        ).fetchall()
    outcomes = [_classify(row) for row in rows]
    counts = {key: 0 for key in ("total", "checked", "migrated", "unchanged", "unclassified", "conflict")}
    counts["total"] = counts["checked"] = len(outcomes)
    for outcome in outcomes:
        counts[outcome["status"]] += 1

    if not dry_run:
        _apply_outcomes(database, outcomes, batch_size)
    return {
        **counts,
        "dry_run": dry_run,
        "diagnostics": [
            {
                "identity": item["identity"], "status": item["status"],
                "diagnostic_code": item["diagnostic_code"], "detail": item["detail"],
                "unknown_fields": sorted(item["unknown"]),
            }
            for item in outcomes
        ],
    }


def _migration_record(outcome: dict[str, Any]) -> MigrationRecord:
    status = str(outcome["status"])
    mapped = {
        "migrated": "converted", "unchanged": "unchanged",
        "unclassified": "review", "conflict": "conflict",
    }[status]
    return MigrationRecord(
        record_key=_record_key(outcome["identity"]), outcome=mapped,
        diagnostic_code=str(outcome["diagnostic_code"]),
        unknown_fields=tuple(sorted(outcome.get("unknown") or {})),
        detail=str(outcome["detail"]),
    )


class DecisionLegacyMigrator:
    name = "decision"
    backup_paths = ("decisions.sqlite",)

    @staticmethod
    def _path(root: str | Path) -> Path:
        return Path(root) / "decisions.sqlite"

    def _outcomes(self, root: str | Path) -> list[dict[str, Any]]:
        path = self._path(root)
        if not path.is_file():
            return []
        with connect_sqlite(path, read_only=True, row_factory=True) as connection:
            rows = connection.execute(
                "SELECT signal_date,universe,horizon,profile,policy_hash,model_version,"
                "payload,created_at FROM selection_snapshots ORDER BY created_at,signal_date"
            ).fetchall()
        return [_classify(row) for row in rows]

    def inspect(self, root: str | Path) -> Iterable[MigrationRecord]:
        return (_migration_record(item) for item in self._outcomes(root))

    def migrate_batch(
        self, root: str | Path, *, after_key: str, limit: int,
    ) -> Iterable[MigrationRecord]:
        selected = [
            item for item in self._outcomes(root)
            if _record_key(item["identity"]) > after_key
        ][:limit]
        if selected:
            _apply_outcomes(self._path(root), selected, max(1, limit))
        return (_migration_record(item) for item in selected)

    def rollback(self, root: str | Path, backup_root: str | Path) -> None:
        source = Path(backup_root) / "decisions.sqlite"
        if not source.is_file():
            raise FileNotFoundError("决策快照备份不存在")
        from quantmaster.data.migration import restore_backup_path

        restore_backup_path(Path(root), Path(backup_root), "decisions.sqlite")


decision_legacy_migrator = DecisionLegacyMigrator()
