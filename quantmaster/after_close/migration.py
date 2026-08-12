"""Explicit migration of schema-labelled after-close snapshots."""

from __future__ import annotations

import json
import shutil
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from quantmaster.after_close.models import SCHEMA_VERSION, AfterCloseSnapshot
from quantmaster.data.legacy_migration import MigrationRecord
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.runtime.sqlite import connect_sqlite


def _record(key: str, outcome: str, code: str, unknown=(), detail: str = "") -> dict[str, Any]:
    return {
        "record_key": key, "outcome": outcome, "diagnostic_code": code,
        "unknown_fields": tuple(sorted(unknown)), "detail": detail,
    }


def _current_payload(payload: dict[str, Any], version: str) -> dict[str, Any]:
    value = json.loads(json.dumps(payload, ensure_ascii=False))
    if version == "1.0":
        value["ingest_id"] = ""
        value["artifact_id"] = ""
    if version in {"1.0", "1.1"}:
        value["shadow_candidates"] = []
        for sector in value.get("sectors") or []:
            sector["sensitivity"] = {}
        for candidate in value.get("candidates") or []:
            candidate["shadow"] = {}
    value["schema_version"] = SCHEMA_VERSION
    return value


def inspect_after_close_snapshots(root: str | Path) -> list[dict[str, Any]]:
    database = Path(root) / "after_close.sqlite"
    if not database.is_file():
        return []
    with connect_sqlite(database, read_only=True, row_factory=True) as connection:
        rows = connection.execute(
            "SELECT snapshot_id,as_of_date,score_version,input_hash,payload_json FROM snapshots "
            "ORDER BY snapshot_id"
        ).fetchall()
    results = []
    for row in rows:
        key = str(row["snapshot_id"])
        try:
            payload = json.loads(str(row["payload_json"]))
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            results.append(_record(key, "review", "after_close_invalid_json", detail=str(exc)))
            continue
        if not isinstance(payload, dict):
            results.append(_record(key, "review", "after_close_payload_not_object"))
            continue
        version = str(payload.get("schema_version") or "")
        if version not in {"1.0", "1.1", SCHEMA_VERSION}:
            results.append(_record(
                key, "review", "after_close_unknown_schema",
                unknown=set(payload) - {"schema_version"}, detail=f"schema_version={version or 'missing'}",
            ))
            continue
        conflicts = [
            field for field in ("snapshot_id", "as_of_date", "score_version", "input_hash")
            if str(payload.get(field) or "") != str(row[field] or "")
        ]
        if conflicts:
            results.append(_record(
                key, "conflict", "after_close_identity_conflict", conflicts,
                "payload 与原记录列不一致",
            ))
            continue
        current = _current_payload(payload, version)
        try:
            AfterCloseSnapshot.from_dict(current)
        except (TypeError, ValueError) as exc:
            results.append(_record(
                key, "review", "after_close_schema_invalid", detail=str(exc),
            ))
            continue
        if version == SCHEMA_VERSION:
            results.append(_record(key, "unchanged", "after_close_current"))
        else:
            blanks = ["shadow_candidates", "sensitivity", "shadow"]
            if version == "1.0":
                blanks += ["ingest_id", "artifact_id"]
            results.append(_record(
                key, "blank", "after_close_optional_fields_empty", blanks,
                f"schema {version} 仅迁移共同字段；新增可选事实保持为空",
            ))
    return results


def migrate_after_close_batch(
    root: str | Path, *, after_key: str = "", limit: int = 250,
) -> list[dict[str, Any]]:
    database = Path(root) / "after_close.sqlite"
    selected = [
        item for item in inspect_after_close_snapshots(root)
        if item["record_key"] > after_key
    ][:limit]
    convertible = {
        item["record_key"] for item in selected
        if item["diagnostic_code"] == "after_close_optional_fields_empty"
    }
    if database.is_file():
        with connect_sqlite(database, row_factory=True) as connection:
            connection.execute("BEGIN IMMEDIATE")
            columns = {
                str(row[1]) for row in connection.execute("PRAGMA table_info(snapshots)")
            }
            if "payload_hash" in columns:
                connection.execute("ALTER TABLE snapshots DROP COLUMN payload_hash")
            for snapshot_id in sorted(convertible):
                row = connection.execute(
                    "SELECT payload_json FROM snapshots WHERE snapshot_id=?", (snapshot_id,),
                ).fetchone()
                payload = json.loads(str(row["payload_json"]))
                current = _current_payload(payload, str(payload["schema_version"]))
                AfterCloseSnapshot.from_dict(current)
                connection.execute(
                    "UPDATE snapshots SET payload_json=? WHERE snapshot_id=?",
                    (strict_json_dumps(current, sort_keys=True), snapshot_id),
                )
    return selected


class AfterCloseLegacyMigrator:
    name = "after_close"

    def inspect(self, root: str | Path) -> Iterable[dict[str, Any]]:
        return (_as_record(item) for item in inspect_after_close_snapshots(root))

    def migrate_batch(
        self, root: str | Path, *, after_key: str, limit: int,
    ) -> Iterable[dict[str, Any]]:
        return (
            _as_record(item)
            for item in migrate_after_close_batch(root, after_key=after_key, limit=limit)
        )

    def rollback(self, root: str | Path, backup_root: str | Path) -> None:
        source = Path(backup_root) / "after_close.sqlite"
        if not source.is_file():
            raise FileNotFoundError("盘后快照备份不存在")
        shutil.copy2(source, Path(root) / "after_close.sqlite")


def _as_record(value: dict[str, Any]) -> MigrationRecord:
    return MigrationRecord(
        record_key=str(value["record_key"]), outcome=str(value["outcome"]),
        diagnostic_code=str(value.get("diagnostic_code") or ""),
        unknown_fields=tuple(value.get("unknown_fields") or ()),
        detail=str(value.get("detail") or ""),
    )


after_close_legacy_migrator = AfterCloseLegacyMigrator()
