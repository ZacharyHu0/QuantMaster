"""One-shot isolation of retired Lab model artifacts."""

from __future__ import annotations

import json
import os
import sqlite3
from collections.abc import Iterable
from pathlib import Path

from quantmaster.data.migration import restore_backup_path
from quantmaster.data.migration_contracts import MigrationRecord
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.runtime.sqlite import connect_sqlite

_V1_REQUIRED = {
    "schema_version", "kind", "features", "sequence_length", "horizon",
    "artifact", "artifact_sha256",
}
_V2_REQUIRED = {
    "schema_version", "kind", "horizons", "features", "feature_names",
    "sequence_length", "protocol", "prediction_artifact", "prediction_sha256",
    "fold_artifacts", "live_artifact", "calibration", "calibration_models",
}


def _manifests(root: Path) -> list[Path]:
    artifact_root = root / "lab_artifacts"
    return sorted(path for path in artifact_root.rglob("manifest*.json") if path.is_file())


def _inspect_one(root: Path, path: Path) -> MigrationRecord | None:
    key = path.relative_to(root).as_posix()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return MigrationRecord(
            key, "conflict", "lab_model_manifest_invalid_json",
            detail="清单无法解析；拒绝猜测格式",
        )
    if not isinstance(payload, dict):
        return MigrationRecord(key, "conflict", "lab_model_manifest_invalid_shape")
    version = payload.get("schema_version")
    if version == 2:
        missing = tuple(sorted(_V2_REQUIRED - payload.keys()))
        return None if not missing else MigrationRecord(
            key, "conflict", "lab_model_schema_v2_corrupt", missing,
            "已标记 current 的清单缺少必需字段；不得误判为旧格式",
        )
    if version == 1:
        missing = tuple(sorted(_V1_REQUIRED - payload.keys()))
        return MigrationRecord(
            key, "conflict" if missing else "review",
            "lab_model_schema_v1_incomplete" if missing else "lab_model_schema_v1_requires_isolation",
            missing or ("protocol", "live_artifact", "prediction_artifact", "calibration", "ood"),
            "v1 权重不能可靠转换为 v2；仅可隔离并使引用不可部署",
        )
    return MigrationRecord(
        key, "conflict", "lab_model_schema_unclassified", ("schema_version",),
        "缺少或未知 schema 标签；拒绝按特征猜测",
    )


class LabModelArtifactMigrator:
    name = "lab-model-artifacts"
    backup_paths = ("lab.sqlite", "lab_artifacts", "migration_quarantine/lab_models")

    def inspect(self, root: Path) -> Iterable[MigrationRecord]:
        for path in _manifests(root):
            record = _inspect_one(root, path)
            if record is not None:
                yield record

    def migrate_batch(
        self, root: Path, *, after_key: str, limit: int,
    ) -> Iterable[MigrationRecord]:
        candidates = [
            record for record in self.inspect(root)
            if record.record_key > after_key
        ][:max(1, int(limit))]
        for record in candidates:
            if record.diagnostic_code != "lab_model_schema_v1_requires_isolation":
                yield record
                continue
            manifest = root / record.record_key
            self._retire_references(root, record.record_key)
            source = manifest.parent
            relative = source.relative_to(root / "lab_artifacts")
            target = root / "migration_quarantine" / "lab_models" / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                if source.exists():
                    raise FileExistsError(f"Lab 模型隔离目标冲突：{target}")
            else:
                os.replace(source, target)
            yield MigrationRecord(
                record.record_key, "blank", "lab_model_schema_v1_isolated",
                record.unknown_fields,
                "旧工件已隔离；manifest 引用留空，版本归档且不可部署",
            )

    @staticmethod
    def _retire_references(root: Path, manifest: str) -> None:
        database = root / "lab.sqlite"
        if not database.is_file():
            return
        with connect_sqlite(database) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute("SELECT id,spec_json FROM factor_versions").fetchall()
            for row in rows:
                spec = json.loads(str(row["spec_json"]))
                model = dict(spec.get("model") or {})
                if model.get("manifest") != manifest:
                    continue
                model["manifest"] = ""
                model["availability"] = "unavailable"
                model["diagnostic_code"] = "lab_model_schema_v1_isolated"
                spec["model"] = model
                connection.execute(
                    "UPDATE factor_versions SET spec_json=?,status='archived' WHERE id=?",
                    (strict_json_dumps(spec, sort_keys=True), row["id"]),
                )
            experiment_rows = connection.execute("SELECT id,result_json FROM experiments").fetchall()
            for row in experiment_rows:
                result = json.loads(str(row["result_json"] or "{}"))
                if result.get("manifest") != manifest:
                    continue
                result["manifest"] = None
                result["model_availability"] = "unavailable"
                result["diagnostic_code"] = "lab_model_schema_v1_isolated"
                connection.execute(
                    "UPDATE experiments SET result_json=? WHERE id=?",
                    (strict_json_dumps(result, sort_keys=True), row["id"]),
                )

    def rollback(self, root: Path, backup_root: Path) -> None:
        for relative in self.backup_paths:
            restore_backup_path(root, backup_root, relative)


lab_model_artifact_migrator = LabModelArtifactMigrator()
