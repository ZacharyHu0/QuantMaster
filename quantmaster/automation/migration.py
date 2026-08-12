"""One-shot migration for retired automation database contracts."""

from __future__ import annotations

import json
import os
from collections.abc import Iterable
from pathlib import Path
from sqlite3 import Connection

from quantmaster.automation.models import utc_now
from quantmaster.automation.store import AUTOMATION_SCHEMA_VERSION, DEFAULT_JOBS
from quantmaster.config import get_config
from quantmaster.data.legacy_migration import MigrationRecord
from quantmaster.runtime.sqlite import connect_sqlite

_V6_DEFAULTS = {
    "fast_news_scan": {"type": "interval", "minutes": 10, "window": "07:00-23:30"},
    "official_news_scan": {"type": "interval", "minutes": 15, "window": "07:00-23:30"},
    "periodic_news_scan": {"type": "interval", "minutes": 60, "window": "07:00-23:30"},
}
_V7_DEFAULTS = {
    "fast_news_scan": {"type": "interval", "minutes": 5},
    "official_news_scan": {"type": "interval", "minutes": 15},
    "periodic_news_scan": {"type": "interval", "minutes": 30},
}


def _decode_schedule(value: object) -> dict | None:
    try:
        decoded = json.loads(str(value or ""))
    except json.JSONDecodeError:
        return None
    return decoded if isinstance(decoded, dict) else None


def _schema_record(version: int) -> MigrationRecord:
    if version == AUTOMATION_SCHEMA_VERSION:
        return MigrationRecord("000:schema", "unchanged")
    if version in {6, 7, 8, 9, 10, 11}:
        return MigrationRecord(
            "000:schema", "converted", f"automation_schema_v{version}_to_v12",
        )
    return MigrationRecord(
        "000:schema", "review", "automation_schema_generation_unclassified",
        ("user_version",), f"仅可确认 v6-v11；实际 user_version={version}",
    )


def _schedule_records(connection: Connection) -> list[MigrationRecord]:
    rows = connection.execute(
        "SELECT name,schedule FROM job_templates WHERE name IN (?,?,?) ORDER BY name",
        tuple(sorted(_V6_DEFAULTS)),
    ).fetchall()
    records: list[MigrationRecord] = []
    for row in rows:
        name = str(row["name"])
        schedule = _decode_schedule(row["schedule"])
        key = f"100:schedule:{name}"
        if schedule is None:
            records.append(MigrationRecord(
                key, "review", "automation_schedule_json_invalid",
                ("schedule",), "原 schedule 不是可确认的 JSON 对象；保持原值",
            ))
        elif schedule in (_V6_DEFAULTS[name], _V7_DEFAULTS[name]):
            records.append(MigrationRecord(
                key, "converted", "automation_exact_retired_default",
                detail="仅完全匹配已确认的历史默认值",
            ))
        else:
            records.append(MigrationRecord(
                key, "unchanged", "automation_custom_schedule_preserved",
            ))
    return records


def _feishu_records(connection: Connection) -> list[MigrationRecord]:
    columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info(bot_accounts)")
    }
    wanted = ["id", "account_id", "secret_target", "status", "last_error"]
    selected = [name for name in wanted if name in columns]
    if not {"id", "account_id", "secret_target"} <= set(selected):
        return []
    rows = connection.execute(
        f"SELECT {','.join(selected)} FROM bot_accounts "
        "WHERE channel='feishu' ORDER BY id"
    ).fetchall()
    records: list[MigrationRecord] = []
    for row in rows:
        value = dict(row)
        if str(value.get("secret_target") or "").strip():
            outcome, code, fields, detail = "unchanged", "", (), ""
        elif value.get("last_error") == "credential_migration_required":
            outcome, code, fields, detail = (
                "unchanged", "feishu_credential_left_unconfigured", (), "",
            )
        else:
            outcome, code, fields, detail = (
                "blank", "feishu_secret_target_missing", ("secret_target",),
                "旧账号只有 App ID，无法证明 App Secret 来源；凭据保持未配置",
            )
        records.append(MigrationRecord(
            f"200:feishu:{value['id']}", outcome, code, fields, detail,
        ))
    return records


def _add_missing_columns(
    connection: Connection, table: str, additions: dict[str, str],
) -> None:
    columns = {
        str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")
    }
    for name, declaration in additions.items():
        if name not in columns:
            connection.execute(f"ALTER TABLE {table} ADD COLUMN {name} {declaration}")


class AutomationContractMigrator:
    name = "automation-contract-v9"
    backup_paths = ("automation.sqlite",)

    @staticmethod
    def _path(root: Path) -> Path:
        return root / "automation.sqlite"

    def inspect(self, root: Path) -> Iterable[MigrationRecord]:
        path = self._path(root)
        if not path.is_file():
            return iter(())
        records: list[MigrationRecord] = []
        with connect_sqlite(path, read_only=True, row_factory=True) as connection:
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            records.append(_schema_record(version))

            tables = {
                str(row[0]) for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            if "job_templates" in tables:
                records.extend(_schedule_records(connection))

            if "bot_accounts" in tables:
                records.extend(_feishu_records(connection))

        app_id = get_config().automation.feishu_app_id.strip()
        secret_present = bool(os.environ.get("QM_FEISHU_APP_SECRET", "").strip())
        if app_id or secret_present:
            complete = bool(app_id and secret_present)
            records.append(MigrationRecord(
                "300:feishu:external-config",
                "review" if complete else "blank",
                (
                    "feishu_legacy_credentials_require_explicit_configure"
                    if complete else "feishu_legacy_credentials_incomplete"
                ),
                ("app_id", "app_secret"),
                (
                    "检测到完整旧配置，但不会读取或写入 secret；请走当前凭据配置流程"
                    if complete else
                    "旧配置缺少 App ID 或 App Secret；可选凭据保持未配置"
                ),
            ))
        return iter(records)

    def migrate_batch(
        self, root: Path, *, after_key: str, limit: int,
    ) -> Iterable[MigrationRecord]:
        pending = [record for record in self.inspect(root) if record.record_key > after_key]
        values = pending[:limit]
        path = self._path(root)
        for record in values:
            if record.record_key == "000:schema" and record.outcome == "converted":
                self._upgrade_schema(path)
            elif record.record_key.startswith("100:schedule:") and record.outcome == "converted":
                name = record.record_key.rsplit(":", 1)[-1]
                with connect_sqlite(path) as connection:
                    connection.execute(
                        "UPDATE job_templates SET schedule=?,updated_at=? WHERE name=?",
                        (json.dumps(DEFAULT_JOBS[name][1]), utc_now(), name),
                    )
            elif record.record_key.startswith("200:feishu:") and record.outcome == "blank":
                account_id = record.record_key.removeprefix("200:feishu:")
                with connect_sqlite(path) as connection:
                    connection.execute(
                        "UPDATE bot_accounts SET status='not_configured',"
                        "last_error='credential_migration_required',updated_at=? WHERE id=?",
                        (utc_now(), account_id),
                    )
        return iter(values)

    @staticmethod
    def _upgrade_schema(path: Path) -> None:
        with connect_sqlite(path) as connection:
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version not in {6, 7, 8, 9, 10, 11, AUTOMATION_SCHEMA_VERSION}:
                raise RuntimeError(
                    f"automation_schema_generation_unclassified: user_version={version}"
                )
            if version == AUTOMATION_SCHEMA_VERSION:
                return
            _add_missing_columns(connection, "notification_targets", {
                "context_token": "TEXT NOT NULL DEFAULT ''",
            })
            _add_missing_columns(connection, "inbound_messages", {
                "chat_type": "TEXT NOT NULL DEFAULT ''",
                "account_id": "TEXT NOT NULL DEFAULT ''",
            })
            _add_missing_columns(connection, "analysis_deliveries", {
                "query": "TEXT NOT NULL DEFAULT ''",
                "mode": "TEXT NOT NULL DEFAULT 'deep'",
            })
            _add_missing_columns(connection, "bot_accounts", {
                "last_validated_at": "TEXT NOT NULL DEFAULT ''",
            })
            connection.execute(
                "CREATE TABLE IF NOT EXISTS scheduler_cursors ("
                "job_name TEXT PRIMARY KEY,window_end REAL NOT NULL,updated_at TEXT NOT NULL)"
            )
            delivery_additions = {
                "lease_owner": "TEXT NOT NULL DEFAULT ''",
                "lease_token": "TEXT NOT NULL DEFAULT ''",
                "lease_expires_at": "REAL NOT NULL DEFAULT 0",
                "heartbeat_at": "REAL NOT NULL DEFAULT 0",
                "retry_after_at": "REAL NOT NULL DEFAULT 0",
                "diagnostic_code": "TEXT NOT NULL DEFAULT ''",
                "ambiguous_at": "TEXT NOT NULL DEFAULT ''",
            }
            _add_missing_columns(connection, "delivery_attempts", delivery_additions)
            connection.execute(
                "UPDATE delivery_attempts SET status=CASE status "
                "WHEN 'delivered' THEN 'sent' WHEN 'retry' THEN 'retry_wait' "
                "WHEN 'failed' THEN 'dead_letter' ELSE status END"
            )
            connection.execute("DROP INDEX IF EXISTS idx_delivery_due")
            connection.execute(
                "CREATE INDEX idx_delivery_due ON delivery_attempts("
                "status,next_attempt_at,lease_expires_at)"
            )
            analysis_additions = {
                "attempts": "INTEGER NOT NULL DEFAULT 0",
                "lease_owner": "TEXT NOT NULL DEFAULT ''",
                "lease_token": "TEXT NOT NULL DEFAULT ''",
                "lease_expires_at": "REAL NOT NULL DEFAULT 0",
                "heartbeat_at": "REAL NOT NULL DEFAULT 0",
                "operation": "TEXT NOT NULL DEFAULT ''",
                "diagnostic_code": "TEXT NOT NULL DEFAULT ''",
                "ambiguous_at": "TEXT NOT NULL DEFAULT ''",
            }
            _add_missing_columns(connection, "analysis_deliveries", analysis_additions)
            connection.execute(
                "UPDATE analysis_deliveries SET status=CASE status "
                "WHEN 'active' THEN 'pending' WHEN 'retry' THEN 'retry_wait' "
                "WHEN 'delivered' THEN 'sent' WHEN 'failed' THEN 'dead_letter' "
                "ELSE status END"
            )
            connection.execute("DROP INDEX IF EXISTS idx_analysis_delivery_due")
            connection.execute(
                "CREATE INDEX idx_analysis_delivery_due ON analysis_deliveries("
                "status,next_attempt_at,lease_expires_at)"
            )
            connection.execute(
                "UPDATE task_runs SET status='interrupted_legacy',finished_at=?,"
                "error=CASE WHEN error='' THEN 'migrated to unified durable jobs' ELSE error END "
                "WHERE status='running'",
                (utc_now(),),
            )
            connection.execute(f"PRAGMA user_version={AUTOMATION_SCHEMA_VERSION}")

    def rollback(self, root: Path, backup_root: Path) -> None:
        source = backup_root / "automation.sqlite"
        if not source.is_file():
            raise RuntimeError("automation migration backup missing")
        from quantmaster.data.migration import restore_backup_path

        restore_backup_path(root, backup_root, "automation.sqlite")


automation_contract_migrator = AutomationContractMigrator()
