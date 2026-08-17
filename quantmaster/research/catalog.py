"""SQLite metadata catalog for research specs, partitions, lineage and jobs."""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any

from quantmaster.data.schema_access import register_research_catalog
from quantmaster.research.contracts import (
    ArtifactKind,
    AssetClass,
    CapabilityState,
    Frequency,
    ResearchSpec,
    RunManifest,
    canonical_json,
    utc_now,
)
from quantmaster.runtime.sqlite import connect_sqlite, execute_sql_script, migrate_schema

RESEARCH_SCHEMA_VERSION = 2


class ResearchSchemaMigrationRequired(RuntimeError):
    """The research catalog needs the explicit remaining-schema migrator."""


class ResearchCatalog:
    """Small transactional source of truth; Parquet remains the numeric store."""

    def __init__(self, path: str | Path, *, read_only: bool = False):
        self.path = Path(path)
        self.read_only = bool(read_only)
        if not self.path.is_file():
            if self.read_only:
                raise FileNotFoundError(self.path)
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._initialize()
        else:
            self._require_current()

    def _connect(self) -> sqlite3.Connection:
        return connect_sqlite(
            self.path,
            timeout=0.25 if self.read_only else 5.0,
            row_factory=True,
            read_only=self.read_only,
        )

    def _initialize(self) -> None:
        with self._connect() as connection:
            migrate_schema(connection, ((RESEARCH_SCHEMA_VERSION, self._schema_v2),))

    @staticmethod
    def _schema_v2(connection: sqlite3.Connection) -> None:
            execute_sql_script(
                connection,
                """
                CREATE TABLE IF NOT EXISTS research_specs (
                    kind TEXT NOT NULL,
                    id TEXT NOT NULL,
                    version TEXT NOT NULL,
                    spec_hash TEXT NOT NULL,
                    spec_json TEXT NOT NULL,
                    registered_at TEXT NOT NULL,
                    PRIMARY KEY (kind,id,version)
                );
                CREATE TABLE IF NOT EXISTS research_partitions (
                    partition_key TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    asset_class TEXT NOT NULL,
                    frequency TEXT NOT NULL,
                    dataset_id TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    path TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    columns_json TEXT NOT NULL,
                    schema_hash TEXT NOT NULL,
                    content_sha256 TEXT NOT NULL,
                    spec_versions_json TEXT NOT NULL,
                    input_hashes_json TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    file_size INTEGER NOT NULL DEFAULT 0,
                    file_mtime_ns INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_research_partitions_lookup
                    ON research_partitions(kind,asset_class,frequency,dataset_id,trade_date);
                CREATE TABLE IF NOT EXISTS research_runs (
                    run_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    manifest_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS research_leases (
                    partition_key TEXT PRIMARY KEY,
                    owner TEXT NOT NULL,
                    expires_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS research_partition_intents (
                    partition_key TEXT PRIMARY KEY,
                    owner TEXT NOT NULL,
                    target_path TEXT NOT NULL,
                    staged_path TEXT NOT NULL,
                    backup_path TEXT NOT NULL,
                    content_sha256 TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS research_capabilities (
                    endpoint TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    min_points INTEGER NOT NULL,
                    detail TEXT NOT NULL,
                    checked_at TEXT NOT NULL
                );
                """
            )
            partition_columns = {
                row[1] for row in connection.execute(
                    "PRAGMA table_info(research_partitions)"
                ).fetchall()
            }
            for name in ("file_size", "file_mtime_ns"):
                if name not in partition_columns:
                    connection.execute(
                        f"ALTER TABLE research_partitions ADD COLUMN {name} "
                        "INTEGER NOT NULL DEFAULT 0"
                    )
    def _require_current(self) -> None:
        with self._connect() as connection:
            tables = {str(row[0]) for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )}
            required = {
                "research_specs", "research_partitions", "research_runs",
                "research_leases", "research_partition_intents",
                "research_capabilities",
            }
            partition_columns = {str(row[1]) for row in connection.execute(
                "PRAGMA table_info(research_partitions)"
            )}
            version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if (
                version != RESEARCH_SCHEMA_VERSION or required - tables
                or {"file_size", "file_mtime_ns"} - partition_columns
                or {"research_jobs", "research_job_events"} & tables
            ):
                raise ResearchSchemaMigrationRequired(
                    "research catalog 不是当前 schema，需执行 research-jobs 一次性迁移"
                )

    @staticmethod
    def partition_key(
        kind: ArtifactKind | str,
        asset_class: AssetClass | str,
        frequency: Frequency | str,
        dataset_id: str,
        trade_date: str,
    ) -> str:
        return ":".join((str(kind), str(asset_class), str(frequency), dataset_id, trade_date))

    def register_spec(self, spec: ResearchSpec) -> dict[str, Any]:
        payload = spec.to_dict()
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT spec_hash FROM research_specs WHERE kind=? AND id=? AND version=?",
                (spec.kind.value, spec.id, spec.version),
            ).fetchone()
            if existing and existing["spec_hash"] != spec.spec_hash:
                raise ValueError(
                    f"研究规格 {spec.kind.value}/{spec.id}@{spec.version} 已存在且内容不同；"
                    "请递增版本"
                )
            connection.execute(
                "INSERT OR IGNORE INTO research_specs "
                "(kind,id,version,spec_hash,spec_json,registered_at) VALUES (?,?,?,?,?,?)",
                (spec.kind.value, spec.id, spec.version, spec.spec_hash,
                 canonical_json(payload), utc_now()),
            )
        return {**payload, "spec_hash": spec.spec_hash}

    def specs(self, kind: ArtifactKind | str | None = None) -> list[dict[str, Any]]:
        query = "SELECT spec_json,spec_hash FROM research_specs"
        params: tuple[Any, ...] = ()
        if kind is not None:
            query += " WHERE kind=?"
            params = (str(kind),)
        query += " ORDER BY kind,id,version"
        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [{**json.loads(row["spec_json"]), "spec_hash": row["spec_hash"]} for row in rows]

    def record_partition(self, value: dict[str, Any]) -> dict[str, Any]:
        required = {
            "kind", "asset_class", "frequency", "dataset_id", "trade_date", "path",
            "row_count", "columns", "schema_hash", "content_sha256",
        }
        missing = sorted(required - value.keys())
        if missing:
            raise ValueError(f"分区元数据缺少字段: {', '.join(missing)}")
        key = self.partition_key(
            value["kind"], value["asset_class"], value["frequency"],
            value["dataset_id"], value["trade_date"],
        )
        payload = {
            **value,
            "partition_key": key,
            "spec_versions": value.get("spec_versions") or {},
            "input_hashes": value.get("input_hashes") or {},
            "run_id": value.get("run_id") or "",
            "updated_at": value.get("updated_at") or utc_now(),
        }
        with self._connect() as connection:
            self._record_partition(connection, payload)
        return payload

    @staticmethod
    def _record_partition(connection: sqlite3.Connection, payload: dict[str, Any]) -> None:
        key = str(payload["partition_key"])
        connection.execute(
            "INSERT OR REPLACE INTO research_partitions "
            "(partition_key,kind,asset_class,frequency,dataset_id,trade_date,path,row_count,"
            "columns_json,schema_hash,content_sha256,spec_versions_json,input_hashes_json,"
            "run_id,updated_at,file_size,file_mtime_ns) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                key, str(payload["kind"]), str(payload["asset_class"]),
                str(payload["frequency"]), payload["dataset_id"], payload["trade_date"],
                payload["path"], int(payload["row_count"]), canonical_json(payload["columns"]),
                payload["schema_hash"], payload["content_sha256"],
                canonical_json(payload.get("spec_versions") or {}),
                canonical_json(payload.get("input_hashes") or {}), payload.get("run_id") or "",
                payload.get("updated_at") or utc_now(), int(payload.get("file_size") or 0),
                int(payload.get("file_mtime_ns") or 0),
            ),
        )

    def begin_partition_write(
        self,
        partition_key: str,
        owner: str,
        *,
        target_path: str,
        staged_path: str,
        backup_path: str,
        content_sha256: str,
        metadata: dict[str, Any],
    ) -> None:
        """Persist the recovery record before replacing a partition file."""
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO research_partition_intents "
                "(partition_key,owner,target_path,staged_path,backup_path,content_sha256,"
                "metadata_json,created_at) VALUES (?,?,?,?,?,?,?,?)",
                (partition_key, owner, target_path, staged_path, backup_path, content_sha256,
                 canonical_json(metadata), time.time()),
            )

    def commit_partition_write(
        self, partition_key: str, owner: str, metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Commit partition metadata and clear its intent in one SQLite transaction."""
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT owner,metadata_json FROM research_partition_intents "
                "WHERE partition_key=?",
                (partition_key,),
            ).fetchone()
            if row is None or row["owner"] != owner:
                raise RuntimeError(f"研究分区写入意图已失效: {partition_key}")
            payload = dict(metadata or json.loads(row["metadata_json"]))
            payload["partition_key"] = partition_key
            self._record_partition(connection, payload)
            connection.execute(
                "DELETE FROM research_partition_intents WHERE partition_key=? AND owner=?",
                (partition_key, owner),
            )
        return payload

    def partition_intents(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT i.*,l.expires_at AS lease_expires FROM research_partition_intents i "
                "LEFT JOIN research_leases l ON l.partition_key=i.partition_key"
            ).fetchall()
        values = []
        for row in rows:
            value = dict(row)
            value["metadata"] = json.loads(value.pop("metadata_json"))
            values.append(value)
        return values

    def discard_partition_intent(self, partition_key: str, owner: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM research_partition_intents WHERE partition_key=? AND owner=?",
                (partition_key, owner),
            )

    def update_partition_file_identity(
        self, partition_key: str, *, file_size: int, file_mtime_ns: int,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE research_partitions SET file_size=?,file_mtime_ns=? "
                "WHERE partition_key=?",
                (int(file_size), int(file_mtime_ns), partition_key),
            )

    def delete_partition(self, partition_key: str) -> bool:
        """Forget metadata only after a caller has durably quarantined the original file."""
        with self._connect() as connection:
            changed = connection.execute(
                "DELETE FROM research_partitions WHERE partition_key=?", (partition_key,),
            ).rowcount
        return bool(changed)

    @staticmethod
    def _partition(row: sqlite3.Row) -> dict[str, Any]:
        value = dict(row)
        value["columns"] = json.loads(value.pop("columns_json"))
        value["spec_versions"] = json.loads(value.pop("spec_versions_json"))
        value["input_hashes"] = json.loads(value.pop("input_hashes_json"))
        return value

    def partition(
        self,
        kind: ArtifactKind | str,
        asset_class: AssetClass | str,
        frequency: Frequency | str,
        dataset_id: str,
        trade_date: str,
    ) -> dict[str, Any] | None:
        key = self.partition_key(kind, asset_class, frequency, dataset_id, trade_date)
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM research_partitions WHERE partition_key=?", (key,)
            ).fetchone()
        return self._partition(row) if row else None

    def partitions(
        self,
        *,
        kind: ArtifactKind | str | None = None,
        asset_class: AssetClass | str | None = None,
        frequency: Frequency | str | None = None,
        dataset_id: str | None = None,
        start: str | None = None,
        end: str | None = None,
    ) -> list[dict[str, Any]]:
        where: list[str] = []
        params: list[Any] = []
        for column, value in (
            ("kind", kind), ("asset_class", asset_class), ("frequency", frequency),
            ("dataset_id", dataset_id),
        ):
            if value is not None:
                where.append(f"{column}=?")
                params.append(str(value))
        if start:
            where.append("trade_date>=?")
            params.append(start)
        if end:
            where.append("trade_date<=?")
            params.append(end)
        query = "SELECT * FROM research_partitions"
        if where:
            query += " WHERE " + " AND ".join(where)
        query += " ORDER BY trade_date,dataset_id"
        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [self._partition(row) for row in rows]

    def trading_dates(
        self,
        asset_class: AssetClass | str,
        frequency: Frequency | str,
        start: str,
        end: str,
    ) -> list[str]:
        """Return dates evidenced by verified raw partitions, never synthetic weekdays."""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT DISTINCT trade_date FROM research_partitions "
                "WHERE kind=? AND asset_class=? AND frequency=? "
                "AND trade_date>=? AND trade_date<=? ORDER BY trade_date",
                (str(ArtifactKind.RAW), str(asset_class), str(frequency), start, end),
            ).fetchall()
        return [str(row[0]) for row in rows]

    def claim(self, partition_key: str, owner: str, ttl_seconds: int = 300) -> bool:
        now = time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute("DELETE FROM research_leases WHERE expires_at<=?", (now,))
            try:
                connection.execute(
                    "INSERT INTO research_leases (partition_key,owner,expires_at) VALUES (?,?,?)",
                    (partition_key, owner, now + ttl_seconds),
                )
            except sqlite3.IntegrityError:
                return False
        return True

    def release(self, partition_key: str, owner: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM research_leases WHERE partition_key=? AND owner=?",
                (partition_key, owner),
            )

    def set_capability(
        self,
        endpoint: str,
        state: CapabilityState,
        *,
        min_points: int = 0,
        detail: str = "",
    ) -> dict[str, Any]:
        value = {
            "endpoint": endpoint,
            "state": state.value,
            "min_points": min_points,
            "detail": detail,
            "checked_at": utc_now(),
        }
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO research_capabilities "
                "(endpoint,state,min_points,detail,checked_at) VALUES (?,?,?,?,?)",
                tuple(value.values()),
            )
        return value

    def capabilities(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM research_capabilities ORDER BY endpoint"
            ).fetchall()
        return [dict(row) for row in rows]

    def save_run(self, manifest: RunManifest | dict[str, Any]) -> dict[str, Any]:
        value = manifest.to_dict() if isinstance(manifest, RunManifest) else dict(manifest)
        run_id = str(value.get("run_id") or "")
        if not run_id:
            raise ValueError("运行清单缺少 run_id")
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO research_runs "
                "(run_id,status,manifest_json,updated_at) VALUES (?,?,?,?)",
                (run_id, str(value.get("status") or "unknown"), canonical_json(value), utc_now()),
            )
        return value

    def run(self, run_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT manifest_json FROM research_runs WHERE run_id=?", (run_id,)
            ).fetchone()
        return json.loads(row["manifest_json"]) if row else None

register_research_catalog(ResearchCatalog)
