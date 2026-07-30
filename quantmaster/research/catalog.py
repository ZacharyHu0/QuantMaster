"""SQLite metadata catalog for research specs, partitions, lineage and jobs."""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any

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


class ResearchCatalog:
    """Small transactional source of truth; Parquet remains the numeric store."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
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
                    updated_at TEXT NOT NULL
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
                CREATE TABLE IF NOT EXISTS research_capabilities (
                    endpoint TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    min_points INTEGER NOT NULL,
                    detail TEXT NOT NULL,
                    checked_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS research_jobs (
                    id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    plan_json TEXT NOT NULL,
                    next_index INTEGER NOT NULL DEFAULT 0,
                    total INTEGER NOT NULL,
                    succeeded INTEGER NOT NULL DEFAULT 0,
                    failed INTEGER NOT NULL DEFAULT 0,
                    cancel_requested INTEGER NOT NULL DEFAULT 0,
                    current_task TEXT NOT NULL DEFAULT '',
                    failures_json TEXT NOT NULL DEFAULT '[]',
                    manifest_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )

    def recover_interrupted_jobs(self) -> int:
        """Called once by the process job owner, never by read-only catalog clients."""
        with self._connect() as connection:
            return connection.execute(
                "UPDATE research_jobs SET status='interrupted',current_task='',updated_at=? "
                "WHERE status IN ('running','cancelling')",
                (utc_now(),),
            ).rowcount

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
            connection.execute(
                "INSERT OR REPLACE INTO research_partitions "
                "(partition_key,kind,asset_class,frequency,dataset_id,trade_date,path,row_count,"
                "columns_json,schema_hash,content_sha256,spec_versions_json,input_hashes_json,"
                "run_id,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    key, str(payload["kind"]), str(payload["asset_class"]),
                    str(payload["frequency"]), payload["dataset_id"], payload["trade_date"],
                    payload["path"], int(payload["row_count"]), canonical_json(payload["columns"]),
                    payload["schema_hash"], payload["content_sha256"],
                    canonical_json(payload["spec_versions"]),
                    canonical_json(payload["input_hashes"]), payload["run_id"],
                    payload["updated_at"],
                ),
            )
        return payload

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

    def create_job(self, job_id: str, mode: str, plan: dict[str, Any]) -> dict[str, Any]:
        now = utc_now()
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO research_jobs "
                "(id,status,mode,plan_json,total,created_at,updated_at) "
                "VALUES (?,'running',?,?,?,?,?)",
                (job_id, mode, canonical_json(plan), len(plan.get("tasks") or ()), now, now),
            )
        return self.job(job_id) or {}

    def update_job(self, job_id: str, **changes: Any) -> dict[str, Any]:
        allowed = {
            "status", "next_index", "succeeded", "failed", "cancel_requested",
            "current_task", "failures_json", "manifest_json",
        }
        assignments: list[str] = []
        params: list[Any] = []
        for key, value in changes.items():
            if key not in allowed:
                raise ValueError(f"不允许更新任务字段: {key}")
            if key.endswith("_json") and not isinstance(value, str):
                value = canonical_json(value)
            assignments.append(f"{key}=?")
            params.append(value)
        assignments.append("updated_at=?")
        params.extend((utc_now(), job_id))
        with self._connect() as connection:
            changed = connection.execute(
                f"UPDATE research_jobs SET {','.join(assignments)} WHERE id=?", params
            ).rowcount
        if not changed:
            raise KeyError(job_id)
        return self.job(job_id) or {}

    @staticmethod
    def _job(row: sqlite3.Row) -> dict[str, Any]:
        value = dict(row)
        value["plan"] = json.loads(value.pop("plan_json"))
        value["failures"] = json.loads(value.pop("failures_json"))
        value["manifest"] = json.loads(value.pop("manifest_json"))
        value["cancel_requested"] = bool(value["cancel_requested"])
        value["progress"] = round(100 * int(value["next_index"]) / max(1, int(value["total"])))
        return value

    def job(self, job_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM research_jobs WHERE id=?", (job_id,)
            ).fetchone()
        return self._job(row) if row else None

    def jobs(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM research_jobs ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [self._job(row) for row in rows]
