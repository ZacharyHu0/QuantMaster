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
from quantmaster.runtime.jobs import lease_deadline
from quantmaster.runtime.sqlite import connect_sqlite


class ResearchCatalog:
    """Small transactional source of truth; Parquet remains the numeric store."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        return connect_sqlite(self.path, row_factory=True)

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
                CREATE TABLE IF NOT EXISTS research_job_events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    attempt INTEGER NOT NULL,
                    event_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES research_jobs(id)
                );
                CREATE INDEX IF NOT EXISTS idx_research_job_events
                    ON research_job_events(job_id,seq);
                """
            )
            columns = {
                row[1] for row in connection.execute(
                    "PRAGMA table_info(research_jobs)"
                ).fetchall()
            }
            additions = {
                "owner": "TEXT NOT NULL DEFAULT ''",
                "lease_expires": "REAL NOT NULL DEFAULT 0",
                "heartbeat_at": "TEXT NOT NULL DEFAULT ''",
                "attempt": "INTEGER NOT NULL DEFAULT 1",
                "task_indexes_json": "TEXT NOT NULL DEFAULT '[]'",
            }
            for name, definition in additions.items():
                if name not in columns:
                    connection.execute(
                        f"ALTER TABLE research_jobs ADD COLUMN {name} {definition}"
                    )
            rows = connection.execute(
                "SELECT id,total,task_indexes_json FROM research_jobs"
            ).fetchall()
            for row in rows:
                if json.loads(row["task_indexes_json"] or "[]"):
                    continue
                connection.execute(
                    "UPDATE research_jobs SET task_indexes_json=? WHERE id=?",
                    (canonical_json(list(range(int(row["total"])))), row["id"]),
                )

    def recover_interrupted_jobs(self) -> int:
        """Recover only abandoned leases; live workers in other processes are untouched."""
        with self._connect() as connection:
            return connection.execute(
                "UPDATE research_jobs SET status='interrupted',owner='',lease_expires=0,"
                "current_task='',updated_at=? WHERE status IN ('running','cancelling') "
                "AND lease_expires<=?",
                (utc_now(), time.time()),
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
            connection.execute("BEGIN IMMEDIATE")
            active = connection.execute(
                "SELECT id FROM research_jobs "
                "WHERE status IN ('queued','running','cancelling') LIMIT 1"
            ).fetchone()
            if active:
                raise ValueError(f"已有研究数据任务正在运行：{active['id']}")
            task_indexes = list(range(len(plan.get("tasks") or ())))
            connection.execute(
                "INSERT INTO research_jobs "
                "(id,status,mode,plan_json,total,task_indexes_json,created_at,updated_at) "
                "VALUES (?,'queued',?,?,?,?,?,?)",
                (
                    job_id, mode, canonical_json(plan), len(task_indexes),
                    canonical_json(task_indexes), now, now,
                ),
            )
            connection.execute(
                "INSERT INTO research_job_events(job_id,attempt,event_json,created_at) "
                "VALUES (?,?,?,?)",
                (job_id, 1, canonical_json({"type": "queued"}), now),
            )
        return self.job(job_id) or {}

    def append_job_event(self, job_id: str, attempt: int, event: dict[str, Any]) -> int:
        with self._connect() as connection:
            cursor = connection.execute(
                "INSERT INTO research_job_events(job_id,attempt,event_json,created_at) "
                "VALUES (?,?,?,?)",
                (job_id, attempt, canonical_json(event), utc_now()),
            )
        return int(cursor.lastrowid)

    def claim_job(self, job_id: str, owner: str, lease_seconds: float = 30.0) -> bool:
        now = utc_now()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            changed = connection.execute(
                "UPDATE research_jobs SET status='running',owner=?,lease_expires=?,"
                "heartbeat_at=?,updated_at=? WHERE id=? AND status IN ('queued','interrupted') "
                "AND cancel_requested=0",
                (owner, lease_deadline(lease_seconds), now, now, job_id),
            ).rowcount
            if changed:
                row = connection.execute(
                    "SELECT attempt FROM research_jobs WHERE id=?", (job_id,)
                ).fetchone()
                connection.execute(
                    "INSERT INTO research_job_events(job_id,attempt,event_json,created_at) "
                    "VALUES (?,?,?,?)",
                    (job_id, int(row["attempt"]), canonical_json({
                        "type": "claimed", "owner": owner,
                    }), now),
                )
        return bool(changed)

    def heartbeat_job(self, job_id: str, owner: str, lease_seconds: float = 30.0) -> bool:
        now = utc_now()
        with self._connect() as connection:
            changed = connection.execute(
                "UPDATE research_jobs SET lease_expires=?,heartbeat_at=?,updated_at=? "
                "WHERE id=? AND owner=? AND status IN ('running','cancelling')",
                (lease_deadline(lease_seconds), now, now, job_id, owner),
            ).rowcount
        return bool(changed)

    def interrupt_owned(self, owner: str) -> int:
        now = utc_now()
        with self._connect() as connection:
            return connection.execute(
                "UPDATE research_jobs SET status='interrupted',owner='',lease_expires=0,"
                "current_task='',updated_at=? WHERE owner=? "
                "AND status IN ('running','cancelling')",
                (now, owner),
            ).rowcount

    def update_job(
        self,
        job_id: str,
        *,
        expected_owner: str | None = None,
        **changes: Any,
    ) -> dict[str, Any]:
        allowed = {
            "status", "next_index", "succeeded", "failed", "cancel_requested",
            "current_task", "failures_json", "manifest_json", "owner", "lease_expires",
            "heartbeat_at", "attempt", "task_indexes_json", "total",
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
        where = "id=?"
        if expected_owner is not None:
            where += " AND owner=?"
            params.append(expected_owner)
        with self._connect() as connection:
            changed = connection.execute(
                f"UPDATE research_jobs SET {','.join(assignments)} WHERE {where}", params
            ).rowcount
        if not changed:
            if expected_owner is not None:
                raise RuntimeError(f"任务租约已丢失：{job_id}")
            raise KeyError(job_id)
        return self.job(job_id) or {}

    @staticmethod
    def _job(row: sqlite3.Row) -> dict[str, Any]:
        value = dict(row)
        value["plan"] = json.loads(value.pop("plan_json"))
        value["failures"] = json.loads(value.pop("failures_json"))
        value["manifest"] = json.loads(value.pop("manifest_json"))
        value["task_indexes"] = json.loads(value.pop("task_indexes_json"))
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

    def job_events(self, job_id: str, after: int = 0, limit: int = 500) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT seq,attempt,event_json,created_at FROM research_job_events "
                "WHERE job_id=? AND seq>? ORDER BY seq LIMIT ?",
                (job_id, max(0, after), max(1, min(limit, 2000))),
            ).fetchall()
        return [{
            "seq": row["seq"], "attempt": row["attempt"],
            "created_at": row["created_at"], **json.loads(row["event_json"]),
        } for row in rows]

    def resume_job(self, job_id: str) -> dict[str, Any]:
        """Create a new immutable attempt without rewriting the original plan."""
        now = utc_now()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            active = connection.execute(
                "SELECT id FROM research_jobs WHERE id<>? "
                "AND status IN ('queued','running','cancelling') LIMIT 1",
                (job_id,),
            ).fetchone()
            if active:
                raise ValueError(f"已有研究数据任务正在运行：{active['id']}")
            row = connection.execute(
                "SELECT status,plan_json,next_index,task_indexes_json,failures_json,attempt "
                "FROM research_jobs WHERE id=?", (job_id,),
            ).fetchone()
            if row is None:
                raise KeyError(job_id)
            status = str(row["status"])
            if status not in {"cancelled", "interrupted", "completed_with_errors"}:
                raise ValueError("当前任务不能续跑")
            task_indexes = json.loads(row["task_indexes_json"] or "[]")
            next_index = int(row["next_index"])
            if status == "completed_with_errors":
                failed_indexes = [
                    int(item["task_index"]) for item in json.loads(row["failures_json"] or "[]")
                    if isinstance(item, dict) and isinstance(item.get("task_index"), int)
                ]
                if not failed_indexes:
                    raise ValueError("没有可重试的数据任务")
                task_indexes = failed_indexes
                next_index = 0
            attempt = int(row["attempt"]) + 1
            connection.execute(
                "UPDATE research_jobs SET status='queued',next_index=?,total=?,succeeded=0,"
                "failed=0,cancel_requested=0,current_task='',failures_json='[]',owner='',"
                "lease_expires=0,heartbeat_at='',attempt=?,task_indexes_json=?,updated_at=? "
                "WHERE id=?",
                (
                    next_index, len(task_indexes), attempt, canonical_json(task_indexes),
                    now, job_id,
                ),
            )
            connection.execute(
                "INSERT INTO research_job_events(job_id,attempt,event_json,created_at) "
                "VALUES (?,?,?,?)",
                (job_id, attempt, canonical_json({
                    "type": "resumed", "previous_status": status,
                }), now),
            )
        return self.job(job_id) or {}
