"""Shared durable-job vocabulary and worker identity helpers."""

from __future__ import annotations

import builtins
import hashlib
import json
import logging
import os
import socket
import sqlite3
import threading
import time
import uuid
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from quantmaster.config import get_config
from quantmaster.runtime.contracts import reject_nonfinite
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.runtime.sqlite import connect_sqlite

logger = logging.getLogger(__name__)

ACTIVE_STATUSES = frozenset({"queued", "running", "cancelling", "interrupted"})
TERMINAL_STATUSES = frozenset(
    {
        "completed",
        "completed_with_errors",
        "failed",
        "cancelled",
    }
)
DEFAULT_LEASE_SECONDS = 30.0


@dataclass(frozen=True)
class WorkerIdentity:
    value: str
    host: str
    pid: int

    @classmethod
    def create(cls, kind: str) -> WorkerIdentity:
        host = socket.gethostname() or "localhost"
        pid = os.getpid()
        suffix = uuid.uuid4().hex[:12]
        return cls(f"{kind}:{host}:{pid}:{suffix}", host, pid)


def lease_deadline(seconds: float = DEFAULT_LEASE_SECONDS) -> float:
    return time.time() + max(5.0, float(seconds))


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical(value: Any) -> str:
    reject_nonfinite(value)
    return strict_json_dumps(value, sort_keys=True)


def _content_hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


class ArtifactIntegrityError(RuntimeError):
    """A persisted artifact no longer matches its committed content hash."""


class JobLeaseLost(RuntimeError):
    """The running worker no longer owns the job lease."""


@dataclass(frozen=True)
class JobOutcome:
    status: str = "completed"
    detail: str = ""
    result_artifact_id: str = ""


class JobHandler(Protocol):
    def __call__(self, context: JobContext, spec: dict[str, Any]) -> JobOutcome: ...


class UnifiedJobStore:
    """Strict job/event/artifact ledger shared by registered runtime task types."""

    def __init__(self, path: Path | None = None):
        self.path = Path(path) if path else get_config().data_root / "jobs.sqlite"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._migrate()

    def _conn(self):
        return connect_sqlite(self.path, timeout=5.0, row_factory=True)

    def _migrate(self) -> None:
        with self._conn() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS runtime_jobs (
                    id TEXT PRIMARY KEY, type TEXT NOT NULL, spec_json TEXT NOT NULL,
                    spec_hash TEXT NOT NULL, idempotency_key TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL, progress INTEGER NOT NULL DEFAULT 0,
                    phase TEXT NOT NULL DEFAULT '', detail TEXT NOT NULL DEFAULT '',
                    attempt INTEGER NOT NULL DEFAULT 1, max_attempts INTEGER NOT NULL DEFAULT 3,
                    owner TEXT NOT NULL DEFAULT '', lease_expires REAL NOT NULL DEFAULT 0,
                    heartbeat_at REAL NOT NULL DEFAULT 0,
                    cancel_requested INTEGER NOT NULL DEFAULT 0,
                    deadline_seconds REAL NOT NULL DEFAULT 300,
                    result_artifact_id TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                    started_at TEXT NOT NULL DEFAULT '', finished_at TEXT NOT NULL DEFAULT '');
                CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_job_idempotency
                    ON runtime_jobs(type,idempotency_key) WHERE idempotency_key<>'';
                CREATE INDEX IF NOT EXISTS idx_runtime_job_status
                    ON runtime_jobs(status,created_at);
                CREATE TABLE IF NOT EXISTS runtime_job_events (
                    job_id TEXT NOT NULL, seq INTEGER NOT NULL, attempt INTEGER NOT NULL,
                    type TEXT NOT NULL, payload_json TEXT NOT NULL, created_at TEXT NOT NULL,
                    PRIMARY KEY(job_id,seq),
                    FOREIGN KEY(job_id) REFERENCES runtime_jobs(id) ON DELETE CASCADE);
                CREATE TABLE IF NOT EXISTS runtime_job_artifacts (
                    id TEXT PRIMARY KEY, job_id TEXT NOT NULL, attempt INTEGER NOT NULL,
                    kind TEXT NOT NULL, schema_version TEXT NOT NULL,
                    payload_json TEXT NOT NULL, content_hash TEXT NOT NULL,
                    lineage_json TEXT NOT NULL, checkpoint_key TEXT NOT NULL DEFAULT '',
                    spec_hash TEXT NOT NULL, created_at TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES runtime_jobs(id) ON DELETE CASCADE);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_runtime_checkpoint
                    ON runtime_job_artifacts(job_id,attempt,checkpoint_key)
                    WHERE checkpoint_key<>'';
                CREATE INDEX IF NOT EXISTS idx_runtime_artifact_kind
                    ON runtime_job_artifacts(job_id,kind,attempt);
                CREATE TABLE IF NOT EXISTS runtime_artifact_repairs (
                    id TEXT PRIMARY KEY, job_id TEXT NOT NULL, artifact_id TEXT NOT NULL,
                    reason TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'queued',
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                    UNIQUE(artifact_id,status));
            """)

    @staticmethod
    def _decode_job(row: Any) -> dict[str, Any] | None:
        if row is None:
            return None
        value = dict(row)
        value["spec"] = json.loads(value.pop("spec_json"))
        value["cancel_requested"] = bool(value["cancel_requested"])
        return value

    @staticmethod
    def _append_event_conn(
        connection: Any,
        job_id: str,
        attempt: int,
        event_type: str,
        payload: Any,
    ) -> int:
        row = connection.execute(
            "SELECT COALESCE(MAX(seq),0)+1 FROM runtime_job_events WHERE job_id=?",
            (job_id,),
        ).fetchone()
        seq = int(row[0])
        connection.execute(
            "INSERT INTO runtime_job_events "
            "(job_id,seq,attempt,type,payload_json,created_at) VALUES (?,?,?,?,?,?)",
            (job_id, seq, attempt, event_type, _canonical(payload), _utc_now()),
        )
        return seq

    def submit(
        self,
        job_type: str,
        spec: Mapping[str, Any],
        *,
        idempotency_key: str = "",
        deadline_seconds: float = 300,
        max_attempts: int = 3,
    ) -> tuple[dict[str, Any], bool]:
        normalized = dict(spec)
        spec_json = _canonical(normalized)
        spec_hash = hashlib.sha256(spec_json.encode("utf-8")).hexdigest()
        key = str(idempotency_key or "").strip()[:200]
        now = _utc_now()
        with self._conn() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if key:
                row = connection.execute(
                    "SELECT * FROM runtime_jobs WHERE type=? AND idempotency_key=?",
                    (job_type, key),
                ).fetchone()
                if row is not None:
                    existing = self._decode_job(row)
                    if existing and existing["spec_hash"] != spec_hash:
                        raise ValueError("Idempotency-Key 已绑定到不同任务规格")
                    return existing or {}, False
            job_id = f"job_{uuid.uuid4().hex}"
            connection.execute(
                "INSERT INTO runtime_jobs "
                "(id,type,spec_json,spec_hash,idempotency_key,status,attempt,max_attempts,"
                "deadline_seconds,created_at,updated_at) VALUES (?,?,?,?,?,'queued',1,?,?,?,?)",
                (
                    job_id,
                    str(job_type),
                    spec_json,
                    spec_hash,
                    key,
                    max(1, min(10, int(max_attempts))),
                    max(1.0, min(3600.0, float(deadline_seconds))),
                    now,
                    now,
                ),
            )
            self._append_event_conn(
                connection,
                job_id,
                1,
                "job_queued",
                {"task_type": job_type, "spec_hash": spec_hash},
            )
            row = connection.execute(
                "SELECT * FROM runtime_jobs WHERE id=?",
                (job_id,),
            ).fetchone()
        return self._decode_job(row) or {}, True

    def get(self, job_id: str) -> dict[str, Any]:
        with self._conn() as connection:
            row = connection.execute(
                "SELECT * FROM runtime_jobs WHERE id=?",
                (job_id,),
            ).fetchone()
        value = self._decode_job(row)
        if value is None:
            raise KeyError(job_id)
        return value

    def list(self, limit: int = 100, *, job_type: str = "") -> list[dict[str, Any]]:
        query = "SELECT * FROM runtime_jobs"
        params: tuple[Any, ...]
        if job_type:
            query += " WHERE type=?"
            params = (job_type, max(1, min(1000, int(limit))))
        else:
            params = (max(1, min(1000, int(limit))),)
        query += " ORDER BY created_at DESC LIMIT ?"
        with self._conn() as connection:
            rows = connection.execute(query, params).fetchall()
        return [self._decode_job(row) or {} for row in rows]

    def events(
        self, job_id: str, after: int = 0, limit: int = 500,
    ) -> builtins.list[dict[str, Any]]:
        self.get(job_id)
        with self._conn() as connection:
            rows = connection.execute(
                "SELECT * FROM runtime_job_events WHERE job_id=? AND seq>? ORDER BY seq LIMIT ?",
                (job_id, max(0, int(after)), max(1, min(2000, int(limit)))),
            ).fetchall()
        return [
            {
                "job_id": job_id,
                "seq": int(row["seq"]),
                "attempt": int(row["attempt"]),
                "type": row["type"],
                "payload": json.loads(row["payload_json"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def append_event(self, job_id: str, event_type: str, payload: Any) -> int:
        with self._conn() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT attempt FROM runtime_jobs WHERE id=?",
                (job_id,),
            ).fetchone()
            if row is None:
                raise KeyError(job_id)
            return self._append_event_conn(
                connection,
                job_id,
                int(row["attempt"]),
                event_type,
                payload,
            )

    def recover_expired(self) -> builtins.list[str]:
        now = time.time()
        recovered: list[str] = []
        with self._conn() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                "SELECT id,attempt FROM runtime_jobs WHERE status IN ('running','cancelling') "
                "AND lease_expires<=?",
                (now,),
            ).fetchall()
            for row in rows:
                connection.execute(
                    "UPDATE runtime_jobs SET status='interrupted',owner='',lease_expires=0,"
                    "phase='等待恢复',detail='worker lease expired',updated_at=? WHERE id=?",
                    (_utc_now(), row["id"]),
                )
                self._append_event_conn(
                    connection,
                    row["id"],
                    int(row["attempt"]),
                    "job_interrupted",
                    {"reason": "worker lease expired"},
                )
                recovered.append(str(row["id"]))
        return recovered

    def claim(self, job_id: str, owner: str, lease_seconds: float = DEFAULT_LEASE_SECONDS) -> bool:
        now = time.time()
        with self._conn() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                "UPDATE runtime_jobs SET status='running',owner=?,lease_expires=?,"
                "heartbeat_at=?,started_at=CASE WHEN started_at='' THEN ? ELSE started_at END,"
                "phase='开始执行',detail='',updated_at=? WHERE id=? "
                "AND status IN ('queued','interrupted') AND lease_expires<=?",
                (owner, lease_deadline(lease_seconds), now, _utc_now(), _utc_now(), job_id, now),
            )
            if cursor.rowcount != 1:
                return False
            attempt = int(
                connection.execute(
                    "SELECT attempt FROM runtime_jobs WHERE id=?",
                    (job_id,),
                ).fetchone()[0]
            )
            self._append_event_conn(
                connection,
                job_id,
                attempt,
                "job_started",
                {"owner": owner},
            )
        return True

    def heartbeat(
        self,
        job_id: str,
        owner: str,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
    ) -> bool:
        now = time.time()
        with self._conn() as connection:
            cursor = connection.execute(
                "UPDATE runtime_jobs SET lease_expires=?,heartbeat_at=?,updated_at=? "
                "WHERE id=? AND owner=? AND status IN ('running','cancelling')",
                (lease_deadline(lease_seconds), now, _utc_now(), job_id, owner),
            )
        return cursor.rowcount == 1

    def progress(
        self,
        job_id: str,
        owner: str,
        progress: int,
        phase: str,
        detail: str = "",
    ) -> None:
        with self._conn() as connection:
            cursor = connection.execute(
                "UPDATE runtime_jobs SET progress=?,phase=?,detail=?,updated_at=? "
                "WHERE id=? AND owner=? AND status IN ('running','cancelling')",
                (
                    max(0, min(100, int(progress))),
                    str(phase)[:200],
                    str(detail)[:1000],
                    _utc_now(),
                    job_id,
                    owner,
                ),
            )
        if cursor.rowcount != 1:
            raise JobLeaseLost(job_id)

    def cancel(self, job_id: str) -> dict[str, Any]:
        with self._conn() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM runtime_jobs WHERE id=?",
                (job_id,),
            ).fetchone()
            if row is None:
                raise KeyError(job_id)
            status = str(row["status"])
            if status in TERMINAL_STATUSES:
                return self._decode_job(row) or {}
            terminal = status in {"queued", "interrupted"}
            next_status = "cancelled" if terminal else "cancelling"
            connection.execute(
                "UPDATE runtime_jobs SET status=?,cancel_requested=1,phase='正在取消',"
                "finished_at=CASE WHEN ? THEN ? ELSE finished_at END,updated_at=? WHERE id=?",
                (next_status, int(terminal), _utc_now(), _utc_now(), job_id),
            )
            self._append_event_conn(
                connection,
                job_id,
                int(row["attempt"]),
                "job_cancel_requested",
                {},
            )
        return self.get(job_id)

    def cancelled(self, job_id: str, owner: str = "") -> bool:
        with self._conn() as connection:
            if owner:
                row = connection.execute(
                    "SELECT cancel_requested,owner FROM runtime_jobs WHERE id=?",
                    (job_id,),
                ).fetchone()
                if row is None or str(row["owner"]) != owner:
                    raise JobLeaseLost(job_id)
            else:
                row = connection.execute(
                    "SELECT cancel_requested FROM runtime_jobs WHERE id=?",
                    (job_id,),
                ).fetchone()
        if row is None:
            raise KeyError(job_id)
        return bool(row["cancel_requested"])

    def finish(
        self,
        job_id: str,
        owner: str,
        outcome: JobOutcome,
    ) -> dict[str, Any]:
        allowed = {*TERMINAL_STATUSES, "interrupted"}
        if outcome.status not in allowed:
            raise ValueError("任务终态非法")
        with self._conn() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT attempt FROM runtime_jobs WHERE id=? AND owner=?",
                (job_id, owner),
            ).fetchone()
            if row is None:
                raise JobLeaseLost(job_id)
            terminal = outcome.status in TERMINAL_STATUSES
            progress = 100 if outcome.status in {"completed", "completed_with_errors"} else None
            connection.execute(
                "UPDATE runtime_jobs SET status=?,progress=COALESCE(?,progress),phase=?,detail=?,"
                "result_artifact_id=?,owner='',lease_expires=0,finished_at=?,updated_at=? "
                "WHERE id=? AND owner=?",
                (
                    outcome.status,
                    progress,
                    "分析完成" if outcome.status.startswith("completed") else outcome.status,
                    outcome.detail[:1000],
                    outcome.result_artifact_id,
                    _utc_now() if terminal else "",
                    _utc_now(),
                    job_id,
                    owner,
                ),
            )
            self._append_event_conn(
                connection,
                job_id,
                int(row["attempt"]),
                "job_terminal",
                {"status": outcome.status, "detail": outcome.detail[:1000]},
            )
        return self.get(job_id)

    def interrupt_owned(self, owner: str) -> builtins.list[str]:
        interrupted: list[str] = []
        with self._conn() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                "SELECT id,attempt FROM runtime_jobs WHERE owner=? AND status IN ('running','cancelling')",
                (owner,),
            ).fetchall()
            for row in rows:
                connection.execute(
                    "UPDATE runtime_jobs SET status='interrupted',owner='',lease_expires=0,"
                    "phase='等待恢复',updated_at=? WHERE id=? AND owner=?",
                    (_utc_now(), row["id"], owner),
                )
                self._append_event_conn(
                    connection,
                    row["id"],
                    int(row["attempt"]),
                    "job_interrupted",
                    {"reason": "worker stopped"},
                )
                interrupted.append(str(row["id"]))
        return interrupted

    def retry(self, job_id: str) -> dict[str, Any]:
        with self._conn() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT status,attempt,max_attempts FROM runtime_jobs WHERE id=?",
                (job_id,),
            ).fetchone()
            if row is None:
                raise KeyError(job_id)
            if str(row["status"]) not in {*TERMINAL_STATUSES, "interrupted"}:
                raise ValueError("当前任务不能重试")
            attempt = int(row["attempt"]) + 1
            if attempt > int(row["max_attempts"]):
                raise ValueError("任务已达到最大尝试次数")
            connection.execute(
                "UPDATE runtime_jobs SET status='queued',progress=0,phase='等待重试',detail='',"
                "attempt=?,owner='',lease_expires=0,heartbeat_at=0,cancel_requested=0,"
                "result_artifact_id='',finished_at='',updated_at=? WHERE id=?",
                (attempt, _utc_now(), job_id),
            )
            self._append_event_conn(
                connection,
                job_id,
                attempt,
                "job_retried",
                {"attempt": attempt},
            )
        return self.get(job_id)

    def write_artifact(
        self,
        job_id: str,
        kind: str,
        payload: Any,
        metadata: Mapping[str, Any] | None = None,
        *,
        checkpoint_key: str = "",
    ) -> dict[str, Any]:
        job = self.get(job_id)
        body = json.loads(_canonical(payload))
        digest = _content_hash(body)
        values = dict(metadata or {})
        declared = str(values.get("content_hash") or "")
        if declared and declared != digest:
            raise ValueError("产物声明哈希与内容不一致")
        artifact_id = f"artifact_{uuid.uuid4().hex}"
        lineage = values.get("lineage") or {}
        if not isinstance(lineage, Mapping):
            raise ValueError("产物血缘必须是 JSON 对象")
        schema = str(values.get("schema_version") or body.get("schema_version") or "1.0")
        with self._conn() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if checkpoint_key:
                connection.execute(
                    "DELETE FROM runtime_job_artifacts WHERE job_id=? AND attempt=? AND checkpoint_key=?",
                    (job_id, int(job["attempt"]), checkpoint_key),
                )
            connection.execute(
                "INSERT INTO runtime_job_artifacts "
                "(id,job_id,attempt,kind,schema_version,payload_json,content_hash,lineage_json,"
                "checkpoint_key,spec_hash,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    artifact_id,
                    job_id,
                    int(job["attempt"]),
                    str(kind)[:200],
                    schema[:50],
                    _canonical(body),
                    digest,
                    _canonical(lineage),
                    checkpoint_key[:200],
                    job["spec_hash"],
                    _utc_now(),
                ),
            )
        return {
            "id": artifact_id,
            "kind": str(kind),
            "schema_version": schema,
            "content_hash": digest,
            "lineage": lineage,
        }

    def _queue_repair(self, artifact: Mapping[str, Any], reason: str) -> None:
        now = _utc_now()
        with self._conn() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO runtime_artifact_repairs "
                "(id,job_id,artifact_id,reason,status,created_at,updated_at) "
                "VALUES (?,?,?,?, 'queued',?,?)",
                (
                    f"repair_{uuid.uuid4().hex}",
                    artifact["job_id"],
                    artifact["id"],
                    reason[:1000],
                    now,
                    now,
                ),
            )

    def artifact(self, artifact_id: str) -> dict[str, Any]:
        with self._conn() as connection:
            row = connection.execute(
                "SELECT * FROM runtime_job_artifacts WHERE id=?",
                (artifact_id,),
            ).fetchone()
        if row is None:
            raise KeyError(artifact_id)
        value = dict(row)
        try:
            payload = json.loads(value["payload_json"])
            lineage = json.loads(value["lineage_json"])
            if _content_hash(payload) != value["content_hash"]:
                raise ArtifactIntegrityError("产物内容哈希校验失败")
            if not isinstance(lineage, dict):
                raise ArtifactIntegrityError("产物血缘不是 JSON 对象")
        except (json.JSONDecodeError, TypeError, ValueError, ArtifactIntegrityError) as exc:
            self._queue_repair(value, str(exc))
            raise ArtifactIntegrityError(str(exc)) from exc
        return {
            **value,
            "payload": payload,
            "lineage": lineage,
        }

    def latest_artifact(self, job_id: str, kind: str) -> dict[str, Any] | None:
        with self._conn() as connection:
            rows = connection.execute(
                "SELECT id FROM runtime_job_artifacts WHERE job_id=? AND kind=? "
                "ORDER BY attempt DESC,created_at DESC",
                (job_id, kind),
            ).fetchall()
        for row in rows:
            try:
                return self.artifact(str(row["id"]))
            except ArtifactIntegrityError:
                continue
        return None

    def checkpoint(self, job_id: str, key: str, spec_hash: str) -> dict[str, Any] | None:
        job = self.get(job_id)
        if job["spec_hash"] != spec_hash:
            return None
        with self._conn() as connection:
            rows = connection.execute(
                "SELECT id,spec_hash FROM runtime_job_artifacts WHERE job_id=? "
                "AND checkpoint_key=? ORDER BY attempt DESC,created_at DESC",
                (job_id, key),
            ).fetchall()
        for row in rows:
            if str(row["spec_hash"]) != spec_hash:
                continue
            try:
                return self.artifact(str(row["id"]))["payload"]
            except ArtifactIntegrityError:
                continue
        return None

    def repairs(self, limit: int = 100) -> builtins.list[dict[str, Any]]:
        with self._conn() as connection:
            rows = connection.execute(
                "SELECT * FROM runtime_artifact_repairs ORDER BY created_at DESC LIMIT ?",
                (max(1, min(1000, int(limit))),),
            ).fetchall()
        return [dict(row) for row in rows]


class JobContext:
    def __init__(
        self,
        runtime: UnifiedJobRuntime,
        job: dict[str, Any],
        lease_alive: threading.Event,
    ):
        self.runtime = runtime
        self.store = runtime.store
        self.job_id = str(job["id"])
        self.spec_hash = str(job["spec_hash"])
        self.attempt = int(job["attempt"])
        self.deadline_seconds = float(job["deadline_seconds"])
        self._lease_alive = lease_alive

    def emit(self, event_type: str, payload: Mapping[str, Any] | None = None) -> int:
        self.ensure_active()
        return self.store.append_event(self.job_id, event_type, dict(payload or {}))

    def progress(self, value: int, phase: str, detail: str = "") -> None:
        self.ensure_active()
        self.store.progress(self.job_id, self.runtime.identity.value, value, phase, detail)

    def cancelled(self) -> bool:
        if not self._lease_alive.is_set() or self.runtime.stopping:
            return True
        return self.store.cancelled(self.job_id, self.runtime.identity.value)

    def ensure_active(self) -> None:
        if not self._lease_alive.is_set():
            raise JobLeaseLost(self.job_id)
        if self.runtime.stopping:
            raise InterruptedError("worker stopped")
        if self.store.cancelled(self.job_id, self.runtime.identity.value):
            raise InterruptedError("job cancelled")

    def write_artifact(
        self,
        kind: str,
        payload: dict[str, Any],
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        self.ensure_active()
        return self.store.write_artifact(self.job_id, kind, payload, metadata)

    def load_checkpoint(self, key: str, spec_hash: str) -> dict[str, Any] | None:
        self.ensure_active()
        return self.store.checkpoint(self.job_id, key, spec_hash)

    def write_checkpoint(self, key: str, spec_hash: str, payload: dict[str, Any]) -> dict[str, Any]:
        self.ensure_active()
        if spec_hash != self.spec_hash:
            raise ValueError("检查点规格与任务规格不一致")
        return self.store.write_artifact(
            self.job_id,
            f"checkpoint.{key}",
            payload,
            {
                "schema_version": payload.get("schema_version") or "1.0",
                "lineage": {"spec_hash": spec_hash, "checkpoint_key": key},
            },
            checkpoint_key=key,
        )


class UnifiedJobRuntime:
    """Handler registry and lease-aware worker pool for extensible task types."""

    def __init__(self, store: UnifiedJobStore | None = None, *, max_workers: int = 2):
        self.store = store or UnifiedJobStore()
        self.identity = WorkerIdentity.create("unified-jobs")
        self._handlers: dict[str, JobHandler] = {}
        self._executor = ThreadPoolExecutor(
            max_workers=max(1, min(8, int(max_workers))),
            thread_name_prefix="qm-unified-job",
        )
        self._active: set[str] = set()
        self._lock = threading.RLock()
        self._started = False
        self._paused = threading.Event()
        self._stop = threading.Event()

    @property
    def stopping(self) -> bool:
        return self._stop.is_set() or self._paused.is_set()

    @property
    def idle(self) -> bool:
        with self._lock:
            return not self._active

    def register(self, job_type: str, handler: JobHandler) -> None:
        name = str(job_type).strip()
        if not name or not callable(handler):
            raise ValueError("任务类型和 handler 不能为空")
        with self._lock:
            existing = self._handlers.get(name)
            if existing is not None and existing is not handler:
                raise ValueError(f"任务类型已注册：{name}")
            self._handlers[name] = handler
            should_schedule = self._started
        if should_schedule:
            for job in self.store.list(1000, job_type=name):
                if job["status"] in {"queued", "interrupted"}:
                    self._schedule(job["id"])

    def start(self) -> None:
        with self._lock:
            if self._stop.is_set():
                raise RuntimeError("任务运行时已经永久停止")
            if self._started:
                if not self._paused.is_set():
                    return
            self._paused.clear()
            self._started = True
        self.store.recover_expired()
        for job in self.store.list(1000):
            if job["status"] in {"queued", "interrupted"} and job["type"] in self._handlers:
                self._schedule(job["id"])

    def submit(
        self,
        job_type: str,
        spec: Mapping[str, Any],
        *,
        idempotency_key: str = "",
        deadline_seconds: float = 300,
    ) -> tuple[dict[str, Any], bool]:
        if self.stopping:
            raise RuntimeError("任务运行时正在维护或已经停止")
        if job_type not in self._handlers:
            raise ValueError(f"任务类型未注册：{job_type}")
        job, created = self.store.submit(
            job_type,
            spec,
            idempotency_key=idempotency_key,
            deadline_seconds=deadline_seconds,
        )
        self.start()
        if job["status"] in {"queued", "interrupted"}:
            self._schedule(job["id"])
        return job, created

    def _schedule(self, job_id: str) -> None:
        with self._lock:
            if self.stopping or job_id in self._active:
                return
            self._active.add(job_id)
        self._executor.submit(self._run, job_id)

    def _heartbeat(
        self,
        job_id: str,
        stopped: threading.Event,
        lease_alive: threading.Event,
    ) -> None:
        while not stopped.wait(5.0):
            if self.store.heartbeat(job_id, self.identity.value):
                continue
            lease_alive.clear()
            return

    def _run(self, job_id: str) -> None:
        heartbeat_stop = threading.Event()
        lease_alive = threading.Event()
        lease_alive.set()
        heartbeat: threading.Thread | None = None
        try:
            if self.stopping:
                return
            if not self.store.claim(job_id, self.identity.value):
                return
            job = self.store.get(job_id)
            handler = self._handlers.get(str(job["type"]))
            if handler is None:
                self.store.finish(
                    job_id,
                    self.identity.value,
                    JobOutcome("failed", f"任务类型未注册：{job['type']}"),
                )
                return
            heartbeat = threading.Thread(
                target=self._heartbeat,
                args=(job_id, heartbeat_stop, lease_alive),
                name=f"unified-heartbeat-{job_id[-8:]}",
                daemon=True,
            )
            heartbeat.start()
            context = JobContext(self, job, lease_alive)
            try:
                outcome = handler(context, dict(job["spec"]))
                context.ensure_active()
            except JobLeaseLost:
                return
            except InterruptedError as exc:
                if not lease_alive.is_set():
                    return
                if self.stopping:
                    return
                outcome = JobOutcome("cancelled", str(exc))
            except (
                ArithmeticError,
                ImportError,
                LookupError,
                OSError,
                RuntimeError,
                sqlite3.Error,
                TypeError,
            ) as exc:
                logger.exception(
                    "Unified job handler failed job_id=%s type=%s",
                    job_id,
                    job.get("type"),
                )
                outcome = JobOutcome("failed", str(exc)[:1000])
            self.store.finish(job_id, self.identity.value, outcome)
        finally:
            heartbeat_stop.set()
            if heartbeat is not None:
                heartbeat.join(timeout=1.0)
            with self._lock:
                self._active.discard(job_id)

    def retry(self, job_id: str) -> dict[str, Any]:
        if self.stopping:
            raise RuntimeError("任务运行时正在维护或已经停止")
        job = self.store.retry(job_id)
        self.start()
        self._schedule(job_id)
        return job

    def pause(self) -> None:
        """Stop accepting work and durably interrupt owned jobs for maintenance."""
        self._paused.set()
        self.store.interrupt_owned(self.identity.value)

    def resume(self) -> None:
        """Resume interrupted jobs after a bounded maintenance window."""
        self.start()

    def stop(self) -> None:
        self._stop.set()
        self._paused.set()
        self.store.interrupt_owned(self.identity.value)
        self._executor.shutdown(wait=False, cancel_futures=True)

    def public(self, job: dict[str, Any]) -> dict[str, Any]:
        elapsed = 0.0
        if job.get("started_at"):
            try:
                elapsed = max(0.0, time.time() - datetime.fromisoformat(job["started_at"]).timestamp())
            except ValueError:
                elapsed = 0.0
        progress = max(0, min(100, int(job.get("progress") or 0)))
        if job["status"] in TERMINAL_STATUSES:
            remaining = 0.0
        elif progress > 2:
            remaining = min(
                float(job["deadline_seconds"]),
                elapsed * max(0, 100 - progress) / progress,
            )
        else:
            remaining = max(0.0, float(job["deadline_seconds"]) - elapsed)
        return {
            "domain": str(job["type"]).partition(".")[0] or "runtime",
            "id": job["id"],
            "type": job["type"],
            "status": job["status"],
            "progress": progress,
            "phase": job.get("phase") or "",
            "detail": job.get("detail") or "",
            "attempt": int(job["attempt"]),
            "cancel_requested": bool(job.get("cancel_requested")),
            "created_at": job["created_at"],
            "updated_at": job["updated_at"],
            "estimated_remaining_seconds": round(max(0.0, remaining)),
            "can_cancel": job["status"] in ACTIVE_STATUSES,
            "can_retry": (
                job["status"] in {*TERMINAL_STATUSES, "interrupted"}
                and int(job["attempt"]) < int(job["max_attempts"])
            ),
            "links": {
                "self": f"/api/v1/jobs/{job['id']}",
                "events": f"/api/v1/jobs/{job['id']}/events",
                "cancel": f"/api/v1/jobs/{job['id']}/cancel",
                "retry": f"/api/v1/jobs/{job['id']}/retry",
            },
        }
