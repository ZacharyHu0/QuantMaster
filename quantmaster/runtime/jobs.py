"""Shared durable-job vocabulary and worker identity helpers."""

from __future__ import annotations

import builtins
import hashlib
import importlib
import json
import logging
import multiprocessing
import os
import queue
import socket
import sqlite3
import tempfile
import threading
import time
import traceback
import uuid
import zlib
from collections.abc import Callable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
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
INLINE_ARTIFACT_LIMIT = 128 * 1024
_CPU_JOB_GATE = threading.Semaphore(1)


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
    return datetime.now(UTC).isoformat()


def _canonical(value: Any) -> str:
    reject_nonfinite(value)
    return strict_json_dumps(value, sort_keys=True)


def _content_hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


class ArtifactIntegrityError(RuntimeError):
    """A persisted artifact no longer matches its committed content hash."""


class JobLeaseLost(RuntimeError):
    """The running worker no longer owns the job lease."""


class JobDeadlineExceeded(RuntimeError):
    """The current attempt exceeded its declared execution deadline."""


@dataclass(frozen=True)
class JobOutcome:
    status: str = "completed"
    detail: str = ""
    result_artifact_id: str = ""


class JobHandler(Protocol):
    def __call__(self, context: JobContext, spec: dict[str, Any]) -> JobOutcome: ...


@dataclass(frozen=True)
class _HandlerRegistration:
    """Execution declaration for one task type.

    ``process_entrypoint`` is deliberately an importable ``module:qualname``
    instead of a pickled callable.  Windows uses ``spawn`` and a bound service
    object commonly captures locks, database connections or web state.  An
    import path gives the compute child a clean interpreter while the parent
    remains the sole lease owner.
    """

    handler: JobHandler
    process_entrypoint: str = ""


class UnifiedJobStore:
    """Strict job/event/artifact ledger shared by registered runtime task types."""

    def __init__(self, path: Path | None = None, *, read_only: bool = False):
        self.path = Path(path) if path else get_config().data_root / "jobs.sqlite"
        self.read_only = bool(read_only)
        # Keep bulky result/checkpoint bodies out of the hot SQLite ledger.
        # The manifest remains transactional in SQLite, while the immutable
        # body is content-addressed beneath the same data root.
        self.artifacts_root = self.path.parent / "derived" / "job-artifacts"
        # Web generations use this class for task status polling.  Opening a
        # nonexistent ledger must be a fast read failure, not an implicit
        # schema migration or a new directory tree.
        if not self.read_only:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.artifacts_root.mkdir(parents=True, exist_ok=True)
            self._migrate()

    def _conn(self):
        return connect_sqlite(
            self.path,
            timeout=0.25 if self.read_only else 5.0,
            row_factory=True,
            read_only=self.read_only,
        )

    def _migrate(self) -> None:
        with self._conn() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS runtime_jobs (
                    id TEXT PRIMARY KEY, type TEXT NOT NULL, spec_json TEXT NOT NULL,
                    spec_hash TEXT NOT NULL, idempotency_key TEXT NOT NULL DEFAULT '',
                    input_fingerprint TEXT NOT NULL DEFAULT '',
                    algorithm_version TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL, progress INTEGER NOT NULL DEFAULT 0,
                    phase TEXT NOT NULL DEFAULT '', detail TEXT NOT NULL DEFAULT '',
                    attempt INTEGER NOT NULL DEFAULT 1, max_attempts INTEGER NOT NULL DEFAULT 2,
                    owner TEXT NOT NULL DEFAULT '', lease_expires REAL NOT NULL DEFAULT 0,
                    lease_token TEXT NOT NULL DEFAULT '',
                    heartbeat_at REAL NOT NULL DEFAULT 0,
                    cancel_requested INTEGER NOT NULL DEFAULT 0,
                    llm_scope TEXT NOT NULL DEFAULT '', llm_revision TEXT NOT NULL DEFAULT '',
                    cancellation_reason TEXT NOT NULL DEFAULT '',
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
                    external_path TEXT NOT NULL DEFAULT '', payload_bytes INTEGER NOT NULL DEFAULT 0,
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
            columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(runtime_jobs)").fetchall()
            }
            for name, declaration in {
                "input_fingerprint": "TEXT NOT NULL DEFAULT ''",
                "algorithm_version": "TEXT NOT NULL DEFAULT ''",
                "lease_token": "TEXT NOT NULL DEFAULT ''",
                "llm_scope": "TEXT NOT NULL DEFAULT ''",
                "llm_revision": "TEXT NOT NULL DEFAULT ''",
                "cancellation_reason": "TEXT NOT NULL DEFAULT ''",
            }.items():
                if name not in columns:
                    connection.execute(
                        f"ALTER TABLE runtime_jobs ADD COLUMN {name} {declaration}"
                    )
            artifact_columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(runtime_job_artifacts)").fetchall()
            }
            for name, declaration in {
                "external_path": "TEXT NOT NULL DEFAULT ''",
                "payload_bytes": "INTEGER NOT NULL DEFAULT 0",
            }.items():
                if name not in artifact_columns:
                    connection.execute(
                        f"ALTER TABLE runtime_job_artifacts ADD COLUMN {name} {declaration}"
                    )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_runtime_job_singleflight "
                "ON runtime_jobs(type,spec_hash,input_fingerprint,algorithm_version,status,created_at)"
            )

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
        input_fingerprint: str = "",
        algorithm_version: str = "",
        deadline_seconds: float = 300,
        max_attempts: int = 2,
        llm_scope: str = "",
        llm_revision: str = "",
    ) -> tuple[dict[str, Any], bool]:
        normalized = dict(spec)
        spec_json = _canonical(normalized)
        spec_hash = hashlib.sha256(spec_json.encode("utf-8")).hexdigest()
        key = str(idempotency_key or "").strip()[:200]
        fingerprint = str(input_fingerprint or "")[:200]
        algorithm = str(algorithm_version or "")[:120]
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
                    value = existing or {}
                    value["created"] = False
                    value["coalesced"] = True
                    return value, False
            # Singleflight is deliberately independent from a client supplied
            # idempotency key.  A scheduler, a manual click and startup
            # recovery with the same canonical input must share one job.
            existing_row = connection.execute(
                "SELECT * FROM runtime_jobs WHERE type=? AND spec_hash=? "
                "AND input_fingerprint=? AND algorithm_version=? "
                "AND status IN ('queued','running','cancelling','interrupted') "
                "ORDER BY created_at DESC LIMIT 1",
                (str(job_type), spec_hash, fingerprint, algorithm),
            ).fetchone()
            if existing_row is not None:
                existing = self._decode_job(existing_row) or {}
                existing["created"] = False
                existing["coalesced"] = True
                return existing, False
            # A completed immutable artifact is a valid singleflight result as
            # well.  Reusing it avoids re-running a provider/CPU pipeline when
            # the canonical specification, input generation and algorithm are
            # identical.  Do not apply this fallback to legacy callers that
            # have not supplied a real versioned input fingerprint.
            if fingerprint and algorithm:
                completed_row = connection.execute(
                    "SELECT * FROM runtime_jobs WHERE type=? AND spec_hash=? "
                    "AND input_fingerprint=? AND algorithm_version=? "
                    "AND status='completed' AND result_artifact_id<>'' "
                    "ORDER BY finished_at DESC LIMIT 1",
                    (str(job_type), spec_hash, fingerprint, algorithm),
                ).fetchone()
                if completed_row is not None:
                    existing = self._decode_job(completed_row) or {}
                    existing["created"] = False
                    existing["coalesced"] = True
                    existing["reused"] = True
                    existing["outcome"] = "unchanged"
                    return existing, False
            job_id = f"job_{uuid.uuid4().hex}"
            connection.execute(
                "INSERT INTO runtime_jobs "
                "(id,type,spec_json,spec_hash,idempotency_key,input_fingerprint,algorithm_version,"
                "status,attempt,max_attempts,deadline_seconds,llm_scope,llm_revision,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?,'queued',1,?,?,?,?,?,?)",
                (
                    job_id,
                    str(job_type),
                    spec_json,
                    spec_hash,
                    key,
                    fingerprint,
                    algorithm,
                    max(1, min(10, int(max_attempts))),
                    max(1.0, min(3600.0, float(deadline_seconds))),
                    str(llm_scope)[:40],
                    str(llm_revision)[:120],
                    now,
                    now,
                ),
            )
            self._append_event_conn(
                connection,
                job_id,
                1,
                "job_queued",
                {
                    "task_type": job_type,
                    "spec_hash": spec_hash,
                    "input_fingerprint": fingerprint,
                    "algorithm_version": algorithm,
                },
            )
            row = connection.execute(
                "SELECT * FROM runtime_jobs WHERE id=?",
                (job_id,),
            ).fetchone()
        value = self._decode_job(row) or {}
        value["created"] = True
        value["coalesced"] = False
        return value, True

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

    def append_event(
        self,
        job_id: str,
        event_type: str,
        payload: Any,
        *,
        owner: str = "",
        lease_token: str = "",
    ) -> int:
        with self._conn() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if owner:
                if not lease_token:
                    raise JobLeaseLost(job_id)
                row = connection.execute(
                    "SELECT attempt FROM runtime_jobs WHERE id=? AND owner=? AND lease_token=? "
                    "AND status IN ('running','cancelling') AND lease_expires>?",
                    (job_id, owner, str(lease_token), time.time()),
                ).fetchone()
            else:
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
                "SELECT id,attempt,max_attempts FROM runtime_jobs WHERE status IN ('running','cancelling') "
                "AND lease_expires<=?",
                (now,),
            ).fetchall()
            for row in rows:
                if int(row["attempt"]) >= int(row["max_attempts"]):
                    connection.execute(
                        "UPDATE runtime_jobs SET status='failed',owner='',lease_token='',lease_expires=0,"
                        "phase='执行失败',detail='worker lease expired after maximum attempts',"
                        "finished_at=?,updated_at=? WHERE id=?",
                        (_utc_now(), _utc_now(), row["id"]),
                    )
                    self._append_event_conn(
                        connection,
                        row["id"],
                        int(row["attempt"]),
                        "job_terminal",
                        {"status": "failed", "detail": "worker lease expired after maximum attempts"},
                    )
                    recovered.append(str(row["id"]))
                    continue
                connection.execute(
                    "UPDATE runtime_jobs SET status='interrupted',owner='',lease_token='',lease_expires=0,"
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

    @staticmethod
    def _field(value: Mapping[str, Any] | Any, name: str) -> Any:
        try:
            return value.get(name) if hasattr(value, "get") else value[name]
        except (KeyError, IndexError):
            return ""

    @classmethod
    def _legacy_llm_without_revision(cls, job: Mapping[str, Any] | Any) -> bool:
        """Identify only persisted task kinds that may invoke a model.

        A skip-LLM crawl and settings.apply are normal recoverable jobs.  Older
        model work without a revision, however, must never silently resume
        against a different credential or provider configuration.
        """
        if str(cls._field(job, "llm_scope") or "") and str(cls._field(job, "llm_revision") or ""):
            return False
        task_type = str(cls._field(job, "type") or "")
        if task_type in {
            "news.reanalyze", "settings.diagnostic", "market.stock_analysis",
            "lab.discover_llm", "lab.cloud_suggestion",
            "automation.contextual_chat", "automation.conversation_compaction",
            "automation.fast_news_scan", "automation.official_news_scan",
            "automation.periodic_news_scan", "automation.news_dead_letter_recovery",
        }:
            return True
        if task_type not in {"news.crawl", "news.source_run"}:
            return False
        spec = cls._field(job, "spec")
        if not isinstance(spec, Mapping):
            try:
                spec = json.loads(str(cls._field(job, "spec_json") or "{}"))
            except (TypeError, ValueError):
                return True
        return not bool(spec.get("skip_llm"))

    def interrupt_legacy_llm(self) -> int:
        """Mark unrevisioned legacy provider work for an explicit manual retry."""
        with self._conn() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                "SELECT * FROM runtime_jobs WHERE llm_scope='' AND status "
                "IN ('queued','interrupted') AND phase<>'需要手动重试'"
            ).fetchall()
            affected: list[str] = []
            for row in rows:
                if not self._legacy_llm_without_revision(row):
                    continue
                connection.execute(
                    "UPDATE runtime_jobs SET status='interrupted',owner='',lease_token='',lease_expires=0,"
                    "phase='需要手动重试',"
                    "detail='legacy LLM job has no execution revision',"
                    "updated_at=? WHERE id=?",
                    (_utc_now(), row["id"]),
                )
                self._append_event_conn(
                    connection, str(row["id"]), int(row["attempt"]), "job_interrupted",
                    {"reason": "legacy_llm_job_missing_revision"},
                )
                affected.append(str(row["id"]))
        return len(affected)

    def requires_llm_manual_retry(self, job: Mapping[str, Any] | Any) -> bool:
        if self._legacy_llm_without_revision(job):
            return True
        scope = str(self._field(job, "llm_scope") or "")
        if not scope:
            return False
        from quantmaster.runtime.llm import get_llm_execution_coordinator

        return not get_llm_execution_coordinator().current(
            scope, str(self._field(job, "llm_revision") or ""),
        )

    def interrupt_stale_llm(self) -> int:
        """Force an explicit retry for queued work from a rotated revision."""
        from quantmaster.runtime.llm import get_llm_execution_coordinator

        coordinator = get_llm_execution_coordinator()
        with self._conn() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                "SELECT id,attempt,llm_scope,llm_revision FROM runtime_jobs "
                "WHERE llm_scope<>'' AND status IN ('queued','interrupted') "
                "AND phase<>'需要手动重试'"
            ).fetchall()
            affected: list[str] = []
            for row in rows:
                if coordinator.current(str(row["llm_scope"]), str(row["llm_revision"])):
                    continue
                connection.execute(
                    "UPDATE runtime_jobs SET status='interrupted',owner='',lease_token='',lease_expires=0,"
                    "cancel_requested=0,phase='需要手动重试',"
                    "detail='LLM configuration revision is stale',updated_at=? WHERE id=?",
                    (_utc_now(), row["id"]),
                )
                self._append_event_conn(
                    connection, str(row["id"]), int(row["attempt"]), "job_interrupted",
                    {"reason": "stale_llm_revision"},
                )
                affected.append(str(row["id"]))
        return len(affected)

    def attach_retry_revision(self, job_id: str) -> dict[str, Any]:
        """Bind an explicit retry to the current opaque execution revision."""
        job = self.get(job_id)
        scope = str(job.get("llm_scope") or "")
        if not scope and not self._legacy_llm_without_revision(job):
            return job
        scope = "news" if scope == "news" or str(job.get("type") or "").startswith("news.") else "global"
        from quantmaster.runtime.llm import get_llm_execution_coordinator

        revision = get_llm_execution_coordinator().revision(scope)
        with self._conn() as connection:
            connection.execute(
                "UPDATE runtime_jobs SET llm_scope=?,llm_revision=?,cancellation_reason='',"
                "detail='已按当前 LLM 配置手动重试',updated_at=? WHERE id=?",
                (scope, revision, _utc_now(), job_id),
            )
        return self.get(job_id)

    def claim(self, job_id: str, owner: str, lease_seconds: float = DEFAULT_LEASE_SECONDS) -> bool:
        now = time.time()
        with self._conn() as connection:
            connection.execute("BEGIN IMMEDIATE")
            current = connection.execute(
                "SELECT status,attempt,max_attempts,type,llm_scope,llm_revision "
                "FROM runtime_jobs WHERE id=?", (job_id,),
            ).fetchone()
            if current is None:
                return False
            status = str(current["status"])
            if status == "interrupted" and self.requires_llm_manual_retry(current):
                return False
            attempt = int(current["attempt"])
            if status not in {"queued", "interrupted"}:
                return False
            next_attempt = attempt + (1 if status == "interrupted" else 0)
            if next_attempt > int(current["max_attempts"]):
                connection.execute(
                    "UPDATE runtime_jobs SET status='failed',phase='执行失败',"
                    "detail='worker lease expired after maximum attempts',owner='',lease_token='',"
                    "lease_expires=0,finished_at=?,updated_at=? WHERE id=?",
                    (_utc_now(), _utc_now(), job_id),
                )
                self._append_event_conn(
                    connection, job_id, attempt, "job_terminal",
                    {"status": "failed", "detail": "worker lease expired after maximum attempts"},
                )
                return False
            token = uuid.uuid4().hex
            cursor = connection.execute(
                "UPDATE runtime_jobs SET status='running',attempt=?,owner=?,lease_token=?,lease_expires=?,"
                "heartbeat_at=?,started_at=CASE WHEN started_at='' THEN ? ELSE started_at END,"
                "phase='开始执行',detail='',updated_at=? WHERE id=? "
                "AND status IN ('queued','interrupted') AND lease_expires<=?",
                (
                    next_attempt, owner, token, lease_deadline(lease_seconds), now,
                    _utc_now(), _utc_now(), job_id, now,
                ),
            )
            if cursor.rowcount != 1:
                return False
            self._append_event_conn(
                connection,
                job_id,
                next_attempt,
                "job_started",
                {"owner": owner, "lease_token": token},
            )
        return True

    def heartbeat(
        self,
        job_id: str,
        owner: str,
        lease_token: str,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
    ) -> bool:
        now = time.time()
        with self._conn() as connection:
            cursor = connection.execute(
                "UPDATE runtime_jobs SET lease_expires=?,heartbeat_at=?,updated_at=? "
                "WHERE id=? AND owner=? AND lease_token=? "
                "AND status IN ('running','cancelling') AND lease_expires>?",
                (
                    lease_deadline(lease_seconds), now, _utc_now(), job_id, owner,
                    str(lease_token), now,
                ),
            )
        return cursor.rowcount == 1

    def progress(
        self,
        job_id: str,
        owner: str,
        lease_token: str,
        progress: int,
        phase: str,
        detail: str = "",
    ) -> None:
        with self._conn() as connection:
            cursor = connection.execute(
                "UPDATE runtime_jobs SET progress=?,phase=?,detail=?,updated_at=? "
                "WHERE id=? AND owner=? AND lease_token=? "
                "AND status IN ('running','cancelling') AND lease_expires>?",
                (
                    max(0, min(100, int(progress))),
                    str(phase)[:200],
                    str(detail)[:1000],
                    _utc_now(),
                    job_id,
                    owner,
                    str(lease_token),
                    time.time(),
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

    def cancel_stale_llm(self, scope: str, revision: str, reason: str) -> dict[str, int]:
        """Cancel queued stale work and signal active requests to converge safely."""
        with self._conn() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                "SELECT id,attempt,status FROM runtime_jobs WHERE llm_scope=? AND llm_revision<>? "
                "AND status IN ('queued','interrupted','running','cancelling')",
                (str(scope), str(revision)),
            ).fetchall()
            now = _utc_now()
            for row in rows:
                terminal = str(row["status"]) in {"queued", "interrupted"}
                connection.execute(
                    "UPDATE runtime_jobs SET status=?,cancel_requested=1,cancellation_reason=?,"
                    "phase='正在取消',detail='LLM configuration changed',"
                    "finished_at=CASE WHEN ? THEN ? ELSE finished_at END,updated_at=? WHERE id=?",
                    (
                        "cancelled" if terminal else "cancelling", str(reason)[:240],
                        int(terminal), now, now, row["id"],
                    ),
                )
                self._append_event_conn(
                    connection, str(row["id"]), int(row["attempt"]),
                    "job_cancel_requested", {"reason": str(reason)[:240]},
                )
        return {
            "queued_cancelled": sum(str(row["status"]) in {"queued", "interrupted"} for row in rows),
            "running_cancelling": sum(str(row["status"]) in {"running", "cancelling"} for row in rows),
        }

    def cancelled(
        self, job_id: str, owner: str = "", lease_token: str = "",
    ) -> bool:
        with self._conn() as connection:
            if owner:
                if not lease_token:
                    raise JobLeaseLost(job_id)
                row = connection.execute(
                    "SELECT cancel_requested,owner,lease_token,lease_expires FROM runtime_jobs WHERE id=?",
                    (job_id,),
                ).fetchone()
                if (
                    row is None
                    or str(row["owner"]) != owner
                    or str(row["lease_token"]) != str(lease_token)
                    or float(row["lease_expires"] or 0) <= time.time()
                ):
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
        *,
        lease_token: str,
    ) -> dict[str, Any]:
        allowed = {*TERMINAL_STATUSES, "interrupted"}
        if outcome.status not in allowed:
            raise ValueError("任务终态非法")
        with self._conn() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT attempt,cancel_requested,llm_scope,llm_revision FROM runtime_jobs "
                "WHERE id=? AND owner=? AND lease_token=? "
                "AND status IN ('running','cancelling') AND lease_expires>?",
                (job_id, owner, str(lease_token), time.time()),
            ).fetchone()
            if row is None:
                raise JobLeaseLost(job_id)
            # Cancellation and configuration rotation are durable fences, not
            # advisory UI state.  A provider can return in the tiny interval
            # between a handler's final ``ensure_active`` and this terminal
            # ledger update; never let that late result revive the task.
            stale_revision = False
            scope = str(row["llm_scope"] or "")
            if scope:
                from quantmaster.runtime.llm import get_llm_execution_coordinator

                stale_revision = not get_llm_execution_coordinator().current(
                    scope, str(row["llm_revision"] or ""),
                )
            if bool(row["cancel_requested"]) or stale_revision:
                outcome = JobOutcome("cancelled", "任务已取消；已丢弃迟到结果")
            terminal = outcome.status in TERMINAL_STATUSES
            progress = 100 if outcome.status in {"completed", "completed_with_errors"} else None
            connection.execute(
                "UPDATE runtime_jobs SET status=?,progress=COALESCE(?,progress),phase=?,detail=?,"
                "result_artifact_id=?,owner='',lease_token='',lease_expires=0,finished_at=?,updated_at=? "
                "WHERE id=? AND owner=? AND lease_token=?",
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
                    str(lease_token),
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
                    "UPDATE runtime_jobs SET status='interrupted',owner='',lease_token='',lease_expires=0,"
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
                "attempt=?,owner='',lease_token='',lease_expires=0,heartbeat_at=0,cancel_requested=0,"
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

    def _external_artifact_path(self, digest: str) -> Path:
        return self.artifacts_root / str(digest)[:2] / f"{digest}.json.z"

    def _write_external_artifact(self, payload: bytes, digest: str) -> str:
        """Durably publish a compressed content-addressed artifact body."""

        target = self._external_artifact_path(digest)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.is_file():
            try:
                restored = zlib.decompress(target.read_bytes())
            except (OSError, zlib.error) as exc:
                raise ArtifactIntegrityError("已存在外置任务产物不可读取") from exc
            if hashlib.sha256(restored).hexdigest() != digest:
                raise ArtifactIntegrityError("已存在外置任务产物哈希不匹配")
        else:
            fd, temporary = tempfile.mkstemp(
                prefix=".job-artifact-", suffix=".json.z.tmp", dir=target.parent,
            )
            temp = Path(temporary)
            try:
                with os.fdopen(fd, "wb") as stream:
                    stream.write(zlib.compress(payload, level=6))
                    stream.flush()
                    os.fsync(stream.fileno())
                restored = zlib.decompress(temp.read_bytes())
                if hashlib.sha256(restored).hexdigest() != digest:
                    raise ArtifactIntegrityError("外置任务产物写入哈希不匹配")
                os.replace(temp, target)
            finally:
                temp.unlink(missing_ok=True)
        return str(target.relative_to(self.path.parent)).replace("\\", "/")

    def _read_external_artifact(self, relative_path: str, digest: str) -> Any:
        path = self.path.parent / str(relative_path)
        try:
            raw = zlib.decompress(path.read_bytes())
            if hashlib.sha256(raw).hexdigest() != str(digest):
                raise ArtifactIntegrityError("外置任务产物哈希不匹配")
            return json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, zlib.error) as exc:
            raise ArtifactIntegrityError("外置任务产物不可读取") from exc

    def write_artifact(
        self,
        job_id: str,
        kind: str,
        payload: Any,
        metadata: Mapping[str, Any] | None = None,
        *,
        checkpoint_key: str = "",
        owner: str = "",
        lease_token: str = "",
    ) -> dict[str, Any]:
        encoded = _canonical(payload).encode("utf-8")
        body = json.loads(encoded.decode("utf-8"))
        digest = hashlib.sha256(encoded).hexdigest()
        values = dict(metadata or {})
        declared = str(values.get("content_hash") or "")
        if declared and declared != digest:
            raise ValueError("产物声明哈希与内容不一致")
        artifact_id = f"artifact_{uuid.uuid4().hex}"
        lineage = values.get("lineage") or {}
        if not isinstance(lineage, Mapping):
            raise ValueError("产物血缘必须是 JSON 对象")
        schema = str(
            values.get("schema_version")
            or (body.get("schema_version") if isinstance(body, Mapping) else "")
            or "1.0"
        )
        external_path = ""
        if len(encoded) > INLINE_ARTIFACT_LIMIT:
            external_path = self._write_external_artifact(encoded, digest)
        inline_payload = "" if external_path else encoded.decode("utf-8")
        with self._conn() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if owner:
                if not lease_token:
                    raise JobLeaseLost(job_id)
                job = connection.execute(
                    "SELECT attempt,spec_hash FROM runtime_jobs WHERE id=? AND owner=? AND lease_token=? "
                    "AND status IN ('running','cancelling') AND lease_expires>?",
                    (job_id, owner, str(lease_token), time.time()),
                ).fetchone()
                if job is None:
                    raise JobLeaseLost(job_id)
            else:
                job = connection.execute(
                    "SELECT attempt,spec_hash FROM runtime_jobs WHERE id=?", (job_id,),
                ).fetchone()
                if job is None:
                    raise KeyError(job_id)
            if checkpoint_key:
                connection.execute(
                    "DELETE FROM runtime_job_artifacts WHERE job_id=? AND attempt=? AND checkpoint_key=?",
                    (job_id, int(job["attempt"]), checkpoint_key),
                )
            connection.execute(
                "INSERT INTO runtime_job_artifacts "
                "(id,job_id,attempt,kind,schema_version,payload_json,content_hash,external_path,"
                "payload_bytes,lineage_json,checkpoint_key,spec_hash,created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    artifact_id,
                    job_id,
                    int(job["attempt"]),
                    str(kind)[:200],
                    schema[:50],
                    inline_payload,
                    digest,
                    external_path,
                    len(encoded),
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
            "external": bool(external_path),
            "lineage": lineage,
        }

    def _queue_repair(self, artifact: Mapping[str, Any], reason: str) -> None:
        # A Web reader reports a corrupt artifact as unavailable.  Repair is
        # queued by the runtime-worker after it observes the same evidence;
        # never turn a GET into a SQLite write.
        if self.read_only:
            return
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
            if str(value.get("external_path") or ""):
                payload = self._read_external_artifact(
                    str(value["external_path"]), str(value["content_hash"]),
                )
            else:
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
        self.input_fingerprint = str(job.get("input_fingerprint") or "")
        self.attempt = int(job["attempt"])
        self._lease_token = str(job.get("lease_token") or "")
        if not self._lease_token:
            raise JobLeaseLost(self.job_id)
        self.deadline_seconds = float(job["deadline_seconds"])
        self._deadline_at = time.monotonic() + self.deadline_seconds
        self._lease_alive = lease_alive
        self.llm_scope = str(job.get("llm_scope") or "")
        self.llm_revision = str(job.get("llm_revision") or "")

    def emit(self, event_type: str, payload: Mapping[str, Any] | None = None) -> int:
        self.ensure_active()
        return self.store.append_event(
            self.job_id, event_type, dict(payload or {}),
            owner=self.runtime.identity.value, lease_token=self._lease_token,
        )

    def progress(self, value: int, phase: str, detail: str = "") -> None:
        self.ensure_active()
        self.store.progress(
            self.job_id, self.runtime.identity.value, self._lease_token,
            value, phase, detail,
        )

    def cancelled(self) -> bool:
        if not self._lease_alive.is_set() or self.runtime.stopping:
            return True
        if self.llm_scope and self.llm_revision:
            from quantmaster.runtime.llm import get_llm_execution_coordinator

            if not get_llm_execution_coordinator().current(self.llm_scope, self.llm_revision):
                return True
        return self.store.cancelled(
            self.job_id, self.runtime.identity.value, self._lease_token,
        )

    def ensure_active(self) -> None:
        if time.monotonic() >= self._deadline_at:
            raise JobDeadlineExceeded(
                f"任务尝试超过截止时间 {self.deadline_seconds:.0f} 秒"
            )
        if not self._lease_alive.is_set():
            raise JobLeaseLost(self.job_id)
        if self.runtime.stopping:
            raise InterruptedError("worker stopped")
        if self.llm_scope and self.llm_revision:
            from quantmaster.runtime.llm import get_llm_execution_coordinator

            if not get_llm_execution_coordinator().current(self.llm_scope, self.llm_revision):
                raise InterruptedError("LLM configuration revision is no longer current")
        if self.store.cancelled(
            self.job_id, self.runtime.identity.value, self._lease_token,
        ):
            raise InterruptedError("job cancelled")

    def write_artifact(
        self,
        kind: str,
        payload: dict[str, Any],
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        self.ensure_active()
        return self.store.write_artifact(
            self.job_id, kind, payload, metadata,
            owner=self.runtime.identity.value, lease_token=self._lease_token,
        )

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
            owner=self.runtime.identity.value,
            lease_token=self._lease_token,
        )


class ProcessJobContext:
    """Lease-fenced context made available to an isolated compute child.

    The child has no scheduler and never renews a lease.  It may only report
    progress, create immutable artifacts, or inspect cancellation through the
    token issued by its parent Supervisor.  A stolen/expired lease therefore
    rejects every late write at the ledger boundary.
    """

    def __init__(
        self,
        store_path: str,
        job_id: str,
        owner: str,
        lease_token: str,
    ) -> None:
        self.store = UnifiedJobStore(Path(store_path))
        self.job_id = str(job_id)
        self.owner = str(owner)
        self._lease_token = str(lease_token)
        job = self.store.get(self.job_id)
        if (
            str(job.get("owner") or "") != self.owner
            or str(job.get("lease_token") or "") != self._lease_token
        ):
            raise JobLeaseLost(self.job_id)
        self.spec_hash = str(job["spec_hash"])
        self.input_fingerprint = str(job.get("input_fingerprint") or "")
        self.attempt = int(job["attempt"])
        self.deadline_seconds = float(job["deadline_seconds"])
        self._deadline_at = time.monotonic() + self.deadline_seconds
        self.llm_scope = str(job.get("llm_scope") or "")
        self.llm_revision = str(job.get("llm_revision") or "")

    def emit(self, event_type: str, payload: Mapping[str, Any] | None = None) -> int:
        self.ensure_active()
        return self.store.append_event(
            self.job_id,
            event_type,
            dict(payload or {}),
            owner=self.owner,
            lease_token=self._lease_token,
        )

    def progress(self, value: int, phase: str, detail: str = "") -> None:
        self.ensure_active()
        self.store.progress(
            self.job_id, self.owner, self._lease_token, value, phase, detail,
        )

    def cancelled(self) -> bool:
        return self.store.cancelled(self.job_id, self.owner, self._lease_token)

    def ensure_active(self) -> None:
        if time.monotonic() >= self._deadline_at:
            raise JobDeadlineExceeded(
                f"任务尝试超过截止时间 {self.deadline_seconds:.0f} 秒"
            )
        if self.llm_scope and self.llm_revision:
            from quantmaster.runtime.llm import get_llm_execution_coordinator

            if not get_llm_execution_coordinator().current(self.llm_scope, self.llm_revision):
                raise InterruptedError("LLM configuration revision is no longer current")
        if self.store.cancelled(self.job_id, self.owner, self._lease_token):
            raise InterruptedError("job cancelled")

    def write_artifact(
        self,
        kind: str,
        payload: dict[str, Any],
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        self.ensure_active()
        return self.store.write_artifact(
            self.job_id,
            kind,
            payload,
            metadata,
            owner=self.owner,
            lease_token=self._lease_token,
        )

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
            owner=self.owner,
            lease_token=self._lease_token,
        )


def _resolve_process_entrypoint(
    value: str,
) -> Callable[[ProcessJobContext, dict[str, Any]], JobOutcome]:
    """Resolve a stable process handler without serialising service objects."""

    module_name, separator, qualified_name = str(value).partition(":")
    if not separator or not module_name or not qualified_name:
        raise ValueError("进程任务入口必须为 module:qualname")
    target: Any = importlib.import_module(module_name)
    for name in qualified_name.split("."):
        target = getattr(target, name)
    if not callable(target):
        raise TypeError(f"进程任务入口不可调用：{value}")
    return target


def _run_process_handler(
    entrypoint: str,
    store_path: str,
    job_id: str,
    owner: str,
    lease_token: str,
    spec: dict[str, Any],
    result_queue: Any,
) -> None:
    """Spawn target: run pure computation while the parent owns the lease."""

    try:
        os.environ["QM_COMPUTE_CHILD"] = "1"
        handler = _resolve_process_entrypoint(entrypoint)
        context = ProcessJobContext(store_path, job_id, owner, lease_token)
        if context.llm_scope:
            from quantmaster.runtime.llm import get_llm_execution_coordinator

            with get_llm_execution_coordinator().lease(
                context, context.llm_scope, context.llm_revision,
            ):
                outcome = handler(context, dict(spec))
        else:
            outcome = handler(context, dict(spec))
        if not isinstance(outcome, JobOutcome):
            raise TypeError("进程任务 handler 必须返回 JobOutcome")
        context.ensure_active()
        result_queue.put({
            "kind": "outcome",
            "status": outcome.status,
            "detail": outcome.detail,
            "result_artifact_id": outcome.result_artifact_id,
        })
    except BaseException as exc:  # child must report before its process exits
        result_queue.put({
            "kind": "error",
            "type": exc.__class__.__name__,
            "detail": str(exc)[:1000],
            "traceback": traceback.format_exc(limit=20)[-6000:],
        })


class UnifiedJobRuntime:
    """Handler registry and lease-aware worker pool for extensible task types."""

    def __init__(
        self,
        store: UnifiedJobStore | None = None,
        *,
        max_workers: int = 2,
        dispatch: bool | None = None,
    ):
        self.store = store or UnifiedJobStore()
        self.identity = WorkerIdentity.create("unified-jobs")
        self._handlers: dict[str, _HandlerRegistration] = {}
        self._executor = ThreadPoolExecutor(
            max_workers=max(1, min(8, int(max_workers))),
            thread_name_prefix="qm-unified-job",
        )
        self._active: set[str] = set()
        self._reschedule_after_active: set[str] = set()
        self._lock = threading.RLock()
        self._started = False
        self._paused = threading.Event()
        self._stop = threading.Event()
        # Web API processes only enqueue/query durable records.  The dedicated
        # Supervisor owns claims, heartbeats and all handler execution.
        self._dispatch_enabled = (
            os.environ.get("QM_WEB_PROCESS") != "1"
            if dispatch is None else bool(dispatch)
        )
        self._dispatcher_stop = threading.Event()
        self._dispatcher: threading.Thread | None = None
        from quantmaster.runtime.llm import get_llm_execution_coordinator

        get_llm_execution_coordinator().register_store(self.store)

    @property
    def stopping(self) -> bool:
        return self._stop.is_set() or self._paused.is_set()

    @property
    def dispatch_enabled(self) -> bool:
        """Whether this process is permitted to claim and execute jobs."""

        return self._dispatch_enabled

    @property
    def idle(self) -> bool:
        with self._lock:
            return not self._active

    def register(
        self,
        job_type: str,
        handler: JobHandler,
        *,
        process_entrypoint: str = "",
    ) -> None:
        """Register a task handler.

        A non-empty ``process_entrypoint`` opts the task into Windows-spawned
        computation.  The normal handler remains available for unit tests and
        controlled in-process fallbacks, but production execution resolves the
        importable entrypoint in a fresh child interpreter.
        """

        name = str(job_type).strip()
        if not name or not callable(handler):
            raise ValueError("任务类型和 handler 不能为空")
        entrypoint = str(process_entrypoint or "").strip()
        if entrypoint:
            _resolve_process_entrypoint(entrypoint)
        with self._lock:
            existing = self._handlers.get(name)
            if existing is not None and (
                existing.handler is not handler
                or existing.process_entrypoint != entrypoint
            ):
                raise ValueError(f"任务类型已注册：{name}")
            self._handlers[name] = _HandlerRegistration(handler, entrypoint)
            should_schedule = self._started
        if should_schedule:
            self._dispatch_pending(job_type=name)

    def _dispatch_pending(self, *, job_type: str = "") -> None:
        if not self._dispatch_enabled or self.stopping:
            return
        self.store.recover_expired()
        self.store.interrupt_legacy_llm()
        self.store.interrupt_stale_llm()
        for job in self.store.list(1000, job_type=job_type):
            if (
                job["status"] in {"queued", "interrupted"}
                and job["type"] in self._handlers
                and not self.store.requires_llm_manual_retry(job)
            ):
                self._schedule(job["id"])

    def _dispatch_loop(self) -> None:
        while not self._dispatcher_stop.wait(0.75):
            try:
                self._dispatch_pending()
            except (OSError, RuntimeError, sqlite3.Error):
                logger.warning("Unified job dispatcher tick failed", exc_info=True)

    def _start_dispatcher(self) -> None:
        if not self._dispatch_enabled:
            return
        if self._dispatcher is not None and self._dispatcher.is_alive():
            return
        self._dispatcher_stop.clear()
        self._dispatcher = threading.Thread(
            target=self._dispatch_loop,
            name=f"qm-job-dispatcher-{self.identity.pid}",
            daemon=True,
        )
        self._dispatcher.start()

    def start(self) -> None:
        if not self._dispatch_enabled:
            # A web-side runtime intentionally has no worker threads.  The
            # persistent child Supervisor will observe the inserted row.
            return
        with self._lock:
            if self._stop.is_set():
                raise RuntimeError("任务运行时已经永久停止")
            if self._started:
                if not self._paused.is_set():
                    return
            self._paused.clear()
            self._started = True
        self._dispatch_pending()
        self._start_dispatcher()

    def submit(
        self,
        job_type: str,
        spec: Mapping[str, Any],
        *,
        idempotency_key: str = "",
        input_fingerprint: str = "",
        algorithm_version: str = "",
        deadline_seconds: float = 300,
        max_attempts: int = 2,
        llm_scope: str = "",
    ) -> tuple[dict[str, Any], bool]:
        if self.stopping:
            raise RuntimeError("任务运行时正在维护或已经停止")
        if job_type not in self._handlers:
            raise ValueError(f"任务类型未注册：{job_type}")
        llm_revision = ""
        if llm_scope:
            from quantmaster.runtime.llm import get_llm_execution_coordinator

            llm_revision = get_llm_execution_coordinator().revision(llm_scope)
        job, created = self.store.submit(
            job_type,
            spec,
            idempotency_key=idempotency_key,
            input_fingerprint=input_fingerprint,
            algorithm_version=algorithm_version,
            deadline_seconds=deadline_seconds,
            max_attempts=max_attempts,
            llm_scope=llm_scope,
            llm_revision=llm_revision,
        )
        self.start()
        if self._dispatch_enabled and job["status"] in {"queued", "interrupted"}:
            self._schedule(job["id"])
        return job, created

    def _schedule(self, job_id: str, *, reschedule_after_active: bool = False) -> None:
        with self._lock:
            if self.stopping:
                return
            if job_id in self._active:
                if reschedule_after_active:
                    self._reschedule_after_active.add(job_id)
                return
            self._active.add(job_id)
        self._executor.submit(self._run, job_id)

    def _heartbeat(
        self,
        job_id: str,
        lease_token: str,
        stopped: threading.Event,
        lease_alive: threading.Event,
    ) -> None:
        while not stopped.wait(5.0):
            if self.store.heartbeat(job_id, self.identity.value, lease_token):
                continue
            lease_alive.clear()
            return

    def _run_process_handler(
        self,
        entrypoint: str,
        job: dict[str, Any],
        lease_token: str,
        lease_alive: threading.Event,
    ) -> JobOutcome:
        """Wait on a compute child while this Supervisor keeps the lease alive."""

        context = multiprocessing.get_context("spawn")
        results = context.Queue(maxsize=1)
        process = context.Process(
            target=_run_process_handler,
            args=(
                entrypoint,
                str(self.store.path),
                str(job["id"]),
                self.identity.value,
                lease_token,
                dict(job["spec"]),
                results,
            ),
            name=f"qm-compute-{str(job['type']).replace('.', '-')}-{str(job['id'])[-8:]}",
            daemon=False,
        )
        process.start()
        deadline = time.monotonic() + float(job["deadline_seconds"])
        try:
            while process.is_alive():
                process.join(0.2)
                if not lease_alive.is_set():
                    raise JobLeaseLost(str(job["id"]))
                if self.stopping:
                    raise InterruptedError("worker stopped")
                if time.monotonic() >= deadline:
                    raise JobDeadlineExceeded(
                        f"任务尝试超过截止时间 {float(job['deadline_seconds']):.0f} 秒"
                    )
            process.join(timeout=0.1)
            try:
                message = results.get(timeout=0.5)
            except queue.Empty:
                message = None
            if not isinstance(message, dict):
                raise RuntimeError(
                    f"计算子进程未返回结果（exit_code={process.exitcode}）"
                )
            if message.get("kind") == "outcome":
                return JobOutcome(
                    str(message.get("status") or "completed"),
                    str(message.get("detail") or ""),
                    str(message.get("result_artifact_id") or ""),
                )
            detail = str(message.get("detail") or message.get("type") or "计算子进程失败")
            if str(message.get("type") or "") == "InterruptedError":
                raise InterruptedError(detail)
            if str(message.get("type") or "") == "JobLeaseLost":
                raise JobLeaseLost(str(job["id"]))
            if str(message.get("type") or "") == "JobDeadlineExceeded":
                raise JobDeadlineExceeded(detail)
            logger.error(
                "Isolated job handler failed job_id=%s type=%s traceback=%s",
                job["id"],
                job["type"],
                str(message.get("traceback") or ""),
            )
            raise RuntimeError(detail)
        finally:
            if process.is_alive():
                process.terminate()
                process.join(timeout=3.0)
            results.close()
            results.join_thread()

    def _run(self, job_id: str) -> None:
        heartbeat_stop = threading.Event()
        lease_alive = threading.Event()
        lease_alive.set()
        heartbeat: threading.Thread | None = None
        retry_delay = 0.0
        try:
            if self.stopping:
                return
            if not self.store.claim(job_id, self.identity.value):
                return
            job = self.store.get(job_id)
            lease_token = str(job.get("lease_token") or "")
            if not lease_token:
                return
            registration = self._handlers.get(str(job["type"]))
            if registration is None:
                self.store.finish(
                    job_id,
                    self.identity.value,
                    JobOutcome("failed", f"任务类型未注册：{job['type']}"),
                    lease_token=lease_token,
                )
                return
            heartbeat = threading.Thread(
                target=self._heartbeat,
                args=(job_id, lease_token, heartbeat_stop, lease_alive),
                name=f"unified-heartbeat-{job_id[-8:]}",
                daemon=True,
            )
            heartbeat.start()
            try:
                if registration.process_entrypoint:
                    # All process-isolated CPU DAGs in one Supervisor share a
                    # single slot by default.  I/O fan-out remains inside the
                    # handler and retains its provider-specific limits.
                    with _CPU_JOB_GATE:
                        outcome = self._run_process_handler(
                            registration.process_entrypoint,
                            job,
                            lease_token,
                            lease_alive,
                        )
                    if not lease_alive.is_set():
                        raise JobLeaseLost(job_id)
                    if self.store.cancelled(job_id, self.identity.value, lease_token):
                        raise InterruptedError("job cancelled")
                else:
                    context = JobContext(self, job, lease_alive)
                    if context.llm_scope:
                        from quantmaster.runtime.llm import get_llm_execution_coordinator

                        with get_llm_execution_coordinator().lease(
                            context, context.llm_scope, context.llm_revision,
                        ):
                            outcome = registration.handler(context, dict(job["spec"]))
                    else:
                        outcome = registration.handler(context, dict(job["spec"]))
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
                ValueError,
            ) as exc:
                logger.exception(
                    "Unified job handler failed job_id=%s type=%s",
                    job_id,
                    job.get("type"),
                )
                outcome = JobOutcome("failed", str(exc)[:1000])
            self.store.finish(
                job_id, self.identity.value, outcome, lease_token=lease_token,
            )
            if (
                outcome.status == "failed"
                and int(job["attempt"]) < int(job["max_attempts"])
                and self._is_transient_failure(outcome.detail)
            ):
                retried = self.store.retry(job_id)
                retry_delay = min(60.0, 5.0 * (2 ** (int(retried["attempt"]) - 2)))
                self.store.append_event(
                    job_id,
                    "job_auto_retry_scheduled",
                    {"delay_seconds": retry_delay, "attempt": retried["attempt"]},
                )
        finally:
            heartbeat_stop.set()
            if heartbeat is not None:
                heartbeat.join(timeout=1.0)
            with self._lock:
                self._active.discard(job_id)
                reschedule = job_id in self._reschedule_after_active
                self._reschedule_after_active.discard(job_id)
            if reschedule and not self.stopping:
                self._schedule(job_id)
            elif retry_delay and not self.stopping:
                timer = threading.Timer(retry_delay, self._schedule, args=(job_id,))
                timer.name = f"unified-retry-{job_id[-8:]}"
                timer.daemon = True
                timer.start()

    @staticmethod
    def _is_transient_failure(detail: str) -> bool:
        value = str(detail).casefold()
        return any(token in value for token in (
            "timeout", "timed out", "temporarily", "temporary", "connection",
            "network", "rate limit", "too many requests", "circuit", "熔断",
            "超时", "连接", "网络", "限流", "暂时", "database is locked",
        ))

    def retry(self, job_id: str) -> dict[str, Any]:
        if self.stopping:
            raise RuntimeError("任务运行时正在维护或已经停止")
        job = self.store.retry(job_id)
        job = self.store.attach_retry_revision(job_id)
        self.start()
        if self._dispatch_enabled:
            self._schedule(job_id, reschedule_after_active=True)
        return job

    def pause(self) -> None:
        """Stop accepting work and durably interrupt owned jobs for maintenance."""
        self._paused.set()
        if self._dispatch_enabled:
            self.store.interrupt_owned(self.identity.value)

    def resume(self) -> None:
        """Resume interrupted jobs after a bounded maintenance window."""
        self.start()

    def stop(self) -> None:
        self._stop.set()
        self._paused.set()
        self._dispatcher_stop.set()
        if self._dispatcher is not None and self._dispatcher.is_alive():
            self._dispatcher.join(timeout=1.0)
        if self._dispatch_enabled:
            self.store.interrupt_owned(self.identity.value)
        self._executor.shutdown(wait=False, cancel_futures=True)

    @staticmethod
    def public(job: dict[str, Any]) -> dict[str, Any]:
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
        manual_retry_required = False
        if str(job.get("status") or "") == "interrupted":
            try:
                manual_retry_required = UnifiedJobStore._legacy_llm_without_revision(job)
                scope = str(job.get("llm_scope") or "")
                if scope:
                    from quantmaster.runtime.llm import get_llm_execution_coordinator

                    manual_retry_required = not get_llm_execution_coordinator().current(
                        scope, str(job.get("llm_revision") or ""),
                    )
            except (OSError, RuntimeError, sqlite3.Error):
                # The job remains safely interrupted when its local revision
                # ledger is unavailable; do not make status rendering fail.
                manual_retry_required = True
        return {
            "domain": str(job["type"]).partition(".")[0] or "runtime",
            "id": job["id"],
            "type": job["type"],
            "status": job["status"],
            "created": bool(job.get("created")),
            "coalesced": bool(job.get("coalesced")),
            "reused": bool(job.get("reused")),
            "outcome": str(job.get("outcome") or ""),
            "input_fingerprint": str(job.get("input_fingerprint") or ""),
            "algorithm_version": str(job.get("algorithm_version") or ""),
            "progress": progress,
            "phase": job.get("phase") or "",
            "detail": job.get("detail") or "",
            "attempt": int(job["attempt"]),
            "cancel_requested": bool(job.get("cancel_requested")),
            "manual_retry_required": manual_retry_required,
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
