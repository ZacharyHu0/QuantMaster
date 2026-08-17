"""Backtest execution and immutable domain-result storage."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from quantmaster.backtest.spec import BacktestSpec, canonical_json
from quantmaster.config import get_config
from quantmaster.data.schema_access import register_backtest_store
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.runtime.sqlite import connect_sqlite

BACKTEST_SCHEMA_VERSION = 2


class BacktestSchemaMigrationRequired(RuntimeError):
    """The backtest domain ledger needs its explicit lifecycle migrator."""


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _content_hash(value: Any) -> str:
    return hashlib.sha256(strict_json_dumps(value, sort_keys=True).encode("utf-8")).hexdigest()


class BacktestStore:
    """Persist immutable backtest outcomes; never own task lifecycle state."""

    def __init__(
        self,
        path: str | Path | None = None,
        artifact_root: str | Path | None = None,
        *,
        read_only: bool = False,
    ):
        self.path = Path(path) if path else get_config().data_root / "backtests.sqlite"
        self.artifact_root = (
            Path(artifact_root) if artifact_root else get_config().data_root / "backtests"
        )
        self.read_only = bool(read_only)
        if not self.path.is_file():
            if self.read_only:
                raise FileNotFoundError(self.path)
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.artifact_root.mkdir(parents=True, exist_ok=True)
            self._initialize_current()
        else:
            self._require_current()
            if not self.read_only:
                self.artifact_root.mkdir(parents=True, exist_ok=True)

    def _conn(self) -> sqlite3.Connection:
        return connect_sqlite(
            self.path,
            timeout=0.25 if self.read_only else 5.0,
            row_factory=True,
            read_only=self.read_only,
        )

    def _initialize_current(self) -> None:
        with self._conn() as connection:
            if connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' LIMIT 1"
            ).fetchone():
                raise BacktestSchemaMigrationRequired(
                    "backtests.sqlite 非空，拒绝按新领域结果库解释"
                )
            connection.executescript("""
                CREATE TABLE backtest_results (
                    job_id TEXT NOT NULL,
                    attempt INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    spec_json TEXT NOT NULL,
                    spec_hash TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    manifest_json TEXT NOT NULL,
                    summary_json TEXT NOT NULL,
                    diagnostic_json TEXT NOT NULL,
                    artifact_path TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(job_id,attempt));
                CREATE INDEX idx_backtest_results_created
                    ON backtest_results(created_at DESC,job_id,attempt);
                CREATE TABLE backtest_store_meta (
                    key TEXT PRIMARY KEY,value TEXT NOT NULL);
            """)
            connection.execute(
                "INSERT INTO backtest_store_meta(key,value) VALUES ('schema_version',?)",
                (str(BACKTEST_SCHEMA_VERSION),),
            )

    def _require_current(self) -> None:
        with self._conn() as connection:
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            required = {"backtest_results", "backtest_store_meta"}
            row = connection.execute(
                "SELECT value FROM backtest_store_meta WHERE key='schema_version'"
            ).fetchone() if "backtest_store_meta" in tables else None
            if required - tables or row is None or str(row[0]) != str(BACKTEST_SCHEMA_VERSION):
                raise BacktestSchemaMigrationRequired(
                    "backtests.sqlite 含旧 lifecycle；需执行 backtest-jobs 一次性迁移"
                )
            columns = {
                str(value[1]) for value in connection.execute(
                    "PRAGMA table_info(backtest_results)"
                )
            }
            expected = {
                "job_id", "attempt", "name", "spec_json", "spec_hash", "outcome",
                "manifest_json", "summary_json", "diagnostic_json", "artifact_path",
                "content_hash", "created_at",
            }
            if columns != expected:
                raise BacktestSchemaMigrationRequired("backtest_results schema 未分类")

    def _relative_artifact(self, job_id: str, attempt: int, digest: str) -> Path:
        safe_job = str(job_id).strip()
        if not safe_job or any(char in safe_job for char in ("/", "\\", "..")):
            raise ValueError("回测 job_id 不能用于结果路径")
        return Path(safe_job) / f"attempt-{max(1, int(attempt))}-{digest}.json"

    def _write_artifact(self, relative: Path, payload: dict[str, Any]) -> None:
        destination = self.artifact_root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.is_file():
            existing = json.loads(destination.read_text(encoding="utf-8"))
            if _content_hash(existing) != _content_hash(payload):
                raise ValueError("回测结果文件与内容哈希冲突")
            return
        descriptor, temp_name = tempfile.mkstemp(
            prefix=".backtest-result.", suffix=".tmp", dir=destination.parent,
        )
        temp = Path(temp_name)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
                handle.write(strict_json_dumps(payload))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp, destination)
        finally:
            temp.unlink(missing_ok=True)

    def save_result(
        self,
        job_id: str,
        attempt: int,
        *,
        name: str,
        spec: dict[str, Any],
        outcome: str,
        manifest: dict[str, Any] | None = None,
        summary: dict[str, Any] | None = None,
        artifact: dict[str, Any] | None = None,
        diagnostic: dict[str, Any] | None = None,
        created_at: str = "",
    ) -> dict[str, Any]:
        if self.read_only:
            raise PermissionError("只读 BacktestStore 不能写入结果")
        normalized = {
            "schema_version": "1.0",
            "job_id": str(job_id),
            "attempt": max(1, int(attempt)),
            "name": str(name),
            "spec": dict(spec),
            "outcome": str(outcome),
            "manifest": dict(manifest or {}),
            "summary": dict(summary or {}),
            "artifact": dict(artifact or {}),
            "diagnostic": dict(diagnostic or {}),
        }
        digest = _content_hash(normalized)
        relative = self._relative_artifact(job_id, attempt, digest)
        self._write_artifact(relative, normalized["artifact"])
        spec_json = canonical_json(normalized["spec"])
        spec_hash = hashlib.sha256(spec_json.encode("utf-8")).hexdigest()
        with self._conn() as connection:
            existing = connection.execute(
                "SELECT content_hash FROM backtest_results WHERE job_id=? AND attempt=?",
                (str(job_id), max(1, int(attempt))),
            ).fetchone()
            if existing is not None and str(existing["content_hash"]) != digest:
                raise ValueError("回测 job/attempt 已绑定不同领域结果")
            connection.execute(
                "INSERT OR IGNORE INTO backtest_results "
                "(job_id,attempt,name,spec_json,spec_hash,outcome,manifest_json,summary_json,"
                "diagnostic_json,artifact_path,content_hash,created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    str(job_id), max(1, int(attempt)), str(name), spec_json, spec_hash,
                    str(outcome), canonical_json(normalized["manifest"]),
                    canonical_json(normalized["summary"]),
                    canonical_json(normalized["diagnostic"]), relative.as_posix(), digest,
                    str(created_at or utc_now()),
                ),
            )
        return self.result(str(job_id), attempt=max(1, int(attempt)), include_artifact=True) or {}

    def _decode(self, row: sqlite3.Row, *, include_artifact: bool) -> dict[str, Any]:
        value = dict(row)
        for field in ("spec_json", "manifest_json", "summary_json", "diagnostic_json"):
            value[field.removesuffix("_json")] = json.loads(value.pop(field) or "{}")
        value["id"] = value["job_id"]
        if include_artifact:
            relative = Path(str(value["artifact_path"]))
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError("回测领域结果路径越界")
            path = (self.artifact_root / relative).resolve()
            if not path.is_relative_to(self.artifact_root.resolve()) or not path.is_file():
                raise FileNotFoundError(str(relative))
            artifact = json.loads(path.read_text(encoding="utf-8"))
            envelope = {
                "schema_version": "1.0",
                "job_id": value["job_id"],
                "attempt": value["attempt"],
                "name": value["name"],
                "spec": value["spec"],
                "outcome": value["outcome"],
                "manifest": value["manifest"],
                "summary": value["summary"],
                "artifact": artifact,
                "diagnostic": value["diagnostic"],
            }
            if _content_hash(envelope) != value["content_hash"]:
                raise ValueError("回测领域结果内容哈希不匹配")
            value["artifact"] = artifact
        return value

    def result(
        self,
        job_id: str,
        *,
        attempt: int | None = None,
        include_artifact: bool = False,
    ) -> dict[str, Any] | None:
        query = "SELECT * FROM backtest_results WHERE job_id=?"
        params: tuple[Any, ...] = (str(job_id),)
        if attempt is not None:
            query += " AND attempt=?"
            params = (str(job_id), max(1, int(attempt)))
        query += " ORDER BY attempt DESC LIMIT 1"
        with self._conn() as connection:
            row = connection.execute(query, params).fetchone()
        return self._decode(row, include_artifact=include_artifact) if row is not None else None

    def results(self, job_id: str) -> list[dict[str, Any]]:
        with self._conn() as connection:
            rows = connection.execute(
                "SELECT * FROM backtest_results WHERE job_id=? ORDER BY attempt",
                (str(job_id),),
            ).fetchall()
        return [self._decode(row, include_artifact=False) for row in rows]


class BacktestService:
    """Execute a validated immutable backtest specification."""

    def run(
        self,
        job_id: str,
        name: str,
        spec: BacktestSpec,
        *,
        progress: Callable[[int, str, str], None],
        cancelled: Callable[[], bool],
        panel: dict[str, pd.DataFrame] | None = None,
        membership: pd.DataFrame | None = None,
        benchmark_close: pd.Series | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        from quantmaster.backtest.application import execute_backtest

        execution = execute_backtest(
            spec,
            progress=progress,
            cancelled=cancelled,
            panel=panel,
            membership=membership,
            benchmark_close=benchmark_close,
            artifact_id=str(job_id),
            artifact_name=str(name),
        )
        return execution["manifest"], {
            "summary": execution["summary"],
            "artifact": execution["artifact"],
        }


register_backtest_store(BacktestStore)
