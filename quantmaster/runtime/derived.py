"""Content-addressed derived artifacts and generation-aware DAG state.

The web process must be able to answer from a published pointer without
walking source files or rebuilding a view.  This module is deliberately small
and dependency-free (apart from pandas for Parquet) so every domain can use
the same durable contract instead of inventing another cache database.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
import time
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import pandas as pd

from quantmaster.config import get_config
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.runtime.sqlite import connect_sqlite, migrate_schema


class DerivedArtifactIntegrityError(RuntimeError):
    """A content-addressed artifact or catalog pointer is not trustworthy."""


def _canonical(value: Any) -> str:
    return strict_json_dumps(value, sort_keys=True, default=str)


def _digest_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _digest_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class DerivedArtifactCatalog:
    """Persistent source generations, DAG cache state and snapshot pointers.

    Files are published before their SQLite manifest/pointer is committed.  A
    failed write can therefore leave an unreferenced object, but can never make
    the current pointer reference a partial object.
    """

    SCHEMA_VERSION = 1

    def __init__(self, root: str | Path | None = None, *, read_only: bool = False):
        self.root = (
            Path(root) if root is not None else get_config().data_root / "derived"
        ).resolve()
        self.read_only = bool(read_only)
        self.artifacts_root = self.root / "artifacts"
        self.path = self.root / "catalog.sqlite"
        # A snapshot reader must never create the derived directory or migrate
        # the catalog.  An absent pointer is a real cold-start state, not a
        # reason for a Web request to acquire the writer lock.
        if self.read_only:
            return
        self.root.mkdir(parents=True, exist_ok=True)
        self.artifacts_root.mkdir(parents=True, exist_ok=True)
        with self._conn() as connection:
            migrate_schema(connection, ((1, self._v1),))

    def _conn(self):
        return connect_sqlite(
            self.path,
            policy="authoritative",
            row_factory=True,
            timeout=0.25 if self.read_only else 30.0,
            read_only=self.read_only,
        )

    @staticmethod
    def _v1(connection) -> None:
        connection.executescript(
            """
            CREATE TABLE source_generations (
                source TEXT NOT NULL,
                partition_key TEXT NOT NULL,
                generation INTEGER NOT NULL,
                content_id TEXT NOT NULL,
                coverage_start TEXT NOT NULL DEFAULT '',
                coverage_end TEXT NOT NULL DEFAULT '',
                updated_at REAL NOT NULL,
                PRIMARY KEY(source, partition_key)
            );
            CREATE INDEX idx_derived_generations_source
                ON source_generations(source, generation, partition_key);

            CREATE TABLE artifact_manifests (
                artifact_id TEXT PRIMARY KEY,
                format TEXT NOT NULL,
                content_sha256 TEXT NOT NULL,
                schema_version TEXT NOT NULL,
                row_count INTEGER NOT NULL DEFAULT 0,
                coverage_start TEXT NOT NULL DEFAULT '',
                coverage_end TEXT NOT NULL DEFAULT '',
                relative_path TEXT NOT NULL,
                bytes INTEGER NOT NULL,
                created_at REAL NOT NULL
            );
            CREATE UNIQUE INDEX idx_derived_artifact_content
                ON artifact_manifests(content_sha256, format);

            CREATE TABLE node_states (
                node TEXT NOT NULL,
                partition_key TEXT NOT NULL,
                input_fingerprint TEXT NOT NULL,
                algorithm_version TEXT NOT NULL,
                output_artifact_id TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL,
                wall_seconds REAL NOT NULL DEFAULT 0,
                updated_at REAL NOT NULL,
                PRIMARY KEY(node, partition_key)
            );
            CREATE INDEX idx_derived_node_fingerprint
                ON node_states(node, input_fingerprint, algorithm_version, status);

            CREATE TABLE current_snapshots (
                domain TEXT NOT NULL,
                snapshot_type TEXT NOT NULL,
                artifact_id TEXT NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY(domain, snapshot_type),
                FOREIGN KEY(artifact_id) REFERENCES artifact_manifests(artifact_id)
            );
            """
        )

    @staticmethod
    def input_fingerprint(
        *,
        schema_version: str | int,
        algorithm_version: str,
        parameters: Mapping[str, Any] | None = None,
        upstream_artifact_ids: Iterable[str] = (),
        source_generations: Iterable[Mapping[str, Any]] = (),
    ) -> str:
        """Return the only valid cache key for a derived node.

        Callers pass catalog generation rows rather than file mtimes.  Ordering
        is normalized here so equivalent source observations always coalesce.
        """

        normalized_generations = sorted(
            [
                {
                    "source": str(item.get("source") or ""),
                    "partition_key": str(item.get("partition_key") or ""),
                    "generation": int(item.get("generation") or 0),
                    "content_id": str(item.get("content_id") or ""),
                }
                for item in source_generations
            ],
            key=lambda item: (
                item["source"], item["partition_key"], item["generation"], item["content_id"],
            ),
        )
        payload = {
            "schema_version": str(schema_version),
            "algorithm_version": str(algorithm_version),
            "parameters": dict(parameters or {}),
            "upstream_artifact_ids": sorted(str(value) for value in upstream_artifact_ids),
            "source_generations": normalized_generations,
        }
        return _digest_bytes(_canonical(payload).encode("utf-8"))

    def advance_source_generation(
        self,
        source: str,
        partition_key: str,
        content_id: str,
        *,
        coverage_start: str = "",
        coverage_end: str = "",
    ) -> dict[str, Any]:
        """Advance a source only when its authoritative content identity changed."""

        key = (str(source), str(partition_key))
        content = str(content_id)
        now = time.time()
        with self._conn() as connection:
            connection.execute("BEGIN IMMEDIATE")
            prior = connection.execute(
                "SELECT * FROM source_generations WHERE source=? AND partition_key=?", key,
            ).fetchone()
            if prior is not None and str(prior["content_id"]) == content:
                # Freshness coverage is an observation about an unchanged
                # authoritative object.  Keep the generation stable (so a
                # no-op source probe cannot invalidate every downstream node),
                # while still recording that the object was verified through
                # a newer trading session.
                connection.execute(
                    "UPDATE source_generations SET coverage_start=?,coverage_end=?,updated_at=? "
                    "WHERE source=? AND partition_key=?",
                    (str(coverage_start), str(coverage_end), now, *key),
                )
                row = connection.execute(
                    "SELECT * FROM source_generations WHERE source=? AND partition_key=?", key,
                ).fetchone()
                return dict(row)
            generation = int(prior["generation"]) + 1 if prior is not None else 1
            connection.execute(
                "INSERT INTO source_generations(source,partition_key,generation,content_id,"
                "coverage_start,coverage_end,updated_at) VALUES(?,?,?,?,?,?,?) "
                "ON CONFLICT(source,partition_key) DO UPDATE SET generation=excluded.generation,"
                "content_id=excluded.content_id,coverage_start=excluded.coverage_start,"
                "coverage_end=excluded.coverage_end,updated_at=excluded.updated_at",
                (*key, generation, content, str(coverage_start), str(coverage_end), now),
            )
            row = connection.execute(
                "SELECT * FROM source_generations WHERE source=? AND partition_key=?", key,
            ).fetchone()
        return dict(row)

    def advance_source_generations(
        self,
        values: Iterable[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        """Advance many partitions in one short catalog transaction.

        Refresh fingerprints frequently include thousands of stock symbols or
        hundreds of daily partitions.  Opening one SQLite transaction for each
        would turn the catalog itself into the new serial bottleneck.
        """

        normalized: dict[tuple[str, str], dict[str, str]] = {}
        for raw in values:
            source = str(raw.get("source") or "")
            partition_key = str(raw.get("partition_key") or "")
            content_id = str(raw.get("content_id") or "")
            if not source or not partition_key or not content_id:
                continue
            normalized[(source, partition_key)] = {
                "content_id": content_id,
                "coverage_start": str(raw.get("coverage_start") or ""),
                "coverage_end": str(raw.get("coverage_end") or ""),
            }
        if not normalized:
            return []
        now = time.time()
        result: list[dict[str, Any]] = []
        with self._conn() as connection:
            connection.execute("BEGIN IMMEDIATE")
            for (source, partition_key), value in sorted(normalized.items()):
                prior = connection.execute(
                    "SELECT * FROM source_generations WHERE source=? AND partition_key=?",
                    (source, partition_key),
                ).fetchone()
                generation = (
                    int(prior["generation"])
                    if prior is not None and str(prior["content_id"]) == value["content_id"]
                    else int(prior["generation"]) + 1 if prior is not None else 1
                )
                connection.execute(
                    "INSERT INTO source_generations(source,partition_key,generation,content_id,"
                    "coverage_start,coverage_end,updated_at) VALUES(?,?,?,?,?,?,?) "
                    "ON CONFLICT(source,partition_key) DO UPDATE SET generation=excluded.generation,"
                    "content_id=excluded.content_id,coverage_start=excluded.coverage_start,"
                    "coverage_end=excluded.coverage_end,updated_at=excluded.updated_at",
                    (
                        source, partition_key, generation, value["content_id"],
                        value["coverage_start"], value["coverage_end"], now,
                    ),
                )
                result.append({
                    "source": source,
                    "partition_key": partition_key,
                    "generation": generation,
                    "content_id": value["content_id"],
                    "coverage_start": value["coverage_start"],
                    "coverage_end": value["coverage_end"],
                    "updated_at": now,
                })
        return result

    def source_generations(
        self, source: str = "", *, partition_keys: Iterable[str] | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if source:
            clauses.append("source=?")
            params.append(str(source))
        keys = [str(value) for value in (partition_keys or ())]
        if keys:
            clauses.append("partition_key IN (" + ",".join("?" for _ in keys) + ")")
            params.extend(keys)
        query = "SELECT * FROM source_generations"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY source,partition_key"
        try:
            with self._conn() as connection:
                rows = connection.execute(query, tuple(params)).fetchall()
        except (FileNotFoundError, sqlite3.OperationalError):
            return []
        return [dict(row) for row in rows]

    def _artifact_target(self, artifact_id: str, suffix: str) -> Path:
        return self.artifacts_root / artifact_id[:2] / f"{artifact_id}.{suffix}"

    def _register_artifact(
        self,
        artifact_id: str,
        *,
        format_name: str,
        schema_version: str,
        row_count: int,
        coverage_start: str,
        coverage_end: str,
        path: Path,
    ) -> dict[str, Any]:
        relative_path = str(path.relative_to(self.root)).replace("\\", "/")
        values = {
            "artifact_id": artifact_id,
            "format": format_name,
            "content_sha256": artifact_id,
            "schema_version": str(schema_version),
            "row_count": int(row_count),
            "coverage_start": str(coverage_start),
            "coverage_end": str(coverage_end),
            "relative_path": relative_path,
            "bytes": int(path.stat().st_size),
            "created_at": time.time(),
        }
        with self._conn() as connection:
            connection.execute(
                "INSERT INTO artifact_manifests(artifact_id,format,content_sha256,schema_version,"
                "row_count,coverage_start,coverage_end,relative_path,bytes,created_at) "
                "VALUES(:artifact_id,:format,:content_sha256,:schema_version,:row_count,"
                ":coverage_start,:coverage_end,:relative_path,:bytes,:created_at) "
                "ON CONFLICT(artifact_id) DO NOTHING",
                values,
            )
            row = connection.execute(
                "SELECT * FROM artifact_manifests WHERE artifact_id=?", (artifact_id,),
            ).fetchone()
        if row is None:
            raise DerivedArtifactIntegrityError("无法登记派生产物")
        return dict(row)

    def put_json(
        self,
        payload: Mapping[str, Any] | list[Any],
        *,
        schema_version: str | int = "1",
        coverage_start: str = "",
        coverage_end: str = "",
    ) -> dict[str, Any]:
        """Write an immutable JSON artifact and verify it before cataloging it."""

        encoded = _canonical(payload).encode("utf-8")
        artifact_id = _digest_bytes(encoded)
        target = self._artifact_target(artifact_id, "json")
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.is_file():
            fd, temporary = tempfile.mkstemp(prefix=".artifact-", suffix=".json.tmp", dir=target.parent)
            temp = Path(temporary)
            try:
                with os.fdopen(fd, "wb") as stream:
                    stream.write(encoded)
                    stream.flush()
                    os.fsync(stream.fileno())
                if _digest_file(temp) != artifact_id:
                    raise DerivedArtifactIntegrityError("JSON 产物写入哈希不匹配")
                try:
                    os.replace(temp, target)
                except FileExistsError:
                    pass
            finally:
                temp.unlink(missing_ok=True)
        if _digest_file(target) != artifact_id:
            raise DerivedArtifactIntegrityError("已存在 JSON 产物哈希不匹配")
        rows = len(payload) if isinstance(payload, list) else 1
        return self._register_artifact(
            artifact_id,
            format_name="json",
            schema_version=str(schema_version),
            row_count=rows,
            coverage_start=coverage_start,
            coverage_end=coverage_end,
            path=target,
        )

    def put_frame(
        self,
        frame: pd.DataFrame,
        *,
        schema_version: str | int = "1",
        coverage_start: str = "",
        coverage_end: str = "",
    ) -> dict[str, Any]:
        """Write a verified immutable Parquet artifact.

        The SHA-256 is of the exact Parquet bytes; reading it back verifies both
        the file and the shape before it becomes addressable from SQLite.
        """

        if not isinstance(frame, pd.DataFrame):
            raise TypeError("派生产物必须是 pandas DataFrame")
        staging_dir = self.artifacts_root / ".staging"
        staging_dir.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=".artifact-", suffix=".parquet.tmp", dir=staging_dir)
        os.close(fd)
        temp = Path(temporary)
        try:
            frame.to_parquet(temp, index=False)
            with temp.open("rb+") as stream:
                os.fsync(stream.fileno())
            restored = pd.read_parquet(temp)
            if list(restored.columns) != list(frame.columns) or len(restored) != len(frame):
                raise DerivedArtifactIntegrityError("Parquet 产物回读 schema 或行数不一致")
            artifact_id = _digest_file(temp)
            target = self._artifact_target(artifact_id, "parquet")
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.is_file():
                if _digest_file(target) != artifact_id:
                    raise DerivedArtifactIntegrityError("已存在 Parquet 产物哈希不匹配")
            else:
                os.replace(temp, target)
            return self._register_artifact(
                artifact_id,
                format_name="parquet",
                schema_version=str(schema_version),
                row_count=len(frame),
                coverage_start=coverage_start,
                coverage_end=coverage_end,
                path=target,
            )
        finally:
            temp.unlink(missing_ok=True)

    def artifact(self, artifact_id: str) -> dict[str, Any]:
        with self._conn() as connection:
            row = connection.execute(
                "SELECT * FROM artifact_manifests WHERE artifact_id=?", (str(artifact_id),),
            ).fetchone()
        if row is None:
            raise KeyError(artifact_id)
        value = dict(row)
        path = self.root / str(value["relative_path"])
        if not path.is_file() or _digest_file(path) != str(value["content_sha256"]):
            raise DerivedArtifactIntegrityError(f"派生产物缺失或哈希不匹配: {artifact_id}")
        value["path"] = path
        return value

    def read_json(self, artifact_id: str) -> Any:
        artifact = self.artifact(artifact_id)
        if artifact["format"] != "json":
            raise TypeError("请求的产物不是 JSON")
        try:
            return json.loads(Path(artifact["path"]).read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            raise DerivedArtifactIntegrityError("JSON 产物不可解析") from exc

    def read_frame(self, artifact_id: str) -> pd.DataFrame:
        artifact = self.artifact(artifact_id)
        if artifact["format"] != "parquet":
            raise TypeError("请求的产物不是 Parquet")
        try:
            frame = pd.read_parquet(artifact["path"])
        except (OSError, ValueError) as exc:
            raise DerivedArtifactIntegrityError("Parquet 产物不可读取") from exc
        if len(frame) != int(artifact["row_count"]):
            raise DerivedArtifactIntegrityError("Parquet 产物行数与 manifest 不一致")
        return frame

    def node_cache_hit(
        self,
        node: str,
        partition_key: str,
        input_fingerprint: str,
        algorithm_version: str,
    ) -> dict[str, Any] | None:
        with self._conn() as connection:
            row = connection.execute(
                "SELECT * FROM node_states WHERE node=? AND partition_key=? "
                "AND input_fingerprint=? AND algorithm_version=? AND status='completed'",
                (str(node), str(partition_key), str(input_fingerprint), str(algorithm_version)),
            ).fetchone()
        if row is None or not str(row["output_artifact_id"]):
            return None
        try:
            artifact = self.artifact(str(row["output_artifact_id"]))
        except (KeyError, DerivedArtifactIntegrityError):
            return None
        return {**dict(row), "artifact": artifact}

    def record_node(
        self,
        node: str,
        partition_key: str,
        input_fingerprint: str,
        algorithm_version: str,
        *,
        output_artifact_id: str = "",
        status: str = "completed",
        wall_seconds: float = 0.0,
    ) -> None:
        if output_artifact_id:
            self.artifact(output_artifact_id)
        with self._conn() as connection:
            connection.execute(
                "INSERT INTO node_states(node,partition_key,input_fingerprint,algorithm_version,"
                "output_artifact_id,status,wall_seconds,updated_at) VALUES(?,?,?,?,?,?,?,?) "
                "ON CONFLICT(node,partition_key) DO UPDATE SET "
                "input_fingerprint=excluded.input_fingerprint,algorithm_version=excluded.algorithm_version,"
                "output_artifact_id=excluded.output_artifact_id,status=excluded.status,"
                "wall_seconds=excluded.wall_seconds,updated_at=excluded.updated_at",
                (
                    str(node), str(partition_key), str(input_fingerprint),
                    str(algorithm_version), str(output_artifact_id), str(status),
                    max(0.0, float(wall_seconds)), time.time(),
                ),
            )

    def publish_snapshot(
        self,
        domain: str,
        snapshot_type: str,
        artifact_id: str,
    ) -> dict[str, Any]:
        """Transactionally switch a current snapshot pointer after verification."""

        return self.publish_snapshots(domain, {snapshot_type: artifact_id})[str(snapshot_type)]

    def publish_snapshots(
        self,
        domain: str,
        snapshots: Mapping[str, str],
    ) -> dict[str, dict[str, Any]]:
        """Atomically advance a coherent set of current snapshot pointers.

        Artifact files are verified before the short catalog transaction.  A
        process crash can therefore leave an unreferenced immutable object,
        but it cannot expose a mix of old/new pointers to page readers.
        """

        verified = {
            str(snapshot_type): self.artifact(str(artifact_id))
            for snapshot_type, artifact_id in snapshots.items()
            if str(snapshot_type) and str(artifact_id)
        }
        if not verified:
            return {}
        now = time.time()
        with self._conn() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.executemany(
                "INSERT INTO current_snapshots(domain,snapshot_type,artifact_id,updated_at) "
                "VALUES(?,?,?,?) ON CONFLICT(domain,snapshot_type) DO UPDATE SET "
                "artifact_id=excluded.artifact_id,updated_at=excluded.updated_at",
                [
                    (str(domain), snapshot_type, str(artifact["artifact_id"]), now)
                    for snapshot_type, artifact in verified.items()
                ],
            )
        return verified

    def current_snapshot_pointer(self, domain: str, snapshot_type: str) -> dict[str, Any] | None:
        """Read only a published pointer, without opening or hashing its artifact.

        Web hot paths use this to cheaply decide whether an immutable in-memory
        projection is still current.  Integrity verification remains mandatory
        when an artifact is first loaded; it is intentionally not repeated for
        every request that serves the same content-addressed object.
        """

        try:
            with self._conn() as connection:
                row = connection.execute(
                    "SELECT artifact_id,updated_at FROM current_snapshots "
                    "WHERE domain=? AND snapshot_type=?",
                    (str(domain), str(snapshot_type)),
                ).fetchone()
        except (FileNotFoundError, sqlite3.OperationalError):
            return None
        if row is None:
            return None
        return {"domain": str(domain), "snapshot_type": str(snapshot_type), **dict(row)}

    def current_snapshot(self, domain: str, snapshot_type: str) -> dict[str, Any] | None:
        """Return the published pointer with one integrity-checked artifact."""

        pointer = self.current_snapshot_pointer(domain, snapshot_type)
        if pointer is None:
            return None
        artifact = self.artifact(str(pointer["artifact_id"]))
        return {**pointer, "artifact": artifact}
