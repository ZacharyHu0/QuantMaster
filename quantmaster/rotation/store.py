"""Versioned rotation cache, authoritative preferences and durable jobs."""

from __future__ import annotations

import builtins
import hashlib
import json
import logging
import os
import sqlite3
import tempfile
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from io import BufferedRandom
from pathlib import Path
from typing import Any

import pandas as pd

from quantmaster.config import get_config
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.runtime.sqlite import connect_sqlite, migrate_schema

ACTIVE_JOB_STATUSES = frozenset({"queued", "running", "cancelling"})
TERMINAL_JOB_STATUSES = frozenset({"completed", "failed", "cancelled"})
logger = logging.getLogger(__name__)
ETF_METADATA_HISTORY_SCHEMA_VERSION = "1.0"
_ETF_METADATA_DERIVED_COLUMNS = frozenset(
    {"observation_id", "observation_content_sha256", "observation_integrity"}
)
_ETF_METADATA_LOCK = threading.RLock()


class RotationIntegrityError(RuntimeError):
    """A rebuildable rotation artifact exists but failed integrity validation."""


@contextmanager
def _etf_metadata_file_lock(path: Path, timeout: float = 30.0) -> Iterator[None]:
    """Serialize the parquet/manifest pair across worker processes."""

    path.parent.mkdir(parents=True, exist_ok=True)
    stream: BufferedRandom = path.open("a+b")
    if path.stat().st_size == 0:
        stream.write(b"\0")
        stream.flush()
    deadline = time.monotonic() + max(0.1, timeout)
    while True:
        try:
            stream.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(  # type: ignore[attr-defined]
                    stream.fileno(), msvcrt.LK_NBLCK, 1,  # type: ignore[attr-defined]
                )
            else:
                import fcntl

                fcntl.flock(  # type: ignore[attr-defined]
                    stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB,  # type: ignore[attr-defined]
                )
            break
        except (OSError, BlockingIOError):
            if time.monotonic() >= deadline:
                stream.close()
                raise RotationIntegrityError("等待 ETF 元数据历史文件锁超时") from None
            time.sleep(0.02)
    try:
        yield
    finally:
        try:
            stream.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(  # type: ignore[attr-defined]
                    stream.fileno(), msvcrt.LK_UNLCK, 1,  # type: ignore[attr-defined]
                )
            else:
                import fcntl

                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)  # type: ignore[attr-defined]
        finally:
            stream.close()


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_metadata_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, dict):
        return {
            str(key): _canonical_metadata_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (set, frozenset)):
        return sorted(
            (_canonical_metadata_value(item) for item in value),
            key=lambda item: strict_json_dumps(item, sort_keys=True),
        )
    if isinstance(value, (list, tuple)):
        return [_canonical_metadata_value(item) for item in value]
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            value = value.item()
        except (TypeError, ValueError):
            pass
    if hasattr(value, "tolist") and not isinstance(value, (str, bytes)):
        try:
            converted = value.tolist()
        except (TypeError, ValueError):
            pass
        else:
            if converted is not value:
                return _canonical_metadata_value(converted)
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, (str, int, float, bool)):
        return value
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except (TypeError, ValueError):
            pass
    return str(value)


def _metadata_content_hash(row: dict[str, Any]) -> str:
    payload = {}
    for key, value in sorted(row.items()):
        if key in _ETF_METADATA_DERIVED_COLUMNS:
            continue
        normalized = _canonical_metadata_value(value)
        if normalized is not None:
            payload[str(key)] = normalized
    return _hash_text(strict_json_dumps(payload, sort_keys=True))


def _metadata_observation_id(symbol: str, observed_at: str) -> str:
    return "etf_meta_observation_" + _hash_text(
        strict_json_dumps(
            {
                "schema_version": ETF_METADATA_HISTORY_SCHEMA_VERSION,
                "symbol": str(symbol).upper(),
                "observed_at": observed_at,
            },
            sort_keys=True,
        )
    )


class RotationStore:
    """Keep rebuildable analytics separate from user-selected L2 preferences."""

    def __init__(self, root: str | Path | None = None):
        base = Path(root) if root is not None else get_config().data_root / "rotation"
        self.root = base.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.cache_path = self.root / "cache.sqlite"
        self.preferences_path = self.root / "preferences.sqlite"
        self.etf_path = self.root / "etf_observations.parquet"
        self.etf_metadata_path = self.root / "etf_metadata.parquet"
        self.etf_metadata_history_path = self.root / "etf_metadata_history.parquet"
        self.etf_metadata_history_manifest_path = (
            self.root / "etf_metadata_history.manifest.json"
        )
        self._initialize()

    def _cache(self) -> sqlite3.Connection:
        return connect_sqlite(self.cache_path, policy="cache", row_factory=True)

    def _preferences(self) -> sqlite3.Connection:
        return connect_sqlite(self.preferences_path, policy="authoritative", row_factory=True)

    @staticmethod
    def _cache_v1(connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            CREATE TABLE snapshots (
                kind TEXT PRIMARY KEY,
                snapshot_id TEXT NOT NULL,
                as_of TEXT NOT NULL,
                generated_at TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                content_sha256 TEXT NOT NULL
            );
            CREATE TABLE taxonomy_nodes (
                code TEXT PRIMARY KEY,
                level TEXT NOT NULL,
                parent_code TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL,
                observed_at REAL NOT NULL
            );
            CREATE INDEX idx_rotation_taxonomy_level
                ON taxonomy_nodes(level, parent_code, code);
            CREATE TABLE theme_catalog (
                code TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                observed_at REAL NOT NULL
            );
            CREATE INDEX idx_rotation_theme_name ON theme_catalog(name, code);
            CREATE TABLE runtime_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at REAL NOT NULL
            );
            """
        )

    @staticmethod
    def _preferences_v1(connection: sqlite3.Connection) -> None:
        connection.execute(
            "CREATE TABLE preferences ("
            "id INTEGER PRIMARY KEY CHECK(id=1),payload_json TEXT NOT NULL,updated_at REAL NOT NULL)"
        )
        connection.execute(
            "INSERT INTO preferences(id,payload_json,updated_at) VALUES(1,?,?)",
            (strict_json_dumps({"l2_codes": [], "theme_limit": 16}), time.time()),
        )

    @staticmethod
    def _cache_v2(connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            CREATE TABLE theme_sync_runs (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                directory_hash TEXT NOT NULL,
                status TEXT NOT NULL,
                total_count INTEGER NOT NULL,
                completed_count INTEGER NOT NULL DEFAULT 0,
                issues_json TEXT NOT NULL DEFAULT '[]',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                UNIQUE(source,directory_hash)
            );
            CREATE TABLE theme_sync_items (
                run_id TEXT NOT NULL REFERENCES theme_sync_runs(id) ON DELETE CASCADE,
                code TEXT NOT NULL,
                name TEXT NOT NULL,
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '',
                error TEXT NOT NULL DEFAULT '',
                pages INTEGER NOT NULL DEFAULT 0,
                updated_at REAL NOT NULL,
                PRIMARY KEY(run_id,code)
            );
            CREATE INDEX idx_theme_sync_items_status
                ON theme_sync_items(run_id,status,code);
            """
        )

    def _initialize(self) -> None:
        with self._cache() as connection:
            migrate_schema(connection, ((1, self._cache_v1), (2, self._cache_v2)))
        with self._preferences() as connection:
            migrate_schema(connection, ((1, self._preferences_v1),))

    def save_snapshots(self, payloads: dict[str, dict[str, Any]]) -> None:
        """Commit a coherent set of computed views in one cache transaction."""
        rows = []
        for kind, payload in payloads.items():
            text = strict_json_dumps(payload)
            meta = payload.get("meta") or {}
            rows.append((
                str(kind), str(meta.get("snapshot_id") or ""), str(meta.get("as_of") or ""),
                str(meta.get("generated_at") or ""), text, _hash_text(text),
            ))
        with self._cache() as connection:
            connection.executemany(
                "INSERT INTO snapshots(kind,snapshot_id,as_of,generated_at,payload_json,"
                "content_sha256) VALUES(?,?,?,?,?,?) ON CONFLICT(kind) DO UPDATE SET "
                "snapshot_id=excluded.snapshot_id,as_of=excluded.as_of,"
                "generated_at=excluded.generated_at,payload_json=excluded.payload_json,"
                "content_sha256=excluded.content_sha256",
                rows,
            )

    def snapshot(self, kind: str) -> dict[str, Any] | None:
        with self._cache() as connection:
            row = connection.execute(
                "SELECT payload_json,content_sha256 FROM snapshots WHERE kind=?", (kind,),
            ).fetchone()
        if row is None:
            return None
        text = str(row["payload_json"])
        if _hash_text(text) != str(row["content_sha256"]):
            raise RotationIntegrityError(f"{kind} 快照内容哈希不匹配")
        try:
            value = json.loads(text)
        except json.JSONDecodeError as exc:
            raise RotationIntegrityError(f"{kind} 快照不是有效 JSON") from exc
        if not isinstance(value, dict):
            raise RotationIntegrityError(f"{kind} 快照根节点不是对象")
        return value

    def snapshots(self) -> list[dict[str, Any]]:
        with self._cache() as connection:
            rows = connection.execute(
                "SELECT kind,snapshot_id,as_of,generated_at FROM snapshots ORDER BY kind"
            ).fetchall()
        return [dict(row) for row in rows]

    def preferences(self) -> dict[str, Any]:
        with self._preferences() as connection:
            row = connection.execute(
                "SELECT payload_json,updated_at FROM preferences WHERE id=1"
            ).fetchone()
        value = json.loads(str(row["payload_json"])) if row else {}
        return {
            "l2_codes": [str(code) for code in value.get("l2_codes") or []],
            "theme_limit": int(value.get("theme_limit") or 16),
            "updated_at": float(row["updated_at"]) if row else 0.0,
        }

    def save_preferences(self, value: dict[str, Any]) -> dict[str, Any]:
        l2_codes = list(dict.fromkeys(
            str(code).strip().upper() for code in value.get("l2_codes") or [] if str(code).strip()
        ))
        if len(l2_codes) > 30:
            raise ValueError("最多关注 30 个申万二级行业")
        theme_limit = int(value.get("theme_limit") or 16)
        if not 8 <= theme_limit <= 32:
            raise ValueError("题材首屏数量需在 8–32 之间")
        payload = {"l2_codes": l2_codes, "theme_limit": theme_limit}
        now = time.time()
        with self._preferences() as connection:
            connection.execute(
                "UPDATE preferences SET payload_json=?,updated_at=? WHERE id=1",
                (strict_json_dumps(payload), now),
            )
        return {**payload, "updated_at": now}

    def replace_taxonomy_nodes(self, nodes: list[dict[str, Any]]) -> None:
        rows = []
        for node in nodes:
            code = str(node.get("code") or "").strip().upper()
            level = str(node.get("level") or "").strip().upper()
            if not code or level not in {"L1", "L2"}:
                continue
            rows.append((
                code, level, str(node.get("parent_code") or "").strip().upper(),
                strict_json_dumps(node), time.time(),
            ))
        with self._cache() as connection:
            connection.execute("DELETE FROM taxonomy_nodes")
            connection.executemany(
                "INSERT INTO taxonomy_nodes(code,level,parent_code,payload_json,observed_at) "
                "VALUES(?,?,?,?,?)",
                rows,
            )

    def taxonomy_nodes(self, level: str | None = None) -> list[dict[str, Any]]:
        with self._cache() as connection:
            if level:
                rows = connection.execute(
                    "SELECT payload_json FROM taxonomy_nodes WHERE level=? ORDER BY code",
                    (str(level).upper(),),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT payload_json FROM taxonomy_nodes ORDER BY level,code"
                ).fetchall()
        result = []
        for row in rows:
            try:
                item = json.loads(str(row["payload_json"]))
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                result.append(item)
        return result

    def replace_themes(self, themes: list[dict[str, Any]]) -> None:
        rows = []
        observed_at = time.time()
        for theme in themes:
            code = str(theme.get("code") or "").strip().upper()
            name = str(theme.get("name") or "").strip()
            if code and name:
                rows.append((code, name, strict_json_dumps(theme), observed_at))
        with self._cache() as connection:
            connection.execute("DELETE FROM theme_catalog")
            connection.executemany(
                "INSERT INTO theme_catalog(code,name,payload_json,observed_at) VALUES(?,?,?,?)",
                rows,
            )

    def themes(self) -> list[dict[str, Any]]:
        with self._cache() as connection:
            rows = connection.execute(
                "SELECT payload_json FROM theme_catalog ORDER BY name,code"
            ).fetchall()
        result = []
        for row in rows:
            try:
                item = json.loads(str(row["payload_json"]))
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                result.append(item)
        return result

    def begin_theme_sync(
        self, source: str, directory_hash: str, total_count: int,
    ) -> dict[str, Any]:
        """Create or resume one source-coherent theme catalog staging run."""
        now = time.time()
        with self._cache() as connection:
            row = connection.execute(
                "SELECT id,status FROM theme_sync_runs WHERE source=? AND directory_hash=?",
                (source, directory_hash),
            ).fetchone()
            if row is None:
                run_id = uuid.uuid4().hex
                connection.execute(
                    "INSERT INTO theme_sync_runs(id,source,directory_hash,status,total_count,"
                    "created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                    (run_id, source, directory_hash, "running", int(total_count), now, now),
                )
            else:
                run_id = str(row["id"])
                connection.execute(
                    "UPDATE theme_sync_runs SET status='running',total_count=?,updated_at=? "
                    "WHERE id=?",
                    (int(total_count), now, run_id),
                )
            rows = connection.execute(
                "SELECT code,payload_json,pages FROM theme_sync_items "
                "WHERE run_id=? AND status='complete'",
                (run_id,),
            ).fetchall()
            attempted_count = int(connection.execute(
                "SELECT COUNT(*) FROM theme_sync_items WHERE run_id=?",
                (run_id,),
            ).fetchone()[0])
        items: dict[str, dict[str, Any]] = {}
        for item in rows:
            try:
                payload = json.loads(str(item["payload_json"]))
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                items[str(item["code"])] = payload
        return {
            "run_id": run_id,
            "items": items,
            "attempted_count": attempted_count,
        }

    def save_theme_sync_item(
        self,
        run_id: str,
        code: str,
        name: str,
        *,
        payload: dict[str, Any] | None = None,
        error: str = "",
        pages: int = 0,
    ) -> None:
        now = time.time()
        status = "complete" if payload else "failed"
        with self._cache() as connection:
            connection.execute(
                "INSERT INTO theme_sync_items(run_id,code,name,status,payload_json,error,pages,"
                "updated_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(run_id,code) DO UPDATE SET "
                "name=excluded.name,status=excluded.status,payload_json=excluded.payload_json,"
                "error=excluded.error,pages=excluded.pages,updated_at=excluded.updated_at",
                (
                    run_id,
                    str(code),
                    str(name),
                    status,
                    strict_json_dumps(payload) if payload else "",
                    str(error)[:500],
                    max(0, int(pages)),
                    now,
                ),
            )
            completed = connection.execute(
                "SELECT COUNT(*) FROM theme_sync_items WHERE run_id=? AND status='complete'",
                (run_id,),
            ).fetchone()[0]
            connection.execute(
                "UPDATE theme_sync_runs SET completed_count=?,updated_at=? WHERE id=?",
                (int(completed), now, run_id),
            )

    def commit_theme_sync(
        self,
        run_id: str,
        themes: list[dict[str, Any]],
        issues: list[str],
    ) -> None:
        """Atomically publish a validated staging run and its audit outcome."""
        observed_at = time.time()
        rows = [
            (
                str(theme.get("code") or "").strip().upper(),
                str(theme.get("name") or "").strip(),
                strict_json_dumps(theme),
                observed_at,
            )
            for theme in themes
            if str(theme.get("code") or "").strip()
            and str(theme.get("name") or "").strip()
            and theme.get("members")
        ]
        if not rows:
            raise ValueError("题材暂存目录没有可提交的有效成分")
        with self._cache() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute("DELETE FROM theme_catalog")
            connection.executemany(
                "INSERT INTO theme_catalog(code,name,payload_json,observed_at) VALUES(?,?,?,?)",
                rows,
            )
            connection.execute(
                "UPDATE theme_sync_runs SET status='completed',completed_count=?,issues_json=?,"
                "updated_at=? WHERE id=?",
                (len(rows), strict_json_dumps(issues), observed_at, run_id),
            )
            connection.commit()

    def reuse_published_theme_sync(self, run_id: str, issues: list[str]) -> None:
        """Close a resumed staging run without rewriting its published catalog.

        A fully traversed partial catalog can be reused when the upstream directory
        is unchanged.  Rewriting ``theme_catalog`` would incorrectly make the old
        observations look freshly downloaded, so only the run audit is updated.
        """
        with self._cache() as connection:
            completed = int(connection.execute(
                "SELECT COUNT(*) FROM theme_sync_items WHERE run_id=? AND status='complete'",
                (run_id,),
            ).fetchone()[0])
            connection.execute(
                "UPDATE theme_sync_runs SET status='completed',completed_count=?,"
                "issues_json=?,updated_at=? WHERE id=?",
                (completed, strict_json_dumps(issues), time.time(), run_id),
            )

    def fail_theme_sync(self, run_id: str, issues: list[str]) -> None:
        with self._cache() as connection:
            connection.execute(
                "UPDATE theme_sync_runs SET status='incomplete',issues_json=?,updated_at=? "
                "WHERE id=?",
                (strict_json_dumps(issues), time.time(), run_id),
            )

    def runtime_state(self, key: str) -> str:
        with self._cache() as connection:
            row = connection.execute(
                "SELECT value FROM runtime_state WHERE key=?", (str(key),),
            ).fetchone()
        return str(row["value"]) if row else ""

    def set_runtime_state(self, key: str, value: str) -> None:
        with self._cache() as connection:
            connection.execute(
                "INSERT INTO runtime_state(key,value,updated_at) VALUES(?,?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value,"
                "updated_at=excluded.updated_at",
                (str(key), str(value), time.time()),
            )

    def save_etf_observations(self, frame: pd.DataFrame) -> None:
        if frame is None or frame.empty:
            return
        fd, temp_name = tempfile.mkstemp(
            prefix=".etf_observations.", suffix=".parquet.tmp", dir=self.root,
        )
        os.close(fd)
        temp = Path(temp_name)
        try:
            frame.to_parquet(temp, index=False)
            with temp.open("rb+") as stream:
                os.fsync(stream.fileno())
            os.replace(temp, self.etf_path)
        finally:
            temp.unlink(missing_ok=True)

    def etf_observations(self) -> pd.DataFrame:
        if not self.etf_path.is_file():
            return pd.DataFrame()
        try:
            return pd.read_parquet(self.etf_path)
        except (OSError, ValueError) as exc:
            logger.error("ETF 观察文件完整性校验失败: %s", self.etf_path, exc_info=True)
            raise RotationIntegrityError("ETF 观察文件损坏，拒绝按空数据继续计算") from exc

    def save_etf_metadata(self, frame: pd.DataFrame) -> None:
        """Persist immutable observations and a tamper-evident history manifest."""

        if frame is None or frame.empty:
            return
        with _ETF_METADATA_LOCK, _etf_metadata_file_lock(
            self.root / ".etf_metadata_history.lock"
        ):
            current = self._prepare_etf_metadata_observations(frame)
            history = (
                self._read_verified_etf_metadata_history()
                if self.etf_metadata_history_path.is_file()
                else pd.DataFrame()
            )
            combined = pd.concat((history, current), ignore_index=True, sort=False)
            conflicts = (
                combined.groupby("observation_id")["observation_content_sha256"].nunique()
                if not combined.empty
                else pd.Series(dtype=int)
            )
            conflicting_ids = sorted(conflicts[conflicts.gt(1)].index.astype(str))
            if conflicting_ids:
                raise RotationIntegrityError(
                    "ETF 元数据观察身份出现冲突内容，拒绝改写历史: "
                    + ", ".join(conflicting_ids[:5])
                )
            combined = (
                combined.sort_values(["observed_at", "symbol", "observation_id"])
                .drop_duplicates("observation_id", keep="first")
                .reset_index(drop=True)
            )
            previous_ids = (
                set(history.get("observation_id", pd.Series(dtype=str)).astype(str))
                if not history.empty
                else set()
            )
            if set(combined["observation_id"].astype(str)) != previous_ids:
                self._write_etf_metadata_history(combined)
            self._write_etf_metadata_frame(
                self.etf_metadata_path,
                current,
                ".etf_metadata.",
            )

    @staticmethod
    def _prepare_etf_metadata_observations(frame: pd.DataFrame) -> pd.DataFrame:
        current = frame.copy()
        if "symbol" not in current:
            raise RotationIntegrityError("ETF 元数据观察缺少 symbol")
        current["symbol"] = current["symbol"].fillna("").astype(str).str.upper()
        if current["symbol"].eq("").any():
            raise RotationIntegrityError("ETF 元数据观察包含空 symbol")
        if "observed_at" not in current:
            current["observed_at"] = pd.Timestamp.now(tz="UTC").isoformat()
        parsed = pd.to_datetime(current["observed_at"], errors="coerce", utc=True)
        missing = parsed.isna()
        if missing.any():
            parsed.loc[missing] = pd.Timestamp.now(tz="UTC")
        current["observed_at"] = parsed.map(lambda value: value.isoformat())
        for column in _ETF_METADATA_DERIVED_COLUMNS:
            if column in current:
                current = current.drop(columns=column)
        records = current.to_dict("records")
        current["observation_id"] = [
            _metadata_observation_id(str(row["symbol"]), str(row["observed_at"]))
            for row in records
        ]
        current["observation_content_sha256"] = [
            _metadata_content_hash(row) for row in records
        ]
        current["observation_integrity"] = "verified"
        return current

    @staticmethod
    def _write_etf_metadata_frame(
        target: Path,
        value: pd.DataFrame,
        prefix: str,
    ) -> None:
        fd, temp_name = tempfile.mkstemp(
            prefix=prefix, suffix=".parquet.tmp", dir=target.parent,
        )
        os.close(fd)
        temp = Path(temp_name)
        try:
            value.to_parquet(temp, index=False)
            with temp.open("rb+") as stream:
                os.fsync(stream.fileno())
            os.replace(temp, target)
        finally:
            temp.unlink(missing_ok=True)

    @staticmethod
    def _history_logical_hash(frame: pd.DataFrame) -> str:
        rows = sorted(
            (
                {
                    "observation_id": str(row.observation_id),
                    "content_sha256": str(row.observation_content_sha256),
                }
                for row in frame[[
                    "observation_id", "observation_content_sha256"
                ]].itertuples(index=False)
            ),
            key=lambda row: row["observation_id"],
        )
        return _hash_text(strict_json_dumps(rows, sort_keys=True))

    def _write_etf_metadata_history(self, frame: pd.DataFrame) -> None:
        self._write_etf_metadata_frame(
            self.etf_metadata_history_path,
            frame,
            ".etf_metadata_history.",
        )
        manifest = {
            "schema_version": ETF_METADATA_HISTORY_SCHEMA_VERSION,
            "artifact": "etf_metadata_history",
            "file_sha256": _hash_file(self.etf_metadata_history_path),
            "logical_sha256": self._history_logical_hash(frame),
            "row_count": len(frame),
            "observation_count": frame["observation_id"].nunique(),
            "written_at": pd.Timestamp.now(tz="UTC").isoformat(),
        }
        manifest["manifest_sha256"] = _hash_text(
            strict_json_dumps(manifest, sort_keys=True)
        )
        encoded = json.dumps(
            manifest,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        )
        fd, temp_name = tempfile.mkstemp(
            prefix=".etf_metadata_history.manifest.",
            suffix=".json.tmp",
            dir=self.root,
        )
        os.close(fd)
        temp = Path(temp_name)
        try:
            temp.write_text(encoded, encoding="utf-8")
            with temp.open("rb+") as stream:
                os.fsync(stream.fileno())
            os.replace(temp, self.etf_metadata_history_manifest_path)
        finally:
            temp.unlink(missing_ok=True)

    def _read_verified_etf_metadata_history(self) -> pd.DataFrame:
        if not self.etf_metadata_history_manifest_path.is_file():
            raise RotationIntegrityError(
                "ETF 元数据历史缺少完整性 manifest；旧历史不得静默升级为可信证据"
            )
        try:
            manifest = json.loads(
                self.etf_metadata_history_manifest_path.read_text(encoding="utf-8")
            )
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            raise RotationIntegrityError("ETF 元数据历史 manifest 损坏") from exc
        claimed_manifest_hash = str(manifest.pop("manifest_sha256", ""))
        actual_manifest_hash = _hash_text(strict_json_dumps(manifest, sort_keys=True))
        if claimed_manifest_hash != actual_manifest_hash:
            raise RotationIntegrityError("ETF 元数据历史 manifest 哈希不匹配")
        if (
            manifest.get("schema_version") != ETF_METADATA_HISTORY_SCHEMA_VERSION
            or manifest.get("artifact") != "etf_metadata_history"
        ):
            raise RotationIntegrityError("ETF 元数据历史 manifest 契约已淘汰或类型错误")
        if _hash_file(self.etf_metadata_history_path) != manifest.get("file_sha256"):
            raise RotationIntegrityError("ETF 元数据历史文件哈希与 manifest 不匹配")
        try:
            history = pd.read_parquet(self.etf_metadata_history_path)
        except (OSError, ValueError) as exc:
            raise RotationIntegrityError("ETF 元数据历史损坏，拒绝丢失 PIT 证据") from exc
        required = {
            "symbol",
            "observed_at",
            "observation_id",
            "observation_content_sha256",
            "observation_integrity",
        }
        if not required.issubset(history.columns):
            raise RotationIntegrityError("ETF 元数据历史缺少不可变观察字段")
        prepared = self._prepare_etf_metadata_observations(history)
        if (
            prepared["observation_id"].tolist() != history["observation_id"].astype(str).tolist()
            or prepared["observation_content_sha256"].tolist()
            != history["observation_content_sha256"].astype(str).tolist()
            or not history["observation_integrity"].astype(str).eq("verified").all()
        ):
            raise RotationIntegrityError("ETF 元数据历史观察身份或内容哈希不匹配")
        if len(history) != int(manifest.get("row_count") or -1):
            raise RotationIntegrityError("ETF 元数据历史行数与 manifest 不匹配")
        if history["observation_id"].nunique() != int(
            manifest.get("observation_count") or -1
        ):
            raise RotationIntegrityError("ETF 元数据历史观察数与 manifest 不匹配")
        if self._history_logical_hash(history) != manifest.get("logical_sha256"):
            raise RotationIntegrityError("ETF 元数据历史逻辑哈希与 manifest 不匹配")
        return history

    def etf_metadata(self) -> pd.DataFrame:
        if not self.etf_metadata_path.is_file():
            return pd.DataFrame()
        try:
            return pd.read_parquet(self.etf_metadata_path)
        except (OSError, ValueError) as exc:
            logger.error("ETF 元数据文件完整性校验失败: %s", self.etf_metadata_path, exc_info=True)
            raise RotationIntegrityError("ETF 元数据文件损坏，拒绝按空目录继续分类") from exc

    def etf_metadata_history(self) -> pd.DataFrame:
        if not self.etf_metadata_history_path.is_file():
            return pd.DataFrame()
        with _ETF_METADATA_LOCK, _etf_metadata_file_lock(
            self.root / ".etf_metadata_history.lock"
        ):
            return self._read_verified_etf_metadata_history()


class RotationJobStore:
    """Durable immutable job specs with lease-based claims and an event stream."""

    def __init__(self, path: str | Path | None = None):
        self.path = (
            Path(path) if path is not None
            else get_config().data_root / "rotation" / "jobs.sqlite"
        ).resolve()
        with self._connect() as connection:
            migrate_schema(connection, ((1, self._v1),))

    def _connect(self) -> sqlite3.Connection:
        return connect_sqlite(self.path, policy="authoritative", row_factory=True)

    @staticmethod
    def _v1(connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            CREATE TABLE jobs (
                id TEXT PRIMARY KEY,
                spec_json TEXT NOT NULL,
                logical_hash TEXT NOT NULL,
                status TEXT NOT NULL,
                progress INTEGER NOT NULL DEFAULT 0,
                phase TEXT NOT NULL DEFAULT '',
                detail TEXT NOT NULL DEFAULT '',
                attempt INTEGER NOT NULL DEFAULT 1,
                worker_owner TEXT NOT NULL DEFAULT '',
                lease_expires_at REAL NOT NULL DEFAULT 0,
                heartbeat_at REAL NOT NULL DEFAULT 0,
                cancel_requested INTEGER NOT NULL DEFAULT 0,
                result_json TEXT NOT NULL DEFAULT '',
                error TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );
            CREATE INDEX idx_rotation_jobs_claim
                ON jobs(status,lease_expires_at,created_at);
            CREATE INDEX idx_rotation_jobs_hash
                ON jobs(logical_hash,status,created_at);
            CREATE TABLE events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
                event_json TEXT NOT NULL,
                created_at REAL NOT NULL
            );
            CREATE INDEX idx_rotation_job_events ON events(job_id,seq);
            """
        )

    @staticmethod
    def _row(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        value = dict(row)
        value["cancel_requested"] = bool(value["cancel_requested"])
        try:
            value["spec"] = json.loads(value.pop("spec_json"))
        except json.JSONDecodeError:
            value["spec"] = {}
        result_text = value.pop("result_json")
        try:
            value["result"] = json.loads(result_text) if result_text else None
        except json.JSONDecodeError:
            value["result"] = None
        return value

    def _event(self, connection: sqlite3.Connection, job_id: str, value: dict[str, Any]) -> None:
        connection.execute(
            "INSERT INTO events(job_id,event_json,created_at) VALUES(?,?,?)",
            (job_id, strict_json_dumps(value), time.time()),
        )

    def create(self, spec: dict[str, Any]) -> dict[str, Any]:
        text = strict_json_dumps(spec, sort_keys=True)
        logical_hash = _hash_text(text)
        now = time.time()
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT * FROM jobs WHERE logical_hash=? AND status IN ('queued','running',"
                "'cancelling') ORDER BY created_at DESC LIMIT 1",
                (logical_hash,),
            ).fetchone()
            if existing is not None:
                return self._row(existing) or {}
            job_id = uuid.uuid4().hex
            connection.execute(
                "INSERT INTO jobs(id,spec_json,logical_hash,status,progress,phase,detail,"
                "created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (job_id, text, logical_hash, "queued", 0, "等待执行", "", now, now),
            )
            self._event(connection, job_id, {"type": "queued", "phase": "等待执行"})
            row = connection.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return self._row(row) or {}

    def get(self, job_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        return self._row(row)

    def list(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (max(1, min(limit, 200)),),
            ).fetchall()
        return [value for row in rows if (value := self._row(row)) is not None]

    def claim(self, owner: str, lease_seconds: float = 45.0) -> dict[str, Any] | None:
        now = time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM jobs WHERE status='queued' OR "
                "(status IN ('running','cancelling') AND lease_expires_at<?) "
                "ORDER BY CASE status WHEN 'queued' THEN 0 ELSE 1 END,created_at LIMIT 1",
                (now,),
            ).fetchone()
            if row is None:
                connection.rollback()
                return None
            attempt = int(row["attempt"]) + (0 if row["status"] == "queued" else 1)
            status = "cancelling" if bool(row["cancel_requested"]) else "running"
            connection.execute(
                "UPDATE jobs SET status=?,attempt=?,worker_owner=?,lease_expires_at=?,"
                "heartbeat_at=?,updated_at=? WHERE id=?",
                (status, attempt, owner, now + lease_seconds, now, now, row["id"]),
            )
            self._event(connection, str(row["id"]), {
                "type": "claimed", "owner": owner, "attempt": attempt,
            })
            connection.commit()
        return self.get(str(row["id"]))

    def heartbeat(self, job_id: str, owner: str, lease_seconds: float = 45.0) -> bool:
        now = time.time()
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE jobs SET heartbeat_at=?,lease_expires_at=?,updated_at=? "
                "WHERE id=? AND worker_owner=? AND status IN ('running','cancelling')",
                (now, now + lease_seconds, now, job_id, owner),
            )
        return cursor.rowcount == 1

    def release_for_handoff(self, job_id: str, owner: str) -> bool:
        """Expire an owned lease without changing the durable task outcome."""
        now = time.time()
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE jobs SET lease_expires_at=0,heartbeat_at=?,updated_at=? "
                "WHERE id=? AND worker_owner=? AND status IN ('running','cancelling')",
                (now, now, job_id, owner),
            )
            if cursor.rowcount:
                self._event(connection, job_id, {
                    "type": "lease_released",
                    "owner": owner,
                    "reason": "worker_shutdown",
                })
        return cursor.rowcount == 1

    def progress(
        self, job_id: str, owner: str, progress: int, phase: str, detail: str = "",
    ) -> None:
        now = time.time()
        value = max(0, min(99, int(progress)))
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE jobs SET progress=?,phase=?,detail=?,heartbeat_at=?,"
                "lease_expires_at=?,updated_at=? WHERE id=? AND worker_owner=? "
                "AND status IN ('running','cancelling')",
                (value, str(phase)[:200], str(detail)[:1000], now, now + 45, now, job_id, owner),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("任务租约已失效")
            self._event(connection, job_id, {
                "type": "progress", "progress": value, "phase": phase, "detail": detail,
            })

    def is_cancel_requested(self, job_id: str, owner: str = "") -> bool:
        with self._connect() as connection:
            if owner:
                row = connection.execute(
                    "SELECT cancel_requested FROM jobs WHERE id=? AND worker_owner=?",
                    (job_id, owner),
                ).fetchone()
            else:
                row = connection.execute(
                    "SELECT cancel_requested FROM jobs WHERE id=?", (job_id,),
                ).fetchone()
        return bool(row and row["cancel_requested"])

    def complete(self, job_id: str, owner: str, result: dict[str, Any]) -> None:
        now = time.time()
        outcome = str(result.get("outcome") or "updated")
        phase = {
            "updated": "分析已更新",
            "partial": "部分更新完成",
            "unchanged": "数据未推进",
        }.get(outcome, "分析已完成")
        detail = "；".join(str(item) for item in result.get("warnings") or [])[:1000]
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE jobs SET status='completed',progress=100,phase=?,"
                "detail=?,result_json=?,error='',lease_expires_at=0,updated_at=? "
                "WHERE id=? AND worker_owner=? AND status IN ('running','cancelling')",
                (phase, detail, strict_json_dumps(result), now, job_id, owner),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("任务租约已失效")
            self._event(connection, job_id, {"type": "completed", "result": result})

    def fail(self, job_id: str, owner: str, error: str, *, cancelled: bool = False) -> None:
        now = time.time()
        status = "cancelled" if cancelled else "failed"
        phase = "已取消" if cancelled else "执行失败"
        with self._connect() as connection:
            cursor = connection.execute(
                "UPDATE jobs SET status=?,phase=?,detail=?,error=?,lease_expires_at=0,"
                "updated_at=? WHERE id=? AND worker_owner=? AND status IN ('running','cancelling')",
                (status, phase, str(error)[:1000], str(error)[:1000], now, job_id, owner),
            )
            if cursor.rowcount:
                self._event(connection, job_id, {"type": status, "error": str(error)[:1000]})

    def cancel(self, job_id: str) -> dict[str, Any]:
        now = time.time()
        with self._connect() as connection:
            row = connection.execute("SELECT status FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise KeyError(job_id)
            status = str(row["status"])
            if status in TERMINAL_JOB_STATUSES:
                return self.get(job_id) or {}
            next_status = "cancelled" if status == "queued" else "cancelling"
            connection.execute(
                "UPDATE jobs SET cancel_requested=1,status=?,phase=?,updated_at=? WHERE id=?",
                (next_status, "已取消" if next_status == "cancelled" else "正在安全停止", now, job_id),
            )
            self._event(connection, job_id, {"type": "cancel_requested"})
        return self.get(job_id) or {}

    def retry(self, job_id: str) -> dict[str, Any]:
        current = self.get(job_id)
        if current is None:
            raise KeyError(job_id)
        if current["status"] not in TERMINAL_JOB_STATUSES:
            raise ValueError("当前任务尚未结束，不能重试")
        created = self.create(current["spec"])
        with self._connect() as connection:
            self._event(connection, str(created["id"]), {"type": "retry_of", "job_id": job_id})
            self._event(connection, job_id, {"type": "retried_as", "job_id": created["id"]})
        return created

    def events(
        self, job_id: str, after: int = 0, limit: int = 500,
    ) -> builtins.list[dict[str, Any]]:
        if self.get(job_id) is None:
            raise KeyError(job_id)
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT seq,event_json,created_at FROM events WHERE job_id=? AND seq>? "
                "ORDER BY seq LIMIT ?",
                (job_id, max(0, int(after)), max(1, min(int(limit), 2000))),
            ).fetchall()
        result = []
        for row in rows:
            try:
                value = json.loads(str(row["event_json"]))
            except json.JSONDecodeError:
                value = {"type": "invalid_event"}
            result.append({"seq": int(row["seq"]), "created_at": row["created_at"], **value})
        return result
