"""本地数据缓存：日线存 Parquet（每个 symbol 一个文件），元信息存 SQLite。

免费数据源普遍有频率限制，本地缓存能显著加速研究迭代，也让回测可复现。
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sqlite3
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import AbstractContextManager
from dataclasses import dataclass
from io import BufferedRandom
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from quantmaster.config import get_config
from quantmaster.runtime.paths import confined_path
from quantmaster.runtime.sqlite import connect_sqlite

logger = logging.getLogger(__name__)

_LOCKS_GUARD = threading.Lock()
_SYMBOL_LOCKS: dict[tuple[str, str], threading.RLock] = {}
_FILE_LOCK_STATE: dict[tuple[str, str], tuple[int, BufferedRandom]] = {}
_INTEGRITY_LOG_GUARD = threading.Lock()
_INTEGRITY_LOGGED_AT: dict[tuple[str, str, str], float] = {}

_META_COLUMNS = (
    "symbol", "start", "end", "updated_at", "coverage_start", "coverage_end",
    "checked_at", "last_source", "last_status", "content_sha256", "row_count",
    "file_size", "file_mtime_ns",
)


def _symbol_lock(root: Path, symbol: str) -> threading.RLock:
    key = (str(root.resolve()), symbol)
    with _LOCKS_GUARD:
        return _SYMBOL_LOCKS.setdefault(key, threading.RLock())


def _acquire_file_lock(path: Path, timeout: float = 30.0) -> BufferedRandom:
    path.parent.mkdir(parents=True, exist_ok=True)
    stream = path.open("a+b")
    if stream.tell() == 0:
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
            return stream
        except (OSError, BlockingIOError):
            if time.monotonic() >= deadline:
                stream.close()
                raise TimeoutError(f"等待数据文件锁超时: {path.name}") from None
            time.sleep(0.02)


def _release_file_lock(stream: BufferedRandom) -> None:
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


class _BarLock(AbstractContextManager["_BarLock"]):
    """Reentrant in-process lock backed by one cross-process file lock."""

    def __init__(self, root: Path, symbol: str) -> None:
        self.root = root.resolve()
        self.symbol = symbol
        self.key = (str(self.root), symbol)
        self.thread_lock = _symbol_lock(root, symbol)

    def __enter__(self) -> _BarLock:
        self.thread_lock.acquire()
        try:
            state = _FILE_LOCK_STATE.get(self.key)
            if state is None:
                lock_path = confined_path(
                    self.root / ".locks",
                    f"{_safe_name(self.symbol)}.lock",
                    label="行情缓存锁",
                )
                _FILE_LOCK_STATE[self.key] = (1, _acquire_file_lock(lock_path))
            else:
                _FILE_LOCK_STATE[self.key] = (state[0] + 1, state[1])
            return self
        except OSError:
            self.thread_lock.release()
            raise

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        try:
            depth, stream = _FILE_LOCK_STATE[self.key]
            if depth == 1:
                _FILE_LOCK_STATE.pop(self.key, None)
                _release_file_lock(stream)
            else:
                _FILE_LOCK_STATE[self.key] = (depth - 1, stream)
        finally:
            self.thread_lock.release()


def _safe_name(symbol: str) -> str:
    safe = os.path.basename(symbol)
    if (
        not safe
        or safe in {".", ".."}
        or safe != symbol
        or re.fullmatch(r"[0-9A-Za-z._^#=-]{1,64}", safe) is None
    ):
        raise ValueError("标的代码包含非法字符")
    return safe


def _legacy_safe_name(symbol: str) -> str:
    """Filename used before strict symbol validation preserved ``=`` and ``#``."""
    return re.sub(r"[^0-9A-Za-z._^-]", "_", symbol)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _sync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


BarIntegrityStatus = Literal["ready", "missing", "corrupt", "orphaned"]


@dataclass(frozen=True)
class BarReadResult:
    """Explicit file-integrity outcome; callers no longer need to infer it from ``None``."""

    frame: pd.DataFrame | None
    status: BarIntegrityStatus
    reason: str = ""
    content_sha256: str = ""


@dataclass(frozen=True)
class BarBatchReadResult:
    """One-pass local panel read with manifest and explicit failures."""

    frames: dict[str, pd.DataFrame]
    failures: dict[str, str]
    manifest: tuple[dict[str, Any], ...]
    elapsed_seconds: float


class BarStore:
    def __init__(self, root: Path | None = None):
        self.root = Path(root) if root else get_config().data_root / "bars"
        self.root.mkdir(parents=True, exist_ok=True)
        self.meta_db = self.root / "meta.sqlite"
        with self._conn() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS bar_meta ("
                "symbol TEXT PRIMARY KEY, start TEXT, end TEXT, updated_at REAL)"
            )
            columns = {
                row[1] for row in conn.execute("PRAGMA table_info(bar_meta)").fetchall()
            }
            additions = {
                "coverage_start": "TEXT",
                "coverage_end": "TEXT",
                "checked_at": "REAL",
                "last_source": "TEXT",
                "last_status": "TEXT",
                "content_sha256": "TEXT NOT NULL DEFAULT ''",
                "row_count": "INTEGER NOT NULL DEFAULT 0",
                "file_size": "INTEGER NOT NULL DEFAULT 0",
                "file_mtime_ns": "INTEGER NOT NULL DEFAULT 0",
            }
            for name, kind in additions.items():
                if name not in columns:
                    try:
                        conn.execute(f"ALTER TABLE bar_meta ADD COLUMN {name} {kind}")
                    except sqlite3.OperationalError as exc:
                        # 多进程同时首次打开旧库时，另一进程可能已完成同一迁移。
                        if "duplicate column" not in str(exc).lower():
                            raise
            # 旧缓存升级时视为已在原更新时间完成检查，避免升级后的第一次启动
            # 把所有标的同时当成未检查数据重新触网。
            conn.execute(
                "UPDATE bar_meta SET "
                "coverage_start=COALESCE(coverage_start,start), "
                "coverage_end=COALESCE(coverage_end,end), "
                "checked_at=COALESCE(checked_at,updated_at), "
                "last_source=COALESCE(last_source,''), "
                "last_status=COALESCE(last_status,'ready')"
            )
            conn.execute(
                "CREATE TABLE IF NOT EXISTS bar_write_intents ("
                "symbol TEXT PRIMARY KEY, target_name TEXT NOT NULL, staged_name TEXT NOT NULL,"
                "backup_name TEXT NOT NULL, content_sha256 TEXT NOT NULL,"
                "metadata_json TEXT NOT NULL, created_at REAL NOT NULL)"
            )
        self._recover_writes()

    def _conn(self) -> sqlite3.Connection:
        return connect_sqlite(self.meta_db, policy="cache")

    def _path(self, symbol: str) -> Path:
        return confined_path(
            self.root, f"{_safe_name(symbol)}.parquet", label="行情缓存",
        )

    def path_for_repair(self, symbol: str) -> Path:
        """Resolve a repair target without exposing arbitrary path construction."""
        return self._path(symbol).resolve()

    def _restore_legacy_path(
        self, symbol: str, target: Path, metadata: dict,
    ) -> bool:
        """Atomically adopt a uniquely owned, hash-matched pre-hardening filename."""
        legacy_name = _legacy_safe_name(symbol)
        if legacy_name == _safe_name(symbol):
            return False
        legacy = confined_path(
            self.root, f"{legacy_name}.parquet", label="旧版行情缓存",
        )
        with self.lock(symbol):
            if target.is_file() or not legacy.is_file():
                return False
            expected_hash = str(metadata.get("content_sha256") or "")
            actual_hash = _file_sha256(legacy)
            if not expected_hash or actual_hash != expected_hash:
                logger.warning(
                    "Legacy bar migration rejected symbol=%s reason=hash mismatch",
                    symbol,
                )
                return False
            with self._conn() as connection:
                rows = connection.execute(
                    "SELECT symbol FROM bar_meta WHERE symbol<>?", (symbol,),
                ).fetchall()
            conflicts = [
                str(row[0]) for row in rows
                if _legacy_safe_name(str(row[0])) == legacy_name
            ]
            if conflicts:
                logger.warning(
                    "Legacy bar migration rejected symbol=%s conflicts=%s",
                    symbol, ",".join(conflicts[:5]),
                )
                return False
            os.replace(legacy, target)
            _sync_directory(self.root)
            logger.info(
                "Legacy bar cache migrated symbol=%s from=%s to=%s",
                symbol, legacy.name, target.name,
            )
            return True

    def _resolve_integrity_repair(
        self, symbol: str, content_hash: str, reason: str,
    ) -> None:
        if self.root.name != "bars":
            return
        try:
            from quantmaster.data.repair import resolve_repair

            resolve_repair(
                "bar", f"{self.root.resolve()}::{symbol}",
                result={
                    "state": "validated", "reason": reason,
                    "content_sha256": content_hash,
                },
            )
        except (OSError, sqlite3.Error, ValueError, TypeError) as exc:
            logger.warning(
                "Unable to reconcile bar repair symbol=%s: %s", symbol, exc,
            )

    def lock(self, symbol: str) -> AbstractContextManager:
        """返回跨 BarStore 实例共享的单标的锁，覆盖读取、拉取和原子替换。"""
        return _BarLock(self.root, symbol)

    def _normalize_frame_index(self, value: pd.DataFrame) -> pd.DataFrame:
        """Return daily bars with a timezone-naive trading-date index.

        Older yfinance cache files retain the exchange timezone in parquet.  Daily
        registry boundaries are date strings, so keeping that timezone would make
        otherwise valid cached dates incomparable with the requested range.
        """
        if isinstance(value.index, pd.DatetimeIndex) and value.index.tz is not None:
            value = value.copy()
            value.index = value.index.tz_localize(None)
        return value

    def read(
        self,
        symbol: str,
        columns: list[str] | None = None,
        *,
        enqueue_repair: bool = True,
    ) -> BarReadResult:
        """Read a cache file and report missing/corrupt/orphaned states explicitly."""
        path = self._path(symbol)
        metadata = self.metadata(symbol)
        migrated = False
        if not path.exists() and metadata is not None:
            migrated = self._restore_legacy_path(symbol, path, metadata)
        if not path.exists():
            if metadata is None:
                return BarReadResult(None, "missing")
            reason = "cataloged bar file is missing"
            self._record_integrity_failure(symbol, reason, metadata, enqueue_repair)
            return BarReadResult(None, "corrupt", reason)
        try:
            orphan_reason = ""
            if metadata is None:
                orphan_reason = "bar file exists without catalog metadata"
                self._record_integrity_failure(symbol, orphan_reason, {}, enqueue_repair)
            stat = path.stat()
            expected_hash = str((metadata or {}).get("content_sha256") or "")
            unchanged = bool(
                expected_hash
                and int((metadata or {}).get("file_size") or 0) == stat.st_size
                and int((metadata or {}).get("file_mtime_ns") or 0) == stat.st_mtime_ns
            )
            if not unchanged:
                actual_hash = _file_sha256(path)
                if expected_hash and actual_hash != expected_hash:
                    reason = "bar content hash mismatch"
                    self._record_integrity_failure(
                        symbol, reason, metadata or {}, enqueue_repair,
                    )
                    return BarReadResult(None, "corrupt", reason, actual_hash)
            value = self._normalize_frame_index(pd.read_parquet(path, columns=columns))
            if orphan_reason:
                # A readable legacy/orphan file is useful as an explicit degraded
                # offline result while its catalog entry is rebuilt.  It must never
                # be promoted to "ready" or silently adopted here.
                return BarReadResult(
                    value, "orphaned", orphan_reason, _file_sha256(path),
                )
            if metadata and columns is None and int(metadata.get("row_count") or 0) not in {
                0, len(value),
            }:
                reason = "bar row count differs from catalog"
                self._record_integrity_failure(
                    symbol, reason, metadata, enqueue_repair,
                )
                return BarReadResult(
                    None, "corrupt", reason, expected_hash if unchanged else actual_hash,
                )
            if not unchanged:
                self._backfill_file_identity(symbol, path, value, actual_hash)
            content_hash = expected_hash or actual_hash
            if migrated and metadata is not None:
                self.mark_status(
                    symbol, "ready", source=str(metadata.get("last_source") or ""),
                )
                self._resolve_integrity_repair(
                    symbol, content_hash, "legacy_filename_migrated",
                )
            return BarReadResult(value, "ready", content_sha256=content_hash)
        except (OSError, ValueError, TypeError, ImportError) as exc:
            reason = f"bar file cannot be read: {type(exc).__name__}: {exc}"
            self._record_integrity_failure(symbol, reason, metadata or {}, enqueue_repair)
            return BarReadResult(None, "corrupt", reason)
        except Exception as exc:
            # PyArrow exposes version-specific exception classes.  This remains a storage
            # boundary, but the failure is classified, logged and persisted for repair.
            reason = f"bar parquet read failed: {type(exc).__name__}: {exc}"
            logger.exception("BarStore read failed symbol=%s path=%s", symbol, path)
            self._record_integrity_failure(symbol, reason, metadata or {}, enqueue_repair)
            return BarReadResult(None, "corrupt", reason)

    def get(self, symbol: str, columns: list[str] | None = None) -> pd.DataFrame | None:
        """Compatibility convenience over :meth:`read`; integrity remains queryable."""
        return self.read(symbol, columns).frame

    def read_many(
        self,
        symbols: list[str],
        columns: list[str] | None = None,
        *,
        start: str = "",
        end: str = "",
        ranges: dict[str, tuple[str, str]] | None = None,
        max_workers: int = 8,
        enqueue_repair: bool = True,
    ) -> BarBatchReadResult:
        """Read catalogued Parquet files exactly once and never contact a provider.

        The catalog is fetched in one logical operation. Files whose size and mtime
        still match the persisted SHA-256 identity skip re-hashing; changed files go
        through the normal integrity boundary before they are returned.
        """
        ordered = list(dict.fromkeys(str(symbol) for symbol in symbols if symbol))
        started = time.perf_counter()
        metadata = self.metadata_many(ordered)
        manifest: list[dict[str, Any]] = []
        for symbol in ordered:
            row = metadata.get(symbol) or {}
            path = self._path(symbol)
            stat = path.stat() if path.is_file() else None
            identity_matches = bool(
                stat is not None
                and int(row.get("file_size") or 0) == stat.st_size
                and int(row.get("file_mtime_ns") or 0) == stat.st_mtime_ns
            )
            manifest.append({
                "symbol": symbol,
                "coverage": [
                    str(row.get("coverage_start") or row.get("start") or ""),
                    str(row.get("coverage_end") or row.get("end") or ""),
                ],
                "bytes": int(stat.st_size if stat is not None else 0),
                "mtime_ns": int(stat.st_mtime_ns if stat is not None else 0),
                "content_sha256": str(row.get("content_sha256") or "") if identity_matches else "",
                "status": str(row.get("last_status") or ("missing" if not path.is_file() else "")),
            })

        def one(symbol: str) -> tuple[pd.DataFrame | None, str]:
            row = metadata.get(symbol)
            path = self._path(symbol)
            if row is None or not path.is_file():
                return None, "本地行情文件不存在"
            try:
                requested_start, requested_end = (ranges or {}).get(symbol, (start, end))
                stat = path.stat()
                unchanged = bool(
                    row.get("content_sha256")
                    and int(row.get("file_size") or 0) == stat.st_size
                    and int(row.get("file_mtime_ns") or 0) == stat.st_mtime_ns
                )
                if unchanged:
                    if columns:
                        try:
                            import pyarrow.parquet as parquet

                            source = parquet.ParquetFile(path, memory_map=True)
                            pandas_meta = source.schema_arrow.pandas_metadata or {}
                            index_columns = [
                                item for item in pandas_meta.get("index_columns", [])
                                if isinstance(item, str)
                            ]
                            projected = list(dict.fromkeys([*columns, *index_columns]))
                            frame = source.read(
                                columns=projected, use_threads=False,
                            ).to_pandas()
                        except (ImportError, OSError, ValueError, TypeError):
                            frame = pd.read_parquet(path, columns=columns)
                    else:
                        frame = pd.read_parquet(path)
                    frame = self._normalize_frame_index(frame)
                    if columns is None and int(row.get("row_count") or 0) not in {0, len(frame)}:
                        unchanged = False
                    else:
                        if requested_start or requested_end:
                            frame = frame.loc[requested_start or None:requested_end or None]
                        return (frame, "") if not frame.empty else (None, "请求区间没有本地行情")
                result = self.read(symbol, columns, enqueue_repair=enqueue_repair)
                frame = result.frame
                if frame is not None and (requested_start or requested_end):
                    frame = frame.loc[requested_start or None:requested_end or None]
                if frame is not None and not frame.empty:
                    return frame, ""
                return None, result.reason or result.status
            except (OSError, RuntimeError, ValueError, TypeError, ImportError) as exc:
                logger.warning("批量读取本地行情失败 symbol=%s: %s", symbol, exc)
                return None, f"本地行情读取失败: {type(exc).__name__}"

        frames: dict[str, pd.DataFrame] = {}
        failures: dict[str, str] = {}
        workers = min(16, max(1, int(max_workers)), max(1, len(ordered)))
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="bar-local") as executor:
            futures = {executor.submit(one, symbol): symbol for symbol in ordered}
            for future in as_completed(futures):
                symbol = futures[future]
                frame, error = future.result()
                if frame is not None:
                    frames[symbol] = frame
                else:
                    failures[symbol] = error
        frames = {symbol: frames[symbol] for symbol in ordered if symbol in frames}
        for item in manifest:
            frame = frames.get(str(item["symbol"]))
            if frame is not None and not frame.empty:
                item["actual_coverage"] = [
                    pd.Timestamp(frame.index.min()).strftime("%Y-%m-%d"),
                    pd.Timestamp(frame.index.max()).strftime("%Y-%m-%d"),
                ]
            elif str(item["symbol"]) in failures:
                item["read_error"] = failures[str(item["symbol"])]
        return BarBatchReadResult(
            frames=frames,
            failures=failures,
            manifest=tuple(manifest),
            elapsed_seconds=time.perf_counter() - started,
        )

    def _record_integrity_failure(
        self,
        symbol: str,
        reason: str,
        metadata: dict,
        enqueue: bool,
    ) -> None:
        self._mark_corrupt(symbol)
        log_key = (str(self.root.resolve()), symbol, reason)
        now = time.time()
        with _INTEGRITY_LOG_GUARD:
            previous = _INTEGRITY_LOGGED_AT.get(log_key, 0.0)
            should_log = now - previous >= 600
            if should_log:
                _INTEGRITY_LOGGED_AT[log_key] = now
        if should_log:
            logger.error("BarStore integrity failure symbol=%s reason=%s", symbol, reason)
        else:
            logger.debug(
                "BarStore repeated integrity failure suppressed symbol=%s reason=%s",
                symbol, reason,
            )
        if not enqueue or self.root.name != "bars":
            return
        try:
            from quantmaster.data.repair import enqueue_repair

            enqueue_repair(
                "bar",
                f"{self.root.resolve()}::{symbol}",
                reason=reason,
                spec={
                    "root": str(self.root.resolve()),
                    "symbol": symbol,
                    "start": str(metadata.get("coverage_start") or metadata.get("start") or ""),
                    "end": str(metadata.get("coverage_end") or metadata.get("end") or ""),
                    "content_sha256": str(metadata.get("content_sha256") or ""),
                },
                source=str(metadata.get("last_source") or "market"),
            )
        except (OSError, sqlite3.Error, ValueError, TypeError) as exc:
            logger.error("Unable to enqueue bar repair symbol=%s: %s", symbol, exc)

    def _mark_corrupt(self, symbol: str) -> None:
        try:
            self.mark_status(symbol, "corrupt")
        except sqlite3.Error as exc:
            logger.error("Unable to mark corrupt bar symbol=%s: %s", symbol, exc)

    @staticmethod
    def _metadata_values(metadata: dict) -> tuple:
        return tuple(metadata.get(column) for column in _META_COLUMNS)

    def _commit_metadata(
        self, connection: sqlite3.Connection, metadata: dict, *, clear_intent: bool,
    ) -> None:
        placeholders = ",".join("?" for _ in _META_COLUMNS)
        connection.execute(
            f"INSERT OR REPLACE INTO bar_meta ({','.join(_META_COLUMNS)}) "
            f"VALUES ({placeholders})",
            self._metadata_values(metadata),
        )
        if clear_intent:
            connection.execute(
                "DELETE FROM bar_write_intents WHERE symbol=?", (metadata["symbol"],),
            )

    def _backfill_file_identity(
        self, symbol: str, path: Path, frame: pd.DataFrame, content_hash: str,
    ) -> None:
        stat = path.stat()
        with self._conn() as connection:
            connection.execute(
                "UPDATE bar_meta SET content_sha256=?,row_count=?,file_size=?,file_mtime_ns=? "
                "WHERE symbol=?",
                (content_hash, len(frame), stat.st_size, stat.st_mtime_ns, symbol),
            )

    def _recover_writes(self) -> None:
        """Complete or roll back interrupted file/catalog commits."""
        with self._conn() as connection:
            rows = connection.execute("SELECT * FROM bar_write_intents").fetchall()
        for row in rows:
            symbol = str(row[0])
            with self.lock(symbol):
                try:
                    names = [str(row[index]) for index in (1, 2, 3)]
                    if any(Path(name).name != name for name in names):
                        raise ValueError("bar write intent path escaped its root")
                    target, staged, backup = (self.root / name for name in names)
                    expected = str(row[4])
                    metadata = json.loads(str(row[5]))
                    target_ready = target.is_file() and _file_sha256(target) == expected
                    staged_ready = staged.is_file() and _file_sha256(staged) == expected
                    if not target_ready and staged_ready:
                        if target.exists() and not backup.exists():
                            os.replace(target, backup)
                        elif target.exists():
                            target.unlink()
                        os.replace(staged, target)
                        _sync_directory(self.root)
                        target_ready = True
                    if target_ready:
                        stat = target.stat()
                        metadata.update({
                            "file_size": stat.st_size,
                            "file_mtime_ns": stat.st_mtime_ns,
                        })
                        with self._conn() as connection:
                            self._commit_metadata(connection, metadata, clear_intent=True)
                        staged.unlink(missing_ok=True)
                        backup.unlink(missing_ok=True)
                    elif backup.is_file():
                        target.unlink(missing_ok=True)
                        os.replace(backup, target)
                        _sync_directory(self.root)
                        staged.unlink(missing_ok=True)
                        with self._conn() as connection:
                            connection.execute(
                                "DELETE FROM bar_write_intents WHERE symbol=?", (symbol,),
                            )
                    else:
                        staged.unlink(missing_ok=True)
                        with self._conn() as connection:
                            connection.execute(
                                "DELETE FROM bar_write_intents WHERE symbol=?", (symbol,),
                            )
                except (OSError, sqlite3.Error, ValueError):
                    # Leave the intent intact; a later startup can retry without guessing.
                    continue

    def put(self, symbol: str, df: pd.DataFrame, replace: bool = False) -> None:
        """写入缓存。

        ``replace=True`` 只用于已确认完整的前复权响应；来源只返回部分区间时
        必须合并保存，避免 AKShare 的缺块响应冲掉本地已有研究数据。
        """
        if df is None or df.empty:
            return
        df = self._normalize_frame_index(df)
        from quantmaster.runtime.maintenance import maintenance_barrier

        maintenance_barrier.require_writable()
        with self.lock(symbol):
            old_meta = self.metadata(symbol)
            if not replace:
                old = self.get(symbol)
                if old is not None and not old.empty:
                    df = pd.concat([old, df])
                    df = df[~df.index.duplicated(keep="last")].sort_index()
            df = df.sort_index()
            target = self._path(symbol)
            fd, temp_name = tempfile.mkstemp(
                prefix=f".{target.stem}.", suffix=".parquet.tmp", dir=self.root)
            os.close(fd)
            temp_path = Path(temp_name)
            try:
                df.to_parquet(temp_path)
                with temp_path.open("rb+") as stream:
                    os.fsync(stream.fileno())
                content_hash = _file_sha256(temp_path)
                now = time.time()
                start, end = str(df.index.min().date()), str(df.index.max().date())
                coverage_start = (old_meta or {}).get("coverage_start") or start
                coverage_end = (old_meta or {}).get("coverage_end") or end
                metadata = {
                    "symbol": symbol, "start": start, "end": end, "updated_at": now,
                    "coverage_start": coverage_start, "coverage_end": coverage_end,
                    "checked_at": now, "last_source": (old_meta or {}).get("last_source", ""),
                    "last_status": "ready", "content_sha256": content_hash,
                    "row_count": len(df), "file_size": temp_path.stat().st_size,
                    "file_mtime_ns": 0,
                }
                backup = self.root / f".{target.stem}.{uuid.uuid4().hex}.parquet.bak"
                with self._conn() as conn:
                    conn.execute(
                        "INSERT OR REPLACE INTO bar_write_intents "
                        "(symbol,target_name,staged_name,backup_name,content_sha256,"
                        "metadata_json,created_at) VALUES (?,?,?,?,?,?,?)",
                        (symbol, target.name, temp_path.name, backup.name, content_hash,
                         json.dumps(metadata, ensure_ascii=False, separators=(",", ":")), now),
                    )
                if target.exists():
                    os.replace(target, backup)
                os.replace(temp_path, target)
                _sync_directory(self.root)
                stat = target.stat()
                metadata.update({"file_size": stat.st_size, "file_mtime_ns": stat.st_mtime_ns})
                with self._conn() as conn:
                    self._commit_metadata(conn, metadata, clear_intent=True)
                backup.unlink(missing_ok=True)
                _sync_directory(self.root)
                self._resolve_integrity_repair(
                    symbol, content_hash, "cache_rewritten",
                )
            finally:
                temp_path.unlink(missing_ok=True)

    def metadata(self, symbol: str) -> dict | None:
        """返回单个标的的缓存覆盖与检查状态。"""
        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM bar_meta WHERE symbol=?", (symbol,)
            ).fetchone()
        return dict(row) if row else None

    def metadata_many(self, symbols: list[str] | None = None) -> dict[str, dict]:
        """批量读取元信息，避免面板加载时为每只股票反复连接 SQLite。"""
        with self._conn() as conn:
            conn.row_factory = sqlite3.Row
            if symbols:
                rows = []
                for start in range(0, len(symbols), 500):
                    chunk = symbols[start:start + 500]
                    placeholders = ",".join("?" for _ in chunk)
                    rows.extend(conn.execute(
                        f"SELECT * FROM bar_meta WHERE symbol IN ({placeholders})", chunk
                    ).fetchall())
            else:
                rows = conn.execute("SELECT * FROM bar_meta").fetchall()
        return {str(row["symbol"]): dict(row) for row in rows}

    def mark_checked(
        self,
        symbol: str,
        start: str,
        end: str,
        *,
        source: str = "",
        status: str = "ready",
        replace_coverage: bool = False,
    ) -> None:
        """记录已经成功检查的请求范围；没有新 K 线时也必须调用。"""
        with self.lock(symbol), self._conn() as conn:
            row = conn.execute(
                "SELECT coverage_start,coverage_end FROM bar_meta WHERE symbol=?", (symbol,)
            ).fetchone()
            if row is None:
                return
            if replace_coverage:
                coverage_start, coverage_end = start, end
            else:
                coverage_start = min(filter(None, (row[0], start)))
                coverage_end = max(filter(None, (row[1], end)))
            conn.execute(
                "UPDATE bar_meta SET coverage_start=?,coverage_end=?,checked_at=?,"
                "last_source=?,last_status=? WHERE symbol=?",
                (coverage_start, coverage_end, time.time(), source, status, symbol),
            )

    def mark_status(self, symbol: str, status: str, source: str = "") -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE bar_meta SET last_status=?,last_source=? WHERE symbol=?",
                (status, source, symbol),
            )

    def freshness(self, symbol: str) -> float | None:
        """距上次更新的秒数；无记录返回 None。"""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT updated_at FROM bar_meta WHERE symbol=?", (symbol,)
            ).fetchone()
        return (time.time() - row[0]) if row else None

    def check_freshness(self, symbol: str) -> float | None:
        """距最近一次成功检查的秒数；与数据实际更新时间分开。"""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT checked_at FROM bar_meta WHERE symbol=?", (symbol,)
            ).fetchone()
        return (time.time() - row[0]) if row and row[0] is not None else None

    def coverage(self, symbol: str) -> tuple[str, str] | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT start, end FROM bar_meta WHERE symbol=?", (symbol,)
            ).fetchone()
        return (row[0], row[1]) if row else None

    def symbols(self) -> list[str]:
        with self._conn() as conn:
            rows = conn.execute("SELECT symbol FROM bar_meta ORDER BY symbol").fetchall()
        return [r[0] for r in rows]


class IntradayBarStore(BarStore):
    """分钟线缓存；按频率隔离目录，避免 1m/5m 数据相互覆盖。"""

    def __init__(self, frequency: str = "5m", root: Path | None = None):
        from quantmaster.data.base import validate_frequency

        self.frequency = validate_frequency(frequency)
        if self.frequency == "1d":
            raise ValueError("IntradayBarStore 仅用于分钟线")
        base = Path(root) if root else get_config().data_root / "bars" / "intraday"
        if self.frequency == "1m":
            directory = "1m"
        elif self.frequency == "5m":
            directory = "5m"
        elif self.frequency == "15m":
            directory = "15m"
        elif self.frequency == "30m":
            directory = "30m"
        elif self.frequency == "60m":
            directory = "60m"
        else:  # validate_frequency 已拒绝未知值；保留显式安全边界。
            raise ValueError("IntradayBarStore 收到未知分钟频率")
        super().__init__(base / directory)

    def _normalize_frame_index(self, value: pd.DataFrame) -> pd.DataFrame:
        """Intraday timestamps retain their provider timezone when one is present."""
        return value
