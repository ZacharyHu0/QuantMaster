"""本地数据缓存：日线存 Parquet（每个 symbol 一个文件），元信息存 SQLite。

免费数据源普遍有频率限制，本地缓存能显著加速研究迭代，也让回测可复现。
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import tempfile
import threading
import time
import uuid
from pathlib import Path

import pandas as pd

from quantmaster.config import get_config
from quantmaster.runtime.sqlite import connect_sqlite

_LOCKS_GUARD = threading.Lock()
_SYMBOL_LOCKS: dict[tuple[str, str], threading.RLock] = {}

_META_COLUMNS = (
    "symbol", "start", "end", "updated_at", "coverage_start", "coverage_end",
    "checked_at", "last_source", "last_status", "content_sha256", "row_count",
    "file_size", "file_mtime_ns",
)


def _symbol_lock(root: Path, symbol: str) -> threading.RLock:
    key = (str(root.resolve()), symbol)
    with _LOCKS_GUARD:
        return _SYMBOL_LOCKS.setdefault(key, threading.RLock())


def _safe_name(symbol: str) -> str:
    return re.sub(r"[^0-9A-Za-z._^-]", "_", symbol)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


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
        return self.root / f"{_safe_name(symbol)}.parquet"

    def lock(self, symbol: str) -> threading.RLock:
        """返回跨 BarStore 实例共享的单标的锁，覆盖读取、拉取和原子替换。"""
        return _symbol_lock(self.root, symbol)

    def get(self, symbol: str, columns: list[str] | None = None) -> pd.DataFrame | None:
        path = self._path(symbol)
        if not path.exists():
            return None
        try:
            metadata = self.metadata(symbol)
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
                    self._mark_corrupt(symbol)
                    return None
            value = pd.read_parquet(path, columns=columns)
            if metadata and columns is None and int(metadata.get("row_count") or 0) not in {
                0, len(value),
            }:
                self._mark_corrupt(symbol)
                return None
            if not unchanged:
                self._backfill_file_identity(symbol, path, value, actual_hash)
            return value
        except Exception:
            self._mark_corrupt(symbol)
            return None

    def _mark_corrupt(self, symbol: str) -> None:
        try:
            self.mark_status(symbol, "corrupt")
        except sqlite3.Error:
            pass

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
            with _symbol_lock(self.root, symbol):
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
        with _symbol_lock(self.root, symbol):
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
                stat = target.stat()
                metadata.update({"file_size": stat.st_size, "file_mtime_ns": stat.st_mtime_ns})
                with self._conn() as conn:
                    self._commit_metadata(conn, metadata, clear_intent=True)
                backup.unlink(missing_ok=True)
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
                placeholders = ",".join("?" for _ in symbols)
                rows = conn.execute(
                    f"SELECT * FROM bar_meta WHERE symbol IN ({placeholders})", symbols
                ).fetchall()
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
        with _symbol_lock(self.root, symbol), self._conn() as conn:
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
        super().__init__(base / self.frequency)
