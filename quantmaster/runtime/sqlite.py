"""Consistent, concurrency-safe SQLite connections and schema migrations."""

from __future__ import annotations

import sqlite3
import threading
import time
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Literal

SQLitePolicy = Literal["authoritative", "cache"]

_INIT_GUARD = threading.Lock()
_INIT_LOCKS: dict[str, threading.RLock] = {}
_WAL_READY: set[str] = set()


class _ManagedConnection(sqlite3.Connection):
    """Commit or roll back, then deterministically release the database handle."""

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()


def _database_key(path: str | Path) -> str:
    return str(Path(path).expanduser().resolve())


def _init_lock(key: str) -> threading.RLock:
    with _INIT_GUARD:
        return _INIT_LOCKS.setdefault(key, threading.RLock())


def _enable_wal(connection: sqlite3.Connection, key: str) -> None:
    if key in _WAL_READY:
        return
    with _init_lock(key):
        if key in _WAL_READY:
            return
        delay = 0.02
        for attempt in range(8):
            try:
                current = str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower()
                if current != "wal":
                    current = str(
                        connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
                    ).lower()
                if current != "wal":
                    raise sqlite3.OperationalError(f"无法启用 WAL，当前模式为 {current}")
                _WAL_READY.add(key)
                return
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc).lower() or attempt == 7:
                    raise
                time.sleep(delay)
                delay = min(delay * 2, 0.5)


def connect_sqlite(
    path: str | Path,
    *,
    policy: SQLitePolicy = "authoritative",
    timeout: float = 30.0,
    row_factory: bool = False,
) -> sqlite3.Connection:
    """Open a configured connection without racing WAL initialization."""
    destination = Path(path).expanduser()
    destination.parent.mkdir(parents=True, exist_ok=True)
    from quantmaster.runtime.maintenance import MaintenanceActiveError, maintenance_barrier

    if maintenance_barrier.frozen and not destination.exists():
        raise MaintenanceActiveError("维护期间不能创建新的 SQLite 数据库")
    key = _database_key(destination)
    connection = sqlite3.connect(
        destination, timeout=timeout, factory=_ManagedConnection,
    )
    try:
        connection.execute(f"PRAGMA busy_timeout={max(1, int(timeout * 1000))}")
        connection.execute("PRAGMA foreign_keys=ON")
        _enable_wal(connection, key)
        connection.execute(
            "PRAGMA synchronous=FULL" if policy == "authoritative"
            else "PRAGMA synchronous=NORMAL"
        )
        if row_factory:
            connection.row_factory = sqlite3.Row
        if maintenance_barrier.frozen:
            connection.execute("PRAGMA query_only=ON")
        return connection
    except Exception:
        connection.close()
        raise


Migration = tuple[int, Callable[[sqlite3.Connection], None]]


def execute_sql_script(connection: sqlite3.Connection, script: str) -> None:
    """Execute a DDL script without ``executescript``'s implicit commit.

    ``sqlite3.Connection.executescript`` commits any active transaction before
    running its input.  Schema callbacks use this helper so the DDL, data
    backfill and ``user_version`` update remain one atomic migration.
    """
    pending: list[str] = []
    for line in script.splitlines(keepends=True):
        pending.append(line)
        statement = "".join(pending)
        if not sqlite3.complete_statement(statement):
            continue
        sql = statement.strip()
        if sql:
            connection.execute(sql)
        pending.clear()
    remainder = "".join(pending).strip()
    if remainder:
        raise sqlite3.OperationalError("incomplete SQL migration statement")


def migrate_schema(
    connection: sqlite3.Connection,
    migrations: Iterable[Migration],
) -> int:
    """Apply ordered migrations transactionally using ``PRAGMA user_version``."""
    current = int(connection.execute("PRAGMA user_version").fetchone()[0])
    for version, migrate in sorted(migrations, key=lambda item: item[0]):
        if version <= current:
            continue
        connection.execute("BEGIN IMMEDIATE")
        try:
            migrate(connection)
            connection.execute(f"PRAGMA user_version={int(version)}")
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        current = version
    return current


def reset_sqlite_runtime_for_tests() -> None:
    """Forget process-local WAL state after tests replace database roots."""
    with _INIT_GUARD:
        _WAL_READY.clear()
        _INIT_LOCKS.clear()
