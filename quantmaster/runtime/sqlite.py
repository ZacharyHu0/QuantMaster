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
    read_only: bool = False,
) -> sqlite3.Connection:
    """Open a configured connection without racing WAL initialization.

    ``read_only`` is deliberately a real SQLite read-only connection, rather
    than merely a convention.  Web snapshot readers use it to guarantee that
    a missing database, a schema migration, or WAL setup can never turn a GET
    into a write or a long lock wait.
    """
    destination = Path(path).expanduser()
    if read_only:
        if not destination.is_file():
            raise FileNotFoundError(destination)
    else:
        destination.parent.mkdir(parents=True, exist_ok=True)
    from quantmaster.runtime.maintenance import MaintenanceActiveError, maintenance_barrier

    if maintenance_barrier.frozen and not destination.exists():
        raise MaintenanceActiveError("维护期间不能创建新的 SQLite 数据库")
    key = _database_key(destination)
    if read_only:
        uri = f"{destination.resolve().as_uri()}?mode=ro"
        connection = sqlite3.connect(
            uri, uri=True, timeout=timeout, factory=_ManagedConnection,
        )
    else:
        connection = sqlite3.connect(
            destination, timeout=timeout, factory=_ManagedConnection,
        )
    try:
        connection.execute(f"PRAGMA busy_timeout={max(1, int(timeout * 1000))}")
        connection.execute("PRAGMA foreign_keys=ON")
        if read_only:
            connection.execute("PRAGMA query_only=ON")
        else:
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


def connect_sqlite_recovery(path: str | Path, *, read_only: bool = False) -> sqlite3.Connection:
    """Open a maintenance-only connection without changing journal policy."""

    destination = Path(path).expanduser()
    if not destination.is_absolute():
        raise ValueError("SQLite recovery path must be absolute")
    if read_only:
        if not destination.is_file():
            raise FileNotFoundError(destination)
        return sqlite3.connect(
            f"{destination.resolve().as_uri()}?mode=ro", uri=True,
            timeout=30.0, factory=_ManagedConnection,
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(destination, timeout=30.0, factory=_ManagedConnection)


def connect_sqlite_diagnostic(
    path: str | Path, *, timeout: float = 0.25,
) -> sqlite3.Connection:
    """Open the main database file for a strictly side-effect-free diagnosis.

    ``immutable=1`` prevents SQLite from creating or consulting WAL/SHM files.
    Callers must inspect and disclose sidecars separately: this connection can
    establish whether the main file is readable, but a non-empty WAL requires
    a later quiescent check before declaring the complete database healthy.
    """

    destination = Path(path).expanduser()
    if not destination.is_absolute():
        raise ValueError("SQLite diagnostic path must be absolute")
    if not destination.is_file():
        raise FileNotFoundError(destination)
    connection = sqlite3.connect(
        f"{destination.resolve().as_uri()}?mode=ro&immutable=1",
        uri=True, timeout=timeout, factory=_ManagedConnection,
    )
    connection.execute("PRAGMA query_only=ON")
    return connection


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
        connection.execute("BEGIN IMMEDIATE")
        try:
            # Read the version *after* the writer lock is held.  Reading it
            # before BEGIN IMMEDIATE lets two first-use constructors both see
            # version zero; the loser would then run CREATE TABLE after the
            # winner committed and report a spurious "already exists" error.
            current = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version <= current:
                connection.rollback()
                continue
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
