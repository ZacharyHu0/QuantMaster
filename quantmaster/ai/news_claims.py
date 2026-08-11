"""Fenced, renewable claims for news analysis work.

The table in this module contains coordination state only.  It is deliberately
safe to rebuild: user-owned news and completed annotations remain in ``news``.
"""

from __future__ import annotations

import secrets
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from quantmaster.runtime.sqlite import connect_sqlite

ClaimMode = Literal["pending", "failed", "dead_letter"]


@dataclass(frozen=True)
class ClaimBatch:
    ids: tuple[int, ...]
    token: str
    recovered_leases: int = 0


def normalize_news_ids(values: list[int] | None) -> list[int] | None:
    """Validate the public 1..1000 positive-ID contract and preserve order."""
    if values is None:
        return None
    selected: list[int] = []
    seen: set[int] = set()
    for raw in values:
        value = int(raw)
        if value <= 0:
            raise ValueError("资讯 ID 必须是正整数")
        if value in seen:
            continue
        seen.add(value)
        selected.append(value)
        if len(selected) > 1000:
            raise ValueError("一次最多处理 1000 个资讯 ID")
    return selected


class NewsClaimStore:
    """Atomic SQLite claims with token fencing and bounded renewable leases."""

    def __init__(self, path: str | Path, *, read_only: bool = False):
        self.path = Path(path)
        self.read_only = bool(read_only)

    def _conn(self) -> sqlite3.Connection:
        return connect_sqlite(
            self.path,
            timeout=0.25 if self.read_only else 10.0,
            row_factory=True,
            read_only=self.read_only,
        )

    @staticmethod
    def migrate(connection: sqlite3.Connection) -> None:
        connection.executescript("""
            CREATE TABLE IF NOT EXISTS news_analysis_claims (
                news_id INTEGER PRIMARY KEY,
                owner TEXT NOT NULL,
                token TEXT NOT NULL,
                task_type TEXT NOT NULL,
                claimed_at REAL NOT NULL,
                heartbeat_at REAL NOT NULL,
                lease_expires_at REAL NOT NULL);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_news_claim_token
                ON news_analysis_claims(token,news_id);
            CREATE INDEX IF NOT EXISTS idx_news_claim_expiry
                ON news_analysis_claims(lease_expires_at);
        """)

    @staticmethod
    def _eligible(mode: ClaimMode, *, manual: bool) -> tuple[str, list[object]]:
        if mode == "failed":
            return "n.analysis_status='failed'", []
        if mode == "dead_letter":
            sql = "n.analysis_status='dead_letter'"
            if manual:
                return sql, []
            return (
                f"{sql} AND n.analysis_recovery_count<3 AND n.next_retry_at<=?",
                [time.time()],
            )
        return (
            "n.analysis_status IN ('pending','failed','recovery') "
            "AND n.analysis_attempts<3 AND n.next_retry_at<=?",
            [time.time()],
        )

    @staticmethod
    def _id_predicate(ids: list[int] | None) -> tuple[str, list[object]]:
        if ids is None:
            return "", []
        if not ids:
            return " AND 0", []
        placeholders = ",".join("?" for _ in ids)
        return f" AND n.id IN ({placeholders})", list(ids)

    def claim(
        self,
        *,
        owner: str,
        task_type: str,
        mode: ClaimMode,
        limit: int,
        ids: list[int] | None = None,
        max_id: int | None = None,
        manual: bool = False,
        lease_seconds: float = 90.0,
        now: float | None = None,
    ) -> ClaimBatch:
        current = time.time() if now is None else float(now)
        normalized = normalize_news_ids(ids)
        eligible, params = self._eligible(mode, manual=manual)
        # _eligible uses the real clock so tests can inject an exact boundary.
        params = [current if isinstance(value, float) else value for value in params]
        id_sql, id_params = self._id_predicate(normalized)
        max_sql = " AND n.id<=?" if max_id is not None else ""
        if max_id is not None:
            id_params.append(int(max_id))
        token = secrets.token_urlsafe(24)
        selected_limit = max(1, min(int(limit), 1000))
        with self._conn() as connection:
            connection.execute("BEGIN IMMEDIATE")
            self.migrate(connection)
            connection.execute(
                "DELETE FROM news_analysis_claims WHERE news_id NOT IN (SELECT id FROM news)"
            )
            rows = connection.execute(
                "SELECT n.id,c.lease_expires_at FROM news n "
                "LEFT JOIN news_analysis_claims c ON c.news_id=n.id WHERE "
                f"{eligible}{id_sql}{max_sql} "
                "AND (c.news_id IS NULL OR c.lease_expires_at<=?) "
                "ORDER BY n.id LIMIT ?",
                [*params, *id_params, current, selected_limit],
            ).fetchall()
            selected = tuple(int(row["id"]) for row in rows)
            recovered = sum(
                row["lease_expires_at"] is not None
                and float(row["lease_expires_at"] or 0) <= current
                for row in rows
            )
            if not selected:
                return ClaimBatch((), "", recovered)
            placeholders = ",".join("?" for _ in selected)
            connection.execute(
                f"DELETE FROM news_analysis_claims WHERE news_id IN ({placeholders}) ",
                selected,
            )
            expiry = current + max(15.0, float(lease_seconds))
            connection.executemany(
                "INSERT INTO news_analysis_claims "
                "(news_id,owner,token,task_type,claimed_at,heartbeat_at,lease_expires_at) "
                "VALUES (?,?,?,?,?,?,?)",
                [
                    (news_id, owner, token, task_type[:80], current, current, expiry)
                    for news_id in selected
                ],
            )
            if mode == "failed":
                connection.execute(
                    "UPDATE news SET analysis_status='pending',analysis_attempts=0,"
                    "analysis_error='',next_retry_at=0,last_failure_code='' "
                    f"WHERE id IN ({placeholders}) AND analysis_status='failed'",
                    selected,
                )
            elif mode == "dead_letter":
                connection.execute(
                    "UPDATE news SET analysis_status='recovery',analysis_attempts=0,"
                    "analysis_recovery_count=analysis_recovery_count+1,next_retry_at=0 "
                    f"WHERE id IN ({placeholders}) AND analysis_status='dead_letter'",
                    selected,
                )
        return ClaimBatch(selected, token, recovered)

    def heartbeat(
        self,
        token: str,
        owner: str,
        *,
        lease_seconds: float = 90.0,
        now: float | None = None,
    ) -> int:
        current = time.time() if now is None else float(now)
        with self._conn() as connection:
            changed = connection.execute(
                "UPDATE news_analysis_claims SET heartbeat_at=?,lease_expires_at=? "
                "WHERE token=? AND owner=? AND lease_expires_at>?",
                (current, current + max(15.0, float(lease_seconds)), token, owner, current),
            ).rowcount
        return int(changed)

    def owns(
        self,
        news_id: int,
        token: str,
        owner: str,
        *,
        now: float | None = None,
        connection: sqlite3.Connection | None = None,
    ) -> bool:
        current = time.time() if now is None else float(now)
        if connection is not None:
            row = connection.execute(
                "SELECT 1 FROM news_analysis_claims WHERE news_id=? AND token=? "
                "AND owner=? AND lease_expires_at>?",
                (int(news_id), token, owner, current),
            ).fetchone()
            return row is not None
        with self._conn() as owned:
            return self.owns(news_id, token, owner, now=current, connection=owned)

    def release(self, token: str, owner: str) -> int:
        if not token:
            return 0
        with self._conn() as connection:
            changed = connection.execute(
                "DELETE FROM news_analysis_claims WHERE token=? AND owner=?",
                (token, owner),
            ).rowcount
        return int(changed)

    def stats(self, *, now: float | None = None) -> dict[str, int]:
        current = time.time() if now is None else float(now)
        with self._conn() as connection:
            if not self.read_only:
                self.migrate(connection)
            row = connection.execute(
                "SELECT COUNT(*) AS total,"
                "SUM(lease_expires_at>?) AS active,"
                "SUM(lease_expires_at<=?) AS expired FROM news_analysis_claims",
                (current, current),
            ).fetchone()
        return {
            "total": int(row["total"] or 0),
            "active": int(row["active"] or 0),
            "expired": int(row["expired"] or 0),
        }
