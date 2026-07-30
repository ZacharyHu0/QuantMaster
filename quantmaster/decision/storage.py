"""每日选股快照持久化，保证事后研究能还原当时真正看到的信号。"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any

from quantmaster.config import get_config
from quantmaster.runtime.sqlite import connect_sqlite


class DecisionStore:
    def __init__(self, path: Path | None = None):
        self.path = Path(path) if path else get_config().data_root / "decisions.sqlite"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._migrate()

    def _conn(self) -> sqlite3.Connection:
        return connect_sqlite(self.path)

    def _migrate(self) -> None:
        with self._conn() as conn:
            exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='selection_snapshots'"
            ).fetchone()
            if not exists:
                conn.execute(
                    "CREATE TABLE selection_snapshots ("
                    "signal_date TEXT NOT NULL, universe TEXT NOT NULL, "
                    "horizon INTEGER NOT NULL, profile TEXT NOT NULL, "
                    "policy_hash TEXT NOT NULL, model_version TEXT NOT NULL, "
                    "payload TEXT NOT NULL, created_at REAL NOT NULL, "
                    "PRIMARY KEY(signal_date,universe,horizon,profile,policy_hash))"
                )
                return
            columns = {
                row[1] for row in conn.execute("PRAGMA table_info(selection_snapshots)")
            }
            if {"profile", "policy_hash"} <= columns:
                return
            conn.execute(
                "CREATE TABLE selection_snapshots_v2 ("
                "signal_date TEXT NOT NULL, universe TEXT NOT NULL, "
                "horizon INTEGER NOT NULL, profile TEXT NOT NULL, "
                "policy_hash TEXT NOT NULL, model_version TEXT NOT NULL, "
                "payload TEXT NOT NULL, created_at REAL NOT NULL, "
                "PRIMARY KEY(signal_date,universe,horizon,profile,policy_hash))"
            )
            conn.execute(
                "INSERT INTO selection_snapshots_v2 "
                "SELECT signal_date,universe,horizon,'legacy',model_version,"
                "model_version,payload,created_at FROM selection_snapshots"
            )
            conn.execute("DROP TABLE selection_snapshots")
            conn.execute("ALTER TABLE selection_snapshots_v2 RENAME TO selection_snapshots")

    def save(self, report: dict[str, Any], universe: str) -> None:
        """同日同模型重复运行覆盖旧快照，避免计划任务重跑产生重复记录。"""
        with self._conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO selection_snapshots "
                "(signal_date,universe,horizon,profile,policy_hash,model_version,payload,created_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (
                    report["signal_date"], universe, report["holding_horizon_days"],
                    report.get("profile", "legacy"),
                    report.get("policy_hash", report.get("model_version", "swing-v1")),
                    report.get("model_version", "swing-v1"),
                    json.dumps(report, ensure_ascii=False, allow_nan=False), time.time(),
                ),
            )

    def history(
        self, universe: str | None = None, limit: int = 30,
        profile: str | None = None,
    ) -> list[dict[str, Any]]:
        if limit < 1:
            return []
        filters, values = [], []
        if universe:
            filters.append("universe=?")
            values.append(universe)
        if profile:
            filters.append("profile=?")
            values.append(profile)
        query = "SELECT payload FROM selection_snapshots "
        if filters:
            query += "WHERE " + " AND ".join(filters) + " "
        query += "ORDER BY signal_date DESC, created_at DESC LIMIT ?"
        values.append(limit)
        with self._conn() as conn:
            rows = conn.execute(query, tuple(values)).fetchall()
        return [json.loads(row[0]) for row in rows]

    def latest(self, universe: str | None = None) -> dict[str, Any] | None:
        rows = self.history(universe=universe, limit=1)
        return rows[0] if rows else None
