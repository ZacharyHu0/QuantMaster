"""每日选股快照持久化，保证事后研究能还原当时真正看到的信号。"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any

from quantmaster.config import get_config


class DecisionStore:
    def __init__(self, path: Path | None = None):
        self.path = Path(path) if path else get_config().data_root / "decisions.sqlite"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS selection_snapshots ("
                "signal_date TEXT NOT NULL, universe TEXT NOT NULL, "
                "horizon INTEGER NOT NULL, model_version TEXT NOT NULL, "
                "payload TEXT NOT NULL, created_at REAL NOT NULL, "
                "PRIMARY KEY(signal_date, universe, horizon, model_version))"
            )

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def save(self, report: dict[str, Any], universe: str) -> None:
        """同日同模型重复运行覆盖旧快照，避免计划任务重跑产生重复记录。"""
        with self._conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO selection_snapshots VALUES (?,?,?,?,?,?)",
                (
                    report["signal_date"], universe, report["holding_horizon_days"],
                    report.get("model_version", "swing-v1"),
                    json.dumps(report, ensure_ascii=False, allow_nan=False), time.time(),
                ),
            )

    def history(self, universe: str | None = None, limit: int = 30) -> list[dict[str, Any]]:
        if limit < 1:
            return []
        query = (
            "SELECT payload FROM selection_snapshots "
            + ("WHERE universe=? " if universe else "")
            + "ORDER BY signal_date DESC, created_at DESC LIMIT ?"
        )
        params: tuple[Any, ...] = (universe, limit) if universe else (limit,)
        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
        return [json.loads(row[0]) for row in rows]

    def latest(self, universe: str | None = None) -> dict[str, Any] | None:
        rows = self.history(universe=universe, limit=1)
        return rows[0] if rows else None
