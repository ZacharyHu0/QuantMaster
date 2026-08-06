from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from quantmaster.after_close.models import AfterCloseSnapshot, utc_now
from quantmaster.config import get_config
from quantmaster.research.contracts import content_hash
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.runtime.sqlite import connect_sqlite


class AfterCloseIntegrityError(RuntimeError):
    pass


class AfterCloseStore:
    def __init__(self, path: Path | None = None):
        self.path = path or get_config().data_root / "after_close.sqlite"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS snapshots (
                    snapshot_id TEXT PRIMARY KEY, as_of_date TEXT NOT NULL,
                    score_version TEXT NOT NULL, input_hash TEXT NOT NULL,
                    payload_json TEXT NOT NULL, payload_hash TEXT NOT NULL,
                    generated_at TEXT NOT NULL, published_at TEXT NOT NULL,
                    UNIQUE(as_of_date,score_version,input_hash));
                CREATE INDEX IF NOT EXISTS idx_after_close_asof
                    ON snapshots(as_of_date DESC,published_at DESC);
                CREATE TABLE IF NOT EXISTS attempts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, status TEXT NOT NULL,
                    as_of_date TEXT NOT NULL DEFAULT '', reasons_json TEXT NOT NULL DEFAULT '[]',
                    coverage_json TEXT NOT NULL DEFAULT '{}', snapshot_id TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS labels (
                    snapshot_id TEXT NOT NULL, horizon INTEGER NOT NULL,
                    payload_json TEXT NOT NULL, calculated_at TEXT NOT NULL,
                    PRIMARY KEY(snapshot_id,horizon));
            """)

    def _conn(self) -> sqlite3.Connection:
        return connect_sqlite(self.path, timeout=20.0, row_factory=True)

    def publish(self, snapshot: AfterCloseSnapshot) -> dict[str, Any]:
        payload = snapshot.to_dict()
        encoded = strict_json_dumps(payload, sort_keys=True)
        digest = content_hash(payload)
        now = utc_now()
        with self._conn() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT payload_hash FROM snapshots WHERE snapshot_id=?",
                (snapshot.snapshot_id,),
            ).fetchone()
            if existing and str(existing["payload_hash"]) != digest:
                raise AfterCloseIntegrityError("盘后快照 ID 已存在但内容不同")
            connection.execute(
                "INSERT OR IGNORE INTO snapshots "
                "(snapshot_id,as_of_date,score_version,input_hash,payload_json,payload_hash,"
                "generated_at,published_at) VALUES (?,?,?,?,?,?,?,?)",
                (snapshot.snapshot_id, snapshot.as_of_date, snapshot.score_version,
                 snapshot.input_hash, encoded, digest, snapshot.generated_at, now),
            )
            connection.execute(
                "INSERT INTO attempts(status,as_of_date,reasons_json,coverage_json,snapshot_id,created_at) "
                "VALUES ('published',?, '[]', ?, ?, ?)",
                (snapshot.as_of_date, strict_json_dumps(snapshot.coverage), snapshot.snapshot_id, now),
            )
        return {"snapshot_id": snapshot.snapshot_id, "payload_hash": digest, "published_at": now}

    def record_failure(
        self, reasons: list[str], *, as_of_date: str = "", coverage: dict | None = None,
    ) -> None:
        with self._conn() as connection:
            connection.execute(
                "INSERT INTO attempts(status,as_of_date,reasons_json,coverage_json,created_at) "
                "VALUES ('rejected',?,?,?,?)",
                (as_of_date, strict_json_dumps(reasons),
                 strict_json_dumps(coverage or {}), utc_now()),
            )

    @staticmethod
    def _decode(row: sqlite3.Row | None) -> AfterCloseSnapshot | None:
        if row is None:
            return None
        payload = json.loads(str(row["payload_json"]))
        if content_hash(payload) != str(row["payload_hash"]):
            raise AfterCloseIntegrityError("盘后快照内容哈希校验失败")
        return AfterCloseSnapshot.from_dict(payload)

    def get(self, snapshot_id: str) -> AfterCloseSnapshot | None:
        with self._conn() as connection:
            row = connection.execute(
                "SELECT * FROM snapshots WHERE snapshot_id=?", (snapshot_id,),
            ).fetchone()
        return self._decode(row)

    def latest(self) -> AfterCloseSnapshot | None:
        with self._conn() as connection:
            row = connection.execute(
                "SELECT * FROM snapshots ORDER BY as_of_date DESC,published_at DESC LIMIT 1"
            ).fetchone()
        return self._decode(row)

    def for_date(self, as_of_date: str) -> AfterCloseSnapshot | None:
        with self._conn() as connection:
            row = connection.execute(
                "SELECT * FROM snapshots WHERE as_of_date=? "
                "ORDER BY published_at DESC LIMIT 1", (as_of_date,),
            ).fetchone()
        return self._decode(row)

    def public_latest(self) -> dict[str, Any] | None:
        snapshot = self.latest()
        if snapshot is None:
            return None
        value = snapshot.to_dict()
        with self._conn() as connection:
            attempt = connection.execute(
                "SELECT * FROM attempts ORDER BY id DESC LIMIT 1"
            ).fetchone()
        if attempt and str(attempt["status"]) == "rejected":
            value["staleness"] = {
                "stale": True,
                "reason": "；".join(json.loads(str(attempt["reasons_json"])))[:1000],
                "last_attempt_at": str(attempt["created_at"]),
                "attempted_as_of": str(attempt["as_of_date"]),
            }
            for candidate in value.get("candidates", []):
                candidate["staleness"] = dict(value["staleness"])
            for sector in value.get("sectors", []):
                sector["staleness"] = dict(value["staleness"])
        return value

    def history(self, limit: int = 30) -> list[dict[str, Any]]:
        with self._conn() as connection:
            rows = connection.execute(
                "SELECT snapshot_id,as_of_date,score_version,input_hash,payload_hash,generated_at,"
                "published_at FROM snapshots ORDER BY as_of_date DESC,published_at DESC LIMIT ?",
                (max(1, min(500, int(limit))),),
            ).fetchall()
        return [dict(row) for row in rows]

    def save_labels(self, snapshot_id: str, horizon: int, payload: dict[str, Any]) -> None:
        with self._conn() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO labels(snapshot_id,horizon,payload_json,calculated_at) "
                "VALUES (?,?,?,?)",
                (snapshot_id, int(horizon), strict_json_dumps(payload), utc_now()),
            )

    def labels(self, snapshot_id: str) -> list[dict[str, Any]]:
        with self._conn() as connection:
            rows = connection.execute(
                "SELECT horizon,payload_json,calculated_at FROM labels "
                "WHERE snapshot_id=? ORDER BY horizon", (snapshot_id,),
            ).fetchall()
        return [
            {"horizon": int(row["horizon"]), "calculated_at": row["calculated_at"],
             **json.loads(str(row["payload_json"]))}
            for row in rows
        ]

    def health(self, limit: int = 100) -> dict[str, Any]:
        snapshots = self.history(limit)
        rows: list[dict[str, Any]] = []
        for meta in snapshots:
            snapshot = self.get(str(meta["snapshot_id"]))
            if snapshot is None:
                continue
            for label in self.labels(snapshot.snapshot_id):
                rows.append({
                    "snapshot_id": snapshot.snapshot_id,
                    "as_of_date": snapshot.as_of_date,
                    "score_version": snapshot.score_version,
                    "market_regime": snapshot.validation.get("market_regime", "unknown"),
                    **label,
                })
        grouped: dict[str, dict[str, Any]] = {}
        for row in rows:
            key = f'{row["score_version"]}:{row["horizon"]}'
            bucket = grouped.setdefault(key, {
                "score_version": row["score_version"], "horizon": row["horizon"],
                "observations": 0, "mean_return": [], "excess_return": [],
                "hit_rate": [], "mean_max_drawdown": [],
            })
            bucket["observations"] += 1
            for source, target in (
                ("mean_return", "mean_return"),
                ("excess_mean_return", "excess_return"),
                ("hit_rate", "hit_rate"),
                ("mean_max_drawdown", "mean_max_drawdown"),
            ):
                if row.get(source) is not None:
                    bucket[target].append(float(row[source]))
        summaries = []
        for bucket in grouped.values():
            summaries.append({
                "score_version": bucket["score_version"],
                "horizon": bucket["horizon"],
                "observations": bucket["observations"],
                "mean_return": (
                    sum(bucket["mean_return"]) / len(bucket["mean_return"])
                    if bucket["mean_return"] else None
                ),
                "excess_return": (
                    sum(bucket["excess_return"]) / len(bucket["excess_return"])
                    if bucket["excess_return"] else None
                ),
                "hit_rate": (
                    sum(bucket["hit_rate"]) / len(bucket["hit_rate"])
                    if bucket["hit_rate"] else None
                ),
                "mean_max_drawdown": (
                    sum(bucket["mean_max_drawdown"]) / len(bucket["mean_max_drawdown"])
                    if bucket["mean_max_drawdown"] else None
                ),
            })
        latest = self.public_latest()
        latest_value = latest or {}
        coverage = latest_value.get("coverage", {})
        issues = list(coverage.get("issues") or [])
        if latest_value.get("staleness", {}).get("stale"):
            issues.append(str(
                latest_value["staleness"].get("reason") or "latest attempt rejected"
            ))
        return {
            "status": "observation" if issues or not rows else "validated_observation",
            "candidate_promotion_allowed": False,
            "reason": "研究健康度仅作观察，不等同于交易建议",
            "coverage": coverage,
            "summaries": sorted(summaries, key=lambda item: (item["score_version"], item["horizon"])),
        }
