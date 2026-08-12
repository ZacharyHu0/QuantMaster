from __future__ import annotations

import json
import math
import sqlite3
from pathlib import Path
from typing import Any

from quantmaster.after_close.models import (
    SCORE_VERSION,
    SHADOW_SCORE_VERSION,
    AfterCloseSnapshot,
    utc_now,
)
from quantmaster.config import get_config
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.runtime.sqlite import connect_sqlite


class AfterCloseIntegrityError(RuntimeError):
    pass


class AfterCloseStore:
    def __init__(self, path: Path | None = None, *, read_only: bool = False):
        self.path = path or get_config().data_root / "after_close.sqlite"
        self.read_only = bool(read_only)
        if self.read_only:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS snapshots (
                    snapshot_id TEXT PRIMARY KEY, as_of_date TEXT NOT NULL,
                    score_version TEXT NOT NULL, input_hash TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
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
                CREATE TABLE IF NOT EXISTS score_control (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    active_version TEXT NOT NULL, previous_version TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL);
            """)

    def _conn(self) -> sqlite3.Connection:
        return connect_sqlite(
            self.path,
            timeout=0.25 if self.read_only else 20.0,
            row_factory=True,
            read_only=self.read_only,
        )

    def _require_writer(self) -> None:
        if self.read_only:
            raise RuntimeError("盘后快照只读视图不能写入")

    def publish(self, snapshot: AfterCloseSnapshot) -> dict[str, Any]:
        self._require_writer()
        payload = snapshot.to_dict()
        encoded = strict_json_dumps(payload, sort_keys=True)
        now = utc_now()
        with self._conn() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT payload_json FROM snapshots WHERE snapshot_id=?",
                (snapshot.snapshot_id,),
            ).fetchone()
            if existing and str(existing["payload_json"]) != encoded:
                raise AfterCloseIntegrityError("盘后快照 ID 已存在但内容不同")
            connection.execute(
                "INSERT OR IGNORE INTO snapshots "
                "(snapshot_id,as_of_date,score_version,input_hash,payload_json,"
                "generated_at,published_at) VALUES (?,?,?,?,?,?,?)",
                (
                    snapshot.snapshot_id,
                    snapshot.as_of_date,
                    snapshot.score_version,
                    snapshot.input_hash,
                    encoded,
                    snapshot.generated_at,
                    now,
                ),
            )
            connection.execute(
                "INSERT INTO attempts(status,as_of_date,reasons_json,coverage_json,snapshot_id,created_at) "
                "VALUES ('published',?, '[]', ?, ?, ?)",
                (snapshot.as_of_date, strict_json_dumps(snapshot.coverage), snapshot.snapshot_id, now),
            )
        return {"snapshot_id": snapshot.snapshot_id, "published_at": now}

    def record_failure(
        self,
        reasons: list[str],
        *,
        as_of_date: str = "",
        coverage: dict | None = None,
    ) -> None:
        self._require_writer()
        with self._conn() as connection:
            connection.execute(
                "INSERT INTO attempts(status,as_of_date,reasons_json,coverage_json,created_at) "
                "VALUES ('rejected',?,?,?,?)",
                (as_of_date, strict_json_dumps(reasons), strict_json_dumps(coverage or {}), utc_now()),
            )

    @staticmethod
    def _decode(row: sqlite3.Row | None) -> AfterCloseSnapshot | None:
        if row is None:
            return None
        payload = json.loads(str(row["payload_json"]))
        return AfterCloseSnapshot.from_dict(payload)

    def get(self, snapshot_id: str) -> AfterCloseSnapshot | None:
        with self._conn() as connection:
            row = connection.execute(
                "SELECT * FROM snapshots WHERE snapshot_id=?",
                (snapshot_id,),
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
                "SELECT * FROM snapshots WHERE as_of_date=? ORDER BY published_at DESC LIMIT 1",
                (as_of_date,),
            ).fetchone()
        return self._decode(row)

    def public_latest(self) -> dict[str, Any] | None:
        snapshot = self.latest()
        if snapshot is None:
            return None
        value = snapshot.to_dict()
        with self._conn() as connection:
            attempt = connection.execute("SELECT * FROM attempts ORDER BY id DESC LIMIT 1").fetchone()
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
                "SELECT snapshot_id,as_of_date,score_version,input_hash,generated_at,"
                "published_at FROM snapshots ORDER BY as_of_date DESC,published_at DESC LIMIT ?",
                (max(1, min(500, int(limit))),),
            ).fetchall()
        return [dict(row) for row in rows]

    def save_labels(self, snapshot_id: str, horizon: int, payload: dict[str, Any]) -> None:
        self._require_writer()
        with self._conn() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO labels(snapshot_id,horizon,payload_json,calculated_at) "
                "VALUES (?,?,?,?)",
                (snapshot_id, int(horizon), strict_json_dumps(payload), utc_now()),
            )

    def labels(self, snapshot_id: str) -> list[dict[str, Any]]:
        with self._conn() as connection:
            rows = connection.execute(
                "SELECT horizon,payload_json,calculated_at FROM labels WHERE snapshot_id=? ORDER BY horizon",
                (snapshot_id,),
            ).fetchall()
        return [
            {
                "horizon": int(row["horizon"]),
                "calculated_at": row["calculated_at"],
                **json.loads(str(row["payload_json"])),
            }
            for row in rows
        ]

    def active_score_version(self) -> str:
        with self._conn() as connection:
            row = connection.execute("SELECT active_version FROM score_control WHERE singleton=1").fetchone()
        value = str(row["active_version"]) if row else SCORE_VERSION
        return value if value in {SCORE_VERSION, SHADOW_SCORE_VERSION} else SCORE_VERSION

    def set_active_score_version(self, version: str) -> dict[str, Any]:
        self._require_writer()
        if version not in {SCORE_VERSION, SHADOW_SCORE_VERSION}:
            raise ValueError(f"未知盘后评分版本: {version}")
        current = self.active_score_version()
        with self._conn() as connection:
            connection.execute(
                "INSERT INTO score_control(singleton,active_version,previous_version,updated_at) "
                "VALUES (1,?,?,?) ON CONFLICT(singleton) DO UPDATE SET "
                "active_version=excluded.active_version,previous_version=excluded.previous_version,"
                "updated_at=excluded.updated_at",
                (version, current, utc_now()),
            )
        return {"active_version": version, "previous_version": current}

    @staticmethod
    def _psi(actual: list[float], expected: list[float]) -> float | None:
        if len(actual) < 20 or len(expected) < 20:
            return None
        ordered = sorted(expected)
        boundaries = [
            ordered[min(len(ordered) - 1, int(len(ordered) * index / 10))] for index in range(1, 10)
        ]

        def bucket(values: list[float]) -> list[float]:
            counts = [0] * 10
            for value in values:
                index = sum(value > boundary for boundary in boundaries)
                counts[index] += 1
            total = len(values)
            return [max(count / total, 1e-6) for count in counts]

        actual_bins, expected_bins = bucket(actual), bucket(expected)
        return sum(
            (left - right) * math.log(left / right)
            for left, right in zip(actual_bins, expected_bins, strict=True)
        )

    def _health_rows(
        self, limit: int,
    ) -> tuple[list[dict[str, Any]], list[AfterCloseSnapshot]]:
        snapshots = self.history(limit)
        rows: list[dict[str, Any]] = []
        loaded: list[AfterCloseSnapshot] = []
        for meta in snapshots:
            snapshot = self.get(str(meta["snapshot_id"]))
            if snapshot is None:
                continue
            loaded.append(snapshot)
            for label in self.labels(snapshot.snapshot_id):
                versions = label.get("score_versions") or {snapshot.score_version: label}
                for score_version, metrics in versions.items():
                    if not isinstance(metrics, dict):
                        continue
                    common = {
                        "snapshot_id": snapshot.snapshot_id,
                        "as_of_date": snapshot.as_of_date,
                        "score_version": score_version,
                        "market_regime": snapshot.validation.get("market_regime", "unknown"),
                        "primary_l1": "all",
                        "filter_signature": strict_json_dumps(snapshot.filters, sort_keys=True),
                        "coverage_anomaly": bool(
                            snapshot.coverage.get("issues")
                            or snapshot.coverage.get("status") not in {None, "complete"}
                        ),
                        "horizon": label["horizon"],
                    }
                    rows.append({**common, **metrics})
                    for primary_l1, sector_metrics in (metrics.get("sector_groups") or {}).items():
                        rows.append(
                            {
                                **common,
                                **sector_metrics,
                                "primary_l1": str(primary_l1),
                            }
                        )
        return rows, loaded

    @staticmethod
    def _group_health_rows(
        rows: list[dict[str, Any]],
    ) -> dict[tuple[str, int, str, str, str], dict[str, Any]]:
        grouped: dict[tuple[str, int, str, str, str], dict[str, Any]] = {}
        for row in rows:
            key = (
                str(row["score_version"]),
                int(row["horizon"]),
                str(row["market_regime"]),
                str(row["primary_l1"]),
                str(row["filter_signature"]),
            )
            bucket = grouped.setdefault(
                key,
                {
                    "score_version": row["score_version"],
                    "horizon": row["horizon"],
                    "market_regime": row["market_regime"],
                    "primary_l1": row["primary_l1"],
                    "filter_signature": row["filter_signature"],
                    "observations": 0,
                    "mean_return": [],
                    "excess_return": [],
                    "excess_vs_csi800": [],
                    "hit_rate": [],
                    "mean_max_drawdown": [],
                    "candidate_turnover": [],
                    "capacity_avg_amount_20d": [],
                    "sector_concentration": [],
                },
            )
            bucket["observations"] += 1
            for source, target in (
                ("mean_return", "mean_return"),
                ("excess_mean_return", "excess_return"),
                ("excess_vs_csi800", "excess_vs_csi800"),
                ("hit_rate", "hit_rate"),
                ("mean_max_drawdown", "mean_max_drawdown"),
                ("candidate_turnover", "candidate_turnover"),
                ("capacity_avg_amount_20d", "capacity_avg_amount_20d"),
                ("sector_concentration", "sector_concentration"),
            ):
                if row.get(source) is not None:
                    bucket[target].append(float(row[source]))
        return grouped

    @staticmethod
    def _health_summary(bucket: dict[str, Any]) -> dict[str, Any]:
        summary = {key: value for key, value in bucket.items() if not isinstance(value, list)}
        summary.update(
            {
                key: (sum(values) / len(values) if values else None)
                for key, values in bucket.items()
                if isinstance(values, list)
            }
        )
        summary["conclusion"] = "样本不足" if bucket["observations"] < 20 else "仅供研究观察"
        return summary

    @staticmethod
    def _historical_features(loaded: list[AfterCloseSnapshot]) -> dict[str, list[float]]:
        historical: dict[str, list[float]] = {}
        for snapshot in loaded[1:61]:
            for feature, values in (snapshot.validation.get("feature_distributions") or {}).items():
                historical.setdefault(str(feature), []).extend(
                    float(value) for value in values if value is not None
                )
        return historical

    @staticmethod
    def _feature_drift_status(psi: float | None) -> str:
        if psi is None:
            return "unavailable"
        if psi >= 0.25:
            return "degraded"
        if psi >= 0.10:
            return "warning"
        return "stable"

    @staticmethod
    def _overall_drift_status(severities: list[str]) -> str:
        for status in ("degraded", "warning", "stable"):
            if status in severities:
                return status
        return "insufficient"

    def _health_drift(self, loaded: list[AfterCloseSnapshot]) -> dict[str, Any]:
        drift: dict[str, Any] = {"status": "insufficient", "features": {}}
        if not loaded:
            return drift
        latest_distributions = loaded[0].validation.get("feature_distributions") or {}
        historical = self._historical_features(loaded)
        severities = []
        for feature in ("coverage", "returns", "amount", "turnover", "volatility", "float_mv"):
            actual = [
                float(value) for value in latest_distributions.get(feature, []) if value is not None
            ]
            psi = self._psi(actual, historical.get(feature, []))
            status = self._feature_drift_status(psi)
            drift["features"][feature] = {"psi": psi, "status": status}
            severities.append(status)
        drift["status"] = self._overall_drift_status(severities)
        return drift

    @staticmethod
    def _five_day_rows(rows: list[dict[str, Any]], version: str) -> list[dict[str, Any]]:
        return [
            row
            for row in rows
            if row["score_version"] == version
            and row["horizon"] == 5
            and row["primary_l1"] == "all"
        ]

    @staticmethod
    def _health_average(values: list[dict[str, Any]], field: str) -> float | None:
        numbers = [float(item[field]) for item in values if item.get(field) is not None]
        return sum(numbers) / len(numbers) if numbers else None

    @staticmethod
    def _not_degraded(left: float | None, right: float | None, tolerance: float) -> bool:
        return left is not None and right is not None and right >= left - tolerance

    def _promotion_checks(self, rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        five_day_v2 = self._five_day_rows(rows, SHADOW_SCORE_VERSION)
        five_day_v1 = self._five_day_rows(rows, SCORE_VERSION)
        v2_excess, v1_excess = (
            self._health_average(five_day_v2, "excess_mean_return"),
            self._health_average(five_day_v1, "excess_mean_return"),
        )
        v2_drawdown, v1_drawdown = (
            self._health_average(five_day_v2, "mean_max_drawdown"),
            self._health_average(five_day_v1, "mean_max_drawdown"),
        )
        anomaly_ratio = (
            sum(bool(item["coverage_anomaly"]) for item in five_day_v2) / len(five_day_v2)
            if five_day_v2
            else 1.0
        )
        promotion_checks = {
            "five_day_snapshots": {
                "value": len(five_day_v2),
                "required": 60,
                "passed": len(five_day_v2) >= 60,
            },
            "coverage_anomaly_ratio": {
                "value": anomaly_ratio,
                "maximum": 0.05,
                "passed": anomaly_ratio < 0.05,
            },
            "excess_not_degraded": {
                "v1": v1_excess,
                "v2": v2_excess,
                "passed": self._not_degraded(v1_excess, v2_excess, 0.002),
            },
            "drawdown_not_degraded": {
                "v1": v1_drawdown,
                "v2": v2_drawdown,
                "passed": self._not_degraded(v1_drawdown, v2_drawdown, 0.005),
            },
        }
        return promotion_checks

    def health(self, limit: int = 100) -> dict[str, Any]:
        rows, loaded = self._health_rows(limit)
        summaries = [
            self._health_summary(bucket)
            for bucket in self._group_health_rows(rows).values()
        ]
        drift = self._health_drift(loaded)
        promotion_checks = self._promotion_checks(rows)
        manual_review_eligible = all(item["passed"] for item in promotion_checks.values())
        latest = self.public_latest()
        latest_value = latest or {}
        coverage = latest_value.get("coverage", {})
        issues = list(coverage.get("issues") or [])
        if latest_value.get("staleness", {}).get("stale"):
            issues.append(str(latest_value["staleness"].get("reason") or "latest attempt rejected"))
        return {
            "status": (
                "degraded"
                if drift["status"] == "degraded"
                else "observation"
                if issues or not rows
                else "validated_observation"
            ),
            "candidate_promotion_allowed": False,
            "manual_review_eligible": manual_review_eligible,
            "active_score_version": self.active_score_version(),
            "promotion_checks": promotion_checks,
            "reason": "研究健康度仅作观察，不等同于交易建议",
            "coverage": coverage,
            "drift": drift,
            "summaries": sorted(
                summaries,
                key=lambda item: (
                    item["score_version"],
                    item["horizon"],
                    item["market_regime"],
                    item["primary_l1"],
                    item["filter_signature"],
                ),
            ),
        }
