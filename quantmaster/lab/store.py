"""Quant Lab 的 SQLite 研究账本与可恢复任务队列。"""

from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path
from typing import Any

from quantmaster.config import get_config
from quantmaster.lab.models import (
    FACTOR_STATUSES,
    FactorSpec,
    canonical_json,
    content_hash,
    utc_now,
)


class LabStore:
    def __init__(self, path: str | Path | None = None):
        self.path = Path(path) if path else get_config().data_root / "lab.sqlite"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._migrate()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _migrate(self) -> None:
        with self._conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS factor_definitions (
                    id TEXT PRIMARY KEY, slug TEXT NOT NULL UNIQUE, name TEXT NOT NULL,
                    kind TEXT NOT NULL, category TEXT NOT NULL, created_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS factor_versions (
                    id TEXT PRIMARY KEY, factor_id TEXT NOT NULL, version INTEGER NOT NULL,
                    parent_id TEXT NOT NULL DEFAULT '', content_hash TEXT NOT NULL,
                    spec_json TEXT NOT NULL, status TEXT NOT NULL, source TEXT NOT NULL,
                    created_by TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                    UNIQUE(factor_id,version), UNIQUE(factor_id,content_hash),
                    FOREIGN KEY(factor_id) REFERENCES factor_definitions(id));
                CREATE TABLE IF NOT EXISTS validation_reports (
                    id TEXT PRIMARY KEY, version_id TEXT NOT NULL, dataset_hash TEXT NOT NULL,
                    report_json TEXT NOT NULL, created_at TEXT NOT NULL,
                    FOREIGN KEY(version_id) REFERENCES factor_versions(id));
                CREATE TABLE IF NOT EXISTS approvals (
                    id TEXT PRIMARY KEY, version_id TEXT NOT NULL, action TEXT NOT NULL,
                    actor TEXT NOT NULL, reason TEXT NOT NULL, created_at TEXT NOT NULL,
                    FOREIGN KEY(version_id) REFERENCES factor_versions(id));
                CREATE TABLE IF NOT EXISTS deployments (
                    id TEXT PRIMARY KEY, universe TEXT NOT NULL, horizon INTEGER NOT NULL,
                    role TEXT NOT NULL DEFAULT 'factor',
                    profile TEXT NOT NULL DEFAULT 'all',
                    scope TEXT NOT NULL DEFAULT 'exact',
                    version_id TEXT NOT NULL, status TEXT NOT NULL,
                    created_at TEXT NOT NULL, retired_at TEXT NOT NULL DEFAULT '',
                    FOREIGN KEY(version_id) REFERENCES factor_versions(id));
                CREATE TABLE IF NOT EXISTS dataset_snapshots (
                    id TEXT PRIMARY KEY, snapshot_hash TEXT NOT NULL UNIQUE,
                    payload_json TEXT NOT NULL, created_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS experiments (
                    id TEXT PRIMARY KEY, name TEXT NOT NULL, method TEXT NOT NULL,
                    dataset_id TEXT NOT NULL DEFAULT '', status TEXT NOT NULL,
                    config_json TEXT NOT NULL, result_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS lab_jobs (
                    id TEXT PRIMARY KEY, kind TEXT NOT NULL, status TEXT NOT NULL,
                    params_json TEXT NOT NULL, result_json TEXT NOT NULL DEFAULT '{}',
                    progress INTEGER NOT NULL DEFAULT 0, phase TEXT NOT NULL DEFAULT '',
                    detail TEXT NOT NULL DEFAULT '', error TEXT NOT NULL DEFAULT '',
                    cancel_requested INTEGER NOT NULL DEFAULT 0, worker TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL, started_at TEXT NOT NULL DEFAULT '',
                    heartbeat_at TEXT NOT NULL DEFAULT '', finished_at TEXT NOT NULL DEFAULT '');
                CREATE TABLE IF NOT EXISTS lab_job_events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL,
                    event_json TEXT NOT NULL, created_at TEXT NOT NULL,
                    FOREIGN KEY(job_id) REFERENCES lab_jobs(id));
                CREATE TABLE IF NOT EXISTS copilot_suggestions (
                    id TEXT PRIMARY KEY, version_id TEXT NOT NULL, base_hash TEXT NOT NULL,
                    payload_json TEXT NOT NULL, outbound_hash TEXT NOT NULL,
                    status TEXT NOT NULL, created_at TEXT NOT NULL,
                    FOREIGN KEY(version_id) REFERENCES factor_versions(id));
                CREATE TABLE IF NOT EXISTS lab_schedule_slots (
                    slot TEXT PRIMARY KEY, created_at TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS idx_factor_versions_status
                    ON factor_versions(status,updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_lab_jobs_status
                    ON lab_jobs(status,created_at);
                CREATE INDEX IF NOT EXISTS idx_job_events
                    ON lab_job_events(job_id,seq);
            """)
            deployment_columns = {
                row[1] for row in conn.execute("PRAGMA table_info(deployments)")
            }
            for name, declaration in (
                ("role", "TEXT NOT NULL DEFAULT 'factor'"),
                ("profile", "TEXT NOT NULL DEFAULT 'all'"),
                ("scope", "TEXT NOT NULL DEFAULT 'exact'"),
            ):
                if name not in deployment_columns:
                    conn.execute(f"ALTER TABLE deployments ADD COLUMN {name} {declaration}")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_deployments_runtime "
                "ON deployments(status,universe,horizon,profile,role)"
            )
            conn.execute("PRAGMA user_version=2")

    @staticmethod
    def _decode(row: sqlite3.Row | None, json_fields: tuple[str, ...] = ()) -> dict | None:
        if row is None:
            return None
        value = dict(row)
        for field in json_fields:
            value[field] = json.loads(value.get(field) or "{}")
        if "cancel_requested" in value:
            value["cancel_requested"] = bool(value["cancel_requested"])
        return value

    def sync_catalog(self, specs: list[FactorSpec]) -> int:
        created = 0
        for spec in specs:
            _factor, version, was_created = self.create_factor(
                spec, status="draft", source="builtin", actor="system")
            # 早期预览版曾把内置目录直接标成 approved；只有从未验证、审批或部署的
            # 系统版本才安全回退为草稿，已有人工证据不被覆盖。
            with self._conn() as conn:
                conn.execute(
                    "UPDATE factor_versions SET status='draft',updated_at=? WHERE id=? "
                    "AND source='builtin' AND status='approved' "
                    "AND NOT EXISTS(SELECT 1 FROM validation_reports r WHERE r.version_id=?) "
                    "AND NOT EXISTS(SELECT 1 FROM approvals a WHERE a.version_id=?) "
                    "AND NOT EXISTS(SELECT 1 FROM deployments d WHERE d.version_id=?)",
                    (utc_now(), version["id"], version["id"], version["id"], version["id"]),
                )
            created += int(was_created and version["version"] == 1)
        return created

    def create_factor(
        self,
        spec: FactorSpec,
        *,
        status: str = "draft",
        source: str = "manual",
        actor: str = "web",
        parent_id: str = "",
    ) -> tuple[dict, dict, bool]:
        if status not in FACTOR_STATUSES:
            raise ValueError(f"未知因子状态: {status}")
        payload = spec.to_dict()
        digest = content_hash(payload)
        now = utc_now()
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM factor_definitions WHERE slug=?", (spec.slug,)).fetchone()
            if row is None:
                factor_id = uuid.uuid4().hex
                conn.execute(
                    "INSERT INTO factor_definitions VALUES (?,?,?,?,?,?)",
                    (factor_id, spec.slug, spec.name, spec.kind, spec.category, now),
                )
                row = conn.execute(
                    "SELECT * FROM factor_definitions WHERE id=?", (factor_id,)).fetchone()
            factor = dict(row)
            existing = conn.execute(
                "SELECT * FROM factor_versions WHERE factor_id=? AND content_hash=?",
                (factor["id"], digest),
            ).fetchone()
            if existing is not None:
                return factor, self._decode(existing, ("spec_json",)) or {}, False
            next_version = conn.execute(
                "SELECT COALESCE(MAX(version),0)+1 FROM factor_versions WHERE factor_id=?",
                (factor["id"],),
            ).fetchone()[0]
            version_id = uuid.uuid4().hex
            conn.execute(
                "INSERT INTO factor_versions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    version_id, factor["id"], next_version, parent_id, digest,
                    canonical_json(payload), status, source, actor, now, now,
                ),
            )
            version = conn.execute(
                "SELECT * FROM factor_versions WHERE id=?", (version_id,)).fetchone()
        return factor, self._decode(version, ("spec_json",)) or {}, True

    def list_factors(
        self, *, status: str | None = None, category: str | None = None,
        search: str = "", limit: int = 100, offset: int = 0,
    ) -> dict[str, Any]:
        clauses, params = [], []
        if status:
            clauses.append("v.status=?")
            params.append(status)
        if category:
            clauses.append("d.category=?")
            params.append(category)
        if search:
            clauses.append("(d.name LIKE ? OR d.slug LIKE ?)")
            params.extend([f"%{search}%", f"%{search}%"])
        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        base = (
            "FROM factor_definitions d JOIN factor_versions v ON v.factor_id=d.id "
            "AND v.version=(SELECT MAX(v2.version) FROM factor_versions v2 WHERE v2.factor_id=d.id) "
        )
        with self._conn() as conn:
            total = conn.execute(f"SELECT COUNT(*) {base}{where}", params).fetchone()[0]
            rows = conn.execute(
                "SELECT d.*,v.id AS version_id,v.version,v.status,v.source,v.spec_json,v.updated_at "
                f"{base}{where} ORDER BY v.updated_at DESC,d.slug LIMIT ? OFFSET ?",
                (*params, max(1, min(limit, 500)), max(0, offset)),
            ).fetchall()
        items = []
        for row in rows:
            value = self._decode(row, ("spec_json",)) or {}
            value["spec"] = value.pop("spec_json")
            items.append(value)
        return {"items": items, "total": total, "limit": limit, "offset": offset}

    def version(self, version_id: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT v.*,d.slug,d.name,d.kind,d.category FROM factor_versions v "
                "JOIN factor_definitions d ON d.id=v.factor_id WHERE v.id=?", (version_id,),
            ).fetchone()
            report = conn.execute(
                "SELECT report_json,created_at FROM validation_reports WHERE version_id=? "
                "ORDER BY created_at DESC LIMIT 1", (version_id,),
            ).fetchone()
        value = self._decode(row, ("spec_json",))
        if value is not None:
            value["spec"] = value.pop("spec_json")
            value["validation"] = json.loads(report[0]) if report else None
        return value

    def save_validation(self, version_id: str, dataset_hash: str, report: dict) -> dict:
        current = self.version(version_id)
        if current is None:
            raise KeyError("因子版本不存在")
        now, report_id = utc_now(), uuid.uuid4().hex
        hard_failures = report.get("gates", {}).get("hard_failures", [])
        if current["status"] == "production":
            next_status = "degraded" if hard_failures else "production"
        elif current["status"] in {"approved", "degraded"}:
            # A successful revalidation does not silently redeploy a degraded model;
            # it returns to approved and still needs an explicit production action.
            next_status = "degraded" if hard_failures else "approved"
        else:
            next_status = "draft" if hard_failures else "candidate"
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO validation_reports VALUES (?,?,?,?,?)",
                (report_id, version_id, dataset_hash, canonical_json(report), now),
            )
            conn.execute(
                "UPDATE factor_versions SET status=?,updated_at=? WHERE id=?",
                (next_status, now, version_id),
            )
        return self.version(version_id) or {}

    def approve(self, version_id: str, *, actor: str, reason: str = "") -> dict:
        value = self.version(version_id)
        if value is None:
            raise KeyError("因子版本不存在")
        report = value.get("validation") or {}
        if not report:
            raise ValueError("候选尚未完成统一验证，不能批准")
        gates = report.get("gates") or {}
        if gates.get("hard_failures"):
            raise ValueError("数据完整性或防泄漏硬门槛未通过，不能批准")
        if gates.get("soft_failures") and not reason.strip():
            raise ValueError("候选未通过全部软门槛；覆盖批准必须填写研究理由")
        now = utc_now()
        with self._conn() as conn:
            conn.execute(
                "UPDATE factor_versions SET status='approved',updated_at=? WHERE id=?",
                (now, version_id),
            )
            conn.execute(
                "INSERT INTO approvals VALUES (?,?,?,?,?,?)",
                (uuid.uuid4().hex, version_id, "approve", actor, reason.strip(), now),
            )
        return self.version(version_id) or {}

    def reject(self, version_id: str, *, actor: str, reason: str) -> dict:
        if not reason.strip():
            raise ValueError("驳回候选时需要填写理由")
        if self.version(version_id) is None:
            raise KeyError("因子版本不存在")
        now = utc_now()
        with self._conn() as conn:
            conn.execute(
                "UPDATE factor_versions SET status='archived',updated_at=? WHERE id=?",
                (now, version_id),
            )
            conn.execute(
                "INSERT INTO approvals VALUES (?,?,?,?,?,?)",
                (uuid.uuid4().hex, version_id, "reject", actor, reason.strip(), now),
            )
        return self.version(version_id) or {}

    def deploy(
        self, version_id: str, *, universe: str, horizon: int, actor: str,
        profile: str = "all", scope: str = "exact",
    ) -> dict:
        value = self.version(version_id)
        if value is None:
            raise KeyError("因子版本不存在")
        if value["status"] not in {"approved", "production", "degraded"}:
            raise ValueError("只有已批准版本可以设为生产 champion")
        if horizon not in {1, 3, 5, 7}:
            raise ValueError("horizon 只支持 1/3/5/7 日")
        if profile not in {"all", "risk_adjusted", "short_term", "stable"}:
            raise ValueError("profile 只支持 all/risk_adjusted/short_term/stable")
        if scope not in {"exact", "a_share"}:
            raise ValueError("scope 只支持 exact/a_share")
        report = value.get("validation") or {}
        gates = report.get("gates") or {}
        if gates.get("hard_failures"):
            raise ValueError("当前验证存在硬门槛失败，不能设为 champion")
        has_horizon_evidence = bool(report.get("horizons")) or report.get("best_horizon") is not None
        if (has_horizon_evidence and str(horizon) not in (report.get("horizons") or {})
                and report.get("best_horizon") != horizon):
            raise ValueError(f"版本没有 {horizon} 日验证证据，不能部署到该周期")
        spec = value.get("spec") or {}
        role = "ml" if spec.get("kind") == "learned" else "factor"
        if role == "ml" and not (spec.get("model") or {}).get("manifest"):
            raise ValueError("学习模型缺少可验证的推理工件")
        now, deployment_id = utc_now(), uuid.uuid4().hex
        with self._conn() as conn:
            evidence = conn.execute(
                "SELECT EXISTS(SELECT 1 FROM validation_reports WHERE version_id=?),"
                "EXISTS(SELECT 1 FROM approvals WHERE version_id=? AND action='approve')",
                (version_id, version_id),
            ).fetchone()
            if not evidence[0] or not evidence[1]:
                raise ValueError("缺少统一验证或人工批准记录，不能设为 champion")
            rows = conn.execute(
                "SELECT id,version_id FROM deployments WHERE universe=? AND horizon=? "
                "AND role=? AND profile=? AND scope=? AND status='active'",
                (universe, horizon, role, profile, scope),
            ).fetchall()
            for row in rows:
                conn.execute(
                    "UPDATE deployments SET status='retired',retired_at=? WHERE id=?",
                    (now, row["id"]),
                )
            conn.execute(
                "INSERT INTO deployments "
                "(id,universe,horizon,role,profile,scope,version_id,status,created_at,retired_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (deployment_id, universe, horizon, role, profile, scope,
                 version_id, "active", now, ""),
            )
            conn.execute(
                "UPDATE factor_versions SET status='production',updated_at=? WHERE id=?",
                (now, version_id),
            )
            conn.execute(
                "INSERT INTO approvals VALUES (?,?,?,?,?,?)",
                (uuid.uuid4().hex, version_id, "deploy", actor,
                 f"{universe}/{horizon}d/{profile}/{role}/{scope}", now),
            )
            for row in rows:
                still_active = conn.execute(
                    "SELECT EXISTS(SELECT 1 FROM deployments WHERE version_id=? AND status='active')",
                    (row["version_id"],),
                ).fetchone()[0]
                if not still_active:
                    conn.execute(
                        "UPDATE factor_versions SET status='approved',updated_at=? "
                        "WHERE id=? AND status='production'", (now, row["version_id"]),
                    )
        return {
            "deployment_id": deployment_id, "role": role, "profile": profile,
            "scope": scope, "version": self.version(version_id),
        }

    def save_snapshot(self, payload: dict) -> dict:
        digest = payload.get("snapshot_hash") or content_hash(payload)
        with self._conn() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO dataset_snapshots VALUES (?,?,?,?)",
                (uuid.uuid4().hex, digest, canonical_json(payload), utc_now()),
            )
            row = conn.execute(
                "SELECT * FROM dataset_snapshots WHERE snapshot_hash=?", (digest,),
            ).fetchone()
        value = self._decode(row, ("payload_json",)) or {}
        value["payload"] = value.pop("payload_json")
        return value

    def create_experiment(self, name: str, method: str, config: dict) -> dict:
        now, experiment_id = utc_now(), uuid.uuid4().hex
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO experiments VALUES (?,?,?,?,?,?,?,?,?)",
                (experiment_id, name, method, "", "queued", canonical_json(config), "{}", now, now),
            )
        return self.experiment(experiment_id) or {}

    def experiment(self, experiment_id: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM experiments WHERE id=?", (experiment_id,)).fetchone()
        return self._decode(row, ("config_json", "result_json"))

    def update_experiment(
        self, experiment_id: str, *, status: str, result: dict | None = None,
        dataset_id: str = "",
    ) -> dict:
        if self.experiment(experiment_id) is None:
            raise KeyError("实验不存在")
        with self._conn() as conn:
            conn.execute(
                "UPDATE experiments SET status=?,result_json=?,dataset_id=CASE "
                "WHEN ?='' THEN dataset_id ELSE ? END,updated_at=? WHERE id=?",
                (status, canonical_json(result or {}), dataset_id, dataset_id, utc_now(), experiment_id),
            )
        return self.experiment(experiment_id) or {}

    def list_experiments(self, limit: int = 50) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM experiments ORDER BY created_at DESC LIMIT ?",
                (max(1, min(limit, 500)),),
            ).fetchall()
        return [self._decode(row, ("config_json", "result_json")) or {} for row in rows]

    def enqueue(self, kind: str, params: dict) -> dict:
        job_id, now = uuid.uuid4().hex, utc_now()
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO lab_jobs "
                "(id,kind,status,params_json,created_at) VALUES (?,?,?,?,?)",
                (job_id, kind, "queued", canonical_json(params), now),
            )
        self.append_event(job_id, {"type": "queued", "progress": 0, "phase": "等待执行"})
        return self.job(job_id) or {}

    def claim_next(self, worker: str, *, allow_scheduled: bool = True) -> dict | None:
        now = utc_now()
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT id FROM lab_jobs WHERE status IN ('queued','interrupted') "
                "AND (? OR params_json NOT LIKE '%\"_scheduled\":true%') "
                "ORDER BY created_at LIMIT 1", (int(allow_scheduled),),
            ).fetchone()
            if row is None:
                return None
            changed = conn.execute(
                "UPDATE lab_jobs SET status='running',worker=?,started_at=CASE "
                "WHEN started_at='' THEN ? ELSE started_at END,heartbeat_at=? "
                "WHERE id=? AND status IN ('queued','interrupted')",
                (worker, now, now, row["id"]),
            ).rowcount
            if not changed:
                return None
        return self.job(row["id"])

    def reserve_schedule(self, slot: str) -> bool:
        """跨进程幂等地占用一个调度时隙，防止双 Worker 重复入队。"""
        with self._conn() as conn:
            changed = conn.execute(
                "INSERT OR IGNORE INTO lab_schedule_slots VALUES (?,?)",
                (slot, utc_now()),
            ).rowcount
        return bool(changed)

    def scheduled_usage_hours(self) -> float:
        """当前 UTC 自然日已消耗的自动研究计算小时数。"""
        with self._conn() as conn:
            value = conn.execute(
                "SELECT COALESCE(SUM(MAX(0,(julianday(CASE WHEN finished_at='' "
                "THEN 'now' ELSE finished_at END)-julianday(started_at))*24)),0) "
                "FROM lab_jobs WHERE started_at<>'' AND created_at>=date('now') "
                "AND params_json LIKE '%\"_scheduled\":true%'"
            ).fetchone()[0]
        return max(0.0, float(value or 0.0))

    def active_deployments(
        self, *, universe: str | None = None, horizon: int | None = None,
        profile: str | None = None, role: str | None = None,
    ) -> list[dict]:
        filters, params = ["status='active'"], []
        for column, value in (
            ("universe", universe), ("horizon", horizon),
            ("profile", profile), ("role", role),
        ):
            if value is not None:
                filters.append(f"{column}=?")
                params.append(value)
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM deployments WHERE " + " AND ".join(filters)
                + " ORDER BY created_at DESC",
                tuple(params),
            ).fetchall()
        return [dict(row) for row in rows]

    def append_event(self, job_id: str, event: dict) -> int:
        with self._conn() as conn:
            cursor = conn.execute(
                "INSERT INTO lab_job_events(job_id,event_json,created_at) VALUES (?,?,?)",
                (job_id, canonical_json(event), utc_now()),
            )
        return int(cursor.lastrowid)

    def update_job(self, job_id: str, progress: int, phase: str, detail: str = "") -> None:
        now = utc_now()
        progress = max(0, min(100, int(progress)))
        with self._conn() as conn:
            conn.execute(
                "UPDATE lab_jobs SET progress=?,phase=?,detail=?,heartbeat_at=? "
                "WHERE id=? AND status='running'", (progress, phase, detail, now, job_id),
            )
        self.append_event(job_id, {
            "type": "progress", "progress": progress, "phase": phase, "detail": detail,
        })

    def request_cancel(self, job_id: str) -> dict:
        with self._conn() as conn:
            changed = conn.execute(
                "UPDATE lab_jobs SET cancel_requested=1 WHERE id=? "
                "AND status IN ('queued','running','paused','interrupted')", (job_id,),
            ).rowcount
        if not changed and self.job(job_id) is None:
            raise KeyError("任务不存在")
        self.append_event(job_id, {"type": "cancel_requested", "phase": "正在安全停止"})
        return self.job(job_id) or {}

    def is_cancel_requested(self, job_id: str) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT cancel_requested FROM lab_jobs WHERE id=?", (job_id,),
            ).fetchone()
        return bool(row and row[0])

    def finish_job(self, job_id: str, *, result: dict | None = None, error: str = "") -> None:
        current = self.job(job_id)
        cancelled = bool(current and current["cancel_requested"])
        status = "cancelled" if cancelled else "failed" if error else "completed"
        progress = current["progress"] if cancelled or error else 100
        now = utc_now()
        with self._conn() as conn:
            conn.execute(
                "UPDATE lab_jobs SET status=?,progress=?,result_json=?,error=?,"
                "finished_at=?,heartbeat_at=? WHERE id=?",
                (status, progress, canonical_json(result or {}), error[:1000], now, now, job_id),
            )
        self.append_event(job_id, {
            "type": status, "progress": progress,
            "phase": "已取消" if cancelled else "执行失败" if error else "执行完成",
            "detail": error[:300],
        })

    def interrupt_stale(self, worker: str = "") -> int:
        with self._conn() as conn:
            if worker:
                cursor = conn.execute(
                    "UPDATE lab_jobs SET status='interrupted',worker='' "
                    "WHERE status='running' AND worker=?", (worker,),
                )
            else:
                cursor = conn.execute(
                    "UPDATE lab_jobs SET status='interrupted',worker='' WHERE status='running'"
                )
        return cursor.rowcount

    def job(self, job_id: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM lab_jobs WHERE id=?", (job_id,)).fetchone()
        value = self._decode(row, ("params_json", "result_json"))
        if value is not None:
            value["params"] = value.pop("params_json")
            value["result"] = value.pop("result_json")
        return value

    def jobs(self, limit: int = 50) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM lab_jobs ORDER BY created_at DESC LIMIT ?",
                (max(1, min(limit, 500)),),
            ).fetchall()
        result = []
        for row in rows:
            value = self._decode(row, ("params_json", "result_json")) or {}
            value["params"] = value.pop("params_json")
            value["result"] = value.pop("result_json")
            result.append(value)
        return result

    def events(self, job_id: str, after: int = 0, limit: int = 500) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT seq,event_json,created_at FROM lab_job_events "
                "WHERE job_id=? AND seq>? ORDER BY seq LIMIT ?",
                (job_id, max(0, after), max(1, min(limit, 2000))),
            ).fetchall()
        return [
            {"seq": row["seq"], "created_at": row["created_at"], **json.loads(row["event_json"])}
            for row in rows
        ]

    def save_suggestion(
        self, version_id: str, base_hash: str, payload: dict, outbound: dict,
    ) -> dict:
        suggestion_id = uuid.uuid4().hex
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO copilot_suggestions VALUES (?,?,?,?,?,?,?)",
                (
                    suggestion_id, version_id, base_hash, canonical_json(payload),
                    content_hash(outbound), "pending", utc_now(),
                ),
            )
        return {"id": suggestion_id, "version_id": version_id, "base_hash": base_hash, **payload}

    def suggestion(self, suggestion_id: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM copilot_suggestions WHERE id=?", (suggestion_id,),
            ).fetchone()
        value = self._decode(row, ("payload_json",))
        if value is not None:
            value["payload"] = value.pop("payload_json")
        return value

    def resolve_suggestion(self, suggestion_id: str, status: str) -> dict:
        if status not in {"accepted", "dismissed"}:
            raise ValueError("建议状态只允许 accepted/dismissed")
        with self._conn() as conn:
            changed = conn.execute(
                "UPDATE copilot_suggestions SET status=? WHERE id=? AND status='pending'",
                (status, suggestion_id),
            ).rowcount
        if not changed:
            raise ValueError("建议不存在或已处理")
        return self.suggestion(suggestion_id) or {}

    def overview(self) -> dict:
        with self._conn() as conn:
            statuses = {
                row["status"]: row["count"] for row in conn.execute(
                    "SELECT status,COUNT(*) AS count FROM factor_versions GROUP BY status"
                )
            }
            running = conn.execute(
                "SELECT COUNT(*) FROM lab_jobs WHERE status IN ('queued','running','paused','interrupted')"
            ).fetchone()[0]
            experiments = conn.execute("SELECT COUNT(*) FROM experiments").fetchone()[0]
            deployments = conn.execute(
                "SELECT COUNT(*) FROM deployments WHERE status='active'"
            ).fetchone()[0]
        return {
            "factor_statuses": statuses,
            "active_jobs": running,
            "experiments": experiments,
            "deployments": deployments,
        }
