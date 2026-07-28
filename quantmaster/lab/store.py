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
                CREATE TABLE IF NOT EXISTS optimization_studies (
                    id TEXT PRIMARY KEY, job_id TEXT NOT NULL DEFAULT '',
                    experiment_id TEXT NOT NULL DEFAULT '', config_hash TEXT NOT NULL,
                    status TEXT NOT NULL, config_json TEXT NOT NULL,
                    result_json TEXT NOT NULL DEFAULT '{}', storage_url TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS bias_audits (
                    id TEXT PRIMARY KEY, version_id TEXT NOT NULL,
                    dataset_hash TEXT NOT NULL, status TEXT NOT NULL,
                    report_json TEXT NOT NULL, created_at TEXT NOT NULL,
                    FOREIGN KEY(version_id) REFERENCES factor_versions(id));
                CREATE TABLE IF NOT EXISTS mining_runs (
                    id TEXT PRIMARY KEY, job_id TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL, config_json TEXT NOT NULL,
                    split_json TEXT NOT NULL DEFAULT '{}', result_json TEXT NOT NULL DEFAULT '{}',
                    snapshot_hash TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS mining_candidates (
                    id TEXT PRIMARY KEY, run_id TEXT NOT NULL, candidate_key TEXT NOT NULL,
                    status TEXT NOT NULL, pareto_rank INTEGER,
                    proposal_json TEXT NOT NULL, metrics_json TEXT NOT NULL DEFAULT '{}',
                    artifact_json TEXT NOT NULL DEFAULT '{}', version_id TEXT NOT NULL DEFAULT '',
                    error TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL, UNIQUE(run_id,candidate_key),
                    FOREIGN KEY(run_id) REFERENCES mining_runs(id));
                CREATE INDEX IF NOT EXISTS idx_factor_versions_status
                    ON factor_versions(status,updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_lab_jobs_status
                    ON lab_jobs(status,created_at);
                CREATE INDEX IF NOT EXISTS idx_job_events
                    ON lab_job_events(job_id,seq);
                CREATE INDEX IF NOT EXISTS idx_studies_status
                    ON optimization_studies(status,updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_bias_audits_version
                    ON bias_audits(version_id,created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_mining_runs_updated
                    ON mining_runs(updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_mining_candidates_run
                    ON mining_candidates(run_id,pareto_rank,created_at);
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
            conn.execute("PRAGMA user_version=4")

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
        if gates.get("bias_audit_required"):
            audit = self.latest_bias_audit(version_id)
            if audit is None or audit.get("status") != "passed":
                raise ValueError("候选尚未通过防前视/PIT 偏差审计，不能设为 champion")
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

    def create_study(self, config: dict, *, storage_url: str = "") -> dict:
        study_id, now = uuid.uuid4().hex, utc_now()
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO optimization_studies "
                "(id,config_hash,status,config_json,storage_url,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (study_id, content_hash(config), "queued", canonical_json(config),
                 storage_url, now, now),
            )
        return self.study(study_id) or {}

    def study(self, study_id: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM optimization_studies WHERE id=?", (study_id,),
            ).fetchone()
        value = self._decode(row, ("config_json", "result_json"))
        if value is not None:
            value["config"] = value.pop("config_json")
            value["result"] = value.pop("result_json")
        return value

    def studies(self, limit: int = 50) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM optimization_studies ORDER BY updated_at DESC LIMIT ?",
                (max(1, min(limit, 500)),),
            ).fetchall()
        result = []
        for row in rows:
            value = self._decode(row, ("config_json", "result_json")) or {}
            value["config"] = value.pop("config_json")
            value["result"] = value.pop("result_json")
            result.append(value)
        return result

    def update_study(
        self, study_id: str, *, status: str | None = None,
        result: dict | None = None, job_id: str | None = None,
        experiment_id: str | None = None, storage_url: str | None = None,
    ) -> dict:
        if self.study(study_id) is None:
            raise KeyError("优化 Study 不存在")
        assignments, params = ["updated_at=?"], [utc_now()]
        for column, value in (
            ("status", status), ("job_id", job_id), ("experiment_id", experiment_id),
            ("storage_url", storage_url),
        ):
            if value is not None:
                assignments.append(f"{column}=?")
                params.append(value)
        if result is not None:
            assignments.append("result_json=?")
            params.append(canonical_json(result))
        params.append(study_id)
        with self._conn() as conn:
            conn.execute(
                f"UPDATE optimization_studies SET {','.join(assignments)} WHERE id=?",
                tuple(params),
            )
        return self.study(study_id) or {}

    def save_bias_audit(
        self, version_id: str, dataset_hash: str, report: dict,
    ) -> dict:
        if self.version(version_id) is None:
            raise KeyError("因子版本不存在")
        audit_id, now = uuid.uuid4().hex, utc_now()
        status = "passed" if report.get("passed") else "failed"
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO bias_audits VALUES (?,?,?,?,?,?)",
                (audit_id, version_id, dataset_hash, status, canonical_json(report), now),
            )
        return self.bias_audit(audit_id) or {}

    def bias_audit(self, audit_id: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM bias_audits WHERE id=?", (audit_id,)).fetchone()
        value = self._decode(row, ("report_json",))
        if value is not None:
            value["report"] = value.pop("report_json")
        return value

    def latest_bias_audit(self, version_id: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM bias_audits WHERE version_id=? ORDER BY created_at DESC LIMIT 1",
                (version_id,),
            ).fetchone()
        value = self._decode(row, ("report_json",))
        if value is not None:
            value["report"] = value.pop("report_json")
        return value

    def create_mining_run(self, config: dict) -> dict:
        run_id, now = uuid.uuid4().hex, utc_now()
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO mining_runs (id,status,config_json,created_at,updated_at) "
                "VALUES (?,?,?,?,?)",
                (run_id, "queued", canonical_json(config), now, now),
            )
        return self.mining_run(run_id) or {}

    def mining_run(self, run_id: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM mining_runs WHERE id=?", (run_id,)).fetchone()
        value = self._decode(row, ("config_json", "split_json", "result_json"))
        if value is not None:
            value["config"] = value.pop("config_json")
            value["split"] = value.pop("split_json")
            value["result"] = value.pop("result_json")
            value["candidates"] = self.mining_candidates(run_id)
        return value

    def mining_runs(self, limit: int = 30) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM mining_runs ORDER BY updated_at DESC LIMIT ?",
                (max(1, min(limit, 200)),),
            ).fetchall()
        result = []
        for row in rows:
            value = self._decode(row, ("config_json", "split_json", "result_json")) or {}
            value["config"] = value.pop("config_json")
            value["split"] = value.pop("split_json")
            value["result"] = value.pop("result_json")
            result.append(value)
        return result

    def update_mining_run(
        self, run_id: str, *, status: str | None = None, job_id: str | None = None,
        split: dict | None = None, result: dict | None = None,
        snapshot_hash: str | None = None,
    ) -> dict:
        assignments, params = ["updated_at=?"], [utc_now()]
        for column, value in (("status", status), ("job_id", job_id),
                              ("snapshot_hash", snapshot_hash)):
            if value is not None:
                assignments.append(f"{column}=?")
                params.append(value)
        for column, value in (("split_json", split), ("result_json", result)):
            if value is not None:
                assignments.append(f"{column}=?")
                params.append(canonical_json(value))
        params.append(run_id)
        with self._conn() as conn:
            changed = conn.execute(
                f"UPDATE mining_runs SET {','.join(assignments)} WHERE id=?", tuple(params),
            ).rowcount
        if not changed:
            raise KeyError("AutoMiner 运行不存在")
        return self.mining_run(run_id) or {}

    def save_mining_candidate(self, run_id: str, candidate: dict) -> dict:
        now = utc_now()
        key = str(candidate["id"])
        proposal = {name: candidate.get(name) for name in (
            "id", "name", "hypothesis", "objective", "required_features", "warmup",
            "parameters", "code", "selected_params", "audit",
        )}
        metrics = {name: candidate.get(name) or {} for name in (
            "train_metrics", "valid_metrics", "test_metrics",
        )}
        row_id = content_hash({"run_id": run_id, "candidate": key})[:32]
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO mining_candidates "
                "(id,run_id,candidate_key,status,pareto_rank,proposal_json,metrics_json,"
                "artifact_json,version_id,error,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(run_id,candidate_key) DO UPDATE SET "
                "status=excluded.status,pareto_rank=excluded.pareto_rank,"
                "proposal_json=excluded.proposal_json,metrics_json=excluded.metrics_json,"
                "artifact_json=excluded.artifact_json,version_id=excluded.version_id,"
                "error=excluded.error,updated_at=excluded.updated_at",
                (row_id, run_id, key, candidate.get("status", "proposed"),
                 candidate.get("pareto_rank"), canonical_json(proposal), canonical_json(metrics),
                 canonical_json(candidate.get("artifact") or {}),
                 candidate.get("factor_version_id", ""), candidate.get("error", ""), now, now),
            )
        return self.mining_candidate(row_id) or {}

    def mining_candidate(self, candidate_id: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM mining_candidates WHERE id=? OR candidate_key=? "
                "ORDER BY updated_at DESC LIMIT 1", (candidate_id, candidate_id),
            ).fetchone()
        value = self._decode(row, ("proposal_json", "metrics_json", "artifact_json"))
        if value is not None:
            value["proposal"] = value.pop("proposal_json")
            value["metrics"] = value.pop("metrics_json")
            value["artifact"] = value.pop("artifact_json")
        return value

    def mining_candidates(self, run_id: str) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM mining_candidates WHERE run_id=? "
                "ORDER BY CASE WHEN pareto_rank IS NULL THEN 999 ELSE pareto_rank END,created_at",
                (run_id,),
            ).fetchall()
        result = []
        for row in rows:
            value = self._decode(row, ("proposal_json", "metrics_json", "artifact_json")) or {}
            value["proposal"] = value.pop("proposal_json")
            value["metrics"] = value.pop("metrics_json")
            value["artifact"] = value.pop("artifact_json")
            result.append(value)
        return result

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

    def update_job(
        self,
        job_id: str,
        progress: int,
        phase: str,
        detail: str = "",
        *,
        event_type: str = "progress",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        now = utc_now()
        progress = max(0, min(100, int(progress)))
        detail = str(detail)[:1000]
        with self._conn() as conn:
            conn.execute(
                "UPDATE lab_jobs SET progress=?,phase=?,detail=?,heartbeat_at=? "
                "WHERE id=? AND status='running'", (progress, phase, detail, now, job_id),
            )
        event = {
            "progress": progress, "phase": phase, "detail": detail, **(metadata or {}),
        }
        event["type"] = event_type
        self.append_event(job_id, event)

    def heartbeat_job(self, job_id: str) -> None:
        """只刷新执行器心跳，不向事件时间线写入高频噪声。"""
        with self._conn() as conn:
            conn.execute(
                "UPDATE lab_jobs SET heartbeat_at=? WHERE id=? AND status='running'",
                (utc_now(), job_id),
            )

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
        current = self.job(job_id) or {}
        cancelled = bool(current and current["cancel_requested"])
        payload = result or {}
        warnings = payload.get("warnings") if isinstance(payload, dict) else []
        warnings = warnings if isinstance(warnings, list) else []
        partial = bool(warnings)
        paused = bool(isinstance(payload, dict) and payload.get("paused"))
        status = (
            "cancelled" if cancelled else "failed" if error else "paused" if paused
            else "completed_with_warnings" if partial else "completed"
        )
        progress = int(current.get("progress") or 0) if cancelled or error or paused else 100
        warning = warnings[0] if warnings else ""
        warning_text = str(warning.get("message") if isinstance(warning, dict) else warning)
        phase = (
            "已取消" if cancelled else "执行失败" if error else "等待恢复" if paused
            else "部分完成" if partial else "执行完成"
        )
        detail = (error if error else warning_text)[:1000]
        now = utc_now()
        with self._conn() as conn:
            conn.execute(
                "UPDATE lab_jobs SET status=?,progress=?,phase=?,detail=?,result_json=?,error=?,"
                "finished_at=?,heartbeat_at=? WHERE id=?",
                (
                    status, progress, phase, detail, canonical_json(payload), error[:1000],
                    now, now, job_id,
                ),
            )
        self.append_event(job_id, {
            "type": status, "progress": progress,
            "phase": phase, "detail": detail[:300],
        })

    def retry_job(self, job_id: str) -> dict:
        source = self.job(job_id)
        if source is None:
            raise KeyError("任务不存在")
        if source["status"] not in {
            "paused", "completed", "completed_with_warnings", "failed", "cancelled",
        }:
            raise ValueError("只能按相同参数重新运行已结束的任务")
        params = dict(source.get("params") or {})
        params.pop("_scheduled", None)
        created = self.enqueue(str(source["kind"]), params)
        self.append_event(created["id"], {
            "type": "retry_of", "source_job_id": job_id,
            "phase": "按历史参数重新运行",
        })
        self.append_event(job_id, {
            "type": "retried_as", "job_id": created["id"],
            "phase": "已创建重新运行任务",
        })
        return self.job(created["id"]) or created

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
            studies = conn.execute("SELECT COUNT(*) FROM optimization_studies").fetchone()[0]
            mining_runs = conn.execute("SELECT COUNT(*) FROM mining_runs").fetchone()[0]
        return {
            "factor_statuses": statuses,
            "active_jobs": running,
            "experiments": experiments,
            "deployments": deployments,
            "studies": studies,
            "mining_runs": mining_runs,
        }
