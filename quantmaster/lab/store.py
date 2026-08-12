"""Quant Lab 的 SQLite 研究账本与可恢复任务队列。"""

from __future__ import annotations

import json
import re
import sqlite3
import time
import uuid
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from quantmaster.config import get_config
from quantmaster.horizons import require_supported_horizon
from quantmaster.lab.models import (
    FACTOR_STATUSES,
    FactorSpec,
    canonical_json,
    content_hash,
    factor_name_key,
    normalize_factor_name,
    utc_now,
)
from quantmaster.runtime.sqlite import connect_sqlite, execute_sql_script, migrate_schema
from quantmaster.trading_sessions import daily_signal_cutoff

_GENERATED_FACTOR_NAME = re.compile(
    r"^(AI|GP)\s+候选\s+(\d+)(?:\s*·\s*[0-9A-Za-z_-]+)?$"
)
LAB_SCHEMA_VERSION = 11


def _collision_safe_factor_name(
    name: str, slug: str, occupied: dict[str, str],
) -> str:
    """Build a short generated name, falling back to a deterministic identifier."""
    generated = _GENERATED_FACTOR_NAME.fullmatch(name)
    if generated:
        family = generated.group(1)
        numbers = [
            int(match.group(2))
            for existing in occupied.values()
            if (match := _GENERATED_FACTOR_NAME.fullmatch(existing))
            and match.group(1) == family
        ]
        number = max([int(generated.group(2)), *numbers], default=0) + 1
        while True:
            candidate = f"{family} 候选 {number}"
            if factor_name_key(candidate) not in occupied:
                return candidate
            number += 1

    identifier = (slug.rsplit("_", 1)[-1] or slug)[:10]
    for suffix in (identifier[:8], identifier, slug[:16]):
        marker = f" · {suffix}"
        candidate = f"{name[:120 - len(marker)].rstrip()}{marker}"
        if factor_name_key(candidate) not in occupied:
            return candidate
    counter = 2
    while True:
        marker = f" · {identifier[:8]}-{counter}"
        candidate = f"{name[:120 - len(marker)].rstrip()}{marker}"
        if factor_name_key(candidate) not in occupied:
            return candidate
        counter += 1


class LabStore:
    def __init__(self, path: str | Path | None = None, *, read_only: bool = False):
        self.path = Path(path) if path else get_config().data_root / "lab.sqlite"
        self.read_only = bool(read_only)
        # A Web read is allowed to inspect an already published Lab ledger,
        # but it must never turn the first visit to the Lab tab into a schema
        # migration, a backup, or a curated-catalog write.  The runtime worker
        # owns those lifecycle operations at startup.
        if self.read_only:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._backup_before_migration()
        self._migrate()

    def _backup_before_migration(self) -> None:
        """Create one recoverable online backup before upgrading a real ledger."""
        if not self.path.is_file() or self.path.stat().st_size == 0:
            return
        try:
            with connect_sqlite(self.path) as source:
                version = int(source.execute("PRAGMA user_version").fetchone()[0])
                if version <= 0 or version >= LAB_SCHEMA_VERSION:
                    return
                backup_dir = self.path.parent / "backups"
                backup_dir.mkdir(parents=True, exist_ok=True)
                target = backup_dir / f"lab-pre-schema-v{LAB_SCHEMA_VERSION}.sqlite"
                if target.exists():
                    return
                with connect_sqlite(target) as destination:
                    source.backup(destination)
        except sqlite3.Error:
            target = self.path.parent / "backups" / f"lab-pre-schema-v{LAB_SCHEMA_VERSION}.sqlite"
            target.unlink(missing_ok=True)
            raise

    def _conn(self) -> sqlite3.Connection:
        return connect_sqlite(
            self.path,
            row_factory=True,
            timeout=0.25 if self.read_only else 30.0,
            read_only=self.read_only,
        )

    def _migrate(self) -> None:
        def schema_v8(conn: sqlite3.Connection) -> None:
            previous_user_version = conn.execute("PRAGMA user_version").fetchone()[0]
            execute_sql_script(conn, """
                CREATE TABLE IF NOT EXISTS factor_definitions (
                    id TEXT PRIMARY KEY, slug TEXT NOT NULL UNIQUE, name TEXT NOT NULL,
                    name_key TEXT NOT NULL, kind TEXT NOT NULL, category TEXT NOT NULL,
                    created_at TEXT NOT NULL);
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
                CREATE TABLE IF NOT EXISTS deployment_evidence (
                    deployment_id TEXT PRIMARY KEY, payload_hash TEXT NOT NULL,
                    payload_json TEXT NOT NULL, created_at TEXT NOT NULL,
                    FOREIGN KEY(deployment_id) REFERENCES deployments(id));
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
                    dataset_id TEXT NOT NULL DEFAULT '',
                    resource_class TEXT NOT NULL DEFAULT 'cpu',
                    preflight_json TEXT NOT NULL DEFAULT '{}',
                    progress INTEGER NOT NULL DEFAULT 0, phase TEXT NOT NULL DEFAULT '',
                    detail TEXT NOT NULL DEFAULT '', error TEXT NOT NULL DEFAULT '',
                    error_code TEXT NOT NULL DEFAULT '', error_json TEXT NOT NULL DEFAULT '{}',
                    telemetry_json TEXT NOT NULL DEFAULT '{}',
                    cancel_requested INTEGER NOT NULL DEFAULT 0, worker TEXT NOT NULL DEFAULT '',
                    llm_scope TEXT NOT NULL DEFAULT '', llm_revision TEXT NOT NULL DEFAULT '',
                    cancellation_reason TEXT NOT NULL DEFAULT '',
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
                CREATE TABLE IF NOT EXISTS lab_publications (
                    id TEXT PRIMARY KEY, kind TEXT NOT NULL, version_id TEXT NOT NULL,
                    experiment_id TEXT NOT NULL, payload_hash TEXT NOT NULL,
                    payload_json TEXT NOT NULL, status TEXT NOT NULL,
                    attempt INTEGER NOT NULL DEFAULT 0, next_run REAL NOT NULL DEFAULT 0,
                    owner TEXT NOT NULL DEFAULT '', lease_expires REAL NOT NULL DEFAULT 0,
                    last_error TEXT NOT NULL DEFAULT '', result_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                    UNIQUE(kind,version_id));
                CREATE TABLE IF NOT EXISTS lab_publication_events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, publication_id TEXT NOT NULL,
                    event_json TEXT NOT NULL, created_at TEXT NOT NULL,
                    FOREIGN KEY(publication_id) REFERENCES lab_publications(id));
                CREATE TABLE IF NOT EXISTS research_cycles (
                    id TEXT PRIMARY KEY, snapshot_id TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL, protocol_json TEXT NOT NULL,
                    result_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL, completed_at TEXT NOT NULL DEFAULT '');
                CREATE TABLE IF NOT EXISTS strategy_candidates (
                    id TEXT PRIMARY KEY, cycle_id TEXT NOT NULL, horizon INTEGER NOT NULL,
                    name TEXT NOT NULL, status TEXT NOT NULL,
                    components_json TEXT NOT NULL, development_json TEXT NOT NULL DEFAULT '{}',
                    sealed_json TEXT NOT NULL DEFAULT '{}', return_curve_json TEXT NOT NULL DEFAULT '{}',
                    shadow_json TEXT NOT NULL DEFAULT '{}', paper_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                    UNIQUE(cycle_id,horizon),
                    FOREIGN KEY(cycle_id) REFERENCES research_cycles(id));
                CREATE TABLE IF NOT EXISTS shadow_signals (
                    id TEXT PRIMARY KEY, strategy_id TEXT NOT NULL, signal_date TEXT NOT NULL,
                    mature_date TEXT NOT NULL DEFAULT '', status TEXT NOT NULL,
                    payload_json TEXT NOT NULL, realized_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL, UNIQUE(strategy_id,signal_date),
                    FOREIGN KEY(strategy_id) REFERENCES strategy_candidates(id));
                CREATE TABLE IF NOT EXISTS promotion_events (
                    id TEXT PRIMARY KEY, strategy_id TEXT NOT NULL,
                    from_status TEXT NOT NULL, to_status TEXT NOT NULL,
                    actor TEXT NOT NULL, reason TEXT NOT NULL,
                    evidence_json TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL,
                    FOREIGN KEY(strategy_id) REFERENCES strategy_candidates(id));
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
                CREATE INDEX IF NOT EXISTS idx_lab_publications_due
                    ON lab_publications(status,next_run,created_at);
                CREATE INDEX IF NOT EXISTS idx_strategy_candidates_status
                    ON strategy_candidates(status,horizon,updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_shadow_signals_maturity
                    ON shadow_signals(strategy_id,status,mature_date);
                CREATE INDEX IF NOT EXISTS idx_promotion_events_strategy
                    ON promotion_events(strategy_id,created_at DESC);
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
            definition_columns = {
                row[1] for row in conn.execute("PRAGMA table_info(factor_definitions)")
            }
            added_name_key = "name_key" not in definition_columns
            if added_name_key:
                conn.execute(
                    "ALTER TABLE factor_definitions "
                    "ADD COLUMN name_key TEXT NOT NULL DEFAULT ''"
                )
            missing_name_keys = conn.execute(
                "SELECT EXISTS(SELECT 1 FROM factor_definitions WHERE name_key='')"
            ).fetchone()[0]
            if previous_user_version < 6 or added_name_key or missing_name_keys:
                self._repair_factor_names(conn)
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_factor_definitions_name_key "
                "ON factor_definitions(name_key)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_deployments_runtime "
                "ON deployments(status,universe,horizon,profile,role)"
            )
            job_columns = {
                row[1] for row in conn.execute("PRAGMA table_info(lab_jobs)")
            }
            for name, declaration in (
                ("dataset_id", "TEXT NOT NULL DEFAULT ''"),
                ("resource_class", "TEXT NOT NULL DEFAULT 'cpu'"),
                ("preflight_json", "TEXT NOT NULL DEFAULT '{}'"),
                ("error_code", "TEXT NOT NULL DEFAULT ''"),
                ("error_json", "TEXT NOT NULL DEFAULT '{}'"),
                ("telemetry_json", "TEXT NOT NULL DEFAULT '{}'"),
                ("llm_scope", "TEXT NOT NULL DEFAULT ''"),
                ("llm_revision", "TEXT NOT NULL DEFAULT ''"),
                ("cancellation_reason", "TEXT NOT NULL DEFAULT ''"),
            ):
                if name not in job_columns:
                    conn.execute(f"ALTER TABLE lab_jobs ADD COLUMN {name} {declaration}")
            conn.execute(
                "UPDATE lab_jobs SET error_code='LEGACY_FAILURE',"
                "error_json=json_object('code','LEGACY_FAILURE','message',error,"
                "'action','查看历史任务详情','retryable',1,'context',json('{}')) "
                "WHERE error<>'' AND error_code=''"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_lab_jobs_resource "
                "ON lab_jobs(status,resource_class,created_at)"
            )

        with self._conn() as conn:
            migrate_schema(conn, ((LAB_SCHEMA_VERSION, schema_v8),))

    @staticmethod
    def _repair_factor_names(conn: sqlite3.Connection) -> None:
        """Assign unique registry labels while preserving every historical version."""
        occupied: dict[str, str] = {}
        rows = conn.execute(
            "SELECT id,slug,name FROM factor_definitions ORDER BY created_at,id"
        ).fetchall()
        for row in rows:
            name = normalize_factor_name(row["name"]) or row["slug"]
            # ASCII comma is the multi-factor delimiter in the workbench. Historical
            # generated labels are made copy-safe without touching immutable specs.
            name = name.replace(",", "，")[:120]
            generated = _GENERATED_FACTOR_NAME.fullmatch(name)
            if generated:
                name = f"{generated.group(1)} 候选 {int(generated.group(2))}"
            key = factor_name_key(name)
            if key in occupied:
                name = _collision_safe_factor_name(name, row["slug"], occupied)
                key = factor_name_key(name)
            conn.execute(
                "UPDATE factor_definitions SET name=?,name_key=? WHERE id=?",
                (name, key, row["id"]),
            )
            occupied[key] = name

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
        if self.read_only:
            return 0
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
        requested_name = normalize_factor_name(spec.name)
        if source == "manual" and "," in requested_name:
            raise ValueError("因子名称不能包含英文逗号；逗号用于分隔多个因子")
        if source != "manual":
            requested_name = requested_name.replace(",", "，")
        now = utc_now()
        with self._conn() as conn:
            # Serialize the name check with the insert so concurrent workers cannot
            # create two definitions with the same Unicode-normalized display name.
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM factor_definitions WHERE slug=?", (spec.slug,)).fetchone()
            if row is None:
                occupied_rows = conn.execute(
                    "SELECT id,slug,name,name_key FROM factor_definitions"
                ).fetchall()
                occupied = {item["name_key"]: item["name"] for item in occupied_rows}
                requested_key = factor_name_key(requested_name)
                conflict = next(
                    (item for item in occupied_rows if item["name_key"] == requested_key),
                    None,
                )
                if conflict is not None and source == "manual":
                    raise ValueError(
                        f"因子名称“{requested_name}”已存在（标识 {conflict['slug']}），"
                        "请使用唯一名称"
                    )
                assigned_name = (
                    _collision_safe_factor_name(requested_name, spec.slug, occupied)
                    if conflict is not None else requested_name
                )
                factor_id = uuid.uuid4().hex
                conn.execute(
                    "INSERT INTO factor_definitions"
                    "(id,slug,name,name_key,kind,category,created_at) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (
                        factor_id, spec.slug, assigned_name,
                        factor_name_key(assigned_name), spec.kind, spec.category, now,
                    ),
                )
                row = conn.execute(
                    "SELECT * FROM factor_definitions WHERE id=?", (factor_id,)).fetchone()
            elif factor_name_key(requested_name) != row["name_key"] and source == "manual":
                raise ValueError(
                    f"该表达式已登记为因子“{row['name']}”（标识 {row['slug']}），"
                    "请使用现有名称或从现有版本修订"
                )
            factor = dict(row)
            if factor["kind"] != spec.kind:
                raise ValueError(
                    f"因子标识 {spec.slug} 已用于 {factor['kind']} 类型，不能改为 {spec.kind}"
                )
            payload = spec.to_dict()
            payload["name"] = factor["name"]
            digest = content_hash(payload)
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
            value.pop("name_key", None)
            items.append(value)
        return {"items": items, "total": total, "limit": limit, "offset": offset}

    def factor_reference(self, reference: str) -> dict | None:
        """Resolve the latest factor version by its stable slug or unique display name."""
        value = normalize_factor_name(reference)
        if not value:
            return None
        with self._conn() as conn:
            row = conn.execute(
                "SELECT d.slug,d.name,d.kind,d.category,v.id AS version_id,v.version,"
                "v.status,v.source,v.spec_json,v.updated_at "
                "FROM factor_definitions d JOIN factor_versions v ON v.factor_id=d.id "
                "AND v.version=(SELECT MAX(v2.version) FROM factor_versions v2 "
                "WHERE v2.factor_id=d.id) "
                "WHERE d.slug=? OR d.name_key=? "
                "ORDER BY CASE WHEN d.slug=? THEN 0 ELSE 1 END LIMIT 1",
                (value, factor_name_key(value), value),
            ).fetchone()
        resolved = self._decode(row, ("spec_json",))
        if resolved is not None:
            resolved["spec"] = resolved.pop("spec_json")
        return resolved

    def runtime_factors(self) -> list[dict]:
        """Return expression factors that can be referenced by the regular workbench."""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT d.slug,d.name,d.category,v.status,v.source,v.spec_json "
                "FROM factor_definitions d JOIN factor_versions v ON v.factor_id=d.id "
                "AND v.version=(SELECT MAX(v2.version) FROM factor_versions v2 "
                "WHERE v2.factor_id=d.id) WHERE d.kind='expression' "
                "ORDER BY d.name_key,d.slug"
            ).fetchall()
        result = []
        for row in rows:
            value = self._decode(row, ("spec_json",)) or {}
            spec = value.pop("spec_json")
            if value["source"] == "builtin" or not spec.get("expression"):
                continue
            result.append({
                "name": value["name"], "slug": value["slug"],
                "description": spec.get("description") or spec.get("rationale") or "",
                "category": value["category"], "status": value["status"],
                "source": "quant_lab",
            })
        return result
    @staticmethod
    def _version_from_conn(conn: sqlite3.Connection, version_id: str) -> dict | None:
        row = conn.execute(
            "SELECT v.*,d.slug,d.name,d.kind,d.category FROM factor_versions v "
            "JOIN factor_definitions d ON d.id=v.factor_id WHERE v.id=?", (version_id,),
        ).fetchone()
        report = conn.execute(
            "SELECT report_json,created_at FROM validation_reports WHERE version_id=? "
            "ORDER BY created_at DESC,id DESC LIMIT 1", (version_id,),
        ).fetchone()
        value = LabStore._decode(row, ("spec_json",))
        if value is not None:
            value["spec"] = value.pop("spec_json")
            value["validation"] = json.loads(report[0]) if report else None
            value["validation_created_at"] = str(report[1]) if report else ""
        return value

    def version(self, version_id: str) -> dict | None:
        with self._conn() as conn:
            return self._version_from_conn(conn, version_id)

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
        require_supported_horizon(horizon)
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
        horizon_evidence = (report.get("horizons") or {}).get(str(horizon))
        if horizon_evidence is not None and not (horizon_evidence.get("gates") or {}).get("passed"):
            raise ValueError(f"版本的 {horizon} 日门槛未通过，不能由其他周期的结果授权部署")
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
            frozen_version = self._version_from_conn(conn, version_id)
            if frozen_version is None:
                raise RuntimeError("部署时无法冻结因子版本证据")
            frozen_payload = {
                "schema_version": 1,
                "deployment": {
                    "id": deployment_id, "universe": universe, "horizon": horizon,
                    "role": role, "profile": profile, "scope": scope,
                    "version_id": version_id, "created_at": now,
                },
                "version": frozen_version,
            }
            conn.execute(
                "INSERT INTO deployment_evidence VALUES (?,?,?,?)",
                (deployment_id, content_hash(frozen_payload), canonical_json(frozen_payload), now),
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
        if payload.get("manifest"):
            from quantmaster.lab.dataset import verify_snapshot_evidence

            verify_snapshot_evidence(payload)
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

    def latest_snapshot(self, universe: str = "") -> dict | None:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM dataset_snapshots ORDER BY created_at DESC LIMIT 50"
            ).fetchall()
        for row in rows:
            value = self._decode(row, ("payload_json",)) or {}
            payload = value.pop("payload_json")
            if universe and str(payload.get("universe") or "") != universe:
                continue
            value["payload"] = payload
            return value
        return None

    def create_experiment(self, name: str, method: str, config: dict) -> dict:
        now, experiment_id = utc_now(), uuid.uuid4().hex
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO experiments VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    experiment_id, name, method, "", "running",
                    canonical_json(config), "{}", now, now,
                ),
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

    def list_experiments(
        self,
        limit: int = 50,
        *,
        status: str | None = None,
        method: str | None = None,
        cursor: str | None = None,
        summary: bool = False,
    ) -> list[dict]:
        clauses: list[str] = []
        params: list[Any] = []
        if status:
            clauses.append("status=?")
            params.append(status)
        if method:
            clauses.append("method=?")
            params.append(method)
        with self._conn() as conn:
            if cursor:
                cursor_row = conn.execute(
                    "SELECT created_at,id FROM experiments WHERE id=?", (cursor,),
                ).fetchone()
                if cursor_row is not None:
                    clauses.append("(created_at<? OR (created_at=? AND id<?))")
                    params.extend([
                        cursor_row["created_at"], cursor_row["created_at"], cursor_row["id"],
                    ])
            where = "WHERE " + " AND ".join(clauses) if clauses else ""
            if summary:
                # A dashboard only needs scalar facts.  Do not read or decode an
                # experiment's potentially large training artifacts just to render
                # a row in a table.
                columns = """
                    id,name,method,dataset_id,status,created_at,updated_at,
                    json_extract(config_json, '$.universe') AS config_universe,
                    json_extract(config_json, '$.start') AS config_start,
                    json_extract(config_json, '$.end') AS config_end,
                    json_extract(config_json, '$.horizon') AS config_horizon,
                    json_extract(config_json, '$.sequence_length') AS config_sequence_length,
                    json_extract(result_json, '$.metrics.correlation') AS result_correlation,
                    json_extract(result_json, '$.version_id') AS result_version_id,
                    json_extract(result_json, '$.version_status') AS result_version_status,
                    json_extract(result_json, '$.device') AS result_device,
                    json_extract(result_json, '$.telemetry.effective_device') AS telemetry_device,
                    json_extract(result_json, '$.telemetry.samples_per_second')
                        AS telemetry_samples_per_second,
                    json_extract(result_json, '$.telemetry.peak_gpu_memory_mb')
                        AS telemetry_peak_gpu_memory_mb,
                    json_extract(result_json, '$.train_samples') AS result_train_samples,
                    json_extract(result_json, '$.validation_samples') AS result_validation_samples
                """
            else:
                columns = "*"
            rows = conn.execute(
                f"SELECT {columns} FROM experiments {where} "
                "ORDER BY created_at DESC,id DESC LIMIT ?",
                (*params, max(1, min(limit, 500))),
            ).fetchall()
        if not summary:
            return [self._decode(row, ("config_json", "result_json")) or {} for row in rows]
        result: list[dict] = []
        for row in rows:
            value = dict(row)
            config = {
                key: value.pop(f"config_{key}")
                for key in ("universe", "start", "end", "horizon", "sequence_length")
                if value.get(f"config_{key}") is not None
            }
            correlation = value.pop("result_correlation")
            telemetry = {
                output: value.pop(column)
                for output, column in (
                    ("effective_device", "telemetry_device"),
                    ("samples_per_second", "telemetry_samples_per_second"),
                    ("peak_gpu_memory_mb", "telemetry_peak_gpu_memory_mb"),
                )
                if value.get(column) is not None
            }
            result_value = {
                key: value.pop(f"result_{key}")
                for key in (
                    "version_id", "version_status", "device", "train_samples",
                    "validation_samples",
                )
                if value.get(f"result_{key}") is not None
            }
            if correlation is not None:
                result_value["metrics"] = {"correlation": correlation}
            if telemetry:
                result_value["telemetry"] = telemetry
            value["config_json"] = config
            value["result_json"] = result_value
            result.append(value)
        return result

    @staticmethod
    def _publication(row: sqlite3.Row | None) -> dict | None:
        if row is None:
            return None
        value = dict(row)
        value["payload"] = json.loads(value.pop("payload_json"))
        value["result"] = json.loads(value.pop("result_json"))
        return value

    def enqueue_publication(
        self,
        kind: str,
        version_id: str,
        experiment_id: str,
        payload: dict,
    ) -> dict:
        """Persist an immutable publish request before touching the target data lake."""
        payload_json = canonical_json(payload)
        payload_hash = content_hash(payload)
        now = utc_now()
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM lab_publications WHERE kind=? AND version_id=?",
                (kind, version_id),
            ).fetchone()
            if row is not None:
                if row["payload_hash"] != payload_hash:
                    raise ValueError("同一模型版本的发布规格不可改写")
                return self._publication(row) or {}
            publication_id = uuid.uuid4().hex
            conn.execute(
                "INSERT INTO lab_publications "
                "(id,kind,version_id,experiment_id,payload_hash,payload_json,status,"
                "next_run,created_at,updated_at) VALUES (?,?,?,?,?,?,'pending',?,?,?)",
                (
                    publication_id, kind, version_id, experiment_id, payload_hash,
                    payload_json, time.time(), now, now,
                ),
            )
            conn.execute(
                "INSERT INTO lab_publication_events(publication_id,event_json,created_at) "
                "VALUES (?,?,?)",
                (publication_id, canonical_json({"type": "pending"}), now),
            )
            row = conn.execute(
                "SELECT * FROM lab_publications WHERE id=?", (publication_id,),
            ).fetchone()
        return self._publication(row) or {}

    def publication(self, publication_id: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM lab_publications WHERE id=?", (publication_id,),
            ).fetchone()
        return self._publication(row)

    def pending_publications(
        self, limit: int = 100, *, due_only: bool = True,
    ) -> list[dict]:
        now = time.time()
        where = (
            "((status='pending' AND next_run<=?) OR "
            "(status='publishing' AND lease_expires<=?))"
            if due_only else "status IN ('pending','publishing')"
        )
        params: list[Any] = [now, now] if due_only else []
        params.append(max(1, min(int(limit), 1000)))
        with self._conn() as conn:
            rows = conn.execute(
                f"SELECT * FROM lab_publications WHERE {where} "
                "ORDER BY created_at LIMIT ?",
                params,
            ).fetchall()
        return [self._publication(row) or {} for row in rows]

    def claim_publication(
        self, publication_id: str, owner: str, *, lease_seconds: float = 120.0,
    ) -> dict | None:
        now = time.time()
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            changed = conn.execute(
                "UPDATE lab_publications SET status='publishing',attempt=attempt+1,owner=?,"
                "lease_expires=?,updated_at=? WHERE id=? AND "
                "((status='pending' AND next_run<=?) OR "
                "(status='publishing' AND lease_expires<=?))",
                (owner, now + max(10.0, lease_seconds), utc_now(), publication_id, now, now),
            ).rowcount
            if not changed:
                return None
            row = conn.execute(
                "SELECT * FROM lab_publications WHERE id=?", (publication_id,),
            ).fetchone()
            conn.execute(
                "INSERT INTO lab_publication_events(publication_id,event_json,created_at) "
                "VALUES (?,?,?)",
                (publication_id, canonical_json({
                    "type": "publishing", "owner": owner, "attempt": int(row["attempt"]),
                }), utc_now()),
            )
        return self._publication(row)

    def complete_publication(
        self, publication_id: str, owner: str, result: dict,
    ) -> bool:
        now = utc_now()
        with self._conn() as conn:
            changed = conn.execute(
                "UPDATE lab_publications SET status='published',owner='',lease_expires=0,"
                "last_error='',result_json=?,updated_at=? "
                "WHERE id=? AND owner=? AND status='publishing'",
                (canonical_json(result), now, publication_id, owner),
            ).rowcount
            if changed:
                conn.execute(
                    "INSERT INTO lab_publication_events(publication_id,event_json,created_at) "
                    "VALUES (?,?,?)",
                    (publication_id, canonical_json({"type": "published", "result": result}), now),
                )
        return bool(changed)

    def fail_publication(
        self, publication_id: str, owner: str, error: str,
    ) -> bool:
        now_epoch = time.time()
        now = utc_now()
        with self._conn() as conn:
            row = conn.execute(
                "SELECT attempt FROM lab_publications WHERE id=? AND owner=? "
                "AND status='publishing'",
                (publication_id, owner),
            ).fetchone()
            if row is None:
                return False
            retry_at = now_epoch + min(60.0 * (2 ** max(0, int(row["attempt"]) - 1)), 86400.0)
            conn.execute(
                "UPDATE lab_publications SET status='pending',owner='',lease_expires=0,"
                "last_error=?,next_run=?,updated_at=? WHERE id=? AND owner=?",
                (error[:2000], retry_at, now, publication_id, owner),
            )
            conn.execute(
                "INSERT INTO lab_publication_events(publication_id,event_json,created_at) "
                "VALUES (?,?,?)",
                (publication_id, canonical_json({
                    "type": "publish_failed", "error": error[:1000],
                    "retry_at": retry_at,
                }), now),
            )
        return True

    def publication_events(self, publication_id: str) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT seq,event_json,created_at FROM lab_publication_events "
                "WHERE publication_id=? ORDER BY seq",
                (publication_id,),
            ).fetchall()
        return [
            {"seq": row["seq"], "created_at": row["created_at"],
             **json.loads(row["event_json"])}
            for row in rows
        ]

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

    def studies(self, limit: int = 50, *, summary: bool = False) -> list[dict]:
        with self._conn() as conn:
            if summary:
                # Study result payloads may hold every trial.  Keep that evidence
                # on the detail route so a page switch never deserializes it.
                columns = """
                    id,job_id,experiment_id,config_hash,status,storage_url,created_at,updated_at,
                    json_extract(config_json, '$.universe') AS config_universe,
                    json_extract(config_json, '$.start') AS config_start,
                    json_extract(config_json, '$.end') AS config_end,
                    json_extract(config_json, '$.budget_hours') AS config_budget_hours,
                    json_extract(config_json, '$.protocol.fold_test_days') AS config_fold_test_days,
                    json_extract(result_json, '$.version_id') AS result_version_id,
                    json_extract(result_json, '$.candidate') AS result_candidate,
                    json_array_length(COALESCE(json_extract(result_json, '$.trials'), '[]'))
                        AS result_trial_count,
                    CASE WHEN json_type(result_json, '$.sealed_metrics') IS NULL
                        THEN 0 ELSE 1 END AS result_sealed
                """
            else:
                columns = "*"
            rows = conn.execute(
                f"SELECT {columns} FROM optimization_studies ORDER BY updated_at DESC LIMIT ?",
                (max(1, min(limit, 500)),),
            ).fetchall()
        if summary:
            result: list[dict] = []
            for row in rows:
                value = dict(row)
                config = {
                    key: value.pop(f"config_{key}")
                    for key in ("universe", "start", "end", "budget_hours")
                    if value.get(f"config_{key}") is not None
                }
                fold_days = value.pop("config_fold_test_days")
                if fold_days is not None:
                    config["protocol"] = {"fold_test_days": fold_days}
                trial_count = int(value.pop("result_trial_count") or 0)
                value["config"] = config
                value["result"] = {
                    key: value.pop(f"result_{key}")
                    for key in ("version_id", "candidate")
                    if value.get(f"result_{key}") is not None
                }
                value["result"]["trial_count"] = trial_count
                value["result"]["sealed"] = bool(value.pop("result_sealed"))
                result.append(value)
            return result
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
        assignments: list[str] = ["updated_at=?"]
        params: list[Any] = [utc_now()]
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
        assignments: list[str] = ["updated_at=?"]
        params: list[Any] = [utc_now()]
        for column, value in (("status", status), ("job_id", job_id),
                              ("snapshot_hash", snapshot_hash)):
            if value is not None:
                assignments.append(f"{column}=?")
                params.append(value)
        for column, payload in (("split_json", split), ("result_json", result)):
            if payload is not None:
                assignments.append(f"{column}=?")
                params.append(canonical_json(payload))
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

    def enqueue(
        self,
        kind: str,
        params: dict,
        *,
        preflight: dict[str, Any] | None = None,
        dataset_id: str = "",
    ) -> dict:
        job_id, now = uuid.uuid4().hex, utc_now()
        admission = dict(preflight or {})
        llm_scope = llm_revision = ""
        if kind in {"discover_llm", "discover_python"}:
            from quantmaster.runtime.llm import get_llm_execution_coordinator

            coordinator = get_llm_execution_coordinator()
            coordinator.register_lab_store(self)
            llm_scope = "global"
            llm_revision = coordinator.revision(llm_scope)
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO lab_jobs "
                "(id,kind,status,params_json,dataset_id,resource_class,preflight_json,"
                "llm_scope,llm_revision,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    job_id, kind, "queued", canonical_json(params), dataset_id,
                    str(admission.get("resource_class") or "cpu"), canonical_json(admission),
                    llm_scope, llm_revision, now,
                ),
            )
        self.append_event(job_id, {
            "type": "queued", "progress": 0, "phase": "等待执行",
            "resource_class": str(admission.get("resource_class") or "cpu"),
        })
        return self.job(job_id) or {}

    def interrupt_legacy_llm(self) -> int:
        """Require an explicit retry for old discovery rows without a revision."""
        with self._conn() as conn:
            changed = conn.execute(
                "UPDATE lab_jobs SET status='interrupted',phase='需要手动重试',"
                "detail='旧 AI 发现任务缺少执行版本，已安全中断',finished_at=? "
                "WHERE kind IN ('discover_llm','discover_python') "
                "AND status IN ('queued','interrupted') AND llm_revision='' "
                "AND phase<>'需要手动重试'",
                (utc_now(),),
            ).rowcount
        return int(changed)

    def interrupt_stale_llm(self) -> int:
        """Do not resume discovery work created under an expired AI revision.

        A configuration rotation normally cancels these rows immediately.  This
        startup recovery covers the narrow crash/lock window where the durable
        revision changed but the Lab ledger was unavailable for that update.
        Such work must be retried explicitly with the current configuration;
        it must never be silently rebound to a new provider setup.
        """
        from quantmaster.runtime.llm import get_llm_execution_coordinator

        coordinator = get_llm_execution_coordinator()
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT id,llm_scope,llm_revision FROM lab_jobs "
                "WHERE kind IN ('discover_llm','discover_python') "
                "AND status IN ('queued','interrupted') "
                "AND cancel_requested=0 AND llm_revision<>'' "
                "AND phase<>'需要手动重试'"
            ).fetchall()
            stale_ids = [
                str(row["id"])
                for row in rows
                if not coordinator.current(
                    str(row["llm_scope"] or "global"), str(row["llm_revision"]),
                )
            ]
            changed = 0
            for job_id in stale_ids:
                changed += conn.execute(
                    "UPDATE lab_jobs SET status='interrupted',phase='需要手动重试',"
                    "detail='AI 配置版本已过期，请按当前配置重新运行',"
                    "cancellation_reason='configuration_revision_expired',finished_at=? "
                    "WHERE id=? AND status IN ('queued','interrupted') "
                    "AND cancel_requested=0",
                    (utc_now(), job_id),
                ).rowcount
        return int(changed)

    def cancel_stale_llm(self, scope: str, revision: str, reason: str) -> dict[str, int]:
        """Fence Lab's own ledger when settings rotate an LLM scope."""
        now = utc_now()
        with self._conn() as conn:
            queued = conn.execute(
                "UPDATE lab_jobs SET status='cancelled',cancel_requested=1,"
                "cancellation_reason=?,phase='已取消',detail=?,finished_at=? "
                "WHERE kind IN ('discover_llm','discover_python') AND status IN ('queued','interrupted') "
                "AND llm_scope=? AND llm_revision<>?",
                (reason[:240], reason[:1000], now, scope, revision),
            ).rowcount
            running = conn.execute(
                "UPDATE lab_jobs SET status='cancelling',cancel_requested=1,"
                "cancellation_reason=?,phase='正在安全停止',detail=? "
                "WHERE kind IN ('discover_llm','discover_python') AND status IN ('running','cancelling') "
                "AND llm_scope=? AND llm_revision<>?",
                (reason[:240], reason[:1000], scope, revision),
            ).rowcount
        return {"queued_cancelled": int(queued), "running_cancelling": int(running)}

    def claim_next(
        self,
        worker: str,
        *,
        allow_scheduled: bool = True,
        max_running: int | None = None,
        resource_limits: dict[str, int] | None = None,
    ) -> dict | None:
        now = utc_now()
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if max_running is not None:
                running = int(conn.execute(
                    "SELECT COUNT(*) FROM lab_jobs WHERE status='running'"
                ).fetchone()[0])
                if running >= max(1, int(max_running)):
                    return None
            resource_clauses: list[str] = []
            resource_params: list[Any] = []
            for resource, limit in (resource_limits or {}).items():
                resource_clauses.append(
                    "NOT (resource_class=? AND (SELECT COUNT(*) FROM lab_jobs "
                    "WHERE status='running' AND resource_class=?)>=?)"
                )
                resource_params.extend([resource, resource, max(1, int(limit))])
            resource_sql = (
                " AND " + " AND ".join(resource_clauses) if resource_clauses else ""
            )
            row = conn.execute(
                "SELECT id FROM lab_jobs WHERE status IN ('queued','interrupted') "
                "AND NOT (kind IN ('discover_llm','discover_python') AND llm_revision='') "
                "AND (? OR params_json NOT LIKE '%\"_scheduled\":true%') "
                f"{resource_sql} ORDER BY created_at LIMIT 1",
                (int(allow_scheduled), *resource_params),
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

    def deployments_as_of(
        self,
        as_of: str,
        *,
        universe: str | None = None,
        horizon: int | None = None,
        profile: str | None = None,
        role: str | None = None,
    ) -> list[dict]:
        """Return deployments that were active at the end of a Shanghai day."""
        try:
            target = date.fromisoformat(as_of)
        except ValueError as exc:
            raise ValueError("部署查看日期需要使用 YYYY-MM-DD 格式") from exc
        cutoff = daily_signal_cutoff(target).astimezone(UTC)
        filters, params = [], []
        for column, value in (
            ("universe", universe), ("horizon", horizon),
            ("profile", profile), ("role", role),
        ):
            if value is not None:
                filters.append(f"{column}=?")
                params.append(value)
        query = (
            "SELECT d.*,e.payload_hash AS evidence_hash,e.payload_json AS evidence_json "
            "FROM deployments d LEFT JOIN deployment_evidence e ON e.deployment_id=d.id"
        )
        if filters:
            query += " WHERE " + " AND ".join(f"d.{item}" for item in filters)
        query += " ORDER BY d.created_at DESC"
        with self._conn() as conn:
            rows = [dict(row) for row in conn.execute(query, tuple(params)).fetchall()]

        def parse(value: str) -> datetime | None:
            if not value:
                return None
            parsed = datetime.fromisoformat(value)
            return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)

        result = []
        for row in rows:
            created = parse(str(row.get("created_at") or ""))
            retired = parse(str(row.get("retired_at") or ""))
            if created is not None and created <= cutoff and (retired is None or retired > cutoff):
                raw_evidence = str(row.pop("evidence_json", "") or "")
                expected_hash = str(row.pop("evidence_hash", "") or "")
                row["version_snapshot"] = None
                row["evidence_status"] = "missing"
                if raw_evidence:
                    try:
                        evidence = json.loads(raw_evidence)
                    except json.JSONDecodeError as exc:
                        raise RuntimeError(
                            f"部署 {row['id']} 的冻结证据无法解析"
                        ) from exc
                    if not expected_hash or content_hash(evidence) != expected_hash:
                        raise RuntimeError(f"部署 {row['id']} 的冻结证据哈希不一致")
                    descriptor = evidence.get("deployment") or {}
                    if (
                        str(descriptor.get("id") or "") != str(row["id"])
                        or str(descriptor.get("version_id") or "") != str(row["version_id"])
                        or str(descriptor.get("universe") or "") != str(row["universe"])
                        or int(descriptor.get("horizon") or 0) != int(row["horizon"])
                        or str(descriptor.get("role") or "") != str(row["role"])
                        or str(descriptor.get("profile") or "") != str(row["profile"])
                        or str(descriptor.get("scope") or "") != str(row["scope"])
                        or str(descriptor.get("created_at") or "") != str(row["created_at"])
                    ):
                        raise RuntimeError(f"部署 {row['id']} 的冻结证据与部署记录不匹配")
                    snapshot = evidence.get("version")
                    if not isinstance(snapshot, dict):
                        raise RuntimeError(f"部署 {row['id']} 缺少冻结版本快照")
                    row["version_snapshot"] = snapshot
                    row["evidence_status"] = "verified"
                    row["evidence_hash"] = expected_hash
                result.append(row)
        result.sort(
            key=lambda item: parse(str(item.get("created_at") or ""))
            or datetime.min.replace(tzinfo=UTC),
            reverse=True,
        )
        slots: dict[tuple[Any, ...], str] = {}
        for row in result:
            slot = tuple(row.get(field) for field in (
                "universe", "horizon", "role", "profile", "scope",
            ))
            if slot in slots:
                raise RuntimeError(
                    f"{as_of} 的部署账本在同一运行槽存在多个 active 版本："
                    f"{slots[slot]}、{row['id']}"
                )
            slots[slot] = str(row["id"])
        return result

    def champion_strategies_as_of(self, as_of: str, *, horizon: int) -> list[dict]:
        """Rebuild champion state from promotion events at the signal cutoff."""
        require_supported_horizon(horizon)
        try:
            target = date.fromisoformat(as_of)
        except ValueError as exc:
            raise ValueError("Champion 查看日期需要使用 YYYY-MM-DD 格式") from exc
        cutoff = daily_signal_cutoff(target).astimezone(UTC)
        with self._conn() as conn:
            events = conn.execute(
                "SELECT e.* FROM promotion_events e "
                "JOIN strategy_candidates s ON s.id=e.strategy_id "
                "WHERE s.horizon=? ORDER BY e.created_at,e.id",
                (horizon,),
            ).fetchall()

        def parse(value: str) -> datetime | None:
            if not value:
                return None
            parsed = datetime.fromisoformat(value)
            return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)

        by_strategy: dict[str, list[dict]] = {}
        for event_row in events:
            event = self._decode(event_row, ("evidence_json",)) or {}
            happened = parse(str(event.get("created_at") or ""))
            if happened is not None and happened <= cutoff:
                by_strategy.setdefault(str(event["strategy_id"]), []).append(event)
        for history in by_strategy.values():
            history.sort(key=lambda event: (
                parse(str(event.get("created_at") or "")) or datetime.min.replace(tzinfo=UTC),
                str(event.get("id") or ""),
            ))
        result: list[dict[str, Any]] = []
        for strategy_id, history in by_strategy.items():
            latest = history[-1]
            if str(latest.get("to_status") or "") != "champion":
                continue
            evidence = latest.get("evidence_json") or {}
            snapshot = evidence.get("strategy_snapshot")
            expected_hash = str(evidence.get("strategy_snapshot_hash") or "")
            if not isinstance(snapshot, dict) or not expected_hash:
                raise RuntimeError(
                    f"策略 {strategy_id} 的 Champion 事件缺少冻结证据"
                )
            if content_hash(snapshot) != expected_hash:
                raise RuntimeError(f"策略 {strategy_id} 的 Champion 冻结证据哈希不一致")
            if (
                str(snapshot.get("id") or "") != strategy_id
                or int(snapshot.get("horizon") or 0) != horizon
                or str(snapshot.get("status") or "") != "champion"
            ):
                raise RuntimeError(f"策略 {strategy_id} 的 Champion 冻结证据与事件不匹配")
            value = json.loads(canonical_json(snapshot))
            value["promotion_events"] = history
            value["promotion_evidence_hash"] = expected_hash
            result.append(value)
        if len(result) > 1:
            ids = ", ".join(sorted(str(item.get("id") or "") for item in result))
            raise RuntimeError(f"{as_of} 的事件账本重建出多个 Champion：{ids}")
        return result

    def append_event(self, job_id: str, event: dict) -> int:
        with self._conn() as conn:
            cursor = conn.execute(
                "INSERT INTO lab_job_events(job_id,event_json,created_at) VALUES (?,?,?)",
                (job_id, canonical_json(event), utc_now()),
            )
        return int(cursor.lastrowid or 0)

    def update_job(
        self,
        job_id: str,
        progress: int,
        phase: str,
        detail: str = "",
        *,
        event_type: str = "progress",
        metadata: dict[str, Any] | None = None,
        expected_worker: str = "",
    ) -> bool:
        now = utc_now()
        progress = max(0, min(100, int(progress)))
        detail = str(detail)[:1000]
        with self._conn() as conn:
            where = "id=? AND status IN ('running','cancelling')"
            params: list[Any] = [progress, phase, detail, now, job_id]
            if expected_worker:
                where += " AND worker=?"
                params.append(expected_worker)
            changed = conn.execute(
                "UPDATE lab_jobs SET progress=?,phase=?,detail=?,heartbeat_at=? "
                f"WHERE {where}", params,
            ).rowcount
        if not changed:
            return False
        event = {
            "progress": progress, "phase": phase, "detail": detail, **(metadata or {}),
        }
        event["type"] = event_type
        self.append_event(job_id, event)
        return True

    def heartbeat_job(self, job_id: str, worker: str = "") -> bool:
        """只刷新执行器心跳，不向事件时间线写入高频噪声。"""
        with self._conn() as conn:
            where = "id=? AND status IN ('running','cancelling')"
            params: list[Any] = [utc_now(), job_id]
            if worker:
                where += " AND worker=?"
                params.append(worker)
            changed = conn.execute(
                f"UPDATE lab_jobs SET heartbeat_at=? WHERE {where}", params,
            ).rowcount
        return bool(changed)

    def request_cancel(self, job_id: str) -> dict:
        with self._conn() as conn:
            changed = conn.execute(
                "UPDATE lab_jobs SET cancel_requested=1 WHERE id=? "
                "AND status IN ('queued','running','cancelling','paused','interrupted')", (job_id,),
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

    def finish_job(
        self,
        job_id: str,
        *,
        result: dict | None = None,
        error: str = "",
        error_info: dict[str, Any] | None = None,
        telemetry: dict[str, Any] | None = None,
        expected_worker: str = "",
    ) -> bool:
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
        failure = dict(error_info or {})
        error_code = str(failure.get("code") or ("INTERNAL_ERROR" if error else ""))
        runtime_telemetry = dict(telemetry or payload.get("telemetry") or {})
        with self._conn() as conn:
            where = "id=?"
            params: list[Any] = [
                status, progress, phase, detail, canonical_json(payload), error[:1000],
                error_code, canonical_json(failure), canonical_json(runtime_telemetry),
                now, now, job_id,
            ]
            if expected_worker:
                where += " AND worker=? AND status IN ('running','cancelling')"
                params.append(expected_worker)
            changed = conn.execute(
                "UPDATE lab_jobs SET "
                "status=CASE WHEN cancel_requested=1 THEN 'cancelled' ELSE ? END,"
                "progress=CASE WHEN cancel_requested=1 THEN progress ELSE ? END,"
                "phase=CASE WHEN cancel_requested=1 THEN '已取消' ELSE ? END,"
                "detail=CASE WHEN cancel_requested=1 THEN "
                "COALESCE(NULLIF(cancellation_reason,''),'任务已取消；已丢弃迟到结果') ELSE ? END,"
                "result_json=CASE WHEN cancel_requested=1 THEN '{}' ELSE ? END,"
                "error=CASE WHEN cancel_requested=1 THEN '' ELSE ? END,"
                "error_code=CASE WHEN cancel_requested=1 THEN '' ELSE ? END,"
                "error_json=CASE WHEN cancel_requested=1 THEN '{}' ELSE ? END,"
                "telemetry_json=CASE WHEN cancel_requested=1 THEN '{}' ELSE ? END,"
                f"finished_at=?,heartbeat_at=? WHERE {where}", params,
            ).rowcount
        if not changed:
            return False
        completed = self.job(job_id) or {}
        status = str(completed.get("status") or status)
        progress = int(completed.get("progress") or progress)
        phase = str(completed.get("phase") or phase)
        detail = str(completed.get("detail") or detail)
        self.append_event(job_id, {
            "type": status, "progress": progress,
            "phase": phase, "detail": detail[:300],
        })
        return True

    def retry_job(self, job_id: str) -> dict:
        source = self.job(job_id)
        if source is None:
            raise KeyError("任务不存在")
        if source["status"] not in {
            "paused", "completed", "completed_with_warnings", "failed", "cancelled", "interrupted",
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

    def interrupt_stale(self, worker: str = "", stale_after_seconds: int = 30) -> int:
        with self._conn() as conn:
            if worker:
                cursor = conn.execute(
                    "UPDATE lab_jobs SET status='interrupted',worker='' "
                    "WHERE status='running' AND worker=?", (worker,),
                )
            else:
                cursor = conn.execute(
                    "UPDATE lab_jobs SET status='interrupted',worker='' "
                    "WHERE status='running' AND (heartbeat_at='' OR "
                    "julianday(heartbeat_at)<julianday('now',?))",
                    (f"-{max(1, int(stale_after_seconds))} seconds",),
                )
        return cursor.rowcount

    def recover_orphaned_records(self) -> dict[str, int]:
        """Close derived ledgers left running after their owning worker disappeared."""
        now = utc_now()
        recovered = {"experiments": 0, "studies": 0, "mining_runs": 0}
        with self._conn() as conn:
            running_jobs = int(conn.execute(
                "SELECT COUNT(*) FROM lab_jobs WHERE status='running'",
            ).fetchone()[0])
            if not running_jobs:
                recovered["experiments"] = conn.execute(
                    "UPDATE experiments SET status='interrupted',updated_at=? "
                    "WHERE status='running'", (now,),
                ).rowcount
            for table, key in (
                ("optimization_studies", "studies"),
                ("mining_runs", "mining_runs"),
            ):
                recovered[key] = conn.execute(
                    f"UPDATE {table} SET status='interrupted',updated_at=? "
                    "WHERE status='running' AND (job_id='' OR NOT EXISTS ("
                    f"SELECT 1 FROM lab_jobs WHERE lab_jobs.id={table}.job_id "
                    "AND lab_jobs.status IN ('queued','running'))) ",
                    (now,),
                ).rowcount
        return recovered

    @staticmethod
    def _public_job(value: dict[str, Any]) -> dict[str, Any]:
        value["params"] = value.pop("params_json")
        value["result"] = value.pop("result_json")
        value["preflight"] = value.pop("preflight_json", {})
        value["error_info"] = value.pop("error_json", {})
        value["telemetry"] = value.pop("telemetry_json", {})
        if value.get("error") and not value.get("error_code"):
            value["error_code"] = "LEGACY_FAILURE"
            value["error_info"] = {
                "code": "LEGACY_FAILURE", "message": value["error"],
                "action": "查看历史任务详情", "retryable": True, "context": {},
            }
        return value

    @staticmethod
    def _summary_job(row: sqlite3.Row) -> dict[str, Any]:
        """Return a stable job-list shape without touching JSON artifact blobs."""
        value = dict(row)
        value.update({
            "params": {}, "result": {}, "preflight": {}, "error_info": {}, "telemetry": {},
        })
        if value.get("error") and not value.get("error_code"):
            value["error_code"] = "LEGACY_FAILURE"
            value["error_info"] = {
                "code": "LEGACY_FAILURE", "message": value["error"],
                "action": "查看历史任务详情", "retryable": True, "context": {},
            }
        return value

    def job(self, job_id: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM lab_jobs WHERE id=?", (job_id,)).fetchone()
        value = self._decode(
            row, ("params_json", "result_json", "preflight_json", "error_json", "telemetry_json"),
        )
        if value is not None:
            self._public_job(value)
        return value

    def jobs(
        self,
        limit: int = 50,
        *,
        status: str | None = None,
        kind: str | None = None,
        cursor: str | None = None,
        offset: int = 0,
        summary: bool = False,
    ) -> list[dict]:
        clauses, params = [], []
        if status:
            clauses.append("status=?")
            params.append(status)
        if kind:
            clauses.append("kind=?")
            params.append(kind)
        with self._conn() as conn:
            if cursor:
                cursor_row = conn.execute(
                    "SELECT created_at,id FROM lab_jobs WHERE id=?", (cursor,),
                ).fetchone()
                if cursor_row is not None:
                    clauses.append("(created_at<? OR (created_at=? AND id<?))")
                    params.extend([
                        cursor_row["created_at"], cursor_row["created_at"], cursor_row["id"],
                    ])
            where = "WHERE " + " AND ".join(clauses) if clauses else ""
            columns = (
                "id,kind,status,dataset_id,resource_class,progress,phase,detail,error,"
                "error_code,cancel_requested,worker,created_at,started_at,heartbeat_at,finished_at"
                if summary else "*"
            )
            rows = conn.execute(
                f"SELECT {columns} FROM lab_jobs {where} "
                "ORDER BY created_at DESC,id DESC LIMIT ? OFFSET ?",
                (*params, max(1, min(limit, 500)), max(0, int(offset))),
            ).fetchall()
        result = []
        for row in rows:
            if summary:
                result.append(self._summary_job(row))
                continue
            value = self._decode(
                row,
                ("params_json", "result_json", "preflight_json", "error_json", "telemetry_json"),
            ) or {}
            self._public_job(value)
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

    def create_research_cycle(
        self, *, snapshot_id: str, protocol: dict[str, Any], status: str = "running",
    ) -> dict:
        cycle_id, now = uuid.uuid4().hex, utc_now()
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO research_cycles VALUES (?,?,?,?,?,?,?)",
                (cycle_id, snapshot_id, status, canonical_json(protocol), "{}", now, ""),
            )
        return self.research_cycle(cycle_id) or {}

    def research_cycle(self, cycle_id: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM research_cycles WHERE id=?", (cycle_id,),
            ).fetchone()
        return self._decode(row, ("protocol_json", "result_json"))

    def latest_research_cycle(self) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM research_cycles ORDER BY created_at DESC LIMIT 1",
            ).fetchone()
        return self._decode(row, ("protocol_json", "result_json"))

    def complete_research_cycle(self, cycle_id: str, result: dict[str, Any]) -> dict:
        with self._conn() as conn:
            changed = conn.execute(
                "UPDATE research_cycles SET status='completed',result_json=?,completed_at=? "
                "WHERE id=?", (canonical_json(result), utc_now(), cycle_id),
            ).rowcount
        if not changed:
            raise KeyError("研究周期不存在")
        return self.research_cycle(cycle_id) or {}

    @staticmethod
    def _decode_strategy(row: sqlite3.Row | None) -> dict | None:
        value = LabStore._decode(row, (
            "components_json", "development_json", "sealed_json", "return_curve_json",
            "shadow_json", "paper_json",
        ))
        if value is None:
            return None
        for field in (
            "components", "development", "sealed_evidence", "return_curve",
            "shadow_summary", "paper_summary",
        ):
            value[field] = value.pop({
                "components": "components_json", "development": "development_json",
                "sealed_evidence": "sealed_json", "return_curve": "return_curve_json",
                "shadow_summary": "shadow_json", "paper_summary": "paper_json",
            }[field])
        return value

    def _strategy_snapshot_from_conn(
        self, conn: sqlite3.Connection, strategy_id: str,
    ) -> dict[str, Any]:
        row = conn.execute(
            "SELECT * FROM strategy_candidates WHERE id=?", (strategy_id,),
        ).fetchone()
        snapshot = self._decode_strategy(row)
        if snapshot is None:
            raise RuntimeError("策略事件无法冻结不存在的候选")
        versions: dict[str, dict[str, Any]] = {}
        missing: list[str] = []
        for component in snapshot.get("components") or []:
            version_id = str(component.get("version_id") or "")
            if not version_id or version_id in versions or version_id in missing:
                continue
            version = self._version_from_conn(conn, version_id)
            if version is None:
                missing.append(version_id)
            else:
                versions[version_id] = version
        snapshot["component_versions"] = versions
        snapshot["missing_component_version_ids"] = missing
        return snapshot

    def _strategy_event_evidence(
        self, conn: sqlite3.Connection, strategy_id: str,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        snapshot = self._strategy_snapshot_from_conn(conn, strategy_id)
        return {
            **(details or {}),
            "strategy_snapshot": snapshot,
            "strategy_snapshot_hash": content_hash(snapshot),
        }

    def _insert_promotion_event(
        self, conn: sqlite3.Connection, *, strategy_id: str,
        from_status: str, to_status: str, actor: str, reason: str,
        created_at: str, details: dict[str, Any] | None = None,
    ) -> None:
        evidence = self._strategy_event_evidence(conn, strategy_id, details)
        conn.execute(
            "INSERT INTO promotion_events VALUES (?,?,?,?,?,?,?,?)",
            (
                uuid.uuid4().hex, strategy_id, from_status, to_status,
                actor, reason, canonical_json(evidence), created_at,
            ),
        )

    def save_strategy_candidate(
        self, *, cycle_id: str, horizon: int, name: str,
        components: list[dict[str, Any]], development: dict[str, Any],
        sealed_evidence: dict[str, Any], return_curve: dict[str, Any] | None = None,
    ) -> dict:
        require_supported_horizon(horizon)
        if not 3 <= len({str(item.get("version_id")) for item in components}) <= 8:
            raise ValueError("多因子组合必须包含 3–8 个不重复成分")
        weights = [float(item.get("weight") or 0) for item in components]
        if any(weight < 0.05 - 1e-9 or weight > 0.35 + 1e-9 for weight in weights):
            raise ValueError("组合成分权重必须在 5%–35% 之间")
        if abs(sum(weights) - 1.0) > 1e-6:
            raise ValueError("组合成分权重之和必须为 1")
        gate = sealed_evidence.get("gates") or {}
        status = "shadow_challenger" if gate.get("passed") else "historical_candidate"
        now = utc_now()
        with self._conn() as conn:
            existing = conn.execute(
                "SELECT id,status FROM strategy_candidates WHERE cycle_id=? AND horizon=?",
                (cycle_id, horizon),
            ).fetchone()
            strategy_id = existing["id"] if existing else uuid.uuid4().hex
            previous_status = str(existing["status"]) if existing else "created"
            if existing:
                conn.execute(
                    "UPDATE strategy_candidates SET name=?,status=?,components_json=?,"
                    "development_json=?,sealed_json=?,return_curve_json=?,updated_at=? WHERE id=?",
                    (name, status, canonical_json(components), canonical_json(development),
                     canonical_json(sealed_evidence), canonical_json(return_curve or {}), now,
                     strategy_id),
                )
            else:
                conn.execute(
                    "INSERT INTO strategy_candidates VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (strategy_id, cycle_id, horizon, name, status,
                     canonical_json(components), canonical_json(development),
                     canonical_json(sealed_evidence), canonical_json(return_curve or {}),
                     "{}", "{}", now, now),
                )
            self._insert_promotion_event(
                conn,
                strategy_id=strategy_id,
                from_status=previous_status,
                to_status=status,
                actor="system",
                reason=(
                    "候选证据修订后重新判定生命周期"
                    if existing
                    else "密封样本门槛通过，自动进入影子"
                    if status == "shadow_challenger"
                    else "保存未通过密封门槛的历史候选"
                ),
                created_at=now,
                details={"sealed_gate": gate, "revision": bool(existing)},
            )
        return self.strategy(strategy_id) or {}

    def strategy(self, strategy_id: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM strategy_candidates WHERE id=?", (strategy_id,),
            ).fetchone()
            events = conn.execute(
                "SELECT * FROM promotion_events WHERE strategy_id=? ORDER BY created_at",
                (strategy_id,),
            ).fetchall()
        value = self._decode_strategy(row)
        if value is not None:
            value["promotion_events"] = [
                self._decode(item, ("evidence_json",)) for item in events
            ]
        return value

    def strategies(
        self, *, horizon: int | None = None, status: str = "", limit: int = 100,
    ) -> list[dict]:
        clauses: list[str] = []
        params: list[Any] = []
        if horizon is not None:
            require_supported_horizon(horizon)
            clauses.append("horizon=?")
            params.append(horizon)
        if status:
            clauses.append("status=?")
            params.append(status)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        params.append(max(1, min(limit, 500)))
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM strategy_candidates" + where
                + " ORDER BY updated_at DESC LIMIT ?", params,
            ).fetchall()
        return [self._decode_strategy(row) or {} for row in rows]

    def update_strategy_tracking(
        self, strategy_id: str, *, shadow: dict[str, Any] | None = None,
        paper: dict[str, Any] | None = None,
    ) -> dict:
        if self.strategy(strategy_id) is None:
            raise KeyError("策略候选不存在")
        assignments, params = [], []
        if shadow is not None:
            assignments.append("shadow_json=?")
            params.append(canonical_json(shadow))
        if paper is not None:
            assignments.append("paper_json=?")
            params.append(canonical_json(paper))
        if assignments:
            assignments.append("updated_at=?")
            params.extend([utc_now(), strategy_id])
            with self._conn() as conn:
                conn.execute(
                    "UPDATE strategy_candidates SET " + ",".join(assignments) + " WHERE id=?",
                    params,
                )
        return self.strategy(strategy_id) or {}

    def save_shadow_signal(
        self, strategy_id: str, *, signal_date: str, mature_date: str,
        payload: dict[str, Any], realized: dict[str, Any] | None = None,
    ) -> dict:
        if self.strategy(strategy_id) is None:
            raise KeyError("策略候选不存在")
        status = "matured" if realized else "pending"
        signal_id = uuid.uuid4().hex
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO shadow_signals VALUES (?,?,?,?,?,?,?,?) "
                "ON CONFLICT(strategy_id,signal_date) DO UPDATE SET "
                "mature_date=excluded.mature_date,status=excluded.status,"
                "payload_json=excluded.payload_json,realized_json=excluded.realized_json",
                (signal_id, strategy_id, signal_date, mature_date, status,
                 canonical_json(payload), canonical_json(realized or {}), utc_now()),
            )
            row = conn.execute(
                "SELECT * FROM shadow_signals WHERE strategy_id=? AND signal_date=?",
                (strategy_id, signal_date),
            ).fetchone()
        return self._decode(row, ("payload_json", "realized_json")) or {}

    def shadow_signals(self, strategy_id: str, limit: int = 100) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM shadow_signals WHERE strategy_id=? "
                "ORDER BY signal_date DESC LIMIT ?", (strategy_id, max(1, min(limit, 500))),
            ).fetchall()
        return [self._decode(row, ("payload_json", "realized_json")) or {} for row in rows]

    def promote_strategy(
        self, strategy_id: str, *, target: str, actor: str, reason: str,
    ) -> dict:
        target_status = {
            "paper": "paper", "champion": "champion",
            "degraded": "degraded", "retired": "retired",
        }.get(target)
        if target_status is None:
            raise ValueError("晋级动作只支持 paper/champion/degraded/retired")
        if not actor.strip() or not reason.strip():
            raise ValueError("人工晋级必须记录操作者和理由")
        current = self.strategy(strategy_id)
        if current is None:
            raise KeyError("策略候选不存在")
        source = str(current["status"])
        if target_status == "paper":
            if source != "shadow_challenger":
                raise ValueError("只有 Shadow Challenger 可以申请模拟盘")
            shadow = current.get("shadow_summary") or {}
            minimum = max(20, 2 * int(current["horizon"]))
            if int(shadow.get("matured_signal_days") or 0) < minimum:
                raise ValueError(f"影子至少需要 {minimum} 个已成熟信号日")
            if float(shadow.get("net_excess_return") or 0) <= 0:
                raise ValueError("影子实际扣费后超额收益必须为正")
            if shadow.get("drawdown_within_stress") is not True or shadow.get("coverage_degraded"):
                raise ValueError("影子回撤或数据覆盖硬门槛未通过")
        elif target_status == "champion":
            if source != "paper":
                raise ValueError("只有模拟盘策略可以申请 Champion")
            paper = current.get("paper_summary") or {}
            if int(paper.get("trading_days") or 0) < 20:
                raise ValueError("模拟盘至少需要 20 个实际交易日")
            if float(paper.get("net_return") or 0) < 0 or int(paper.get("persistent_anomalies") or 0):
                raise ValueError("模拟盘扣费后收益为负或存在持续订单/数据异常")
        elif target_status not in {"degraded", "retired"}:
            raise ValueError("不允许该生命周期迁移")
        sealed_gate = (current.get("sealed_evidence") or {}).get("gates") or {}
        if target_status in {"paper", "champion"} and not sealed_gate.get("passed"):
            raise ValueError("密封集硬门槛未通过，理由不能覆盖")
        now = utc_now()
        with self._conn() as conn:
            if target_status == "champion":
                champion_rows = conn.execute(
                    "SELECT * FROM strategy_candidates WHERE horizon=? AND status='champion' "
                    "AND id<>? ORDER BY updated_at DESC",
                    (current["horizon"], strategy_id),
                ).fetchall()
                if len(champion_rows) > 1:
                    raise RuntimeError("当前账本存在多个 Champion，拒绝继续替换")
                champion = champion_rows[0] if champion_rows else None
                if champion is not None:
                    old = self._decode_strategy(champion) or {}
                    new_metrics = (current.get("sealed_evidence") or {}).get("metrics") or {}
                    old_metrics = (old.get("sealed_evidence") or {}).get("metrics") or {}
                    new_ir = float(new_metrics.get("net_information_ratio") or 0)
                    old_ir = float(old_metrics.get("net_information_ratio") or 0)
                    new_dd = float(new_metrics.get("max_drawdown") or 1)
                    old_dd = float(old_metrics.get("max_drawdown") or 1)
                    new_return = float(new_metrics.get("net_annual_excess_return") or 0)
                    old_return = float(old_metrics.get("net_annual_excess_return") or 0)
                    superior = new_ir >= old_ir + 0.10 or (
                        abs(new_ir - old_ir) <= 0.10
                        and new_dd <= old_dd * 0.90 and new_return >= old_return
                    )
                    if not superior:
                        raise ValueError("Challenger 未满足 Champion 替换优势门槛")
                    conn.execute(
                        "UPDATE strategy_candidates SET status='degraded',updated_at=? WHERE id=?",
                        (now, old["id"]),
                    )
                    self._insert_promotion_event(
                        conn,
                        strategy_id=str(old["id"]),
                        from_status="champion",
                        to_status="degraded",
                        actor=actor.strip(),
                        reason=f"被更优 Champion 替换：{reason.strip()}",
                        created_at=now,
                        details={"replacement_strategy_id": strategy_id},
                    )
            changed = conn.execute(
                "UPDATE strategy_candidates SET status=?,updated_at=? WHERE id=? AND status=?",
                (target_status, now, strategy_id, source),
            ).rowcount
            if changed != 1:
                raise RuntimeError("策略生命周期已并发变化，拒绝写入过期晋级事件")
            self._insert_promotion_event(
                conn,
                strategy_id=strategy_id,
                from_status=source,
                to_status=target_status,
                actor=actor.strip(),
                reason=reason.strip(),
                created_at=now,
                details={"sealed_gate": sealed_gate},
            )
        return self.strategy(strategy_id) or {}

    def strategy_return_curve(self, strategy_id: str) -> dict[str, Any]:
        current = self.strategy(strategy_id)
        if current is None:
            raise KeyError("策略候选不存在")
        from quantmaster.lab.strategy import return_curve_points

        latest: dict[int, dict[str, Any]] = {}
        for item in self.strategies(limit=500):
            latest.setdefault(int(item["horizon"]), item)
        challenger = return_curve_points(list(latest.values()))
        champions = return_curve_points(self.strategies(status="champion", limit=20))
        baseline = [
            {"horizon": point["horizon"], "annual_net_excess_return": float(
                (point.get("return_curve") or {}).get("baseline_annual_net_excess_return") or 0
            )}
            for point in latest.values()
        ]
        return {
            "strategy_id": strategy_id, "horizons": [1, 3, 5, 7, 10, 20, 30],
            "challenger": challenger, "champion": champions, "baseline": baseline,
        }

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
            job_statuses = {
                row["status"]: row["count"] for row in conn.execute(
                    "SELECT status,COUNT(*) AS count FROM lab_jobs GROUP BY status"
                )
            }
            experiments = conn.execute("SELECT COUNT(*) FROM experiments").fetchone()[0]
            deployments = conn.execute(
                "SELECT COUNT(*) FROM deployments WHERE status='active'"
            ).fetchone()[0]
            studies = conn.execute("SELECT COUNT(*) FROM optimization_studies").fetchone()[0]
            mining_runs = conn.execute("SELECT COUNT(*) FROM mining_runs").fetchone()[0]
            strategy_statuses = {
                row["status"]: row["count"] for row in conn.execute(
                    "SELECT status,COUNT(*) AS count FROM strategy_candidates GROUP BY status"
                )
            }
        return {
            "factor_statuses": statuses,
            "job_statuses": job_statuses,
            "active_jobs": running,
            "experiments": experiments,
            "deployments": deployments,
            "studies": studies,
            "mining_runs": mining_runs,
            "strategy_statuses": strategy_statuses,
        }


def read_runtime_factors(path: str | Path | None = None) -> list[dict]:
    """Read published Quant Lab factors without bootstrapping its ledger.

    The regular factor page may be opened before Quant Lab has ever run.  It
    uses a real read-only SQLite connection and treats an absent or pre-schema
    ledger as an empty optional catalog, never entering LabStore's
    backup/migration path.
    """

    database = Path(path) if path else get_config().data_root / "lab.sqlite"
    try:
        with connect_sqlite(database, timeout=0.25, row_factory=True, read_only=True) as conn:
            rows = conn.execute(
                "SELECT d.slug,d.name,d.category,v.status,v.source,v.spec_json "
                "FROM factor_definitions d JOIN factor_versions v ON v.factor_id=d.id "
                "AND v.version=(SELECT MAX(v2.version) FROM factor_versions v2 "
                "WHERE v2.factor_id=d.id) WHERE d.kind='expression' "
                "ORDER BY d.name_key,d.slug"
            ).fetchall()
    except (FileNotFoundError, sqlite3.OperationalError):
        return []
    result: list[dict] = []
    for row in rows:
        try:
            spec = json.loads(str(row["spec_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if str(row["source"] or "") == "builtin" or not spec.get("expression"):
            continue
        result.append({
            "name": str(row["name"] or ""),
            "slug": str(row["slug"] or ""),
            "description": spec.get("description") or spec.get("rationale") or "",
            "category": str(row["category"] or ""),
            "status": str(row["status"] or ""),
            "source": "quant_lab",
        })
    return result
