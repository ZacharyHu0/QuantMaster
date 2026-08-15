from __future__ import annotations

import json
import sqlite3
from contextlib import closing

from quantmaster.data.migration import backup_sqlite_tree
from quantmaster.research import AssetClass, ExecutionPlan, Frequency, PlanTask
from quantmaster.research.catalog import ResearchCatalog
from quantmaster.research.job_migration import ResearchJobLegacyMigrator
from quantmaster.research.jobs import ResearchJobManager
from quantmaster.runtime.jobs import UnifiedJobStore


def _legacy_catalog(root) -> None:
    path = root / "research_lake" / "_meta" / "catalog.sqlite"
    path.parent.mkdir(parents=True, exist_ok=True)
    plan = ExecutionPlan(
        id="legacy-plan",
        start="2024-01-02",
        end="2024-01-02",
        target_dates=("2024-01-02",),
        asset_classes=(AssetClass.STOCK,),
        frequency=Frequency.DAILY,
        datasets=("stock_bars",),
        selected_specs=(),
        tasks=(PlanTask(
            "sync", "stock_bars", AssetClass.STOCK, Frequency.DAILY, "2024-01-02",
        ),),
    ).to_dict()
    manifest = {
        "run_id": "research-completed",
        "plan_hash": plan["plan_hash"],
        "status": "completed_with_errors",
        "input_partitions": [],
        "output_partitions": [],
    }
    with closing(sqlite3.connect(path)) as connection:
        ResearchCatalog._schema_v2(connection)
        connection.executescript("""
            CREATE TABLE research_jobs (
                id TEXT PRIMARY KEY,status TEXT NOT NULL,mode TEXT NOT NULL,
                plan_json TEXT NOT NULL,next_index INTEGER NOT NULL DEFAULT 0,
                total INTEGER NOT NULL,succeeded INTEGER NOT NULL DEFAULT 0,
                failed INTEGER NOT NULL DEFAULT 0,cancel_requested INTEGER NOT NULL DEFAULT 0,
                current_task TEXT NOT NULL DEFAULT '',failures_json TEXT NOT NULL DEFAULT '[]',
                manifest_json TEXT NOT NULL DEFAULT '{}',created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,owner TEXT NOT NULL DEFAULT '',
                lease_expires REAL NOT NULL DEFAULT 0,heartbeat_at TEXT NOT NULL DEFAULT '',
                attempt INTEGER NOT NULL DEFAULT 1,task_indexes_json TEXT NOT NULL DEFAULT '[]'
            );
            CREATE TABLE research_job_events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,job_id TEXT NOT NULL,
                attempt INTEGER NOT NULL,event_json TEXT NOT NULL,created_at TEXT NOT NULL,
                FOREIGN KEY(job_id) REFERENCES research_jobs(id)
            );
            PRAGMA user_version=1;
        """)
        failure = [{"task_index": 0, "task": plan["tasks"][0], "error": "offline"}]
        connection.execute(
            "INSERT INTO research_jobs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "research-completed", "completed_with_errors", "historical",
                json.dumps(plan), 1, 1, 0, 1, 0, "", json.dumps(failure),
                json.dumps(manifest), "2026-08-01T00:00:00+00:00",
                "2026-08-01T00:01:00+00:00", "", 0.0, "", 2, json.dumps([0]),
            ),
        )
        connection.execute(
            "INSERT INTO research_runs(run_id,status,manifest_json,updated_at) VALUES (?,?,?,?)",
            (
                "research-completed", "completed_with_errors", json.dumps(manifest),
                "2026-08-01T00:01:00+00:00",
            ),
        )
        empty_plan = ExecutionPlan(
            id="active-plan",
            start="2024-01-02",
            end="2024-01-02",
            target_dates=("2024-01-02",),
            asset_classes=(AssetClass.STOCK,),
            frequency=Frequency.DAILY,
            datasets=(),
            selected_specs=(),
            tasks=(),
        ).to_dict()
        connection.execute(
            "INSERT INTO research_jobs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "research-running", "running", "historical", json.dumps(empty_plan),
                0, 0, 0, 0, 0, "", "[]", "{}",
                "2026-08-01T00:02:00+00:00", "2026-08-01T00:03:00+00:00",
                "legacy-worker", 2_000_000_000.0, "2026-08-01T00:03:00+00:00", 1, "[]",
            ),
        )
        connection.execute(
            "INSERT INTO research_job_events(job_id,attempt,event_json,created_at) "
            "VALUES (?,?,?,?)",
            (
                "research-completed", 2,
                '{"type":"completed_with_errors","failed":1}',
                "2026-08-01T00:01:00+00:00",
            ),
        )
        connection.execute(
            "INSERT INTO research_job_events(job_id,attempt,event_json,created_at) "
            "VALUES (?,?,?,?)",
            (
                "research-running", 1, '{"type":"claimed","owner":"legacy-worker"}',
                "2026-08-01T00:03:00+00:00",
            ),
        )
        connection.commit()


def test_research_job_migration_preserves_progress_outcome_and_provenance(tmp_path):
    _legacy_catalog(tmp_path)
    migrator = ResearchJobLegacyMigrator()

    inspected = list(migrator.inspect(tmp_path))
    assert [(item.record_key, item.outcome) for item in inspected] == [
        ("research-lake", "review"),
    ]
    assert [item.outcome for item in migrator.migrate_batch(
        tmp_path, after_key="", limit=1,
    )] == ["converted"]

    store = UnifiedJobStore(tmp_path / "jobs.sqlite", read_only=True)
    completed = ResearchJobManager._project(store, store.get("research-completed"))
    running = ResearchJobManager._project(store, store.get("research-running"))
    assert completed["status"] == "completed"
    assert completed["outcome"] == "completed_with_warnings"
    assert completed["failures"][0]["error"] == "offline"
    assert completed["manifest"]["plan_hash"] == completed["plan"]["plan_hash"]
    assert running["status"] == "interrupted"
    assert any(event["type"] == "job_started" for event in store.events("research-running"))
    assert list(migrator.inspect(tmp_path)) == []
    catalog = ResearchCatalog(
        tmp_path / "research_lake" / "_meta" / "catalog.sqlite", read_only=True,
    )
    with catalog._connect() as connection:
        tables = {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        version = connection.execute("PRAGMA user_version").fetchone()[0]
    assert version == 2
    assert not {"research_jobs", "research_job_events"} & tables


def test_research_job_migration_rejects_unknown_status_before_writing(tmp_path):
    _legacy_catalog(tmp_path)
    path = tmp_path / "research_lake" / "_meta" / "catalog.sqlite"
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("UPDATE research_jobs SET status='future_state' WHERE id='research-running'")
        connection.commit()

    records = list(ResearchJobLegacyMigrator().migrate_batch(
        tmp_path, after_key="", limit=1,
    ))

    assert records[0].outcome == "conflict"
    assert "status:future_state" in records[0].unknown_fields
    assert not (tmp_path / "jobs.sqlite").exists()
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("SELECT COUNT(*) FROM research_jobs").fetchone()[0] == 2


def test_research_job_migration_rejects_dangling_partition_before_writing(tmp_path):
    _legacy_catalog(tmp_path)
    path = tmp_path / "research_lake" / "_meta" / "catalog.sqlite"
    with closing(sqlite3.connect(path)) as connection:
        manifest = json.loads(connection.execute(
            "SELECT manifest_json FROM research_jobs WHERE id='research-running'"
        ).fetchone()[0])
        manifest["output_partitions"] = [{"partition_key": "missing-partition"}]
        connection.execute(
            "UPDATE research_jobs SET manifest_json=? WHERE id='research-running'",
            (json.dumps(manifest),),
        )
        connection.commit()

    records = list(ResearchJobLegacyMigrator().migrate_batch(
        tmp_path, after_key="", limit=1,
    ))

    assert records[0].outcome == "conflict"
    assert "research-running:output_partitions:0:dangling" in records[0].unknown_fields
    assert not (tmp_path / "jobs.sqlite").exists()


def test_research_job_migration_backup_restores_legacy_tables_and_removes_new_store(tmp_path):
    root = tmp_path / "data"
    root.mkdir(exist_ok=True)
    _legacy_catalog(root)
    migrator = ResearchJobLegacyMigrator()
    backup = tmp_path / "backup"
    backup_sqlite_tree(root, backup, extra_paths=migrator.backup_paths)
    list(migrator.migrate_batch(root, after_key="", limit=1))

    migrator.rollback(root, backup)

    assert not (root / "jobs.sqlite").exists()
    path = root / "research_lake" / "_meta" / "catalog.sqlite"
    with closing(sqlite3.connect(path)) as connection:
        row = connection.execute(
            "SELECT id,status FROM research_jobs WHERE id='research-completed'"
        ).fetchone()
        version = connection.execute("PRAGMA user_version").fetchone()[0]
    assert row == ("research-completed", "completed_with_errors")
    assert version == 1
