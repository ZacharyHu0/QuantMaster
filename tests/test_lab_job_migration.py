from __future__ import annotations

import json
import sqlite3
from contextlib import closing

from quantmaster.data.migration import backup_sqlite_tree
from quantmaster.lab.job_migration import LabJobLegacyMigrator
from quantmaster.lab.jobs import LabJobManager
from quantmaster.lab.store import LabStore
from quantmaster.runtime.jobs import UnifiedJobStore


def _legacy_lab(root) -> None:
    path = root / "lab.sqlite"
    store = LabStore(path)
    study = store.create_study({
        "universe": "demo",
        "start": "2024-01-01",
        "end": "2024-12-31",
        "protocol": {"horizons": [3], "sealed": True},
    })
    store.update_study(study["id"], status="completed", result={
        "trials": [{"number": 1, "score": 0.42}],
        "warnings": [{"code": "PARTIAL", "message": "保留已完成 trial"}],
    })
    mining = store.create_mining_run({"universe": "demo", "rounds": 2})
    store.update_mining_run(
        mining["id"], status="running", result={"candidate_count": 1},
    )
    now = "2026-08-01T00:00:00+00:00"
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("PRAGMA foreign_keys=OFF")
        connection.execute("DROP TABLE lab_worker_results")
        connection.executescript("""
            CREATE TABLE lab_jobs (
                id TEXT PRIMARY KEY,kind TEXT NOT NULL,status TEXT NOT NULL,
                params_json TEXT NOT NULL,result_json TEXT NOT NULL DEFAULT '{}',
                dataset_id TEXT NOT NULL DEFAULT '',resource_class TEXT NOT NULL DEFAULT 'cpu',
                preflight_json TEXT NOT NULL DEFAULT '{}',progress INTEGER NOT NULL DEFAULT 0,
                phase TEXT NOT NULL DEFAULT '',detail TEXT NOT NULL DEFAULT '',
                error TEXT NOT NULL DEFAULT '',error_code TEXT NOT NULL DEFAULT '',
                error_json TEXT NOT NULL DEFAULT '{}',telemetry_json TEXT NOT NULL DEFAULT '{}',
                cancel_requested INTEGER NOT NULL DEFAULT 0,worker TEXT NOT NULL DEFAULT '',
                llm_scope TEXT NOT NULL DEFAULT '',llm_revision TEXT NOT NULL DEFAULT '',
                cancellation_reason TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL,
                started_at TEXT NOT NULL DEFAULT '',heartbeat_at TEXT NOT NULL DEFAULT '',
                finished_at TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE lab_job_events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,job_id TEXT NOT NULL,
                event_json TEXT NOT NULL,created_at TEXT NOT NULL
            );
            CREATE TABLE lab_schedule_slots (
                slot TEXT PRIMARY KEY,created_at TEXT NOT NULL
            );
        """)
        completed_result = {
            "trials": [{"number": 1, "score": 0.42}],
            "warnings": [{"code": "PARTIAL", "message": "保留已完成 trial"}],
        }
        ready = {"runnable": True, "resource_class": "cpu"}
        connection.execute(
            "INSERT INTO lab_jobs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "lab-completed", "optimize", "completed_with_warnings",
                json.dumps({"study_id": study["id"]}), json.dumps(completed_result),
                "snapshot-1", "cpu", json.dumps(ready), 100, "部分完成",
                "保留已完成 trial", "", "", "{}", json.dumps({"seconds": 12}),
                0, "legacy-worker", "", "", "", now, now, now,
                "2026-08-01T00:12:00+00:00",
            ),
        )
        connection.execute(
            "INSERT INTO lab_jobs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "lab-running", "discover_python", "running",
                json.dumps({"run_id": mining["id"], "rounds": 2}), "{}",
                "", "external", json.dumps({"runnable": True, "resource_class": "external"}),
                40, "候选挖掘", "已保存候选 1", "", "", "{}", "{}", 0,
                "legacy-worker", "global", "legacy-revision", "", now, now, now, "",
            ),
        )
        connection.execute(
            "UPDATE optimization_studies SET job_id=? WHERE id=?",
            ("lab-completed", study["id"]),
        )
        connection.execute(
            "UPDATE mining_runs SET job_id=? WHERE id=?",
            ("lab-running", mining["id"]),
        )
        for job_id, event in (
            ("lab-completed", {"type": "completed_with_warnings", "progress": 100}),
            ("lab-running", {
                "type": "partition_checkpoint",
                "stage": "candidate",
                "partition": "candidate-1",
                "persisted": 1,
            }),
        ):
            connection.execute(
                "INSERT INTO lab_job_events(job_id,event_json,created_at) VALUES (?,?,?)",
                (job_id, json.dumps(event), now),
            )
        connection.execute(
            "INSERT INTO lab_publications "
            "(id,kind,version_id,experiment_id,payload_hash,payload_json,status,created_at,updated_at) "
            "VALUES (?,?,?,?,?,?,?, ?,?)",
            (
                "publication-1", "model_predictions", "version-1", "experiment-1",
                "hash", json.dumps({"partitions": ["p1"]}), "published", now, now,
            ),
        )
        connection.execute(
            "INSERT INTO research_cycles VALUES (?,?,?,?,?,?,?)",
            ("cycle-1", "", "completed", "{}", "{}", now, now),
        )
        connection.execute(
            "INSERT INTO strategy_candidates VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "champion-1", "cycle-1", 3, "保留冠军", "champion", "[]", "{}",
                "{}", "{}", "{}", "{}", now, now,
            ),
        )
        connection.execute("PRAGMA user_version=11")
        connection.commit()


def test_lab_job_migration_preserves_domain_results_and_unifies_lifecycle(tmp_path):
    _legacy_lab(tmp_path)
    migrator = LabJobLegacyMigrator()

    assert [(item.record_key, item.outcome) for item in migrator.inspect(tmp_path)] == [
        ("quant-lab", "review"),
    ]
    assert [item.outcome for item in migrator.migrate_batch(
        tmp_path, after_key="", limit=1,
    )] == ["converted"]

    jobs = UnifiedJobStore(tmp_path / "jobs.sqlite", read_only=True)
    completed = LabJobManager._project(jobs, jobs.get("lab-completed"))
    running = LabJobManager._project(jobs, jobs.get("lab-running"))
    assert completed["status"] == "completed"
    assert completed["outcome"] == "completed_with_warnings"
    assert completed["result"]["trials"] == [{"number": 1, "score": 0.42}]
    assert running["status"] == "interrupted"
    assert running["checkpoint"]["partition"] == "candidate-1"
    assert jobs.get("lab-running")["llm_scope"] == "global"
    assert any(
        event["type"] == "legacy_lab_partition_checkpoint"
        for event in jobs.events("lab-running")
    )

    lab = LabStore(tmp_path / "lab.sqlite", read_only=True)
    assert lab.worker_result("lab-completed")["outcome"] == "completed_with_warnings"
    assert lab.study(completed["params"]["study_id"])["config"]["protocol"]["sealed"]
    assert lab.mining_run(running["params"]["run_id"])["result"]["candidate_count"] == 1
    assert lab.publication("publication-1")["status"] == "published"
    assert lab.strategy("champion-1")["status"] == "champion"
    with closing(sqlite3.connect(tmp_path / "lab.sqlite")) as connection:
        tables = {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        version = connection.execute("PRAGMA user_version").fetchone()[0]
    assert version == 12
    assert not {"lab_jobs", "lab_job_events", "lab_schedule_slots"} & tables
    assert list(migrator.inspect(tmp_path)) == []


def test_lab_job_migration_rejects_unknown_status_before_writing(tmp_path):
    _legacy_lab(tmp_path)
    with closing(sqlite3.connect(tmp_path / "lab.sqlite")) as connection:
        connection.execute("UPDATE lab_jobs SET status='future_state' WHERE id='lab-running'")
        connection.commit()

    records = list(LabJobLegacyMigrator().migrate_batch(
        tmp_path, after_key="", limit=1,
    ))

    assert records[0].outcome == "conflict"
    assert "status:future_state" in records[0].unknown_fields
    assert not (tmp_path / "jobs.sqlite").exists()
    with closing(sqlite3.connect(tmp_path / "lab.sqlite")) as connection:
        assert connection.execute("SELECT COUNT(*) FROM lab_jobs").fetchone()[0] == 2


def test_lab_job_migration_rejects_dangling_domain_and_lease_before_writing(tmp_path):
    _legacy_lab(tmp_path)
    with closing(sqlite3.connect(tmp_path / "lab.sqlite")) as connection:
        connection.execute("UPDATE mining_runs SET job_id='missing-job'")
        connection.execute("UPDATE lab_jobs SET heartbeat_at='' WHERE id='lab-running'")
        connection.commit()

    records = list(LabJobLegacyMigrator().migrate_batch(
        tmp_path, after_key="", limit=1,
    ))

    assert records[0].outcome == "conflict"
    assert any("dangling_job" in value for value in records[0].unknown_fields)
    assert "lab-running:lease_evidence" in records[0].unknown_fields
    assert not (tmp_path / "jobs.sqlite").exists()


def test_lab_job_migration_rejects_dangling_trial_and_artifact_before_writing(tmp_path):
    _legacy_lab(tmp_path)
    with closing(sqlite3.connect(tmp_path / "lab.sqlite")) as connection:
        result = {
            "trials": [{"number": 1, "score": 0.42}],
            "recommended": {"number": 9},
            "prediction_artifact": "lab_artifacts/missing.parquet",
        }
        connection.execute(
            "UPDATE optimization_studies SET result_json=?",
            (json.dumps(result),),
        )
        connection.commit()

    records = list(LabJobLegacyMigrator().migrate_batch(
        tmp_path, after_key="", limit=1,
    ))

    assert records[0].outcome == "conflict"
    assert any("dangling_trial" in value for value in records[0].unknown_fields)
    assert any("prediction_artifact:dangling" in value for value in records[0].unknown_fields)
    assert not (tmp_path / "jobs.sqlite").exists()


def test_lab_job_migration_is_idempotent_after_partial_target_import(tmp_path):
    _legacy_lab(tmp_path)
    migrator = LabJobLegacyMigrator()
    from quantmaster.lab.job_migration import _convert

    with closing(connect := sqlite3.connect(tmp_path / "lab.sqlite")):
        connect.row_factory = sqlite3.Row
        converted = [
            _convert(connect, row)
            for row in connect.execute("SELECT * FROM lab_jobs ORDER BY created_at,id")
        ]
    target = UnifiedJobStore(tmp_path / "jobs.sqlite")
    for record, events, artifacts, _domain in converted:
        target.import_legacy_job(record, events=events, artifacts=artifacts)

    assert [item.outcome for item in migrator.migrate_batch(
        tmp_path, after_key="", limit=1,
    )] == ["converted"]
    assert len([
        job for job in target.list(100) if str(job["type"]).startswith("lab.")
    ]) == 2
    assert len(target.events("lab-running")) == 1


def test_lab_job_migration_backup_restores_legacy_tables_and_removes_new_store(tmp_path):
    root = tmp_path / "data"
    root.mkdir(exist_ok=True)
    _legacy_lab(root)
    migrator = LabJobLegacyMigrator()
    backup = tmp_path / "backup"
    backup_sqlite_tree(root, backup, extra_paths=migrator.backup_paths)
    list(migrator.migrate_batch(root, after_key="", limit=1))

    migrator.rollback(root, backup)

    assert not (root / "jobs.sqlite").exists()
    with closing(sqlite3.connect(root / "lab.sqlite")) as connection:
        row = connection.execute(
            "SELECT id,status FROM lab_jobs WHERE id='lab-completed'"
        ).fetchone()
        version = connection.execute("PRAGMA user_version").fetchone()[0]
    assert row == ("lab-completed", "completed_with_warnings")
    assert version == 11
