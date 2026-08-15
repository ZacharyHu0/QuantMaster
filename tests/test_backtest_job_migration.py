from __future__ import annotations

import json
import sqlite3
from contextlib import closing

from quantmaster.backtest.job_migration import BacktestJobLegacyMigrator
from quantmaster.backtest.jobs import BACKTEST_TASK_TYPE, BacktestJobManager
from quantmaster.backtest.spec import BacktestSpec
from quantmaster.backtest.workbench import BacktestStore
from quantmaster.data.migration import backup_sqlite_tree
from quantmaster.runtime.jobs import UnifiedJobStore


def _spec(name: str) -> BacktestSpec:
    return BacktestSpec.model_validate({
        "name": name,
        "strategy": {"kind": "factor", "factor": "mom_20d", "top_n": 3},
        "universe": "demo",
        "start": "2023-01-01",
        "end": "2023-12-31",
        "benchmark": None,
        "initial_capital": 100_000,
    })


def _legacy_database(root) -> None:
    root.mkdir(parents=True, exist_ok=True)
    artifacts = root / "backtests"
    artifacts.mkdir()
    completed = _spec("completed")
    failed = _spec("failed")
    running = _spec("running")
    manifest = {
        "config_hash": completed.snapshot_hash,
        "strategy_snapshot": completed.strategy.model_dump(mode="json"),
        "data_quality": {"status": "complete", "source": "local-cache"},
        "warnings": [{"code": "sandbox", "message": "仅用于研究"}],
        "formal_eligible": False,
    }
    artifact = {
        "manifest": manifest,
        "metrics": {"annual_return": 0.1},
        "nav": [["2023-01-03", 1.0]],
        "trades": [{"symbol": "600000.SH", "shares": 100}],
    }
    completed_path = artifacts / "completed" / "result.json"
    completed_path.parent.mkdir()
    completed_path.write_text(json.dumps(artifact), encoding="utf-8")
    path = root / "backtests.sqlite"
    with closing(sqlite3.connect(path)) as connection:
        connection.executescript("""
            CREATE TABLE backtest_runs (
                id TEXT PRIMARY KEY, name TEXT NOT NULL, status TEXT NOT NULL,
                config_json TEXT NOT NULL, config_hash TEXT NOT NULL,
                manifest_json TEXT NOT NULL DEFAULT '{}', result_json TEXT NOT NULL DEFAULT '{}',
                artifact_path TEXT NOT NULL DEFAULT '', progress INTEGER NOT NULL DEFAULT 0,
                phase TEXT NOT NULL DEFAULT '', detail TEXT NOT NULL DEFAULT '',
                error TEXT NOT NULL DEFAULT '', cancel_requested INTEGER NOT NULL DEFAULT 0,
                worker TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL,
                started_at TEXT NOT NULL DEFAULT '', heartbeat_at TEXT NOT NULL DEFAULT '',
                finished_at TEXT NOT NULL DEFAULT '');
            CREATE TABLE backtest_events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL,
                event_json TEXT NOT NULL, created_at TEXT NOT NULL,
                FOREIGN KEY(run_id) REFERENCES backtest_runs(id));
            CREATE INDEX idx_backtest_status ON backtest_runs(status,created_at);
            CREATE INDEX idx_backtest_events ON backtest_events(run_id,seq);
            CREATE TABLE backtest_store_meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);
            INSERT INTO backtest_store_meta VALUES ('schema_version','1');
        """)
        connection.execute(
            "INSERT INTO backtest_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "completed", "completed", "completed",
                json.dumps(completed.model_dump(mode="json")), completed.snapshot_hash,
                json.dumps(manifest), json.dumps({"metrics": artifact["metrics"]}),
                str(completed_path), 100, "执行完成", "", "", 0, "",
                "2026-08-01T00:00:00+00:00", "2026-08-01T00:00:01+00:00",
                "2026-08-01T00:01:00+00:00", "2026-08-01T00:01:00+00:00",
            ),
        )
        connection.execute(
            "INSERT INTO backtest_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "failed", "failed", "failed", json.dumps(failed.model_dump(mode="json")),
                failed.snapshot_hash, "{}", json.dumps({"problem": {"code": "offline"}}),
                "", 40, "执行失败", "", "行情不可用", 0, "",
                "2026-08-01T00:02:00+00:00", "2026-08-01T00:02:01+00:00",
                "2026-08-01T00:02:30+00:00", "2026-08-01T00:02:30+00:00",
            ),
        )
        connection.execute(
            "INSERT INTO backtest_runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "running", "running", "running", json.dumps(running.model_dump(mode="json")),
                running.snapshot_hash, "{}", "{}", "", 30, "加载本地行情", "", "", 0,
                "legacy-worker", "2026-08-01T00:03:00+00:00",
                "2026-08-01T00:03:01+00:00", "2026-08-01T00:03:30+00:00", "",
            ),
        )
        connection.executemany(
            "INSERT INTO backtest_events(run_id,event_json,created_at) VALUES (?,?,?)",
            (
                ("completed", '{"type":"completed","progress":100}', "2026-08-01T00:01:00+00:00"),
                ("running", '{"type":"progress","progress":30}', "2026-08-01T00:03:30+00:00"),
            ),
        )
        connection.commit()


def test_backtest_job_migration_preserves_results_provenance_and_lifecycle(tmp_path):
    _legacy_database(tmp_path)
    migrator = BacktestJobLegacyMigrator()
    assert [(record.record_key, record.outcome) for record in migrator.inspect(tmp_path)] == [
        ("backtests", "review"),
    ]
    assert [record.outcome for record in migrator.migrate_batch(
        tmp_path, after_key="", limit=1,
    )] == ["converted"]

    jobs = UnifiedJobStore(tmp_path / "jobs.sqlite", read_only=True)
    manager = BacktestJobManager(
        BacktestStore(tmp_path / "backtests.sqlite", tmp_path / "backtests", read_only=True),
        runtime=None,
    )
    manager._jobs_path = jobs.path
    completed = manager._project(jobs, jobs.get("completed"), include_artifact=True)
    failed = manager._project(jobs, jobs.get("failed"))
    running = manager._project(jobs, jobs.get("running"))
    assert completed["status"] == "completed"
    assert completed["outcome"] == "completed_with_warnings"
    assert completed["artifact"]["trades"][0]["symbol"] == "600000.SH"
    assert completed["manifest"]["data_quality"]["source"] == "local-cache"
    assert failed["result"]["problem"]["code"] == "offline"
    assert running["status"] == "interrupted"
    assert all(job["type"] == BACKTEST_TASK_TYPE for job in jobs.list(10))
    assert any(event["type"] == "legacy_backtest_progress" for event in jobs.events("running"))
    assert list(migrator.inspect(tmp_path)) == []
    with closing(sqlite3.connect(tmp_path / "backtests.sqlite")) as connection:
        tables = {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
    assert "backtest_results" in tables
    assert not {"backtest_runs", "backtest_events"} & tables


def test_backtest_job_migration_rejects_unknown_status_before_writing(tmp_path):
    _legacy_database(tmp_path)
    with closing(sqlite3.connect(tmp_path / "backtests.sqlite")) as connection:
        connection.execute("UPDATE backtest_runs SET status='future_state' WHERE id='running'")
        connection.commit()
    record = next(iter(BacktestJobLegacyMigrator().migrate_batch(
        tmp_path, after_key="", limit=1,
    )))
    assert record.outcome == "conflict"
    assert "status:future_state" in record.unknown_fields
    assert not (tmp_path / "jobs.sqlite").exists()


def test_backtest_job_migration_rejects_missing_provenance_before_writing(tmp_path):
    _legacy_database(tmp_path)
    with closing(sqlite3.connect(tmp_path / "backtests.sqlite")) as connection:
        manifest = json.loads(connection.execute(
            "SELECT manifest_json FROM backtest_runs WHERE id='completed'"
        ).fetchone()[0])
        manifest.pop("data_quality")
        connection.execute(
            "UPDATE backtest_runs SET manifest_json=? WHERE id='completed'",
            (json.dumps(manifest),),
        )
        connection.commit()
    record = next(iter(BacktestJobLegacyMigrator().migrate_batch(
        tmp_path, after_key="", limit=1,
    )))
    assert record.outcome == "conflict"
    assert "completed:data_quality" in record.unknown_fields
    assert not (tmp_path / "jobs.sqlite").exists()


def test_backtest_job_migration_rejects_lease_and_target_conflicts_before_source_write(
    tmp_path,
):
    _legacy_database(tmp_path)
    with closing(sqlite3.connect(tmp_path / "backtests.sqlite")) as connection:
        connection.execute("UPDATE backtest_runs SET worker='' WHERE id='running'")
        connection.commit()
    migrator = BacktestJobLegacyMigrator()
    lease = next(iter(migrator.migrate_batch(tmp_path, after_key="", limit=1)))
    assert lease.outcome == "conflict"
    assert "running:lease_evidence" in lease.unknown_fields
    assert not (tmp_path / "jobs.sqlite").exists()

    with closing(sqlite3.connect(tmp_path / "backtests.sqlite")) as connection:
        connection.execute("UPDATE backtest_runs SET worker='legacy-worker' WHERE id='running'")
        connection.commit()
    UnifiedJobStore(tmp_path / "jobs.sqlite").import_legacy_job({
        "id": "completed",
        "type": BACKTEST_TASK_TYPE,
        "spec": {"wrong": True},
        "status": "failed",
    })
    target = next(iter(migrator.migrate_batch(tmp_path, after_key="", limit=1)))
    assert target.outcome == "conflict"
    assert "completed:target_collision" in target.unknown_fields
    with closing(sqlite3.connect(tmp_path / "backtests.sqlite")) as connection:
        assert connection.execute("SELECT COUNT(*) FROM backtest_runs").fetchone()[0] == 3


def test_backtest_job_migration_is_idempotent_after_partial_target_import(tmp_path):
    _legacy_database(tmp_path)
    migrator = BacktestJobLegacyMigrator()
    from quantmaster.backtest.job_migration import _convert

    with closing(connecting := sqlite3.connect(tmp_path / "backtests.sqlite")):
        connecting.row_factory = sqlite3.Row
        row = connecting.execute("SELECT * FROM backtest_runs WHERE id='failed'").fetchone()
        record, events, _result = _convert(tmp_path, connecting, row)
    UnifiedJobStore(tmp_path / "jobs.sqlite").import_legacy_job(
        record,
        events=events,
        artifacts=(),
    )

    assert [value.outcome for value in migrator.migrate_batch(
        tmp_path, after_key="", limit=1,
    )] == ["converted"]
    assert len(UnifiedJobStore(tmp_path / "jobs.sqlite").list(10)) == 3
    assert BacktestStore(tmp_path / "backtests.sqlite", tmp_path / "backtests").result(
        "failed"
    )["outcome"] == "failed"


def test_backtest_job_migration_backup_restores_legacy_rows_and_artifacts(tmp_path):
    root = tmp_path / "data"
    _legacy_database(root)
    migrator = BacktestJobLegacyMigrator()
    backup = tmp_path / "backup"
    backup_sqlite_tree(root, backup, extra_paths=migrator.backup_paths)
    list(migrator.migrate_batch(root, after_key="", limit=1))

    migrator.rollback(root, backup)

    assert not (root / "jobs.sqlite").exists()
    with closing(sqlite3.connect(root / "backtests.sqlite")) as connection:
        assert connection.execute(
            "SELECT status FROM backtest_runs WHERE id='completed'"
        ).fetchone()[0] == "completed"
    assert (root / "backtests" / "completed" / "result.json").is_file()
