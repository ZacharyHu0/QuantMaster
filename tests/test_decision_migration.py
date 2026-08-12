"""Explicit decision migration; readers never perform these conversions."""

from __future__ import annotations

import json
import sqlite3

import pytest

from quantmaster.decision import DecisionStore, migrate_decision_snapshots


def _insert(store: DecisionStore, *, payload: object, signal_date: str = "2024-01-02",
            universe: str = "demo", horizon: int = 3, profile: str = "legacy-key",
            policy_hash: str = "legacy-key", model_version: str = "") -> None:
    with store._conn() as connection:
        connection.execute(
            "INSERT INTO selection_snapshots "
            "(signal_date,universe,horizon,profile,policy_hash,model_version,payload,created_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (
                signal_date, universe, horizon, profile, policy_hash, model_version,
                payload if isinstance(payload, str) else json.dumps(payload), 1_704_153_600.0,
            ),
        )


def test_decision_migration_backfills_row_facts_and_leaves_ambiguous_optional_empty(tmp_path):
    path = tmp_path / "decisions.sqlite"
    store = DecisionStore(path)
    _insert(store, payload={"picks": [{"symbol": "A"}], "mystery_score": 42})

    dry = migrate_decision_snapshots(path)
    assert dry["migrated"] == 1
    assert dry["diagnostics"][0]["diagnostic_code"] == (
        "decision_payload_migrated_with_optional_empty"
    )
    assert dry["diagnostics"][0]["unknown_fields"] == ["mystery_score"]
    with pytest.raises(RuntimeError, match="一次性迁移"):
        store.history()

    backup = tmp_path / "backup" / "decisions.sqlite"
    applied = migrate_decision_snapshots(path, dry_run=False, backup_path=backup)
    assert backup.is_file()
    assert applied["migrated"] == 1
    snapshot = store.history()[0]
    assert snapshot["signal_date"] == "2024-01-02"
    assert snapshot["universe"] == "demo"
    assert snapshot["holding_horizon_days"] == 3
    assert snapshot["created_at"] == "2024-01-02T00:00:00+00:00"
    assert snapshot["profile"] is None
    assert snapshot["policy_hash"] is None
    assert snapshot["model_version"] is None
    assert "model_snapshot" not in snapshot
    assert "mystery_score" not in snapshot
    with sqlite3.connect(path) as connection:
        audit = connection.execute(
            "SELECT status,diagnostic_code,unknown_fields_json FROM decision_migration_audit"
        ).fetchone()
    assert audit[:2] == ("migrated", "decision_payload_migrated_with_optional_empty")
    assert json.loads(audit[2]) == {"mystery_score": 42}


def test_decision_migration_is_idempotent_after_apply(tmp_path):
    path = tmp_path / "decisions.sqlite"
    store = DecisionStore(path)
    _insert(store, payload={"picks": []})
    migrate_decision_snapshots(
        path, dry_run=False, backup_path=tmp_path / "first.sqlite", batch_size=1,
    )

    rerun = migrate_decision_snapshots(path)
    assert rerun["migrated"] == 0
    assert rerun["unchanged"] == 1
    reapplied = migrate_decision_snapshots(
        path, dry_run=False, backup_path=tmp_path / "second.sqlite", batch_size=1,
    )
    assert reapplied["migrated"] == 0
    assert reapplied["unchanged"] == 1
    assert store.history()[0]["picks"] == []


def test_decision_migration_does_not_reclassify_invalid_current_or_conflicting_old(tmp_path):
    path = tmp_path / "decisions.sqlite"
    store = DecisionStore(path)
    _insert(store, payload={
        "decision_schema_version": 1,
        "signal_date": "2024-01-02",
        "universe": "other",
        "holding_horizon_days": 3,
        "created_at": "2024-01-02T00:00:00+00:00",
        "picks": [],
    })
    _insert(
        store, payload={"signal_date": "2099-01-01", "picks": []},
        signal_date="2024-01-03",
    )
    _insert(store, payload="{broken", signal_date="2024-01-04")

    report = migrate_decision_snapshots(path)
    assert report["migrated"] == 0
    assert report["conflict"] == 2
    assert report["unclassified"] == 1
    assert {item["diagnostic_code"] for item in report["diagnostics"]} == {
        "decision_payload_row_identity_mismatch",
        "decision_payload_identity_conflict",
        "decision_payload_invalid_json",
    }


def test_apply_requires_recoverable_backup(tmp_path):
    path = tmp_path / "decisions.sqlite"
    DecisionStore(path)
    with pytest.raises(ValueError, match="backup_path"):
        migrate_decision_snapshots(path, dry_run=False)
