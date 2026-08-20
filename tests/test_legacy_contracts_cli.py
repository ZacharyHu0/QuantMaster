from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from scripts.maintenance import legacy_contracts


def _args(root: Path, backup: Path | None = None) -> list[str]:
    values = [
        "status", "--data-root", str(root), "--confirm-root", str(root),
        "--writer-stopped-evidence", "test writer stopped",
    ]
    if backup is not None:
        values.extend(("--backup-root", str(backup), "--confirm-backup-root", str(backup)))
    return values


def test_external_backup_root_is_resolved_and_injected(tmp_path, monkeypatch, capsys):
    root = tmp_path / "data"
    external = tmp_path / "external" / "quantmaster-backups"
    root.mkdir(exist_ok=True)
    captured = {}

    class Manager:
        ACTIVE = frozenset()

        def __init__(self, value, **kwargs):
            captured.update(root=Path(value), **kwargs)

        @staticmethod
        def latest():
            return None

    monkeypatch.setattr(legacy_contracts, "LegacyMigrationManager", Manager)
    assert legacy_contracts.main(_args(root, external)) == 0
    assert captured["root"] == root.resolve()
    assert captured["backup_root"] == external.resolve()
    assert capsys.readouterr().out.strip() == "null"


def test_external_backup_requires_exact_confirmation(tmp_path):
    root = tmp_path / "data"
    root.mkdir(exist_ok=True)
    args = _args(root)
    args.extend(("--backup-root", str(tmp_path / "one"), "--confirm-backup-root", str(tmp_path / "two")))
    with pytest.raises(SystemExit, match="exactly match"):
        legacy_contracts.main(args)


@pytest.mark.parametrize("backup", (Path("data"), Path("data") / "nested"))
def test_external_backup_rejects_data_tree(tmp_path, backup):
    root = (tmp_path / "data").resolve()
    root.mkdir(exist_ok=True)
    candidate = (tmp_path / backup).resolve()
    with pytest.raises(SystemExit, match="outside"):
        legacy_contracts.main(_args(root, candidate))


def test_external_backup_rejects_drive_or_filesystem_root(tmp_path):
    root = (tmp_path / "data").resolve()
    root.mkdir(exist_ok=True)
    broad = Path(root.anchor)
    with pytest.raises(SystemExit, match="drive/filesystem root"):
        legacy_contracts.main(_args(root, broad))


def test_plan_reports_confirmed_roots_without_starting_migration(
    tmp_path, monkeypatch, capsys,
):
    root = (tmp_path / "confirmed-data").resolve()
    stockdb = (tmp_path / "stockdb").resolve()
    backup = (tmp_path / "external" / "backups").resolve()
    root.mkdir()
    captured = {}

    class Manager:
        def __init__(self, value, **kwargs):
            captured.update(root=Path(value), **kwargs)

        @staticmethod
        def plan(domain):
            return {"domain": domain, "required_backup_bytes": 123}

    monkeypatch.setattr(legacy_contracts, "LegacyMigrationManager", Manager)
    args = [
        "plan", "--domain", "fixture", "--data-root", str(root),
        "--confirm-root", str(root), "--stockdb-root", str(stockdb),
        "--confirm-stockdb-root", str(stockdb), "--backup-root", str(backup),
        "--confirm-backup-root", str(backup),
    ]

    assert legacy_contracts.main(args) == 0
    assert captured["root"] == root
    assert captured["stockdb_root"] == stockdb
    assert captured["backup_root"] == backup
    assert json.loads(capsys.readouterr().out)["required_backup_bytes"] == 123


def test_apply_requires_reviewed_plan_and_writer_stop_evidence(tmp_path):
    root = (tmp_path / "confirmed-data").resolve()
    stockdb = (tmp_path / "stockdb").resolve()
    backup = (tmp_path / "external" / "backups").resolve()
    root.mkdir()
    args = [
        "apply", "--domain", "fixture", "--data-root", str(root),
        "--confirm-root", str(root), "--stockdb-root", str(stockdb),
        "--confirm-stockdb-root", str(stockdb), "--backup-root", str(backup),
        "--confirm-backup-root", str(backup),
    ]

    with pytest.raises(SystemExit, match="writer-stopped-evidence"):
        legacy_contracts.main(args)
    with pytest.raises(SystemExit, match="accept-plan"):
        legacy_contracts.main([*args, "--writer-stopped-evidence", "stopped"])


def test_plan_registers_startup_schema_factories_in_clean_process(tmp_path):
    root = (tmp_path / "data").resolve()
    stockdb = (tmp_path / "stockdb").resolve()
    backup = (tmp_path / "external" / "backups").resolve()
    root.mkdir(exist_ok=True)
    stockdb.mkdir(exist_ok=True)
    with sqlite3.connect(root / "paper.sqlite") as connection:
        connection.executescript("""
            CREATE TABLE paper_accounts(id TEXT);
            CREATE TABLE paper_cycles(id TEXT);
            CREATE TABLE paper_orders(id TEXT);
            CREATE TABLE paper_auto_runs(id TEXT);
            PRAGMA user_version=0;
        """)
    args = [
        "plan", "--domain", "startup-schema", "--data-root", str(root),
        "--confirm-root", str(root), "--stockdb-root", str(stockdb),
        "--confirm-stockdb-root", str(stockdb), "--backup-root", str(backup),
        "--confirm-backup-root", str(backup),
    ]
    source = (
        "from scripts.maintenance import legacy_contracts; "
        f"raise SystemExit(legacy_contracts.main({args!r}))"
    )
    environment = {**os.environ, "PYTHONIOENCODING": "utf-8"}
    result = subprocess.run(
        [sys.executable, "-c", source],
        cwd=Path(__file__).resolve().parents[1],
        env=environment,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr.decode("utf-8")
    report = json.loads(result.stdout.decode("utf-8"))
    assert report["domain"] == "startup-schema"
    assert report["conflicts"] == []
    assert report["migration_evidence"] == [{
        "record_key": "schema:paper",
        "outcome": "review",
        "diagnostic_code": "startup_schema_upgrade_required",
        "unknown_fields": [],
        "detail": "paper schema 需要显式升级或人工确认",
    }]
    assert not backup.exists()
