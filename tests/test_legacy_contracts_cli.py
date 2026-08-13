from __future__ import annotations

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
