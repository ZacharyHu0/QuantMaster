"""Offline legacy-contract migration with explicit writer-stop evidence."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from quantmaster.data.legacy_migration import (
    LegacyMigrationManager,
    OfflineMaintenanceEvidence,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("apply", "resume", "rollback", "status"))
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--domain")
    parser.add_argument("--run-id")
    parser.add_argument("--batch-size", type=int, default=250)
    parser.add_argument("--confirm-root", required=True, type=Path)
    parser.add_argument("--backup-root", type=Path)
    parser.add_argument("--confirm-backup-root", type=Path)
    parser.add_argument("--writer-stopped-evidence", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root, confirmed = args.data_root.resolve(), args.confirm_root.resolve()
    if root != confirmed:
        raise SystemExit("--confirm-root must exactly match --data-root")
    backup_root = None
    if args.backup_root is not None or args.confirm_backup_root is not None:
        if args.backup_root is None or args.confirm_backup_root is None:
            raise SystemExit("external backup requires both --backup-root and --confirm-backup-root")
        backup_root = args.backup_root.resolve()
        confirmed_backup = args.confirm_backup_root.resolve()
        if backup_root != confirmed_backup:
            raise SystemExit("--confirm-backup-root must exactly match --backup-root")
        if backup_root == Path(backup_root.anchor):
            raise SystemExit("--backup-root cannot be a drive/filesystem root")
        if backup_root == root or root in backup_root.parents or backup_root in root.parents:
            raise SystemExit("external --backup-root must be outside and disjoint from --data-root")
    evidence = OfflineMaintenanceEvidence(confirmed, True, args.writer_stopped_evidence)
    manager = LegacyMigrationManager(
        root, backup_root=backup_root, offline_evidence=evidence,
    )
    task: dict | None
    if args.action == "apply":
        if not args.domain:
            raise SystemExit("apply requires --domain")
        task = manager.create(args.domain, mode="apply", batch_size=args.batch_size)
    elif args.action == "resume":
        if not args.run_id:
            raise SystemExit("resume requires --run-id")
        task = manager.resume(args.run_id, batch_size=args.batch_size)
    elif args.action == "rollback":
        if not args.run_id:
            raise SystemExit("rollback requires --run-id")
        print(json.dumps(manager.rollback(args.run_id), ensure_ascii=False, indent=2))
        return 0
    else:
        task = manager.get(args.run_id) if args.run_id else manager.latest()
        print(json.dumps(task, ensure_ascii=False, indent=2))
        return 0
    assert task is not None
    while task["status"] in manager.ACTIVE:
        time.sleep(0.1)
        task = manager.get(task["id"])
    print(json.dumps(task, ensure_ascii=False, indent=2))
    return 0 if task["status"] == "completed" else 1


if __name__ == "__main__":
    sys.exit(main())
