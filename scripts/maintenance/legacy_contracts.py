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
    parser.add_argument("action", choices=("plan", "apply", "resume", "rollback", "status"))
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--domain")
    parser.add_argument("--run-id")
    parser.add_argument("--batch-size", type=int, default=250)
    parser.add_argument("--confirm-root", required=True, type=Path)
    parser.add_argument("--stockdb-root", type=Path)
    parser.add_argument("--confirm-stockdb-root", type=Path)
    parser.add_argument("--backup-root", type=Path)
    parser.add_argument("--confirm-backup-root", type=Path)
    parser.add_argument("--writer-stopped-evidence", default="")
    parser.add_argument("--accept-plan", action="store_true")
    return parser


def _confirmed_root(value: Path, confirmed: Path, label: str) -> Path:
    if not value.is_absolute() or not confirmed.is_absolute():
        raise SystemExit(f"{label} requires absolute paths")
    resolved = value.resolve()
    if resolved != confirmed.resolve():
        raise SystemExit(f"confirm {label} must exactly match {label}")
    return resolved


def _resolved_backup_root(args: argparse.Namespace, root: Path) -> Path | None:
    if args.backup_root is None and args.confirm_backup_root is None:
        return None
    if args.backup_root is None or args.confirm_backup_root is None:
        raise SystemExit("external backup requires both --backup-root and --confirm-backup-root")
    backup_root = _confirmed_root(
        args.backup_root, args.confirm_backup_root, "--backup-root",
    )
    if backup_root == Path(backup_root.anchor):
        raise SystemExit("--backup-root cannot be a drive/filesystem root")
    if backup_root == root or root in backup_root.parents or backup_root in root.parents:
        raise SystemExit("external --backup-root must be outside and disjoint from --data-root")
    return backup_root


def _resolved_stockdb_root(args: argparse.Namespace, root: Path) -> Path:
    if args.stockdb_root is None or args.confirm_stockdb_root is None:
        raise SystemExit(
            "plan/apply requires both --stockdb-root and --confirm-stockdb-root"
        )
    stockdb = _confirmed_root(
        args.stockdb_root, args.confirm_stockdb_root, "--stockdb-root",
    )
    if stockdb == root or root in stockdb.parents or stockdb in root.parents:
        raise SystemExit("--stockdb-root must be outside and disjoint from --data-root")
    return stockdb


def _execute(manager: LegacyMigrationManager, args: argparse.Namespace) -> dict | None:
    if args.action == "plan":
        if not args.domain:
            raise SystemExit("plan requires --domain")
        print(json.dumps(manager.plan(args.domain), ensure_ascii=False, indent=2))
        return None
    if args.action == "apply":
        if not args.domain:
            raise SystemExit("apply requires --domain")
        if not args.accept_plan:
            raise SystemExit("apply requires --accept-plan after reviewing plan output")
        print(json.dumps({"preflight": manager.plan(args.domain)}, ensure_ascii=False, indent=2))
        sys.stdout.flush()
        return manager.create(args.domain, mode="apply", batch_size=args.batch_size)
    if args.action == "resume":
        if not args.run_id:
            raise SystemExit("resume requires --run-id")
        return manager.resume(args.run_id, batch_size=args.batch_size)
    if args.action == "rollback":
        if not args.run_id:
            raise SystemExit("rollback requires --run-id")
        print(json.dumps(manager.rollback(args.run_id), ensure_ascii=False, indent=2))
        return None
    task = manager.get(args.run_id) if args.run_id else manager.latest()
    print(json.dumps(task, ensure_ascii=False, indent=2))
    return None


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    root = _confirmed_root(args.data_root, args.confirm_root, "--data-root")
    backup_root = _resolved_backup_root(args, root)
    if args.action in {"plan", "apply"} and backup_root is None:
        raise SystemExit("plan/apply requires an explicitly confirmed external backup root")
    stockdb_root = (
        _resolved_stockdb_root(args, root)
        if args.action in {"plan", "apply"} else None
    )
    if args.action in {"apply", "resume", "rollback"}:
        if not args.writer_stopped_evidence.strip():
            raise SystemExit("write actions require --writer-stopped-evidence")
    evidence = OfflineMaintenanceEvidence(root, True, args.writer_stopped_evidence)
    manager = LegacyMigrationManager(
        root, backup_root=backup_root, stockdb_root=stockdb_root,
        offline_evidence=evidence,
    )
    task = _execute(manager, args)
    if task is None:
        return 0
    while task["status"] in manager.ACTIVE:
        time.sleep(0.1)
        task = manager.get(task["id"])
    print(json.dumps(task, ensure_ascii=False, indent=2))
    return 0 if task["status"] == "completed" else 1


if __name__ == "__main__":
    sys.exit(main())
