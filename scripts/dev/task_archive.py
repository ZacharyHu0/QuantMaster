"""Owner-requested archival of unfinished tasks before managed removal."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import zipfile
from pathlib import Path

from scripts.dev import tasks
from scripts.dev.task_recovery import queue_cleanup, write_json


def archive_root(primary: Path, slug: str) -> Path:
    return primary / ".artifacts" / "task-archives" / slug


def file_hash(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def checkout_files(target: Path) -> dict[str, Path]:
    """Never follow checkout links into another task or the primary interpreter."""
    result = {}
    if not target.exists():
        return result
    for directory, dirs, files in os.walk(target, followlinks=False, onerror=_raise_walk_error):
        parent = Path(directory)
        for name in dirs + files:
            path = parent / name
            relative = path.relative_to(target).as_posix()
            if relative == ".git":
                if name in dirs:
                    raise SystemExit("TASK_ARCHIVE_UNSUPPORTED: embedded Git directory")
                continue
            if path.is_symlink() or path.is_junction():
                raise SystemExit("TASK_ARCHIVE_UNSUPPORTED: checkout contains a link; preserve it")
            if name in files:
                result[relative] = path
    return result


def _raise_walk_error(error: OSError) -> None:
    raise error


def verify_archive(primary: Path, slug: str, *, allow_partial: bool) -> dict:
    root = archive_root(primary, slug)
    receipt = json.loads((root / "receipt.json").read_text(encoding="utf-8"))
    branch = f"codex/{slug}"
    if receipt.get("schema") != 1 or receipt.get("slug") != slug or receipt.get("branch") != branch:
        raise SystemExit("TASK_ARCHIVE_INVALID: receipt identity mismatch")
    bundle = root / "history.bundle"
    snapshot = root / "checkout.zip"
    if file_hash(bundle) != receipt["bundle_sha256"] or file_hash(snapshot) != receipt["snapshot_sha256"]:
        raise SystemExit("TASK_ARCHIVE_INVALID: archive checksum mismatch")
    tasks.git(["bundle", "verify", str(bundle)], cwd=primary)
    heads = tasks.git(["bundle", "list-heads", str(bundle)], cwd=primary).stdout.splitlines()
    if f"{receipt['head']} refs/heads/{branch}" not in heads:
        raise SystemExit("TASK_ARCHIVE_INVALID: bundle is missing exact task head")
    current = tasks.git(["rev-parse", "--verify", f"refs/heads/{branch}"], cwd=primary, check=False)
    if current.returncode == 0 and current.stdout.strip() != receipt["head"]:
        raise SystemExit("TASK_HEAD_MOVED: archive no longer matches the task branch")
    target = primary / ".worktrees" / slug
    files = checkout_files(target)
    if not allow_partial and set(files) != set(receipt["files"]):
        raise SystemExit("TASK_ARCHIVE_CHANGED: checkout file set changed during archival")
    verify_snapshot(snapshot, receipt["files"])
    for name, path in files.items():
        if name not in receipt["files"] or file_hash(path) != receipt["files"][name]:
            raise SystemExit("TASK_ARCHIVE_CHANGED: new or changed content must be preserved")
    return receipt


def verify_snapshot(snapshot: Path, expected: dict[str, str]) -> None:
    with zipfile.ZipFile(snapshot) as archive:
        if set(archive.namelist()) != set(expected) or archive.testzip() is not None:
            raise SystemExit("TASK_ARCHIVE_INVALID: snapshot contents mismatch")
        for name, digest in expected.items():
            if hashlib.sha256(archive.read(name)).hexdigest() != digest:
                raise SystemExit("TASK_ARCHIVE_INVALID: snapshot file checksum mismatch")


def archive_task(primary: Path, slug: str) -> None:
    """Explicit command; backup is verified before any checkout/branch deletion."""
    if not tasks.SLUG_PATTERN.fullmatch(slug):
        raise SystemExit("TASK_CONTEXT_INVALID: invalid slug")
    target = primary / ".worktrees" / slug
    root = archive_root(primary, slug)
    if target.resolve() != (primary / ".worktrees").resolve() / slug:
        raise SystemExit("TASK_CONTEXT_INVALID: checkout escapes its task root")
    if root.resolve() != (primary / ".artifacts/task-archives").resolve() / slug:
        raise SystemExit("TASK_CONTEXT_INVALID: archive escapes its managed root")
    with tasks.task_artifact_lease(primary / ".artifacts/worktrees" / slug):
        if not (root / "receipt.json").exists():
            _create_archive(primary, slug, target, root)
        verify_archive(primary, slug, allow_partial=True)
    tasks.remove(slug)


def _create_archive(primary: Path, slug: str, target: Path, root: Path) -> None:
    branch = f"codex/{slug}"
    branches = tasks.worktree_branches(primary)
    if target in branches:
        if branches[target] != f"refs/heads/{branch}":
            raise SystemExit("TASK_CONTEXT_INVALID: archive checkout branch mismatch")
        tasks.require_no_git_operation(target)
    elif target.exists():
        raise SystemExit("TASK_CONTEXT_INVALID: unregistered checkout needs separate evidence")
    head = tasks.git(["rev-parse", f"refs/heads/{branch}"], cwd=primary).stdout.strip()
    files = checkout_files(target)
    root.mkdir(parents=True, exist_ok=True)
    bundle = root / "history.bundle"
    tasks.git(["bundle", "create", str(bundle), branch], cwd=primary)
    digests = {}
    snapshot = root / "checkout.zip"
    with zipfile.ZipFile(snapshot, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True) as archive:
        for name, path in sorted(files.items()):
            archive.write(path, name)
            digests[name] = file_hash(path)
    payload = {
        "schema": 1, "slug": slug, "branch": branch, "head": head,
        "bundle_sha256": file_hash(bundle), "snapshot_sha256": file_hash(snapshot), "files": digests,
        "disposition": "archived_unmerged", "cleanup_started": False,
    }
    write_json(root / "receipt.json", payload)
    verify_archive(primary, slug, allow_partial=False)


def remove_archived(primary: Path, slug: str) -> None:
    root = archive_root(primary, slug)
    raw = json.loads((root / "receipt.json").read_text(encoding="utf-8"))
    receipt = verify_archive(primary, slug, allow_partial=bool(raw.get("cleanup_started")))
    target = primary / ".worktrees" / slug
    branch = f"codex/{slug}"
    branches = tasks.worktree_branches(primary)
    if target in branches:
        if branches[target] != f"refs/heads/{branch}":
            raise SystemExit("TASK_CONTEXT_INVALID: archive checkout branch mismatch")
        tasks.require_no_git_operation(target)
        # Status is read immediately before Git writes; dirty bytes are in the verified snapshot.
        tasks.git(["status", "--short"], cwd=target)
    receipt["cleanup_started"] = True
    write_json(root / "receipt.json", receipt)
    tasks.update_task_manifest(primary, slug, state="checkout_pending_cleanup",
                               fields={"disposition": "archived_unmerged"})
    try:
        if target in branches:
            result = tasks.git(["worktree", "remove", "--force", str(target)], cwd=primary, check=False)
            if result.returncode and target in tasks.registered_worktrees(primary):
                raise OSError("TASK_ARCHIVE_CHECKOUT_PENDING: Git could not remove archived checkout")
        if target.exists():
            verify_archive(primary, slug, allow_partial=True)
            tasks._remove_verified_tree(
                target.resolve(), expected_parent=(primary / ".worktrees").resolve(),
                scope="archived checkout", error_code=tasks.TASK_CHECKOUT_ACL_UNRECOVERABLE,
                retry=f"tasks.py remove {slug}", retained="verified archive retained",
            )
        if tasks.git(["show-ref", "--verify", "--quiet", f"refs/heads/{branch}"],
                     cwd=primary, check=False).returncode == 0:
            tasks.git(["branch", "-D", branch], cwd=primary)
    except (OSError, subprocess.SubprocessError, SystemExit) as exc:
        queue_cleanup(primary, slug, "checkout_pending_cleanup", exc)
        return
    tasks.update_task_manifest(primary, slug, state="pending_cleanup")
    try:
        tasks.remove_task_artifacts(primary, slug)
    except (OSError, SystemExit) as exc:
        queue_cleanup(primary, slug, "pending_cleanup", exc)
        return
    tasks.task_remove_intent_path(primary, slug).unlink(missing_ok=True)
    tasks.update_task_manifest(primary, slug, state="removed",
                               fields={"cleanup_attempts": 0, "next_retry_at": None})
    print(f"[task] archived and removed {slug}; history and checkout snapshot retained")
