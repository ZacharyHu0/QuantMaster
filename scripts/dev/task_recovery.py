"""Durable merge evidence, cleanup retries and local task inventory.

All mutations are called under tasks.py's primary/admin and per-task leases.
No background process, elevated shell or delete-by-age policy is required.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from scripts.dev import tasks

PENDING_STATES = {"checkout_pending_cleanup", "pending_cleanup"}
SHA = re.compile(r"[0-9a-f]{40}")
MAX_AUTOMATIC_ATTEMPTS = 5


def dispatch_recovery(args: argparse.Namespace, primary: Path) -> None:
    if args.command == "finish":
        finish(args.slug, args.pr, merge=args.merge)
    elif args.command == "status":
        print(json.dumps(inventory(primary), ensure_ascii=False, indent=2))
    elif args.command == "retry-cleanup":
        results = retry_cleanup(primary, apply=args.apply, slug=args.slug)
        print(json.dumps(results, ensure_ascii=False, indent=2))
    else:
        raise ValueError(f"Unknown recovery command: {args.command}")


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.stem}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def receipt_path(primary: Path, slug: str) -> Path:
    return primary / ".artifacts" / "task-merges" / f"{slug}.json"


def read_receipt(primary: Path, slug: str) -> dict[str, object] | None:
    path = receipt_path(primary, slug)
    if not path.exists():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise SystemExit("TASK_MERGE_RECEIPT_INVALID: unreadable receipt") from exc
    if not isinstance(value, dict) or value.get("schema") != 1 or value.get("slug") != slug:
        raise SystemExit("TASK_MERGE_RECEIPT_INVALID: schema or slug mismatch")
    if any(not SHA.fullmatch(str(value.get(key, ""))) for key in ("head_sha", "merge_sha", "base_sha")):
        raise SystemExit("TASK_MERGE_RECEIPT_INVALID: missing commit identity")
    return value


def verified_merge(primary: Path, slug: str, branch: str) -> bool:
    receipt = read_receipt(primary, slug)
    if receipt is None:
        return False
    owner, repo = tasks.github_remote_repo(primary)
    head = tasks.git(["rev-parse", f"{branch}^{{commit}}"], cwd=primary, check=False)
    if receipt.get("repository") != f"{owner}/{repo}" or receipt.get("branch") != branch:
        raise SystemExit("TASK_MERGE_RECEIPT_INVALID: repository or branch mismatch")
    if head.returncode or head.stdout.strip() != receipt["head_sha"]:
        raise SystemExit("TASK_HEAD_MOVED: branch differs from merged PR; preserve checkout")
    if tasks.git(
        ["merge-base", "--is-ancestor", str(receipt["merge_sha"]), "main"],
        cwd=primary, check=False,
    ).returncode:
        raise SystemExit("TASK_MAIN_BEHIND: merge receipt is not reachable from local main")
    return True


def github_pr(primary: Path, repository: str, number: int) -> dict:
    result = subprocess.run(
        ["gh", "api", f"repos/{repository}/pulls/{number}"], cwd=primary,
        capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=60,
    )
    if result.returncode:
        raise SystemExit("TASK_GITHUB_UNAVAILABLE: could not read PR; retry finish")
    try:
        value = json.loads(result.stdout)
    except ValueError as exc:
        raise SystemExit("TASK_GITHUB_INVALID: invalid PR response") from exc
    if not isinstance(value, dict):
        raise SystemExit("TASK_GITHUB_INVALID: PR response is not an object")
    return value


def validate_pr(pr: dict, repository: str, slug: str, head: str, number: int) -> None:
    if (
        pr.get("number") != number
        or pr.get("base", {}).get("repo", {}).get("full_name") != repository
        or pr.get("base", {}).get("ref") != "main"
        or (pr.get("head", {}).get("repo") or {}).get("full_name") != repository
        or pr.get("head", {}).get("ref") != f"codex/{slug}"
        or pr.get("head", {}).get("sha") != head
    ):
        raise SystemExit("TASK_PR_MISMATCH: PR must match repository, main, branch and exact head")


def record_merge(primary: Path, slug: str, number: int, pr: dict, repository: str) -> None:
    if not pr.get("merged") or not SHA.fullmatch(str(pr.get("merge_commit_sha", ""))):
        raise SystemExit("TASK_MERGE_PENDING: PR is not merged yet; retry finish")
    write_json(receipt_path(primary, slug), {
        "schema": 1, "slug": slug, "repository": repository, "pr": number,
        "branch": f"codex/{slug}", "head_sha": pr["head"]["sha"],
        "base_sha": pr["base"]["sha"], "merge_sha": pr["merge_commit_sha"],
        "merged_at": pr.get("merged_at"), "recorded_at": datetime.now(UTC).isoformat(),
    })


def merge_ready_pr(primary: Path, slug: str, number: int, repository: str, pr: dict) -> dict:
    target = primary / ".worktrees" / slug
    tasks.preflight_task(primary, slug)
    if tasks.git(["status", "--porcelain"], cwd=target).stdout.strip():
        raise SystemExit("TASK_DIRTY: commit or preserve changes before finish")
    if pr.get("draft") or pr.get("state") != "open":
        raise SystemExit("TASK_PR_NOT_READY: finish requires an open Ready PR")
    head = pr["head"]["sha"]
    if tasks.git(["rev-parse", "HEAD"], cwd=target).stdout.strip() != head:
        raise SystemExit("TASK_HEAD_MOVED: re-read PR before merging")
    # Reuse the established exact-SHA full CI gate. Never mark Ready or use --admin here.
    tasks.ready(target, ui=False, rust=False, package=False, accept_ci=True)
    subprocess.run(
        ["gh", "pr", "merge", str(number), "--repo", repository,
         "--squash", "--match-head-commit", head],
        cwd=primary, check=True, timeout=60,
    )
    merged = github_pr(primary, repository, number)
    validate_pr(merged, repository, slug, head, number)
    return merged


def finish(slug: str, number: int, *, merge: bool = False) -> None:
    """Default records an already merged PR; --merge explicitly requests a new merge."""
    if not tasks.SLUG_PATTERN.fullmatch(slug) or number < 1:
        raise SystemExit("TASK_CONTEXT_INVALID: invalid slug or PR number")
    primary = tasks.primary_root(tasks.ROOT)
    artifacts = primary / ".artifacts" / "worktrees" / slug
    with tasks.task_artifact_lease(artifacts):
        owner, repo = tasks.github_remote_repo(primary)
        repository = f"{owner}/{repo}"
        receipt = read_receipt(primary, slug)
        if receipt is None:
            head = tasks.git(["rev-parse", f"codex/{slug}^{{commit}}"], cwd=primary).stdout.strip()
            pr = github_pr(primary, repository, number)
            validate_pr(pr, repository, slug, head, number)
            if not pr.get("merged") and merge:
                pr = merge_ready_pr(primary, slug, number, repository, pr)
            record_merge(primary, slug, number, pr, repository)
            receipt = read_receipt(primary, slug)
        assert receipt is not None
        if receipt.get("pr") != number or receipt.get("repository") != repository:
            raise SystemExit("TASK_PR_MISMATCH: receipt belongs to another PR or repository")
        # Persist before fetching: an interrupted sync must never repeat the merge.
        if tasks.git(
            ["merge-base", "--is-ancestor", str(receipt["merge_sha"]), "main"],
            cwd=primary, check=False,
        ).returncode:
            tasks.require_primary_control(primary)
            tasks.git(["fetch", "origin", "main"], cwd=primary)
            tasks.git(["merge", "--ff-only", "origin/main"], cwd=primary)
    tasks.remove(slug)
    # Bookkeeping is deliberately a separate existing command. It may post comments;
    # cleanup retries must never generate public messages as a side effect.
    print("[task] finish recorded; run github_sync.py reconcile to preview bookkeeping")


def preserve_deliverables(primary: Path, slug: str) -> None:
    """Move non-disposable outputs outside the deletion root without overwriting anything."""
    source = primary / ".artifacts" / "worktrees" / slug
    if not source.exists():
        return
    destination = primary / ".artifacts" / "task-deliverables" / slug
    if destination.resolve().parent != (primary / ".artifacts" / "task-deliverables").resolve():
        raise SystemExit("TASK_DELIVERABLE_PATH_INVALID: archive escapes managed root")
    for entry in source.iterdir():
        if entry.name in tasks.DISPOSABLE_ARTIFACT_NAMES:
            continue
        destination.mkdir(parents=True, exist_ok=True)
        archived = destination / entry.name
        if archived.exists() or archived.is_symlink():
            raise SystemExit("TASK_DELIVERABLE_CONFLICT: archive exists; preserve both copies")
        # Rename does not follow links and is atomic on this filesystem.
        entry.rename(archived)


def queue_cleanup(primary: Path, slug: str, state: str, error: object) -> None:
    manifest = tasks.ensure_task_manifest(primary, slug)
    attempts = int(manifest.get("cleanup_attempts", 0)) + 1
    delay = timedelta(seconds=min(3600, 30 * 2 ** min(attempts, 7)))
    tasks.update_task_manifest(primary, slug, state=state,
        last_error=tasks.redact_public_text(error), fields={
            "cleanup_attempts": attempts,
            "next_retry_at": (datetime.now(UTC) + delay).isoformat(),
        })


def retry_cleanup(primary: Path, *, apply: bool, slug: str | None = None) -> list[dict[str, object]]:
    if slug is not None and not tasks.SLUG_PATTERN.fullmatch(slug):
        raise SystemExit("TASK_CONTEXT_INVALID: invalid slug")
    root = primary / ".artifacts" / "task-manifests"
    paths = [root / f"{slug}.json"] if slug else sorted(root.glob("*.json"))
    results = []
    for path in paths:
        try:
            manifest = tasks.read_task_manifest(primary, path.stem)
        except (SystemExit, OSError) as exc:
            results.append({"slug": path.stem, "outcome": "invalid_manifest",
                            "reason": tasks.redact_public_text(exc)})
            continue
        if not manifest or manifest["state"] not in PENDING_STATES:
            continue
        due = datetime.fromisoformat(str(manifest.get("next_retry_at") or datetime.now(UTC).isoformat()))
        paused = int(manifest.get("cleanup_attempts", 0)) >= MAX_AUTOMATIC_ATTEMPTS
        outcome = "manual_retry_required" if paused else "waiting"
        if slug or (not paused and due <= datetime.now(UTC)):
            outcome = "eligible"
            if apply:
                try:
                    tasks.remove(path.stem)
                    outcome = str((tasks.read_task_manifest(primary, path.stem) or {})["state"])
                except (SystemExit, RuntimeError, OSError, subprocess.SubprocessError) as exc:
                    queue_cleanup(primary, path.stem, str(manifest["state"]), exc)
                    outcome = "blocked"
        results.append({"slug": path.stem, "outcome": outcome})
    return results


def inventory(primary: Path) -> list[dict[str, object]]:
    """Report all live state, including pre-manifest tasks and orphan removal intents."""
    branches = tasks.worktree_branches(primary)
    slugs = set()
    folders = (".worktrees", ".artifacts/worktrees", ".artifacts/task-manifests", ".artifacts/task-remove")
    for folder in folders:
        root = primary / folder
        if root.is_dir():
            slugs.update(p.stem if p.suffix == ".json" else p.name for p in root.iterdir())
    slugs.update(line.removeprefix("codex/") for line in tasks.git_lines(
        ["for-each-ref", "--format=%(refname:short)", "refs/heads/codex/"], cwd=primary,
    ))
    items = []
    for slug in sorted(slugs):
        if not tasks.SLUG_PATTERN.fullmatch(slug):
            continue
        try:
            items.append(_inventory_item(primary, slug, branches))
        except (SystemExit, OSError, subprocess.SubprocessError) as exc:
            items.append({"slug": slug, "state": "invalid", "reason": tasks.redact_public_text(exc)})
    return items


def _inventory_item(primary: Path, slug: str, branches: dict[Path, str | None]) -> dict[str, object]:
    target = (primary / ".worktrees" / slug).resolve()
    manifest = tasks.read_task_manifest(primary, slug)
    branch_exists = tasks.git(
        ["show-ref", "--verify", "--quiet", f"refs/heads/codex/{slug}"], cwd=primary, check=False,
    ).returncode == 0
    dirty = None
    if target in branches:
        status = tasks.git(["status", "--porcelain"], cwd=target, check=False)
        dirty = bool(status.stdout.strip()) if status.returncode == 0 else None
    return {
        "slug": slug, "state": manifest["state"] if manifest else "legacy_unclassified",
        "registered": target in branches, "checkout_exists": target.exists(),
        "branch_exists": branch_exists, "dirty": dirty,
        "branch_matches": branches.get(target) == f"refs/heads/codex/{slug}",
        "artifacts_exist": (primary / ".artifacts/worktrees" / slug).exists(),
        "remove_intent": tasks.task_remove_intent_path(primary, slug).exists(),
        "merge_receipt": receipt_path(primary, slug).exists(),
        "last_error": (manifest or {}).get("last_error", ""),
        "cleanup_attempts": (manifest or {}).get("cleanup_attempts", 0),
        "next_retry_at": (manifest or {}).get("next_retry_at"),
    }
