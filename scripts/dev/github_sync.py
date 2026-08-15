"""Reconcile QuantMaster GitHub task bookkeeping.

The default run is read-only and exits non-zero when findings need attention.
``--apply`` executes only safe, idempotent fixes and reports anything that still
requires an owner or maintainer decision.

Checks:

* a merged PR whose ``Closes #<issue>`` reference is still open;
* duplicate issues: identical normalized titles among open issues, or an open issue whose
  title already has a completed closed issue;
* Draft PRs that have not been updated within the configured threshold.

Safe fixes under ``--apply``:

* close a merged-but-open issue and leave a comment (skipped when the issue has
  the ``blocked`` label);
* mark the newer duplicate issue ``duplicate``, comment, and close it;
* comment on a stale Draft PR asking for a status update.

Everything else is reported as a suggestion and never changed automatically.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

from quantmaster.logging_config import redact_sensitive_text

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_STALE_HOURS = 48
HTTP_PER_PAGE = 100


def run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command, cwd=ROOT, check=check, capture_output=True, text=True,
        encoding="utf-8", errors="replace",
    )


def repo_from_remote() -> tuple[str, str]:
    result = run(["git", "remote", "get-url", "origin"], check=False)
    if result.returncode:
        raise SystemExit("无法读取 origin URL；请使用 --repo owner/name 显式指定仓库")
    url = result.stdout.strip()
    match = re.match(
        r"(?:https?://(?:[^@/]+@)?github\.com/|git@github\.com:)"
        r"([^/]+)/([^/]+?)(?:\.git)?/?$",
        url,
    )
    if not match:
        raise SystemExit(f"无法从 origin URL 解析 GitHub owner/repo：{url}")
    return match.group(1), match.group(2)


def gh_json(owner: str, repo: str, args: list[str], fields: str) -> list[dict]:
    command = [
        "gh", *args, "--repo", f"{owner}/{repo}",
        "--json", fields,
    ]
    result = run(command, check=False)
    if result.returncode:
        raise SystemExit(
            f"gh 调用失败：{' '.join(command)}\n{result.stderr.strip() or result.stdout.strip()}"
        )
    try:
        payload = json.loads(result.stdout or "[]")
    except json.JSONDecodeError as exc:
        raise SystemExit(f"gh 返回无法解析：{result.stdout[:200]}") from exc
    return payload


def issue_lookup(payload: list[dict]) -> dict[int, dict]:
    return {int(item["number"]): item for item in payload}


def normalized_title(title: str) -> str:
    return re.sub(r"\s+", " ", title.strip().lower())


def assert_public_github_body(body: str) -> None:
    """Fail closed before a comment can publish a path or credential."""
    if redact_sensitive_text(body) != body:
        raise ValueError("拒绝发送包含本地路径或敏感信息的 GitHub 文本")


def comment_issue(owner: str, repo: str, number: int, body: str) -> None:
    assert_public_github_body(body)
    result = run(
        [
            "gh", "issue", "comment", str(number),
            "--repo", f"{owner}/{repo}", "--body", body,
        ],
        check=False,
    )
    if result.returncode:
        print(f"  ! 评论失败 issue #{number}: {result.stderr.strip() or result.stdout.strip()}")


def close_issue(owner: str, repo: str, number: int, reason: str = "completed") -> None:
    result = run(
        [
            "gh", "api", "-X", "PATCH", f"repos/{owner}/{repo}/issues/{number}",
            "-f", "state=closed", "-f", f"state_reason={reason}",
        ],
        check=False,
    )
    if result.returncode:
        print(f"  ! 关闭失败 issue #{number}: {result.stderr.strip() or result.stdout.strip()}")


def comment_pr(owner: str, repo: str, number: int, body: str) -> None:
    assert_public_github_body(body)
    result = run(
        [
            "gh", "pr", "comment", str(number),
            "--repo", f"{owner}/{repo}", "--body", body,
        ],
        check=False,
    )
    if result.returncode:
        print(f"  ! 评论失败 PR #{number}: {result.stderr.strip() or result.stdout.strip()}")


def ensure_duplicate_label(owner: str, repo: str) -> None:
    run(
        [
            "gh", "label", "create", "duplicate",
            "--repo", f"{owner}/{repo}",
            "--description", "Duplicate of an existing issue",
            "--color", "ededed",
        ],
        check=False,
    )


def add_duplicate_label(owner: str, repo: str, number: int) -> None:
    run(
        [
            "gh", "issue", "edit", str(number),
            "--repo", f"{owner}/{repo}", "--add-label", "duplicate",
        ],
        check=False,
    )


def find_duplicates(issues: dict[int, dict]) -> list[tuple[int, int, str]]:
    grouped: dict[str, list[int]] = {}
    for number, item in issues.items():
        grouped.setdefault(normalized_title(item.get("title", "")), []).append(number)
    duplicates: list[tuple[int, int, str]] = []
    for title, numbers in grouped.items():
        if len(numbers) < 2:
            continue
        ordered = sorted(numbers)
        for duplicate in ordered[1:]:
            duplicates.append((ordered[0], duplicate, title))
    return duplicates


def find_closed_duplicates(
    issues: dict[int, dict], closed_issues: dict[int, dict],
) -> list[tuple[int, int, str]]:
    """Open issues whose normalized title already has a completed closed issue."""
    closed_by_title: dict[str, int] = {}
    for number, item in closed_issues.items():
        if item.get("stateReason") != "COMPLETED":
            continue
        closed_by_title.setdefault(normalized_title(item.get("title", "")), number)
    duplicates: list[tuple[int, int, str]] = []
    for number, item in issues.items():
        title = normalized_title(item.get("title", ""))
        if title in closed_by_title and closed_by_title[title] != number:
            duplicates.append((closed_by_title[title], number, item.get("title", "")))
    return duplicates


def issue_labels(issue: dict) -> set[str]:
    return {label.get("name", "") for label in issue.get("labels") or []}


def fix_duplicate(
    owner: str, repo: str, duplicate: int, body: str, dry_run: bool,
) -> None:
    print(
        f"  -> {'[dry-run]' if dry_run else 'apply'}: "
        f"加 duplicate 标签、评论并关闭 #{duplicate}"
    )
    if dry_run:
        return
    ensure_duplicate_label(owner, repo)
    add_duplicate_label(owner, repo, duplicate)
    comment_issue(owner, repo, duplicate, body)
    close_issue(owner, repo, duplicate, reason="not_planned")


def reconcile_merged_issues(
    owner: str, repo: str, merged: list[dict], issues: dict[int, dict], dry_run: bool,
) -> tuple[int, int, int]:
    findings = 0
    applied = 0
    blocked = 0
    for pr in merged:
        number = int(pr.get("number") or 0)
        for reference in pr.get("closingIssuesReferences") or []:
            issue_number = int(reference.get("number") or 0)
            issue = issues.get(issue_number)
            if issue is None:
                continue
            findings += 1
            title = issue.get("title", "")
            print(f"- PR #{number} 已合并，但 issue #{issue_number}「{title}」仍 open")
            if "blocked" in issue_labels(issue):
                blocked += 1
                print("  = 带 blocked 标签，需要 owner 决策；脚本不会关闭。")
                continue
            print(f"  -> {'[dry-run]' if dry_run else 'apply'}: 评论并关闭 issue #{issue_number}")
            if dry_run:
                continue
            comment_issue(owner, repo, issue_number, f"已由合并的 PR #{number} 完成；自动关闭。")
            close_issue(owner, repo, issue_number)
            applied += 1
    return findings, applied, blocked


def reconcile_duplicate_issues(
    owner: str, repo: str, issues: dict[int, dict],
    closed_issues: dict[int, dict], dry_run: bool,
) -> tuple[int, int, int]:
    findings = 0
    applied = 0
    blocked = 0
    pairs = find_duplicates(issues) + find_closed_duplicates(issues, closed_issues)
    for original, duplicate, title in pairs:
        if "blocked" in issue_labels(issues[duplicate]):
            blocked += 1
            print(
                f"- open issue #{duplicate} 与 issue #{original} 标题相同，"
                f"但带 blocked 标签：「{title}」；需要 owner 决策。"
            )
            continue
        findings += 1
        print(f"- open issue #{duplicate} 与 issue #{original} 标题相同：「{title}」")
        fix_duplicate(
            owner, repo, duplicate,
            f"Duplicate of #{original}; closing this record.",
            dry_run,
        )
        if not dry_run:
            applied += 1
    return findings, applied, blocked


def reconcile_stale_drafts(
    owner: str, repo: str, pulls: list[dict], stale_hours: int, dry_run: bool,
) -> tuple[int, int]:
    findings = 0
    applied = 0
    stale_cutoff = datetime.now(UTC) - timedelta(hours=stale_hours)
    for pr in pulls:
        if not pr.get("isDraft"):
            continue
        updated = pr.get("updatedAt")
        if not updated:
            continue
        try:
            updated_at = datetime.fromisoformat(updated.replace("Z", "+00:00"))
        except ValueError:
            continue
        if updated_at >= stale_cutoff:
            continue
        number = int(pr.get("number") or 0)
        findings += 1
        print(
            f"- Draft PR #{number}「{pr.get('title', '')}」自 {updated_at:%Y-%m-%d %H:%M}Z 未更新"
        )
        body = (
            "该 Draft PR 超过 "
            f"{stale_hours} 小时未更新。请二选一：标记为 Ready 并完成 integration gate，"
            "或说明阻塞原因、把 Issue/Project 置为 Blocked 并给出解除条件。"
        )
        print(f"  -> {'[dry-run]' if dry_run else 'apply'}: 在 PR 评论提醒")
        if dry_run:
            continue
        comment_pr(owner, repo, number, body)
        applied += 1
    return findings, applied


def load_github_state(owner: str, repo: str) -> tuple[dict, dict, list, list]:
    issues = issue_lookup(gh_json(
        owner, repo,
        ["issue", "list", "--state", "open", "--limit", str(HTTP_PER_PAGE)],
        "number,title,labels,updatedAt",
    ))
    closed_issues = issue_lookup(gh_json(
        owner, repo,
        ["issue", "list", "--state", "closed", "--limit", str(HTTP_PER_PAGE)],
        "number,title,state,stateReason,updatedAt",
    ))
    pulls = gh_json(
        owner, repo,
        ["pr", "list", "--state", "open", "--limit", str(HTTP_PER_PAGE)],
        "number,title,isDraft,updatedAt",
    )
    merged = [
        item for item in gh_json(
            owner, repo,
            ["pr", "list", "--state", "merged", "--limit", str(HTTP_PER_PAGE)],
            "number,title,mergedAt,closingIssuesReferences",
        )
        if item.get("mergedAt")
    ]
    return issues, closed_issues, pulls, merged


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", nargs="?", choices=["reconcile"], default="reconcile",
        help="要执行的命令（当前仅支持 reconcile）",
    )
    parser.add_argument("--repo", help="GitHub owner/repo；默认读取 origin remote")
    parser.add_argument(
        "--stale-hours", type=int, default=DEFAULT_STALE_HOURS,
        help=f"超过多少小时未更新的 Draft PR 视为陈旧（默认 {DEFAULT_STALE_HOURS}）",
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="执行列出的安全修复；默认只读 dry-run",
    )
    args = parser.parse_args()

    if args.repo:
        if "/" not in args.repo:
            raise SystemExit("--repo 必须是 owner/name 形式")
        owner, repo = args.repo.split("/", 1)
    else:
        owner, repo = repo_from_remote()

    dry_run = not args.apply
    mode = "dry-run" if dry_run else "apply"
    print(f"[github-sync] repo={owner}/{repo} mode={mode} stale-hours={args.stale_hours}")

    issues, closed_issues, pulls, merged = load_github_state(owner, repo)

    print("\n== 已合并 PR 但仍 open 的 Closes Issue ==")
    merged_findings, merged_applied, merged_blocked = reconcile_merged_issues(
        owner, repo, merged, issues, dry_run,
    )

    print("\n== 重复的 Issue ==")
    dup_findings, dup_applied, dup_blocked = reconcile_duplicate_issues(
        owner, repo, issues, closed_issues, dry_run,
    )

    print(f"\n== 超过 {args.stale_hours} 小时未更新的 Draft PR ==")
    stale_findings, stale_applied = reconcile_stale_drafts(
        owner, repo, pulls, args.stale_hours, dry_run,
    )

    findings = merged_findings + dup_findings + stale_findings
    applied = merged_applied + dup_applied + stale_applied
    blocked = merged_blocked + dup_blocked
    print(
        "\n[github-sync] summary "
        f"findings={findings} applied={applied} blocked_or_owner_decision={blocked}"
    )
    if findings == 0:
        print("[github-sync] OK：没有需要处理的管理记录问题")
        return 0
    if dry_run:
        print("[github-sync] dry-run：确认后使用 --apply 执行脚本列出的安全修复")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
