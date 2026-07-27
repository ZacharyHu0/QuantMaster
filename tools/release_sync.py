"""Bind QuantMaster release commits on ``main`` to their GitHub push.

The tracked hooks call this module before and after every commit.  A release
commit is validated from the Git index, then pushed only after Git has made an
immutable commit.  Failed pushes leave a marker inside ``.git`` and the next
release is blocked until the previous commit is synchronized.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
import subprocess
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
RELEASE_FILE = "quantmaster/release.py"
CHANGELOG_FILE = "CHANGELOG.md"
PENDING_MARKER = "quantmaster-release-sync.json"
VERSION_PATTERN = re.compile(r"^\d+\.\d+\.\d+$")
CHANGELOG_PATTERN = re.compile(
    r"^## v(?P<version>\d+\.\d+\.\d+)[（(](?P<date>\d{4}-\d{2}-\d{2})[）)]",
    re.MULTILINE,
)
RESOLVE_PATTERN = re.compile(r"^github\.com:443:[0-9a-fA-F:.]+$")


def run_git(
    args: list[str],
    *,
    check: bool = False,
    configs: list[tuple[str, str]] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run Git against this checkout without relying on global safe.directory."""
    command = ["git", "-c", f"safe.directory={ROOT.as_posix()}"]
    for key, value in configs or []:
        command.extend(["-c", f"{key}={value}"])
    command.extend(args)
    return subprocess.run(
        command,
        cwd=ROOT,
        check=check,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def git_text(args: list[str], *, required: bool = True) -> str:
    result = run_git(args)
    if required and result.returncode:
        detail = result.stderr.strip() or result.stdout.strip() or "Git command failed"
        raise RuntimeError(detail)
    return result.stdout.strip()


def release_assignments(source: str) -> dict[str, str]:
    """Read VERSION and RELEASE_DATE without importing application code."""
    tree = ast.parse(source)
    values: dict[str, str] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Constant):
            continue
        for target in node.targets:
            if (
                isinstance(target, ast.Name)
                and target.id in {"VERSION", "RELEASE_DATE"}
                and isinstance(node.value.value, str)
            ):
                values[target.id] = node.value.value
    return values


def version_tuple(value: str) -> tuple[int, int, int]:
    if not VERSION_PATTERN.fullmatch(value):
        raise ValueError(f"不是有效的语义版本号：{value!r}")
    return tuple(int(part) for part in value.split("."))  # type: ignore[return-value]


def validate_metadata(
    release_source: str,
    changelog_source: str,
    *,
    today: date | None = None,
) -> list[str]:
    """Return user-facing release bookkeeping errors."""
    errors: list[str] = []
    try:
        values = release_assignments(release_source)
    except (SyntaxError, ValueError) as exc:
        return [f"{RELEASE_FILE} 无法解析：{exc}"]

    version = values.get("VERSION", "")
    release_date = values.get("RELEASE_DATE", "")
    try:
        version_tuple(version)
    except ValueError as exc:
        errors.append(str(exc))
    try:
        parsed_date = date.fromisoformat(release_date)
    except ValueError:
        parsed_date = None
        errors.append(f"RELEASE_DATE 不是有效日期：{release_date!r}")
    expected_date = today or date.today()
    if parsed_date is not None and parsed_date != expected_date:
        errors.append(
            f"RELEASE_DATE 必须是实际发布日期 {expected_date.isoformat()}，当前为 {release_date}"
        )

    releases_at = release_source.find("RELEASES")
    current_release = release_source[releases_at : releases_at + 900]
    if releases_at < 0 or '"version": VERSION' not in current_release:
        errors.append("RELEASES 第一项必须使用 VERSION")
    if releases_at < 0 or '"date": RELEASE_DATE' not in current_release:
        errors.append("RELEASES 第一项必须使用 RELEASE_DATE")

    changelog_match = CHANGELOG_PATTERN.search(changelog_source)
    if not changelog_match:
        errors.append("CHANGELOG.md 顶部缺少 `## vX.Y.Z（YYYY-MM-DD）`")
    else:
        if changelog_match.group("version") != version:
            errors.append(
                "CHANGELOG.md 顶部版本与 VERSION 不一致："
                f"{changelog_match.group('version')} != {version}"
            )
        if changelog_match.group("date") != release_date:
            errors.append(
                "CHANGELOG.md 顶部日期与 RELEASE_DATE 不一致："
                f"{changelog_match.group('date')} != {release_date}"
            )
        following = changelog_source[changelog_match.end() :]
        next_release = CHANGELOG_PATTERN.search(following)
        current_notes = following[: next_release.start() if next_release else len(following)]
        if not re.search(r"^###\s+.+", current_notes, re.MULTILINE):
            errors.append("CHANGELOG.md 当前版本缺少用户可读的小节")
        if not re.search(r"^-\s+.+", current_notes, re.MULTILINE):
            errors.append("CHANGELOG.md 当前版本缺少变更条目")
    return errors


def read_committed(path: str, revision: str = "HEAD") -> str:
    return git_text(["show", f"{revision}:{path}"])


def read_staged(path: str) -> str:
    return git_text(["show", f":{path}"])


def staged_paths() -> set[str]:
    output = git_text(["diff", "--cached", "--name-only", "--diff-filter=ACMR"], required=False)
    return {line.strip().replace("\\", "/") for line in output.splitlines() if line.strip()}


def current_branch() -> str:
    return git_text(["branch", "--show-current"], required=False)


def git_path(name: str) -> Path:
    value = git_text(["rev-parse", "--git-path", name])
    path = Path(value)
    return path if path.is_absolute() else ROOT / path


def pending_marker() -> Path:
    return git_path(PENDING_MARKER)


def write_pending(commit: str, version: str, error: str = "") -> None:
    marker = pending_marker()
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(
        json.dumps(
            {
                "commit": commit,
                "version": version,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "last_error": error[-2000:],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def clear_pending() -> None:
    marker = pending_marker()
    if marker.exists():
        marker.unlink()


def local_and_tracking_heads() -> tuple[str, str]:
    local = git_text(["rev-parse", "HEAD"])
    tracking = git_text(["rev-parse", "refs/remotes/origin/main"], required=False)
    return local, tracking


def verify_previous_release_synced() -> list[str]:
    if current_branch() != "main":
        return []
    errors: list[str] = []
    marker = pending_marker()
    if marker.exists():
        errors.append(
            "上一个版本仍标记为待推送；先运行 `python tools/release_sync.py push`"
        )
    local, tracking = local_and_tracking_heads()
    if not tracking:
        errors.append("缺少 origin/main 跟踪引用；先运行 `git fetch origin main`")
    elif local != tracking:
        errors.append(
            "本地 main 与 origin/main 尚未同步；先运行 `python tools/release_sync.py push`"
        )
    return errors


def print_errors(errors: list[str], title: str) -> int:
    if not errors:
        return 0
    print(f"[QuantMaster] {title}", file=sys.stderr)
    for error in errors:
        print(f"  - {error}", file=sys.stderr)
    return 1


def pre_commit() -> int:
    paths = staged_paths()
    if not paths:
        return 0
    required = {RELEASE_FILE, CHANGELOG_FILE}
    missing = sorted(required - paths)
    errors = [f"每次提交都必须同时暂存 {path}" for path in missing]
    if missing:
        return print_errors(errors, "发布元数据不完整，提交已阻止")

    release_source = read_staged(RELEASE_FILE)
    changelog_source = read_staged(CHANGELOG_FILE)
    errors.extend(validate_metadata(release_source, changelog_source))
    staged_version = release_assignments(release_source).get("VERSION", "")
    try:
        head_version = release_assignments(read_committed(RELEASE_FILE)).get("VERSION", "")
        if version_tuple(staged_version) <= version_tuple(head_version):
            errors.append(f"VERSION 必须递增：{staged_version} <= {head_version}")
    except (RuntimeError, ValueError, SyntaxError) as exc:
        errors.append(f"无法比较当前与待提交版本：{exc}")
    errors.extend(verify_previous_release_synced())
    if print_errors(errors, "发布提交校验失败"):
        return 1
    print(f"[QuantMaster] 发布提交校验通过：v{staged_version}")
    return 0


def committed_release_errors(revision: str = "HEAD") -> tuple[str, list[str]]:
    release_source = read_committed(RELEASE_FILE, revision)
    changelog_source = read_committed(CHANGELOG_FILE, revision)
    version = release_assignments(release_source).get("VERSION", "")
    return version, validate_metadata(release_source, changelog_source)


def release_changed_in_head() -> bool:
    output = git_text(
        ["diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD"],
        required=False,
    )
    return RELEASE_FILE in {line.strip().replace("\\", "/") for line in output.splitlines()}


def config_value(key: str, default: str = "") -> str:
    value = git_text(["config", "--local", "--get", key], required=False)
    return value or default


def auto_push_enabled() -> bool:
    environment = os.getenv("QM_RELEASE_AUTO_PUSH", "").strip().lower()
    if environment in {"0", "false", "no", "off"}:
        return False
    configured = config_value("quantmaster.releaseAutoPush", "true").lower()
    return configured not in {"0", "false", "no", "off"}


def push_config_variants(resolve: str = "") -> list[list[tuple[str, str]]]:
    common = [
        ("http.version", "HTTP/1.1"),
        ("http.lowSpeedLimit", "1"),
        ("http.lowSpeedTime", "120"),
    ]
    variants: list[list[tuple[str, str]]] = []
    if resolve and RESOLVE_PATTERN.fullmatch(resolve):
        variants.append([*common, ("http.curloptResolve", resolve)])
    variants.append(common)
    return variants


def push_current_release() -> int:
    if current_branch() != "main":
        print("[QuantMaster] 当前不是 main；不会自动推送。")
        return 0
    version, errors = committed_release_errors()
    if print_errors(errors, "HEAD 发布元数据校验失败，未推送"):
        return 1

    commit = git_text(["rev-parse", "HEAD"])
    write_pending(commit, version)
    try:
        retries = max(1, min(6, int(config_value("quantmaster.releasePushRetries", "3"))))
    except ValueError:
        retries = 3
    resolve = config_value("quantmaster.githubResolve")
    variants = push_config_variants(resolve)
    failures: list[str] = []
    for attempt in range(retries):
        configs = variants[attempt % len(variants)]
        result = run_git(["push", "origin", "HEAD:main"], configs=configs)
        if result.returncode == 0:
            run_git(["update-ref", "refs/remotes/origin/main", commit])
            clear_pending()
            print(f"[QuantMaster] v{version} 已自动推送到 origin/main ({commit[:8]})")
            return 0
        detail = result.stderr.strip() or result.stdout.strip() or "unknown push error"
        failures.append(detail)
        if attempt + 1 < retries:
            time.sleep(min(5, 1 + attempt * 2))

    last_error = failures[-1] if failures else "unknown push error"
    write_pending(commit, version, last_error)
    print("[QuantMaster] 自动推送失败，提交已保留并标记为待同步。", file=sys.stderr)
    print(last_error, file=sys.stderr)
    print(
        "恢复后运行：python tools/release_sync.py push",
        file=sys.stderr,
    )
    return 1


def post_commit() -> int:
    if current_branch() != "main" or not release_changed_in_head():
        return 0
    if not auto_push_enabled():
        print("[QuantMaster] 自动推送已禁用；本次提交未上传。")
        return 0
    return push_current_release()


def install_hooks(args: argparse.Namespace) -> int:
    expected_hooks = [ROOT / ".githooks" / name for name in ("pre-commit", "post-commit")]
    missing = [path for path in expected_hooks if not path.is_file()]
    if missing:
        return print_errors([f"钩子文件不可用：{path}" for path in missing], "安装失败")
    settings = [
        ("core.hooksPath", ".githooks"),
        ("quantmaster.releaseAutoPush", "true"),
        ("quantmaster.releasePushRetries", str(args.retries)),
    ]
    if args.github_resolve:
        if not RESOLVE_PATTERN.fullmatch(args.github_resolve):
            return print_errors(
                ["--github-resolve 必须形如 github.com:443:140.82.114.4"],
                "安装失败",
            )
        settings.append(("quantmaster.githubResolve", args.github_resolve))
    for key, value in settings:
        result = run_git(["config", "--local", key, value])
        if result.returncode:
            return print_errors([result.stderr.strip()], "写入 Git 配置失败")
    print("[QuantMaster] Git hooks 已启用：版本提交将在 main 上自动推送。")
    return status()


def status() -> int:
    branch = current_branch() or "(detached)"
    version, metadata_errors = committed_release_errors()
    local, tracking = local_and_tracking_heads()
    marker = pending_marker()
    hooks_path = config_value("core.hooksPath")
    print(f"branch: {branch}")
    print(f"version: {version}")
    print(f"local: {local[:12]}")
    print(f"origin/main: {tracking[:12] if tracking else '(missing)'}")
    print(f"hooks: {'on' if hooks_path == '.githooks' else 'off'}")
    print(f"auto-push: {'on' if auto_push_enabled() else 'off'}")
    print(f"pending: {'yes' if marker.exists() else 'no'}")
    errors = list(metadata_errors)
    if branch == "main" and local != tracking:
        errors.append("main 尚未与 origin/main 同步")
    if marker.exists():
        errors.append(f"存在待同步标记：{marker}")
    return print_errors(errors, "发布同步状态异常")


def check_worktree() -> int:
    release_source = (ROOT / RELEASE_FILE).read_text(encoding="utf-8")
    changelog_source = (ROOT / CHANGELOG_FILE).read_text(encoding="utf-8")
    errors = validate_metadata(release_source, changelog_source)
    if print_errors(errors, "发布元数据校验失败"):
        return 1
    version = release_assignments(release_source)["VERSION"]
    print(f"[QuantMaster] 发布元数据有效：v{version}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    install_parser = subparsers.add_parser("install", help="启用仓库级 Git hooks")
    install_parser.add_argument("--retries", type=int, default=3, choices=range(1, 7))
    install_parser.add_argument(
        "--github-resolve",
        default="",
        help="可选的 GitHub HTTPS 固定解析，例如 github.com:443:140.82.114.4",
    )
    subparsers.add_parser("check", help="检查工作区发布元数据")
    subparsers.add_parser("status", help="显示本地与 origin/main 同步状态")
    subparsers.add_parser("push", help="重试推送当前 main 发布提交")
    subparsers.add_parser("pre-commit", help=argparse.SUPPRESS)
    subparsers.add_parser("post-commit", help=argparse.SUPPRESS)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    commands: dict[str, Any] = {
        "install": lambda: install_hooks(args),
        "check": check_worktree,
        "status": status,
        "push": push_current_release,
        "pre-commit": pre_commit,
        "post-commit": post_commit,
    }
    return int(commands[args.command]())


if __name__ == "__main__":
    raise SystemExit(main())
