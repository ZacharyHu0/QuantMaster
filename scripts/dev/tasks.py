"""Isolated task worktrees and change-aware validation for QuantMaster."""

from __future__ import annotations

import argparse
import fnmatch
import json
import re
import subprocess
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
IMPACT_FILE = Path(__file__).with_name("test-impact.json")
SLUG_PATTERN = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
VERSION_PATHS = frozenset({"quantmaster/release.py"})


def git(args: list[str], *, cwd: Path = ROOT, check: bool = True) -> subprocess.CompletedProcess[str]:
    command = ["git", "-c", f"safe.directory={cwd.as_posix()}", *args]
    return subprocess.run(
        command, cwd=cwd, check=check, capture_output=True, text=True,
        encoding="utf-8", errors="replace",
    )


def git_lines(args: list[str], *, cwd: Path = ROOT) -> list[str]:
    result = git(args, cwd=cwd)
    return [line.strip().replace("\\", "/") for line in result.stdout.splitlines() if line.strip()]


def primary_root(cwd: Path = ROOT) -> Path:
    records = git_lines(["worktree", "list", "--porcelain"], cwd=cwd)
    for line in records:
        if line.startswith("worktree "):
            return Path(line.removeprefix("worktree ")).resolve()
    raise RuntimeError("无法确定主 worktree")


def project_python(cwd: Path = ROOT) -> Path:
    primary = primary_root(cwd)
    for candidate in (primary / ".venv/Scripts/python.exe", primary / ".venv/bin/python"):
        if candidate.is_file():
            return candidate
    raise RuntimeError(f"项目虚拟环境不存在：{primary / '.venv'}")


def matches(path: str, pattern: str) -> bool:
    return fnmatch.fnmatchcase(path, pattern) or (
        pattern.endswith("/**") and path == pattern[:-3]
    )


@dataclass(frozen=True)
class Impact:
    mode: str
    tests: tuple[str, ...] = ()
    unknown: tuple[str, ...] = ()


def _tests_for_path(path: str, config: dict) -> tuple[set[str], bool]:
    selected: set[str] = set()
    matched = False
    for rule in config["rules"]:
        if not any(matches(path, pattern) for pattern in rule["paths"]):
            continue
        matched = True
        selected.update(rule.get("tests", []))
        if rule.get("tests_from_paths"):
            selected.add(path)
    return selected, matched


def select_impact(paths: list[str], config_path: Path = IMPACT_FILE) -> Impact:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    normalized = sorted({path.replace("\\", "/") for path in paths})
    if not normalized:
        return Impact("none")
    docs = config["docs_only"]
    if all(any(matches(path, pattern) for pattern in docs) for path in normalized):
        return Impact("docs")
    if any(
        matches(path, pattern)
        for path in normalized
        for pattern in config["full_suite"]
    ):
        return Impact("full")

    selected = set(config["always"])
    unknown: list[str] = []
    for path in normalized:
        if any(matches(path, pattern) for pattern in docs):
            continue
        path_tests, matched = _tests_for_path(path, config)
        selected.update(path_tests)
        if not matched:
            unknown.append(path)
    if unknown:
        return Impact("full", unknown=tuple(unknown))
    return Impact("selected", tuple(sorted(selected)))


def changed_paths(cwd: Path, *, staged: bool, base: str) -> list[str]:
    if staged:
        return git_lines(["diff", "--cached", "--name-only", "--diff-filter=ACMR"], cwd=cwd)
    merge_base = git(["merge-base", base, "HEAD"], cwd=cwd).stdout.strip()
    committed = git_lines(["diff", "--name-only", "--diff-filter=ACMR", merge_base, "HEAD"], cwd=cwd)
    working = git_lines(["diff", "--name-only", "--diff-filter=ACMR", "HEAD"], cwd=cwd)
    untracked = git_lines(["ls-files", "--others", "--exclude-standard"], cwd=cwd)
    return sorted(set(committed + working + untracked))


def run(command: list[str], *, cwd: Path) -> None:
    print(f"[task] {' '.join(command)}", flush=True)
    subprocess.run(command, cwd=cwd, check=True)


def check(cwd: Path, *, staged: bool = False, base: str = "origin/main") -> Impact:
    paths = changed_paths(cwd, staged=staged, base=base)
    impact = select_impact(paths)
    print(f"[task] changed paths: {len(paths)}; validation: {impact.mode}")
    python = str(project_python(cwd))
    python_paths = [path for path in paths if path.endswith(".py") and (cwd / path).is_file()]
    if python_paths:
        run([python, "-m", "ruff", "check", *python_paths], cwd=cwd)
    if impact.mode == "full":
        if impact.unknown:
            print("[task] unknown paths force full validation: " + ", ".join(impact.unknown))
        run([python, "scripts/ci/run.py", "--full"], cwd=cwd)
    elif impact.mode == "selected":
        temp = cwd / ".artifacts/pytest" / f"impact-{uuid.uuid4().hex[:10]}"
        run([
            python, "-m", "pytest", "--full", *impact.tests,
            "--timeout=180", "--durations=20", "--basetemp", str(temp),
        ], cwd=cwd)
    elif impact.mode == "docs":
        print("[task] documentation-only change: Python tests skipped")
    else:
        print("[task] no changes to validate")
    return impact


def start(slug: str) -> None:
    if not SLUG_PATTERN.fullmatch(slug):
        raise SystemExit("slug 只允许小写字母、数字和单连字符")
    primary = primary_root(ROOT)
    branch = f"codex/{slug}"
    target = primary / ".worktrees" / slug
    if target.exists():
        raise SystemExit(f"worktree 已存在：{target}")
    git(["show-ref", "--verify", "--quiet", "refs/remotes/origin/main"], cwd=primary)
    git(["worktree", "add", "-b", branch, str(target), "origin/main"], cwd=primary)
    print(f"[task] created {branch} at {target}")


def ready(cwd: Path, *, ui: bool, rust: bool, package: bool) -> None:
    branch = git(["branch", "--show-current"], cwd=cwd).stdout.strip()
    status = git(["status", "--porcelain"], cwd=cwd).stdout.strip()
    behind = bool(
        git(
            ["merge-base", "--is-ancestor", "origin/main", "HEAD"],
            cwd=cwd,
            check=False,
        ).returncode
    )
    changed = changed_paths(cwd, staged=False, base="origin/main")
    validate_ready_state(branch, status, behind, changed)
    args = [str(project_python(cwd)), "scripts/ci/run.py", "--full"]
    if ui:
        args.append("--ui")
    if rust:
        args.append("--rust")
    if package:
        args.append("--package")
    run(args, cwd=cwd)
    print("[task] READY: 可 squash 为一个独立 main 提交；仅在明确发布时更新版本元数据")


def validate_ready_state(branch: str, status: str, behind: bool, changed: list[str]) -> None:
    if not branch.startswith("codex/"):
        raise SystemExit("ready 只能在 codex/<task-slug> 任务分支运行")
    if status:
        raise SystemExit("工作区不干净；请先提交任务改动")
    if behind:
        raise SystemExit("任务分支落后于 origin/main；请先更新并解决冲突")
    version_changes = VERSION_PATHS.intersection(changed)
    if version_changes:
        raise SystemExit("任务分支不得修改版本元数据：" + ", ".join(sorted(version_changes)))


def remove(slug: str) -> None:
    if not SLUG_PATTERN.fullmatch(slug):
        raise SystemExit("无效 slug")
    primary = primary_root(ROOT)
    target = (primary / ".worktrees" / slug).resolve()
    expected_parent = (primary / ".worktrees").resolve()
    if target.parent != expected_parent:
        raise SystemExit("拒绝移除预期目录之外的 worktree")
    branch = f"codex/{slug}"
    if not target.is_dir():
        raise SystemExit(f"worktree 不存在：{target}")
    if git(["status", "--porcelain"], cwd=target).stdout.strip():
        raise SystemExit("worktree 不干净，拒绝移除")
    remaining = set(git_lines(["diff", "--name-only", branch, "main"], cwd=primary))
    if remaining - VERSION_PATHS:
        raise SystemExit(f"{branch} 尚未完整 squash 到 main，拒绝移除")
    git(["worktree", "remove", str(target)], cwd=primary)
    git(["branch", "-d", branch], cwd=primary)
    print(f"[task] removed {branch} and {target}")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest="command", required=True)
    start_parser = commands.add_parser("start")
    start_parser.add_argument("slug")
    check_parser = commands.add_parser("check")
    check_parser.add_argument("--staged", action="store_true")
    check_parser.add_argument("--base", default="origin/main")
    ready_parser = commands.add_parser("ready")
    ready_parser.add_argument("--ui", action="store_true")
    ready_parser.add_argument("--rust", action="store_true")
    ready_parser.add_argument("--package", action="store_true")
    remove_parser = commands.add_parser("remove")
    remove_parser.add_argument("slug")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    cwd = Path.cwd().resolve()
    try:
        if args.command == "start":
            start(args.slug)
        elif args.command == "check":
            check(cwd, staged=args.staged, base=args.base)
        elif args.command == "ready":
            ready(cwd, ui=args.ui, rust=args.rust, package=args.package)
        elif args.command == "remove":
            remove(args.slug)
    except (RuntimeError, subprocess.CalledProcessError) as exc:
        print(f"[task] FAILED: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
