"""Isolated task worktrees and change-aware validation for QuantMaster."""

from __future__ import annotations

import argparse
import fnmatch
import json
import os
import re
import shutil
import stat
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


def registered_worktrees(primary: Path) -> set[Path]:
    return {
        Path(line.removeprefix("worktree ")).resolve()
        for line in git_lines(["worktree", "list", "--porcelain"], cwd=primary)
        if line.startswith("worktree ")
    }


def task_integrated(primary: Path, branch: str) -> bool:
    """Accept ancestry, cherry-picks, or an aggregate squash already present on main."""
    if git(["merge-base", "--is-ancestor", branch, "main"], cwd=primary, check=False).returncode == 0:
        return True
    outstanding = git(
        ["log", "--cherry-pick", "--right-only", "--no-merges", "--format=%H", f"main...{branch}"],
        cwd=primary,
    ).stdout.strip()
    if not outstanding:
        return True
    base = git(["merge-base", "main", branch], cwd=primary).stdout.strip()
    patch = git(["diff", "--binary", base, branch], cwd=primary).stdout
    if not patch:
        return True
    checked = subprocess.run(
        ["git", "-c", f"safe.directory={primary.as_posix()}", "apply", "--reverse", "--check"],
        cwd=primary, input=patch, capture_output=True, text=True,
        encoding="utf-8", errors="replace", check=False,
    )
    return checked.returncode == 0


def remove_primary_venv_link(target: Path, primary: Path) -> bool:
    """Remove only a task-local reparse point that resolves to the primary venv."""
    link = target / ".venv"
    if not link.exists():
        return False
    attributes = getattr(link.lstat(), "st_file_attributes", 0)
    if not link.is_symlink() and not bool(attributes & stat.FILE_ATTRIBUTE_REPARSE_POINT):
        raise SystemExit(f"{link} 不是目录联接，拒绝自动删除")
    if link.resolve() != (primary / ".venv").resolve():
        raise SystemExit(f"{link} 未指向主 worktree 虚拟环境，拒绝自动删除")
    os.rmdir(link)
    return True


def remove_empty_residual(target: Path) -> None:
    if not target.exists():
        return
    entries = list(target.iterdir())
    if entries:
        names = ", ".join(sorted(entry.name for entry in entries))
        raise SystemExit(f"worktree 登记已移除，但目录仍有其他内容，拒绝删除：{names}")
    target.rmdir()


def residual_checkout_clean(primary: Path, target: Path, branch: str) -> bool:
    """Verify a deregistered checkout still matches its task branch exactly."""
    marker = target / ".git"
    if not marker.is_file():
        return False
    marker_value = marker.read_text(encoding="utf-8", errors="replace").strip()
    if not marker_value.startswith("gitdir: "):
        return False
    recorded = Path(marker_value.removeprefix("gitdir: "))
    expected = primary / ".git" / "worktrees" / target.name
    if recorded.resolve() != expected.resolve():
        return False
    index = primary / ".artifacts" / "task-remove" / f"{uuid.uuid4().hex}.index"
    index.parent.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "GIT_INDEX_FILE": str(index), "GIT_WORK_TREE": str(target)}
    command = ["git", "-c", f"safe.directory={primary.as_posix()}"]
    try:
        read_tree = subprocess.run(
            [*command, "read-tree", branch], cwd=primary, env=env,
            capture_output=True, text=True, encoding="utf-8", errors="replace", check=False,
        )
        if read_tree.returncode:
            return False
        refreshed = subprocess.run(
            [*command, "update-index", "--refresh"], cwd=primary, env=env,
            capture_output=True, text=True, encoding="utf-8", errors="replace", check=False,
        )
        if refreshed.returncode:
            return False
        tracked = subprocess.run(
            [*command, "diff-files", "--quiet"],
            cwd=primary, env=env, capture_output=True, text=True,
            encoding="utf-8", errors="replace", check=False,
        )
        untracked = subprocess.run(
            [*command, "ls-files", "--others", "--exclude-standard"],
            cwd=primary, env=env, capture_output=True, text=True,
            encoding="utf-8", errors="replace", check=False,
        )
        return (
            tracked.returncode == 0
            and untracked.returncode == 0
            and not untracked.stdout.strip()
        )
    finally:
        index.unlink(missing_ok=True)


def remove_verified_residual(primary: Path, target: Path, branch: str) -> None:
    if not target.exists():
        return
    remove_primary_venv_link(target, primary)
    if not residual_checkout_clean(primary, target, branch):
        raise SystemExit("worktree 登记已移除，但残留 checkout 无法证明干净，拒绝删除")
    resolved = target.resolve()
    expected_parent = (primary / ".worktrees").resolve()
    if resolved.parent != expected_parent:
        raise SystemExit("拒绝删除预期目录之外的残留 worktree")
    def make_writable(function, path, error):
        if not isinstance(error, PermissionError):
            raise error
        os.chmod(path, stat.S_IWRITE)
        function(path)

    try:
        shutil.rmtree(resolved, onexc=make_writable)
    except PermissionError as exc:
        blocked = Path(exc.filename or resolved)
        raise SystemExit(
            "已证明残留 checkout 干净，但 Windows ACL 阻止删除："
            f"{blocked}；修复该路径权限后重新运行 remove"
        ) from None


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
    branch_exists = git(
        ["show-ref", "--verify", "--quiet", f"refs/heads/{branch}"],
        cwd=primary, check=False,
    ).returncode == 0
    registered = target in registered_worktrees(primary)
    if not branch_exists and not registered and not target.exists():
        print(f"[task] {branch} 已清理")
        return
    if branch_exists and not task_integrated(primary, branch):
        raise SystemExit(f"{branch} 尚未完整 squash 到 main，拒绝移除")
    if registered:
        if git(["status", "--porcelain"], cwd=target).stdout.strip():
            raise SystemExit("worktree 不干净，拒绝移除")
        remove_primary_venv_link(target, primary)
        result = git(["worktree", "remove", str(target)], cwd=primary, check=False)
        still_registered = target in registered_worktrees(primary)
        if result.returncode and still_registered:
            detail = result.stderr.strip() or result.stdout.strip() or "unknown Git error"
            raise RuntimeError(f"Git worktree 移除失败：{detail}")
        if target.exists():
            remove_verified_residual(primary, target, branch)
    else:
        remove_verified_residual(primary, target, branch)
    if branch_exists:
        git(["branch", "-D", branch], cwd=primary)
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
