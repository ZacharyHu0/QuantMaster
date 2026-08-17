"""Isolated task worktrees and change-aware validation for QuantMaster."""

from __future__ import annotations

import argparse
import contextlib
import fnmatch
import json
import os
import re
import shutil
import socket
import stat
import subprocess
import sys
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from scripts.dev.pytest_windows_acl import (
        AclRecoveryError,
        make_writable,
        prepare_pytest_directory,
        restore_acl_inheritance,
    )
except ModuleNotFoundError:
    from pytest_windows_acl import (
        AclRecoveryError,
        make_writable,
        prepare_pytest_directory,
        restore_acl_inheritance,
    )

from quantmaster.logging_config import redact_public_text  # noqa: E402

IMPACT_FILE = Path(__file__).with_name("test-impact.json")
SLUG_PATTERN = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
VERSION_PATHS = frozenset({"quantmaster/release.py", "CHANGELOG.md"})
VALIDATION_EVIDENCE = "validation/full.json"
TASK_LEASE = ".task-running.lock"
COMPLETION_SCHEMA = 1
REMOVE_INTENT_SCHEMA = 1
TASK_ARTIFACT_ACL_UNRECOVERABLE = "TASK_ARTIFACT_ACL_UNRECOVERABLE"
TASK_CHECKOUT_ACL_UNRECOVERABLE = "TASK_CHECKOUT_ACL_UNRECOVERABLE"
_WINDOWS_TRANSIENT_CLEANUP_ERRORS = frozenset({32, 33, 145})


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


def task_changed_paths(cwd: Path) -> list[str]:
    """Return committed changes introduced by this task, not inherited main history."""

    return git_lines(["diff", "--name-only", "--diff-filter=ACMR", "main...HEAD"], cwd=cwd)


def _try_lock(stream) -> bool:
    if os.name == "nt":
        import msvcrt

        try:
            stream.seek(0)
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            return False
        return True
    import fcntl

    try:
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        return False
    return True


def _unlock(stream) -> None:
    if os.name == "nt":
        import msvcrt

        stream.seek(0)
        msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
        return
    import fcntl

    fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


@contextlib.contextmanager
def task_artifact_lease(artifacts: Path):
    lease_root = artifacts.parents[1] / "task-leases"
    prepare_pytest_directory(lease_root)
    marker = lease_root / f"{artifacts.name}{TASK_LEASE}"
    with marker.open("a+b") as stream:
        if stream.tell() == 0:
            stream.write(b"0")
            stream.flush()
        if not _try_lock(stream):
            raise RuntimeError(f"任务工件正在被另一进程使用：{artifacts}")
        try:
            yield
        finally:
            _unlock(stream)


def task_artifacts_active(artifacts: Path) -> bool:
    marker = artifacts.parents[1] / "task-leases" / f"{artifacts.name}{TASK_LEASE}"
    prepare_pytest_directory(marker.parent)
    with marker.open("a+b") as stream:
        if not _try_lock(stream):
            return True
        _unlock(stream)
    return False


def cleanup_task_lease_marker(primary: Path, slug: str) -> bool:
    """Remove an orphaned task lease marker after its artifact root is gone.

    The caller must not hold the same marker's lock.  If another process owns
    the marker, deletion is skipped so the active lease stays observable.
    """
    marker = primary / ".artifacts" / "task-leases" / f"{slug}{TASK_LEASE}"
    if not marker.exists():
        return False
    try:
        with marker.open("a+b") as stream:
            if not _try_lock(stream):
                return False
            _unlock(stream)
        marker.unlink(missing_ok=True)
        return True
    except OSError:
        return False


def task_completion_path(primary: Path, slug: str) -> Path:
    return primary / ".artifacts" / "task-completions" / f"{slug}.json"


def record_task_completion(
    primary: Path, slug: str, *, branch: str, superseded_by: str | None,
) -> Path:
    root = task_completion_path(primary, slug).parent
    prepare_pytest_directory(root)
    path = root / f"{slug}.json"
    temporary = root / f".{slug}.{uuid.uuid4().hex}.tmp"
    payload = {
        "schema": COMPLETION_SCHEMA,
        "slug": slug,
        "branch": branch,
        "main_commit": git(["rev-parse", "main^{commit}"], cwd=primary).stdout.strip(),
        "superseded_by": superseded_by or "",
        "completed_at": datetime.now(UTC).isoformat(),
    }
    temporary.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)
    return path


def valid_task_completion(primary: Path, slug: str) -> bool:
    path = task_completion_path(primary, slug)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return False
    if payload.get("schema") != COMPLETION_SCHEMA or payload.get("slug") != slug:
        return False
    commit = str(payload.get("main_commit") or "")
    return bool(re.fullmatch(r"[0-9a-f]{40}", commit)) and git(
        ["merge-base", "--is-ancestor", commit, "main"], cwd=primary, check=False,
    ).returncode == 0


def task_remove_intent_path(primary: Path, slug: str) -> Path:
    return primary / ".artifacts" / "task-remove" / f"{slug}.json"


def record_task_remove_intent(primary: Path, slug: str, *, branch: str) -> Path:
    root = task_remove_intent_path(primary, slug).parent
    prepare_pytest_directory(root)
    path = root / f"{slug}.json"
    temporary = root / f".{slug}.{uuid.uuid4().hex}.tmp"
    payload = {
        "schema": REMOVE_INTENT_SCHEMA,
        "slug": slug,
        "branch": branch,
        "branch_commit": git(["rev-parse", f"{branch}^{{commit}}"], cwd=primary).stdout.strip(),
        "target": str((primary / ".worktrees" / slug).resolve()),
        "recorded_at": datetime.now(UTC).isoformat(),
    }
    temporary.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)
    return path


def valid_task_remove_intent(primary: Path, target: Path, branch: str) -> bool:
    path = task_remove_intent_path(primary, target.name)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return False
    if payload.get("schema") != REMOVE_INTENT_SCHEMA:
        return False
    if payload.get("slug") != target.name or payload.get("branch") != branch:
        return False
    if payload.get("target") != str(target.resolve()):
        return False
    branch_commit = str(payload.get("branch_commit") or "")
    if not re.fullmatch(r"[0-9a-f]{40}", branch_commit):
        return False
    current = git(["rev-parse", f"{branch}^{{commit}}"], cwd=primary, check=False)
    return current.returncode == 0 and current.stdout.strip() == branch_commit


def run(command: list[str], *, cwd: Path) -> None:
    print(f"[task] {redact_public_text(' '.join(command))}", flush=True)
    primary = primary_root(cwd)
    artifacts = primary / ".artifacts" / "worktrees" / cwd.name
    env = os.environ.copy()
    env["RUFF_CACHE_DIR"] = str(artifacts / "cache" / "ruff")
    env["MYPY_CACHE_DIR"] = str(artifacts / "cache" / "mypy")
    env["UV_CACHE_DIR"] = str(artifacts / "uv-cache")
    env["QM_CONFIG_PATH"] = os.devnull
    env["QM_FREE_STOCKDB_ROOT"] = str(artifacts / "runtime" / "tests" / "free-stockdb")
    env["QM_TASK_LEASE_HELD"] = str(artifacts.resolve())
    with task_artifact_lease(artifacts):
        subprocess.run(command, cwd=cwd, env=env, check=True)


def validation_evidence_path(cwd: Path) -> Path:
    primary = primary_root(cwd)
    return primary / ".artifacts" / "worktrees" / cwd.name / VALIDATION_EVIDENCE


def project_environment_identity(python: Path, *, cwd: Path) -> str:
    command = [
        str(python), "-c",
        "import importlib.metadata as m; "
        "print('\\n'.join(sorted(f'{d.metadata[\"Name\"]}=={d.version}' for d in m.distributions())))",
    ]
    return subprocess.run(
        command, cwd=cwd, check=True, capture_output=True, text=True, encoding="utf-8",
    ).stdout


def full_validation_identity(
    cwd: Path, *, base: str, ui: bool = False, rust: bool = False, package: bool = False,
) -> dict[str, object]:
    python = project_python(cwd)
    python_stat = python.stat()
    return {
        "commit": git(["rev-parse", "HEAD"], cwd=cwd).stdout.strip(),
        "base": git(["rev-parse", base], cwd=cwd).stdout.strip(),
        "python": str(python.resolve()),
        "python_size": python_stat.st_size,
        "python_mtime_ns": python_stat.st_mtime_ns,
        "environment": project_environment_identity(python, cwd=cwd),
        "ui": ui,
        "rust": rust,
        "package": package,
    }


def record_full_validation(cwd: Path, identity: dict[str, object]) -> None:
    if git(["status", "--porcelain"], cwd=cwd).stdout.strip():
        return
    target = validation_evidence_path(cwd)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(".tmp")
    temporary.write_text(json.dumps(identity, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(target)


def has_full_validation(cwd: Path, identity: dict[str, object]) -> bool:
    target = validation_evidence_path(cwd)
    try:
        recorded = json.loads(target.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return False
    return recorded == identity


def github_remote_repo(cwd: Path) -> tuple[str, str]:
    result = git(["config", "--get", "remote.origin.url"], cwd=cwd, check=False)
    if result.returncode:
        raise SystemExit(
            "缺少 origin remote，无法读取 GitHub CI 证据；"
            "请本地运行 tasks.py ready（不带 --accept-ci）"
        )
    url = result.stdout.strip()
    match = re.match(
        r"(?:https?://(?:[^@/]+@)?github\.com/|git@github\.com:)"
        r"([^/]+)/([^/]+?)(?:\.git)?/?$",
        url,
    )
    if not match:
        raise SystemExit(f"无法从 origin URL 解析 GitHub owner/repo：{url}")
    return match.group(1), match.group(2)


def ci_has_heavy_success(owner: str, repo: str, run_id: int) -> bool:
    """A CI run counts as the full gate only when at least one heavy matrix job succeeded."""
    result = subprocess.run(
        [
            "gh", "api", f"repos/{owner}/{repo}/actions/runs/{run_id}/jobs",
            "--jq", "[.jobs[] | {name, conclusion}]",
        ],
        capture_output=True, text=True, encoding="utf-8", errors="replace", check=False,
    )
    if result.returncode:
        return False
    try:
        jobs = json.loads(result.stdout or "[]")
    except json.JSONDecodeError:
        return False
    heavy_prefixes = ("coverage-shard", "quality-package-audit", "windows-package")
    return any(
        job.get("conclusion") == "success"
        and any(job.get("name", "").startswith(prefix) for prefix in heavy_prefixes)
        for job in jobs
    )


def green_ci_runs(owner: str, repo: str, sha: str) -> list[dict[str, object]]:
    endpoint = (
        f"repos/{owner}/{repo}/actions/runs"
        f"?head_sha={quote(sha)}&per_page=100"
    )
    result = subprocess.run(
        [
            "gh", "api", endpoint,
            "--jq",
            "[.workflow_runs[] | {id, name, status, conclusion, head_sha, html_url}]",
        ],
        capture_output=True, text=True, encoding="utf-8", errors="replace", check=False,
    )
    if result.returncode:
        raise SystemExit(
            "gh api 调用失败；无法复用 CI 证据。请确认 gh 已登录且网络可用，"
            "或本地运行 tasks.py ready（不带 --accept-ci）"
        )
    try:
        runs = json.loads(result.stdout or "[]")
    except json.JSONDecodeError as exc:
        raise SystemExit(f"gh api 返回无法解析：{result.stdout[:200]}") from exc
    matching = [run for run in runs if run.get("head_sha") == sha]
    if not matching:
        raise SystemExit(f"GitHub Actions 没有该 commit 的 run：{sha}")
    pending = [run.get("name") for run in matching if run.get("status") != "completed"]
    if pending:
        raise SystemExit("CI 仍在运行，请等完成后再使用 --accept-ci：" + ", ".join(map(str, pending)))
    failed = [
        run.get("name") for run in matching
        if run.get("conclusion") not in {"success", "neutral", "skipped"}
    ]
    if failed:
        raise SystemExit("CI 未全绿：" + ", ".join(map(str, failed)))
    successful = [
        run for run in matching
        if run.get("conclusion") == "success" and run.get("name") == "CI"
    ]
    if not any(
        isinstance(run.get("id"), int) and ci_has_heavy_success(owner, repo, int(run["id"]))
        for run in successful
    ):
        raise SystemExit(
            "CI 只有 Draft 快检记录，没有完整重 job（coverage-shard / quality-package-audit / "
            "windows-package）成功证据；请把 PR 标记为 Ready 并等待完整矩阵完成"
        )
    return successful


def check(cwd: Path, *, staged: bool = False, base: str = "origin/main") -> Impact:
    paths = changed_paths(cwd, staged=staged, base=base)
    impact = select_impact(paths)
    print(f"[task] changed paths: {len(paths)}; validation: {impact.mode}")
    python = str(project_python(cwd))
    python_paths = [path for path in paths if path.endswith(".py") and (cwd / path).is_file()]
    if python_paths and impact.mode != "full":
        run([python, "-m", "ruff", "check", *python_paths], cwd=cwd)
    if impact.mode == "full":
        if impact.unknown:
            print("[task] unknown paths force full validation: " + ", ".join(impact.unknown))
        run([python, "scripts/ci/run.py", "--full"], cwd=cwd)
        record_full_validation(cwd, full_validation_identity(cwd, base=base))
    elif impact.mode == "selected":
        primary = primary_root(cwd)
        temp = (
            primary / ".artifacts" / "worktrees" / cwd.name
            / "pytest" / f"impact-{uuid.uuid4().hex[:10]}"
        )
        cache = prepare_pytest_directory(temp.parent / "cache")
        run([
            python, "-m", "pytest",
            "-o", f"cache_dir={cache}",
            "--full", *impact.tests,
            "--timeout=180", "--durations=20", "--basetemp", str(temp),
        ], cwd=cwd)
        shutil.rmtree(temp)
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
    artifact_root = primary / ".artifacts" / "worktrees" / slug
    for directory in (
        artifact_root / "cache" / "ruff",
        artifact_root / "cache" / "mypy",
        artifact_root / "uv-cache",
        artifact_root / "pytest" / "cache",
        artifact_root / "pytest" / "runs",
        artifact_root / "runtime" / "tests" / "data",
        artifact_root / "runtime" / "tests" / "free-stockdb",
        artifact_root / "runtime" / "tests" / "provider-cache",
    ):
        prepare_pytest_directory(directory)
    print(f"[task] created {branch} (local path omitted)")


def registered_worktrees(primary: Path) -> set[Path]:
    return {
        Path(line.removeprefix("worktree ")).resolve()
        for line in git_lines(["worktree", "list", "--porcelain"], cwd=primary)
        if line.startswith("worktree ")
    }


def serve(
    slug: str, *, open_browser: bool = False, stockdb_root: str | None = None,
) -> None:
    """Run one registered task as an isolated foreground development instance."""

    if not SLUG_PATTERN.fullmatch(slug):
        raise SystemExit("无效 slug")
    stable_root: Path | None = None
    if stockdb_root is not None:
        stable_root = Path(stockdb_root).expanduser()
        if not stable_root.is_absolute():
            raise SystemExit("--stockdb-root 必须是绝对路径")
        stable_root = stable_root.resolve()
    primary = primary_root(ROOT)
    tasks_root = (primary / ".worktrees").resolve()
    target = (tasks_root / slug).resolve()
    registered = registered_worktrees(primary)
    if target.parent != tasks_root or target not in registered:
        raise SystemExit(f"任务 worktree 未登记：{target}")
    branch = git(["branch", "--show-current"], cwd=target).stdout.strip()
    if branch != f"codex/{slug}":
        raise SystemExit(f"任务 worktree 分支不匹配：{branch or 'detached HEAD'}")

    task_targets = sorted(
        path for path in registered
        if path.parent == tasks_root
        and git(["branch", "--show-current"], cwd=path).stdout.strip()
        == f"codex/{path.name}"
    )
    port = 18686 + task_targets.index(target)
    if port > 65535:
        raise SystemExit("任务 worktree 数量超过可分配端口范围")
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        listener.bind(("127.0.0.1", port))
    except OSError:
        raise SystemExit(f"开发端口已被占用：127.0.0.1:{port}") from None
    finally:
        listener.close()

    artifacts = primary / ".artifacts" / "worktrees" / slug
    dev = artifacts / "runtime" / "dev"
    data = dev / "data"
    managed_stockdb = dev / "free-stockdb"
    for directory in (dev, data, data / "logs", managed_stockdb):
        prepare_pytest_directory(directory)
    config_path = dev / "config.yaml"
    config_path.write_text(json.dumps({
        "server": {"host": "127.0.0.1", "port": port},
        "data": {
            "free_stockdb_managed": False,
            "free_stockdb_auto_update": False,
        },
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    environment = os.environ.copy()
    environment.update({
        "QM_CONFIG_PATH": str(config_path),
        "QM_DATA_ROOT": str(data),
        "QM_FREE_STOCKDB_ROOT": str(managed_stockdb),
        "QM_FREE_STOCKDB_CONTROL_PATH": str(dev / "control.sqlite"),
        "QM_FREE_STOCKDB_MANAGED": "false",
        "QM_FREE_STOCKDB_AUTO_UPDATE": "false",
    })
    if stable_root is None:
        environment.pop("QM_FREE_STOCKDB_SDK_PATH", None)
    else:
        environment["QM_FREE_STOCKDB_SDK_PATH"] = str(stable_root / "pybao")

    command = [str(project_python(primary)), "-m", "quantmaster.server.cli", "serve"]
    if open_browser:
        command.append("--open")
    print(f"[task] serving codex/{slug} at http://127.0.0.1:{port}", flush=True)
    with task_artifact_lease(artifacts):
        subprocess.run(command, cwd=target, env=environment, check=True)


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
        return False
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


def _remove_verified_tree(
    root: Path,
    *,
    expected_parent: Path,
    scope: str,
    error_code: str,
    retry: str,
    retained: str,
) -> None:
    if root.parent != expected_parent:
        raise SystemExit(f"拒绝删除预期目录之外的{scope}")
    if not root.exists():
        return

    def residual_message(blocked: Path, *, kind: str, reason: object) -> str:
        relative_blocked = blocked.relative_to(root).as_posix()
        return (
            f"{error_code}: kind={kind}; "
            f"root={redact_public_text(root)}; "
            f"blocked={redact_public_text(relative_blocked)}; "
            f"reason={redact_public_text(reason)}; "
            f"retry={retry}；{retained}"
        )

    def checked_blocked(error: BaseException) -> Path:
        blocked = type(root)(getattr(error, "filename", None) or root).resolve()
        if blocked != root and root not in blocked.parents:
            raise SystemExit(
                f"拒绝清理{scope}之外的路径：{redact_public_text(blocked)}"
            ) from None
        return blocked

    def raise_residual(blocked: Path, *, kind: str, reason: object) -> None:
        raise SystemExit(
            residual_message(blocked, kind=kind, reason=reason)
        ) from None

    try:
        shutil.rmtree(root, onexc=make_writable)
    except PermissionError as exc:
        blocked = checked_blocked(exc)
        winerror = getattr(exc, "winerror", None)
        def remove_empty_root() -> bool:
            if winerror != 5:
                return False
            try:
                root.rmdir()
            except OSError:
                return False
            return True

        if remove_empty_root():
            return
        if winerror in _WINDOWS_TRANSIENT_CLEANUP_ERRORS:
            raise_residual(
                blocked,
                kind="transient_lock",
                reason=f"winerror={winerror}: {exc}",
            )
        try:
            restore_acl_inheritance(blocked)
        except AclRecoveryError as acl_error:
            raise_residual(blocked, kind=acl_error.kind, reason=acl_error)
        except OSError as acl_error:
            raise_residual(blocked, kind="transient", reason=acl_error)
        if remove_empty_root():
            return
        try:
            shutil.rmtree(root, onexc=make_writable)
            return
        except OSError as retry_error:
            blocked = checked_blocked(retry_error)
            winerror = getattr(retry_error, "winerror", None)
            if winerror in _WINDOWS_TRANSIENT_CLEANUP_ERRORS:
                kind = "transient_lock"
                reason = f"winerror={winerror}: {retry_error}"
            elif isinstance(retry_error, PermissionError):
                kind = "deletion_denied"
                reason = retry_error
            else:
                raise
            raise_residual(blocked, kind=kind, reason=reason)
    except OSError as exc:
        blocked = checked_blocked(exc)
        winerror = getattr(exc, "winerror", None)
        if winerror in _WINDOWS_TRANSIENT_CLEANUP_ERRORS:
            raise_residual(
                blocked,
                kind="transient_lock",
                reason=f"winerror={winerror}: {exc}",
            )
        raise


def remove_verified_residual(
    primary: Path,
    target: Path,
    branch: str,
    *,
    retry_command: str | None = None,
) -> None:
    if not target.exists():
        return
    remove_primary_venv_link(target, primary)
    clean_checkout = residual_checkout_clean(primary, target, branch)
    if not clean_checkout and not valid_task_remove_intent(primary, target, branch):
        raise SystemExit("worktree 登记已移除，但残留 checkout 无法证明干净，拒绝删除")
    resolved = target.resolve()
    retry = retry_command or (
        f".\\.venv\\Scripts\\python.exe scripts/dev/tasks.py remove "
        f"{branch.removeprefix('codex/')}"
    )
    _remove_verified_tree(
        resolved,
        expected_parent=(primary / ".worktrees").resolve(),
        scope="残留 checkout",
        error_code=TASK_CHECKOUT_ACL_UNRECOVERABLE,
        retry=retry,
        retained="残留 checkout 和任务分支已保留",
    )


def remove_task_artifacts(
    primary: Path, slug: str, *, retry_command: str | None = None,
) -> None:
    artifact_root = (primary / ".artifacts" / "worktrees" / slug).resolve()
    expected_parent = (primary / ".artifacts" / "worktrees").resolve()
    if artifact_root.parent != expected_parent:
        raise SystemExit("拒绝删除预期目录之外的任务工件")
    retry = retry_command or (
        f".\\.venv\\Scripts\\python.exe scripts/dev/tasks.py remove {slug}"
    )
    _remove_verified_tree(
        artifact_root,
        expected_parent=expected_parent,
        scope="任务工件",
        error_code=TASK_ARTIFACT_ACL_UNRECOVERABLE,
        retry=retry,
        retained="工件和任务分支已保留",
    )


DISPOSABLE_ARTIFACT_NAMES = frozenset({"cache", "pytest", "uv-cache", "runtime"})


def disposable_legacy_artifact_names(path: Path) -> list[str] | None:
    """Return child names for a legacy artifact root that only contains throwaway state."""
    try:
        names = sorted(entry.name for entry in path.iterdir())
    except OSError:
        return None
    if all(name in DISPOSABLE_ARTIFACT_NAMES for name in names):
        return names
    return None


def gc_task_artifacts(
    *, apply: bool, retention_days: int, adopt_legacy_orphans: bool = False,
) -> None:
    if retention_days < 0:
        raise SystemExit("retention days 不能为负数")
    primary = primary_root(ROOT)
    root = (primary / ".artifacts" / "worktrees").resolve()
    root.mkdir(parents=True, exist_ok=True)
    cutoff = datetime.now(UTC) - timedelta(days=retention_days)
    registered = registered_worktrees(primary)
    states = (
        "removed", "eligible", "protected", "active", "retained", "invalid", "failed",
    )
    counts = {key: 0 for key in states}

    for artifacts in sorted((path for path in root.iterdir() if path.is_dir()), key=lambda path: path.name):
        slug = artifacts.name
        if not SLUG_PATTERN.fullmatch(slug):
            names = disposable_legacy_artifact_names(artifacts)
            if names is None:
                counts["invalid"] += 1
                print(f"[task-gc] invalid slug with unknown content, skipped: {artifacts}")
                continue
            contents = ", ".join(names) if names else "empty"
            if not apply:
                counts["eligible"] += 1
                print(f"[task-gc] eligible legacy invalid root ({contents}): {artifacts}")
                continue
            try:
                with task_artifact_lease(artifacts):
                    if disposable_legacy_artifact_names(artifacts) is None:
                        counts["invalid"] += 1
                        print(f"[task-gc] state changed, invalid content skipped: {artifacts}")
                        continue
                    remove_task_artifacts(primary, slug)
                cleanup_task_lease_marker(primary, slug)
                counts["removed"] += 1
                print(f"[task-gc] removed legacy invalid root: {artifacts}")
            except (OSError, SystemExit) as exc:
                counts["failed"] += 1
                print(f"[task-gc] failed: {slug}: {exc}")
            continue
        target = (primary / ".worktrees" / slug).resolve()
        branch = f"codex/{slug}"
        branch_exists = git(
            ["show-ref", "--verify", "--quiet", f"refs/heads/{branch}"],
            cwd=primary, check=False,
        ).returncode == 0
        if target in registered or target.exists() or branch_exists:
            counts["protected"] += 1
            print(f"[task-gc] protected task state: {slug}")
            continue
        completed = valid_task_completion(primary, slug)
        if not completed and not adopt_legacy_orphans:
            counts["protected"] += 1
            print(f"[task-gc] orphan lacks completion evidence: {slug}")
            continue
        if task_artifacts_active(artifacts):
            counts["active"] += 1
            print(f"[task-gc] active lease: {slug}")
            continue
        modified = datetime.fromtimestamp(artifacts.stat().st_mtime, UTC)
        if modified > cutoff:
            counts["retained"] += 1
            expires = modified + timedelta(days=retention_days)
            print(f"[task-gc] retained until {expires:%Y-%m-%d %H:%M:%S%z}: {slug}")
            continue
        if not apply:
            counts["eligible"] += 1
            print(f"[task-gc] eligible: {slug}")
            continue
        try:
            with task_artifact_lease(artifacts):
                current_registered = registered_worktrees(primary)
                current_branch = git(
                    ["show-ref", "--verify", "--quiet", f"refs/heads/{branch}"],
                    cwd=primary, check=False,
                ).returncode == 0
                if target in current_registered or target.exists() or current_branch:
                    counts["protected"] += 1
                    print(f"[task-gc] state changed, protected: {slug}")
                    continue
                if not valid_task_completion(primary, slug):
                    if not adopt_legacy_orphans:
                        counts["protected"] += 1
                        print(f"[task-gc] completion evidence missing: {slug}")
                        continue
                    record_task_completion(
                        primary, slug, branch=f"codex/{slug}",
                        superseded_by="legacy-orphan-owner-authorized",
                    )
                remove_task_artifacts(primary, slug)
            cleanup_task_lease_marker(primary, slug)
            counts["removed"] += 1
            print(f"[task-gc] removed: {slug}")
        except (OSError, SystemExit) as exc:
            counts["failed"] += 1
            print(f"[task-gc] failed: {slug}: {exc}")

    lease_root = primary / ".artifacts" / "task-leases"
    if lease_root.is_dir():
        for marker in sorted(lease_root.glob(f"*{TASK_LEASE}")):
            slug = marker.name[: -len(TASK_LEASE)]
            if not SLUG_PATTERN.fullmatch(slug):
                continue
            artifacts = primary / ".artifacts" / "worktrees" / slug
            target = (primary / ".worktrees" / slug).resolve()
            branch = f"codex/{slug}"
            branch_exists = git(
                ["show-ref", "--verify", "--quiet", f"refs/heads/{branch}"],
                cwd=primary, check=False,
            ).returncode == 0
            if artifacts.exists() or target.exists() or branch_exists:
                continue
            if task_artifacts_active(artifacts):
                counts["active"] += 1
                continue
            if not apply:
                counts["eligible"] += 1
                print(f"[task-gc] eligible orphan lease marker: {marker}")
                continue
            if cleanup_task_lease_marker(primary, slug):
                counts["removed"] += 1
                print(f"[task-gc] removed orphan lease marker: {marker.name}")

    print("[task-gc] summary " + " ".join(f"{key}={value}" for key, value in counts.items()))
    if counts["failed"]:
        raise SystemExit("部分任务工件清理失败")


def ready(cwd: Path, *, ui: bool, rust: bool, package: bool, accept_ci: bool = False) -> None:
    branch = git(["branch", "--show-current"], cwd=cwd).stdout.strip()
    status = git(["status", "--porcelain"], cwd=cwd).stdout.strip()
    behind = False
    if not accept_ci:
        behind_origin = bool(git(
            ["merge-base", "--is-ancestor", "origin/main", "HEAD"], cwd=cwd, check=False,
        ).returncode)
        behind_local = bool(git(
            ["merge-base", "--is-ancestor", "main", "HEAD"], cwd=cwd, check=False,
        ).returncode)
        behind = behind_origin or behind_local
    task_changes = task_changed_paths(cwd)
    validate_ready_state(branch, status, behind, task_changes)
    integration_base = git(["merge-base", "main", "HEAD"], cwd=cwd).stdout.strip()
    identity = full_validation_identity(
        cwd, base=integration_base, ui=ui, rust=rust, package=package,
    )
    if has_full_validation(cwd, identity):
        print("[task] identical clean-commit full validation already passed; reusing evidence")
        print("[task] READY: 可 squash 为一个独立 main 提交；仅在明确发布时更新版本元数据")
        return
    if accept_ci:
        owner, repo = github_remote_repo(cwd)
        ci_runs = green_ci_runs(owner, repo, str(identity["commit"]))
        record_full_validation(cwd, identity)
        for ci_run in ci_runs:
            print(f"[task] CI full-gate evidence: {ci_run.get('html_url')}")
        print("[task] READY（复用绿色 CI）：可 squash 为一个独立 main 提交；仅在明确发布时更新版本元数据")
        return
    args = [str(project_python(cwd)), "scripts/ci/run.py", "--full"]
    if ui:
        args.append("--ui")
    if rust:
        args.append("--rust")
    if package:
        args.append("--package")
    run(args, cwd=cwd)
    record_full_validation(cwd, identity)
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


def superseding_main_commit(primary: Path, commit: str | None) -> str | None:
    if commit is None:
        return None
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise SystemExit("--superseded-by 必须是完整的 40 位小写 Git commit SHA")
    exists = git(
        ["cat-file", "-e", f"{commit}^{{commit}}"], cwd=primary, check=False,
    ).returncode == 0
    on_main = git(
        ["merge-base", "--is-ancestor", commit, "main"], cwd=primary, check=False,
    ).returncode == 0
    if not exists or not on_main:
        raise SystemExit(f"替代证据提交不在 main 中：{commit}")
    return commit


def _remove_locked(
    slug: str, *, superseded_by: str | None = None,
    adopt_partial_removal: bool = False,
) -> None:
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
    replacement = superseding_main_commit(primary, superseded_by)
    retry_command = (
        f".\\.venv\\Scripts\\python.exe scripts/dev/tasks.py remove {slug}"
        + (f" --superseded-by {replacement}" if replacement else "")
    )
    if not branch_exists and not registered and not target.exists():
        artifacts = primary / ".artifacts" / "worktrees" / slug
        if replacement is not None:
            record_task_completion(
                primary, slug, branch=branch, superseded_by=replacement,
            )
        elif artifacts.exists() and not valid_task_completion(primary, slug):
            raise SystemExit(
                f"{branch} 仅剩孤儿工件但缺少完成凭据；"
                "请使用 gc --adopt-legacy-orphans 做一次性所有者授权清理"
            )
        remove_task_artifacts(primary, slug, retry_command=retry_command)
        print(f"[task] {branch} 已清理")
        return
    if branch_exists and not task_integrated(primary, branch) and replacement is None:
        raise SystemExit(f"{branch} 尚未完整 squash 到 main，拒绝移除")
    if adopt_partial_removal:
        if registered or not target.exists() or (target / ".git").exists():
            raise SystemExit("--adopt-partial-removal 仅用于未登记且缺少 .git 的残留 checkout")
        record_task_remove_intent(primary, slug, branch=branch)
    if registered:
        if git(["status", "--porcelain"], cwd=target).stdout.strip():
            raise SystemExit("worktree 不干净，拒绝移除")
        remove_primary_venv_link(target, primary)
        record_task_remove_intent(primary, slug, branch=branch)
        result = git(["worktree", "remove", str(target)], cwd=primary, check=False)
        still_registered = target in registered_worktrees(primary)
        if result.returncode and still_registered:
            task_remove_intent_path(primary, slug).unlink(missing_ok=True)
            detail = result.stderr.strip() or result.stdout.strip() or "unknown Git error"
            raise RuntimeError(f"Git worktree 移除失败：{detail}")
        if target.exists():
            remove_verified_residual(
                primary, target, branch, retry_command=retry_command,
            )
    else:
        remove_verified_residual(
            primary, target, branch, retry_command=retry_command,
        )
    record_task_completion(
        primary, slug, branch=branch, superseded_by=replacement,
    )
    # Artifacts may be owned by the current sandbox identity while Git metadata
    # requires a different one.  Clean them before the final Git write so the
    # documented retry can finish branch removal without stranding ACLs.
    remove_task_artifacts(primary, slug, retry_command=retry_command)
    if branch_exists:
        git(["branch", "-D", branch], cwd=primary)
    task_remove_intent_path(primary, slug).unlink(missing_ok=True)
    evidence = f"; superseded by main commit {replacement}" if replacement else ""
    print(f"[task] removed {branch} and {target}{evidence}")


def remove(
    slug: str, *, superseded_by: str | None = None,
    adopt_partial_removal: bool = False,
) -> None:
    if not SLUG_PATTERN.fullmatch(slug):
        raise SystemExit("无效 slug")
    primary = primary_root(ROOT)
    artifacts = primary / ".artifacts" / "worktrees" / slug
    with task_artifact_lease(artifacts):
        _remove_locked(
            slug, superseded_by=superseded_by,
            adopt_partial_removal=adopt_partial_removal,
        )
    cleanup_task_lease_marker(primary, slug)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest="command", required=True)
    start_parser = commands.add_parser("start")
    start_parser.add_argument("slug")
    serve_parser = commands.add_parser("serve")
    serve_parser.add_argument("slug")
    serve_parser.add_argument("--open", action="store_true")
    serve_parser.add_argument("--stockdb-root")
    check_parser = commands.add_parser("check")
    check_parser.add_argument("--staged", action="store_true")
    check_parser.add_argument("--base", default="origin/main")
    ready_parser = commands.add_parser("ready")
    ready_parser.add_argument("--ui", action="store_true")
    ready_parser.add_argument("--rust", action="store_true")
    ready_parser.add_argument("--package", action="store_true")
    ready_parser.add_argument(
        "--accept-ci", action="store_true",
        help="复用同一 commit 上绿色完整 CI 作为验证证据，不再本地重跑全套",
    )
    remove_parser = commands.add_parser("remove")
    remove_parser.add_argument("slug")
    remove_parser.add_argument("--superseded-by")
    remove_parser.add_argument("--adopt-partial-removal", action="store_true")
    gc_parser = commands.add_parser("gc")
    gc_parser.add_argument("--apply", action="store_true")
    gc_parser.add_argument("--retention-days", type=int, default=7)
    gc_parser.add_argument("--adopt-legacy-orphans", action="store_true")
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    cwd = Path.cwd().resolve()
    try:
        if args.command == "start":
            start(args.slug)
        elif args.command == "serve":
            serve(
                args.slug, open_browser=args.open,
                stockdb_root=args.stockdb_root,
            )
        elif args.command == "check":
            check(cwd, staged=args.staged, base=args.base)
        elif args.command == "ready":
            ready(
                cwd, ui=args.ui, rust=args.rust, package=args.package,
                accept_ci=args.accept_ci,
            )
        elif args.command == "remove":
            remove(
                args.slug, superseded_by=args.superseded_by,
                adopt_partial_removal=args.adopt_partial_removal,
            )
        elif args.command == "gc":
            gc_task_artifacts(
                apply=args.apply, retention_days=args.retention_days,
                adopt_legacy_orphans=args.adopt_legacy_orphans,
            )
    except (RuntimeError, subprocess.CalledProcessError) as exc:
        print(f"[task] FAILED: {redact_public_text(exc)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
