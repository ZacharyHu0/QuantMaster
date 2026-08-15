"""Contracts for isolated task development and impact-based validation."""

import json
import os
import socket
import stat
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from scripts.dev.pytest_windows_acl import prepare_pytest_directory
from scripts.dev.tasks import (
    Impact,
    full_validation_identity,
    gc_task_artifacts,
    has_full_validation,
    ready,
    record_full_validation,
    record_task_remove_intent,
    remove,
    remove_empty_residual,
    remove_primary_venv_link,
    remove_task_artifacts,
    remove_verified_residual,
    select_impact,
    superseding_main_commit,
    task_artifact_lease,
    task_artifacts_active,
    task_changed_paths,
    task_remove_intent_path,
    validate_ready_state,
)

ROOT = Path(__file__).resolve().parents[1]


def _assert_ordered(text: str, *needles: str) -> None:
    positions = [text.index(needle) for needle in needles]
    assert positions == sorted(positions)


def _windows_cleanup_error(code: int, path: Path) -> OSError:
    error = OSError(f"locked: {path}")
    error.winerror = code
    error.filename = str(path)
    return error


def _hold_direct_pytest_task_lease(primary: str, ready, release) -> None:
    from scripts.dev import pytest_windows_acl

    root = Path(primary)
    worktree = root / ".worktrees" / "task"
    os.chdir(worktree)
    cleanups = []
    pytest_windows_acl._install_task_artifact_lease(
        SimpleNamespace(add_cleanup=cleanups.append),
    )
    ready.set()
    if not release.wait(10):
        raise RuntimeError("test did not release direct pytest lease")
    cleanups.pop()()


def _temporary_task_repo(
    tmp_path: Path, **branches: str,
) -> tuple[Path, Path, dict[str, Path]]:
    primary = tmp_path / "primary"
    primary.mkdir()
    python = primary / ".venv" / "Scripts" / "python.exe"
    python.parent.mkdir(parents=True)
    python.touch()
    targets: dict[str, Path] = {}
    for slug in branches:
        target = primary / ".worktrees" / slug
        target.mkdir(parents=True)
        targets[slug] = target
    return primary, python, targets


def _capture_task_launches(
    monkeypatch, tasks, primary: Path, python: Path,
    targets: dict[str, Path], branches: dict[str, str],
) -> list[dict]:
    launches = []
    real_run = subprocess.run

    def capture_process(command, *args, **kwargs):
        if command and command[0] == str(python):
            artifacts = primary / ".artifacts" / "worktrees" / kwargs["cwd"].name
            launches.append({
                "command": command, **kwargs,
                "leased": tasks.task_artifacts_active(artifacts),
            })
            return subprocess.CompletedProcess(command, 0)
        if command and command[0] == "git":
            assert command[:3] == [
                "git", "-c", f"safe.directory={Path(kwargs['cwd']).as_posix()}",
            ]
            git_args = command[3:]
            if git_args == ["worktree", "list", "--porcelain"]:
                records = [f"worktree {primary}", "branch refs/heads/main"]
                records.extend(
                    line
                    for slug, target in targets.items()
                    for line in (f"worktree {target}", f"branch refs/heads/{branches[slug]}")
                )
                return subprocess.CompletedProcess(command, 0, stdout="\n".join(records), stderr="")
            if git_args == ["branch", "--show-current"]:
                return subprocess.CompletedProcess(
                    command, 0, stdout=branches[Path(kwargs["cwd"]).name] + "\n", stderr="",
                )
        return real_run(command, *args, **kwargs)

    monkeypatch.setattr(tasks, "ROOT", primary)
    monkeypatch.setattr(tasks.subprocess, "run", capture_process)
    return launches


def test_serve_runs_registered_tasks_with_isolated_runtime(tmp_path, monkeypatch):
    from scripts.dev import tasks

    branches = {"alpha": "codex/alpha", "beta": "codex/beta"}
    primary, python, targets = _temporary_task_repo(tmp_path, **branches)
    stable_stockdb = tmp_path / "stable-stockdb"
    stable_stockdb.mkdir()
    launches = _capture_task_launches(
        monkeypatch, tasks, primary, python, targets, branches,
    )

    assert tasks.main([
        "serve", "alpha", "--open", "--stockdb-root", str(stable_stockdb),
    ]) == 0
    assert tasks.main(["serve", "beta"]) == 0

    alpha_dev = primary / ".artifacts/worktrees/alpha/runtime/dev"
    beta_dev = primary / ".artifacts/worktrees/beta/runtime/dev"
    alpha, beta = launches
    assert alpha["command"] == [
        str(python), "-m", "quantmaster.cli", "serve", "--reload", "--open",
    ]
    assert alpha["cwd"] == targets["alpha"]
    assert json.loads((alpha_dev / "config.yaml").read_text(encoding="utf-8")) == {
        "server": {"host": "127.0.0.1", "port": 18686},
        "data": {"free_stockdb_managed": False, "free_stockdb_auto_update": False},
    }
    assert alpha["env"]["QM_CONFIG_PATH"] == str(alpha_dev / "config.yaml")
    assert alpha["env"]["QM_DATA_ROOT"] == str(alpha_dev / "data")
    assert alpha["env"]["QM_FREE_STOCKDB_ROOT"] == str(alpha_dev / "free-stockdb")
    assert alpha["env"]["QM_FREE_STOCKDB_SDK_PATH"] == str(stable_stockdb / "pybao")
    assert alpha["env"]["QM_FREE_STOCKDB_CONTROL_PATH"] == str(alpha_dev / "control.sqlite")
    assert alpha["env"]["QM_FREE_STOCKDB_MANAGED"] == "false"
    assert alpha["env"]["QM_FREE_STOCKDB_AUTO_UPDATE"] == "false"
    assert (alpha_dev / "data/logs").is_dir()
    assert (alpha_dev / "free-stockdb").is_dir()
    assert beta["cwd"] == targets["beta"]
    assert json.loads((beta_dev / "config.yaml").read_text(encoding="utf-8"))["server"] == {
        "host": "127.0.0.1", "port": 18687,
    }
    assert beta["env"]["QM_CONFIG_PATH"] == str(beta_dev / "config.yaml")
    assert beta["env"]["QM_DATA_ROOT"] == str(beta_dev / "data")
    assert beta["env"]["QM_FREE_STOCKDB_CONTROL_PATH"] == str(beta_dev / "control.sqlite")
    assert alpha["leased"] is beta["leased"] is True


def test_serve_port_allocation_ignores_non_task_worktrees(tmp_path, monkeypatch):
    from scripts.dev import tasks

    branches = {"aaa": "feature/not-a-task", "alpha": "codex/alpha"}
    primary, python, targets = _temporary_task_repo(tmp_path, **branches)
    launches = _capture_task_launches(
        monkeypatch, tasks, primary, python, targets, branches,
    )

    assert tasks.main(["serve", "alpha"]) == 0
    config = json.loads(Path(launches[0]["env"]["QM_CONFIG_PATH"]).read_text(encoding="utf-8"))
    assert config["server"]["port"] == 18686


def test_serve_rejects_invalid_task_and_stockdb_before_launch(tmp_path, monkeypatch):
    from scripts.dev import tasks

    branches = {"alpha": "codex/alpha"}
    primary, python, targets = _temporary_task_repo(tmp_path, **branches)
    launches = _capture_task_launches(
        monkeypatch, tasks, primary, python, targets, branches,
    )

    with pytest.raises(SystemExit, match="未登记"):
        tasks.main(["serve", "missing"])
    with pytest.raises(SystemExit, match="必须是绝对路径"):
        tasks.main(["serve", "alpha", "--stockdb-root", "relative-stockdb"])
    assert launches == []


def test_serve_rejects_occupied_port_before_launch(tmp_path, monkeypatch):
    from scripts.dev import tasks

    branches = {"alpha": "codex/alpha"}
    primary, python, targets = _temporary_task_repo(tmp_path, **branches)
    launches = _capture_task_launches(
        monkeypatch, tasks, primary, python, targets, branches,
    )
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 18686))
    try:
        with pytest.raises(SystemExit, match="开发端口已被占用"):
            tasks.main(["serve", "alpha"])
    finally:
        listener.close()
    assert launches == []


def test_documentation_only_changes_skip_python_tests():
    assert select_impact(["README.md", "docs/guide.md"]) == Impact("docs")


def test_task_artifact_lease_is_external_and_reusable_after_use(tmp_path):
    artifacts = tmp_path / ".artifacts" / "worktrees" / "task"
    with task_artifact_lease(artifacts):
        marker = tmp_path / ".artifacts" / "task-leases" / "task.task-running.lock"
        assert marker.is_file()
        assert task_artifacts_active(artifacts) is True
    assert marker.is_file()
    assert task_artifacts_active(artifacts) is False


def test_task_runner_marks_outer_lease_for_child_process(monkeypatch, tmp_path):
    from scripts.dev import tasks

    primary = tmp_path / "primary"
    worktree = primary / ".worktrees" / "task"
    worktree.mkdir(parents=True)
    monkeypatch.setattr(tasks, "primary_root", lambda cwd: primary)

    def child(command, *, cwd, env, check):
        artifacts = primary / ".artifacts" / "worktrees" / "task"
        assert env["QM_TASK_LEASE_HELD"] == str(artifacts.resolve())
        assert task_artifacts_active(artifacts) is True

    monkeypatch.setattr(tasks.subprocess, "run", child)

    tasks.run(["python", "check.py"], cwd=worktree)


def test_direct_pytest_lease_blocks_cleanup_across_processes(tmp_path):
    import multiprocessing

    primary = tmp_path / "primary"
    worktree = primary / ".worktrees" / "task"
    artifacts = primary / ".artifacts" / "worktrees" / "task"
    worktree.mkdir(parents=True)
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    release = context.Event()
    process = context.Process(
        target=_hold_direct_pytest_task_lease,
        args=(str(primary), ready, release),
    )
    process.start()
    try:
        assert ready.wait(10)
        assert task_artifacts_active(artifacts) is True
        with pytest.raises(RuntimeError, match="另一进程"):
            with task_artifact_lease(artifacts):
                pytest.fail("cleanup lease must not overlap direct pytest")
    finally:
        release.set()
        process.join(10)
    assert process.exitcode == 0
    assert task_artifacts_active(artifacts) is False


def test_gc_removes_only_orphan_artifacts_and_protects_task_state(monkeypatch, tmp_path):
    from scripts.dev import tasks

    primary = tmp_path / "primary"
    orphan = primary / ".artifacts" / "worktrees" / "orphan"
    protected = primary / ".artifacts" / "worktrees" / "protected"
    orphan.mkdir(parents=True)
    protected.mkdir(parents=True)
    (primary / ".worktrees" / "protected").mkdir(parents=True)

    class Result:
        returncode = 1

    monkeypatch.setattr(tasks, "primary_root", lambda cwd: primary)
    monkeypatch.setattr(tasks, "registered_worktrees", lambda root: set())
    monkeypatch.setattr(tasks, "valid_task_completion", lambda root, slug: True)
    monkeypatch.setattr(tasks, "git", lambda *args, **kwargs: Result())
    gc_task_artifacts(apply=True, retention_days=0, adopt_legacy_orphans=True)

    assert not orphan.exists()
    assert protected.exists()


def test_gc_removes_legacy_invalid_root_with_only_disposable_content(monkeypatch, tmp_path):
    from scripts.dev import tasks

    primary = tmp_path / "primary"
    invalid = primary / ".artifacts" / "worktrees" / "Quant"
    (invalid / "cache").mkdir(parents=True)
    (invalid / "pytest").mkdir(parents=True)

    class Result:
        returncode = 1

    monkeypatch.setattr(tasks, "primary_root", lambda cwd: primary)
    monkeypatch.setattr(tasks, "registered_worktrees", lambda root: set())
    monkeypatch.setattr(tasks, "git", lambda *args, **kwargs: Result())
    gc_task_artifacts(apply=True, retention_days=7)

    assert not invalid.exists()


def test_gc_removes_orphan_lease_marker_when_artifact_root_is_gone(monkeypatch, tmp_path):
    from scripts.dev import tasks

    primary = tmp_path / "primary"
    marker = primary / ".artifacts" / "task-leases" / "old.task-running.lock"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text("0", encoding="utf-8")

    class Result:
        returncode = 1
        stdout = ""

    monkeypatch.setattr(tasks, "primary_root", lambda cwd: primary)
    monkeypatch.setattr(tasks, "registered_worktrees", lambda root: set())
    monkeypatch.setattr(tasks, "valid_task_completion", lambda root, slug: True)
    monkeypatch.setattr(tasks, "git", lambda *args, **kwargs: Result())
    gc_task_artifacts(apply=True, retention_days=0, adopt_legacy_orphans=True)

    assert not marker.exists()


def test_gc_preserves_lease_marker_for_protected_task(monkeypatch, tmp_path):
    from scripts.dev import tasks

    primary = tmp_path / "primary"
    artifacts = primary / ".artifacts" / "worktrees" / "active"
    (primary / ".worktrees" / "active").mkdir(parents=True)
    artifacts.mkdir(parents=True)
    marker = primary / ".artifacts" / "task-leases" / "active.task-running.lock"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text("0", encoding="utf-8")

    class Result:
        returncode = 1
        stdout = ""

    monkeypatch.setattr(tasks, "primary_root", lambda cwd: primary)
    monkeypatch.setattr(tasks, "registered_worktrees", lambda root: set())
    monkeypatch.setattr(tasks, "valid_task_completion", lambda root, slug: True)
    monkeypatch.setattr(tasks, "git", lambda *args, **kwargs: Result())
    gc_task_artifacts(apply=True, retention_days=0, adopt_legacy_orphans=True)

    assert marker.exists()
    assert artifacts.exists()


def test_gc_preserves_legacy_invalid_root_with_unknown_content(monkeypatch, tmp_path):
    from scripts.dev import tasks

    primary = tmp_path / "primary"
    invalid = primary / ".artifacts" / "worktrees" / "Quant"
    invalid.mkdir(parents=True)
    (invalid / "notes.md").write_text("keep me", encoding="utf-8")

    class Result:
        returncode = 1

    monkeypatch.setattr(tasks, "primary_root", lambda cwd: primary)
    monkeypatch.setattr(tasks, "registered_worktrees", lambda root: set())
    monkeypatch.setattr(tasks, "git", lambda *args, **kwargs: Result())
    gc_task_artifacts(apply=True, retention_days=7)

    assert (invalid / "notes.md").is_file()


def test_gc_requires_completion_evidence_and_honors_retention(monkeypatch, tmp_path):
    import os
    import time

    from scripts.dev import tasks

    primary = tmp_path / "primary"
    missing = primary / ".artifacts" / "worktrees" / "missing"
    recent = primary / ".artifacts" / "worktrees" / "recent"
    expired = primary / ".artifacts" / "worktrees" / "expired"
    for path in (missing, recent, expired):
        path.mkdir(parents=True)
    old = time.time() - 10 * 86400
    os.utime(expired, (old, old))

    class Result:
        returncode = 1

    monkeypatch.setattr(tasks, "primary_root", lambda cwd: primary)
    monkeypatch.setattr(tasks, "registered_worktrees", lambda root: set())
    monkeypatch.setattr(tasks, "git", lambda *args, **kwargs: Result())
    monkeypatch.setattr(
        tasks, "valid_task_completion", lambda root, slug: slug != "missing",
    )
    gc_task_artifacts(apply=True, retention_days=7)

    assert missing.exists()
    assert recent.exists()
    assert not expired.exists()


def test_data_changes_select_adjacent_contracts_and_architecture():
    impact = select_impact(["quantmaster/data/storage.py"])
    assert impact.mode == "selected"
    assert "tests/test_architecture.py" in impact.tests
    assert "tests/test_data_resilience.py" in impact.tests


def test_test_only_change_does_not_pay_for_unrelated_architecture_scan():
    impact = select_impact(["tests/test_news_workbench.py"])
    assert "tests/test_architecture.py" not in impact.tests


def test_changelog_is_documentation_only():
    assert select_impact(["CHANGELOG.md"]) == Impact("docs")


def test_static_server_change_includes_browser_contract():
    impact = select_impact(["quantmaster/server/static/app.js"])
    assert impact.mode == "selected"
    assert "tests/test_server.py" in impact.tests
    assert "tests/test_ui_management.py" in impact.tests


def test_rotation_change_selects_full_only_rotation_tests():
    impact = select_impact(["quantmaster/rotation/service.py"])
    assert impact.mode == "selected"
    assert "tests/test_rotation_store_service.py" in impact.tests


def test_explicit_test_change_runs_that_test_with_full_semantics():
    impact = select_impact(["tests/test_news_workbench.py"])
    assert impact.mode == "selected"
    assert "tests/test_news_workbench.py" in impact.tests


def test_infrastructure_and_mapping_changes_force_full_suite():
    assert select_impact(["tests/conftest.py"]).mode == "full"
    assert select_impact(["scripts/dev/test-impact.json"]).mode == "full"


def test_unknown_paths_fail_safe_to_full_suite():
    impact = select_impact(["unexpected/new-contract.txt"])
    assert impact.mode == "full"
    assert impact.unknown == ("unexpected/new-contract.txt",)


def test_impact_map_references_existing_tests():
    config = __import__("json").loads(
        (ROOT / "scripts/dev/test-impact.json").read_text(encoding="utf-8")
    )
    referenced = set(config["always"])
    for rule in config["rules"]:
        referenced.update(rule.get("tests", []))
    missing = sorted(path for path in referenced if not (ROOT / path).is_file())
    assert missing == []


def test_ready_state_accepts_clean_current_task_branch():
    validate_ready_state("codex/storage-fix", "", False, ["quantmaster/data/storage.py"])


def test_ready_for_review_event_escalates_the_pull_request_ci_matrix() -> None:
    workflow = yaml.load(
        (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"),
        Loader=yaml.BaseLoader,
    )

    assert workflow["on"]["pull_request"]["types"] == [
        "opened",
        "synchronize",
        "reopened",
        "ready_for_review",
        "converted_to_draft",
    ]
    assert workflow["on"]["push"]["branches"] == ["main", "claude/**"]
    jobs = workflow["jobs"]
    assert jobs["fast-gate"]["if"] == (
        "github.event_name == 'pull_request' && github.event.pull_request.draft == true"
    )
    assert "if" not in jobs["core"]
    for job in (
        "rust-quality",
        "native-parity",
        "coverage-shard",
        "coverage",
        "browser",
        "windows-package",
        "quality-package-audit",
    ):
        assert jobs[job]["if"] == "github.event.pull_request.draft != true"
    assert workflow["concurrency"] == {
        "group": "ci-${{ github.workflow }}-${{ github.ref }}",
        "cancel-in-progress": "${{ github.event_name == 'pull_request' }}",
    }


def test_documented_integration_order_has_no_ready_accept_ci_cycle() -> None:
    contracts = {
        "AGENTS.md": (
            "Push the aligned commit while the PR is still Draft",
            "Draft fast/core",
            "mark the PR Ready",
            "full CI matrix",
            "`tasks.py ready --accept-ci`",
            "squash-merge",
        ),
        "docs/development-workflow.md": (
            "Push the aligned commit while the PR is still Draft",
            "Draft fast/core",
            "mark the PR Ready",
            "full CI matrix",
            "`tasks.py ready --accept-ci`",
            "squash-merge",
        ),
        "docs/github-workflow.md": (
            "Draft fast/core",
            "标记 Ready",
            "完整 CI",
            "`tasks.py ready --accept-ci`",
            "squash merge",
        ),
        ".github/pull_request_template.md": (
            "Draft fast/core",
            "标记 Ready",
            "完整 CI",
            "tasks.py ready --accept-ci",
            "squash merge",
        ),
    }
    for relative, needles in contracts.items():
        _assert_ordered((ROOT / relative).read_text(encoding="utf-8"), *needles)


def test_ready_state_rejects_main_dirty_behind_and_version_changes():
    import pytest

    with pytest.raises(SystemExit, match="codex"):
        validate_ready_state("main", "", False, [])
    with pytest.raises(SystemExit, match="不干净"):
        validate_ready_state("codex/task", "M file.py", False, [])
    with pytest.raises(SystemExit, match="落后"):
        validate_ready_state("codex/task", "", True, [])
    with pytest.raises(SystemExit, match="版本元数据"):
        validate_ready_state("codex/task", "", False, ["quantmaster/release.py"])
    with pytest.raises(SystemExit, match="版本元数据"):
        validate_ready_state("codex/task", "", False, ["CHANGELOG.md"])


def test_ready_state_rejects_task_changelog_updates():
    import pytest

    with pytest.raises(SystemExit, match="版本元数据"):
        validate_ready_state("codex/task", "", False, ["CHANGELOG.md"])


def test_accept_ci_ready_uses_selected_merge_base_when_main_advances(
    monkeypatch,
    tmp_path,
) -> None:
    from scripts.dev import tasks

    git_calls = []

    def fake_git(args, *, cwd, check=True):
        git_calls.append(args)
        if args == ["branch", "--show-current"]:
            return SimpleNamespace(stdout="codex/task\n", returncode=0)
        if args == ["status", "--porcelain"]:
            return SimpleNamespace(stdout="", returncode=0)
        if args == ["merge-base", "main", "HEAD"]:
            return SimpleNamespace(stdout="selected-base\n", returncode=0)
        pytest.fail(f"accept-ci must not follow moving main ancestry: {args}")

    observed = {}
    monkeypatch.setattr(tasks, "git", fake_git)
    monkeypatch.setattr(tasks, "task_changed_paths", lambda _cwd: ["scripts/dev/tasks.py"])
    monkeypatch.setattr(
        tasks,
        "full_validation_identity",
        lambda _cwd, **kwargs: observed.setdefault(
            "identity",
            {"commit": "exact-head", "base": kwargs["base"]},
        ),
    )
    monkeypatch.setattr(tasks, "has_full_validation", lambda *_args: False)
    monkeypatch.setattr(tasks, "github_remote_repo", lambda _cwd: ("owner", "repo"))

    def green_ci_runs(owner, repo, sha):
        observed["ci"] = (owner, repo, sha)
        return [{"html_url": "https://example.test/full"}]

    monkeypatch.setattr(tasks, "green_ci_runs", green_ci_runs)
    monkeypatch.setattr(
        tasks,
        "record_full_validation",
        lambda _cwd, identity: observed.setdefault("recorded", identity),
    )

    ready(tmp_path, ui=False, rust=False, package=False, accept_ci=True)

    assert git_calls == [
        ["branch", "--show-current"],
        ["status", "--porcelain"],
        ["merge-base", "main", "HEAD"],
    ]
    assert observed["identity"]["base"] == "selected-base"
    assert observed["ci"] == ("owner", "repo", "exact-head")
    assert observed["recorded"] == observed["identity"]


def test_local_ready_still_rejects_a_branch_behind_main(monkeypatch, tmp_path) -> None:
    from scripts.dev import tasks

    def fake_git(args, *, cwd, check=True):
        if args == ["branch", "--show-current"]:
            return SimpleNamespace(stdout="codex/task\n", returncode=0)
        if args == ["status", "--porcelain"]:
            return SimpleNamespace(stdout="", returncode=0)
        if args == ["merge-base", "main", "HEAD"]:
            return SimpleNamespace(stdout="selected-base\n", returncode=0)
        if args[:2] == ["merge-base", "--is-ancestor"]:
            return SimpleNamespace(stdout="", returncode=1)
        raise AssertionError(args)

    monkeypatch.setattr(tasks, "git", fake_git)
    monkeypatch.setattr(tasks, "task_changed_paths", lambda _cwd: [])

    with pytest.raises(SystemExit, match="落后于 origin/main"):
        ready(tmp_path, ui=False, rust=False, package=False)


def test_task_changed_paths_excludes_inherited_main_history(monkeypatch, tmp_path):
    from scripts.dev import tasks

    observed = []

    def lines(args, *, cwd):
        observed.append((args, cwd))
        return ["quantmaster/data/reference_market.py"]

    monkeypatch.setattr(tasks, "git_lines", lines)

    assert task_changed_paths(tmp_path) == ["quantmaster/data/reference_market.py"]
    assert observed == [(
        ["diff", "--name-only", "--diff-filter=ACMR", "main...HEAD"], tmp_path,
    )]


def test_full_validation_evidence_requires_exact_identity(monkeypatch, tmp_path):
    from scripts.dev import tasks

    evidence = tmp_path / "full.json"
    python = tmp_path / "python.exe"
    python.touch()
    monkeypatch.setattr(tasks, "validation_evidence_path", lambda cwd: evidence)
    monkeypatch.setattr(tasks, "project_python", lambda cwd: python)
    monkeypatch.setattr(tasks, "project_environment_identity", lambda python, *, cwd: "env")
    monkeypatch.setattr(
        tasks, "git",
        lambda args, *, cwd, check=True: SimpleNamespace(
            stdout="" if args[0] == "status" else ("task-sha\n" if args[-1] == "HEAD" else "base-sha\n")
        ),
    )
    identity = full_validation_identity(tmp_path, base="origin/main")
    record_full_validation(tmp_path, identity)

    assert has_full_validation(tmp_path, identity)
    assert not has_full_validation(tmp_path, {**identity, "ui": True})


def test_prepare_pytest_cache_precreates_directory_without_acl_probe(monkeypatch, tmp_path):
    from quantmaster.runtime import storage_governance

    cache = tmp_path / "task-artifacts" / "pytest" / "cache"
    monkeypatch.setattr(
        storage_governance,
        "inspect_acl",
        lambda _path: pytest.fail("routine pytest preparation must not inspect ACLs"),
    )

    assert prepare_pytest_directory(cache) == cache
    assert cache.is_dir()


def test_windows_pytest_plugin_preserves_precreated_basetemp(monkeypatch, tmp_path):
    from scripts.dev import pytest_windows_acl

    monkeypatch.setattr(
        pytest_windows_acl,
        "os",
        SimpleNamespace(name="nt", access=os.access, W_OK=os.W_OK),
    )
    basetemp = tmp_path / "pytest" / "run"
    factory = SimpleNamespace(_given_basetemp=basetemp, _basetemp=None)
    cache = tmp_path / "pytest" / "cache"
    cleanups = []
    config = SimpleNamespace(
        cache=SimpleNamespace(_cachedir=cache),
        _tmp_path_factory=factory,
        add_cleanup=cleanups.append,
    )
    pytest_windows_acl.pytest_configure(config)

    assert factory._basetemp == basetemp.resolve()
    assert basetemp.is_dir()
    assert cache.is_dir()
    assert len(cleanups) == 1
    cleanups.pop()()


def test_windows_pytest_plugin_prevents_pytest_from_replacing_prepared_basetemp(
    monkeypatch, tmp_path,
):
    from _pytest.tmpdir import TempPathFactory

    from scripts.dev import pytest_windows_acl

    monkeypatch.setattr(
        pytest_windows_acl,
        "os",
        SimpleNamespace(name="nt", access=os.access, W_OK=os.W_OK),
    )
    basetemp = tmp_path / "pytest" / "full-1"
    factory = TempPathFactory(
        given_basetemp=basetemp,
        retention_count=3,
        retention_policy="all",
        trace=lambda *_args: None,
        basetemp=None,
        _ispytest=True,
    )
    cleanups = []
    config = SimpleNamespace(
        cache=None,
        _tmp_path_factory=factory,
        add_cleanup=cleanups.append,
    )

    pytest_windows_acl.pytest_configure(config)
    sentinel = basetemp / "prepared-by-task-tooling"
    sentinel.write_text("keep", encoding="utf-8")

    assert factory.getbasetemp() == basetemp.resolve()
    assert sentinel.read_text(encoding="utf-8") == "keep"
    cleanups.pop()()


@pytest.mark.skipif(os.name != "nt", reason="Windows ACL contract")
def test_windows_pytest_fixture_tree_inherits_acl_and_is_removable(tmp_path):
    import shutil

    from _pytest.tmpdir import TempPathFactory

    from quantmaster.runtime.storage_governance import inspect_acl
    from scripts.dev import pytest_windows_acl

    basetemp = tmp_path / "prepared-basetemp"
    factory = TempPathFactory(
        given_basetemp=basetemp,
        retention_count=3,
        retention_policy="all",
        trace=lambda *_args: None,
        basetemp=None,
        _ispytest=True,
    )
    cleanups = []
    config = SimpleNamespace(
        cache=None,
        _tmp_path_factory=factory,
        add_cleanup=cleanups.append,
    )
    pytest_windows_acl.pytest_configure(config)

    fixture = factory.mktemp("catalog-seed")
    nested = fixture / "second-level"
    nested.mkdir()

    assert inspect_acl(basetemp).inherited is True
    assert inspect_acl(fixture).inherited is True
    assert inspect_acl(nested).inherited is True
    shutil.rmtree(basetemp)
    assert not basetemp.exists()
    cleanups.pop()()


def test_windows_pytest_plugin_is_inert_on_other_platforms(monkeypatch, tmp_path):
    from scripts.dev import pytest_windows_acl

    monkeypatch.setattr(pytest_windows_acl, "os", SimpleNamespace(name="posix"))
    basetemp = tmp_path / "pytest" / "run"
    factory = SimpleNamespace(_given_basetemp=basetemp, _basetemp=None)
    cache = tmp_path / "pytest" / "cache"
    cleanups = []
    config = SimpleNamespace(
        cache=SimpleNamespace(_cachedir=cache),
        _tmp_path_factory=factory,
        add_cleanup=cleanups.append,
    )

    pytest_windows_acl.pytest_configure(config)

    assert factory._basetemp is None
    assert not basetemp.exists()
    assert not cache.exists()
    assert cleanups == []


def test_remove_empty_residual_is_idempotent_and_rejects_content(tmp_path):
    target = tmp_path / "task"
    remove_empty_residual(target)
    target.mkdir()
    remove_empty_residual(target)
    assert not target.exists()
    target.mkdir()
    (target / "user.txt").write_text("keep", encoding="utf-8")
    import pytest

    with pytest.raises(SystemExit, match="其他内容"):
        remove_empty_residual(target)
    assert (target / "user.txt").is_file()


def test_remove_primary_venv_link_leaves_regular_directory_for_checkout_cleanup(tmp_path):
    target = tmp_path / "task"
    primary = tmp_path / "primary"
    (target / ".venv").mkdir(parents=True)
    (primary / ".venv").mkdir(parents=True)
    assert remove_primary_venv_link(target, primary) is False
    assert (target / ".venv").is_dir()


def test_remove_cleans_ignored_task_venv_after_git_leaves_residual(
    monkeypatch, tmp_path,
):
    from scripts.dev import tasks

    primary = tmp_path / "primary"
    target = primary / ".worktrees" / "recovery"
    python = target / ".venv" / "Scripts" / "python.exe"
    python.parent.mkdir(parents=True)
    python.touch()
    registered = {target}

    class Result:
        def __init__(self, returncode=0, stdout=""):
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = ""

    def fake_git(args, **_kwargs):
        if args[:2] == ["worktree", "remove"]:
            registered.clear()
        if args[0] == "rev-parse" and args[1].startswith("codex/recovery"):
            return Result(stdout="a" * 40)
        return Result()

    monkeypatch.setattr(tasks, "primary_root", lambda cwd: primary)
    monkeypatch.setattr(tasks, "registered_worktrees", lambda root: registered.copy())
    monkeypatch.setattr(tasks, "task_integrated", lambda root, branch: True)
    monkeypatch.setattr(tasks, "record_task_completion", lambda *args, **kwargs: None)
    monkeypatch.setattr(tasks, "remove_task_artifacts", lambda *args, **kwargs: None)
    monkeypatch.setattr(tasks, "git", fake_git)

    remove("recovery")

    assert not target.exists()


def test_remove_recovers_after_git_registration_was_already_removed(monkeypatch, tmp_path):
    from scripts.dev import tasks

    primary = tmp_path / "primary"
    target = primary / ".worktrees" / "recovery"
    target.mkdir(parents=True)
    calls = []

    class Result:
        def __init__(self, returncode=0):
            self.returncode = returncode
            self.stdout = ""

    def fake_git(args, **kwargs):
        calls.append(args)
        if args[:3] == ["show-ref", "--verify", "--quiet"]:
            return Result(0)
        return Result(0)

    monkeypatch.setattr(tasks, "primary_root", lambda cwd: primary)
    monkeypatch.setattr(tasks, "registered_worktrees", lambda root: {primary})
    monkeypatch.setattr(tasks, "task_integrated", lambda root, branch: True)
    monkeypatch.setattr(tasks, "record_task_remove_intent", lambda *args, **kwargs: None)
    monkeypatch.setattr(tasks, "remove_verified_residual", lambda root, path, branch: path.rmdir())
    monkeypatch.setattr(tasks, "git", fake_git)
    remove("recovery")
    assert not target.exists()
    assert ["branch", "-D", "codex/recovery"] in calls


def test_remove_preserves_branch_when_artifact_cleanup_fails(monkeypatch, tmp_path):
    from scripts.dev import tasks

    primary = tmp_path / "primary"
    artifacts = primary / ".artifacts" / "worktrees" / "recovery"
    artifacts.mkdir(parents=True)
    calls: list[str] = []

    class Result:
        returncode = 0
        stdout = ""

    def fake_git(args, **_kwargs):
        if args[:2] == ["branch", "-D"]:
            calls.append("branch")
        return Result()

    def blocked_artifacts(*_args, **_kwargs):
        calls.append("artifacts")
        raise SystemExit("Windows ACL blocked")

    monkeypatch.setattr(tasks, "primary_root", lambda cwd: primary)
    monkeypatch.setattr(tasks, "registered_worktrees", lambda root: set())
    monkeypatch.setattr(tasks, "task_integrated", lambda root, branch: True)
    monkeypatch.setattr(
        tasks, "record_task_completion", lambda *args, **kwargs: calls.append("completion"),
    )
    monkeypatch.setattr(tasks, "remove_task_artifacts", blocked_artifacts)
    monkeypatch.setattr(tasks, "git", fake_git)

    with pytest.raises(SystemExit, match="ACL blocked"):
        remove("recovery")

    assert calls == ["completion", "artifacts"]
    assert artifacts.exists()


def test_remove_intent_recovers_checkout_after_git_partially_removed_it(monkeypatch, tmp_path):
    from scripts.dev import tasks

    primary = tmp_path / "primary"
    target = primary / ".worktrees" / "recovery"
    target.mkdir(parents=True)
    (target / "runtime").mkdir()

    class Result:
        returncode = 0
        stdout = "a" * 40

    monkeypatch.setattr(tasks, "git", lambda *args, **kwargs: Result())
    record_task_remove_intent(primary, "recovery", branch="codex/recovery")
    assert task_remove_intent_path(primary, "recovery").is_file()
    remove_verified_residual(primary, target, "codex/recovery")
    assert not target.exists()


def test_remove_intent_rejects_branch_moved_after_partial_removal(monkeypatch, tmp_path):
    import pytest

    from scripts.dev import tasks

    primary = tmp_path / "primary"
    target = primary / ".worktrees" / "recovery"
    target.mkdir(parents=True)

    class Result:
        returncode = 0
        stdout = "a" * 40

    monkeypatch.setattr(tasks, "git", lambda *args, **kwargs: Result())
    record_task_remove_intent(primary, "recovery", branch="codex/recovery")
    Result.stdout = "b" * 40
    with pytest.raises(SystemExit, match="无法证明干净"):
        remove_verified_residual(primary, target, "codex/recovery")


def test_remove_explicitly_adopts_legacy_partial_checkout(monkeypatch, tmp_path):
    from scripts.dev import tasks

    primary = tmp_path / "primary"
    target = primary / ".worktrees" / "recovery"
    target.mkdir(parents=True)
    (target / "runtime").mkdir()

    class Result:
        returncode = 0
        stdout = "a" * 40

    monkeypatch.setattr(tasks, "primary_root", lambda cwd: primary)
    monkeypatch.setattr(tasks, "registered_worktrees", lambda root: set())
    monkeypatch.setattr(tasks, "task_integrated", lambda *args: True)
    monkeypatch.setattr(tasks, "remove_task_artifacts", lambda *args, **kwargs: None)
    monkeypatch.setattr(tasks, "record_task_completion", lambda *args, **kwargs: None)
    monkeypatch.setattr(tasks, "git", lambda *args, **kwargs: Result())
    remove("recovery", adopt_partial_removal=True)
    assert not target.exists()


def test_remove_refuses_partial_adoption_while_checkout_is_registered(monkeypatch, tmp_path):
    import pytest

    from scripts.dev import tasks

    primary = tmp_path / "primary"
    target = primary / ".worktrees" / "recovery"
    target.mkdir(parents=True)

    class Result:
        returncode = 0
        stdout = ""

    monkeypatch.setattr(tasks, "primary_root", lambda cwd: primary)
    monkeypatch.setattr(tasks, "registered_worktrees", lambda root: {target})
    monkeypatch.setattr(tasks, "task_integrated", lambda *args: True)
    monkeypatch.setattr(tasks, "git", lambda *args, **kwargs: Result())
    with pytest.raises(SystemExit, match="仅用于未登记"):
        remove("recovery", adopt_partial_removal=True)


def test_remove_cleans_task_artifacts_after_checkout_and_branch(monkeypatch, tmp_path):
    from scripts.dev import tasks

    primary = tmp_path / "primary"
    artifacts = primary / ".artifacts" / "worktrees" / "recovery"
    (artifacts / "pytest" / "cache").mkdir(parents=True)
    marker = primary / ".artifacts" / "task-leases" / "recovery.task-running.lock"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text("0", encoding="utf-8")

    class Result:
        returncode = 1
        stdout = ""

    monkeypatch.setattr(tasks, "primary_root", lambda cwd: primary)
    monkeypatch.setattr(tasks, "registered_worktrees", lambda root: set())
    monkeypatch.setattr(tasks, "valid_task_completion", lambda root, slug: True)
    monkeypatch.setattr(tasks, "git", lambda *args, **kwargs: Result())
    remove("recovery")
    assert not artifacts.exists()
    assert not marker.exists()


def test_remove_task_artifacts_reports_acl_block(monkeypatch, tmp_path):
    import pytest

    from scripts.dev import tasks

    primary = tmp_path / "primary"
    artifacts = primary / ".artifacts" / "worktrees" / "recovery"
    blocked = artifacts / "pytest" / "cache"
    blocked.mkdir(parents=True)
    monkeypatch.setattr(
        tasks.shutil, "rmtree",
        lambda *args, **kwargs: (_ for _ in ()).throw(PermissionError(13, "denied", blocked)),
    )
    with pytest.raises(SystemExit) as captured:
        remove_task_artifacts(primary, "recovery")
    message = str(captured.value)
    assert "kind=deletion_denied" in message
    assert "blocked=pytest/cache" in message
    assert "retry=" in message
    assert str(artifacts) not in message
    assert artifacts.exists()


def test_remove_task_artifacts_refuses_acl_recovery_outside_task_root(
    monkeypatch, tmp_path,
):
    from scripts.dev import tasks

    primary = tmp_path / "primary"
    artifacts = primary / ".artifacts" / "worktrees" / "recovery"
    outside = primary / "keep"
    artifacts.mkdir(parents=True)
    outside.mkdir()
    monkeypatch.setattr(tasks.os, "name", "nt")
    monkeypatch.setattr(
        tasks.shutil, "rmtree",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            PermissionError(13, "denied", outside),
        ),
    )
    monkeypatch.setattr(
        tasks.subprocess, "run",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("outside path must not reach PowerShell"),
        ),
    )

    with pytest.raises(SystemExit, match="任务工件之外"):
        remove_task_artifacts(primary, "recovery")

    assert outside.exists()


def test_remove_task_artifacts_retries_after_restoring_acl_inheritance(
    monkeypatch, tmp_path,
):
    from scripts.dev import pytest_windows_acl, tasks

    primary = tmp_path / "primary"
    artifacts = primary / ".artifacts" / "worktrees" / "recovery"
    artifacts.mkdir(parents=True)
    calls: list[str] = []

    def remove(path, **_kwargs):
        calls.append("remove")
        if calls.count("remove") == 1:
            raise PermissionError(13, "denied", path)

    monkeypatch.setattr(tasks.shutil, "rmtree", remove)
    monkeypatch.setattr(
        pytest_windows_acl, "os", SimpleNamespace(name="nt", environ=os.environ),
    )

    def restore(command, **_kwargs):
        assert "$ErrorActionPreference='Stop'" in command[-1]
        calls.append("restore")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(tasks.subprocess, "run", restore)

    remove_task_artifacts(primary, "recovery")

    assert calls == ["remove", "restore", "remove"]


def test_remove_task_artifacts_targets_denied_child_without_enumerating(
    monkeypatch, tmp_path,
):
    from scripts.dev import pytest_windows_acl, tasks

    primary = tmp_path / "primary"
    artifacts = primary / ".artifacts" / "worktrees" / "recovery"
    blocked = artifacts / "pytest" / "runs" / "denied"
    blocked.mkdir(parents=True)
    calls: list[str] = []
    original_rmtree = tasks.shutil.rmtree

    def remove(path, **kwargs):
        calls.append("remove")
        if calls.count("remove") == 1:
            raise PermissionError(13, "denied", blocked)
        return original_rmtree(path, **kwargs)

    def restore(command, **kwargs):
        environment = kwargs["env"]
        if "Get-ChildItem" in command[-1]:
            calls.append("enumerate")
            return SimpleNamespace(returncode=1, stdout="", stderr="access denied")
        calls.append("restore-target")
        assert type(blocked)(environment["QM_TASK_ARTIFACT_BLOCKED"]) == blocked.resolve()
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(tasks.shutil, "rmtree", remove)
    monkeypatch.setattr(
        pytest_windows_acl, "os", SimpleNamespace(name="nt", environ=os.environ),
    )
    monkeypatch.setattr(tasks.subprocess, "run", restore)

    remove_task_artifacts(primary, "recovery")

    assert calls == ["remove", "restore-target", "remove"]
    assert not artifacts.exists()


def test_remove_task_artifacts_classifies_uninspectable_acl(monkeypatch, tmp_path):
    from scripts.dev import pytest_windows_acl, tasks

    primary = tmp_path / "primary"
    artifacts = primary / ".artifacts" / "worktrees" / "recovery"
    artifacts.mkdir(parents=True)
    calls: list[str] = []

    def remove(path, **_kwargs):
        calls.append("remove")
        raise PermissionError(13, "denied", path)

    def restore(command, **kwargs):
        calls.append("inspect")
        assert "GetAccessControl" in command[-1]
        assert kwargs["env"]["QM_TASK_ARTIFACT_BLOCKED"] == str(artifacts.resolve())
        return SimpleNamespace(returncode=1, stdout="", stderr="access denied")

    monkeypatch.setattr(tasks.shutil, "rmtree", remove)
    monkeypatch.setattr(
        pytest_windows_acl, "os", SimpleNamespace(name="nt", environ=os.environ),
    )
    monkeypatch.setattr(tasks.subprocess, "run", restore)

    with pytest.raises(SystemExit) as captured:
        remove_task_artifacts(primary, "recovery")

    message = str(captured.value)
    assert "TASK_ARTIFACT_ACL_UNRECOVERABLE" in message
    assert "kind=inspection_denied" in message
    assert "blocked=." in message
    assert "retry=" in message
    assert str(artifacts) not in message
    assert calls == ["remove", "inspect"]
    assert artifacts.exists()


def test_remove_task_artifacts_classifies_transient_acl_failure(
    monkeypatch, tmp_path,
):
    from scripts.dev import pytest_windows_acl, tasks

    primary = tmp_path / "primary"
    artifacts = primary / ".artifacts" / "worktrees" / "recovery"
    blocked = artifacts / "pytest" / "runs" / "denied"
    blocked.mkdir(parents=True)

    def remove(path, **_kwargs):
        raise PermissionError(13, "denied", blocked)

    def restore(command, **_kwargs):
        assert "GetAccessControl" in command[-1]
        return SimpleNamespace(returncode=1, stdout="", stderr="PowerShell stopped")

    monkeypatch.setattr(tasks.shutil, "rmtree", remove)
    monkeypatch.setattr(
        pytest_windows_acl, "os", SimpleNamespace(name="nt", environ=os.environ),
    )
    monkeypatch.setattr(tasks.subprocess, "run", restore)

    with pytest.raises(SystemExit) as captured:
        remove_task_artifacts(primary, "recovery")

    message = str(captured.value)
    assert "kind=transient" in message
    assert "blocked=<local-path>" in message
    assert "PowerShell stopped" in message
    assert str(artifacts) not in message
    assert artifacts.exists()


@pytest.mark.parametrize("winerror", [32, 33, 145])
def test_remove_task_artifacts_reports_transient_lock_without_acl_recovery(
    monkeypatch, tmp_path, winerror,
):
    from scripts.dev import tasks

    primary = tmp_path / "primary"
    artifacts = primary / ".artifacts" / "worktrees" / "recovery"
    blocked = artifacts / "pytest" / "runs" / "locked"
    blocked.mkdir(parents=True)

    monkeypatch.setattr(
        tasks.shutil, "rmtree",
        lambda path, **_kwargs: (_ for _ in ()).throw(
            _windows_cleanup_error(winerror, blocked),
        ),
    )
    monkeypatch.setattr(
        tasks.subprocess, "run",
        lambda *args, **kwargs: pytest.fail("transient locks must not invoke PowerShell"),
    )

    with pytest.raises(SystemExit) as captured:
        remove_task_artifacts(primary, "recovery")

    message = str(captured.value)
    assert "kind=transient_lock" in message
    assert f"winerror={winerror}" in message
    assert "retry=" in message
    assert str(artifacts) not in message
    assert artifacts.exists()


@pytest.mark.parametrize(
    ("retry_winerror", "expected_kind"),
    ((32, "transient_lock"), (None, "deletion_denied")),
)
def test_remove_task_artifacts_classifies_failure_after_acl_recovery(
    monkeypatch, tmp_path, retry_winerror, expected_kind,
):
    from scripts.dev import pytest_windows_acl, tasks

    primary = tmp_path / "primary"
    artifacts = primary / ".artifacts" / "worktrees" / "recovery"
    blocked = artifacts / "pytest" / "runs" / "denied"
    blocked.mkdir(parents=True)
    calls = []

    def remove(path, **_kwargs):
        calls.append("remove")
        if calls.count("remove") == 1:
            raise PermissionError(13, "denied", blocked)
        if retry_winerror is None:
            raise PermissionError(13, "still denied", blocked)
        raise _windows_cleanup_error(retry_winerror, blocked)

    monkeypatch.setattr(tasks.shutil, "rmtree", remove)
    monkeypatch.setattr(
        pytest_windows_acl, "os", SimpleNamespace(name="nt", environ=os.environ),
    )
    monkeypatch.setattr(
        tasks.subprocess, "run",
        lambda *args, **kwargs: calls.append("restore") or SimpleNamespace(
            returncode=0, stdout="", stderr="",
        ),
    )

    with pytest.raises(SystemExit) as captured:
        remove_task_artifacts(primary, "recovery")

    message = str(captured.value)
    assert f"kind={expected_kind}" in message
    assert "retry=" in message
    assert str(artifacts) not in message
    assert calls == ["remove", "restore", "remove"]
    assert artifacts.exists()


def test_remove_recovers_acl_artifacts_after_git_state_is_gone(monkeypatch, tmp_path):
    from scripts.dev import pytest_windows_acl, tasks

    primary = tmp_path / "primary"
    artifacts = primary / ".artifacts" / "worktrees" / "recovery"
    artifacts.mkdir(parents=True)
    calls: list[str] = []
    original_rmtree = tasks.shutil.rmtree

    class Result:
        returncode = 1
        stdout = ""

    def remove_once_blocked(path, **kwargs):
        calls.append("remove")
        if calls.count("remove") == 1:
            raise PermissionError(13, "denied", path)
        return original_rmtree(path, **kwargs)

    monkeypatch.setattr(tasks, "primary_root", lambda cwd: primary)
    monkeypatch.setattr(tasks, "registered_worktrees", lambda root: set())
    monkeypatch.setattr(tasks, "valid_task_completion", lambda root, slug: True)
    monkeypatch.setattr(
        tasks, "task_artifact_lease", lambda path: tasks.contextlib.nullcontext(),
    )
    monkeypatch.setattr(tasks, "git", lambda *args, **kwargs: Result())
    monkeypatch.setattr(tasks.shutil, "rmtree", remove_once_blocked)
    monkeypatch.setattr(
        pytest_windows_acl, "os", SimpleNamespace(name="nt", environ=os.environ),
    )
    monkeypatch.setattr(
        tasks.subprocess, "run",
        lambda *args, **kwargs: calls.append("restore") or SimpleNamespace(
            returncode=0, stdout="", stderr="",
        ),
    )

    remove("recovery")
    remove("recovery")

    assert calls == ["remove", "restore", "remove"]
    assert not artifacts.exists()


def test_remove_refuses_unintegrated_recovery_branch(monkeypatch, tmp_path):
    import pytest

    from scripts.dev import tasks

    primary = tmp_path / "primary"
    target = primary / ".worktrees" / "recovery"
    task_venv = target / ".venv" / "Scripts" / "python.exe"
    task_venv.parent.mkdir(parents=True)
    task_venv.touch()

    class Result:
        returncode = 0
        stdout = ""

    monkeypatch.setattr(tasks, "primary_root", lambda cwd: primary)
    monkeypatch.setattr(tasks, "registered_worktrees", lambda root: {primary})
    monkeypatch.setattr(tasks, "task_integrated", lambda root, branch: False)
    monkeypatch.setattr(tasks, "git", lambda *args, **kwargs: Result())
    with pytest.raises(SystemExit, match="尚未完整 squash"):
        remove("recovery")
    assert task_venv.is_file()


def test_superseding_commit_must_be_immutable_main_commit(monkeypatch, tmp_path):
    import pytest

    from scripts.dev import tasks

    with pytest.raises(SystemExit, match="完整的 40 位"):
        superseding_main_commit(tmp_path, "main")

    calls = []

    class Result:
        returncode = 0

    monkeypatch.setattr(
        tasks, "git", lambda args, **kwargs: calls.append(args) or Result(),
    )
    commit = "a" * 40
    assert superseding_main_commit(tmp_path, commit) == commit
    assert ["cat-file", "-e", f"{commit}^{{commit}}"] in calls
    assert ["merge-base", "--is-ancestor", commit, "main"] in calls


def test_remove_accepts_explicit_superseding_main_commit(monkeypatch, tmp_path):
    from scripts.dev import tasks

    primary = tmp_path / "primary"
    target = primary / ".worktrees" / "recovery"
    target.mkdir(parents=True)

    class Result:
        returncode = 0
        stdout = ""

    artifact_calls = []

    monkeypatch.setattr(tasks, "primary_root", lambda cwd: primary)
    monkeypatch.setattr(tasks, "registered_worktrees", lambda root: {target})
    monkeypatch.setattr(tasks, "task_integrated", lambda root, branch: False)
    monkeypatch.setattr(tasks, "superseding_main_commit", lambda root, commit: commit)
    monkeypatch.setattr(tasks, "remove_verified_residual", lambda *args: None)
    monkeypatch.setattr(
        tasks, "remove_task_artifacts",
        lambda *args, **kwargs: artifact_calls.append(kwargs),
    )
    monkeypatch.setattr(tasks, "git", lambda *args, **kwargs: Result())

    commit = "a" * 40
    remove("recovery", superseded_by=commit)

    assert artifact_calls == [{
        "retry_command": (
            ".\\.venv\\Scripts\\python.exe scripts/dev/tasks.py remove recovery "
            f"--superseded-by {commit}"
        ),
    }]


def test_remove_verified_residual_requires_clean_checkout(monkeypatch, tmp_path):
    import pytest

    from scripts.dev import tasks

    primary = tmp_path / "primary"
    target = primary / ".worktrees" / "recovery"
    target.mkdir(parents=True)
    monkeypatch.setattr(tasks, "residual_checkout_clean", lambda *args: False)
    with pytest.raises(SystemExit, match="无法证明干净"):
        remove_verified_residual(primary, target, "codex/recovery")
    assert target.exists()


def test_remove_verified_residual_deletes_only_proven_checkout(monkeypatch, tmp_path):
    from scripts.dev import tasks

    primary = tmp_path / "primary"
    target = primary / ".worktrees" / "recovery"
    target.mkdir(parents=True)
    (target / "tracked.txt").write_text("old checkout", encoding="utf-8")
    monkeypatch.setattr(tasks, "residual_checkout_clean", lambda *args: True)
    remove_verified_residual(primary, target, "codex/recovery")
    assert not target.exists()


def test_remove_verified_residual_clears_readonly_files(monkeypatch, tmp_path):
    from scripts.dev import tasks

    primary = tmp_path / "primary"
    target = primary / ".worktrees" / "recovery"
    target.mkdir(parents=True)
    readonly = target / "cache.bin"
    readonly.write_bytes(b"cache")
    readonly.chmod(stat.S_IREAD)
    monkeypatch.setattr(tasks, "residual_checkout_clean", lambda *args: True)
    remove_verified_residual(primary, target, "codex/recovery")
    assert not target.exists()


def test_remove_verified_residual_reports_acl_block(monkeypatch, tmp_path):
    import pytest

    from scripts.dev import tasks

    primary = tmp_path / "primary"
    target = primary / ".worktrees" / "recovery"
    target.mkdir(parents=True)
    blocked = target / ".artifacts" / "pytest" / "cache"
    monkeypatch.setattr(tasks, "residual_checkout_clean", lambda *args: True)
    monkeypatch.setattr(
        tasks.shutil, "rmtree",
        lambda *args, **kwargs: (_ for _ in ()).throw(PermissionError(13, "denied", blocked)),
    )
    with pytest.raises(SystemExit, match=r"Windows ACL.*pytest[\\/]cache"):
        remove_verified_residual(primary, target, "codex/recovery")
    assert target.exists()
