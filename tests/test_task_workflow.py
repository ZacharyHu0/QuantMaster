"""Contracts for isolated task development and impact-based validation."""

import os
import stat
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.dev.pytest_windows_acl import prepare_pytest_directory
from scripts.dev.tasks import (
    Impact,
    full_validation_identity,
    gc_task_artifacts,
    has_full_validation,
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


def test_ready_state_allows_task_changelog_updates():
    validate_ready_state("codex/task", "", False, ["CHANGELOG.md"])


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


def test_prepare_pytest_cache_precreates_directory(tmp_path):
    cache = tmp_path / "task-artifacts" / "pytest" / "cache"

    assert prepare_pytest_directory(cache) == cache
    assert cache.is_dir()


def test_windows_pytest_plugin_preserves_precreated_basetemp(monkeypatch, tmp_path):
    from scripts.dev import pytest_windows_acl

    monkeypatch.setattr(pytest_windows_acl, "os", SimpleNamespace(name="nt"))
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

    monkeypatch.setattr(pytest_windows_acl, "os", SimpleNamespace(name="nt"))
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


def test_remove_primary_venv_link_rejects_regular_directory(tmp_path):
    target = tmp_path / "task"
    primary = tmp_path / "primary"
    (target / ".venv").mkdir(parents=True)
    (primary / ".venv").mkdir(parents=True)
    import pytest

    with pytest.raises(SystemExit, match="不是目录联接"):
        remove_primary_venv_link(target, primary)
    assert (target / ".venv").is_dir()


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

    def blocked_artifacts(*_args):
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
    monkeypatch.setattr(tasks, "remove_task_artifacts", lambda *args: None)
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

    class Result:
        returncode = 1
        stdout = ""

    monkeypatch.setattr(tasks, "primary_root", lambda cwd: primary)
    monkeypatch.setattr(tasks, "registered_worktrees", lambda root: set())
    monkeypatch.setattr(tasks, "valid_task_completion", lambda root, slug: True)
    monkeypatch.setattr(tasks, "git", lambda *args, **kwargs: Result())
    remove("recovery")
    assert not artifacts.exists()


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
    with pytest.raises(SystemExit, match=r"Windows ACL.*pytest[\\/]cache"):
        remove_task_artifacts(primary, "recovery")
    assert artifacts.exists()


def test_remove_task_artifacts_retries_after_restoring_acl_inheritance(
    monkeypatch, tmp_path,
):
    from scripts.dev import tasks

    primary = tmp_path / "primary"
    artifacts = primary / ".artifacts" / "worktrees" / "recovery"
    artifacts.mkdir(parents=True)
    calls: list[str] = []

    def remove(path, **_kwargs):
        calls.append("remove")
        if calls.count("remove") == 1:
            raise PermissionError(13, "denied", path)

    monkeypatch.setattr(tasks.shutil, "rmtree", remove)
    monkeypatch.setattr(tasks.os, "name", "nt")

    def restore(command, **_kwargs):
        assert "$ErrorActionPreference='Stop'" in command[-1]
        calls.append("restore")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(tasks.subprocess, "run", restore)

    remove_task_artifacts(primary, "recovery")

    assert calls == ["remove", "restore", "remove"]


def test_remove_recovers_acl_artifacts_after_git_state_is_gone(monkeypatch, tmp_path):
    from scripts.dev import tasks

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
        tasks, "os", SimpleNamespace(name="nt", environ=os.environ),
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
    target.mkdir(parents=True)

    class Result:
        returncode = 0
        stdout = ""

    monkeypatch.setattr(tasks, "primary_root", lambda cwd: primary)
    monkeypatch.setattr(tasks, "registered_worktrees", lambda root: {primary})
    monkeypatch.setattr(tasks, "task_integrated", lambda root, branch: False)
    monkeypatch.setattr(tasks, "git", lambda *args, **kwargs: Result())
    with pytest.raises(SystemExit, match="尚未完整 squash"):
        remove("recovery")
    assert target.exists()


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

    monkeypatch.setattr(tasks, "primary_root", lambda cwd: primary)
    monkeypatch.setattr(tasks, "registered_worktrees", lambda root: {target})
    monkeypatch.setattr(tasks, "task_integrated", lambda root, branch: False)
    monkeypatch.setattr(tasks, "superseding_main_commit", lambda root, commit: commit)
    monkeypatch.setattr(tasks, "remove_verified_residual", lambda *args: None)
    monkeypatch.setattr(tasks, "remove_task_artifacts", lambda *args: None)
    monkeypatch.setattr(tasks, "git", lambda *args, **kwargs: Result())

    remove("recovery", superseded_by="a" * 40)


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
