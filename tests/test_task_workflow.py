"""Contracts for isolated task development and impact-based validation."""

import stat
from pathlib import Path
from types import SimpleNamespace

from scripts.dev.pytest_windows_acl import prepare_pytest_directory
from scripts.dev.tasks import (
    Impact,
    remove,
    remove_empty_residual,
    remove_primary_venv_link,
    remove_verified_residual,
    select_impact,
    validate_ready_state,
)

ROOT = Path(__file__).resolve().parents[1]


def test_documentation_only_changes_skip_python_tests():
    assert select_impact(["README.md", "docs/guide.md"]) == Impact("docs")


def test_data_changes_select_adjacent_contracts_and_architecture():
    impact = select_impact(["quantmaster/data/storage.py"])
    assert impact.mode == "selected"
    assert "tests/test_architecture.py" in impact.tests
    assert "tests/test_data_resilience.py" in impact.tests


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


def test_prepare_pytest_cache_precreates_directory(tmp_path):
    cache = tmp_path / "task-artifacts" / "pytest" / "cache"

    assert prepare_pytest_directory(cache) == cache
    assert cache.is_dir()


def test_windows_pytest_plugin_preserves_precreated_basetemp(monkeypatch, tmp_path):
    from scripts.dev import pytest_windows_acl

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
    cleanups.pop()()


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
    monkeypatch.setattr(tasks, "remove_verified_residual", lambda root, path, branch: path.rmdir())
    monkeypatch.setattr(tasks, "git", fake_git)
    remove("recovery")
    assert not target.exists()
    assert ["branch", "-D", "codex/recovery"] in calls


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
