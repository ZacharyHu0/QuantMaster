"""Local CI must reuse the primary checkout virtual environment."""

import subprocess
import sys
from pathlib import Path

from scripts.ci import run


def test_primary_root_uses_git_common_directory(monkeypatch, tmp_path):
    primary = tmp_path / "primary"
    common = primary / ".git"

    class Result:
        returncode = 0
        stdout = str(common)

    monkeypatch.setattr(run.subprocess, "run", lambda *args, **kwargs: Result())
    assert run.primary_root() == primary.resolve()


def test_project_python_uses_primary_worktree(monkeypatch, tmp_path):
    primary = tmp_path / "primary"
    interpreter = primary / ".venv" / "Scripts" / "python.exe"
    interpreter.parent.mkdir(parents=True)
    interpreter.touch()
    monkeypatch.setattr(run, "primary_root", lambda: primary)
    assert run.project_python() == Path(interpreter)


def test_artifact_root_is_task_scoped_outside_linked_checkout(monkeypatch, tmp_path):
    primary = tmp_path / "primary"
    worktree = primary / ".worktrees" / "feature"
    monkeypatch.setattr(run, "ROOT", worktree)
    monkeypatch.setattr(run, "primary_root", lambda: primary)
    assert run.artifact_root() == primary / ".artifacts" / "worktrees" / "feature"


def test_artifact_root_keeps_primary_artifact_contract(monkeypatch, tmp_path):
    primary = tmp_path / "primary"
    monkeypatch.setattr(run, "ROOT", primary)
    monkeypatch.setattr(run, "primary_root", lambda: primary)
    assert run.artifact_root() == primary / ".artifacts"


def test_pytest_args_override_checkout_cache_dir(monkeypatch, tmp_path):
    cache = tmp_path / "external-cache"
    monkeypatch.setattr(run, "PYTEST_CACHE", cache)
    assert run.pytest_args("tests/test_one.py") == [
        "-m", "pytest", "-p", "scripts.dev.pytest_windows_acl",
        "-o", f"cache_dir={cache}", "tests/test_one.py",
    ]


def test_prepare_pytest_cache_precreates_directory(tmp_path):
    cache = tmp_path / "task-artifacts" / "pytest" / "cache"

    assert run.prepare_pytest_directory(cache) == cache
    assert cache.is_dir()


def test_run_redirects_static_tool_caches(monkeypatch, tmp_path):
    artifacts = tmp_path / "artifacts"
    captured = {}

    class Result:
        returncode = 0

    monkeypatch.setattr(run, "ARTIFACTS", artifacts)
    monkeypatch.setattr(
        run.subprocess, "run",
        lambda *args, **kwargs: captured.update(kwargs) or Result(),
    )
    run.run("test", ["-c", "pass"])
    assert captured["env"]["RUFF_CACHE_DIR"] == str(artifacts / "cache" / "ruff")
    assert captured["env"]["MYPY_CACHE_DIR"] == str(artifacts / "cache" / "mypy")


def test_ci_script_can_import_project_modules_when_executed_directly():
    result = subprocess.run(
        [sys.executable, "scripts/ci/run.py", "--help"],
        cwd=run.ROOT, capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr
