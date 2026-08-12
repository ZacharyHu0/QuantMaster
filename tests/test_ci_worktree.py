"""Local CI must reuse the primary checkout virtual environment."""

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
