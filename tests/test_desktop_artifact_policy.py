from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.release import check_desktop_artifact as policy
from scripts.release.check_desktop_artifact import check_artifact


def _fake_clean_git(monkeypatch, *, untracked: str = "") -> tuple[str, list[list[str]]]:
    head = "a" * 40
    calls: list[list[str]] = []

    def run(command, **_kwargs):
        calls.append(command)
        if command[1] == "diff-index":
            return SimpleNamespace(returncode=0, stdout="")
        if command[1] == "ls-files":
            return SimpleNamespace(returncode=0, stdout=untracked)
        return SimpleNamespace(returncode=0, stdout=f"{head}\n")

    monkeypatch.setattr(policy.subprocess, "run", run)
    return head, calls


def test_packaged_build_identity_allows_untracked_docs(tmp_path: Path, monkeypatch) -> None:
    from scripts.release.check_desktop_artifact import packaged_build_sha

    head, calls = _fake_clean_git(monkeypatch)
    root = tmp_path / "repository"

    assert packaged_build_sha(root) == head
    assert calls[1] == [
        "git", "ls-files", "--others", "--exclude-standard", "--",
        *policy.PACKAGED_INPUT_PATHS,
    ]


@pytest.mark.parametrize(
    "relative_path",
    [
        "quantmaster/experimental.py",
        "quantmaster/server/static/experimental.js",
        "packaging/entry.py",
    ],
)
def test_packaged_build_identity_rejects_untracked_inputs(
    tmp_path: Path,
    monkeypatch,
    relative_path: str,
) -> None:
    from scripts.release.check_desktop_artifact import packaged_build_sha

    _fake_clean_git(monkeypatch, untracked=f"{relative_path}\n")

    with pytest.raises(RuntimeError, match="untracked build input"):
        packaged_build_sha(tmp_path / "repository")


def test_desktop_artifact_policy_rejects_oversized_file(tmp_path: Path) -> None:
    artifact = tmp_path / "QuantMaster.exe"
    artifact.write_bytes(b"x" * 1025)

    assert check_artifact(artifact, max_mib=0)


def test_desktop_artifact_policy_rejects_forbidden_modules(tmp_path: Path) -> None:
    artifact = tmp_path / "QuantMaster.exe"
    artifact.write_bytes(b"ok")
    analysis = tmp_path / "Analysis-00.toc"
    analysis.write_text("  ('torch.linalg', 'torch/linalg.py', 'PYMODULE'),\n", encoding="utf-8")

    errors = check_artifact(artifact, analysis=analysis)

    assert errors == ["forbidden optional modules were bundled: torch.linalg"]


def test_desktop_artifact_policy_accepts_normal_build(tmp_path: Path) -> None:
    artifact = tmp_path / "QuantMaster.exe"
    artifact.write_bytes(b"ok")
    analysis = tmp_path / "Analysis-00.toc"
    analysis.write_text("  ('numpy.fft', 'numpy/fft.py', 'PYMODULE'),\n", encoding="utf-8")

    assert check_artifact(artifact, analysis=analysis) == []
