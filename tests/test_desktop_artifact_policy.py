from __future__ import annotations

from pathlib import Path

from scripts.release.check_desktop_artifact import check_artifact


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
