from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "release.yml"


def test_release_workflow_guards_historical_backfills() -> None:
    source = WORKFLOW.read_text(encoding="utf-8")

    assert "release_tag:" in source
    assert "target_sha:" in source
    assert "git merge-base --is-ancestor" in source
    assert "target_sha must be a full lowercase commit SHA" in source
    assert 'tag $RELEASE_TAG does not match target version' in source
    assert "immutable tag $RELEASE_TAG" in source
    assert "inputs.target_sha || github.ref" in source
    assert "inputs.release_tag || github.ref_name" in source
    assert "inputs.release_tag != '' && 'false' || 'true'" in source


def test_release_workflow_builds_native_wheels_without_maturin_develop() -> None:
    source = WORKFLOW.read_text(encoding="utf-8")

    assert "maturin develop" not in source
    assert "maturin build" in source
    assert "lock_args+=(--locked)" in source
    assert "fail_on_unmatched_files: true" in source
