from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "release.yml"
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
BACKFILL = ROOT / ".github" / "release-backfill.json"


def test_release_workflow_guards_historical_backfills() -> None:
    source = WORKFLOW.read_text(encoding="utf-8")

    assert "release_tag:" in source
    assert "target_sha:" in source
    assert "git merge-base --is-ancestor" in source
    assert "target_sha must be a full lowercase commit SHA" in source
    assert "tag $release_tag does not match target version" in source
    assert "immutable tag $release_tag" in source
    assert "required immutable tag $release_tag is missing" in source
    assert 'git push origin "refs/tags/${release_tag}"' not in source
    assert "fromJSON(needs.prepare.outputs.targets)" in source
    assert "matrix.target.target_sha" in source
    assert "matrix.target.make_latest" in source


def test_release_workflow_builds_native_wheels_without_maturin_develop() -> None:
    source = WORKFLOW.read_text(encoding="utf-8")

    assert "maturin develop" not in source
    assert "maturin build" in source
    assert "if [[ -f rust/quantmaster-kernel/Cargo.lock ]]" in source
    assert source.count("maturin build") == 2
    assert "fail_on_unmatched_files: true" in source
    assert "if: matrix.target.make_latest" in source
    assert "retention-days: 90" in source


def test_ci_matrix_collects_every_platform_result() -> None:
    source = CI_WORKFLOW.read_text(encoding="utf-8")

    assert "strategy:\n      fail-fast: false\n      matrix:" in source


def test_release_backfill_manifest_is_unique_and_immutable() -> None:
    payload = json.loads(BACKFILL.read_text(encoding="utf-8"))
    releases = payload["releases"]
    tags = [item["tag"] for item in releases]

    assert payload["schema_version"] == 1
    assert tags == [
        "v0.10.1",
        "v0.10.2",
        "v0.11.0",
        "v0.11.1",
        "v0.12.0",
        "v0.13.6",
        "v0.13.8",
    ]
    assert len(tags) == len(set(tags))
    assert all(len(item["target_sha"]) == 40 for item in releases)
