from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "release.yml"
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"


def test_release_workflow_only_accepts_future_tag_pushes() -> None:
    source = WORKFLOW.read_text(encoding="utf-8")

    assert 'tags: ["v*"]' in source
    assert "workflow_dispatch:" not in source
    assert "release-backfill" not in source
    assert not (ROOT / ".github" / "release-backfill.json").exists()
    assert "github.ref_name" in source
    assert "git merge-base --is-ancestor" in source
    assert "matrix.target" not in source


def test_release_workflow_builds_locked_native_desktops() -> None:
    source = WORKFLOW.read_text(encoding="utf-8")

    assert "maturin develop" not in source
    assert source.count("maturin build") == 1
    assert "uv sync --locked" in source
    assert "setuptools==83.0.0" in (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert "dtolnay/rust-toolchain@35d8a35b823d6c20db516f5c35eb0a9640942c17" in source


def test_release_workflow_publishes_once_with_supply_chain_evidence() -> None:
    source = WORKFLOW.read_text(encoding="utf-8")

    assert source.count("softprops/action-gh-release@") == 1
    assert "softprops/action-gh-release@3bb12739c298aeb8a4eeaf626c5b8d85266b0e65" in source
    assert "anchore/sbom-action@e22c389904149dbc22b58101806040fa8d37a610" in source
    assert 'syft-version: "v1.50.0"' in source
    assert "actions/attest-build-provenance@0f67c3f4856b2e3261c31976d6725780e5e4c373" in source
    assert "SHA256SUMS" in source
    assert "make_latest: true" in source
    assert "body_path: .release-notes.md" in source
    assert "awk '/^## v/" in source


def test_ci_matrix_collects_every_platform_result_and_audits_rust() -> None:
    source = CI_WORKFLOW.read_text(encoding="utf-8")

    assert "strategy:\n      fail-fast: false\n      matrix:" in source
    assert source.count(
        "dtolnay/rust-toolchain@35d8a35b823d6c20db516f5c35eb0a9640942c17"
    ) == 3
    assert "cargo install cargo-audit --version 0.22.2 --locked" in source
