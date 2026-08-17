"""Machine-checkable for_version manifest and section coverage audit."""

from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
DATA_MIGRATION = ROOT / "quantmaster" / "data" / "migration.py"
DECISION_MIGRATION = ROOT / "quantmaster" / "decision" / "migration.py"

FOR_VERSION_RE = re.compile(r"^# for_version:\s+(\S+)\s*(?:\(consolidated from (.+)\)|\((.+)\))?$")

# The 15 sections that were merged from dropped modules. Each tuple is
# (version, original module path, minimum expected definitions).
# This is the authoritative machine-checkable manifest for the `for_version`
# acceptance criterion; it must stay in sync with the `# for_version:` markers
# embedded in quantmaster/data/migration.py.
MIGRATION_MANIFEST = [
    ("v1.0", "quantmaster.data.migration_contracts"),
    ("v1.0", "quantmaster.data.legacy_migrations"),
    ("v1.0", "quantmaster.data.remaining_schema_migration"),
    ("v1.0", "quantmaster.data.startup_schema_migration"),
    ("v1.0", "quantmaster.data.store_schema_migration"),
    ("v1.0", "quantmaster.data.job_migration"),
    ("v1.0", "quantmaster.after_close.migration"),
    ("v1.0", "quantmaster.ai.news_migration"),
    ("v1.0", "quantmaster.automation.migration"),
    ("v1.0", "quantmaster.backtest.job_migration"),
    ("v1.0", "quantmaster.backtest.paper_legacy_migration"),
    ("v1.0", "quantmaster.lab.job_migration"),
    ("v1.0", "quantmaster.lab.model_migration"),
    ("v1.0", "quantmaster.research.job_migration"),
    ("v1.0", "quantmaster.data.legacy_migration"),
]


def _parse_sections(path: Path) -> list[dict[str, Any]]:
    """Return one dict per `# for_version:` section marker."""
    lines = path.read_text(encoding="utf-8").splitlines()
    sections: list[dict[str, Any]] = []
    for idx, ln in enumerate(lines):
        match = FOR_VERSION_RE.match(ln.strip())
        if not match:
            continue
        sections.append(
            {
                "line": idx + 1,
                "version": match.group(1),
                "source": match.group(2) or match.group(3) or "",
                "start": idx,
            }
        )
    return sections


def _count_top_level_defs(lines: list[str], start: int, end: int) -> int:
    tree = ast.parse("\n".join(lines[start:end]))
    count = 0
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            count += 1
    return count


def _get_module_top_defs(lines: list[str], up_to: int) -> int:
    tree = ast.parse("\n".join(lines[:up_to]))
    return sum(
        1 for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    )


def test_migration_manifest_matches_for_version_markers() -> None:
    """Every manifest entry must match an embedded for_version marker."""
    sections = _parse_sections(DATA_MIGRATION)

    markers = [(s["version"], s["source"]) for s in sections]
    manifest = list(MIGRATION_MANIFEST)

    assert markers == manifest, (
        f"MIGRATION_MANIFEST drifts from embedded for_version markers. "
        f"manifest has {len(manifest)} entries, file has {len(markers)}."
    )


def test_migration_manifest_is_complete_and_15_sections() -> None:
    """Acceptance: 15 consolidated sections, each tagged with a version."""
    sections = _parse_sections(DATA_MIGRATION)
    assert len(sections) == len(MIGRATION_MANIFEST) == 15

    for section in sections:
        assert section["version"], "for_version marker missing version token"
        assert section["source"], "for_version marker missing source module"


def test_every_surviving_migration_function_lives_in_a_for_version_section() -> None:
    """
    Every top-level def/class after the first for_version marker must be
    reachable from that marker's section. The pre-header area (infrastructure
    classes like MigrationError, DataMigrationManager) is allowed to be
    unmarked because it is new code, not migrated code.
    """
    lines = DATA_MIGRATION.read_text(encoding="utf-8").splitlines()
    sections = _parse_sections(DATA_MIGRATION)

    first_section_start = sections[0]["start"]

    for idx, section in enumerate(sections):
        end = sections[idx + 1]["start"] if idx + 1 < len(sections) else len(lines)
        defs = _count_top_level_defs(lines, section["start"], end)
        assert defs >= 1, (
            f"Section at L{section['line']} ({section['source']}) "
            "contains zero top-level definitions"
        )

    pre_header = _get_module_top_defs(lines, first_section_start)
    assert pre_header >= 1, "Expected pre-header infrastructure definitions"


def test_decision_migration_has_for_version_marker() -> None:
    """The decision module keeps its own for_version marker."""
    lines = DECISION_MIGRATION.read_text(encoding="utf-8").splitlines()
    for _idx, ln in enumerate(lines):
        match = FOR_VERSION_RE.match(ln.strip())
        if match:
            assert match.group(1), "for_version marker missing version token"
            return
    pytest.fail("quantmaster/decision/migration.py has no for_version marker")
