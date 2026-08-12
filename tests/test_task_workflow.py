"""Contracts for isolated task development and impact-based validation."""

from pathlib import Path

from scripts.dev.tasks import Impact, select_impact

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
