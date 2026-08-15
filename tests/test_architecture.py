"""Executable architecture and policy contracts."""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

from scripts.ci.architecture import (
    build_graph,
    cycles,
    import_targets,
    layer_violations,
    resolve_source_imports,
)

PACKAGE_ROOT = Path(__file__).resolve().parents[1] / "quantmaster"
STATIC_ROOT = PACKAGE_ROOT / "server" / "static"
sys.path.insert(0, str(PACKAGE_ROOT.parent))


def _all_imports(path: Path) -> set[str]:
    return {ref.target for ref in import_targets(path, PACKAGE_ROOT)}


def test_in_memory_resolver_finds_lazy_relative_and_barrel_imports():
    refs = resolve_source_imports(
        """
from quantmaster import server
from .. import cli
from ..server import app
def load():
    from ..runtime import jobs
""",
        module="quantmaster.market.fixture",
    )
    targets = {ref.target for ref in refs}
    assert {
        "quantmaster",
        "quantmaster.server",
        "quantmaster.cli",
        "quantmaster.server.app",
        "quantmaster.runtime",
        "quantmaster.runtime.jobs",
    } <= targets


def test_in_memory_resolver_finds_literal_dynamic_imports():
    refs = resolve_source_imports(
        """
import importlib
from importlib import import_module
__import__("quantmaster.server.app")
importlib.import_module("quantmaster.cli")
import_module("quantmaster.runtime.jobs")
""",
        module="quantmaster.market.fixture",
    )
    targets = {ref.target for ref in refs}
    assert {
        "quantmaster.server.app",
        "quantmaster.cli",
        "quantmaster.runtime.jobs",
    } <= targets


def test_tarjan_reports_module_and_package_cycles():
    graph = {"a": {"b"}, "b": {"a", "c"}, "c": set()}
    assert cycles(graph) == (frozenset({"a", "b"}),)
    assert cycles({"pkg": {"pkg"}}) == (frozenset({"pkg"}),)


def test_production_tree_has_no_forbidden_edges_or_module_package_cycles():
    graph = build_graph(PACKAGE_ROOT)
    assert not layer_violations(graph)
    assert not cycles(graph.imports)
    assert not cycles(graph.package_imports)


def test_composition_root_is_transport_only():
    lines = (PACKAGE_ROOT / "server" / "app.py").read_text(encoding="utf-8").splitlines()
    assert len(lines) < 500
    assert not any(
        line.startswith("@app.")
        and any(line.startswith(f"@app.{method}") for method in ("get", "post", "put", "patch", "delete"))
        for line in lines
    )


def test_production_sqlite_connections_use_shared_runtime_factory():
    allowed = {"runtime/sqlite.py", "data/migration.py"}
    violations = []
    for path in PACKAGE_ROOT.rglob("*.py"):
        relative = path.relative_to(PACKAGE_ROOT).as_posix()
        if relative in allowed:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if (
                isinstance(node.func.value, ast.Name)
                and node.func.value.id == "sqlite3"
                and node.func.attr == "connect"
            ):
                violations.append(f"{relative}:{node.lineno}")
    assert not violations, "direct sqlite3.connect bypasses runtime policy:\n" + "\n".join(
        violations
    )


def test_frontend_does_not_reference_unversioned_api_routes():
    pattern = re.compile(r"/api/(?!v1(?:/|$))")
    violations = []
    for path in STATIC_ROOT.rglob("*"):
        if path.suffix not in {".html", ".js"}:
            continue
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if pattern.search(line):
                violations.append(f"{path.relative_to(PACKAGE_ROOT)}:{line_number}")
    assert not violations, "frontend references removed API routes:\n" + "\n".join(violations)
