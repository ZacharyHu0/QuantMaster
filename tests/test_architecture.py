"""Executable module-boundary rules that prevent dependency drift."""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1] / "quantmaster"
STATIC_ROOT = PACKAGE_ROOT / "server" / "static"
sys.path.insert(0, str(PACKAGE_ROOT.parent))


def _module(path: Path) -> str:
    return ".".join(path.relative_to(PACKAGE_ROOT.parent).with_suffix("").parts)


def _top_level_imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imports: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
        elif isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
    return imports


def test_domain_and_runtime_modules_do_not_depend_on_server_transport():
    violations = []
    for path in PACKAGE_ROOT.rglob("*.py"):
        relative = path.relative_to(PACKAGE_ROOT)
        if relative.parts[0] == "server" or relative.as_posix() == "cli.py":
            continue
        for imported in _top_level_imports(path):
            if imported == "quantmaster.server" or imported.startswith("quantmaster.server."):
                violations.append(f"{relative.as_posix()} -> {imported}")
    assert not violations, "transport dependency leaked into domain:\n" + "\n".join(violations)


def test_quantmaster_has_no_top_level_import_cycles():
    paths = [path for path in PACKAGE_ROOT.rglob("*.py") if path.name != "__init__.py"]
    modules = {_module(path): path for path in paths}
    graph = {
        module: {name for name in _top_level_imports(path) if name in modules}
        for module, path in modules.items()
    }
    visited: set[str] = set()
    active: set[str] = set()
    stack: list[str] = []
    cycles: list[list[str]] = []

    def visit(module: str) -> None:
        visited.add(module)
        active.add(module)
        stack.append(module)
        for dependency in graph[module]:
            if dependency not in visited:
                visit(dependency)
            elif dependency in active:
                cycles.append([*stack[stack.index(dependency):], dependency])
        stack.pop()
        active.remove(module)

    for module in graph:
        if module not in visited:
            visit(module)
    assert not cycles, "top-level import cycle:\n" + "\n".join(
        " -> ".join(cycle) for cycle in cycles
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


def test_broad_exception_policy_does_not_expand():
    from scripts.ci.exception_policy import analyze

    assert analyze() == []
