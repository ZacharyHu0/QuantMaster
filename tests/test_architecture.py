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


def _resolved_import_from(path: Path, node: ast.ImportFrom) -> set[str]:
    parts: list[str] = []
    if node.level:
        package = list(path.relative_to(PACKAGE_ROOT.parent).with_suffix("").parts[:-1])
        parents = node.level - 1
        if parents > len(package):
            return set()
        parts.extend(package[:len(package) - parents])
    if node.module:
        parts.extend(node.module.split("."))
    base = ".".join(parts)
    imports = {base} if base else set()
    imports.update(
        f"{base}.{alias.name}" if base else alias.name
        for alias in node.names
        if alias.name != "*"
    )
    return imports


def _all_imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            imports.update(_resolved_import_from(path, node))
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


def test_lab_modules_do_not_hide_server_dependencies_inside_functions():
    violations = []
    for path in (PACKAGE_ROOT / "lab").rglob("*.py"):
        for imported in _all_imports(path):
            if imported == "quantmaster.server" or imported.startswith("quantmaster.server."):
                violations.append(
                    f"{path.relative_to(PACKAGE_ROOT).as_posix()} -> {imported}"
                )
    assert not violations, "Lab domain depends on server transport:\n" + "\n".join(violations)


def test_market_capability_has_no_deep_transport_dependency():
    violations = []
    for path in (PACKAGE_ROOT / "market").rglob("*.py"):
        for imported in _all_imports(path):
            if imported == "quantmaster.cli" or imported.startswith("quantmaster.server"):
                violations.append(
                    f"{path.relative_to(PACKAGE_ROOT).as_posix()} -> {imported}",
                )
    assert not violations, "market capability depends on transport:\n" + "\n".join(violations)


def test_import_resolver_canonicalizes_market_transport_import_forms():
    fixture = PACKAGE_ROOT / "market" / "fixture.py"
    specimens = {
        "from quantmaster import server": "quantmaster.server",
        "from quantmaster import cli": "quantmaster.cli",
        "from .. import server": "quantmaster.server",
        "from ..server import app": "quantmaster.server.app",
    }
    for source, expected in specimens.items():
        node = next(
            item for item in ast.walk(ast.parse(source)) if isinstance(item, ast.ImportFrom)
        )
        assert expected in _resolved_import_from(fixture, node)


def test_runtime_imports_do_not_hide_new_domain_wiring_inside_functions():
    existing_runtime_adapters = {
        ("llm.py", "quantmaster.ai.llm"),
        ("sqlite_recovery.py", "quantmaster.data.free_stockdb_runtime"),
    }
    foundation = {
        "quantmaster.config",
        "quantmaster.logging_config",
        "quantmaster.release",
    }
    violations = []
    runtime_root = PACKAGE_ROOT / "runtime"
    for path in runtime_root.glob("*.py"):
        for imported in _all_imports(path):
            if not imported.startswith("quantmaster."):
                continue
            if imported.startswith("quantmaster.runtime.") or any(
                imported == item or imported.startswith(f"{item}.") for item in foundation
            ):
                continue
            if any(
                path.name == filename
                and (imported == adapter or imported.startswith(f"{adapter}."))
                for filename, adapter in existing_runtime_adapters
            ):
                continue
            violations.append(f"runtime/{path.name} -> {imported}")
    assert not violations, "runtime wiring must live in bootstrap:\n" + "\n".join(violations)


def test_stockdb_data_runtime_does_not_dispatch_domain_events():
    path = PACKAGE_ROOT / "data" / "free_stockdb_runtime.py"
    domain_roots = {
        "after_close", "ai", "analysis", "automation", "backtest", "decision",
        "factors", "lab", "market", "portfolio", "research", "rotation",
    }
    violations = []
    for imported in _all_imports(path):
        parts = imported.split(".")
        if len(parts) >= 3 and parts[:2] == ["quantmaster", "data"]:
            continue
        if len(parts) >= 2 and parts[0] == "quantmaster" and parts[1] in domain_roots:
            violations.append(imported)
    assert not violations, "StockDB delivery leaked into data:\n" + "\n".join(violations)


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
