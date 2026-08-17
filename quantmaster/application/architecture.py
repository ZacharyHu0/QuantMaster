"""Standard-library architecture graph for the production package.

The resolver deliberately follows syntax rather than importing application
modules.  That keeps the gate deterministic and safe on a cold data directory.
"""

from __future__ import annotations

import ast
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

ROOT_PACKAGE = "quantmaster"
OUTER_LAYERS = frozenset({"application", "server"})
RUNTIME_FOUNDATION = frozenset({"config", "logging_config", "release", "runtime"})


@dataclass(frozen=True)
class ImportRef:
    source: str
    target: str
    line: int


@dataclass(frozen=True)
class ProductionGraph:
    modules: frozenset[str]
    packages: frozenset[str]
    imports: dict[str, frozenset[str]]
    package_imports: dict[str, frozenset[str]]


def production_paths(root: Path) -> tuple[Path, ...]:
    return tuple(sorted(root.rglob("*.py")))


def sqlite_connect_lines(root: Path) -> tuple[tuple[str, int], ...]:
    """Find direct sqlite connections for the deep doctor check."""
    findings: list[tuple[str, int]] = []
    for path in production_paths(root):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "sqlite3"
                and node.func.attr == "connect"
            ):
                findings.append((path.relative_to(root).as_posix(), node.lineno))
    return tuple(sorted(findings))


def module_name(path: Path, package_root: Path) -> str:
    relative = path.relative_to(package_root.parent).with_suffix("")
    parts = list(relative.parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _current_package(module: str, path: Path) -> str:
    return module if path.name == "__init__.py" else module.rpartition(".")[0]


def _relative_base(module: str, path: Path, level: int) -> str | None:
    package = _current_package(module, path)
    parts = package.split(".") if package else []
    if level > len(parts):
        return None
    if level > 1:
        parts = parts[: len(parts) - level + 1]
    return ".".join(parts)


def _import_from_targets(module: str, path: Path, node: ast.ImportFrom) -> set[str]:
    if node.level:
        base = _relative_base(module, path, node.level)
        if base is None:
            return set()
        if node.module:
            base = f"{base}.{node.module}" if base else node.module
    else:
        base = node.module or ""
    targets = {base} if base else set()
    for alias in node.names:
        if alias.name == "*":
            continue
        targets.add(f"{base}.{alias.name}" if base else alias.name)
    return targets


def _literal_import_targets(tree: ast.AST) -> Iterable[tuple[str, int]]:
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = ""
        if isinstance(node.func, ast.Name):
            name = node.func.id
        elif (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "importlib"
        ):
            name = f"{node.func.value.id}.{node.func.attr}"
        if name not in {"__import__", "import_module", "importlib.import_module"}:
            continue
        if not node.args or not isinstance(node.args[0], ast.Constant):
            continue
        if isinstance(node.args[0].value, str):
            yield node.args[0].value, node.lineno


def import_targets(path: Path, package_root: Path) -> tuple[ImportRef, ...]:
    module = module_name(path, package_root)
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    refs: list[ImportRef] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            refs.extend(
                ImportRef(module, alias.name, node.lineno)
                for alias in node.names
            )
        elif isinstance(node, ast.ImportFrom):
            refs.extend(
                ImportRef(module, target, node.lineno)
                for target in _import_from_targets(module, path, node)
            )
    refs.extend(
        ImportRef(module, target, line)
        for target, line in _literal_import_targets(tree)
    )
    return tuple(refs)


def resolve_source_imports(source: str, *, module: str) -> tuple[ImportRef, ...]:
    """Resolve an in-memory specimen with the same rules as a production file."""
    path = Path("__init__.py" if module == ROOT_PACKAGE else "fixture.py")
    tree = ast.parse(source, filename=str(path))
    refs: list[ImportRef] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            refs.extend(
                ImportRef(module, alias.name, node.lineno)
                for alias in node.names
            )
        elif isinstance(node, ast.ImportFrom):
            refs.extend(
                ImportRef(module, target, node.lineno)
                for target in _import_from_targets(module, path, node)
            )
    refs.extend(
        ImportRef(module, target, line)
        for target, line in _literal_import_targets(tree)
    )
    return tuple(refs)


def _known_packages(paths: Iterable[Path], package_root: Path) -> set[str]:
    packages = {ROOT_PACKAGE}
    for path in paths:
        if path.name == "__init__.py":
            packages.add(module_name(path, package_root))
    return packages


def _canonical_target(target: str, known: set[str]) -> str | None:
    candidates = [
        value
        for value in known
        if target == value or target.startswith(f"{value}.")
    ]
    return max(candidates, key=len) if candidates else None


def build_graph(package_root: Path) -> ProductionGraph:
    paths = production_paths(package_root)
    modules = {module_name(path, package_root) for path in paths}
    packages = _known_packages(paths, package_root)
    known = modules | packages
    imports: dict[str, set[str]] = {module: set() for module in modules}
    for path in paths:
        source = module_name(path, package_root)
        resolved = [
            (ref, target)
            for ref in import_targets(path, package_root)
            if (target := _canonical_target(ref.target, known)) is not None
        ]
        for ref, target in resolved:
            if target in packages and any(
                other.line == ref.line
                and child != target
                and child.startswith(f"{target}.")
                for other, child in resolved
            ):
                continue
            imports[source].add(target)
    package_nodes = packages | {_package_for(module, packages) for module in modules}
    package_imports: dict[str, set[str]] = {package: set() for package in package_nodes}
    for source, targets in imports.items():
        source_package = _package_for(source, packages)
        for target in targets:
            target_package = _package_for(target, packages)
            if source_package != target_package:
                package_imports[source_package].add(target_package)
    return ProductionGraph(
        frozenset(modules),
        frozenset(package_nodes),
        {key: frozenset(value) for key, value in imports.items()},
        {key: frozenset(value) for key, value in package_imports.items()},
    )


def _package_for(module: str, packages: set[str] | frozenset[str]) -> str:
    candidates = [
        package
        for package in packages
        if package != ROOT_PACKAGE
        and (module == package or module.startswith(f"{package}."))
    ]
    if candidates:
        return max(candidates, key=len)
    return ROOT_PACKAGE if module == ROOT_PACKAGE else module


def strongly_connected_components(
    graph: dict[str, Iterable[str]],
) -> tuple[frozenset[str], ...]:
    index = 0
    indices: dict[str, int] = {}
    lowlinks: dict[str, int] = {}
    stack: list[str] = []
    active: set[str] = set()
    result: list[frozenset[str]] = []

    def visit(node: str) -> None:
        nonlocal index
        indices[node] = lowlinks[node] = index
        index += 1
        stack.append(node)
        active.add(node)
        for dependency in graph.get(node, ()):
            if dependency not in indices:
                visit(dependency)
                lowlinks[node] = min(lowlinks[node], lowlinks[dependency])
            elif dependency in active:
                lowlinks[node] = min(lowlinks[node], indices[dependency])
        if lowlinks[node] != indices[node]:
            return
        component: set[str] = set()
        while True:
            dependency = stack.pop()
            active.remove(dependency)
            component.add(dependency)
            if dependency == node:
                break
        result.append(frozenset(component))

    for node in sorted(graph):
        if node not in indices:
            visit(node)
    return tuple(result)


def cycles(graph: dict[str, Iterable[str]]) -> tuple[frozenset[str], ...]:
    components = strongly_connected_components(graph)
    return tuple(
        component
        for component in components
        if len(component) > 1
        or any(node in set(graph.get(node, ())) for node in component)
    )


def layer_violations(graph: ProductionGraph) -> tuple[str, ...]:
    violations: list[str] = []
    for source, targets in graph.imports.items():
        source_parts = source.split(".")
        source_layer = source_parts[1] if len(source_parts) > 1 else ""
        for target in targets:
            target_parts = target.split(".")
            if len(target_parts) < 2:
                continue
            target_layer = target_parts[1]
            forbidden = (
                source_layer == "runtime"
                and target_layer not in RUNTIME_FOUNDATION
            ) or (
                source_layer not in OUTER_LAYERS | {"runtime", ""}
                and target_layer in OUTER_LAYERS
            )
            if forbidden:
                violations.append(f"{source} -> {target}")
    return tuple(sorted(set(violations)))
