"""Compatibility entry point for the production architecture scanner."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from quantmaster.application import architecture as _architecture  # noqa: E402 — needs sys.path above

ImportRef = _architecture.ImportRef
ProductionGraph = _architecture.ProductionGraph
build_graph = _architecture.build_graph
cycles = _architecture.cycles
import_targets = _architecture.import_targets
layer_violations = _architecture.layer_violations
module_name = _architecture.module_name
production_paths = _architecture.production_paths
resolve_source_imports = _architecture.resolve_source_imports
sqlite_connect_lines = _architecture.sqlite_connect_lines
strongly_connected_components = _architecture.strongly_connected_components

__all__ = [
    "ImportRef",
    "ProductionGraph",
    "build_graph",
    "cycles",
    "import_targets",
    "layer_violations",
    "module_name",
    "production_paths",
    "resolve_source_imports",
    "sqlite_connect_lines",
    "strongly_connected_components",
]
