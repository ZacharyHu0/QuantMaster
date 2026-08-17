"""Worktree conftest: route imports to this worktree instead of the editable install.

The project venv has an editable install for `quantmaster` pointing at the
primary checkout.  Running pytest from a task worktree therefore resolves
`quantmaster.*` from the wrong location.  This conftest reconfigures
sys.meta_path and sys.path so every test sees the worktree source.
"""
from __future__ import annotations

import sys
from pathlib import Path

_WORKTREE = str(Path(__file__).parent.resolve())
if _WORKTREE not in sys.path:
    sys.path.insert(0, _WORKTREE)

for finder in list(sys.meta_path):
    module_name = getattr(finder, "__module__", "")
    if "__editable___quantmaster" in module_name:
        sys.meta_path.remove(finder)

for mod in list(sys.modules):
    if mod == "quantmaster" or mod.startswith("quantmaster."):
        del sys.modules[mod]
