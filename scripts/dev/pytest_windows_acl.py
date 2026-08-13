"""Keep pytest temporary directories deletable across Windows sandbox identities."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from _pytest.config import hookimpl


def prepare_pytest_directory(path: Path) -> Path:
    """Create a pytest-owned directory without replacing inherited ACLs."""
    from quantmaster.runtime.storage_governance import prepare_writable_directory

    target = path.resolve()
    prepare_writable_directory(target)
    return target


@hookimpl(trylast=True)
def pytest_configure(config: Any) -> None:
    if os.name != "nt":
        return
    cache = getattr(config, "cache", None)
    cachedir = getattr(cache, "_cachedir", None)
    if cachedir is not None:
        prepare_pytest_directory(Path(cachedir))

    factory = getattr(config, "_tmp_path_factory", None)
    given_basetemp = getattr(factory, "_given_basetemp", None)
    if factory is None or given_basetemp is None:
        return

    # TempPathFactory.getbasetemp() deletes an existing --basetemp and recreates
    # it with mode=0700.  On Windows that replacement protects the DACL instead
    # of inheriting the task artifact ACL, so a later sandbox identity cannot
    # remove the directory.  Bind the prepared directory as the resolved base
    # before pytest gets a chance to replace it.
    factory._basetemp = prepare_pytest_directory(Path(given_basetemp))
