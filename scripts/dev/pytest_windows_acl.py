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

    basetemp = Path(given_basetemp)
    prepare_pytest_directory(basetemp)
    factory._basetemp = basetemp.resolve()

    import _pytest.tmpdir

    original = _pytest.tmpdir.make_numbered_dir

    def make_numbered_dir_inheriting_acl(
        root: Path, prefix: str, mode: int = 0o700,
    ) -> Path:
        del mode
        return original(root, prefix, 0o777)

    _pytest.tmpdir.make_numbered_dir = make_numbered_dir_inheriting_acl
    config.add_cleanup(
        lambda: setattr(_pytest.tmpdir, "make_numbered_dir", original)
    )
