import os
from pathlib import Path

import pytest

from scripts.release.smoke_frozen_runtime import _assert_same_identity, _pid_alive


def test_frozen_runtime_smoke_requires_one_exact_application_identity():
    identity = {
        "build_sha": "a" * 40,
        "slot_id": "slot-a",
        "runtime_generation": "b" * 32,
    }

    _assert_same_identity(identity, {**identity, "pid": 2}, {**identity, "pid": 3})

    with pytest.raises(RuntimeError, match="runtime_generation"):
        _assert_same_identity(
            identity,
            {**identity, "runtime_generation": "c" * 32},
            identity,
        )


@pytest.mark.skipif(os.name != "nt", reason="Windows process contract")
def test_frozen_runtime_smoke_observes_windows_process_liveness():
    assert _pid_alive(os.getpid())
    assert not _pid_alive(0xFFFFFFFF)


def test_windows_package_and_release_workflows_run_the_frozen_smoke():
    root = Path(__file__).parents[1]
    ci = (root / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    release = (root / ".github/workflows/release.yml").read_text(encoding="utf-8")

    assert "windows-package:" in ci
    assert "scripts/release/smoke_frozen_runtime.py" in ci
    assert "scripts/release/smoke_frozen_runtime.py" in release
    assert "if: runner.os == 'Windows'" in release
