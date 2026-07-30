"""Bounded process-start helpers for transient Windows launch failures."""

from __future__ import annotations

import subprocess
import time
from collections.abc import Sequence
from os import PathLike
from typing import Any

_TRANSIENT_WINDOWS_ERRORS = {5, 32}


def run_process(
    command: Sequence[str | PathLike[str]],
    *,
    start_attempts: int = 4,
    retry_delay: float = 0.05,
    **kwargs: Any,
) -> subprocess.CompletedProcess[Any]:
    """Run a process, retrying only transient Windows CreateProcess failures.

    Error 5 (access denied) and 32 (sharing violation) can be produced briefly by
    endpoint scanners while a freshly started executable is inspected.  Runtime
    errors and non-Windows launch errors are deliberately not retried.
    """
    attempts = max(1, int(start_attempts))
    for attempt in range(attempts):
        try:
            return subprocess.run(list(command), **kwargs)
        except OSError as exc:
            retryable = getattr(exc, "winerror", None) in _TRANSIENT_WINDOWS_ERRORS
            if not retryable or attempt + 1 >= attempts:
                raise
            time.sleep(max(0.0, retry_delay) * (2**attempt))
    raise AssertionError("unreachable")
