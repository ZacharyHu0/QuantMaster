"""Runtime-owned worker components shared by spawned worker processes."""

from __future__ import annotations

from quantmaster.bootstrap_hooks import (
    register_server_worker_hooks,
    server_worker_hooks_registered,
)
from quantmaster.settings_control import settings_manager
from quantmaster.settings_jobs import get_settings_jobs, shutdown_settings_jobs


def register_worker_components() -> None:
    """Install components that do not require importing the Web package.

    The Web process may later replace the diagnostics callbacks with its HTTP
    diagnostics implementation.  A spawned runtime worker has no Web module
    or request cache, so its lifecycle only needs the durable settings jobs.
    """
    if server_worker_hooks_registered():
        return
    register_server_worker_hooks(
        settings_manager=settings_manager(),
        get_settings_jobs=get_settings_jobs,
        shutdown_settings_jobs=shutdown_settings_jobs,
        start_diagnostics_sampler=lambda: None,
        stop_diagnostics_sampler=lambda: None,
    )
