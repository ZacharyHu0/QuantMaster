"""Web-owned overrides for the runtime worker component seam."""

from __future__ import annotations

from quantmaster.server.settings_control import settings_manager
from quantmaster.server.worker_hooks import register_server_worker_hooks


def register_worker_components() -> None:
    from quantmaster.server.diagnostics import (
        start_diagnostics_sampler,
        stop_diagnostics_sampler,
    )
    from quantmaster.server.settings_jobs import (
        get_settings_jobs,
        shutdown_settings_jobs,
    )

    register_server_worker_hooks(
        settings_manager=settings_manager(),
        get_settings_jobs=get_settings_jobs,
        shutdown_settings_jobs=shutdown_settings_jobs,
        start_diagnostics_sampler=start_diagnostics_sampler,
        stop_diagnostics_sampler=stop_diagnostics_sampler,
    )
