"""Web-facing compatibility seam for the runtime settings control."""

from quantmaster.settings_control import (
    apply_runtime,
    register_settings_control,
    settings_manager,
)

__all__ = ["apply_runtime", "register_settings_control", "settings_manager"]
