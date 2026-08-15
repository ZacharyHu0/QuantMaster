"""Optional PyInstaller splash lifecycle for the frozen Windows shell."""

from __future__ import annotations

import os
import sys
from types import ModuleType


def _backend() -> ModuleType | None:
    if not getattr(sys, "frozen", False):
        return None
    backend = sys.modules.get("pyi_splash")
    if backend is None:
        if "_PYI_SPLASH_IPC" not in os.environ:
            return None
        try:
            import pyi_splash
        except ImportError:
            return None
        backend = pyi_splash
    try:
        return backend if backend.is_alive() else None
    except (ConnectionError, OSError, RuntimeError):
        return None


def update_splash(stage: str) -> None:
    """Show a real startup stage when the bootloader splash is active."""
    backend = _backend()
    if backend is not None:
        try:
            backend.update_text(str(stage))
        except (ConnectionError, OSError, RuntimeError):
            pass


def splash_active() -> bool:
    """Return whether this frozen process still owns a live splash."""
    return _backend() is not None


def close_splash() -> None:
    """Close the splash without changing source, onedir, or suppressed runs."""
    backend = _backend()
    if backend is not None:
        try:
            backend.close()
        except (ConnectionError, OSError, RuntimeError):
            pass
