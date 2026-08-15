"""Manual-reload port shared by Web handlers and the lifecycle supervisor."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

_trigger_path: Callable[[], Path | None] | None = None
_request_reload: Callable[[Path], None] | None = None


def register_reload_controller(
    trigger_path: Callable[[], Path | None],
    request_reload: Callable[[Path], None],
) -> None:
    global _trigger_path, _request_reload
    _trigger_path = trigger_path
    _request_reload = request_reload


def manual_reload_trigger_path() -> Path | None:
    if _trigger_path is None:
        raise RuntimeError("热更新监督器尚未注册")
    return _trigger_path()


def request_manual_reload(path: Path) -> None:
    if _request_reload is None:
        raise RuntimeError("热更新监督器尚未注册")
    _request_reload(path)
