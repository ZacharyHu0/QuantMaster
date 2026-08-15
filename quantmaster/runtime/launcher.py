"""Stable user launcher and Windows shortcut creation."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

from quantmaster.runtime.activation import (
    FULL_SHA,
    ActivationBlocked,
    _is_link,
    installed_app_root,
)

STABLE_LAUNCHER_NAME = "QuantMaster Stable Launcher.cmd"
SHORTCUT_NAME = "QuantMaster.lnk"
_SHORTCUT_SUBDIRECTORY = Path("Microsoft") / "Windows" / "Start Menu" / "Programs"


def stable_launcher_path(app_root: str | Path | None = None) -> Path:
    """Return the fixed launcher path; it never points at a versioned slot."""

    root = Path(app_root).resolve() if app_root is not None else installed_app_root()
    return root / STABLE_LAUNCHER_NAME


def launcher_target_path(app_root: str | Path | None = None) -> Path:
    """Return the fixed pointer consumed by the stable launcher."""

    root = Path(app_root).resolve() if app_root is not None else installed_app_root()
    return root / "launcher.target"


def read_launcher_target(app_root: str | Path | None = None) -> str:
    """Read one validated immutable-slot SHA from ``launcher.target``."""

    path = launcher_target_path(app_root)
    try:
        lines = path.read_text(encoding="ascii").splitlines()
    except (OSError, UnicodeError) as exc:
        raise ActivationBlocked("launcher_target_unreadable", "稳定 launcher target 不可读") from exc
    if len(lines) != 1 or FULL_SHA.fullmatch(lines[0]) is None:
        raise ActivationBlocked("launcher_target_invalid", "稳定 launcher target 不是完整 lowercase SHA")
    return lines[0]


def stable_slot_executable(app_root: str | Path | None = None) -> Path:
    """Resolve the active executable without consulting checkout or Python."""

    raw_root = Path(app_root) if app_root is not None else installed_app_root()
    if _is_link(raw_root):
        raise ActivationBlocked("active_slot_unavailable", "稳定 launcher target 对应的不可变槽不可用")
    root = raw_root.resolve()
    build_sha = read_launcher_target(root)
    slots = root / "slots"
    slot = slots / build_sha
    executable = slot / "QuantMaster.exe"
    if (
        _is_link(slots) or not slot.is_dir() or _is_link(slot)
        or not executable.is_file() or _is_link(executable)
    ):
        raise ActivationBlocked("active_slot_unavailable", "稳定 launcher target 对应的不可变槽不可用")
    return executable


def user_shortcut_path(app_data: str | Path | None = None) -> Path:
    raw = str(app_data or os.environ.get("APPDATA", "")).strip()
    if not raw:
        raise ActivationBlocked("appdata_required", "APPDATA 必须存在才能创建用户快捷方式")
    root = Path(raw).expanduser().resolve()
    if not root.is_absolute():
        raise ActivationBlocked("appdata_required", "APPDATA 必须是绝对路径")
    return root / _SHORTCUT_SUBDIRECTORY / SHORTCUT_NAME


def _launcher_script() -> str:
    # %~dp0 is the fixed installed app root.  launcher.target is the only
    # mutable input, and it is validated as a full lowercase SHA before the
    # target is expanded into a command path.
    return (
        "@echo off\r\n"
        "setlocal EnableExtensions DisableDelayedExpansion\r\n"
        "set \"QM_APP_ROOT=%~dp0\"\r\n"
        "set \"QM_APP_DIR=%QM_APP_ROOT:~0,-1%\"\r\n"
        "\"%SystemRoot%\\System32\\fsutil.exe\" reparsepoint query "
        "\"%QM_APP_DIR%\" >nul 2>&1 && exit /b 6\r\n"
        "set \"QM_TARGET=\"\r\n"
        "set \"QM_LINES=\"\r\n"
        "for /f %%N in ('%SystemRoot%\\System32\\findstr.exe /n \"^\" "
        "\"%QM_APP_ROOT%launcher.target\" ^| %SystemRoot%\\System32\\find.exe /c \":\"') "
        "do set \"QM_LINES=%%N\"\r\n"
        "if not \"%QM_LINES%\"==\"1\" exit /b 3\r\n"
        "for /f \"delims=\" %%A in ('%SystemRoot%\\System32\\findstr.exe /r /x "
        "\"[0-9a-f]*\" \"%QM_APP_ROOT%launcher.target\" 2^>nul') do set \"QM_TARGET=%%A\"\r\n"
        "if not defined QM_TARGET exit /b 3\r\n"
        "if \"%QM_TARGET:~39,1%\"==\"\" exit /b 3\r\n"
        "if not \"%QM_TARGET:~40,1%\"==\"\" exit /b 3\r\n"
        "set \"QM_SLOTS=%QM_APP_ROOT%slots\"\r\n"
        "if not exist \"%QM_SLOTS%\\\" exit /b 4\r\n"
        "\"%SystemRoot%\\System32\\fsutil.exe\" reparsepoint query \"%QM_SLOTS%\" >nul 2>&1 && exit /b 6\r\n"
        "set \"QM_SLOT=%QM_SLOTS%\\%QM_TARGET%\"\r\n"
        "if not exist \"%QM_SLOT%\\\" exit /b 4\r\n"
        "\"%SystemRoot%\\System32\\fsutil.exe\" reparsepoint query \"%QM_SLOT%\" >nul 2>&1 && exit /b 6\r\n"
        "set \"QM_EXE=%QM_SLOT%\\QuantMaster.exe\"\r\n"
        "if not exist \"%QM_EXE%\" exit /b 4\r\n"
        "\"%SystemRoot%\\System32\\fsutil.exe\" reparsepoint query \"%QM_EXE%\" >nul 2>&1 && exit /b 6\r\n"
        "pushd \"%QM_SLOT%\" >nul || exit /b 5\r\n"
        "\"%QM_EXE%\" serve %*\r\n"
        "set \"QM_EXIT_CODE=%ERRORLEVEL%\"\r\n"
        "popd >nul\r\n"
        "exit /b %QM_EXIT_CODE%\r\n"
    )


def _write_launcher(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(_launcher_script(), encoding="ascii", newline="")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _powershell() -> str:
    value = shutil.which("powershell.exe") or shutil.which("powershell")
    if not value:
        raise ActivationBlocked("shortcut_unavailable", "找不到 Windows PowerShell 快捷方式组件")
    return value


def create_stable_shortcut(
    *, app_root: str | Path | None = None, shortcut: str | Path | None = None,
) -> dict[str, Any]:
    """Create one Start Menu shortcut targeting only the fixed launcher file."""

    if os.name != "nt":
        raise ActivationBlocked("windows_only", "稳定快捷方式只支持 Windows")
    root = Path(app_root).resolve() if app_root is not None else installed_app_root()
    launcher = stable_launcher_path(root)
    _write_launcher(launcher)
    target = Path(shortcut).resolve() if shortcut is not None else user_shortcut_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    script = (
        "$shell = New-Object -ComObject WScript.Shell; "
        "$item = $shell.CreateShortcut($args[0]); "
        "$item.TargetPath = $args[1]; "
        "$item.WorkingDirectory = $args[2]; "
        "$item.Description = 'QuantMaster stable immutable-slot launcher'; "
        "$item.Save()"
    )
    try:
        subprocess.run(
            [
                _powershell(), "-NoProfile", "-NonInteractive", "-Command", script,
                str(target), str(launcher), str(root),
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ActivationBlocked("shortcut_failed", "无法保存 QuantMaster 用户快捷方式") from exc
    return {
        "status": "configured",
        "launcher": str(launcher),
        "shortcut": str(target),
        "target_kind": "fixed_stable_launcher",
    }
