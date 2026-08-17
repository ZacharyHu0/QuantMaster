"""QuantMaster 打包辅助。

供 PyInstaller spec 等打包流程读取运行时版本，避免直接 exec 发布元数据文件。
完整的制品检查与尺寸门禁继续由 ``scripts/release/check_desktop_artifact.py`` 拥有。
"""

from __future__ import annotations

from quantmaster.release import VERSION


def packaged_version() -> str:
    """Return the runtime version embedded into packaged artifacts."""

    return VERSION


def packaged_version_tuple() -> tuple[int, int, int, int]:
    """Return a 4-tuple suitable for Windows VSVersionInfo."""

    major, minor, patch = VERSION.split(".")
    return int(major), int(minor), int(patch), 0
