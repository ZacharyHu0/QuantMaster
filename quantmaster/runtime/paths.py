"""Filesystem boundaries for paths stored in user-visible manifests."""

from __future__ import annotations

import os
import re
from pathlib import Path, PureWindowsPath

_SAFE_COMPONENT = re.compile(r"[0-9A-Za-z._-]+")


def confined_path(root: str | Path, relative: object, *, label: str = "文件") -> Path:
    """Resolve a manifest path below *root* without joining untrusted path syntax."""
    text = str(relative or "").replace("\\", "/")
    if not text or "\x00" in text or PureWindowsPath(text).drive or text.startswith("/"):
        raise ValueError(f"{label}路径无效")
    parts: list[str] = []
    for part in text.split("/"):
        safe = os.path.basename(part)
        if (
            not safe
            or safe in {".", ".."}
            or safe != part
            or _SAFE_COMPONENT.fullmatch(safe) is None
        ):
            raise ValueError(f"{label}路径无效")
        parts.append(safe)
    boundary = Path(root).resolve()
    candidate = boundary.joinpath(*parts).resolve()
    if candidate == boundary or not candidate.is_relative_to(boundary):
        raise ValueError(f"{label}路径越出数据目录")
    return candidate
