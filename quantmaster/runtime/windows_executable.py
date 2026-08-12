"""Native Windows executable identity resources for observable worker roles."""

from __future__ import annotations

import ctypes
import os
import struct
from ctypes import wintypes
from pathlib import Path


def _pad(data: bytes) -> bytes:
    return data + b"\0" * (-len(data) % 4)


def _block(
    key: str,
    *,
    value: bytes = b"",
    value_length: int = 0,
    value_type: int = 1,
    children: tuple[bytes, ...] = (),
) -> bytes:
    body = struct.pack("<HHH", 0, value_length, value_type)
    body += key.encode("utf-16le") + b"\0\0"
    body = _pad(body) + value
    body = _pad(body) + b"".join(children)
    return struct.pack("<H", len(body)) + body[2:]


def _text_block(key: str, value: str) -> bytes:
    encoded = value.encode("utf-16le") + b"\0\0"
    return _block(key, value=encoded, value_length=len(value) + 1)


def build_version_resource(
    version: str,
    *,
    description: str = "QuantMaster",
    internal_name: str = "QuantMaster",
    original_filename: str = "QuantMaster.exe",
) -> bytes:
    numbers = [int(part) for part in version.split(".")]
    if len(numbers) != 3 or any(number < 0 or number > 65535 for number in numbers):
        raise ValueError(f"无效版本号：{version}")
    major, minor, patch = numbers
    fixed = struct.pack(
        "<13I",
        0xFEEF04BD,
        0x00010000,
        (major << 16) | minor,
        patch << 16,
        (major << 16) | minor,
        patch << 16,
        0x3F,
        0,
        0x00040004,
        1,
        0,
        0,
        0,
    )
    display_version = f"{version}.0"
    strings = {
        "CompanyName": "QuantMaster Contributors",
        "FileDescription": description,
        "FileVersion": display_version,
        "InternalName": internal_name,
        "OriginalFilename": original_filename,
        "ProductName": "QuantMaster",
        "ProductVersion": display_version,
    }
    table = _block(
        "040904B0",
        children=tuple(_text_block(key, value) for key, value in strings.items()),
    )
    translation = _block(
        "Translation",
        value=struct.pack("<HH", 0x0409, 0x04B0),
        value_length=4,
        value_type=0,
    )
    return _block(
        "VS_VERSION_INFO",
        value=fixed,
        value_length=len(fixed),
        value_type=0,
        children=(
            _block("StringFileInfo", children=(table,)),
            _block("VarFileInfo", children=(translation,)),
        ),
    )


def write_version_resource(
    executable: Path,
    version: str,
    *,
    description: str,
    internal_name: str,
    original_filename: str | None = None,
) -> None:
    """Replace the executable's VERSIONINFO with a human-readable role."""

    if os.name != "nt":
        raise OSError("Windows 可执行文件资源只能在 Windows 上写入")
    filename = str(original_filename or executable.name)
    payload = build_version_resource(
        version,
        description=description,
        internal_name=internal_name,
        original_filename=filename,
    )
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)  # type: ignore[attr-defined]
    begin = kernel32.BeginUpdateResourceW
    begin.argtypes = (ctypes.c_wchar_p, wintypes.BOOL)
    begin.restype = wintypes.HANDLE
    update = kernel32.UpdateResourceW
    update.argtypes = (
        wintypes.HANDLE,
        ctypes.c_void_p,
        ctypes.c_void_p,
        wintypes.WORD,
        ctypes.c_void_p,
        wintypes.DWORD,
    )
    update.restype = wintypes.BOOL
    end = kernel32.EndUpdateResourceW
    end.argtypes = (wintypes.HANDLE, wintypes.BOOL)
    end.restype = wintypes.BOOL

    handle = begin(str(executable), False)
    if not handle:
        raise ctypes.WinError(ctypes.get_last_error())  # type: ignore[attr-defined]
    buffer = ctypes.create_string_buffer(payload)
    committed = False
    try:
        if not update(
            handle,
            ctypes.c_void_p(16),
            ctypes.c_void_p(1),
            0x0409,
            ctypes.cast(buffer, ctypes.c_void_p),
            len(payload),
        ):
            raise ctypes.WinError(ctypes.get_last_error())  # type: ignore[attr-defined]
        if not end(handle, False):
            raise ctypes.WinError(ctypes.get_last_error())  # type: ignore[attr-defined]
        committed = True
    finally:
        if not committed:
            end(handle, True)
