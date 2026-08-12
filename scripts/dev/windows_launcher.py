"""Build a branded CPython host that remains the QuantMaster root process.

Copying a virtual-environment redirector gives the launcher the right filename,
but that redirector immediately creates a base ``python.exe`` process. Windows
then sees the application workers as children of Python rather than children of
QuantMaster. This builder copies the real CPython host and its runtime DLLs
beside the venv instead, so editable imports still work while ``QuantMaster.exe``
itself owns the complete process tree.
"""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
import shutil
import struct
import sys
from pathlib import Path

from quantmaster.runtime.windows_executable import build_version_resource

_SCHEMA = 3
_RT_ICON = 3
_RT_GROUP_ICON = 14
_RT_VERSION = 16


def _digest(*paths: Path) -> str:
    value = hashlib.sha256(f"launcher-v{_SCHEMA}".encode())
    for path in paths:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                value.update(chunk)
    return value.hexdigest()


def _read_icon(path: Path) -> tuple[list[bytes], bytes]:
    """Return RT_ICON payloads and the matching RT_GROUP_ICON directory."""
    payload = path.read_bytes()
    if len(payload) < 6:
        raise ValueError("图标文件不完整")
    reserved, kind, count = struct.unpack_from("<HHH", payload)
    if reserved != 0 or kind != 1 or count < 1:
        raise ValueError("需要包含至少一个图像的 Windows ICO 文件")
    directory_end = 6 + count * 16
    if len(payload) < directory_end:
        raise ValueError("图标目录不完整")

    images: list[bytes] = []
    group_entries: list[bytes] = []
    for index in range(count):
        entry = struct.unpack_from("<BBBBHHII", payload, 6 + index * 16)
        width, height, colors, entry_reserved, planes, bits, size, offset = entry
        if size < 1 or offset < directory_end or offset + size > len(payload):
            raise ValueError("图标图像范围无效")
        images.append(payload[offset:offset + size])
        group_entries.append(struct.pack(
            "<BBBBHHIH",
            width,
            height,
            colors,
            entry_reserved,
            planes,
            bits,
            size,
            index + 1,
        ))
    return images, struct.pack("<HHH", 0, 1, count) + b"".join(group_entries)


def _integer_resource(identifier: int) -> ctypes.c_void_p:
    return ctypes.c_void_p(identifier)


def _version_resource(version: str) -> bytes:
    return build_version_resource(version)


def _write_resources(executable: Path, icon: Path, version: str) -> None:
    if os.name != "nt":
        raise OSError("Windows 启动器只能在 Windows 上生成")
    images, group = _read_icon(icon)
    version_data = _version_resource(version)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    begin = kernel32.BeginUpdateResourceW
    begin.argtypes = (ctypes.c_wchar_p, ctypes.c_bool)
    begin.restype = ctypes.c_void_p
    update = kernel32.UpdateResourceW
    update.argtypes = (
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_ushort,
        ctypes.c_void_p,
        ctypes.c_uint,
    )
    update.restype = ctypes.c_bool
    end = kernel32.EndUpdateResourceW
    end.argtypes = (ctypes.c_void_p, ctypes.c_bool)
    end.restype = ctypes.c_bool

    handle = begin(str(executable), False)
    if not handle:
        raise ctypes.WinError(ctypes.get_last_error())
    committed = False
    buffers: list[ctypes.Array[ctypes.c_char]] = []
    try:
        for identifier, image in enumerate(images, start=1):
            buffer = ctypes.create_string_buffer(image)
            buffers.append(buffer)
            if not update(
                handle,
                _integer_resource(_RT_ICON),
                _integer_resource(identifier),
                0,
                ctypes.cast(buffer, ctypes.c_void_p),
                len(image),
            ):
                raise ctypes.WinError(ctypes.get_last_error())
        group_buffer = ctypes.create_string_buffer(group)
        buffers.append(group_buffer)
        if not update(
            handle,
            _integer_resource(_RT_GROUP_ICON),
            _integer_resource(1),
            0,
            ctypes.cast(group_buffer, ctypes.c_void_p),
            len(group),
        ):
            raise ctypes.WinError(ctypes.get_last_error())
        version_buffer = ctypes.create_string_buffer(version_data)
        buffers.append(version_buffer)
        if not update(
            handle,
            _integer_resource(_RT_VERSION),
            _integer_resource(1),
            0x0409,
            ctypes.cast(version_buffer, ctypes.c_void_p),
            len(version_data),
        ):
            raise ctypes.WinError(ctypes.get_last_error())
        if not end(handle, False):
            raise ctypes.WinError(ctypes.get_last_error())
        committed = True
    finally:
        if not committed:
            end(handle, True)


def _runtime_dependencies(source: Path) -> tuple[Path, ...]:
    """Return the adjacent DLLs required by a renamed CPython host."""

    names = (
        f"python{sys.version_info.major}{sys.version_info.minor}.dll",
        f"python{sys.version_info.major}.dll",
        "vcruntime140.dll",
        "vcruntime140_1.dll",
    )
    return tuple(path for name in names if (path := source.with_name(name)).is_file())


def _copy_runtime_dependencies(source: Path, output: Path) -> None:
    for dependency in _runtime_dependencies(source):
        destination = output.with_name(dependency.name)
        source_stat = dependency.stat()
        try:
            current = destination.stat()
        except FileNotFoundError:
            current = None
        if current is not None and current.st_size == source_stat.st_size:
            continue
        temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
        try:
            shutil.copy2(dependency, temporary)
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)


def build_launcher(source: Path, icon: Path, output: Path, version: str) -> bool:
    """Create or refresh the launcher; return whether a file was replaced."""
    source = source.resolve()
    icon = icon.resolve()
    output = output.resolve()
    if not source.is_file():
        raise FileNotFoundError(f"未找到虚拟环境解释器：{source}")
    if not icon.is_file():
        raise FileNotFoundError(f"未找到 QuantMaster 图标：{icon}")

    dependencies = _runtime_dependencies(source)
    fingerprint = hashlib.sha256(
        (_digest(source, icon, *dependencies) + "|" + version).encode(),
    ).hexdigest()
    marker = output.with_suffix(output.suffix + ".json")
    try:
        current = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        current = {}
    dependencies_ready = all(
        output.with_name(path.name).is_file()
        and output.with_name(path.name).stat().st_size == path.stat().st_size
        for path in dependencies
    )
    if output.is_file() and dependencies_ready and current.get("fingerprint") == fingerprint:
        return False

    output.parent.mkdir(parents=True, exist_ok=True)
    _copy_runtime_dependencies(source, output)
    temporary = output.with_name(f".{output.name}.{os.getpid()}.tmp")
    try:
        shutil.copy2(source, temporary)
        _write_resources(temporary, icon, version)
        os.replace(temporary, output)
        marker.write_text(
            json.dumps({"schema": _SCHEMA, "fingerprint": fingerprint}),
            encoding="utf-8",
        )
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    return True


def main() -> int:
    from quantmaster.release import VERSION

    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--icon", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    base_source = Path(getattr(sys, "_base_executable", ""))
    source = base_source if base_source.is_file() else args.source
    build_launcher(source, args.icon, args.output, VERSION)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
