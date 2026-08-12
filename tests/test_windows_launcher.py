from __future__ import annotations

import struct

import pytest

from quantmaster.release import VERSION
from scripts.dev.windows_launcher import _read_icon, _version_resource


def test_root_serve_wrapper_forwards_to_development_launcher() -> None:
    from pathlib import Path

    wrapper = (Path(__file__).parents[1] / "qm-serve.cmd").read_text(encoding="utf-8")

    assert 'call "%~dp0scripts\\dev\\serve.cmd" %*' in wrapper
    assert "exit /b %ERRORLEVEL%" in wrapper


def test_project_icon_has_valid_group_and_images() -> None:
    from pathlib import Path

    icon = Path(__file__).parents[1] / "packaging" / "quantmaster.ico"
    images, group = _read_icon(icon)

    reserved, kind, count = struct.unpack_from("<HHH", group)
    assert (reserved, kind, count) == (0, 1, len(images))
    assert len(images) >= 6
    assert all(images)


def test_launcher_version_resource_uses_quantmaster_identity() -> None:
    payload = _version_resource(VERSION)

    assert struct.unpack_from("<H", payload)[0] == len(payload)
    assert "QuantMaster".encode("utf-16le") in payload
    assert "QuantMaster.exe".encode("utf-16le") in payload
    assert f"{VERSION}.0".encode("utf-16le") in payload


def test_pyinstaller_collects_scipy_array_api_compatibility_modules() -> None:
    from pathlib import Path

    spec = (Path(__file__).parents[1] / "packaging" / "quantmaster.spec").read_text(
        encoding="utf-8",
    )
    assert 'collect_submodules("scipy._external.array_api_compat")' in spec
    assert 'release_scope["VERSION"]' in spec
    assert "version=version_info" in spec
    assert 'if sys.platform == "win32":' in spec


@pytest.mark.parametrize("version", ["1.2", "1.2.3.4", "1.x.3", "-1.2.3"])
def test_launcher_version_resource_rejects_invalid_versions(version: str) -> None:
    with pytest.raises((ValueError, TypeError)):
        _version_resource(version)
