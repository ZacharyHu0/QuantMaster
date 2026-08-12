from __future__ import annotations

import os
import struct
import subprocess
import sys

import pytest

from quantmaster.release import VERSION
from scripts.dev.windows_launcher import (
    _read_icon,
    _runtime_dependencies,
    _version_resource,
    build_launcher,
)


def test_root_serve_wrapper_forwards_to_development_launcher() -> None:
    from pathlib import Path

    wrapper = (Path(__file__).parents[1] / "qm-serve.cmd").read_text(encoding="utf-8")

    assert 'call "%~dp0scripts\\dev\\serve.cmd" %*' in wrapper
    assert "exit /b %ERRORLEVEL%" in wrapper


def test_development_launcher_keeps_quantmaster_attached_to_ctrl_c() -> None:
    from pathlib import Path

    launcher = (Path(__file__).parents[1] / "scripts/dev/serve.cmd").read_text(
        encoding="utf-8",
    )

    assert '"%QM_LAUNCHER%" -m quantmaster.cli serve --reload %*' in launcher
    assert 'start "QuantMaster" /b' not in launcher


def test_packaged_entry_dispatches_multiprocessing_before_app_imports() -> None:
    from pathlib import Path

    entry = (Path(__file__).parents[1] / "packaging" / "entry.py").read_text(
        encoding="utf-8",
    )

    assert entry.index("multiprocessing.freeze_support()") < entry.index(
        "from quantmaster.cli import main"
    )


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


def test_launcher_uses_real_cpython_host_dependencies(tmp_path) -> None:
    host = tmp_path / "python.exe"
    host.write_bytes(b"host")
    runtime = tmp_path / "python312.dll"
    runtime.write_bytes(b"runtime")
    stable_abi = tmp_path / "python3.dll"
    stable_abi.write_bytes(b"stable")

    assert _runtime_dependencies(host) == (runtime, stable_abi)


@pytest.mark.skipif(os.name != "nt", reason="Windows executable contract")
def test_launcher_is_the_python_host_instead_of_a_redirector(tmp_path) -> None:
    from pathlib import Path

    venv = tmp_path / "venv"
    scripts = venv / "Scripts"
    scripts.mkdir(parents=True)
    current_config = Path(sys.prefix) / "pyvenv.cfg"
    (venv / "pyvenv.cfg").write_bytes(current_config.read_bytes())
    source = Path(sys._base_executable)
    output = scripts / "QuantMaster.exe"
    icon = Path(__file__).parents[1] / "packaging" / "quantmaster.ico"
    build_launcher(source, icon, output, VERSION)

    process = subprocess.Popen(
        [str(output), "-c", "import os; print(os.getpid())"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    stdout, stderr = process.communicate(timeout=10)

    assert process.returncode == 0, stderr
    assert int(stdout.strip()) == process.pid


def test_pyinstaller_collects_only_required_scipy_array_api_modules() -> None:
    from pathlib import Path

    spec = (Path(__file__).parents[1] / "packaging" / "quantmaster.spec").read_text(
        encoding="utf-8",
    )
    assert '"scipy._external.array_api_compat.common._fft"' in spec
    assert '"scipy._external.array_api_compat.numpy.fft"' in spec
    assert 'collect_submodules("scipy._external.array_api_compat")' not in spec
    assert '"torch"' in spec
    assert '"qrcode.tests"' in spec
    assert 'release_scope["VERSION"]' in spec
    assert "version=version_info" in spec
    assert 'if sys.platform == "win32":' in spec


@pytest.mark.parametrize("version", ["1.2", "1.2.3.4", "1.x.3", "-1.2.3"])
def test_launcher_version_resource_rejects_invalid_versions(version: str) -> None:
    with pytest.raises((ValueError, TypeError)):
        _version_resource(version)
