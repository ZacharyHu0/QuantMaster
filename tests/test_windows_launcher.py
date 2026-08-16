from __future__ import annotations

import multiprocessing
import os
import runpy
import shutil
import struct
import subprocess
import sys
import zlib
from types import ModuleType, SimpleNamespace

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

    assert '"%QM_LAUNCHER%" -m quantmaster.cli serve %*' in launcher
    assert 'start "QuantMaster" /b' not in launcher


def test_stable_launcher_reads_only_the_validated_active_slot(tmp_path) -> None:
    from quantmaster.runtime.launcher import (
        _launcher_script,
        read_launcher_target,
        stable_slot_executable,
    )

    sha = "a" * 40
    slot = tmp_path / "slots" / sha
    slot.mkdir(parents=True)
    executable = slot / "QuantMaster.exe"
    executable.write_bytes(b"candidate")
    (tmp_path / "launcher.target").write_text(f"{sha}\n", encoding="ascii")

    assert read_launcher_target(tmp_path) == sha
    assert stable_slot_executable(tmp_path) == executable.resolve()
    script = _launcher_script()
    assert "launcher.target" in script
    assert (
        "for /f \"usebackq delims=\" %%A in (\"%QM_APP_ROOT%launcher.target\") "
        "do set \"QM_TARGET=%%A\"" in script
    )
    assert 'delims=0123456789abcdef' in script
    assert 'set "QM_SLOT=%QM_SLOTS%\\%QM_TARGET%"' in script
    assert 'if not "%QM_LINES%"=="1" (' in script
    assert 'reparsepoint query "%QM_SLOT%"' in script
    assert '"%QM_EXE%" serve --open %*' in script
    assert 'call :qm_fail 3' in script
    assert 'if /i not "%QM_LAUNCHER_NO_PAUSE%"=="1" pause' in script
    assert "python" not in script.lower()


def test_stable_launcher_rejects_tampered_target(tmp_path) -> None:
    from quantmaster.runtime.activation import ActivationBlocked
    from quantmaster.runtime.launcher import read_launcher_target

    (tmp_path / "launcher.target").write_text("checkout\n", encoding="ascii")
    with pytest.raises(ActivationBlocked, match="完整 lowercase SHA"):
        read_launcher_target(tmp_path)


@pytest.mark.skipif(os.name != "nt", reason="Windows stable launcher contract")
def test_stable_shortcut_starts_the_exact_single_line_slot(tmp_path, monkeypatch) -> None:
    from quantmaster.runtime.launcher import create_stable_shortcut

    monkeypatch.setenv("QM_LAUNCHER_NO_PAUSE", "1")
    root = tmp_path / "app"
    sha = "a" * 40
    slot = root / "slots" / sha
    slot.mkdir(parents=True)
    shutil.copy2(sys.executable, slot / "QuantMaster.exe")
    shutil.copy2(os.path.join(sys.prefix, "pyvenv.cfg"), slot / "pyvenv.cfg")
    (root / "launcher.target").write_text(f"{sha}\n", encoding="ascii")
    run = subprocess.run
    monkeypatch.setattr(
        "quantmaster.runtime.launcher.subprocess.run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0),
    )
    create_stable_shortcut(app_root=root, shortcut=tmp_path / "QuantMaster.lnk")

    result = run(
        ["cmd.exe", "/d", "/c", "call", str(root / "QuantMaster Stable Launcher.cmd")],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )

    assert result.returncode == 2  # copied Python host was invoked with `serve --open`


@pytest.mark.skipif(os.name != "nt", reason="Windows stable launcher contract")
@pytest.mark.parametrize(
    "target",
    (
        f"{'a' * 40}\n\n",
        f"{'a' * 40}\n..\\outside\n",
        r"..\outside".ljust(40, "a") + "\n",
        r"C:\outside".ljust(40, "a") + "\n",
        "safe:stream".ljust(40, "a") + "\n",
    ),
)
def test_stable_shortcut_rejects_unsafe_target_before_process_start(
    tmp_path, monkeypatch, target: str,
) -> None:
    from quantmaster.runtime.launcher import create_stable_shortcut

    monkeypatch.setenv("QM_LAUNCHER_NO_PAUSE", "1")
    root = tmp_path / "app"
    root.mkdir()
    run = subprocess.run
    monkeypatch.setattr(
        "quantmaster.runtime.launcher.subprocess.run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0),
    )
    create_stable_shortcut(app_root=root, shortcut=tmp_path / "QuantMaster.lnk")
    outside = root / "outside"
    outside.mkdir()
    shutil.copy2(sys.executable, outside / "QuantMaster.exe")
    (root / "launcher.target").write_text(target, encoding="ascii")

    result = run(
        ["cmd.exe", "/d", "/c", "call", str(root / "QuantMaster Stable Launcher.cmd")],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )

    assert result.returncode == 3
    assert "QuantMaster launcher error" in result.stdout


@pytest.mark.skipif(os.name != "nt", reason="Windows junction contract")
def test_stable_slot_executable_rejects_a_junction_candidate(tmp_path) -> None:
    from quantmaster.runtime.activation import ActivationBlocked
    from quantmaster.runtime.launcher import stable_slot_executable

    root = tmp_path / "app"
    slots = root / "slots"
    outside = tmp_path / "outside"
    slots.mkdir(parents=True)
    outside.mkdir()
    (outside / "QuantMaster.exe").write_bytes(b"outside")
    sha = "a" * 40
    subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(slots / sha), str(outside)],
        check=True,
        capture_output=True,
    )
    (root / "launcher.target").write_text(f"{sha}\n", encoding="ascii")

    with pytest.raises(ActivationBlocked, match="不可变槽不可用"):
        stable_slot_executable(root)


@pytest.mark.skipif(os.name != "nt", reason="Windows junction contract")
def test_stable_shortcut_rejects_a_junction_before_process_start(
    tmp_path, monkeypatch,
) -> None:
    from quantmaster.runtime.launcher import create_stable_shortcut

    monkeypatch.setenv("QM_LAUNCHER_NO_PAUSE", "1")
    root = tmp_path / "app"
    slots = root / "slots"
    outside = tmp_path / "outside"
    slots.mkdir(parents=True)
    outside.mkdir()
    shutil.copy2(sys.executable, outside / "QuantMaster.exe")
    shutil.copy2(os.path.join(sys.prefix, "pyvenv.cfg"), outside / "pyvenv.cfg")
    sha = "a" * 40
    subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(slots / sha), str(outside)],
        check=True,
        capture_output=True,
    )
    (root / "launcher.target").write_text(f"{sha}\n", encoding="ascii")
    run = subprocess.run
    monkeypatch.setattr(
        "quantmaster.runtime.launcher.subprocess.run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0),
    )
    create_stable_shortcut(app_root=root, shortcut=tmp_path / "QuantMaster.lnk")

    result = run(
        ["cmd.exe", "/d", "/c", "call", str(root / "QuantMaster Stable Launcher.cmd")],
        cwd=root,
        capture_output=True,
        timeout=10,
        check=False,
    )

    assert result.returncode == 6


@pytest.mark.skipif(os.name != "nt", reason="Windows junction contract")
def test_stable_shortcut_rejects_a_junction_application_root(
    tmp_path, monkeypatch,
) -> None:
    from quantmaster.runtime.launcher import create_stable_shortcut

    monkeypatch.setenv("QM_LAUNCHER_NO_PAUSE", "1")
    root = tmp_path / "app"
    outside = tmp_path / "outside-app"
    sha = "a" * 40
    slot = outside / "slots" / sha
    slot.mkdir(parents=True)
    shutil.copy2(sys.executable, slot / "QuantMaster.exe")
    shutil.copy2(os.path.join(sys.prefix, "pyvenv.cfg"), slot / "pyvenv.cfg")
    (outside / "launcher.target").write_text(f"{sha}\n", encoding="ascii")
    subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(root), str(outside)],
        check=True,
        capture_output=True,
    )
    run = subprocess.run
    monkeypatch.setattr(
        "quantmaster.runtime.launcher.subprocess.run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0),
    )
    create_stable_shortcut(app_root=root, shortcut=tmp_path / "QuantMaster.lnk")

    result = run(
        ["cmd.exe", "/d", "/c", "call", str(root / "QuantMaster Stable Launcher.cmd")],
        cwd=tmp_path,
        capture_output=True,
        timeout=10,
        check=False,
    )

    assert result.returncode == 6


def test_packaged_entry_dispatches_multiprocessing_before_app_imports() -> None:
    from pathlib import Path

    entry = (Path(__file__).parents[1] / "packaging" / "entry.py").read_text(
        encoding="utf-8",
    )

    freeze = entry.index("multiprocessing.freeze_support()")
    stdout = entry.index('sys.stdout.reconfigure(encoding="utf-8")')
    configure = entry.index("configure_installed_instance()")
    cli_import = entry.index("from quantmaster.cli import main")
    stage = entry.index('update_splash("正在加载本地配置")')
    close = entry.index("close_splash()")

    assert configure < freeze < stdout < stage < cli_import < close
    assert all("%" not in line for line in entry.splitlines() if "update_splash" in line)


def test_packaged_child_binds_installed_instance_before_multiprocessing_dispatch(
    monkeypatch,
) -> None:
    from pathlib import Path

    calls: list[str] = []

    class ChildDispatched(RuntimeError):
        pass

    config = ModuleType("quantmaster.config")
    config.configure_installed_instance = lambda: calls.append("configure")
    monkeypatch.setitem(sys.modules, "quantmaster.config", config)

    def dispatch_child() -> None:
        calls.append("freeze")
        raise ChildDispatched

    monkeypatch.setattr(multiprocessing, "freeze_support", dispatch_child)

    with pytest.raises(ChildDispatched):
        runpy.run_path(
            Path(__file__).parents[1] / "packaging" / "entry.py",
            run_name="__main__",
        )

    assert calls == ["configure", "freeze"]


@pytest.mark.parametrize(
    ("arguments", "closed_before_handler"),
    (
        (("doctor",), True),
        (("backtest",), True),
        (("app",), False),
        (("serve",), False),
    ),
)
def test_packaged_entry_closes_cli_splash_before_long_handler(
    monkeypatch,
    arguments: tuple[str, ...],
    closed_before_handler: bool,
) -> None:
    from pathlib import Path

    calls = []
    config = ModuleType("quantmaster.config")
    config.configure_installed_instance = lambda: calls.append("configure")
    cli = ModuleType("quantmaster.cli")
    cli.main = lambda _arguments: calls.append(("main", "close" in calls)) or 0
    windows_app = ModuleType("quantmaster.runtime.windows_app")
    windows_app.initialize_windows_app_process = lambda **_kwargs: calls.append("initialize")
    splash = ModuleType("quantmaster.runtime.splash")
    splash.update_splash = lambda stage: calls.append(("stage", stage))
    splash.close_splash = lambda: calls.append("close")
    monkeypatch.setitem(sys.modules, "quantmaster.config", config)
    monkeypatch.setitem(sys.modules, "quantmaster.cli", cli)
    monkeypatch.setitem(sys.modules, "quantmaster.runtime.windows_app", windows_app)
    monkeypatch.setitem(sys.modules, "quantmaster.runtime.splash", splash)
    monkeypatch.setattr(multiprocessing, "freeze_support", lambda: None)
    monkeypatch.setattr(sys, "argv", ["QuantMaster.exe", *arguments])
    monkeypatch.setattr(sys, "stdout", SimpleNamespace(reconfigure=lambda **_kwargs: None))

    with pytest.raises(SystemExit, match="0"):
        runpy.run_path(
            Path(__file__).parents[1] / "packaging" / "entry.py",
            run_name="__main__",
        )

    handler_call = next(call for call in calls if isinstance(call, tuple) and call[0] == "main")
    assert handler_call == ("main", closed_before_handler)
    assert calls[-1] == "close"


def test_splash_brand_png_is_a_fixed_size_dark_panel_for_the_existing_wordmark() -> None:
    from pathlib import Path

    image = Path(__file__).parents[1] / "packaging" / "quantmaster-splash.png"
    payload = image.read_bytes()

    assert payload[:8] == b"\x89PNG\r\n\x1a\n"
    assert struct.unpack("!II", payload[16:24]) == (760, 300)

    offset = 8
    compressed = bytearray()
    while offset < len(payload):
        length = struct.unpack("!I", payload[offset : offset + 4])[0]
        kind = payload[offset + 4 : offset + 8]
        chunk = payload[offset + 8 : offset + 8 + length]
        offset += length + 12
        if kind == b"IDAT":
            compressed.extend(chunk)
    raw = zlib.decompress(compressed)
    stride = 760 * 4
    assert all(raw[row * (stride + 1)] == 0 for row in range(300))

    def pixel(x: int, y: int) -> tuple[int, int, int, int]:
        start = y * (stride + 1) + 1 + x * 4
        return tuple(raw[start : start + 4])

    assert pixel(0, 0) == (255, 0, 255, 255)
    assert pixel(380, 290) == (5, 5, 5, 255)
    assert pixel(170, 120) == (244, 247, 251, 255)
    assert pixel(242, 73) == (57, 135, 229, 255)
    assert pixel(447, 98) == (57, 135, 229, 255)


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


def test_pyinstaller_does_not_collect_scipy() -> None:
    from pathlib import Path

    spec = (Path(__file__).parents[1] / "packaging" / "quantmaster.spec").read_text(
        encoding="utf-8",
    )
    assert '"scipy.' not in spec
    assert '"sklearn", "scipy"' in spec
    assert '"torch"' in spec
    assert '"qrcode.tests"' in spec
    assert 'release_scope["VERSION"]' in spec
    assert "version=version_info" in spec
    assert 'if sys.platform == "win32":' in spec


def test_pyinstaller_prunes_unused_lark_oapi_and_pyarrow_payloads() -> None:
    from pathlib import Path

    spec = (Path(__file__).parents[1] / "packaging" / "quantmaster.spec").read_text(
        encoding="utf-8",
    )
    assert 'collect_submodules("lark_oapi"' not in spec
    assert '"lark_oapi.api.im.v1"' in spec
    assert '"lark_oapi.channel.events"' in spec
    assert '"lark_oapi.ws.client"' in spec
    assert '"lark_oapi.adapter"' in spec
    for module in (
        "pyarrow.flight",
        "pyarrow.substrait",
        "pyarrow.cuda",
        "pyarrow.dataset",
        "pyarrow.feather",
        "pyarrow.parquet.encryption",
    ):
        assert f'"{module}"' in spec


def test_pyinstaller_runtime_hook_binds_clean_full_git_sha() -> None:
    from pathlib import Path

    spec = (Path(__file__).parents[1] / "packaging" / "quantmaster.spec").read_text(
        encoding="utf-8",
    )
    assert "build_sha = packaged_build_sha(project_root)" in spec
    assert '"diff-index"' not in spec
    assert '"status", "--porcelain"' not in spec
    assert "bind_packaged_build" in spec
    assert "runtime_hooks=[str(runtime_identity_hook)]" in spec


def test_pyinstaller_defaults_to_onefile_and_requires_explicit_windows_onedir(
    monkeypatch,
) -> None:
    from pathlib import Path
    from types import SimpleNamespace

    spec = (Path(__file__).parents[1] / "packaging" / "quantmaster.spec").read_text(
        encoding="utf-8",
    )
    layout_source = "package_layout =" + spec.split("package_layout =", 1)[1].split(
        "project_root =", 1,
    )[0]
    tail = "pyz = PYZ" + spec.split("pyz = PYZ", 1)[1]
    analysis = SimpleNamespace(
        pure=object(), scripts=object(), binaries=object(), datas=object(),
    )

    def evaluate(platform: str, package_layout: str | None = None):
        calls = []
        monkeypatch.setattr(sys, "platform", platform)
        if package_layout is None:
            monkeypatch.delenv("QM_DESKTOP_LAYOUT", raising=False)
        else:
            monkeypatch.setenv("QM_DESKTOP_LAYOUT", package_layout)

        class SplashResult:
            def __init__(self):
                self.binaries = object()

        splash_result = SplashResult()

        def record(name):
            def invoke(*args, **kwargs):
                calls.append((name, args, kwargs))
                return splash_result if name == "Splash" else object()

            return invoke

        scope = {"os": os, "sys": sys}
        exec(compile(layout_source, "quantmaster.spec", "exec"), scope)
        exec(compile(tail, "quantmaster.spec", "exec"), {
            "a": analysis,
            "PYZ": lambda _pure: object(),
            "Splash": record("Splash"),
            "EXE": record("EXE"),
            "COLLECT": record("COLLECT"),
            "project_root": Path("project"),
            "os": os,
            "sys": sys,
            "version_info": None,
            "onedir_measurement": scope["onedir_measurement"],
        })
        return calls

    windows = evaluate("win32")
    windows_onedir = evaluate("win32", "onedir-measurement")
    posix = evaluate("linux")

    assert windows[1][2]["exclude_binaries"] is False
    assert analysis.binaries in windows[1][1]
    assert analysis.datas in windows[1][1]
    assert windows[0][2]["full_tk"] is False
    assert windows[0][2]["text_pos"] == (32, 270)
    assert windows[0][2]["text_color"] == "#f4f7fb"
    assert windows[0][2]["text_default"]
    assert windows[0][1][0].name == "quantmaster-splash.png"
    assert windows[0][2].get("progress_bar") is None
    assert [call[0] for call in windows] == ["Splash", "EXE"]
    assert windows_onedir[0][2]["exclude_binaries"] is True
    assert analysis.binaries not in windows_onedir[0][1]
    assert [call[0] for call in windows_onedir] == ["EXE", "COLLECT"]
    assert analysis.binaries in windows_onedir[1][1]
    assert analysis.datas in windows_onedir[1][1]
    assert posix[0][2]["exclude_binaries"] is False
    assert analysis.binaries in posix[0][1]
    assert analysis.datas in posix[0][1]
    assert [call[0] for call in posix] == ["EXE"]

    with pytest.raises(ValueError, match="Windows-only"):
        evaluate("linux", "onedir-measurement")


def test_pyinstaller_rejects_non_windows_onedir_before_build_side_effects() -> None:
    from pathlib import Path

    spec = (Path(__file__).parents[1] / "packaging" / "quantmaster.spec").read_text(
        encoding="utf-8",
    )

    rejection = spec.index('package_layout = os.environ.get("QM_DESKTOP_LAYOUT"')
    runtime_hook_write = spec.index("runtime_identity_hook.write_text")
    analysis = spec.index("a = Analysis(")

    assert rejection < runtime_hook_write < analysis


def test_pyinstaller_spec_loads_identity_policy_outside_project_sys_path(
    tmp_path, monkeypatch,
) -> None:
    from pathlib import Path
    from types import ModuleType, SimpleNamespace

    project_root = Path(__file__).parents[1].resolve()
    spec_path = project_root / "packaging" / "quantmaster.spec"
    source = spec_path.read_text(encoding="utf-8")
    bootstrap = source.split("version_info = None", 1)[0] + "version_info = None\n"
    outside = tmp_path / "outside"
    workpath = tmp_path / "work"
    outside.mkdir()
    workpath.mkdir()
    monkeypatch.chdir(outside)
    monkeypatch.setattr(
        sys,
        "path",
        [
            entry for entry in sys.path
            if entry and Path(entry).resolve() != project_root
        ],
    )
    for name in tuple(sys.modules):
        if name == "scripts" or name.startswith("scripts."):
            monkeypatch.delitem(sys.modules, name)

    hooks = ModuleType("PyInstaller.utils.hooks")
    hooks.collect_submodules = lambda *_args, **_kwargs: []
    monkeypatch.setitem(sys.modules, "PyInstaller", ModuleType("PyInstaller"))
    monkeypatch.setitem(sys.modules, "PyInstaller.utils", ModuleType("PyInstaller.utils"))
    monkeypatch.setitem(sys.modules, "PyInstaller.utils.hooks", hooks)
    head = "a" * 40

    def clean_git(command, **_kwargs):
        if command[1] == "diff-index":
            return SimpleNamespace(returncode=0, stdout="")
        if command[1] == "ls-files":
            return SimpleNamespace(returncode=0, stdout="")
        return SimpleNamespace(returncode=0, stdout=f"{head}\n")

    monkeypatch.setattr(subprocess, "run", clean_git)
    scope = {
        "SPECPATH": str(project_root / "packaging"),
        "workpath": str(workpath),
    }

    exec(compile(bootstrap, str(spec_path), "exec"), scope)

    assert scope["build_sha"] == head
    assert head in (workpath / "quantmaster_runtime_identity.py").read_text(
        encoding="utf-8",
    )


@pytest.mark.parametrize("version", ["1.2", "1.2.3.4", "1.x.3", "-1.2.3"])
def test_launcher_version_resource_rejects_invalid_versions(version: str) -> None:
    with pytest.raises((ValueError, TypeError)):
        _version_resource(version)
