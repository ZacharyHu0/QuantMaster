"""Run the GitHub CI quality gates locally before creating a release commit."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def project_python() -> Path:
    """Return the repository virtualenv interpreter on every supported OS."""

    candidates = (
        ROOT / ".venv" / "Scripts" / "python.exe",
        ROOT / ".venv" / "bin" / "python",
    )
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise SystemExit("[local-ci] project interpreter missing: run uv sync first")


PYTHON = project_python()


def run(label: str, args: list[str], *, env: dict[str, str] | None = None) -> None:
    command = [str(PYTHON), *args]
    print(f"\n[local-ci] {label}: {' '.join(command)}", flush=True)
    completed = subprocess.run(command, cwd=ROOT, env=env, check=False)
    if completed.returncode:
        raise SystemExit(f"[local-ci] FAILED: {label} (exit {completed.returncode})")


def run_external(
    label: str,
    command: list[str],
    *,
    cwd: Path = ROOT,
    env: dict[str, str] | None = None,
) -> None:
    print(f"\n[local-ci] {label}: {' '.join(command)}", flush=True)
    completed = subprocess.run(command, cwd=cwd, env=env, check=False)
    if completed.returncode:
        raise SystemExit(f"[local-ci] FAILED: {label} (exit {completed.returncode})")


def pytest_shard(shard: int) -> None:
    run(
        f"full test shard {shard}/3",
        [
            "-m",
            "pytest",
            "--full",
            "--splits",
            "3",
            "--group",
            str(shard),
            "--ignore=tests/test_ui_management.py",
            "--timeout=180",
            "--durations=30",
        ],
    )


def rust_environment() -> dict[str, str]:
    """Expose complete Windows SDK and MSVC libraries to the linker."""

    env = os.environ.copy()
    if os.name != "nt":
        return env
    sdk_root = Path(os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")) / (
        "Windows Kits/10/Lib"
    )
    sdk_versions = sorted(
        (
            path
            for path in sdk_root.iterdir()
            if (path / "um/x64/kernel32.Lib").is_file()
            and (path / "ucrt/x64/ucrt.lib").is_file()
        ),
        reverse=True,
    ) if sdk_root.is_dir() else []
    vs_root = Path(os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")) / (
        "Microsoft Visual Studio"
    )
    msvc_versions = sorted(
        (
            path
            for path in vs_root.glob("*/BuildTools/VC/Tools/MSVC/*/lib/x64")
            if (path / "legacy_stdio_definitions.lib").is_file()
        ),
        reverse=True,
    )
    if not sdk_versions or not msvc_versions:
        return env
    sdk = sdk_versions[0]
    libraries = [str(msvc_versions[0]), str(sdk / "ucrt/x64"), str(sdk / "um/x64")]
    if env.get("LIB"):
        libraries.append(env["LIB"])
    env["LIB"] = os.pathsep.join(libraries)
    return env


def run_shards() -> None:
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {pool.submit(pytest_shard, shard): shard for shard in (1, 2, 3)}
        for future in as_completed(futures):
            future.result()


def smoke_fresh_wheel() -> None:
    """Install the wheel in an isolated environment and exercise the CLI."""

    wheels = sorted((ROOT / "dist").glob("quantmaster-*.whl"))
    if not wheels:
        raise SystemExit("[local-ci] wheel build did not produce a wheel")
    with tempfile.TemporaryDirectory(prefix="quantmaster-wheel-") as raw_temp:
        temp = Path(raw_temp)
        wheel = temp / wheels[-1].name
        shutil.copy2(wheels[-1], wheel)
        target = temp / "site"
        run_external(
            "fresh wheel install",
            [str(PYTHON), "-m", "pip", "install", "--no-deps", "--target", str(target), str(wheel)],
            cwd=temp,
        )
        smoke_env = os.environ.copy()
        smoke_env["QM_CONFIG_PATH"] = os.devnull
        smoke_env["QM_DATA_ROOT"] = str(temp / "data")
        smoke_env["PYTHONPATH"] = str(target)
        invoke = "from quantmaster.cli import main; raise SystemExit(main(ARGS))"
        run_external(
            "fresh wheel CLI help",
            [str(PYTHON), "-c", invoke.replace("ARGS", "['--help']")],
            cwd=temp,
            env=smoke_env,
        )
        run_external(
            "fresh wheel doctor",
            [str(PYTHON), "-c", invoke.replace("ARGS", "['doctor', '--deep']")],
            cwd=temp,
            env=smoke_env,
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ui", action="store_true", help="also run Chromium management tests")
    parser.add_argument("--package", action="store_true", help="also build and smoke-test the wheel/EXE")
    parser.add_argument("--rust", action="store_true", help="also run Rust format/check/clippy/test gates")
    parser.add_argument("--all", action="store_true", help="run UI, package and Rust gates too")
    parser.add_argument(
        "--serial",
        action="store_true",
        help="run full shards serially instead of in parallel",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run("ruff", ["-m", "ruff", "check", "quantmaster", "tests", "tools"])
    run("exception policy", ["tools/exception_policy.py"])
    run("complexity policy", ["tools/complexity_policy.py"])
    run("mypy", ["-m", "mypy"])
    run(
        "core tests",
        [
            "-m",
            "pytest",
            "tests/test_architecture.py",
            "tests/test_runtime_foundations.py",
            "tests/test_runtime_jobs.py",
            "tests/test_release_sync.py",
            "tests/test_settings.py",
            "--timeout=180",
            "--durations=30",
        ],
    )
    if args.serial:
        for shard in (1, 2, 3):
            pytest_shard(shard)
    else:
        run_shards()

    if args.ui or args.all:
        ui_env = os.environ.copy()
        ui_env["QM_RUN_UI"] = "1"
        run(
            "Chromium management",
            ["-m", "pytest", "tests/test_ui_management.py", "--timeout=180"],
            env=ui_env,
        )

    if args.rust or args.all:
        manifest = "rust/quantmaster-kernel/Cargo.toml"
        rust_env = rust_environment()
        run_external(
            "Rust fmt", ["cargo", "fmt", "--manifest-path", manifest, "--check"], env=rust_env,
        )
        run_external(
            "Rust check", ["cargo", "check", "--manifest-path", manifest, "--locked"], env=rust_env,
        )
        run_external(
            "Rust clippy",
            ["cargo", "clippy", "--manifest-path", manifest, "--locked", "--", "-D", "warnings"],
            env=rust_env,
        )
        run_external(
            "Rust tests", ["cargo", "test", "--manifest-path", manifest, "--locked"], env=rust_env,
        )

    if args.package or args.all:
        run("wheel build", ["-m", "build"])
        smoke_fresh_wheel()
        run("PyInstaller smoke", ["-m", "PyInstaller", "--noconfirm", "packaging/quantmaster.spec"])
        exe = ROOT / "dist" / "QuantMaster.exe"
        if exe.exists():
            run_external("EXE help", [str(exe), "--help"])
        else:
            print("[local-ci] EXE help skipped: platform output is not QuantMaster.exe")

    print("\n[local-ci] ALL REQUESTED GATES PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
