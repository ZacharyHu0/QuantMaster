"""Run the GitHub CI quality gates locally before creating a release commit."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def primary_root() -> Path:
    """Return the primary checkout even when CI runs inside a linked worktree."""
    git_pointer = ROOT / ".git"
    if git_pointer.is_file():
        line = git_pointer.read_text(encoding="utf-8").strip()
        if line.startswith("gitdir: "):
            worktree_git_dir = Path(line.removeprefix("gitdir: "))
            if worktree_git_dir.parent.name == "worktrees":
                return worktree_git_dir.parents[2].resolve()
    result = subprocess.run(
        [
            "git", "-c", f"safe.directory={ROOT.as_posix()}",
            "rev-parse", "--path-format=absolute", "--git-common-dir",
        ],
        cwd=ROOT, capture_output=True, text=True, encoding="utf-8",
        errors="replace",
        check=False,
    )
    if result.returncode:
        return ROOT
    common = Path(result.stdout.strip())
    return common.parent.resolve() if common.name == ".git" else ROOT


def artifact_root() -> Path:
    """Keep task artifacts outside the checkout Git must later remove."""
    primary = primary_root()
    if ROOT == primary:
        return primary / ".artifacts"
    return primary / ".artifacts" / "worktrees" / ROOT.name


ARTIFACTS = artifact_root()
PYTEST_ROOT = ARTIFACTS / "pytest" / "runs"
LOCAL_PYTEST_DURATIONS = ARTIFACTS / "pytest" / "durations.json"


def pytest_durations_path(primary: Path, artifacts: Path) -> Path:
    shared = primary / ".artifacts" / "pytest" / "durations.json"
    return shared if shared.is_file() else artifacts / "pytest" / "durations.json"


PYTEST_DURATIONS = pytest_durations_path(primary_root(), ARTIFACTS)
PACKAGE_ROOT = ARTIFACTS / "packages"
RUN_ROOT = PYTEST_ROOT / uuid.uuid4().hex[:12]
PYTEST_CACHE = ARTIFACTS / "pytest" / "cache"
_CLEANUP_ATTEMPTS = 8
_CLEANUP_INITIAL_DELAY_SECONDS = 0.1
_CLEANUP_MAX_DELAY_SECONDS = 1.0
_WINDOWS_TRANSIENT_CLEANUP_ERRORS = frozenset({32, 33, 145})
_cleanup_sleep = time.sleep


def prepare_pytest_directory(path: Path) -> Path:
    """Create a pytest directory without replacing inherited Windows ACLs."""
    from quantmaster.runtime.storage_governance import prepare_writable_directory

    target = path.resolve()
    prepare_writable_directory(target)
    return target


def cleanup_run_root(path: Path) -> None:
    """Remove one successful run despite transient locks or disappearing entries."""

    delay = _CLEANUP_INITIAL_DELAY_SECONDS
    for attempt in range(1, _CLEANUP_ATTEMPTS + 1):
        try:
            shutil.rmtree(path)
            return
        except FileNotFoundError as exc:
            try:
                path.stat()
            except FileNotFoundError:
                return
            if attempt == _CLEANUP_ATTEMPTS:
                raise RuntimeError(
                    f"[local-ci] successful run cleanup kept changing after {attempt} "
                    f"attempts; retained evidence at {path}"
                ) from exc
        except OSError as exc:
            if getattr(exc, "winerror", None) not in _WINDOWS_TRANSIENT_CLEANUP_ERRORS:
                raise
            if attempt == _CLEANUP_ATTEMPTS:
                raise RuntimeError(
                    f"[local-ci] successful run cleanup remained locked after {attempt} "
                    f"attempts; retained evidence at {path}"
                ) from exc
        _cleanup_sleep(delay)
        delay = min(delay * 2, _CLEANUP_MAX_DELAY_SECONDS)


def pytest_args(*args: str) -> list[str]:
    return [
        "-m", "pytest",
        "-o", f"cache_dir={PYTEST_CACHE}", *args,
    ]


def project_python() -> Path:
    """Return the repository virtualenv interpreter on every supported OS."""

    primary = primary_root()
    candidates = (
        primary / ".venv" / "Scripts" / "python.exe",
        primary / ".venv" / "bin" / "python",
    )
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise SystemExit(f"[local-ci] project interpreter missing under {primary}: run uv sync first")


PYTHON = project_python()


def run(label: str, args: list[str], *, env: dict[str, str] | None = None) -> None:
    command = [str(PYTHON), *args]
    print(f"\n[local-ci] {label}: {' '.join(command)}", flush=True)
    effective_env = os.environ.copy()
    effective_env["RUFF_CACHE_DIR"] = str(ARTIFACTS / "cache" / "ruff")
    effective_env["MYPY_CACHE_DIR"] = str(ARTIFACTS / "cache" / "mypy")
    effective_env["UV_CACHE_DIR"] = str(ARTIFACTS / "uv-cache")
    effective_env["QM_CONFIG_PATH"] = os.devnull
    effective_env["QM_FREE_STOCKDB_ROOT"] = str(
        ARTIFACTS / "runtime" / "tests" / "free-stockdb"
    )
    effective_env.update(env or {})
    completed = subprocess.run(command, cwd=ROOT, env=effective_env, check=False)
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
        pytest_args(
            "--full",
            "--splits",
            "3",
            "--group",
            str(shard),
            "--splitting-algorithm",
            "least_duration",
            "--durations-path",
            str(PYTEST_DURATIONS),
            "--ignore=tests/test_ui_management.py",
            "--timeout=180",
            "--durations=30",
            "--basetemp",
            str(RUN_ROOT / f"full-{shard}"),
        ),
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


def run_shards(max_workers: int = 3) -> None:
    """Run the three duration-balanced shards concurrently."""
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(pytest_shard, shard): shard for shard in (1, 2, 3)}
        for future in as_completed(futures):
            future.result()


def smoke_fresh_wheel() -> None:
    """Install the wheel in an isolated environment and exercise the CLI."""

    wheels = sorted((PACKAGE_ROOT / "python").glob("quantmaster-*.whl"))
    if not wheels:
        raise SystemExit("[local-ci] wheel build did not produce a wheel")
    with tempfile.TemporaryDirectory(prefix="quantmaster-wheel-") as raw_temp:
        temp = Path(raw_temp)
        wheel = temp / wheels[-1].name
        shutil.copy2(wheels[-1], wheel)
        target = temp / "site"
        install_env = os.environ.copy()
        install_env["UV_CACHE_DIR"] = str(ARTIFACTS / "uv-cache")
        run_external(
            "fresh wheel install",
            [
                "uv", "pip", "install", "--python", str(PYTHON), "--no-deps",
                "--target", str(target), str(wheel),
            ],
            cwd=temp,
            env=install_env,
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
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--fast", action="store_true", help="run static checks and core tests")
    mode.add_argument("--full", action="store_true", help="run static checks and full Python suite")
    parser.add_argument("--ui", action="store_true", help="also run Chromium management tests")
    parser.add_argument("--package", action="store_true", help="also build and smoke-test the wheel/EXE")
    parser.add_argument("--rust", action="store_true", help="also run Rust format/check/clippy/test gates")
    parser.add_argument("--all", action="store_true", help="run UI, package and Rust gates too")
    parser.add_argument(
        "--serial",
        action="store_true",
        help="run full shards serially instead of in parallel",
    )
    parser.add_argument(
        "--refresh-durations",
        action="store_true",
        help="sample the complete suite serially and refresh local pytest split timings",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    prepare_pytest_directory(RUN_ROOT)
    prepare_pytest_directory(PYTEST_CACHE)
    PACKAGE_ROOT.mkdir(parents=True, exist_ok=True)
    passed = False
    try:
        run("ruff", ["-m", "ruff", "check", "quantmaster", "tests", "scripts"])
        run("exception policy", ["scripts/ci/exception_policy.py"])
        run("complexity policy", ["scripts/ci/complexity_policy.py"])
        run("mypy", ["-m", "mypy"])
        if args.refresh_durations:
            run(
                "refresh full-suite durations",
                pytest_args(
                    "--full", "--ignore=tests/test_ui_management.py",
                    "--store-durations", "--clean-durations", "--durations-path",
                    str(LOCAL_PYTEST_DURATIONS), "--timeout=180", "--durations=30",
                    "--basetemp", str(RUN_ROOT / "duration-sample"),
                ),
            )
        elif args.full or args.all:
            if args.serial:
                for shard in (1, 2, 3):
                    pytest_shard(shard)
            else:
                run_shards()
        else:
            run(
                "core tests",
                pytest_args(
                    "tests/test_architecture.py",
                    "tests/test_runtime_foundations.py", "tests/test_runtime_jobs.py",
                    "tests/test_release_sync.py", "tests/test_settings.py",
                    "--timeout=180", "--durations=30", "--basetemp",
                    str(RUN_ROOT / "fast"),
                ),
            )

        if args.ui or args.all:
            ui_env = os.environ.copy()
            ui_env["QM_RUN_UI"] = "1"
            run(
                "Chromium management",
                pytest_args(
                    "tests/test_ui_management.py", "--timeout=180",
                    "--basetemp", str(RUN_ROOT / "ui"),
                ),
                env=ui_env,
            )

        if args.rust or args.all:
            manifest = "rust/quantmaster-kernel/Cargo.toml"
            rust_env = rust_environment()
            run_external(
                "Rust fmt", ["cargo", "fmt", "--manifest-path", manifest, "--check"],
                env=rust_env,
            )
            run_external(
                "Rust check", ["cargo", "check", "--manifest-path", manifest, "--locked"],
                env=rust_env,
            )
            run_external(
                "Rust clippy",
                ["cargo", "clippy", "--manifest-path", manifest, "--locked", "--", "-D", "warnings"],
                env=rust_env,
            )
            run_external(
                "Rust tests", ["cargo", "test", "--manifest-path", manifest, "--locked"],
                env=rust_env,
            )

        if args.package or args.all:
            build_env = os.environ.copy()
            build_env["UV_CACHE_DIR"] = str(ARTIFACTS / "uv-cache")
            run_external(
                "wheel build",
                ["uv", "build", "--out-dir", str(PACKAGE_ROOT / "python")],
                env=build_env,
            )
            smoke_fresh_wheel()
            run_external(
                "PyInstaller smoke",
                [
                    "uv", "run", "--no-project", "--python", str(PYTHON),
                    "--with", "PyInstaller==6.19.0", "-m", "PyInstaller", "--noconfirm",
                    "--distpath", str(PACKAGE_ROOT / "desktop"), "--workpath",
                    str(ARTIFACTS / "build" / "pyinstaller"), "packaging/quantmaster.spec",
                ],
                env=build_env,
            )
            exe = PACKAGE_ROOT / "desktop" / "QuantMaster.exe"
            if exe.exists():
                run(
                    "desktop artifact policy",
                    [
                        "scripts/release/check_desktop_artifact.py", str(exe), "--analysis",
                        str(ARTIFACTS / "build" / "pyinstaller/quantmaster/Analysis-00.toc"),
                    ],
                )
                run_external("EXE help", [str(exe), "--help"])
                with tempfile.TemporaryDirectory(prefix="quantmaster-exe-") as raw_temp:
                    exe_env = os.environ.copy()
                    exe_env["QM_CONFIG_PATH"] = os.devnull
                    exe_env["QM_DATA_ROOT"] = str(Path(raw_temp) / "data")
                    run_external("EXE doctor", [str(exe), "doctor", "--deep"], env=exe_env)
            else:
                print("[local-ci] EXE help skipped: platform output is not QuantMaster.exe")

        print("\n[local-ci] ALL REQUESTED GATES PASSED")
        passed = True
        return 0
    finally:
        if passed:
            cleanup_run_root(RUN_ROOT)


if __name__ == "__main__":
    raise SystemExit(main())
