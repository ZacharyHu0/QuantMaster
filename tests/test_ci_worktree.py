"""Local CI must reuse the primary checkout virtual environment."""

import subprocess
import sys
import time
from pathlib import Path

import pytest

from scripts.ci import run


def test_primary_root_uses_git_common_directory(monkeypatch, tmp_path):
    primary = tmp_path / "primary"
    common = primary / ".git"

    class Result:
        returncode = 0
        stdout = str(common)

    captured = {}
    monkeypatch.setattr(run, "ROOT", tmp_path / "standalone")
    monkeypatch.setattr(
        run.subprocess, "run",
        lambda *args, **kwargs: captured.update(kwargs) or Result(),
    )
    assert run.primary_root() == primary.resolve()
    assert captured["encoding"] == "utf-8"
    assert captured["errors"] == "replace"


def test_primary_root_reads_unicode_linked_worktree_pointer(monkeypatch, tmp_path):
    primary = tmp_path / "研究" / "Quant"
    worktree = primary / ".worktrees" / "task"
    git_dir = primary / ".git" / "worktrees" / "task"
    worktree.mkdir(parents=True)
    (worktree / ".git").write_text(f"gitdir: {git_dir.as_posix()}\n", encoding="utf-8")
    monkeypatch.setattr(run, "ROOT", worktree)
    monkeypatch.setattr(
        run.subprocess,
        "run",
        lambda *args, **kwargs: pytest.fail("Git fallback should not run"),
    )

    assert run.primary_root() == primary.resolve()


def test_project_python_uses_primary_worktree(monkeypatch, tmp_path):
    primary = tmp_path / "primary"
    interpreter = primary / ".venv" / "Scripts" / "python.exe"
    interpreter.parent.mkdir(parents=True)
    interpreter.touch()
    monkeypatch.setattr(run, "primary_root", lambda: primary)
    assert run.project_python() == Path(interpreter)


def test_artifact_root_is_task_scoped_outside_linked_checkout(monkeypatch, tmp_path):
    primary = tmp_path / "primary"
    worktree = primary / ".worktrees" / "feature"
    monkeypatch.setattr(run, "ROOT", worktree)
    monkeypatch.setattr(run, "primary_root", lambda: primary)
    assert run.artifact_root() == primary / ".artifacts" / "worktrees" / "feature"


def test_artifact_root_keeps_primary_artifact_contract(monkeypatch, tmp_path):
    primary = tmp_path / "primary"
    monkeypatch.setattr(run, "ROOT", primary)
    monkeypatch.setattr(run, "primary_root", lambda: primary)
    assert run.artifact_root() == primary / ".artifacts"


def test_task_full_suite_reads_existing_primary_duration_cache(tmp_path):
    primary = tmp_path / "primary"
    artifacts = primary / ".artifacts" / "worktrees" / "task"
    shared = primary / ".artifacts" / "pytest" / "durations.json"
    shared.parent.mkdir(parents=True)
    shared.touch()

    assert run.pytest_durations_path(primary, artifacts) == shared


def test_pytest_args_override_checkout_cache_dir(monkeypatch, tmp_path):
    cache = tmp_path / "external-cache"
    monkeypatch.setattr(run, "PYTEST_CACHE", cache)
    assert run.pytest_args("tests/test_one.py") == [
        "-m", "pytest",
        "-o", f"cache_dir={cache}", "tests/test_one.py",
    ]


def test_prepare_pytest_cache_precreates_directory(tmp_path):
    from scripts.dev.pytest_windows_acl import prepare_pytest_directory

    cache = tmp_path / "task-artifacts" / "pytest" / "cache"

    assert run.prepare_pytest_directory is prepare_pytest_directory
    assert run.prepare_pytest_directory(cache) == cache
    assert cache.is_dir()


def _windows_cleanup_error(code: int, path: Path) -> OSError:
    error = OSError(f"locked: {path}")
    error.winerror = code
    error.filename = str(path)
    return error


def _task_run_root(tmp_path: Path, monkeypatch) -> Path:
    pytest_root = tmp_path / "task-artifacts" / "pytest" / "runs"
    pytest_root.mkdir(parents=True)
    monkeypatch.setattr(run, "PYTEST_ROOT", pytest_root)
    return pytest_root / ("a" * 12)


def test_cleanup_run_root_retries_when_child_disappears_during_walk(monkeypatch, tmp_path):
    run_root = _task_run_root(tmp_path, monkeypatch)
    run_root.mkdir()
    attempts = []
    delays = []

    def remove(path, **_kwargs):
        attempts.append(path)
        if len(attempts) == 1:
            raise FileNotFoundError(2, "missing child", path / "ui" / "jobs.sqlite-shm")

    monkeypatch.setattr(run.shutil, "rmtree", remove)
    monkeypatch.setattr(run, "_cleanup_sleep", delays.append)

    run.cleanup_run_root(run_root)

    assert attempts == [run_root, run_root]
    assert delays == [run._CLEANUP_INITIAL_DELAY_SECONDS]


def test_cleanup_run_root_accepts_concurrently_removed_root(monkeypatch, tmp_path):
    run_root = _task_run_root(tmp_path, monkeypatch)
    attempts = []
    sleeps = []
    monkeypatch.setattr(
        run.shutil, "rmtree",
        lambda path, **_kwargs: attempts.append(path) or (_ for _ in ()).throw(
            FileNotFoundError(2, "missing root", path),
        ),
    )
    monkeypatch.setattr(run, "_cleanup_sleep", sleeps.append)

    run.cleanup_run_root(run_root)

    assert attempts == [run_root]
    assert sleeps == []


def test_cleanup_delay_spy_does_not_replace_process_global_sleep(monkeypatch):
    sleeps = []
    process_sleep = time.sleep

    monkeypatch.setattr(run, "_cleanup_sleep", sleeps.append)
    time.sleep(0)

    assert time.sleep is process_sleep
    assert sleeps == []


def test_cleanup_run_root_does_not_hide_permission_error_while_probing_root(
    monkeypatch, tmp_path,
):
    run_root = _task_run_root(tmp_path, monkeypatch)
    denied = PermissionError(13, "denied", run_root)

    class DeniedRoot:
        def stat(self):
            raise denied

    monkeypatch.setattr(run, "_verified_run_root", lambda _path: DeniedRoot())
    monkeypatch.setattr(
        run.shutil, "rmtree",
        lambda path, **_kwargs: (_ for _ in ()).throw(
            FileNotFoundError(2, "missing child", path)
        ),
    )

    with pytest.raises(PermissionError) as captured:
        run.cleanup_run_root(run_root)

    assert captured.value is denied


def test_cleanup_run_root_retries_transient_windows_lock(monkeypatch, tmp_path):
    run_root = _task_run_root(tmp_path, monkeypatch)
    attempts = []
    delays = []

    def remove(path, **_kwargs):
        attempts.append(path)
        if len(attempts) == 1:
            raise _windows_cleanup_error(32, path)

    monkeypatch.setattr(run.shutil, "rmtree", remove)
    monkeypatch.setattr(run, "_cleanup_sleep", delays.append)

    run.cleanup_run_root(run_root)

    assert attempts == [run_root, run_root]
    assert delays == [run._CLEANUP_INITIAL_DELAY_SECONDS]


def test_cleanup_run_root_retries_directory_not_empty(monkeypatch, tmp_path):
    run_root = _task_run_root(tmp_path, monkeypatch)
    run_root.mkdir()
    attempts = []
    delays = []

    def remove(path, **_kwargs):
        attempts.append(path)
        if len(attempts) == 1:
            raise _windows_cleanup_error(145, path)

    monkeypatch.setattr(run.shutil, "rmtree", remove)
    monkeypatch.setattr(run, "_cleanup_sleep", delays.append)

    run.cleanup_run_root(run_root)

    assert attempts == [run_root, run_root]
    assert delays == [run._CLEANUP_INITIAL_DELAY_SECONDS]


def test_cleanup_run_root_reports_persistent_lock_and_retains_path(monkeypatch, tmp_path):
    run_root = _task_run_root(tmp_path, monkeypatch)
    attempts = []
    monkeypatch.setattr(
        run.shutil, "rmtree",
        lambda path, **_kwargs: attempts.append(path) or (_ for _ in ()).throw(
            _windows_cleanup_error(32, path),
        ),
    )
    monkeypatch.setattr(run, "_cleanup_sleep", lambda _delay: None)

    with pytest.raises(RuntimeError) as captured:
        run.cleanup_run_root(run_root)

    assert "retained evidence at <local-path>" in str(captured.value)
    assert str(run_root) not in str(captured.value)
    assert attempts == [run_root] * run._CLEANUP_ATTEMPTS


def test_cleanup_run_root_rejects_acl_error_outside_run_root(monkeypatch, tmp_path):
    run_root = _task_run_root(tmp_path, monkeypatch)
    outside = tmp_path / "outside" / "blocked"
    error = _windows_cleanup_error(5, outside)
    attempts = []
    sleeps = []
    restored = []
    monkeypatch.setattr(
        run.shutil, "rmtree",
        lambda path, **_kwargs: attempts.append(path) or (_ for _ in ()).throw(error),
    )
    monkeypatch.setattr(run, "_cleanup_sleep", sleeps.append)
    monkeypatch.setattr(run, "restore_acl_inheritance", restored.append)

    with pytest.raises(RuntimeError, match="outside run root"):
        run.cleanup_run_root(run_root)

    assert attempts == [run_root]
    assert sleeps == []
    assert restored == []


def test_cleanup_run_root_restores_acl_for_nested_permission_error(monkeypatch, tmp_path):
    run_root = _task_run_root(tmp_path, monkeypatch)
    blocked = run_root / "primary" / ".git" / "objects" / "aa" / "object"
    error = _windows_cleanup_error(5, blocked)
    attempts = []
    sleeps = []
    restored = []

    def remove(path, **_kwargs):
        attempts.append(path)
        if len(attempts) == 1:
            raise error

    monkeypatch.setattr(run.shutil, "rmtree", remove)
    monkeypatch.setattr(run, "_cleanup_sleep", sleeps.append)
    monkeypatch.setattr(run, "restore_acl_inheritance", restored.append)

    run.cleanup_run_root(run_root)

    assert attempts == [run_root, run_root]
    assert sleeps == [run._CLEANUP_INITIAL_DELAY_SECONDS]
    assert restored == [blocked.resolve()]


def test_cleanup_run_root_removes_nested_git_repository(tmp_path, monkeypatch):
    run_root = _task_run_root(tmp_path, monkeypatch)
    repository = run_root / "primary"
    objects = repository / ".git" / "objects"
    (objects / "info").mkdir(parents=True)
    (repository / ".git" / "HEAD").write_text(
        "ref: refs/heads/main\n", encoding="utf-8",
    )
    for index in range(10):
        fanout = objects / f"{index:02x}"
        fanout.mkdir()
        (fanout / ("a" * 38)).write_bytes(f"object {index}".encode())
    (objects / "info" / "commit-graph").write_bytes(b"commit graph")

    run.cleanup_run_root(run_root)

    assert not run_root.exists()


@pytest.mark.parametrize(
    "kind",
    (
        "pytest-root", "artifact-root", "checkout", "bad-run-id",
        "symlink-root", "junction-root",
    ),
)
def test_cleanup_run_root_rejects_unverified_destructive_root_before_rmtree(
    monkeypatch, tmp_path, kind,
):
    pytest_root = tmp_path / "task-artifacts" / "pytest" / "runs"
    monkeypatch.setattr(run, "PYTEST_ROOT", pytest_root)
    invalid = {
        "pytest-root": pytest_root,
        "artifact-root": pytest_root.parents[2],
        "checkout": tmp_path / "checkout" / ("a" * 12),
        "bad-run-id": pytest_root / "run",
        "symlink-root": pytest_root / ("a" * 12),
        "junction-root": pytest_root / ("a" * 12),
    }[kind]
    attempts = []
    monkeypatch.setattr(
        run.Path, "is_symlink",
        lambda self: kind == "symlink-root" and self == invalid,
    )
    monkeypatch.setattr(
        run.Path, "is_junction",
        lambda self: kind == "junction-root" and self == invalid,
    )
    monkeypatch.setattr(run.shutil, "rmtree", lambda path, **_kwargs: attempts.append(path))

    with pytest.raises(RuntimeError, match="verified pytest run root"):
        run.cleanup_run_root(invalid)

    assert attempts == []


def test_cleanup_run_root_repairs_more_than_eight_acl_nodes_in_one_walk(
    monkeypatch, tmp_path,
):
    run_root = _task_run_root(tmp_path, monkeypatch)
    blocked = [
        run_root / "primary" / ".git" / "objects" / f"{index:02x}" / "object"
        for index in range(10)
    ]
    restored = []
    retried = []
    attempts = []

    def remove(path, *, onexc):
        attempts.append(path)
        for child in blocked:
            onexc(retried.append, str(child), _windows_cleanup_error(5, child))

    monkeypatch.setattr(run.shutil, "rmtree", remove)
    monkeypatch.setattr(run, "restore_acl_inheritance", restored.append)

    run.cleanup_run_root(run_root)

    assert attempts == [run_root.resolve()]
    assert restored == [path.resolve() for path in blocked]
    assert retried == [str(path) for path in blocked]


def test_run_redirects_static_tool_caches(monkeypatch, tmp_path):
    artifacts = tmp_path / "artifacts"
    captured = {}

    class Result:
        returncode = 0

    monkeypatch.setattr(run, "ARTIFACTS", artifacts)
    monkeypatch.setattr(
        run.subprocess, "run",
        lambda *args, **kwargs: captured.update(kwargs) or Result(),
    )
    run.run("test", ["-c", "pass"])
    assert captured["env"]["RUFF_CACHE_DIR"] == str(artifacts / "cache" / "ruff")
    assert captured["env"]["MYPY_CACHE_DIR"] == str(artifacts / "cache" / "mypy")


def test_fresh_wheel_install_uses_uv_without_project_pip(monkeypatch, tmp_path):
    packages = tmp_path / "packages"
    wheel = packages / "python" / "quantmaster-1.0.0-py3-none-any.whl"
    wheel.parent.mkdir(parents=True)
    wheel.touch()
    artifacts = tmp_path / "artifacts"
    python = tmp_path / ".venv" / "Scripts" / "python.exe"
    calls = []
    monkeypatch.setattr(run, "PACKAGE_ROOT", packages)
    monkeypatch.setattr(run, "ARTIFACTS", artifacts)
    monkeypatch.setattr(run, "PYTHON", python)
    monkeypatch.setattr(
        run, "run_external",
        lambda label, command, **kwargs: calls.append((label, command, kwargs)),
    )

    run.smoke_fresh_wheel()

    label, command, kwargs = calls[0]
    assert label == "fresh wheel install"
    assert command[:5] == ["uv", "pip", "install", "--python", str(python)]
    assert command[5:7] == ["--no-deps", "--target"]
    assert Path(command[7]).name == "site"
    assert Path(command[8]).name == wheel.name
    assert Path(command[7]).parent == Path(command[8]).parent == kwargs["cwd"]
    assert kwargs["env"]["UV_CACHE_DIR"] == str(artifacts / "uv-cache")


@pytest.mark.parametrize("windows", [True, False])
def test_default_package_lane_builds_onefile_on_every_platform(monkeypatch, tmp_path, windows):
    artifacts = tmp_path / "artifacts"
    artifact_name = "QuantMaster.exe" if windows else "QuantMaster"
    application = artifacts / "packages" / "desktop" / artifact_name
    application.parent.mkdir(parents=True)
    application.write_bytes(b"stale")
    external_calls = []
    project_calls = []
    monkeypatch.setattr(run, "ARTIFACTS", artifacts)
    monkeypatch.setattr(run, "PACKAGE_ROOT", artifacts / "packages")
    monkeypatch.setattr(run, "RUN_ROOT", artifacts / "pytest" / "run")
    monkeypatch.setattr(run, "PYTEST_CACHE", artifacts / "pytest" / "cache")
    monkeypatch.setattr(run, "IS_WINDOWS", windows)
    monkeypatch.setenv("QM_DESKTOP_LAYOUT", "onedir-measurement")
    monkeypatch.setattr(
        run, "parse_args",
        lambda: run.argparse.Namespace(
            fast=False, full=False, ui=False, package=True, rust=False, all=False,
            serial=False, refresh_durations=False, measure_onedir=False,
        ),
    )
    monkeypatch.setattr(run, "prepare_pytest_directory", lambda path: path)
    monkeypatch.setattr(run, "cleanup_run_root", lambda path: None)
    monkeypatch.setattr(run, "smoke_fresh_wheel", lambda: None)
    monkeypatch.setattr(
        run, "run",
        lambda label, command, **kwargs: project_calls.append((label, command, kwargs)),
    )
    def run_external(label, command, **kwargs):
        if label == "PyInstaller smoke":
            assert not application.exists()
            application.parent.mkdir(parents=True, exist_ok=True)
            application.write_bytes(b"fresh")
        external_calls.append((label, command, kwargs))

    monkeypatch.setattr(run, "run_external", run_external)

    assert run.main() == 0

    label, command, kwargs = next(call for call in external_calls if call[0] == "PyInstaller smoke")
    assert label == "PyInstaller smoke"
    assert command[:10] == [
        "uv", "run", "--no-project", "--python", str(run.PYTHON),
        "--with", "PyInstaller==6.19.0", "-m", "PyInstaller", "--noconfirm",
    ]
    assert kwargs["env"]["UV_CACHE_DIR"] == str(artifacts / "uv-cache")
    assert kwargs["env"]["QM_DESKTOP_LAYOUT"] == "onefile"
    assert all(label != "PyInstaller smoke" for label, _command, _kwargs in project_calls)
    _label, command, _kwargs = next(
        call for call in project_calls if call[0] == "desktop artifact policy"
    )
    analysis = str(artifacts / "build" / "pyinstaller/quantmaster/Analysis-00.toc")
    expected = [
        "scripts/release/check_desktop_artifact.py", str(application), "--analysis", analysis,
    ]
    assert command == expected
    if windows:
        assert next(
            call[1] for call in external_calls
            if call[0] == "EXE runtime identity smoke"
        ) == [
            str(run.PYTHON),
            "scripts/release/smoke_frozen_runtime.py",
            str(application),
        ]
        assert all(call[0] not in {"EXE help", "EXE doctor"} for call in external_calls)
    else:
        assert not any(call[0] == "EXE runtime identity smoke" for call in external_calls)
        assert next(call[1] for call in external_calls if call[0] == "EXE help") == [
            str(application), "--help",
        ]
        assert next(call[1] for call in external_calls if call[0] == "EXE doctor") == [
            str(application), "doctor", "--deep",
        ]


def test_explicit_windows_onedir_lane_only_measures_and_reports(monkeypatch, tmp_path):
    artifacts = tmp_path / "artifacts"
    application = artifacts / "packages" / "desktop" / "QuantMaster"
    (application / "_internal").mkdir(parents=True)
    (application / "QuantMaster.exe").touch()
    external_calls = []
    project_calls = []
    monkeypatch.setattr(run, "ARTIFACTS", artifacts)
    monkeypatch.setattr(run, "PACKAGE_ROOT", artifacts / "packages")
    monkeypatch.setattr(run, "RUN_ROOT", artifacts / "pytest" / "run")
    monkeypatch.setattr(run, "PYTEST_CACHE", artifacts / "pytest" / "cache")
    monkeypatch.setattr(run, "IS_WINDOWS", True)
    monkeypatch.setattr(
        run, "parse_args",
        lambda: run.argparse.Namespace(
            fast=False, full=False, ui=False, package=True, rust=False, all=False,
            serial=False, refresh_durations=False, measure_onedir=True,
        ),
    )
    monkeypatch.setattr(run, "prepare_pytest_directory", lambda path: path)
    monkeypatch.setattr(run, "cleanup_run_root", lambda path: None)
    monkeypatch.setattr(run, "smoke_fresh_wheel", lambda: None)
    monkeypatch.setattr(
        run, "run",
        lambda label, command, **kwargs: project_calls.append((label, command, kwargs)),
    )
    def run_external(label, command, **kwargs):
        if label == "PyInstaller smoke":
            assert not application.exists()
            (application / "_internal").mkdir(parents=True)
            (application / "QuantMaster.exe").touch()
        external_calls.append((label, command, kwargs))

    monkeypatch.setattr(run, "run_external", run_external)

    assert run.main() == 0

    _label, _command, build_kwargs = next(
        call for call in external_calls if call[0] == "PyInstaller smoke"
    )
    assert build_kwargs["env"]["QM_DESKTOP_LAYOUT"] == "onedir-measurement"
    _label, command, _kwargs = next(
        call for call in project_calls if call[0] == "desktop artifact measurement"
    )
    assert command == [
        "scripts/release/check_desktop_artifact.py",
        str(application),
        "--analysis",
        str(artifacts / "build" / "pyinstaller/quantmaster/Analysis-00.toc"),
        "--experimental-onedir",
        "--archive",
        str(application.with_suffix(".zip")),
        "--report",
        str(application.with_suffix(".sizes.json")),
    ]
    measurement_root = artifacts / "packages" / "desktop" / "measurement"
    assert next(
        call[1] for call in external_calls if call[0] == "onedir startup budgets"
    ) == [
        str(run.PYTHON),
        "scripts/release/smoke_frozen_runtime.py",
        str(application / "QuantMaster.exe"),
        "--onedir-smoke",
        "--evidence",
        str(measurement_root / "startup-budgets.json"),
        "--instance-root",
        str(measurement_root),
    ]
    assert all(call[0] != "EXE runtime identity smoke" for call in external_calls)


def test_onedir_measurement_requires_the_package_lane(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["run.py", "--measure-onedir"])

    with pytest.raises(SystemExit) as raised:
        run.parse_args()

    assert raised.value.code == 2


def test_default_windows_package_and_release_workflows_keep_onefile():
    root = Path(__file__).parents[1]
    ci = (root / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    release = (root / ".github/workflows/release.yml").read_text(encoding="utf-8")

    assert "windows-package:" in ci
    assert "scripts/release/smoke_frozen_runtime.py" in ci
    assert "scripts/release/smoke_frozen_runtime.py" in release
    assert "QuantMaster.exe" in ci
    assert "QuantMaster.exe" in release
    assert "./.artifacts/packages/desktop/QuantMaster --help" in ci
    assert '"$executable" --help' in release
    assert "QuantMaster-windows.exe" in release
    assert "QuantMaster-windows.zip" not in release
    windows_job = ci.split("\n  windows-package:", 1)[1].split(
        "\n  windows-onedir-measurement:", 1,
    )[0]
    assert "actions/upload-artifact@" in windows_job
    assert "name: windows-desktop-package" in windows_job
    assert ".artifacts/packages/desktop/QuantMaster.exe" in windows_job
    assert ".artifacts/packages/desktop/QuantMaster.zip" not in windows_job
    assert "retention-days: 7" in windows_job


def test_manual_onedir_measurement_is_opt_in_and_not_a_release_asset():
    root = Path(__file__).parents[1]
    ci = (root / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    release = (root / ".github/workflows/release.yml").read_text(encoding="utf-8")

    assert "measure_onedir:" in ci
    measurement = ci.split("\n  windows-onedir-measurement:", 1)[1]
    assert "github.event_name == 'workflow_dispatch' && inputs.measure_onedir" in measurement
    assert "QM_DESKTOP_LAYOUT: onedir-measurement" in measurement
    assert "--experimental-onedir" in measurement
    assert "scripts/release/smoke_frozen_runtime.py" in measurement
    assert "--help-layout onedir" in measurement
    assert "windows-onedir-measurement" not in release


def test_windows_frozen_readiness_decision_is_documented():
    root = Path(__file__).parents[1]
    decision = (
        root / "docs/decisions/0002-windows-frozen-readiness-and-help-budgets.md"
    ).read_text(encoding="utf-8")

    assert "Discussion #95" in decision
    assert "onefile" in decision and "20 秒" in decision
    assert "onedir" in decision and "1.5 秒" in decision
    assert "PYINSTALLER_SUPPRESS_SPLASH_SCREEN=1" in decision
    assert "不显示百分比" in decision
    assert "监听" in decision and "`core_ready` 后" in decision
    assert "/api/v1/health" in decision
    assert "/api/v1/diagnostics" in decision


def test_release_keeps_three_onefiles_and_closes_all_downloaded_checksums():
    release = (Path(__file__).parents[1] / ".github/workflows/release.yml").read_text(
        encoding="utf-8",
    )

    assert "asset: QuantMaster-macos\n" in release
    assert "asset: QuantMaster-linux\n" in release
    assert "asset: QuantMaster-windows.exe\n" in release
    assert "report: QuantMaster-" not in release
    assert "merge-multiple: true" in release
    assert (
        "sha256sum QuantMaster-linux QuantMaster-macos QuantMaster-windows.exe\n"
        "          QuantMaster.cdx.json > SHA256SUMS"
    ) in release


def test_full_shards_use_three_way_parallelism(monkeypatch):
    observed = {}

    class Future:
        def result(self):
            return None

    class Pool:
        def __init__(self, max_workers):
            observed["max_workers"] = max_workers

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def submit(self, function, shard):
            return Future()

    monkeypatch.setattr(run, "ThreadPoolExecutor", Pool)
    monkeypatch.setattr(run, "as_completed", lambda futures: futures)
    monkeypatch.setattr(run, "pytest_shard", lambda shard: None)

    run.run_shards()
    assert observed["max_workers"] == 3


def test_ci_script_can_import_project_modules_when_executed_directly():
    result = subprocess.run(
        [sys.executable, "scripts/ci/run.py", "--help"],
        cwd=run.ROOT, capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr
