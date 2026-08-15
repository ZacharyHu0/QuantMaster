import json
import os
import subprocess
import uuid
from pathlib import Path
from zipfile import ZIP_STORED, ZipFile

import pytest

from scripts.dev import live


def _archive(path: Path, *members: tuple[str, bytes]) -> Path:
    with ZipFile(path, "w", compression=ZIP_STORED) as output:
        for name, content in members:
            output.writestr(name, content)
    return path


def _small_report(build_sha: str = "a" * 40) -> dict[str, object]:
    return {
        "mode": "onedir-measurement",
        "build_sha": build_sha,
        "onedir_bytes": 8,
        "zip_bytes": 8,
        "within_zip_target": True,
        "within_hard_limits": True,
        "limit_failures": [],
        "module_attribution": [],
        "errors": [],
    }


def _smoke_report(build_sha: str = "a" * 40) -> dict[str, object]:
    return {
        "layout": "onedir",
        "build_sha": build_sha,
        "slot_id": build_sha,
        "runtime_generation": "b" * 32,
        "help_seconds": 0.2,
        "help_budget_seconds": 1.5,
        "core_ready_seconds": 0.4,
        "processes_stopped": True,
        "port_released": True,
        "executable_unchanged": True,
    }


def _git(cwd: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-c", f"safe.directory={cwd.as_posix()}", *args],
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if completed.returncode:
        raise AssertionError(f"git {' '.join(args)} failed: {completed.stderr.strip()}")
    return completed.stdout.strip()


def _verified_repository(tmp_path: Path, monkeypatch) -> tuple[Path, Path, str]:
    source = Path(__file__).resolve().parents[1]
    sha = _git(source, "rev-parse", "HEAD~1")
    task_sha = _git(source, "rev-parse", "HEAD")
    common_dir = Path(_git(source, "rev-parse", "--git-common-dir"))
    if not common_dir.is_absolute():
        common_dir = source / common_dir

    primary = tmp_path.parent / f"repo-{uuid.uuid4().hex[:8]}"
    for path in (
        primary,
        primary / ".git",
        primary / ".git" / "objects",
        primary / ".git" / "objects" / "info",
        primary / ".git" / "refs" / "heads",
        primary / ".git" / "refs" / "remotes" / "origin",
        primary / ".git" / "refs" / "test",
        primary / ".git" / "worktrees",
    ):
        live.tasks.prepare_pytest_directory(path)
    _git(primary, "init", "-b", "main")
    (primary / ".git" / "objects" / "info" / "alternates").write_bytes(
        f"{(common_dir / 'objects').resolve()}\n".encode(),
    )
    _git(primary, "update-ref", "refs/heads/main", sha)
    _git(primary, "reset", "--hard", sha)
    _git(primary, "update-ref", "refs/remotes/origin/main", sha)
    _git(primary, "update-ref", "refs/test/task", task_sha)
    live.tasks.prepare_pytest_directory(primary / ".worktrees")
    target = primary / ".worktrees" / "task"
    _git(primary, "worktree", "add", "-b", "codex/task", str(target), sha)
    evidence = {
        "commit": sha,
        "base": sha,
        "python": "python",
        "python_size": 1,
        "python_mtime_ns": 1,
        "environment": "environment",
        "ui": False,
        "rust": False,
        "package": True,
    }
    evidence_path = primary / ".artifacts" / "worktrees" / "task" / "validation" / "full.json"
    evidence_path.parent.mkdir(parents=True)
    evidence_path.write_text(json.dumps(evidence), encoding="utf-8")
    monkeypatch.setattr(live.tasks, "full_validation_identity", lambda *_args, **_kwargs: evidence)
    monkeypatch.setattr(live.tasks, "project_python", lambda *_args, **_kwargs: Path("python"))
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "localappdata"))
    return primary, target, sha


def _task_sha(primary: Path) -> str:
    return _git(primary, "rev-parse", "refs/test/task")


def test_stage_builds_from_fixed_git_snapshot_when_primary_changes(
    tmp_path: Path, monkeypatch,
) -> None:
    if os.name != "nt":
        pytest.skip("slot staging is Windows-only")
    primary, _target, sha = _verified_repository(tmp_path, monkeypatch)
    verified_readme = (primary / "README.md").read_bytes()

    def build(project_root, *_args):
        assert project_root != primary
        assert _git(project_root, "rev-parse", "HEAD") == sha
        assert (project_root / "README.md").read_bytes() == verified_readme
        (primary / "README.md").write_text("changed after validation\n", encoding="utf-8")
        build_root = Path(_args[-1])
        archive = _archive(
            build_root / "QuantMaster.zip", ("QuantMaster/QuantMaster.exe", b"exe"),
        )
        return archive, _small_report(sha)

    smoke_calls = []
    monkeypatch.setattr(live, "_build_onedir", build)
    monkeypatch.setattr(
        live.smoke_frozen_runtime,
        "smoke",
        lambda executable, *, layout: smoke_calls.append((executable, layout))
        or _smoke_report(sha),
    )
    result = live.stage("task", cwd=primary)

    assert result["build_sha"] == sha
    assert smoke_calls == [(Path(result["slot"]) / "QuantMaster.exe", "onedir")]


@pytest.mark.parametrize("protected_name", ["active", "previous"])
def test_stage_refuses_to_write_an_active_or_previous_slot(
    tmp_path: Path, monkeypatch, protected_name: str,
) -> None:
    if os.name != "nt":
        pytest.skip("slot staging is Windows-only")
    primary, _target, sha = _verified_repository(tmp_path, monkeypatch)
    active = tmp_path / "localappdata" / "QuantMaster" / "app" / "active.json"
    active.parent.mkdir(parents=True)
    state = {
        "schema": 1,
        "active": sha if protected_name == "active" else "",
        "previous": sha if protected_name == "previous" else "",
        "pending": "",
        "status": "stable",
        "last_error": "",
    }
    expected = (json.dumps(state) + "\n").encode()
    active.write_bytes(expected)
    monkeypatch.setattr(live, "_build_onedir", lambda *_args: pytest.fail("must not build"))

    with pytest.raises(live.StageBlocked) as failure:
        live.stage("task", cwd=primary)

    assert failure.value.reason == "protected_slot"
    assert active.read_bytes() == expected


@pytest.mark.parametrize(
    ("case", "reason"),
    [
        ("dirty-main", "main_dirty"),
        ("unsynchronized-main", "main_unsynchronized"),
        ("missing-package-gate", "package_evidence_required"),
        ("environment-mismatch", "stale_validation_evidence"),
        ("stale-task-evidence", "stale_validation_evidence"),
        ("different-task-tree", "task_tree_mismatch"),
    ],
)
def test_stage_rejects_untrusted_main_or_task_evidence(
    tmp_path: Path, monkeypatch, case: str, reason: str,
) -> None:
    if os.name != "nt":
        pytest.skip("slot staging is Windows-only")
    primary, target, sha = _verified_repository(tmp_path, monkeypatch)
    evidence_path = primary / ".artifacts" / "worktrees" / "task" / "validation" / "full.json"
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    if case == "dirty-main":
        (primary / "README.md").write_text("dirty\n", encoding="utf-8")
    elif case == "unsynchronized-main":
        _git(primary, "update-ref", "refs/remotes/origin/main", _task_sha(primary))
    elif case == "missing-package-gate":
        evidence["package"] = False
        evidence_path.write_text(json.dumps(evidence), encoding="utf-8")
    elif case == "environment-mismatch":
        monkeypatch.setattr(
            live.tasks,
            "full_validation_identity",
            lambda *_args, **_kwargs: {**evidence, "environment": "changed"},
        )
    else:
        task_sha = _task_sha(primary)
        _git(target, "reset", "--hard", task_sha)
        if case == "different-task-tree":
            evidence["commit"] = task_sha
            evidence["base"] = sha
            evidence_path.write_text(json.dumps(evidence), encoding="utf-8")
            monkeypatch.setattr(
                live.tasks, "full_validation_identity", lambda *_args, **_kwargs: evidence,
            )
    monkeypatch.setattr(live, "_build_onedir", lambda *_args: pytest.fail("must not build"))

    with pytest.raises(live.StageBlocked) as failure:
        live.stage("task", cwd=primary)

    assert failure.value.reason == reason


def test_stage_keeps_a_complete_slot_if_active_state_changes_after_marking(
    tmp_path: Path, monkeypatch,
) -> None:
    if os.name != "nt":
        pytest.skip("slot staging is Windows-only")
    primary, _target, sha = _verified_repository(tmp_path, monkeypatch)
    active = tmp_path / "localappdata" / "QuantMaster" / "app" / "active.json"
    active.parent.mkdir(parents=True)
    active.write_text(
        json.dumps({
            "schema": live.ACTIVE_STATE_SCHEMA,
            "active": "c" * 40,
            "previous": "",
            "pending": "",
        }),
        encoding="utf-8",
    )

    def build(_project, *_args):
        build_root = Path(_args[-1])
        return _archive(
            build_root / "QuantMaster.zip", ("QuantMaster/QuantMaster.exe", b"exe"),
        ), _small_report(sha)

    write_marker = live._write_marker

    def mark_then_change_active(slot, payload):
        write_marker(slot, payload)
        active.write_text(
            json.dumps({
                "schema": live.ACTIVE_STATE_SCHEMA,
                "active": "d" * 40,
                "previous": "",
                "pending": "",
            }),
            encoding="utf-8",
        )

    monkeypatch.setattr(live, "_build_onedir", build)
    monkeypatch.setattr(live, "_write_marker", mark_then_change_active)
    monkeypatch.setattr(
        live.smoke_frozen_runtime, "smoke", lambda *_args, **_kwargs: _smoke_report(sha),
    )

    with pytest.raises(live.StageBlocked) as failure:
        live.stage("task", cwd=primary)

    slot = active.parent / "slots" / sha
    assert failure.value.reason == "active_state_changed"
    assert (slot / live.STAGE_MARKER).is_file()
    assert (slot / "QuantMaster.exe").read_bytes() == b"exe"


def test_stage_fails_closed_while_the_application_lifecycle_lock_is_held(
    tmp_path: Path, monkeypatch,
) -> None:
    if os.name != "nt":
        pytest.skip("slot staging is Windows-only")
    primary, _target, _sha = _verified_repository(tmp_path, monkeypatch)
    app_root = tmp_path / "localappdata" / "QuantMaster" / "app"
    app_root.mkdir(parents=True)
    marker = app_root / live.LIFECYCLE_LOCK
    with marker.open("a+b") as stream:
        stream.write(b"0")
        stream.flush()
        assert live.tasks._try_lock(stream)
        try:
            with pytest.raises(live.StageBlocked) as failure:
                live.stage("task", cwd=primary)
        finally:
            live.tasks._unlock(stream)

    assert failure.value.reason == "lifecycle_busy"


def test_stage_rejects_non_string_active_state_sha(
    tmp_path: Path, monkeypatch,
) -> None:
    if os.name != "nt":
        pytest.skip("slot staging is Windows-only")
    primary, _target, _sha = _verified_repository(tmp_path, monkeypatch)
    active = tmp_path / "localappdata" / "QuantMaster" / "app" / "active.json"
    active.parent.mkdir(parents=True)
    active.write_text(
        json.dumps({
            "schema": live.ACTIVE_STATE_SCHEMA,
            "active": int("1" * 40),
            "previous": "",
            "pending": "",
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(live, "_build_onedir", lambda *_args: pytest.fail("must not build"))

    with pytest.raises(live.StageBlocked) as failure:
        live.stage("task", cwd=primary)

    assert failure.value.reason == "active_state_invalid"


@pytest.mark.parametrize("member_kind", ("drive", "ads"))
def test_stage_rejects_windows_archive_aliases_before_writing_outside(
    tmp_path: Path, monkeypatch, member_kind: str,
) -> None:
    if os.name != "nt":
        pytest.skip("slot staging is Windows-only")
    primary, _target, sha = _verified_repository(tmp_path, monkeypatch)
    outside = tmp_path / "outside.txt"
    malicious = (
        f"QuantMaster/{outside.as_posix()}"
        if member_kind == "drive"
        else "QuantMaster/readme.txt:outside"
    )

    def build(_project, *_args):
        build_root = Path(_args[-1])
        return _archive(
            build_root / "QuantMaster.zip",
            ("QuantMaster/QuantMaster.exe", b"exe"),
            (malicious, b"escape"),
        ), _small_report(sha)

    monkeypatch.setattr(live, "_build_onedir", build)
    monkeypatch.setattr(
        live.smoke_frozen_runtime,
        "smoke",
        lambda *_args, **_kwargs: _smoke_report(sha),
    )

    with pytest.raises(live.StageBlocked) as failure:
        live.stage("task", cwd=primary)

    assert failure.value.reason == "unsafe_archive"
    assert not outside.exists()
    assert not (tmp_path / "localappdata" / "QuantMaster" / "app" / "slots" / sha).exists()


def test_extract_rejects_traversal_without_writing_outside(tmp_path: Path) -> None:
    archive = _archive(tmp_path / "bad.zip", ("QuantMaster/../outside.txt", b"no"))
    extraction = tmp_path / "extract"
    extraction.mkdir()

    with pytest.raises(live.StageBlocked, match="不安全") as failure:
        live._extract_archive(archive, extraction)

    assert failure.value.reason == "unsafe_archive"
    assert not (tmp_path / "outside.txt").exists()
    assert not (extraction / "QuantMaster").exists()


def test_extract_requires_a_regular_root_launcher(tmp_path: Path) -> None:
    archive = _archive(tmp_path / "missing.exe.zip", ("QuantMaster/readme.txt", b"no"))

    with pytest.raises(live.StageBlocked) as failure:
        live._extract_archive(archive, tmp_path / "extract")

    assert failure.value.reason == "invalid_slot"


def test_existing_partial_slot_is_not_repaired_implicitly(tmp_path: Path) -> None:
    slot = tmp_path / "slots" / ("a" * 40)
    slot.mkdir(parents=True)
    (slot / "QuantMaster.exe").write_bytes(b"partial")

    with pytest.raises(live.StageBlocked) as failure:
        live._read_marker(slot, "a" * 40)

    assert failure.value.reason == "partial_slot"
    assert (slot / "QuantMaster.exe").read_bytes() == b"partial"


def test_existing_marker_without_complete_package_evidence_fails_closed(
    tmp_path: Path, monkeypatch,
) -> None:
    if os.name != "nt":
        pytest.skip("slot staging is Windows-only")
    primary = tmp_path / "primary"
    primary.mkdir()
    (primary / ".artifacts" / "worktrees" / "task").mkdir(parents=True)
    sha = "a" * 40
    local_appdata = tmp_path / "localappdata"
    slot = local_appdata / "QuantMaster" / "app" / "slots" / sha
    slot.mkdir(parents=True)
    (slot / "QuantMaster.exe").write_bytes(b"unverified")
    (slot / live.STAGE_MARKER).write_text(
        json.dumps({
            "schema": live.STAGE_SCHEMA,
            "status": "staged",
            "build_sha": sha,
            "slot_id": sha,
            "smoke": _smoke_report(),
        }),
        encoding="utf-8",
    )
    monkeypatch.setenv("LOCALAPPDATA", str(local_appdata))
    monkeypatch.setattr(live, "_validate_primary", lambda _cwd: (primary, sha))
    monkeypatch.setattr(live, "_validate_task", lambda *_args: (primary, {"commit": sha}))
    monkeypatch.setattr(
        live.smoke_frozen_runtime, "smoke", lambda *_args, **_kwargs: pytest.fail("must not smoke"),
    )

    with pytest.raises(live.StageBlocked) as failure:
        live.stage("task", cwd=primary)

    assert failure.value.reason == "partial_slot"
    assert (slot / "QuantMaster.exe").read_bytes() == b"unverified"


def test_stage_smoke_failure_removes_only_the_new_candidate(tmp_path: Path, monkeypatch) -> None:
    if os.name != "nt":
        pytest.skip("slot staging is Windows-only")
    primary = tmp_path / "primary"
    primary.mkdir()
    artifacts = primary / ".artifacts" / "worktrees" / "task"
    artifacts.mkdir(parents=True)
    local_appdata = tmp_path / "localappdata"
    active = local_appdata / "QuantMaster" / "app" / "active.json"
    active.parent.mkdir(parents=True)
    active.write_bytes(
        (json.dumps({
            "schema": live.ACTIVE_STATE_SCHEMA,
            "active": "c" * 40,
            "previous": "",
            "pending": "",
        }) + "\n").encode(),
    )
    sha = "a" * 40
    evidence = {
        "commit": sha,
        "base": "b" * 40,
        "python": "python",
        "python_size": 1,
        "python_mtime_ns": 1,
        "environment": "environment",
        "ui": False,
        "rust": False,
        "package": True,
    }

    monkeypatch.setenv("LOCALAPPDATA", str(local_appdata))
    monkeypatch.setattr(live, "_validate_primary", lambda _cwd: (primary, sha))
    monkeypatch.setattr(live, "_validate_task", lambda *_args: (primary, evidence))
    monkeypatch.setattr(live, "_snapshot_main", lambda *_args: primary)
    monkeypatch.setattr(live.tasks, "project_python", lambda *_args: Path("python"))

    def build(_project, *_args):
        build_root = Path(_args[-1])
        archive = _archive(Path(build_root) / "QuantMaster.zip", ("QuantMaster/QuantMaster.exe", b"exe"))
        return archive, _small_report()

    monkeypatch.setattr(live, "_build_onedir", build)
    monkeypatch.setattr(
        live.smoke_frozen_runtime,
        "smoke",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("worker missing")),
    )

    with pytest.raises(live.StageBlocked) as failure:
        live.stage("task", cwd=primary)

    slot = local_appdata / "QuantMaster" / "app" / "slots" / sha
    assert failure.value.reason == "packaged_smoke_failed"
    assert not slot.exists()
    assert json.loads(active.read_bytes())["active"] == "c" * 40


def test_same_complete_slot_is_idempotent_without_rebuilding(tmp_path: Path, monkeypatch) -> None:
    if os.name != "nt":
        pytest.skip("slot staging is Windows-only")
    primary = tmp_path / "primary"
    primary.mkdir()
    artifacts = primary / ".artifacts" / "worktrees" / "task"
    artifacts.mkdir(parents=True)
    local_appdata = tmp_path / "localappdata"
    slot = local_appdata / "QuantMaster" / "app" / "slots" / ("a" * 40)
    slot.mkdir(parents=True)
    (slot / "QuantMaster.exe").write_bytes(b"exe")
    payload = {
        "schema": live.STAGE_SCHEMA,
        "status": "staged",
        "idempotent": False,
        "source_task": "task",
        "source_task_commit": "a" * 40,
        "build_sha": "a" * 40,
        "slot_id": "a" * 40,
        "slot": str(slot),
        "size": _small_report(),
        "smoke": _smoke_report(),
        "staged_at": "2026-08-16T00:00:00+00:00",
    }
    (slot / live.STAGE_MARKER).write_text(json.dumps(payload), encoding="utf-8")
    evidence = {
        "commit": "a" * 40,
        "base": "b" * 40,
        "python": "python",
        "python_size": 1,
        "python_mtime_ns": 1,
        "environment": "environment",
        "ui": False,
        "rust": False,
        "package": True,
    }
    monkeypatch.setenv("LOCALAPPDATA", str(local_appdata))
    monkeypatch.setattr(live, "_validate_primary", lambda _cwd: (primary, "a" * 40))
    monkeypatch.setattr(live, "_validate_task", lambda *_args: (primary, evidence))
    monkeypatch.setattr(live, "_build_onedir", lambda *_args: pytest.fail("must not rebuild"))
    smoke_calls = []
    monkeypatch.setattr(
        live.smoke_frozen_runtime,
        "smoke",
        lambda executable, *, layout: smoke_calls.append((executable, layout))
        or _smoke_report(),
    )

    result = live.stage("task", cwd=primary)

    assert result["status"] == "staged"
    assert result["idempotent"] is True
    assert smoke_calls == [(slot / "QuantMaster.exe", "onedir")]
