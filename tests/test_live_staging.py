import json
import os
from pathlib import Path
from zipfile import ZIP_STORED, ZipFile

import pytest

from scripts.dev import live


def _archive(path: Path, *members: tuple[str, bytes]) -> Path:
    with ZipFile(path, "w", compression=ZIP_STORED) as output:
        for name, content in members:
            output.writestr(name, content)
    return path


def _small_report() -> dict[str, object]:
    return {
        "mode": "onedir-measurement",
        "build_sha": "a" * 40,
        "onedir_bytes": 8,
        "zip_bytes": 8,
        "within_zip_target": True,
        "within_hard_limits": True,
        "limit_failures": [],
        "module_attribution": [],
        "errors": [],
    }


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
    active.write_bytes(b'{"active":"old"}\n')
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

    def build(_project, _sha, _artifacts, build_root):
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
    assert active.read_bytes() == b'{"active":"old"}\n'


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
        "source_task": "task",
        "build_sha": "a" * 40,
        "slot_id": "a" * 40,
        "smoke": {
            "layout": "onedir",
            "build_sha": "a" * 40,
            "slot_id": "a" * 40,
            "processes_stopped": True,
            "port_released": True,
        },
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

    result = live.stage("task", cwd=primary)

    assert result["status"] == "staged"
    assert result["idempotent"] is True
