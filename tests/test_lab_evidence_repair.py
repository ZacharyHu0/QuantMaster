from __future__ import annotations

import os
import subprocess
import tempfile
import zipfile
from pathlib import Path

import pytest

from quantmaster.runtime import lab_evidence_repair as repair
from quantmaster.runtime.identity import get_application_identity
from quantmaster.runtime.storage_governance import (
    create_inheriting_temporary_directory,
    prepare_writable_directory,
)
from quantmaster.runtime.worker_ipc import RuntimeCommandServer

pytestmark = pytest.mark.skipif(os.name != "nt", reason="native Windows DACL maintenance")


@pytest.fixture
def evidence(tmp_path):
    # Reuse the existing native future-cleanup-SID probe, without a new account.
    from test_live_staging import _cleanup_identity_acl

    root = tmp_path / "evidence-data"
    root.mkdir()
    _cleanup_identity_acl(root, grant=True)
    target = root / "lab_evidence"
    target.mkdir(mode=0o700)
    snapshot = target / "old-snapshot"
    snapshot.mkdir()
    (snapshot / "manifest.json").write_bytes(b'{"evidence":"preserved"}')
    (snapshot / "matrix.parquet").write_bytes(bytes(range(256)) * 128)
    identity = get_application_identity()
    frozen = {"valid": True}

    def status(operation, payload):
        assert operation == "maintenance.status"
        assert payload == {"token": "test-lease"}
        return {
            **frozen, "state": "frozen", "worker_id": "fixture-worker", "pid": os.getpid(),
            "participants": ["fixture-lab-writer"],
        }

    server = RuntimeCommandServer(status, root=root)
    server.start()
    options = {
        "receipt": tmp_path / "repair-receipt.jsonl", "confirmation": str(target),
        "identity": identity, "token": "test-lease",
        "expected_inventory": repair._inventory_digest(repair._inventory(target)),
    }
    try:
        yield root, target, options, frozen
    finally:
        server.stop()


def test_native_private_root_repair_preserves_evidence_and_restores_grants(evidence):
    from test_live_staging import _cleanup_identity_acl

    root, target, options, _frozen = evidence
    original = repair._inventory(target)
    paths = [root, *[target / item["name"] for item in original]]
    original_acls = repair._acls(paths)
    assert original_acls[1]["protected"]
    with pytest.raises(PermissionError, match="ACL"):
        create_inheriting_temporary_directory(target)
    preview = repair.repair(root, action="preview", **options)
    assert preview["files"] == 2 and preview["needs_repair"]
    assert not options["receipt"].exists()
    result = repair.repair(root, action="apply", **options)
    assert result["status"] == "apply_complete"
    assert repair._inventory(target) == original
    _cleanup_identity_acl(target, grant=False)
    assert repair._acls([root]) == original_acls[:1]
    assert repair.repair(root, action="apply", **options)["status"] == "already_inheriting"
    restored = repair.repair(root, action="restore", **options)
    assert restored["status"] == "restore_complete"
    assert repair._inventory(target) == original
    assert repair._same_grants(repair._acls(paths), original_acls)


def test_new_extract_and_same_volume_publish_keep_cleanup_grants(tmp_path):
    from test_live_staging import _cleanup_identity_acl

    root = tmp_path / "evidence-data"
    root.mkdir()
    _cleanup_identity_acl(root, grant=True)
    evidence = root / "lab_evidence"
    staged = create_inheriting_temporary_directory(evidence, prefix=".dataset-")
    archive = tmp_path / "evidence.zip"
    with zipfile.ZipFile(archive, "w") as output:
        output.writestr("nested/matrix.parquet", b"unchanged")
    with zipfile.ZipFile(archive) as source:
        source.extractall(staged)
    staged.replace(evidence / "published")
    _cleanup_identity_acl(evidence, grant=False)
    # Demonstrate why promoting a private tempfile root is not the normal path.
    with tempfile.TemporaryDirectory(dir=root) as temporary:
        with zipfile.ZipFile(archive) as source:
            source.extractall(temporary)
        Path(temporary, "nested").replace(root / "private-published")
    with pytest.raises(AssertionError, match="cleanup identity grant was lost"):
        _cleanup_identity_acl(root / "private-published", grant=False)


def test_requires_live_frozen_worker_and_rejects_real_writer(evidence):
    root, target, options, frozen = evidence
    frozen["valid"] = False
    with pytest.raises(ValueError, match="MAINTENANCE_UNCONFIRMED"):
        repair.repair(root, action="apply", **options)
    frozen["valid"] = True
    with (target / "old-snapshot" / "matrix.parquet").open("ab"):
        with pytest.raises(PermissionError, match="WRITER_OR_ACCESS_BLOCKED"):
            repair.repair(root, action="apply", **options)
    assert not options["receipt"].exists()


def test_refuses_changed_evidence_on_restore(evidence):
    root, target, options, _frozen = evidence
    repair.repair(root, action="apply", **options)
    (target / "old-snapshot" / "manifest.json").write_bytes(b"changed")
    with pytest.raises(ValueError, match="RESTORE_IDENTITY_CHANGED"):
        repair.repair(root, action="restore", **options)
    assert repair._acls([target])[0]["protected"] is False


def test_receipt_precedes_write_and_supports_recovery_after_interruption(evidence, monkeypatch):
    root, target, options, _frozen = evidence
    original = repair._acls

    def interrupted(paths, **kwargs):
        result = original(paths, **kwargs)
        if kwargs.get("action") == "apply":
            assert options["receipt"].read_text(encoding="utf-8")
            raise OSError("simulated interruption after native ACL write")
        return result

    monkeypatch.setattr(repair, "_acls", interrupted)
    with pytest.raises(OSError, match="simulated interruption"):
        repair.repair(root, action="apply", **options)
    monkeypatch.setattr(repair, "_acls", original)
    assert repair.repair(root, action="restore", **options)["status"] == "restore_complete"
    assert original([target])[0]["protected"]


def test_refuses_protected_descendant_and_unknown_private_acl(evidence):
    root, target, options, _frozen = evidence
    private = target / "private-child"
    private.mkdir(mode=0o700)
    with pytest.raises(ValueError, match="PROTECTED_DESCENDANT"):
        repair.repair(root, action="apply", **options)
    private.rmdir()
    # Unknown private rules must not be treated as the recognized legacy shape.
    acls = repair._acls([root, target])
    acls[1]["rules"][0]["allow"] = False
    with pytest.raises(ValueError, match="UNKNOWN_PRIVATE_DACL"):
        repair._validate_acls(acls)


def test_reparse_escape_and_wrong_confirmation_are_rejected(evidence, tmp_path):
    root, target, options, _frozen = evidence
    outside = tmp_path / "outside"
    outside.mkdir()
    link = target / "junction"
    result = subprocess.run(
        ["cmd.exe", "/c", "mklink", "/J", str(link), str(outside)], capture_output=True, check=False,
    )
    assert result.returncode == 0
    try:
        with pytest.raises(ValueError, match="REPARSE_REJECTED"):
            repair.repair(root, action="apply", **options)
    finally:
        link.rmdir()
    with pytest.raises(ValueError, match="EXACT_CONFIRMATION_REQUIRED"):
        repair.repair(root, action="apply", **{**options, "confirmation": str(root)})
    assert not options["receipt"].exists()
    assert prepare_writable_directory(root).inherited


def test_hardlink_escape_is_rejected(evidence, tmp_path):
    root, target, options, _frozen = evidence
    outside = tmp_path / "outside.parquet"
    outside.write_bytes(b"must not change ACL through another link")
    (target / "linked.parquet").hardlink_to(outside)
    with pytest.raises(ValueError, match="HARDLINK_REJECTED"):
        repair.repair(root, action="apply", **options)
    assert not options["receipt"].exists()


def test_apply_rejects_changes_since_approved_preview(evidence):
    root, target, options, _frozen = evidence
    (target / "old-snapshot" / "matrix.parquet").write_bytes(b"changed since preview")
    with pytest.raises(ValueError, match="PREVIEW_CHANGED"):
        repair.repair(root, action="apply", **options)
    assert not options["receipt"].exists()
    assert repair._acls([target])[0]["protected"]


def test_share_handles_block_new_writes_and_rename(evidence):
    _root, target, _options, _frozen = evidence
    matrix = target / "old-snapshot" / "matrix.parquet"
    with repair._deny_writers([target, matrix]):
        with pytest.raises(PermissionError):
            matrix.open("ab")
        with pytest.raises(PermissionError):
            target.rename(target.with_name("escaped"))


def test_receipt_cannot_escape_into_evidence_using_parent_segments(evidence):
    root, target, options, _frozen = evidence
    # An apparently external receipt must not resolve inside immutable evidence.
    ambiguous = root / ".." / root.name / "lab_evidence" / "receipt.jsonl"
    with pytest.raises(ValueError, match="RECEIPT_INVALID"):
        repair.repair(root, action="apply", **{**options, "receipt": ambiguous})
    assert not (target / "receipt.jsonl").exists()
