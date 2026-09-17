"""Preview, repair and restore only the legacy Windows lab_evidence root DACL.

No evidence is copied or replaced. A durable private receipt precedes the sole
ACL write; the operator owns the existing worker maintenance lease throughout.
"""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
import stat
import subprocess
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from quantmaster.runtime.identity import ApplicationIdentity
from quantmaster.runtime.storage_governance import StorageBoundaryError
from quantmaster.runtime.worker_ipc import call_worker_command


def _target(data_root: Path) -> Path:
    if not data_root.is_absolute() or os.name != "nt":
        raise StorageBoundaryError("LAB_ACL_TARGET_INVALID")
    target = data_root / "lab_evidence"
    for path in (target, *target.parents):
        if path.lstat().st_file_attributes & stat.FILE_ATTRIBUTE_REPARSE_POINT:
            raise StorageBoundaryError("LAB_ACL_REPARSE_REJECTED")
    if data_root.name.lower() in {".artifacts", ".worktrees"} or not target.is_dir():
        raise StorageBoundaryError("LAB_ACL_TARGET_INVALID")
    return target.resolve()


def _inventory(target: Path) -> list[dict[str, Any]]:
    entries = []
    pending = [target]
    while pending:
        path = pending.pop(0)
        info = path.lstat()
        if info.st_file_attributes & stat.FILE_ATTRIBUTE_REPARSE_POINT:
            raise StorageBoundaryError("LAB_ACL_REPARSE_REJECTED")
        if not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)):
            raise StorageBoundaryError("LAB_ACL_OBJECT_REJECTED")
        if stat.S_ISREG(info.st_mode) and info.st_nlink != 1:
            raise StorageBoundaryError("LAB_ACL_HARDLINK_REJECTED")
        if path.is_dir():
            pending.extend(sorted(path.iterdir()))
        digest = ""
        if path.is_file():
            with path.open("rb") as stream:
                digest = hashlib.file_digest(stream, "sha256").hexdigest()
        entries.append({
            "name": path.relative_to(target).as_posix(),
            "identity": [info.st_dev, info.st_ino],
            "directory": path.is_dir(),
            "bytes": info.st_size if path.is_file() else 0,
            "modified_ns": info.st_mtime_ns,
            "sha256": digest,
        })
    return entries


@contextmanager
def _deny_writers(paths: list[Path]):
    """Real Windows share-mode barrier: reject writers and prevent rename/delete."""
    from ctypes import wintypes

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.CreateFileW.argtypes = [
        wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, ctypes.c_void_p,
        wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE,
    ]
    kernel.CreateFileW.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    handles = []
    try:
        for path in paths:
            # GENERIC_READ, FILE_SHARE_READ, OPEN_EXISTING,
            # BACKUP_SEMANTICS (directories). WRITE_DAC remains usable for repair.
            handle = kernel.CreateFileW(str(path), 0x80000000, 1, None, 3, 0x02000000, None)
            if handle == wintypes.HANDLE(-1).value:
                raise PermissionError("LAB_ACL_WRITER_OR_ACCESS_BLOCKED")
            handles.append(handle)
        yield
    finally:
        for handle in reversed(handles):
            kernel.CloseHandle(handle)


_ACL_SCRIPT = r"""
$ErrorActionPreference='Stop'
[Console]::OutputEncoding=[Text.Encoding]::UTF8
$request=[Console]::In.ReadToEnd() | ConvertFrom-Json
$section=[System.Security.AccessControl.AccessControlSections]::Access
$sidType=[System.Security.Principal.SecurityIdentifier]
$result=@(foreach($path in $request.paths) {
  $item=Get-Item -LiteralPath $path -Force
  if($item.Attributes -band [IO.FileAttributes]::ReparsePoint){throw 'reparse'}
  $acl=if($item.PSIsContainer){[IO.Directory]::GetAccessControl($path)}
    else{[IO.File]::GetAccessControl($path)}
  if($request.action -ne 'inspect') {
    if(-not $item.PSIsContainer -or $item.Name -ne 'lab_evidence'){throw 'scope'}
    if($acl.GetSecurityDescriptorSddlForm($section) -cne $request.expected){throw 'changed'}
    if($request.action -eq 'apply') {$acl.SetAccessRuleProtection($false,$true)}
    elseif($request.action -eq 'restore') {
      $acl.SetSecurityDescriptorSddlForm($request.original,$section)
    } else {throw 'action'}
    [IO.Directory]::SetAccessControl($path,$acl)
    $acl=[IO.Directory]::GetAccessControl($path)
  }
  @{
    sddl=$acl.GetSecurityDescriptorSddlForm($section)
    owner=$acl.GetOwner($sidType).Value
    owned=($acl.GetOwner($sidType).Value -eq
      [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value)
    protected=$acl.AreAccessRulesProtected
    canonical=$acl.AreAccessRulesCanonical
    rules=@($acl.GetAccessRules($true,$true,$sidType) | ForEach-Object {@{
      sid=$_.IdentityReference.Value; inherited=$_.IsInherited
      rights=[int]$_.FileSystemRights; allow=($_.AccessControlType -eq 'Allow')
      inheritance=[int]$_.InheritanceFlags; propagation=[int]$_.PropagationFlags
    }})
  }
})
ConvertTo-Json -InputObject $result -Depth 6 -Compress
"""


def _acls(paths: list[Path], *, action: str = "inspect", expected: str = "", original: str = ""):
    result = subprocess.run(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", _ACL_SCRIPT],
        input=json.dumps({
            "paths": [str(path) for path in paths], "action": action,
            "expected": expected, "original": original,
        }), capture_output=True, text=True, encoding="utf-8", timeout=60, check=False,
    )
    if result.returncode:
        # Native stderr may contain production paths or account names.
        raise PermissionError("LAB_ACL_NATIVE_OPERATION_FAILED")
    return json.loads(result.stdout)


def _validate_acls(acls: list[dict[str, Any]]) -> None:
    parent, root, *children = acls
    if parent["protected"] or not parent["canonical"]:
        raise StorageBoundaryError("LAB_ACL_PARENT_REJECTED")
    if not root["owned"] or not all(item["canonical"] for item in acls):
        raise StorageBoundaryError("LAB_ACL_OWNER_OR_RULES_REJECTED")
    if any(not rule["allow"] for item in acls for rule in item["rules"]):
        raise StorageBoundaryError("LAB_ACL_UNKNOWN_PRIVATE_DACL")
    if any(item["protected"] for item in children):
        raise StorageBoundaryError("LAB_ACL_PROTECTED_DESCENDANT")
    if not root["protected"]:
        return
    rules = root["rules"]
    if len(rules) != 3 or {rule["sid"] for rule in rules} != {
        "S-1-3-4", "S-1-5-18", "S-1-5-32-544",
    } or not all(
        rule["allow"] and not rule["inherited"] and rule["rights"] == 2032127
        and rule["inheritance"] == 3 and rule["propagation"] == 0 for rule in rules
    ):
        raise StorageBoundaryError("LAB_ACL_UNKNOWN_PRIVATE_DACL")


def _same_grants(left: list[dict[str, Any]], right: list[dict[str, Any]]) -> bool:
    # Windows adds SE_DACL_AUTO_INHERITED and reorders equivalent Allow ACEs
    # during propagation. Restore the original grants/protection, not those
    # incidental serialized flags; preserve both raw SDDLs in the receipt.
    def normalized(items):
        return [{
            **{key: item[key] for key in ("owner", "owned", "protected", "canonical")},
            "rules": sorted(json.dumps(rule, sort_keys=True) for rule in item["rules"]),
        } for item in items]

    return normalized(left) == normalized(right)


def _verify_grants(before: list[dict[str, Any]], after: list[dict[str, Any]]) -> None:
    for original, current in zip(before[1:], after[1:], strict=True):
        if current["owner"] != original["owner"] or current["protected"]:
            raise StorageBoundaryError("LAB_ACL_GRANT_VERIFICATION_FAILED")
        if [rule for rule in current["rules"] if not rule["inherited"]] != [
            rule for rule in original["rules"] if not rule["inherited"]
        ]:
            raise StorageBoundaryError("LAB_ACL_EXPLICIT_RULES_CHANGED")
        for grant in before[0]["rules"]:
            if grant["allow"] and grant["inheritance"] == 3 and grant["propagation"] == 0:
                if not any(
                    rule["sid"] == grant["sid"] and rule["inherited"] and rule["allow"]
                    and rule["rights"] & grant["rights"] == grant["rights"]
                    for rule in current["rules"]
                ):
                    raise StorageBoundaryError("LAB_ACL_GRANT_VERIFICATION_FAILED")


def _lease(data_root: Path, identity: ApplicationIdentity, token: str) -> dict[str, Any]:
    response = call_worker_command(
        "maintenance.status", {"token": token}, root=data_root,
        application_identity=identity, timeout=10,
    )
    if not token or response.get("valid") is not True or response.get("state") != "frozen":
        raise StorageBoundaryError("LAB_ACL_MAINTENANCE_UNCONFIRMED")
    if not response.get("participants") or not response.get("worker_id"):
        raise StorageBoundaryError("LAB_ACL_MAINTENANCE_UNCONFIRMED")
    return {key: response[key] for key in ("worker_id", "pid", "state", "participants")}


def _record(receipt: Path, value: dict[str, Any], *, create: bool = False) -> None:
    with receipt.open("x" if create else "a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, ensure_ascii=False) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def _validate_receipt(receipt: Path, target: Path) -> None:
    if not receipt.is_absolute() or not receipt.parent.is_dir() or receipt.is_relative_to(target):
        raise StorageBoundaryError("LAB_ACL_RECEIPT_INVALID")
    if receipt != Path(os.path.abspath(receipt)):
        raise StorageBoundaryError("LAB_ACL_RECEIPT_INVALID")
    for path in (receipt.parent, *receipt.parent.parents):
        if path.lstat().st_file_attributes & stat.FILE_ATTRIBUTE_REPARSE_POINT:
            raise StorageBoundaryError("LAB_ACL_RECEIPT_INVALID")
    if receipt.is_symlink() or (receipt.exists() and not receipt.is_file()):
        raise StorageBoundaryError("LAB_ACL_RECEIPT_INVALID")
    if receipt.exists() and receipt.stat().st_nlink != 1:
        raise StorageBoundaryError("LAB_ACL_RECEIPT_INVALID")


def _restore_baseline(receipt: Path, target: Path, inventory, before):
    saved = json.loads(receipt.read_text(encoding="utf-8").splitlines()[0])
    if saved["schema"] != 1 or saved["target"] != str(target) or saved["inventory"] != inventory:
        raise StorageBoundaryError("LAB_ACL_RESTORE_IDENTITY_CHANGED")
    if saved["acls"][0] != before[0]:
        raise StorageBoundaryError("LAB_ACL_PARENT_CHANGED")
    expected = saved["acls"]
    if not _same_grants(before, expected):
        _verify_grants(expected, before)
    return expected


def _verify_result(target, inventory, before, after, action, expected):
    if _inventory(target) != inventory or after[0] != before[0]:
        raise StorageBoundaryError("LAB_ACL_POSTCHECK_CHANGED_RESTORE_REQUIRED")
    if action == "restore":
        if not _same_grants(after, expected):
            raise StorageBoundaryError("LAB_ACL_RESTORE_VERIFICATION_FAILED")
    else:
        _validate_acls(after)
        _verify_grants(before, after)


def _inventory_digest(inventory) -> str:
    return hashlib.sha256(json.dumps(inventory, sort_keys=True).encode()).hexdigest()


def repair(
    data_root: Path, *, action: str, receipt: Path, confirmation: str,
    identity: ApplicationIdentity, token: str, expected_inventory: str = "",
) -> dict[str, Any]:
    target = _target(data_root)
    if action not in {"preview", "apply", "restore"} or confirmation != str(target):
        raise StorageBoundaryError("LAB_ACL_EXACT_CONFIRMATION_REQUIRED")
    _validate_receipt(receipt, target)
    lease = _lease(data_root, identity, token)
    inventory = _inventory(target)
    paths = [target.parent, *[target / item["name"] for item in inventory]]
    with _deny_writers(paths):
        if _inventory(target) != inventory:
            raise StorageBoundaryError("LAB_ACL_EVIDENCE_CHANGED")
        before = _acls(paths)
        _validate_acls(before)
        summary = {
            "action": action, "scope": "lab_evidence root DACL only",
            "objects": len(inventory), "files": sum(not item["directory"] for item in inventory),
            "bytes": sum(item["bytes"] for item in inventory),
            "needs_repair": before[1]["protected"],
            "inventory_sha256": _inventory_digest(inventory),
        }
        if action == "preview":
            return {**summary, "inventory": inventory}
        if _lease(data_root, identity, token) != lease:
            raise StorageBoundaryError("LAB_ACL_WORKER_CHANGED")
        if action == "restore":
            expected = _restore_baseline(receipt, target, inventory, before)
        else:
            if expected_inventory != summary["inventory_sha256"]:
                raise StorageBoundaryError("LAB_ACL_PREVIEW_CHANGED")
            if not before[1]["protected"]:
                return {**summary, "status": "already_inheriting"}
            expected = before
            _record(receipt, {
                "schema": 1, "target": str(target), "inventory": inventory,
                "acls": before, "lease": lease,
            }, create=True)
        _acls(
            [target], action=action, expected=before[1]["sddl"],
            original=expected[1]["sddl"] if action == "restore" else "",
        )
        after = _acls(paths)
        _verify_result(target, inventory, before, after, action, expected)
        if _lease(data_root, identity, token) != lease:
            raise StorageBoundaryError("LAB_ACL_WORKER_CHANGED_RESTORE_REQUIRED")
        _record(receipt, {"status": action + "_complete", "acls": after})
        return {**summary, "status": action + "_complete"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["preview", "apply", "restore"])
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--confirm", required=True)
    parser.add_argument("--build-sha", required=True)
    parser.add_argument("--slot-id", required=True)
    parser.add_argument("--generation", required=True)
    parser.add_argument("--maintenance-token", required=True)
    parser.add_argument("--expected-inventory", default="", help="apply requires the approved preview digest")
    args = parser.parse_args()
    try:
        result = repair(
            args.data_root, action=args.action, receipt=args.receipt, confirmation=args.confirm,
            identity=ApplicationIdentity(args.build_sha, args.slot_id, args.generation),
            token=args.maintenance_token, expected_inventory=args.expected_inventory,
        )
    except (OSError, ValueError, RuntimeError, KeyError, IndexError, subprocess.SubprocessError) as exc:
        code = str(exc) if str(exc).startswith("LAB_ACL_") else "LAB_ACL_OPERATION_FAILED"
        print(json.dumps({"status": "blocked", "code": code}))
        return 2
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
