"""Keep pytest temporary directories deletable across Windows sandbox identities."""

from __future__ import annotations

import os
import stat
import subprocess
from os import environ
from pathlib import Path
from typing import Any

from _pytest.config import hookimpl


class AclRecoveryError(PermissionError):
    """An ACL recovery attempt failed with a retryable diagnosis."""

    def __init__(self, kind: str, detail: str) -> None:
        self.kind = kind
        super().__init__(detail)


def prepare_pytest_directory(path: Path) -> Path:
    """Create a writable pytest directory without replacing inherited ACLs."""
    target = path.resolve()
    target.mkdir(parents=True, exist_ok=True)
    if not os.access(target, os.W_OK):
        raise PermissionError(f"目录不可写: {target}")
    return target


def make_writable(function: Any, path: str | bytes, error: BaseException) -> None:
    """Clear a read-only file attribute before retrying a tree removal."""
    if not isinstance(error, PermissionError):
        raise error
    os.chmod(path, stat.S_IWRITE)
    function(path)


def restore_acl_inheritance(path: Path) -> None:
    """Restore inherited Windows ACLs for one path blocked during cleanup."""
    if os.name != "nt":
        return
    blocked = path.resolve()
    script = (
        "$ErrorActionPreference='Stop';"
        "$item=Get-Item -LiteralPath $env:QM_TASK_ARTIFACT_BLOCKED -Force;"
        "$acl=$null;"
        "try{"
        "$acl=if($item.PSIsContainer){"
        "[System.IO.Directory]::GetAccessControl($item.FullName)"
        "}else{[System.IO.File]::GetAccessControl($item.FullName)}"
        "}catch{"
        "if($_.Exception.Message -notmatch '(?i)access denied|unauthorized'){throw};"
        "$parent=if($item.PSIsContainer){$item.Parent}else{$item.Directory};"
        "while($null -ne $parent -and $null -eq $acl){"
        "try{$acl=[System.IO.Directory]::GetAccessControl($parent.FullName)}"
        "catch{"
        "if($_.Exception.Message -notmatch '(?i)access denied|unauthorized'){throw};"
        "$parent=$parent.Parent"
        "}"
        "};"
        "};"
        "if($null -eq $acl){throw '无法读取目标或父目录 ACL'};"
        "if($acl.AreAccessRulesProtected){"
        "$acl.SetAccessRuleProtection($false,$true)"
        "};"
        "if($item.PSIsContainer){"
        "[System.IO.Directory]::SetAccessControl($item.FullName,$acl)"
        "}else{"
        "if($acl -is [System.Security.AccessControl.FileSecurity]){"
        "[System.IO.File]::SetAccessControl($item.FullName,$acl)"
        "}else{"
        "$fileAcl=New-Object System.Security.AccessControl.FileSecurity;"
        "foreach($rule in $acl.Access){"
        "$fileAcl.AddAccessRule("
        "[System.Security.AccessControl.FileSystemAccessRule]::new("
        "$rule.IdentityReference,$rule.FileSystemRights,$rule.AccessControlType))};"
        "$fileAcl.SetAccessRuleProtection($false,$true);"
        "[System.IO.File]::SetAccessControl($item.FullName,$fileAcl)"
        "}"
        "}"
    )
    environment = os.environ.copy()
    environment["QM_TASK_ARTIFACT_BLOCKED"] = str(blocked)
    try:
        result = subprocess.run(
            ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script],
            env=environment,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise AclRecoveryError(
            "transient", f"{type(exc).__name__}: {exc}"
        ) from exc
    if result.returncode:
        detail = result.stderr.strip() or result.stdout.strip() or "unknown ACL error"
        lowered = detail.casefold()
        kind = (
            "inspection_denied"
            if "unauthorizedaccessexception" in lowered
            or "access denied" in lowered
            or "access is denied" in lowered
            else "transient"
        )
        raise AclRecoveryError(kind, detail)


def _install_task_artifact_lease(config: Any) -> None:
    """Keep direct pytest runs from racing task-worktree removal."""
    from scripts.dev.tasks import task_artifact_lease

    cwd = Path.cwd().resolve()
    if cwd.parent.name != ".worktrees":
        return
    primary = cwd.parents[1]
    artifacts = (primary / ".artifacts" / "worktrees" / cwd.name).resolve()
    if environ.get("QM_TASK_LEASE_HELD") == str(artifacts):
        return
    previous = environ.get("QM_TASK_LEASE_HELD")
    lease = task_artifact_lease(artifacts)
    lease.__enter__()
    environ["QM_TASK_LEASE_HELD"] = str(artifacts)

    def release() -> None:
        if previous is None:
            environ.pop("QM_TASK_LEASE_HELD", None)
        else:
            environ["QM_TASK_LEASE_HELD"] = previous
        lease.__exit__(None, None, None)

    config.add_cleanup(release)


def _install_inheriting_tmp_path_factory(config: Any) -> None:
    """Route pytest fixture directories through the verified Windows creator."""
    from _pytest.pathlib import find_suffixes, parse_num
    from _pytest.tmpdir import TempPathFactory

    original = TempPathFactory.mktemp

    def mktemp_inheriting_acl(
        factory: TempPathFactory, basename: str, numbered: bool = True,
    ) -> Path:
        relative = factory._ensure_relative_to_basetemp(basename)
        if numbered:
            root = factory.getbasetemp()
            for _attempt in range(10):
                number = max(map(parse_num, find_suffixes(root, relative)), default=-1) + 1
                path = root / f"{relative}{number}"
                try:
                    path.mkdir()
                except FileExistsError:
                    continue
                break
            else:
                raise OSError(f"无法创建 pytest fixture 目录：{root / relative}")
        else:
            path = factory.getbasetemp() / relative
            path.mkdir()
        factory._trace("mktemp", path)
        return path

    TempPathFactory.mktemp = mktemp_inheriting_acl
    config.add_cleanup(lambda: setattr(TempPathFactory, "mktemp", original))


@hookimpl(trylast=True)
def pytest_configure(config: Any) -> None:
    _install_task_artifact_lease(config)
    if os.name != "nt":
        return
    cache = getattr(config, "cache", None)
    cachedir = getattr(cache, "_cachedir", None)
    if cachedir is not None:
        prepare_pytest_directory(Path(cachedir))

    factory = getattr(config, "_tmp_path_factory", None)
    given_basetemp = getattr(factory, "_given_basetemp", None)
    if factory is None or given_basetemp is None:
        return

    # TempPathFactory.getbasetemp() deletes an existing --basetemp and recreates
    # it with mode=0700.  On Windows that replacement protects the DACL instead
    # of inheriting the task artifact ACL, so a later sandbox identity cannot
    # remove the directory.  Bind the prepared directory as the resolved base
    # before pytest gets a chance to replace it.
    factory._basetemp = prepare_pytest_directory(Path(given_basetemp))
    _install_inheriting_tmp_path_factory(config)
