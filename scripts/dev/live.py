"""Stage one verified Windows onedir candidate without changing activation state."""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import uuid
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.dev import tasks  # noqa: E402
from scripts.release import check_desktop_artifact, smoke_frozen_runtime  # noqa: E402

FULL_SHA = re.compile(r"[0-9a-f]{40}")
STAGE_SCHEMA = 1
ACTIVE_STATE_SCHEMA = 1
STAGE_MARKER = ".quantmaster-stage.json"
LIFECYCLE_LOCK = ".lifecycle.lock"
MAX_EXTRACTED_BYTES = check_desktop_artifact.ONEDIR_MAX_MIB * 1024 * 1024


class StageBlocked(RuntimeError):
    """A deliberate fail-closed staging decision."""

    def __init__(self, reason: str, detail: str, **context: object) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail
        self.context = context


def _block(reason: str, detail: str, **context: object) -> None:
    raise StageBlocked(reason, detail, **context)


def _full_sha(value: object, *, label: str) -> str:
    result = str(value or "")
    if FULL_SHA.fullmatch(result) is None:
        _block("invalid_sha", f"{label} 不是完整 lowercase Git SHA", value=result)
    return result


def _is_link(path: Path) -> bool:
    return path.is_symlink() or bool(getattr(path, "is_junction", lambda: False)())


def _status(cwd: Path) -> str:
    return tasks.git(
        ["status", "--porcelain", "--untracked-files=all"], cwd=cwd,
    ).stdout.strip()


def _validate_primary(cwd: Path) -> tuple[Path, str]:
    if os.name != "nt":
        _block("windows_only", "verified onedir staging requires Windows")
    primary = tasks.primary_root(cwd)
    if primary != cwd.resolve():
        _block("primary_checkout_required", f"必须从 primary checkout 运行：{primary}")
    branch = tasks.git(["branch", "--show-current"], cwd=primary).stdout.strip()
    if branch != "main":
        _block("main_checkout_required", f"当前 checkout 不是 clean main：{branch or 'detached HEAD'}")
    status = _status(primary)
    if status:
        _block("main_dirty", "local main 不干净", status=status)
    head = _full_sha(tasks.git(["rev-parse", "HEAD^{commit}"], cwd=primary).stdout.strip(), label="HEAD")
    main = _full_sha(tasks.git(["rev-parse", "main^{commit}"], cwd=primary).stdout.strip(), label="main")
    if head != main:
        _block("main_checkout_required", "HEAD 没有指向 local main", head=head, main=main)
    remote = tasks.git(["rev-parse", "origin/main^{commit}"], cwd=primary, check=False)
    if remote.returncode:
        _block("main_unsynchronized", "无法解析 origin/main；不会自动 fetch")
    origin = _full_sha(remote.stdout.strip(), label="origin/main")
    if main != origin:
        _block("main_unsynchronized", "local main 与 origin/main 不一致", main=main, origin=origin)
    return primary, main


def _task_target(primary: Path, slug: str) -> tuple[Path, str]:
    if tasks.SLUG_PATTERN.fullmatch(slug) is None:
        _block("invalid_task", f"无效 task slug：{slug}")
    target = (primary / ".worktrees" / slug).resolve()
    if target not in tasks.registered_worktrees(primary) or not target.is_dir():
        _block("task_worktree_missing", f"task worktree 未登记或不存在：{target}")
    expected_branch = f"codex/{slug}"
    branch = tasks.git(["branch", "--show-current"], cwd=target).stdout.strip()
    if branch != expected_branch:
        _block("task_branch_mismatch", f"task worktree 分支不匹配：{branch or 'detached HEAD'}")
    if _status(target):
        _block("task_dirty", f"task worktree 不干净：{target}")
    return target, expected_branch


def _load_evidence(target: Path) -> dict[str, object]:
    evidence_path = tasks.validation_evidence_path(target)
    try:
        evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        _block("validation_evidence_missing", f"ready --package evidence 不可读：{evidence_path}: {exc}")
    if not isinstance(evidence, dict):
        _block("validation_evidence_unknown", "ready evidence 不是 JSON object")
    if evidence.get("package") is not True:
        _block("package_evidence_required", "缺少 exact ready --package evidence")
    for name in ("ui", "rust"):
        if not isinstance(evidence.get(name), bool):
            _block("validation_evidence_unknown", f"ready evidence 的 {name} 字段无效")
    return evidence


def _validate_task_commit(
    primary: Path, branch: str, evidence: dict[str, object], main_sha: str,
) -> tuple[str, str]:
    task_sha = _full_sha(evidence.get("commit"), label="validated task commit")
    base_sha = _full_sha(evidence.get("base"), label="validation base")
    branch_sha_result = tasks.git(
        ["rev-parse", f"refs/heads/{branch}"], cwd=primary, check=False,
    )
    if branch_sha_result.returncode:
        _block("validated_task_missing", f"validated task branch 不存在：{branch}")
    branch_sha = _full_sha(branch_sha_result.stdout.strip(), label="task branch")
    if branch_sha != task_sha:
        _block(
            "stale_validation_evidence",
            "ready evidence 不再对应 task branch HEAD",
            evidence=task_sha,
            branch=branch_sha,
        )
    if tasks.git(
        ["merge-base", "--is-ancestor", base_sha, task_sha], cwd=primary, check=False,
    ).returncode:
        _block("invalid_validation_base", "validation base 不是 task commit 的祖先", base=base_sha)
    if tasks.git(
        ["merge-base", "--is-ancestor", base_sha, main_sha], cwd=primary, check=False,
    ).returncode:
        _block("stale_validation_base", "validation base 不在当前 main 历史中", base=base_sha)
    return task_sha, base_sha


def _validate_evidence_identity(
    target: Path, evidence: dict[str, object], base_sha: str,
) -> None:
    try:
        current_identity = tasks.full_validation_identity(
            target,
            base=base_sha,
            ui=evidence["ui"],
            rust=evidence["rust"],
            package=True,
        )
    except (OSError, RuntimeError, subprocess.CalledProcessError) as exc:
        _block("validation_evidence_unknown", f"无法重新确认 ready evidence：{exc}")
    if current_identity != evidence:
        _block("stale_validation_evidence", "ready evidence 与当前 task 环境不完全一致")


def _validate_same_tree(primary: Path, main_sha: str, task_sha: str) -> None:
    main_tree = tasks.git(["rev-parse", f"{main_sha}^{{tree}}"], cwd=primary).stdout.strip()
    task_tree = tasks.git(["rev-parse", f"{task_sha}^{{tree}}"], cwd=primary).stdout.strip()
    if main_tree != task_tree:
        _block(
            "task_tree_mismatch",
            "validated task tree 未被当前 main 完整表示",
            main_tree=main_tree,
            task_tree=task_tree,
        )


def _validate_task(primary: Path, slug: str, main_sha: str) -> tuple[Path, dict[str, object]]:
    target, branch = _task_target(primary, slug)
    evidence = _load_evidence(target)
    task_sha, base_sha = _validate_task_commit(primary, branch, evidence, main_sha)
    _validate_evidence_identity(target, evidence, base_sha)
    _validate_same_tree(primary, main_sha, task_sha)
    return target, evidence


def _slot_paths(main_sha: str) -> tuple[Path, Path, Path]:
    local_appdata = os.environ.get("LOCALAPPDATA", "").strip()
    if not local_appdata or not Path(local_appdata).is_absolute():
        _block("localappdata_required", "LOCALAPPDATA 必须是绝对路径")
    app_root = Path(local_appdata) / "QuantMaster" / "app"
    slots = app_root / "slots"
    for path in (app_root.parent, app_root, slots):
        if path.exists() and _is_link(path):
            _block("unsafe_slot_root", f"槽路径不能是 link/junction：{path}")
    return app_root, slots, slots / main_sha


@contextlib.contextmanager
def application_lifecycle_lock(app_root: Path):
    """Serialize staging with the future activation helper for one installation."""

    app_root.mkdir(parents=True, exist_ok=True)
    marker = app_root / LIFECYCLE_LOCK
    if _is_link(marker) or (marker.exists() and not marker.is_file()):
        _block("unsafe_lifecycle_lock", f"应用生命周期锁不是普通文件：{marker}")
    try:
        with marker.open("a+b") as stream:
            if stream.tell() == 0:
                stream.write(b"0")
                stream.flush()
            if not tasks._try_lock(stream):
                _block("lifecycle_busy", "另一个 staging/activation 操作正在进行")
            try:
                yield
            finally:
                tasks._unlock(stream)
    except StageBlocked:
        raise
    except OSError as exc:
        _block("lifecycle_lock_failed", f"无法持有应用生命周期锁：{exc}")


def _active_snapshot(active: Path, main_sha: str) -> tuple[bool, bytes]:
    if _is_link(active) or (active.exists() and not active.is_file()):
        _block("active_state_invalid", f"active.json 不是普通文件：{active}")
    if not active.exists():
        return False, b""
    try:
        raw = active.read_bytes()
        state = json.loads(raw)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        _block("active_state_invalid", f"active.json 不可读：{active}: {exc}")
    if not isinstance(state, dict) or type(state.get("schema")) is not int:
        _block("active_state_invalid", "active.json 缺少受支持的 schema")
    if state["schema"] != ACTIVE_STATE_SCHEMA:
        _block("active_state_unknown", f"不支持 active.json schema：{state['schema']}")
    for name in ("active", "previous", "pending"):
        value = state.get(name, "")
        if value not in (None, "") and (
            not isinstance(value, str) or FULL_SHA.fullmatch(value) is None
        ):
            _block("active_state_invalid", f"active.json 的 {name} 不是完整 SHA")
    protected = [name for name in ("active", "previous") if state.get(name) == main_sha]
    if protected:
        _block(
            "protected_slot",
            "staging 不得写入 active/previous 槽",
            references=protected,
            build_sha=main_sha,
        )
    return True, raw


def _assert_active_unchanged(active: Path, snapshot: tuple[bool, bytes]) -> None:
    current = (active.exists(), active.read_bytes() if active.exists() else b"")
    if current != snapshot:
        _block("active_state_changed", "staging 期间 active.json 发生变化；候选已放弃")


def _build_onedir(
    project_root: Path,
    python: Path,
    main_sha: str,
    task_artifacts: Path,
    build_root: Path,
) -> tuple[Path, dict[str, object]]:
    desktop_root = build_root / "desktop"
    analysis_root = build_root / "pyinstaller"
    build_env = os.environ.copy()
    build_env["QM_DESKTOP_LAYOUT"] = "onedir-measurement"
    build_env["UV_CACHE_DIR"] = str(task_artifacts / "uv-cache")
    command = [
        "uv", "run", "--no-project", "--python", str(python),
        "--with", "PyInstaller==6.19.0", "-m", "PyInstaller", "--noconfirm",
        "--distpath", str(desktop_root), "--workpath", str(analysis_root),
        "packaging/quantmaster.spec",
    ]
    completed = subprocess.run(command, cwd=project_root, env=build_env, check=False)
    if completed.returncode:
        _block("package_build_failed", f"PyInstaller onedir build failed ({completed.returncode})")
    application = desktop_root / "QuantMaster"
    if not application.is_dir():
        _block("package_build_failed", f"PyInstaller did not produce onedir：{application}")
    archive = build_root / "QuantMaster.zip"
    report_path = build_root / "QuantMaster.sizes.json"
    report = check_desktop_artifact.package_onedir(
        application,
        archive,
        report_path,
        analysis=analysis_root / "quantmaster" / "Analysis-00.toc",
        build_sha=main_sha,
    )
    errors = list(report.get("errors", []))
    if errors:
        _block("package_measurement_failed", "; ".join(str(error) for error in errors))
    if report.get("build_sha") != main_sha:
        _block("package_identity_mismatch", "onedir size report 的 build_sha 不等于 main SHA")
    if not report.get("within_hard_limits"):
        _block(
            "size_budget_exceeded",
            "; ".join(str(error) for error in report.get("limit_failures", [])),
            size_report=report,
        )
    return archive, report


def _snapshot_main(primary: Path, main_sha: str, build_root: Path) -> Path:
    """Materialize one exact commit without reading the mutable primary work tree."""

    snapshot = build_root / "source"
    try:
        tasks.git(
            ["clone", "--shared", "--no-checkout", "--quiet", str(primary), str(snapshot)],
            cwd=primary,
        )
        tasks.git(
            ["-c", f"core.hooksPath={os.devnull}", "checkout", "--detach", "--quiet", main_sha],
            cwd=snapshot,
        )
        if _status(snapshot):
            _block("source_snapshot_failed", "exact main snapshot 不干净")
        snapshot_sha = tasks.git(
            ["rev-parse", "HEAD^{commit}"], cwd=snapshot,
        ).stdout.strip()
        if snapshot_sha != main_sha:
            _block(
                "source_snapshot_failed",
                "exact main snapshot 身份不匹配",
                expected=main_sha,
                actual=snapshot_sha,
            )
    except StageBlocked:
        raise
    except (OSError, subprocess.CalledProcessError) as exc:
        _block("source_snapshot_failed", f"无法创建 exact main snapshot：{exc}")
    return snapshot


def _safe_member_name(name: str) -> PurePosixPath:
    if "\\" in name or not name.startswith("QuantMaster/"):
        _block("unsafe_archive", f"ZIP member 不在 QuantMaster 根目录：{name}")
    relative = PurePosixPath(name.removeprefix("QuantMaster/"))
    if relative.is_absolute() or not relative.parts or any(
        part in {"", ".", ".."} for part in relative.parts
    ):
        _block("unsafe_archive", f"ZIP member path 不安全：{name}")
    return relative


def _validate_slot_tree(root: Path) -> None:
    if not root.is_dir() or _is_link(root):
        _block("invalid_slot", f"候选槽根目录无效：{root}")
    launcher = root / "QuantMaster.exe"
    if not launcher.is_file() or _is_link(launcher):
        _block("invalid_slot", f"候选槽缺少普通 QuantMaster.exe：{launcher}")
    for path in root.rglob("*"):
        if _is_link(path):
            _block("unsafe_archive", f"候选槽包含 link/junction：{path}")


def _extract_archive(archive: Path, extraction_parent: Path) -> Path:
    if not archive.is_file():
        _block("package_archive_missing", f"onedir ZIP 不存在：{archive}")
    extracted_root = extraction_parent / "QuantMaster"
    seen: set[str] = set()
    extracted_bytes = 0
    try:
        with ZipFile(archive) as packaged:
            for member in packaged.infolist():
                relative = _safe_member_name(member.filename)
                key = relative.as_posix()
                if key in seen:
                    _block("unsafe_archive", f"ZIP member 重复：{member.filename}")
                seen.add(key)
                mode = member.external_attr >> 16
                if mode and stat.S_ISLNK(mode):
                    _block("unsafe_archive", f"ZIP member 不能是 symlink：{member.filename}")
                destination = extracted_root / relative
                if member.is_dir():
                    destination.mkdir(parents=True, exist_ok=True)
                    continue
                extracted_bytes += member.file_size
                if extracted_bytes > MAX_EXTRACTED_BYTES:
                    _block("size_budget_exceeded", "ZIP 展开后超过 onedir hard limit")
                destination.parent.mkdir(parents=True, exist_ok=True)
                with packaged.open(member) as source, destination.open("xb") as target:
                    shutil.copyfileobj(source, target)
    except StageBlocked:
        raise
    except (OSError, ValueError) as exc:
        _block("archive_extract_failed", f"无法展开 onedir ZIP：{exc}")
    _validate_slot_tree(extracted_root)
    return extracted_root


def _read_marker(slot: Path, main_sha: str) -> dict[str, object] | None:
    if not slot.exists():
        return None
    if _is_link(slot) or not slot.is_dir():
        _block("conflicting_slot", f"目标槽不是普通目录：{slot}")
    marker = slot / STAGE_MARKER
    if not marker.is_file() or _is_link(marker):
        _block("partial_slot", f"目标槽存在但缺少完整 staging marker：{slot}")
    try:
        payload = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        _block("partial_slot", f"staging marker 不可读：{marker}: {exc}")
    if not isinstance(payload, dict) or payload.get("schema") != STAGE_SCHEMA:
        _block("conflicting_slot", f"staging marker schema 冲突：{marker}")
    if payload.get("status") != "staged" or payload.get("build_sha") != main_sha:
        _block("conflicting_slot", f"staging marker 与目标 SHA 冲突：{marker}")
    if payload.get("slot_id") != main_sha:
        _block("conflicting_slot", f"staging marker slot_id 冲突：{marker}")
    size = payload.get("size")
    complete = (
        payload.get("idempotent") is False
        and isinstance(payload.get("source_task"), str)
        and tasks.SLUG_PATTERN.fullmatch(str(payload["source_task"])) is not None
        and FULL_SHA.fullmatch(str(payload.get("source_task_commit") or "")) is not None
        and payload.get("slot") == str(slot)
        and isinstance(payload.get("staged_at"), str)
        and bool(payload["staged_at"])
        and isinstance(size, dict)
        and size.get("mode") == "onedir-measurement"
        and size.get("build_sha") == main_sha
        and size.get("within_hard_limits") is True
        and not size.get("errors")
        and not size.get("limit_failures")
    )
    if not complete:
        _block("partial_slot", f"staging marker 缺少完整 package evidence：{marker}")
    try:
        _verify_smoke(payload.get("smoke"), main_sha)
    except StageBlocked:
        _block("partial_slot", f"staging marker 缺少匹配的 packaged smoke：{marker}")
    _validate_slot_tree(slot)
    return payload


def _write_marker(slot: Path, payload: dict[str, object]) -> None:
    marker = slot / STAGE_MARKER
    temporary = slot / f".{STAGE_MARKER}.{uuid.uuid4().hex}.tmp"
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, marker)


def _remove_owned_slot(slot: Path) -> None:
    if slot.exists() and not _is_link(slot) and slot.is_dir():
        shutil.rmtree(slot)


def _verify_smoke(smoke: object, main_sha: str) -> dict[str, object]:
    if not isinstance(smoke, dict):
        _block("packaged_smoke_rejected", "packaged smoke 没有结构化结果")
    if smoke.get("layout") != "onedir":
        _block("packaged_smoke_rejected", "packaged smoke 未确认 onedir layout")
    for name in ("build_sha", "slot_id"):
        if smoke.get(name) != main_sha:
            _block("runtime_identity_mismatch", f"packaged smoke 的 {name} 不等于 main SHA")
    generation = smoke.get("runtime_generation")
    if not isinstance(generation, str) or re.fullmatch(r"[0-9a-f]{32}", generation) is None:
        _block("runtime_identity_mismatch", "packaged smoke 的 runtime_generation 无效")
    if (
        smoke.get("help_budget_seconds") != 1.5
        or not isinstance(smoke.get("help_seconds"), (int, float))
        or float(smoke["help_seconds"]) > 1.5
        or not isinstance(smoke.get("core_ready_seconds"), (int, float))
        or not smoke.get("processes_stopped")
        or not smoke.get("port_released")
        or smoke.get("executable_unchanged") is not True
    ):
        _block("packaged_smoke_rejected", "packaged smoke 未满足完整 onedir 合同")
    return smoke


def _run_packaged_smoke(slot: Path, main_sha: str) -> dict[str, object]:
    try:
        smoke_result = smoke_frozen_runtime.smoke(
            slot / "QuantMaster.exe", layout="onedir",
        )
    except Exception as exc:
        _block("packaged_smoke_failed", f"packaged smoke failed：{exc}")
    return _verify_smoke(smoke_result, main_sha)


def _stage_candidate(
    primary: Path, slug: str, main_sha: str, evidence: dict[str, object],
) -> dict[str, object]:
    task_artifacts = primary / ".artifacts" / "worktrees" / slug
    app_root, slots, slot = _slot_paths(main_sha)
    active = app_root / "active.json"
    with application_lifecycle_lock(app_root):
        active_snapshot = _active_snapshot(active, main_sha)
        existing = _read_marker(slot, main_sha)
        if existing is not None:
            smoke = _run_packaged_smoke(slot, main_sha)
            _assert_active_unchanged(active, active_snapshot)
            return {**existing, "smoke": smoke, "idempotent": True}

        slots.mkdir(parents=True, exist_ok=True)
        owned_slot = False
        try:
            with tempfile.TemporaryDirectory(
                prefix=f"stage-{main_sha[:12]}-", dir=task_artifacts,
            ) as raw_build:
                build_root = Path(raw_build)
                snapshot = _snapshot_main(primary, main_sha, build_root)
                archive, size_report = _build_onedir(
                    snapshot,
                    tasks.project_python(primary),
                    main_sha,
                    task_artifacts,
                    build_root,
                )
                with tempfile.TemporaryDirectory(
                    prefix=f".{main_sha[:12]}-", dir=slots,
                ) as raw_extract:
                    extracted_root = _extract_archive(archive, Path(raw_extract))
                    if slot.exists():
                        _block("conflicting_slot", f"目标槽在构建期间出现：{slot}")
                    os.replace(extracted_root, slot)
                    owned_slot = True
                smoke = _run_packaged_smoke(slot, main_sha)
                payload: dict[str, object] = {
                    "schema": STAGE_SCHEMA,
                    "status": "staged",
                    "idempotent": False,
                    "source_task": slug,
                    "source_task_commit": evidence["commit"],
                    "build_sha": main_sha,
                    "slot_id": main_sha,
                    "slot": str(slot),
                    "size": size_report,
                    "smoke": smoke,
                    "staged_at": datetime.now(UTC).isoformat(),
                }
                _write_marker(slot, payload)
                owned_slot = False
                _assert_active_unchanged(active, active_snapshot)
                return payload
        finally:
            if owned_slot:
                _remove_owned_slot(slot)


def stage(slug: str, *, cwd: Path | None = None) -> dict[str, object]:
    """Build, extract, smoke and mark one immutable slot without activation."""

    current = (cwd or Path.cwd()).resolve()
    primary, main_sha = _validate_primary(current)
    task_artifacts = primary / ".artifacts" / "worktrees" / slug
    if not task_artifacts.is_dir():
        _block("task_artifacts_missing", f"task artifacts 不存在：{task_artifacts}")
    with tasks.task_artifact_lease(task_artifacts):
        _task_target, evidence = _validate_task(primary, slug, main_sha)
        return _stage_candidate(primary, slug, main_sha, evidence)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    stage_parser = commands.add_parser("stage", help="stage one verified local main onedir slot")
    stage_parser.add_argument("--from-task", required=True, dest="slug")
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        result = stage(args.slug)
    except StageBlocked as exc:
        print(json.dumps({"status": "blocked", "blocker": {
            "reason": exc.reason, "detail": exc.detail, **exc.context,
        }}, ensure_ascii=False, default=str))
        return 1
    except Exception as exc:
        print(json.dumps({"status": "blocked", "blocker": {
            "reason": "internal_error", "detail": str(exc),
        }}, ensure_ascii=False))
        return 1
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
