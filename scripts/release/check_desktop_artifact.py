"""Check release onefiles or report an experimental Windows onedir build."""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import subprocess
import tempfile
from collections import defaultdict
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

ONEDIR_MAX_MIB = 350
ONEFILE_MAX_MIB = 350
ZIP_TARGET_MIB = 125
ZIP_MAX_MIB = 130
V1_15_4_ZIP_BYTES = 127_674_671
GROWTH_ATTRIBUTION_BYTES = 2 * 1024 * 1024
FORBIDDEN_MODULES = (
    "torch", "dask", "pytest", "_pytest", "qrcode.tests",
    # Feishu adapter module not used by QuantMaster's automation flows.
    "lark_oapi.adapter",
    # PyArrow optional/backend modules not needed by the Research Lake path.
    "pyarrow.acero", "pyarrow.cuda", "pyarrow.dataset", "pyarrow.feather",
    "pyarrow.flight", "pyarrow.gandiva", "pyarrow.json", "pyarrow.substrait",
    "pyarrow.parquet.encryption",
)
MODULE_ENTRY = re.compile(r"(?m)^\s*\('([^']+)'[,)]")
PACKAGED_INPUT_PATHS = (
    "quantmaster",
    "packaging",
    "scripts/release/check_desktop_artifact.py",
)


def packaged_build_sha(project_root: Path) -> str:
    """Return HEAD only when every tracked build input matches it."""

    root = project_root.resolve()
    tracked_tree_changed = subprocess.run(
        ["git", "diff-index", "--quiet", "HEAD", "--", *PACKAGED_INPUT_PATHS],
        cwd=root,
    ).returncode
    if tracked_tree_changed:
        raise RuntimeError("QuantMaster packages require the tracked Git tree to match HEAD")
    untracked_inputs = subprocess.run(
        [
            "git", "ls-files", "--others", "--exclude-standard", "--",
            *PACKAGED_INPUT_PATHS,
        ],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    if untracked_inputs:
        raise RuntimeError(
            "QuantMaster packages reject untracked build inputs: "
            + ", ".join(untracked_inputs)
        )
    build_sha = subprocess.run(
        ["git", "rev-parse", "--verify", "HEAD"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if re.fullmatch(r"[0-9a-f]{40}", build_sha) is None:
        raise RuntimeError("QuantMaster packages require a full lowercase Git SHA")
    return build_sha


def _check_analysis(analysis: Path | None) -> list[str]:
    errors: list[str] = []
    if analysis is not None:
        if not analysis.is_file():
            errors.append(f"PyInstaller analysis does not exist: {analysis}")
        else:
            modules = set(MODULE_ENTRY.findall(analysis.read_text(encoding="utf-8")))
            bundled = sorted(
                module
                for module in modules
                if any(module == root or module.startswith(f"{root}.") for root in FORBIDDEN_MODULES)
            )
            if bundled:
                preview = ", ".join(bundled[:8])
                errors.append(f"forbidden optional modules were bundled: {preview}")
    return errors


def check_onefile(
    artifact: Path,
    *,
    analysis: Path | None = None,
    max_bytes: int = ONEFILE_MAX_MIB * 1024 * 1024,
) -> list[str]:
    if not artifact.is_file():
        return [f"desktop onefile does not exist: {artifact}"]
    errors = []
    size = artifact.stat().st_size
    if size > max_bytes:
        errors.append(f"desktop onefile is {size} bytes; hard limit is {max_bytes} bytes")
    return errors + _check_analysis(analysis)


def _module_path(relative: str) -> str:
    parts = relative.split("/")
    if parts[0] == "_internal" and len(parts) > 1:
        return "/".join(parts[:2])
    return parts[0]


def _link_or_junction(path: Path) -> bool:
    return path.is_symlink() or path.is_junction()


def _restore_output(destination: Path, backup: Path | None) -> None:
    if backup is None:
        destination.unlink(missing_ok=True)
    else:
        os.replace(backup, destination)


def _publish_pair(outputs: tuple[tuple[Path, Path], tuple[Path, Path]]) -> None:
    """Replace both outputs, restoring the previous pair if either publish fails."""

    backups: dict[Path, Path] = {}
    reserved_backups: list[Path] = []
    preserved_backups: set[Path] = set()
    try:
        for _source, destination in outputs:
            if destination.exists():
                with tempfile.NamedTemporaryFile(
                    prefix=f".{destination.name}.",
                    suffix=".bak",
                    dir=destination.parent,
                    delete=False,
                ) as raw_backup:
                    backup = Path(raw_backup.name)
                reserved_backups.append(backup)
                os.replace(destination, backup)
                backups[destination] = backup
        for source, destination in outputs:
            os.replace(source, destination)
    except OSError as publish_error:
        rollback_failures: list[Path] = []
        for _source, destination in outputs:
            owned_backup = backups.get(destination)
            try:
                _restore_output(destination, owned_backup)
            except OSError:
                rollback_failures.append(destination)
                if owned_backup is not None:
                    preserved_backups.add(owned_backup)
        if rollback_failures:
            failed = ", ".join(str(path) for path in rollback_failures)
            recovery = ", ".join(
                sorted(str(path) for path in preserved_backups)
            ) or "none"
            raise RuntimeError(
                f"desktop onedir publish failed: {publish_error}; "
                f"rollback incomplete for: {failed}; "
                f"preserved recovery backups: {recovery}"
            ) from publish_error
        raise
    finally:
        for backup in set(reserved_backups).difference(preserved_backups):
            backup.unlink(missing_ok=True)


def package_onedir(  # noqa: C901, RUF100 -- security validation and transactional publication share one artifact fence
    application: Path,
    archive: Path,
    report_path: Path,
    *,
    analysis: Path | None = None,
    zip_target_bytes: int | None = None,
    zip_max_bytes: int | None = None,
    onedir_max_bytes: int | None = None,
    baseline_zip_bytes: int = V1_15_4_ZIP_BYTES,
    attribution_growth_bytes: int = GROWTH_ATTRIBUTION_BYTES,
    build_sha: str = "",
) -> dict[str, object]:
    """Package one immutable application directory and report its real bytes."""

    if _link_or_junction(application):
        return {
            "errors": [f"desktop onedir root must not be a link or junction: {application}"],
        }
    try:
        application = application.resolve(strict=True)
    except OSError as exc:
        return {"errors": [f"desktop onedir root cannot be resolved: {exc}"]}
    if not application.is_dir():
        return {
            "errors": [f"desktop artifact must be an onedir application: {application}"],
        }
    root_name = application.name
    if root_name != "QuantMaster":
        return {
            "errors": [
                f"desktop onedir root must normalize to QuantMaster: {application}"
            ],
        }
    launcher = application / "QuantMaster.exe"
    try:
        launcher_stat = launcher.lstat()
    except OSError:
        launcher_stat = None
    if (
        launcher_stat is None
        or _link_or_junction(launcher)
        or not stat.S_ISREG(launcher_stat.st_mode)
    ):
        return {
            "errors": [
                "desktop onedir requires a regular root-level QuantMaster.exe"
            ],
        }

    resolved_outputs: list[tuple[str, Path]] = []
    for label, output in (("archive", archive), ("report", report_path)):
        if _link_or_junction(output):
            return {
                "errors": [
                    f"desktop onedir {label} must not be a link or junction: {output}"
                ],
            }
        resolved = output.resolve()
        if resolved.is_relative_to(application):
            return {
                "errors": [
                    f"desktop onedir {label} must not be inside the application: {output}"
                ],
            }
        if output.exists() and not output.is_file():
            return {"errors": [f"desktop onedir {label} must be a regular file: {output}"]}
        if not resolved.parent.is_dir():
            return {
                "errors": [f"desktop onedir {label} parent does not exist: {output.parent}"],
            }
        resolved_outputs.append((label, resolved))
    if resolved_outputs[0][1] == resolved_outputs[1][1]:
        return {"errors": ["desktop onedir archive and report must be distinct"]}
    if archive.exists() and report_path.exists() and os.path.samefile(archive, report_path):
        return {"errors": ["desktop onedir archive and report must not alias"]}

    zip_target_bytes = (
        zip_target_bytes if zip_target_bytes is not None else ZIP_TARGET_MIB * 1024 * 1024
    )
    zip_max_bytes = (
        zip_max_bytes if zip_max_bytes is not None else ZIP_MAX_MIB * 1024 * 1024
    )
    onedir_max_bytes = (
        onedir_max_bytes if onedir_max_bytes is not None else ONEDIR_MAX_MIB * 1024 * 1024
    )

    files: list[tuple[Path, str]] = []
    for path in application.rglob("*"):
        try:
            relative_path = path.relative_to(application)
        except ValueError:
            return {
                "errors": [f"desktop onedir member resolves outside application: {path}"],
            }
        if relative_path.is_absolute() or any(
            part in {"", ".", ".."} for part in relative_path.parts
        ):
            return {"errors": [f"desktop onedir member path is unsafe: {path}"]}
        if _link_or_junction(path):
            return {
                "errors": [
                    f"desktop onedir member must not be a link or junction: {path}"
                ],
            }
        member_stat = path.lstat()
        if stat.S_ISREG(member_stat.st_mode):
            files.append((path, relative_path.as_posix()))
        elif not stat.S_ISDIR(member_stat.st_mode):
            return {"errors": [f"desktop onedir member must be regular: {path}"]}
    files.sort(key=lambda item: item[1])
    for label, output in (("archive", archive), ("report", report_path)):
        if output.exists():
            for path, _relative in files:
                if os.path.samefile(output, path):
                    return {
                        "errors": [
                            f"desktop onedir {label} aliases application member: {path}"
                        ],
                    }
    analysis_errors = _check_analysis(analysis)
    if analysis_errors:
        return {"errors": analysis_errors}

    archive_temp: Path | None = None
    report_temp: Path | None = None
    try:
        installed_sizes: dict[str, int] = {}
        with tempfile.NamedTemporaryFile(
            mode="w+b",
            prefix=f".{archive.name}.",
            suffix=".tmp",
            dir=archive.parent,
            delete=False,
        ) as raw_archive:
            archive_temp = Path(raw_archive.name)
            with ZipFile(raw_archive, "w", compression=ZIP_DEFLATED, compresslevel=9) as output:
                for path, relative in files:
                    before_open = path.lstat()
                    if _link_or_junction(path) or not stat.S_ISREG(before_open.st_mode):
                        return {
                            "errors": [
                                f"desktop onedir member changed before open: {path}"
                            ],
                        }
                    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(
                        os, "O_NOFOLLOW", 0,
                    )
                    try:
                        descriptor = os.open(path, flags)
                    except OSError as exc:
                        return {
                            "errors": [f"desktop onedir member cannot be opened: {path}: {exc}"],
                        }
                    try:
                        opened = os.fstat(descriptor)
                        if (
                            not stat.S_ISREG(opened.st_mode)
                            or not os.path.samestat(before_open, opened)
                        ):
                            return {
                                "errors": [
                                    f"desktop onedir member identity changed: {path}"
                                ],
                            }
                        installed_sizes[relative] = opened.st_size
                        member = ZipInfo(
                            f"{root_name}/{relative}",
                            date_time=(1980, 1, 1, 0, 0, 0),
                        )
                        member.compress_type = ZIP_DEFLATED
                        member.create_system = 3
                        executable = len(Path(relative).parts) == 1 and path.name in {
                            "QuantMaster", "QuantMaster.exe",
                        }
                        member.external_attr = (
                            (0o100755 if executable else 0o100644) << 16
                        )
                        with output.open(member, "w") as destination:
                            while chunk := os.read(descriptor, 1024 * 1024):
                                destination.write(chunk)
                    finally:
                        os.close(descriptor)
            raw_archive.flush()
            os.fsync(raw_archive.fileno())

        compressed_sizes: dict[str, int] = {}
        with ZipFile(archive_temp) as packaged:
            prefix = f"{root_name}/"
            for member in packaged.infolist():
                compressed_sizes[member.filename.removeprefix(prefix)] = member.compress_size

        attribution: dict[str, dict[str, int]] = defaultdict(
            lambda: {"installed_bytes": 0, "zip_bytes": 0, "files": 0},
        )
        for relative, installed_bytes in installed_sizes.items():
            item = attribution[_module_path(relative)]
            item["installed_bytes"] += installed_bytes
            item["zip_bytes"] += compressed_sizes[relative]
            item["files"] += 1
        modules = [
            {"path": path, **sizes}
            for path, sizes in sorted(
                attribution.items(),
                key=lambda item: (-item[1]["installed_bytes"], item[0]),
            )
        ]

        onedir_bytes = sum(installed_sizes.values())
        zip_bytes = archive_temp.stat().st_size
        static_bytes = sum(
            size for relative, size in installed_sizes.items()
            if relative.startswith("_internal/quantmaster/server/static/")
        )
        growth = zip_bytes - baseline_zip_bytes
        limit_failures: list[str] = []
        if onedir_bytes > onedir_max_bytes:
            limit_failures.append(
                f"onedir is {onedir_bytes} bytes; hard limit is {onedir_max_bytes} bytes"
            )
        if zip_bytes > zip_max_bytes:
            limit_failures.append(
                f"ZIP is {zip_bytes} bytes; hard limit is {zip_max_bytes} bytes"
            )

        report: dict[str, object] = {
            "mode": "onedir-measurement",
            "build_sha": build_sha,
            "onedir_bytes": onedir_bytes,
            "onedir_hard_limit_bytes": onedir_max_bytes,
            "zip_bytes": zip_bytes,
            "zip_target_bytes": zip_target_bytes,
            "zip_hard_limit_bytes": zip_max_bytes,
            "within_zip_target": zip_bytes <= zip_target_bytes,
            "within_hard_limits": not limit_failures,
            "v1_15_4_zip_baseline_bytes": baseline_zip_bytes,
            "zip_growth_bytes": growth,
            "growth_attribution_threshold_bytes": attribution_growth_bytes,
            "growth_attribution_required": growth > attribution_growth_bytes,
            "static_bytes": static_bytes,
            "module_attribution": modules,
            "limit_failures": limit_failures,
            "errors": [],
        }
        with tempfile.NamedTemporaryFile(
            mode="w+b",
            prefix=f".{report_path.name}.",
            suffix=".tmp",
            dir=report_path.parent,
            delete=False,
        ) as raw_report:
            report_temp = Path(raw_report.name)
            raw_report.write(
                (json.dumps(report, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
            )
            raw_report.flush()
            os.fsync(raw_report.fileno())

        assert archive_temp is not None and report_temp is not None
        _publish_pair(((archive_temp, archive), (report_temp, report_path)))
        return report
    finally:
        for temporary in (archive_temp, report_temp):
            if temporary is not None:
                temporary.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("application", type=Path)
    parser.add_argument("--analysis", type=Path)
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--report", type=Path)
    parser.add_argument(
        "--experimental-onedir",
        action="store_true",
        help="write a deterministic onedir ZIP and size report without enforcing its budgets",
    )
    args = parser.parse_args()
    if not args.experimental_onedir:
        errors = check_onefile(args.application, analysis=args.analysis)
        if errors:
            for error in errors:
                print(f"[desktop-artifact] {error}")
            return 1
        print(
            f"[desktop-artifact] ok: {args.application} "
            f"({args.application.stat().st_size / (1024 * 1024):.1f} MiB)"
        )
        return 0
    archive = args.archive or args.application.with_suffix(".zip")
    report_path = args.report or args.application.with_suffix(".sizes.json")
    report = package_onedir(
        args.application,
        archive,
        report_path,
        analysis=args.analysis,
        build_sha=packaged_build_sha(Path.cwd()),
    )
    errors = list(report["errors"])
    if errors:
        for error in errors:
            print(f"[desktop-artifact] {error}")
        return 1
    for failure in report["limit_failures"]:
        print(f"[desktop-artifact] measurement exceeds budget: {failure}")
    target = "within target" if report["within_zip_target"] else "above target"
    print(
        f"[desktop-artifact] ok: {archive} "
        f"({int(report['zip_bytes']) / (1024 * 1024):.1f} MiB, {target})"
    )
    print(f"[desktop-artifact] size report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
