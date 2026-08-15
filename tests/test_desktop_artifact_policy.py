from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from zipfile import ZipFile

import pytest

from scripts.release import check_desktop_artifact as policy
from scripts.release.check_desktop_artifact import check_onefile, package_onedir


def _fake_clean_git(
    monkeypatch,
    *,
    untracked: str = "",
    tracked_change: str = "",
) -> tuple[str, list[list[str]]]:
    head = "a" * 40
    calls: list[list[str]] = []

    def run(command, **_kwargs):
        calls.append(command)
        if command[1] == "diff-index":
            inputs = command[command.index("--") + 1 :]
            changed = bool(tracked_change) and any(
                tracked_change == root or tracked_change.startswith(f"{root}/")
                for root in inputs
            )
            return SimpleNamespace(returncode=int(changed), stdout="")
        if command[1] == "ls-files":
            inputs = command[command.index("--") + 1 :]
            matching = [
                path
                for path in untracked.splitlines()
                if any(path == root or path.startswith(f"{root}/") for root in inputs)
            ]
            return SimpleNamespace(returncode=0, stdout="\n".join(matching))
        return SimpleNamespace(returncode=0, stdout=f"{head}\n")

    monkeypatch.setattr(policy.subprocess, "run", run)
    return head, calls


def test_packaged_build_identity_allows_untracked_docs(tmp_path: Path, monkeypatch) -> None:
    from scripts.release.check_desktop_artifact import packaged_build_sha

    head, calls = _fake_clean_git(monkeypatch)
    root = tmp_path / "repository"

    assert packaged_build_sha(root) == head
    assert policy.PACKAGED_INPUT_PATHS == (
        "quantmaster",
        "packaging",
        "scripts/release/check_desktop_artifact.py",
    )
    assert calls[0] == [
        "git", "diff-index", "--quiet", "HEAD", "--",
        *policy.PACKAGED_INPUT_PATHS,
    ]
    assert calls[1] == [
        "git", "ls-files", "--others", "--exclude-standard", "--",
        *policy.PACKAGED_INPUT_PATHS,
    ]


@pytest.mark.parametrize(
    "relative_path",
    [
        "quantmaster/experimental.py",
        "quantmaster/server/static/experimental.js",
        "packaging/entry.py",
        "packaging/quantmaster-splash.png",
    ],
)
def test_packaged_build_identity_rejects_untracked_inputs(
    tmp_path: Path,
    monkeypatch,
    relative_path: str,
) -> None:
    from scripts.release.check_desktop_artifact import packaged_build_sha

    _fake_clean_git(monkeypatch, untracked=f"{relative_path}\n")

    with pytest.raises(RuntimeError, match="untracked build input"):
        packaged_build_sha(tmp_path / "repository")


def test_packaged_build_identity_rejects_dirty_artifact_policy_script(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from scripts.release.check_desktop_artifact import packaged_build_sha

    _fake_clean_git(
        monkeypatch,
        tracked_change="scripts/release/check_desktop_artifact.py",
    )

    with pytest.raises(RuntimeError, match="tracked Git tree"):
        packaged_build_sha(tmp_path / "repository")


def test_packaged_build_identity_rejects_policy_script_removed_from_head_but_left_untracked(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from scripts.release.check_desktop_artifact import packaged_build_sha

    _fake_clean_git(
        monkeypatch,
        untracked="scripts/release/check_desktop_artifact.py\n",
    )

    with pytest.raises(RuntimeError, match="untracked build input"):
        packaged_build_sha(tmp_path / "repository")


def _fake_onedir(tmp_path: Path) -> Path:
    application = tmp_path / "QuantMaster"
    (application / "_internal" / "numpy").mkdir(parents=True)
    (application / "_internal" / "quantmaster" / "server" / "static").mkdir(
        parents=True,
    )
    (application / "QuantMaster.exe").write_bytes(b"launcher")
    (application / "_internal" / "numpy" / "core.pyd").write_bytes(b"numpy")
    (
        application / "_internal" / "quantmaster" / "server" / "static" / "app.js"
    ).write_bytes(b"static")
    return application


def test_onedir_policy_writes_a_deterministic_zip_and_real_file_attribution(
    tmp_path: Path,
) -> None:
    application = _fake_onedir(tmp_path)
    first = tmp_path / "first.zip"
    second = tmp_path / "second.zip"

    build_sha = "a" * 40
    first_report = package_onedir(
        application, first, tmp_path / "first.json", build_sha=build_sha,
    )
    second_report = package_onedir(
        application, second, tmp_path / "second.json", build_sha=build_sha,
    )

    assert first.read_bytes() == second.read_bytes()
    with ZipFile(first) as archive:
        members = archive.infolist()
    assert [member.filename for member in members] == sorted(
        member.filename for member in members
    )
    assert all(member.date_time == (1980, 1, 1, 0, 0, 0) for member in members)
    assert members[0].filename.startswith("QuantMaster/")
    assert first_report == second_report
    assert first_report["mode"] == "onedir-measurement"
    assert first_report["build_sha"] == build_sha
    assert first_report["static_bytes"] == len(b"static")
    assert "static_limit_bytes" not in first_report
    numpy = next(
        item for item in first_report["module_attribution"]
        if item["path"] == "_internal/numpy"
    )
    assert numpy["installed_bytes"] == len(b"numpy")
    assert numpy["files"] == 1


def test_onedir_policy_reports_targets_and_limits_separately(
    tmp_path: Path,
) -> None:
    application = _fake_onedir(tmp_path)
    report_path = tmp_path / "QuantMaster.sizes.json"

    target_only = package_onedir(
        application,
        tmp_path / "target-only.zip",
        report_path,
        zip_target_bytes=0,
        zip_max_bytes=10_000,
        onedir_max_bytes=10_000,
        baseline_zip_bytes=0,
        attribution_growth_bytes=0,
    )

    assert target_only["within_zip_target"] is False
    assert target_only["growth_attribution_required"] is True
    assert target_only["limit_failures"] == []
    assert target_only["errors"] == []
    assert report_path.read_text(encoding="utf-8").endswith("\n")

    hard_failure = package_onedir(
        application,
        tmp_path / "hard-failure.zip",
        report_path,
        zip_target_bytes=0,
        zip_max_bytes=0,
        onedir_max_bytes=0,
    )

    assert any(
        "ZIP" in error and "hard limit" in error
        for error in hard_failure["limit_failures"]
    )
    assert any(
        "onedir" in error and "hard limit" in error
        for error in hard_failure["limit_failures"]
    )
    assert hard_failure["errors"] == []


def test_onedir_policy_rejects_a_onefile_artifact(tmp_path: Path) -> None:
    executable = tmp_path / "QuantMaster.exe"
    executable.write_bytes(b"onefile")

    report = package_onedir(executable, tmp_path / "QuantMaster.zip", tmp_path / "sizes.json")

    assert report["errors"] == [f"desktop artifact must be an onedir application: {executable}"]
    assert not (tmp_path / "QuantMaster.zip").exists()


def test_onedir_policy_rejects_unsafe_normalized_root_before_writing(
    tmp_path: Path,
) -> None:
    application = tmp_path / "build" / "QuantMaster"
    application.mkdir(parents=True)
    (application / "QuantMaster.exe").write_bytes(b"launcher")
    unsafe_root = application / ".."
    archive = tmp_path / "QuantMaster.zip"
    report_path = tmp_path / "QuantMaster.sizes.json"

    report = package_onedir(unsafe_root, archive, report_path)

    assert report["errors"]
    assert "root" in report["errors"][0]
    assert not archive.exists()
    assert not report_path.exists()


def test_onedir_policy_rejects_file_symlinks_before_writing(tmp_path: Path) -> None:
    application = _fake_onedir(tmp_path)
    outside = tmp_path / "outside.bin"
    outside.write_bytes(b"outside")
    link = application / "_internal" / "outside.bin"
    try:
        link.symlink_to(outside)
    except OSError as exc:
        pytest.skip(f"file symlink unavailable: {exc}")
    archive = tmp_path / "QuantMaster.zip"
    report_path = tmp_path / "QuantMaster.sizes.json"

    report = package_onedir(application, archive, report_path)

    assert report["errors"]
    assert "must not be a link" in report["errors"][0]
    assert not archive.exists()
    assert not report_path.exists()


def test_onedir_policy_checks_every_member_for_symlinks(
    tmp_path: Path,
    monkeypatch,
) -> None:
    application = _fake_onedir(tmp_path).resolve()
    flagged = application / "_internal" / "numpy" / "core.pyd"
    original_is_symlink = Path.is_symlink

    def is_symlink(path: Path) -> bool:
        return path == flagged or original_is_symlink(path)

    monkeypatch.setattr(Path, "is_symlink", is_symlink)
    archive = tmp_path / "QuantMaster.zip"
    report_path = tmp_path / "QuantMaster.sizes.json"

    report = package_onedir(application, archive, report_path)

    assert report["errors"] == [
        f"desktop onedir member must not be a link or junction: {flagged}"
    ]
    assert not archive.exists()
    assert not report_path.exists()


def test_onedir_policy_rejects_members_outside_the_application(
    tmp_path: Path,
    monkeypatch,
) -> None:
    application = _fake_onedir(tmp_path).resolve()
    outside = tmp_path / "outside.bin"
    outside.write_bytes(b"outside")
    original_rglob = Path.rglob

    def unsafe_rglob(path: Path, pattern: str):
        if path == application:
            return iter([outside])
        return original_rglob(path, pattern)

    monkeypatch.setattr(Path, "rglob", unsafe_rglob)
    archive = tmp_path / "QuantMaster.zip"
    report_path = tmp_path / "QuantMaster.sizes.json"

    report = package_onedir(application, archive, report_path)

    assert report["errors"]
    assert "outside" in report["errors"][0]
    assert not archive.exists()
    assert not report_path.exists()


@pytest.mark.parametrize("output", ["archive", "report"])
def test_onedir_policy_rejects_outputs_inside_the_application(
    tmp_path: Path,
    output: str,
) -> None:
    application = _fake_onedir(tmp_path)
    archive = tmp_path / "QuantMaster.zip"
    report_path = tmp_path / "QuantMaster.sizes.json"
    if output == "archive":
        archive = application / "QuantMaster.zip"
    else:
        report_path = application / "QuantMaster.sizes.json"

    report = package_onedir(application, archive, report_path)

    assert report["errors"]
    assert "inside" in report["errors"][0]
    assert not archive.exists()
    assert not report_path.exists()


@pytest.mark.parametrize("output_name", ["archive", "report"])
def test_onedir_policy_rejects_output_hardlinks_to_members_without_mutation(
    tmp_path: Path,
    output_name: str,
) -> None:
    application = _fake_onedir(tmp_path)
    launcher = application / "QuantMaster.exe"
    original = launcher.read_bytes()
    archive = tmp_path / "QuantMaster.zip"
    report_path = tmp_path / "QuantMaster.sizes.json"
    alias = archive if output_name == "archive" else report_path
    try:
        os.link(launcher, alias)
    except OSError as exc:
        pytest.skip(f"hardlinks unavailable: {exc}")

    report = package_onedir(application, archive, report_path)

    assert report["errors"]
    assert "aliases application member" in report["errors"][0]
    assert launcher.read_bytes() == original
    assert alias.read_bytes() == original
    other = report_path if output_name == "archive" else archive
    assert not other.exists()


def test_onedir_policy_rejects_interlinked_outputs_without_mutation(
    tmp_path: Path,
) -> None:
    application = _fake_onedir(tmp_path)
    archive = tmp_path / "QuantMaster.zip"
    report_path = tmp_path / "QuantMaster.sizes.json"
    archive.write_bytes(b"sentinel")
    try:
        os.link(archive, report_path)
    except OSError as exc:
        pytest.skip(f"hardlinks unavailable: {exc}")

    report = package_onedir(application, archive, report_path)

    assert report["errors"] == ["desktop onedir archive and report must not alias"]
    assert archive.read_bytes() == b"sentinel"
    assert report_path.read_bytes() == b"sentinel"


def test_onedir_policy_keeps_published_outputs_unchanged_when_report_build_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    application = _fake_onedir(tmp_path)
    archive = tmp_path / "QuantMaster.zip"
    report_path = tmp_path / "QuantMaster.sizes.json"
    archive.write_bytes(b"old archive")
    report_path.write_bytes(b"old report")

    def fail_json(*_args, **_kwargs):
        raise RuntimeError("report failed")

    monkeypatch.setattr(policy.json, "dumps", fail_json)

    with pytest.raises(RuntimeError, match="report failed"):
        package_onedir(application, archive, report_path)

    assert archive.read_bytes() == b"old archive"
    assert report_path.read_bytes() == b"old report"
    assert not list(tmp_path.glob("*.tmp"))


@pytest.mark.parametrize("preexisting", [False, True])
def test_onedir_policy_rolls_back_both_outputs_when_report_publish_fails(
    tmp_path: Path,
    monkeypatch,
    preexisting: bool,
) -> None:
    application = _fake_onedir(tmp_path)
    archive = tmp_path / "QuantMaster.zip"
    report_path = tmp_path / "QuantMaster.sizes.json"
    if preexisting:
        archive.write_bytes(b"old archive")
        report_path.write_bytes(b"old report")
    real_replace = policy.os.replace

    def fail_report_publish(source, destination):
        if Path(destination) == report_path and Path(source).suffix == ".tmp":
            raise OSError("report publish failed")
        return real_replace(source, destination)

    monkeypatch.setattr(policy.os, "replace", fail_report_publish)

    with pytest.raises(OSError, match="report publish failed"):
        package_onedir(application, archive, report_path)

    if preexisting:
        assert archive.read_bytes() == b"old archive"
        assert report_path.read_bytes() == b"old report"
    else:
        assert not archive.exists()
        assert not report_path.exists()
    assert not list(tmp_path.glob(".*.tmp"))
    assert not list(tmp_path.glob(".*.bak"))


def test_onedir_policy_preserves_old_backup_when_rollback_is_locked(
    tmp_path: Path,
    monkeypatch,
) -> None:
    application = _fake_onedir(tmp_path)
    archive = tmp_path / "QuantMaster.zip"
    report_path = tmp_path / "QuantMaster.sizes.json"
    archive.write_bytes(b"old archive")
    report_path.write_bytes(b"old report")
    real_replace = policy.os.replace

    def fail_publish_and_archive_restore(source, destination):
        source = Path(source)
        destination = Path(destination)
        if destination == report_path and source.suffix == ".tmp":
            raise OSError("report publish failed")
        if destination == archive and source.suffix == ".bak":
            raise PermissionError("archive restore locked")
        return real_replace(source, destination)

    monkeypatch.setattr(policy.os, "replace", fail_publish_and_archive_restore)

    with pytest.raises(RuntimeError) as raised:
        package_onedir(application, archive, report_path)

    backups = list(tmp_path.glob(".QuantMaster.zip.*.bak"))
    assert len(backups) == 1
    assert backups[0].read_bytes() == b"old archive"
    assert str(backups[0]) in str(raised.value)
    assert "report publish failed" in str(raised.value)
    assert "rollback incomplete" in str(raised.value)
    assert report_path.read_bytes() == b"old report"
    assert archive.read_bytes() != b"old archive"
    assert not list(tmp_path.glob(".QuantMaster.sizes.json.*.bak"))
    assert not list(tmp_path.glob(".*.tmp"))


def test_onedir_policy_rejects_member_identity_change_before_publish(
    tmp_path: Path,
    monkeypatch,
) -> None:
    application = _fake_onedir(tmp_path)
    archive = tmp_path / "QuantMaster.zip"
    report_path = tmp_path / "QuantMaster.sizes.json"
    monkeypatch.setattr(policy.os.path, "samestat", lambda _before, _opened: False)

    report = package_onedir(application, archive, report_path)

    assert report["errors"]
    assert "identity changed" in report["errors"][0]
    assert not archive.exists()
    assert not report_path.exists()


def test_onedir_policy_fstats_and_reads_each_member_from_the_opened_descriptor(
    tmp_path: Path,
    monkeypatch,
) -> None:
    application = _fake_onedir(tmp_path).resolve()
    archive = tmp_path / "QuantMaster.zip"
    report_path = tmp_path / "QuantMaster.sizes.json"
    real_open = policy.os.open
    real_fstat = policy.os.fstat
    real_read = policy.os.read
    member_descriptors: set[int] = set()
    fstat_descriptors: set[int] = set()
    read_descriptors: set[int] = set()
    member_flags: list[int] = []

    def open_file(path, flags, *args, **kwargs):
        descriptor = real_open(path, flags, *args, **kwargs)
        if Path(path).is_relative_to(application):
            member_descriptors.add(descriptor)
            member_flags.append(flags)
        return descriptor

    def fstat(descriptor):
        if descriptor in member_descriptors:
            fstat_descriptors.add(descriptor)
        return real_fstat(descriptor)

    def read(descriptor, size):
        if descriptor in member_descriptors:
            read_descriptors.add(descriptor)
        return real_read(descriptor, size)

    monkeypatch.setattr(policy.os, "open", open_file)
    monkeypatch.setattr(policy.os, "fstat", fstat)
    monkeypatch.setattr(policy.os, "read", read)

    report = package_onedir(application, archive, report_path)

    assert report["errors"] == []
    assert member_descriptors == fstat_descriptors == read_descriptors
    if hasattr(policy.os, "O_NOFOLLOW"):
        assert all(flags & policy.os.O_NOFOLLOW for flags in member_flags)


@pytest.mark.parametrize("location", ["root", "member", "archive", "report"])
def test_onedir_policy_rejects_portable_junction_contract(
    tmp_path: Path,
    monkeypatch,
    location: str,
) -> None:
    application = _fake_onedir(tmp_path).resolve()
    archive = tmp_path / "QuantMaster.zip"
    report_path = tmp_path / "QuantMaster.sizes.json"
    flagged = {
        "root": application,
        "member": application / "_internal" / "numpy",
        "archive": archive,
        "report": report_path,
    }[location]
    original_is_junction = Path.is_junction

    def is_junction(path: Path) -> bool:
        return path == flagged or original_is_junction(path)

    monkeypatch.setattr(Path, "is_junction", is_junction)

    report = package_onedir(application, archive, report_path)

    assert report["errors"]
    assert "link or junction" in report["errors"][0]
    assert not archive.exists()
    assert not report_path.exists()


@pytest.mark.parametrize("launcher_kind", ["missing", "directory"])
def test_onedir_policy_requires_root_regular_launcher(
    tmp_path: Path,
    launcher_kind: str,
) -> None:
    application = _fake_onedir(tmp_path)
    launcher = application / "QuantMaster.exe"
    launcher.unlink()
    if launcher_kind == "directory":
        launcher.mkdir()
    archive = tmp_path / "QuantMaster.zip"
    report_path = tmp_path / "QuantMaster.sizes.json"

    report = package_onedir(application, archive, report_path)

    assert report["errors"]
    assert "regular root-level QuantMaster.exe" in report["errors"][0]
    assert not archive.exists()
    assert not report_path.exists()


@pytest.mark.skipif(os.name != "nt", reason="Windows junction contract")
def test_onedir_policy_rejects_real_windows_root_junction(tmp_path: Path) -> None:
    target_root = tmp_path / "target"
    target = _fake_onedir(target_root)
    junction = tmp_path / "QuantMaster"
    result = subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(junction), str(target)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode:
        pytest.skip(f"junction unavailable: {result.stderr or result.stdout}")
    assert junction.is_junction()
    archive = tmp_path / "QuantMaster.zip"
    report_path = tmp_path / "QuantMaster.sizes.json"

    report = package_onedir(junction, archive, report_path)

    assert report["errors"]
    assert "link or junction" in report["errors"][0]
    assert not archive.exists()
    assert not report_path.exists()


def test_onedir_policy_rejects_forbidden_analysis_modules(tmp_path: Path) -> None:
    application = _fake_onedir(tmp_path)
    analysis = tmp_path / "Analysis-00.toc"
    analysis.write_text("  ('torch.linalg', 'torch/linalg.py', 'PYMODULE'),\n", encoding="utf-8")

    report = package_onedir(
        application,
        tmp_path / "QuantMaster.zip",
        tmp_path / "sizes.json",
        analysis=analysis,
    )

    assert report["errors"] == ["forbidden optional modules were bundled: torch.linalg"]


def test_experimental_onedir_cli_reports_oversize_without_failing(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    application = _fake_onedir(tmp_path)
    archive = tmp_path / "QuantMaster.zip"
    report = tmp_path / "QuantMaster.sizes.json"
    monkeypatch.setattr(policy, "packaged_build_sha", lambda _root: "a" * 40)
    monkeypatch.setattr(
        policy,
        "ZIP_MAX_MIB",
        0,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "check_desktop_artifact.py",
            str(application),
            "--experimental-onedir",
            "--archive",
            str(archive),
            "--report",
            str(report),
        ],
    )

    assert policy.main() == 0
    payload = policy.json.loads(report.read_text(encoding="utf-8"))
    assert payload["mode"] == "onedir-measurement"
    assert payload["within_hard_limits"] is False
    assert payload["limit_failures"]
    assert "measurement exceeds budget" in capsys.readouterr().out


def test_default_artifact_cli_checks_onefile_without_creating_an_archive(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    artifact = tmp_path / "QuantMaster.exe"
    artifact.write_bytes(b"onefile")
    monkeypatch.setattr(sys, "argv", ["check_desktop_artifact.py", str(artifact)])

    assert policy.main() == 0
    assert "ok:" in capsys.readouterr().out
    assert not artifact.with_suffix(".zip").exists()
    assert not artifact.with_suffix(".sizes.json").exists()


def test_posix_onefile_policy_checks_size_and_analysis_without_an_archive(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "QuantMaster"
    artifact.write_bytes(b"onefile")
    analysis = tmp_path / "Analysis-00.toc"
    analysis.write_text("  ('torch.linalg', 'torch/linalg.py', 'PYMODULE'),\n", encoding="utf-8")

    errors = check_onefile(artifact, analysis=analysis, max_bytes=0)

    assert errors == [
        f"desktop onefile is {len(b'onefile')} bytes; hard limit is 0 bytes",
        "forbidden optional modules were bundled: torch.linalg",
    ]
    assert not (tmp_path / "QuantMaster.zip").exists()
