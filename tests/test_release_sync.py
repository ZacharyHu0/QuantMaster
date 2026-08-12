"""Release metadata and automatic GitHub synchronization guard."""

import json
import subprocess
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from scripts.release.sync import (
    CHANGELOG_FILE,
    RELEASE_FILE,
    ci_recovery_errors,
    github_https_push_url,
    is_next_patch,
    post_commit,
    pre_commit,
    push_config_variants,
    release_assignments,
    release_today,
    replace_failed_release,
    run_git,
    validate_metadata,
    verify_previous_release_tag,
    version_tuple,
)

ROOT = Path(__file__).resolve().parents[1]


def valid_sources() -> tuple[str, str]:
    release = '''VERSION = "1.2.3"
RELEASE_DATE = "2026-07-27"
RELEASES = ({"version": VERSION, "date": RELEASE_DATE, "sections": ()},)
'''
    changelog = """# Changelog

## v1.2.3（2026-07-27）

### 发布同步
- 自动推送 main
"""
    return release, changelog


def test_repository_release_metadata_is_consistent():
    errors = validate_metadata(
        (ROOT / RELEASE_FILE).read_text(encoding="utf-8"),
        (ROOT / CHANGELOG_FILE).read_text(encoding="utf-8"),
        today=release_today(),
    )
    assert errors == []


def test_validate_metadata_accepts_matching_release():
    release, changelog = valid_sources()
    assert validate_metadata(release, changelog, today=date(2026, 7, 27)) == []
    assert release_assignments(release) == {
        "VERSION": "1.2.3",
        "RELEASE_DATE": "2026-07-27",
    }


def test_validate_metadata_reports_mismatch_and_stale_date():
    release, changelog = valid_sources()
    changelog = changelog.replace("v1.2.3", "v1.2.2")
    errors = validate_metadata(release, changelog, today=date(2026, 7, 28))
    assert any("实际发布日期" in error for error in errors)
    assert any("顶部版本" in error for error in errors)


def test_historical_metadata_allows_past_date_but_rejects_future_date():
    release, changelog = valid_sources()
    assert validate_metadata(
        release, changelog, today=date(2026, 7, 28), require_today=False,
    ) == []
    errors = validate_metadata(
        release, changelog, today=date(2026, 7, 26), require_today=False,
    )
    assert any("不得晚于" in error for error in errors)


def test_previous_release_tag_must_point_to_head(monkeypatch):
    from scripts.release import sync as release_sync

    monkeypatch.setattr(release_sync, "current_branch", lambda: "main")
    values = {("rev-list", "-n", "1", "v1.2.3"): "abc", ("rev-parse", "HEAD"): "def"}
    monkeypatch.setattr(release_sync, "git_text", lambda args, required=True: values[tuple(args)])
    assert "未指向" in verify_previous_release_tag("1.2.3")[0]


def test_exact_ci_failure_recovery_allows_missing_previous_tag(monkeypatch):
    from scripts.release import sync as release_sync

    monkeypatch.setattr(release_sync, "current_branch", lambda: "main")
    values = {("rev-list", "-n", "1", "v1.2.3"): "", ("rev-parse", "HEAD"): "abc"}
    monkeypatch.setattr(release_sync, "git_text", lambda args, required=True: values[tuple(args)])
    monkeypatch.setattr(
        release_sync,
        "read_ci_recovery",
        lambda: ({"version": "1.2.3", "commit": "abc", "run_id": 12345}, ""),
    )
    assert verify_previous_release_tag("1.2.3") == []


def test_ci_failure_recovery_rejects_mismatched_commit(monkeypatch):
    from scripts.release import sync as release_sync

    monkeypatch.setattr(
        release_sync,
        "read_ci_recovery",
        lambda: ({"version": "1.2.3", "commit": "old", "run_id": 12345}, ""),
    )
    recovered, errors = ci_recovery_errors("1.2.3", "new")
    assert recovered is False
    assert "提交不匹配" in errors[0]


def test_ci_failure_recovery_only_allows_next_patch():
    assert is_next_patch("1.2.3", "1.2.4") is True
    assert is_next_patch("1.2.3", "1.2.5") is False
    assert is_next_patch("1.2.3", "1.3.0") is False


def test_same_version_replacement_requires_exact_failed_run(monkeypatch):
    from scripts.release import sync as release_sync

    monkeypatch.setattr(release_sync, "committed_release_errors", lambda: ("1.2.3", []))
    monkeypatch.setattr(release_sync, "current_branch", lambda: "main")
    monkeypatch.setattr(release_sync, "verify_previous_release_synced", list)
    monkeypatch.setattr(
        release_sync,
        "read_ci_recovery",
        lambda: ({
            "mode": "replace", "version": "1.2.3", "commit": "old",
            "tag_target": "old", "run_id": 42,
        }, ""),
    )
    values = {
        ("status", "--porcelain"): "",
        ("rev-list", "-n", "1", "v1.2.3"): "old",
        ("rev-parse", "HEAD"): "new",
    }
    monkeypatch.setattr(
        release_sync, "git_text", lambda args, required=True: values[tuple(args)],
    )
    monkeypatch.setattr(
        release_sync, "run_git",
        lambda args, **kwargs: subprocess.CompletedProcess(args, 0, "", ""),
    )
    monkeypatch.setattr(
        release_sync, "_failed_run_matches",
        lambda run_id, commit: ["run evidence mismatch"],
    )
    assert replace_failed_release() == 1


def test_same_version_authorization_binds_tagged_failed_commit(monkeypatch, tmp_path):
    from scripts.release import sync as release_sync

    monkeypatch.setattr(release_sync, "current_branch", lambda: "main")
    monkeypatch.setattr(release_sync, "committed_release_errors", lambda: ("1.2.3", []))
    monkeypatch.setattr(release_sync, "verify_previous_release_synced", list)
    values = {
        ("rev-parse", "HEAD"): "fixed",
        ("rev-list", "-n", "1", "v1.2.3"): "failed",
    }
    monkeypatch.setattr(
        release_sync, "git_text", lambda args, required=True: values[tuple(args)],
    )
    monkeypatch.setattr(
        release_sync, "run_git",
        lambda args, **kwargs: subprocess.CompletedProcess(args, 0, "", ""),
    )
    monkeypatch.setattr(release_sync, "_failed_run_matches", lambda run_id, commit: [])
    marker = tmp_path / "recovery.json"
    monkeypatch.setattr(release_sync, "ci_recovery_marker", lambda: marker)
    assert release_sync.authorize_ci_recovery(42, replace=True) == 0
    recovery = json.loads(marker.read_text(encoding="utf-8"))
    assert recovery["commit"] == "failed"
    assert recovery["tag_target"] == "failed"


def test_same_version_replacement_moves_only_authorized_tag(monkeypatch):
    from scripts.release import sync as release_sync

    monkeypatch.setattr(release_sync, "committed_release_errors", lambda: ("1.2.3", []))
    monkeypatch.setattr(release_sync, "current_branch", lambda: "main")
    monkeypatch.setattr(release_sync, "verify_previous_release_synced", list)
    monkeypatch.setattr(
        release_sync,
        "read_ci_recovery",
        lambda: ({
            "mode": "replace", "version": "1.2.3", "commit": "old",
            "tag_target": "old", "run_id": 42,
        }, ""),
    )
    values = {
        ("status", "--porcelain"): "",
        ("rev-list", "-n", "1", "v1.2.3"): "old",
        ("rev-parse", "HEAD"): "new",
    }
    monkeypatch.setattr(
        release_sync, "git_text", lambda args, required=True: values[tuple(args)],
    )
    calls = []
    monkeypatch.setattr(
        release_sync, "run_git",
        lambda args, **kwargs: calls.append(args) or subprocess.CompletedProcess(args, 0, "", ""),
    )
    monkeypatch.setattr(release_sync, "_failed_run_matches", lambda run_id, commit: [])
    monkeypatch.setattr(release_sync, "clear_ci_recovery", lambda: None)
    monkeypatch.setattr(
        release_sync.subprocess, "run",
        lambda args, **kwargs: subprocess.CompletedProcess(args, 0, "", ""),
    )
    assert replace_failed_release() == 0
    assert ["push", "--force", "origin", "refs/tags/v1.2.3"] in calls


def test_release_clock_uses_asia_shanghai_date_at_utc_boundary():
    assert release_today(datetime(2026, 7, 30, 22, tzinfo=UTC)) == date(
        2026, 7, 31,
    )


@pytest.mark.parametrize(
    ("left", "right"),
    [("1.2.3", (1, 2, 3)), ("10.0.12", (10, 0, 12))],
)
def test_version_tuple(left, right):
    assert version_tuple(left) == right


def test_version_tuple_rejects_non_semver():
    with pytest.raises(ValueError):
        version_tuple("1.2")


def test_push_config_prefers_valid_local_resolve_then_falls_back():
    variants = push_config_variants("github.com:443:140.82.114.4")
    assert ("http.curloptResolve", "github.com:443:140.82.114.4") in variants[0]
    assert ("credential.useHttpPath", "true") in variants[0]
    assert ("http.sslVerify", "true") in variants[0]
    assert all(key != "http.curloptResolve" for key, _ in variants[-1])


def test_push_config_ignores_invalid_resolve():
    variants = push_config_variants("example.com:443:127.0.0.1")
    assert len(variants) == 1
    assert all(key != "http.curloptResolve" for key, _ in variants[0])


def test_git_timeout_becomes_retryable_failure(monkeypatch):
    from scripts.release import sync as release_sync

    def expire(*args, **kwargs):
        raise subprocess.TimeoutExpired(args[0], kwargs["timeout"])

    monkeypatch.setattr(release_sync.subprocess, "run", expire)
    result = run_git(["push", "origin", "HEAD:main"], timeout_seconds=30)
    assert result.returncode == 124
    assert "timed out after 30 seconds" in result.stderr


def test_github_push_url_defaults_to_repository_owner():
    assert github_https_push_url("https://github.com/ZacharyHu0/QuantMaster.git") == (
        "https://ZacharyHu0@github.com/ZacharyHu0/QuantMaster.git"
    )


def test_github_push_url_accepts_explicit_account_and_rejects_ssh():
    assert github_https_push_url(
        "https://github.com/example/project", "release-bot",
    ) == "https://release-bot@github.com/example/project.git"
    assert github_https_push_url("git@github.com:example/project.git") == ""


def test_task_branch_commit_skips_release_gate(monkeypatch):
    from scripts.release import sync as release_sync

    monkeypatch.setattr(release_sync, "staged_paths", lambda: {"quantmaster/data/storage.py"})
    monkeypatch.setattr(release_sync, "current_branch", lambda: "codex/storage-fix")
    assert pre_commit() == 0


def test_main_regular_commit_skips_release_gate(monkeypatch):
    from scripts.release import sync as release_sync

    monkeypatch.setattr(release_sync, "staged_paths", lambda: {"quantmaster/data/storage.py"})
    monkeypatch.setattr(release_sync, "current_branch", lambda: "main")
    monkeypatch.setattr(
        release_sync, "verify_previous_release_synced",
        lambda: (_ for _ in ()).throw(AssertionError("ordinary commit reached release gate")),
    )
    assert pre_commit() == 0


def test_main_partial_release_metadata_is_rejected(monkeypatch):
    from scripts.release import sync as release_sync

    monkeypatch.setattr(release_sync, "staged_paths", lambda: {RELEASE_FILE})
    monkeypatch.setattr(release_sync, "current_branch", lambda: "main")
    assert pre_commit() == 1


def test_main_changelog_only_commit_is_allowed(monkeypatch):
    from scripts.release import sync as release_sync

    monkeypatch.setattr(release_sync, "staged_paths", lambda: {CHANGELOG_FILE})
    monkeypatch.setattr(release_sync, "current_branch", lambda: "main")
    assert pre_commit() == 0


def test_version_commit_does_not_require_a_tag_or_auto_push(monkeypatch):
    from scripts.release import sync as release_sync

    release, changelog = valid_sources()
    today = release_today().isoformat()
    release = release.replace("2026-07-27", today)
    changelog = changelog.replace("2026-07-27", today)
    previous = release.replace('VERSION = "1.2.3"', 'VERSION = "1.2.2"')
    monkeypatch.setattr(release_sync, "staged_paths", lambda: {RELEASE_FILE, CHANGELOG_FILE})
    monkeypatch.setattr(release_sync, "current_branch", lambda: "main")
    monkeypatch.setattr(
        release_sync, "read_staged",
        lambda path: release if path == RELEASE_FILE else changelog,
    )
    monkeypatch.setattr(release_sync, "read_committed", lambda path: previous)
    monkeypatch.setattr(release_sync, "run_local_ci", lambda: 0)
    monkeypatch.setattr(
        release_sync, "verify_previous_release_tag",
        lambda version: (_ for _ in ()).throw(AssertionError("tag gate called")),
    )
    assert pre_commit() == 0
    monkeypatch.setattr(
        release_sync, "push_current_release",
        lambda: (_ for _ in ()).throw(AssertionError("implicit push called")),
    )
    assert post_commit() == 0


@pytest.mark.parametrize("release_path", [RELEASE_FILE])
def test_task_branch_rejects_release_metadata(monkeypatch, release_path):
    from scripts.release import sync as release_sync

    monkeypatch.setattr(release_sync, "staged_paths", lambda: {release_path})
    monkeypatch.setattr(release_sync, "current_branch", lambda: "codex/storage-fix")
    assert pre_commit() == 1
