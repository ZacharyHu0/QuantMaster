"""Release metadata and automatic GitHub synchronization guard."""

from datetime import date
from pathlib import Path

import pytest

from tools.release_sync import (
    CHANGELOG_FILE,
    RELEASE_FILE,
    github_https_push_url,
    push_config_variants,
    release_assignments,
    validate_metadata,
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
        today=date(2026, 7, 28),
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


def test_github_push_url_defaults_to_repository_owner():
    assert github_https_push_url("https://github.com/ZacharyHu0/QuantMaster.git") == (
        "https://ZacharyHu0@github.com/ZacharyHu0/QuantMaster.git"
    )


def test_github_push_url_accepts_explicit_account_and_rejects_ssh():
    assert github_https_push_url(
        "https://github.com/example/project", "release-bot",
    ) == "https://release-bot@github.com/example/project.git"
    assert github_https_push_url("git@github.com:example/project.git") == ""
