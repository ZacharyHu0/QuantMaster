"""Release subpackage split contract: facade, history, validate, packaging."""

from __future__ import annotations

import pytest

from quantmaster.release import RELEASE_DATE, RELEASES, VERSION
from quantmaster.release.history import (
    RELEASE_HISTORY_URL,
    release_lookup,
    release_sections,
)
from quantmaster.release.packaging import packaged_version, packaged_version_tuple
from quantmaster.release.validate import (
    parse_semver,
    release_date,
    release_metadata_is_valid,
    validate_release_entry,
    validate_release_metadata,
)


def test_release_facade_exports_only_the_three_public_names():
    import quantmaster.release as release_module

    assert release_module.__all__ == ("RELEASES", "RELEASE_DATE", "VERSION")
    assert VERSION
    assert RELEASE_DATE
    assert len(RELEASES) >= 10


def test_release_history_lookup_wraps_the_facade_metadata():
    assert release_lookup(VERSION)["version"] == VERSION
    assert release_lookup(VERSION)["release_date"] == RELEASE_DATE
    assert release_lookup("a" * 40) == {
        "version": "",
        "release_date": "",
        "sha": "a" * 40,
    }
    sections = release_sections(VERSION)
    assert sections and sections[0]["title"]
    assert RELEASE_HISTORY_URL.startswith("https://github.com/")


def test_validate_release_metadata_accepts_the_installed_registry():
    assert validate_release_metadata() == []
    assert release_metadata_is_valid() == (True, [])


def test_validate_release_entry_rejects_malformed_shapes():
    assert validate_release_entry(None) == ["发布条目必须是 mapping"]
    assert validate_release_entry({}) != []
    malformed = {
        "version": "not-semver",
        "date": "not-a-date",
        "sections": ({"title": "", "items": ("",)},),
    }
    errors = validate_release_entry(malformed)
    assert any("VERSION" not in error and "语义版本" in error for error in errors)
    assert any("发布日期" in error for error in errors)
    assert any("title" in error for error in errors)


def test_validate_parsers_round_trip_and_reject():
    assert parse_semver("1.16.2") == (1, 16, 2)
    assert release_date("2026-08-16").isoformat() == "2026-08-16"
    with pytest.raises(ValueError):
        parse_semver("1.16")
    with pytest.raises(ValueError):
        release_date("08/16/2026")


def test_packaging_version_sources_the_runtime_version():
    assert packaged_version() == VERSION
    assert packaged_version_tuple() == (*tuple(int(part) for part in VERSION.split(".")), 0)
