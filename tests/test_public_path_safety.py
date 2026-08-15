from __future__ import annotations

import pytest

from scripts.ci.sanitize_reports import scrub_file
from scripts.dev.github_sync import assert_public_github_body


def test_github_body_rejects_local_paths_before_publish():
    with pytest.raises(ValueError, match="本地路径"):
        assert_public_github_body(r"CI failed at C:\Users\example\Quant\tests\test.py")


def test_github_body_accepts_public_task_identifiers():
    assert_public_github_body("Task no-github-local-paths; branch codex/no-github-local-paths")


def test_report_scrubber_rewrites_local_paths(tmp_path):
    report = tmp_path / "report.xml"
    report.write_text(
        r'<failure message="C:\Users\example\Quant\tests\test.py" />',
        encoding="utf-8",
    )

    assert scrub_file(report) is True
    assert report.read_text(encoding="utf-8") == '<failure message="<local-path>" />'
    assert scrub_file(report) is False
