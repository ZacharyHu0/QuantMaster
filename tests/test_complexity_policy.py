"""Owner-auditable complexity ratchet: ``complexity_policy.py``."""

from __future__ import annotations

import json
from pathlib import Path

from scripts.ci import complexity_policy

FAKE_FINDINGS = [
    {
        "filename": str(Path.cwd() / "quantmaster" / "a.py"),
        "location": {"row": 10, "column": 5},
        "message": "`foo` is too complex (15 > 10)",
    },
    {
        "filename": str(Path.cwd() / "quantmaster" / "b.py"),
        "location": {"row": 20, "column": 5},
        "message": "`bar` is too complex (12 > 10)",
    },
]

FAKE_FINDINGS_GROWN = [
    *FAKE_FINDINGS,
    {
        "filename": str(Path.cwd() / "quantmaster" / "c.py"),
        "location": {"row": 30, "column": 5},
        "message": "`baz` is too complex (11 > 10)",
    },
]


def _normalize(findings):
    return complexity_policy.normalize_findings(findings)


def _write_baseline(count: int, findings: list[dict]) -> None:
    payload = {
        "count": count,
        "reason": "fixture",
        "at": "2026-01-01T00:00:00+00:00",
        "findings": findings,
    }
    complexity_policy.BASELINE_FILE.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def _remove_baseline() -> None:
    if complexity_policy.BASELINE_FILE.exists():
        complexity_policy.BASELINE_FILE.unlink()


# -- normalization ----------------------------------------------------------

def test_normalize_finding_parses_message_and_location():
    norm = complexity_policy.normalize_finding(FAKE_FINDINGS[0])
    assert norm["file"] == "quantmaster/a.py"
    assert norm["line"] == 10
    assert norm["function"] == "foo"
    assert norm["complexity"] == 15


def test_normalize_finding_defaults_on_bad_message():
    norm = complexity_policy.normalize_finding(
        {"filename": str(Path.cwd() / "x.py"), "location": {"row": 1}}
    )
    assert norm["function"] == "(unknown)"
    assert norm["complexity"] == 0


def test_normalize_findings_sorts_stably():
    mixed = FAKE_FINDINGS[::-1]
    result = _normalize(mixed)
    assert [f["file"] for f in result] == ["quantmaster/a.py", "quantmaster/b.py"]


# -- baseline file round-trip -----------------------------------------------

def test_save_and_load_baseline(tmp_path, monkeypatch):
    monkeypatch.setattr(complexity_policy, "BASELINE_FILE", tmp_path / "baseline.json")
    findings = _normalize(FAKE_FINDINGS)

    complexity_policy.save_baseline(findings, "fixture update")
    assert (tmp_path / "baseline.json").is_file()

    data = json.loads((tmp_path / "baseline.json").read_text(encoding="utf-8"))
    assert data["count"] == 2
    assert data["reason"] == "fixture update"
    assert len(data["findings"]) == 2
    assert data["findings"][0]["file"] == "quantmaster/a.py"


def test_load_baseline_returns_none_when_absent(tmp_path, monkeypatch):
    monkeypatch.setattr(complexity_policy, "BASELINE_FILE", tmp_path / "missing.json")
    assert complexity_policy.load_baseline() is None
    assert complexity_policy.load_baseline_findings() == []


def test_baseline_file_uses_relative_paths_only(tmp_path, monkeypatch):
    monkeypatch.setattr(complexity_policy, "BASELINE_FILE", tmp_path / "baseline.json")
    findings = _normalize(FAKE_FINDINGS)
    complexity_policy.save_baseline(findings, "fixture")
    text = (tmp_path / "baseline.json").read_text(encoding="utf-8")
    # No absolute filesystem paths should leak into the version-controlled file.
    assert str(Path.cwd()) not in text


# -- diff computation -------------------------------------------------------

def test_compute_diff_detects_added_and_removed():
    baseline = _normalize(FAKE_FINDINGS)
    current = _normalize(FAKE_FINDINGS_GROWN)

    added, removed = complexity_policy.compute_diff(baseline, current)
    assert len(added) == 1
    assert added[0]["function"] == "baz"
    assert removed == []


def test_compute_diff_matches_by_function_name_across_line_moves():
    baseline = _normalize(FAKE_FINDINGS)
    current = [
        {
            "file": "quantmaster/a.py",
            "line": 999,
            "function": "foo",
            "complexity": 15,
        },
        *baseline[1:],
    ]
    added, removed = complexity_policy.compute_diff(baseline, current)
    assert added == []
    assert removed == []


# -- CLI flows --------------------------------------------------------------

def test_default_check_passes_when_count_matches(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(complexity_policy, "BASELINE_FILE", tmp_path / "baseline.json")
    _write_baseline(2, _normalize(FAKE_FINDINGS))
    monkeypatch.setattr(complexity_policy, "run_ruff_c901", lambda: FAKE_FINDINGS)

    assert complexity_policy.main([]) == 0
    out = capsys.readouterr().out
    assert "complexity policy ok" in out
    assert "2/2" in out


def test_default_check_rejects_growth_with_diff(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(complexity_policy, "BASELINE_FILE", tmp_path / "baseline.json")
    _write_baseline(2, _normalize(FAKE_FINDINGS))
    monkeypatch.setattr(complexity_policy, "run_ruff_c901", lambda: FAKE_FINDINGS_GROWN)

    assert complexity_policy.main([]) == 1
    err = capsys.readouterr().err
    assert "inventory grew" in err
    assert "+1 new" in err
    assert "baz" in err
    assert "--accept" in err


def test_default_check_falls_back_to_constant_when_no_baseline(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(complexity_policy, "BASELINE_FILE", tmp_path / "nope.json")
    monkeypatch.setattr(complexity_policy, "FALLBACK_BASELINE", 2)
    monkeypatch.setattr(complexity_policy, "run_ruff_c901", lambda: FAKE_FINDINGS)

    assert complexity_policy.main([]) == 0


def test_accept_creates_baseline_with_reason(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(complexity_policy, "BASELINE_FILE", tmp_path / "baseline.json")
    monkeypatch.setattr(complexity_policy, "run_ruff_c901", lambda: FAKE_FINDINGS)

    assert complexity_policy.main(["--accept", "--reason", "unit test"]) == 0
    out = capsys.readouterr().out
    assert "baseline accepted" in out
    assert "2 C901 findings" in out

    data = json.loads((tmp_path / "baseline.json").read_text(encoding="utf-8"))
    assert data["reason"] == "unit test"
    assert data["count"] == 2


def test_accept_requires_reason(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(complexity_policy, "BASELINE_FILE", tmp_path / "baseline.json")
    monkeypatch.setattr(complexity_policy, "run_ruff_c901", lambda: FAKE_FINDINGS)

    assert complexity_policy.main(["--accept"]) == 2
    err = capsys.readouterr().err
    assert "--reason" in err
