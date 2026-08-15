"""Ratchet the historical Ruff C901 inventory without grandfathering growth.

The ceiling is enforced by a version-controlled baseline file
(`complexity_baseline.json` next to this module).  Tasks must not edit the
ceiling or let the inventory grow beyond it.  Owner-authorized growth is a
single command:

    python scripts/ci/complexity_policy.py --accept --reason "<audit note>"

That writes the current Ruff C901 inventory and the audit note into the
baseline file and returns success.  The default path (no ``--accept``) still
rejects any growth, prints a reviewable diff, and shows the approval
command to unblock the gate.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
BASELINE_FILE = Path(__file__).with_name("complexity_baseline.json")
# Fallback ceiling for bootstrapping the very first baseline file.  Once
# ``complexity_baseline.json`` exists, it becomes the authoritative ceiling.
FALLBACK_BASELINE = 170

_MESSAGE_RE = re.compile(r"^(?P<name>`[^`]+`)\s+is too complex\s*\((?P<score>\d+)\s*>\s*\d+\)")


def _repo_relative(path: str) -> str:
    """Turn an absolute filesystem path into a repo-relative POSIX path."""

    try:
        return Path(path).relative_to(ROOT).as_posix()
    except (ValueError, OSError):
        return path.replace("\\", "/")


def run_ruff_c901() -> list[dict[str, Any]]:
    """Return the raw Ruff C901 findings as a JSON-decoded list."""

    result = subprocess.run(
        [
            sys.executable, "-m", "ruff", "check", "quantmaster", "tests", "scripts",
            "--no-cache", "--select", "C901", "--output-format", "json",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    try:
        return json.loads(result.stdout or "[]")
    except json.JSONDecodeError:
        print(result.stderr or result.stdout, file=sys.stderr)
        return []


def load_baseline() -> dict[str, Any] | None:
    """Load the version-controlled baseline file, or ``None`` if absent."""

    if not BASELINE_FILE.is_file():
        return None
    with BASELINE_FILE.open(encoding="utf-8") as fh:
        return json.load(fh)


def normalize_finding(raw: dict[str, Any]) -> dict[str, Any]:
    """Extract stable, human-reviewable keys from a Ruff C901 finding."""

    message = raw.get("message", "")
    match = _MESSAGE_RE.match(message)
    function = match.group("name").strip("`") if match else "(unknown)"
    complexity = int(match.group("score")) if match else 0
    loc = raw.get("location") or {}
    rel_path = _repo_relative(raw.get("filename", ""))
    return {
        "file": rel_path,
        "line": int(loc.get("row", 0)),
        "function": function,
        "complexity": complexity,
    }


def _finding_sort_key(f: dict[str, Any]) -> tuple[str, int]:
    """Stable sort key: (relative path, line)."""

    return (f.get("file", ""), f.get("line", 0))


def _finding_match_key(f: dict[str, Any]) -> tuple[str, str]:
    """Matching key for diffs: (relative path, function name)."""

    return (f.get("file", ""), f.get("function", ""))


def normalize_findings(raw_findings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Normalize and sort a list of raw Ruff findings for stable serialization."""

    normalized = [normalize_finding(f) for f in raw_findings]
    return sorted(normalized, key=_finding_sort_key)


def save_baseline(findings: list[dict[str, Any]], reason: str) -> None:
    """Write the current inventory and audit note into the baseline file."""

    payload: dict[str, Any] = {
        "count": len(findings),
        "reason": reason,
        "at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "findings": findings,
    }
    BASELINE_FILE.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def load_baseline_findings() -> list[dict[str, Any]]:
    """Return the finding list from the baseline file.

    The baseline file already stores normalized findings; do not re-normalize.
    """

    data = load_baseline()
    if not data:
        return []
    return list(data.get("findings", []))


def compute_diff(
    baseline: list[dict[str, Any]],
    current: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return (added, removed) findings by matching on (file, function)."""

    baseline_map: dict[tuple[str, str], dict[str, Any]] = {
        _finding_match_key(f): f for f in baseline
    }
    current_map: dict[tuple[str, str], dict[str, Any]] = {
        _finding_match_key(f): f for f in current
    }
    added = [
        f for key, f in current_map.items()
        if key not in baseline_map
    ]
    removed = [
        f for key, f in baseline_map.items()
        if key not in current_map
    ]
    return (sorted(added, key=_finding_sort_key), sorted(removed, key=_finding_sort_key))


def print_diff(
    baseline_count: int,
    current: list[dict[str, Any]],
    current_count: int,
) -> None:
    """Print a reviewable diff and the owner-approval command."""

    baseline = load_baseline_findings()
    added, removed = compute_diff(baseline, current)

    print(f"Ruff C901 inventory grew: {current_count} > baseline {baseline_count}",
          file=sys.stderr)
    print(f"  +{len(added)} new, -{len(removed)} removed", file=sys.stderr)

    if added:
        print("  new findings:", file=sys.stderr)
        for f in added:
            print(
                f"    {f['file']}:{f['line']} {f['function']} "
                f"(complexity {f['complexity']})",
                file=sys.stderr,
            )
    if removed:
        print("  removed findings:", file=sys.stderr)
        for f in removed:
            print(
                f"    {f['file']}:{f['line']} {f['function']} "
                f"(complexity {f['complexity']})",
                file=sys.stderr,
            )

    print(file=sys.stderr)
    print("To approve (owner-authorized), run:", file=sys.stderr)
    print(
        "  python scripts/ci/complexity_policy.py --accept --reason \"<audit note>\"",
        file=sys.stderr,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--accept", action="store_true",
        help=(
            "owner-authorized baseline update: write the current Ruff C901 "
            "inventory and audit note into the version-controlled baseline file"
        ),
    )
    parser.add_argument(
        "--reason", default="",
        help="audit reason; required with --accept",
    )
    args = parser.parse_args(argv)

    raw_findings = run_ruff_c901()
    current = normalize_findings(raw_findings)
    current_count = len(current)

    if args.accept:
        if not args.reason:
            print("--accept requires --reason for the audit trail", file=sys.stderr)
            return 2
        save_baseline(current, args.reason)
        print(f"complexity baseline accepted: {current_count} C901 findings")
        print(f"  reason: {args.reason}")
        try:
            rel = BASELINE_FILE.relative_to(ROOT)
        except ValueError:
            rel = BASELINE_FILE
        print(f"  baseline: {rel}")
        return 0

    baseline_data = load_baseline()
    baseline_count = baseline_data["count"] if baseline_data else FALLBACK_BASELINE

    if current_count > baseline_count:
        print_diff(baseline_count, current, current_count)
        return 1

    print(f"complexity policy ok: {current_count}/{baseline_count} C901 ceiling")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
