"""Ratchet the historical Ruff C901 inventory without grandfathering growth."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
# Stock research and rotation snapshot orchestration now have independently
# testable lifecycle, source-planning, matrix-loading, validation, computation,
# and publication steps.
# Keep this as the owner-defined ceiling. Refactors may reduce the current count,
# but tasks must not edit the ceiling or add findings beyond it.
BASELINE = 170


def main() -> int:
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
        findings = json.loads(result.stdout or "[]")
    except json.JSONDecodeError:
        print(result.stderr or result.stdout, file=sys.stderr)
        return 2
    count = len(findings)
    if count > BASELINE:
        print(f"Ruff C901 inventory grew: {count} > audited baseline {BASELINE}", file=sys.stderr)
        return 1
    print(f"complexity policy ok: {count}/{BASELINE} C901 ceiling")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
