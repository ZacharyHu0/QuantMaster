"""Ratchet the historical Ruff C901 inventory without grandfathering growth."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
# v1.6.0 adds audited, contract-heavy ingestion and point-in-time orchestration.
# Keep this as an exact ratchet: later work must lower the number when refactoring
# and cannot add another complex entry point unnoticed.
BASELINE = 193


def main() -> int:
    result = subprocess.run(
        [
            sys.executable, "-m", "ruff", "check", "quantmaster", "tests", "tools",
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
    if count < BASELINE:
        print(
            f"Ruff C901 baseline is stale: {count} < {BASELINE}; lower BASELINE",
            file=sys.stderr,
        )
        return 1
    print(f"complexity policy ok: {count}/{BASELINE} historical C901 findings")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
