"""Enforce the final C901 ceiling and the zero-complexity outer-layer rule."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
# This is the owner-defined final ceiling from Issue #79/#87.  A lower inventory
# is welcome; a new finding is never grandfathered into the ceiling.
BASELINE = 130

TRANSPORT_ORCHESTRATION_PREFIXES = (
    "quantmaster/server/",
    "quantmaster/cli.py",
    "quantmaster/automation/",
    "quantmaster/lab/service.py",
    "quantmaster/lab/worker.py",
    "quantmaster/backtest/application.py",
    "quantmaster/research/jobs.py",
)


def _relative_path(filename: str) -> str:
    return Path(filename).resolve().relative_to(ROOT).as_posix()


def _is_transport_orchestration(path: str) -> bool:
    return any(
        path == prefix.rstrip("/") or path.startswith(prefix)
        for prefix in TRANSPORT_ORCHESTRATION_PREFIXES
    )


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
    outer_findings = [
        (_relative_path(item["filename"]), item["name"])
        for item in findings
        if _is_transport_orchestration(_relative_path(item["filename"]))
    ]
    if outer_findings:
        print("transport/orchestration C901 findings are forbidden:", file=sys.stderr)
        for path, name in outer_findings:
            print(f"{path}: {name}", file=sys.stderr)
        return 1
    if count > BASELINE:
        print(f"Ruff C901 inventory grew: {count} > audited baseline {BASELINE}", file=sys.stderr)
        return 1
    print(f"complexity policy ok: {count}/{BASELINE} C901 ceiling; transport/orchestration: 0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
