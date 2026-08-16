"""Remove local absolute paths from text reports before CI uploads them."""

from __future__ import annotations

import argparse
from pathlib import Path

from quantmaster.logging_config import redact_public_text

TEXT_SUFFIXES = frozenset({".json", ".log", ".txt", ".xml"})


def _report_files(inputs: list[Path]) -> list[Path]:
    files: list[Path] = []
    for item in inputs:
        if item.is_file() and item.suffix.lower() in TEXT_SUFFIXES:
            files.append(item)
        elif item.is_dir():
            files.extend(
                path for path in item.rglob("*")
                if path.is_file() and path.suffix.lower() in TEXT_SUFFIXES
            )
    return sorted(set(files))


def scrub_file(path: Path) -> bool:
    original = path.read_text(encoding="utf-8", errors="replace")
    scrubbed = redact_public_text(original)
    if scrubbed == original:
        return False
    path.write_text(scrubbed, encoding="utf-8")
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("reports", nargs="+", type=Path)
    args = parser.parse_args()
    changed = sum(scrub_file(path) for path in _report_files(args.reports))
    print(f"scrubbed {changed} report file(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
