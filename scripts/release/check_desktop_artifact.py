"""Reject oversized desktop artifacts and forbidden optional dependency bundles."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

DEFAULT_MAX_MIB = 350
FORBIDDEN_MODULES = ("torch", "dask", "pytest", "_pytest", "qrcode.tests")
MODULE_ENTRY = re.compile(r"(?m)^\s*\('([^']+)'[,)]")


def check_artifact(
    artifact: Path,
    *,
    analysis: Path | None = None,
    max_mib: int = DEFAULT_MAX_MIB,
) -> list[str]:
    errors: list[str] = []
    if not artifact.is_file():
        return [f"desktop artifact does not exist: {artifact}"]
    size_mib = artifact.stat().st_size / (1024 * 1024)
    if size_mib > max_mib:
        errors.append(f"desktop artifact is {size_mib:.1f} MiB; limit is {max_mib} MiB")
    if analysis is not None:
        if not analysis.is_file():
            errors.append(f"PyInstaller analysis does not exist: {analysis}")
        else:
            modules = set(MODULE_ENTRY.findall(analysis.read_text(encoding="utf-8")))
            bundled = sorted(
                module
                for module in modules
                if any(module == root or module.startswith(f"{root}.") for root in FORBIDDEN_MODULES)
            )
            if bundled:
                preview = ", ".join(bundled[:8])
                errors.append(f"forbidden optional modules were bundled: {preview}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    parser.add_argument("--analysis", type=Path)
    parser.add_argument("--max-mib", type=int, default=DEFAULT_MAX_MIB)
    args = parser.parse_args()
    errors = check_artifact(args.artifact, analysis=args.analysis, max_mib=args.max_mib)
    if errors:
        for error in errors:
            print(f"[desktop-artifact] {error}")
        return 1
    size_mib = args.artifact.stat().st_size / (1024 * 1024)
    print(f"[desktop-artifact] ok: {args.artifact} ({size_mib:.1f} MiB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
