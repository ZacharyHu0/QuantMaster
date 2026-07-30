"""Prevent broad exception handling from expanding beyond the audited legacy baseline."""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "quantmaster"

# Reductions are always accepted.  Any new file or any per-file increase fails CI and
# requires replacing the broad catch with classified exceptions or an explicit boundary.
BASELINE = {
    "quantmaster/ai/crawler.py": 3,
    "quantmaster/ai/news_sources.py": 2,
    "quantmaster/analysis/stock.py": 6,
    "quantmaster/automation/channels/weixin.py": 2,
    "quantmaster/automation/commands.py": 2,
    "quantmaster/automation/runtime.py": 2,
    "quantmaster/automation/service.py": 7,
    "quantmaster/backtest/paper_accounts.py": 2,
    "quantmaster/backtest/spec.py": 1,
    "quantmaster/backtest/validation.py": 1,
    "quantmaster/backtest/workbench.py": 2,
    "quantmaster/cli.py": 9,
    "quantmaster/credentials.py": 1,
    "quantmaster/data/akshare_source.py": 2,
    "quantmaster/data/fundamentals.py": 3,
    "quantmaster/data/industry.py": 3,
    "quantmaster/data/instruments.py": 3,
    "quantmaster/data/maintenance.py": 2,
    "quantmaster/data/migration.py": 4,
    "quantmaster/data/names.py": 1,
    "quantmaster/data/registry.py": 6,
    "quantmaster/data/repair.py": 1,
    "quantmaster/data/research.py": 1,
    "quantmaster/data/resilience.py": 3,
    "quantmaster/data/storage.py": 1,
    "quantmaster/data/tushare_source.py": 2,
    "quantmaster/decision/hybrid.py": 3,
    "quantmaster/doctor.py": 1,
    "quantmaster/factors/mining/genetic.py": 1,
    "quantmaster/factors/mining/llm_miner.py": 1,
    "quantmaster/factors/mining/python_miner.py": 4,
    "quantmaster/factors/python_artifact.py": 1,
    "quantmaster/lab/ml.py": 2,
    "quantmaster/lab/service.py": 10,
    "quantmaster/lab/worker.py": 1,
    "quantmaster/logging_config.py": 2,
    "quantmaster/portfolio/csv_import.py": 1,
    "quantmaster/portfolio/performance.py": 1,
    "quantmaster/research/adapters.py": 2,
    "quantmaster/research/engine.py": 1,
    "quantmaster/research/jobs.py": 3,
    "quantmaster/research/kernel.py": 3,
    "quantmaster/research/lake.py": 2,
    "quantmaster/rotation/provider.py": 3,
    "quantmaster/rotation/service.py": 2,
    "quantmaster/runtime/maintenance.py": 2,
    "quantmaster/runtime/process.py": 2,
    "quantmaster/runtime/sqlite.py": 2,
    "quantmaster/server/app.py": 19,
    "quantmaster/server/automation.py": 11,
    "quantmaster/server/diagnostics.py": 1,
    "quantmaster/server/lab.py": 14,
    "quantmaster/server/management.py": 6,
    "quantmaster/server/news.py": 9,
    "quantmaster/server/problems.py": 7,
    "quantmaster/server/trading.py": 11,
    "quantmaster/settings_checks.py": 4,
}

STRICT_PREFIXES = (
    "quantmaster/runtime/",
    "quantmaster/data/storage.py",
    "quantmaster/data/repair.py",
    "quantmaster/server/security.py",
)


def _broad_handlers(tree: ast.AST) -> list[ast.ExceptHandler]:
    return [
        node for node in ast.walk(tree)
        if isinstance(node, ast.ExceptHandler)
        and isinstance(node.type, ast.Name)
        and node.type.id == "Exception"
    ]


def _has_explicit_boundary_action(handler: ast.ExceptHandler) -> bool:
    for node in ast.walk(handler):
        if isinstance(node, ast.Raise):
            return True
        if not isinstance(node, ast.Call):
            continue
        function = node.func
        if isinstance(function, ast.Attribute) and function.attr in {
            "debug", "info", "warning", "error", "exception", "critical",
        }:
            return True
    return False


def analyze() -> list[str]:
    violations: list[str] = []
    current: dict[str, int] = {}
    for path in PACKAGE.rglob("*.py"):
        relative = path.relative_to(ROOT).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        handlers = _broad_handlers(tree)
        if handlers:
            current[relative] = len(handlers)
        allowed = BASELINE.get(relative, 0)
        if len(handlers) > allowed:
            violations.append(
                f"{relative}: broad catches {len(handlers)} exceed audited baseline {allowed}"
            )
        if relative.startswith(STRICT_PREFIXES):
            for handler in handlers:
                if not _has_explicit_boundary_action(handler):
                    violations.append(
                        f"{relative}:{handler.lineno}: critical broad catch does not log or re-raise"
                    )
    stale = sorted(path for path, count in BASELINE.items() if count and path not in current)
    if stale:
        violations.append(
            "exception baseline contains removed files; lower the baseline: " + ", ".join(stale)
        )
    return violations


def main() -> int:
    violations = analyze()
    if violations:
        print("\n".join(violations))
        return 1
    print(f"exception policy ok: {sum(BASELINE.values())} audited broad catches; no expansion")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
