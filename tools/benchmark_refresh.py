"""Offline, reproducible refresh benchmark for the local DAG hot path.

It intentionally generates its own deterministic market fixture and never
opens a provider.  That makes a regression report comparable across machines
and safe to run in CI or an air-gapped desktop installation.
"""

from __future__ import annotations

import argparse
import json
import platform
import statistics
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from quantmaster.rotation.analytics import analyze_group_rotation, compute_trend_matrices
from quantmaster.runtime.derived import DerivedArtifactCatalog


def _fixture(days: int, symbols: int, groups: int) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, dict[str, Any]]]:
    rng = np.random.default_rng(20260811)
    dates = pd.bdate_range("2025-01-02", periods=days)
    columns = [f"{600000 + index:06d}.SH" for index in range(symbols)]
    returns = rng.normal(0.00035, 0.014, (days, symbols))
    close = pd.DataFrame(
        20.0 * np.exp(np.cumsum(returns, axis=0)), index=dates, columns=columns,
    )
    amount = close.mul(rng.uniform(500_000, 3_000_000, symbols), axis=1)
    membership = max(8, min(48, max(8, symbols // 12)))
    values = {
        f"THEME{index:04d}": {
            "code": f"THEME{index:04d}",
            "name": f"离线题材 {index:04d}",
            "members": [columns[(index * 7 + offset) % symbols] for offset in range(membership)],
            "level": "concept",
        }
        for index in range(groups)
    }
    return close, amount, values


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * percentile)))
    return ordered[position]


def _measure(name: str, runs: int, operation) -> dict[str, Any]:
    samples: list[float] = []
    value: Any = None
    for _ in range(max(1, runs)):
        started = time.perf_counter()
        value = operation()
        samples.append((time.perf_counter() - started) * 1000.0)
    return {
        "scenario": name,
        "runs": len(samples),
        "min_ms": round(min(samples), 3),
        "p50_ms": round(statistics.median(samples), 3),
        "p95_ms": round(_percentile(samples, 0.95), 3),
        "max_ms": round(max(samples), 3),
        "value": value,
    }


def run_benchmark(*, scenario: str, runs: int, days: int, symbols: int, groups: int) -> dict[str, Any]:
    close, amount, memberships = _fixture(days, symbols, groups)
    names = {symbol: symbol for symbol in close.columns}
    results: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="qm-refresh-benchmark-") as temporary:
        catalog = DerivedArtifactCatalog(Path(temporary) / "derived")
        generation = catalog.advance_source_generation(
            "fixture.stock_bars", "2025-01", "fixture-bars-v1",
            coverage_start=str(close.index.min().date()), coverage_end=str(close.index.max().date()),
        )
        fingerprint = catalog.input_fingerprint(
            schema_version=2,
            algorithm_version="benchmark-v1",
            parameters={"days": days, "symbols": symbols, "groups": groups},
            source_generations=[generation],
        )

        def compute() -> dict[str, Any]:
            trend = compute_trend_matrices(close)
            return analyze_group_rotation(
                close, memberships, names=names, amount=amount, kind="theme", trend=trend,
            )

        output: dict[str, Any] | None = None
        if scenario in {"all", "cold", "warm", "incremental", "rebuild"}:
            measured = _measure("cold" if scenario == "all" else scenario, runs, compute)
            output = measured.pop("value")
            measured["groups_published"] = len(output["items"])
            results.append(measured)

        if output is None:
            output = compute()
        artifact = catalog.put_json({"schema_version": 2, "data": output})
        catalog.record_node(
            "benchmark.themes", "all", fingerprint, "benchmark-v1",
            output_artifact_id=artifact["artifact_id"],
        )
        catalog.publish_snapshot("benchmark", "themes", artifact["artifact_id"])

        if scenario in {"all", "warm"}:
            # Warm reads deliberately touch only the published pointer/manifest.
            measured = _measure(
                "warm", runs,
                lambda: catalog.current_snapshot("benchmark", "themes")["artifact"]["artifact_id"],
            )
            measured.pop("value")
            results.append(measured)
        if scenario in {"all", "noop"}:
            measured = _measure(
                "noop", max(10, runs),
                lambda: catalog.node_cache_hit("benchmark.themes", "all", fingerprint, "benchmark-v1") is not None,
            )
            assert measured.pop("value") is True
            measured["remote_calls"] = 0
            results.append(measured)

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "machine": {
            "platform": platform.platform(),
            "python": sys.version.split()[0],
            "processor": platform.processor() or "unknown",
        },
        "fixture": {"days": days, "symbols": symbols, "groups": groups, "network": "disabled"},
        "results": results,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=("all", "cold", "warm", "noop", "incremental", "rebuild"), default="all")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--days", type=int, default=180)
    parser.add_argument("--symbols", type=int, default=480)
    parser.add_argument("--groups", type=int, default=978)
    parser.add_argument("--output", type=Path, help="optional JSON report path")
    arguments = parser.parse_args()
    report = run_benchmark(
        scenario=arguments.scenario,
        runs=max(1, arguments.runs),
        days=max(40, arguments.days),
        symbols=max(16, arguments.symbols),
        groups=max(1, arguments.groups),
    )
    text = json.dumps(report, ensure_ascii=False, indent=2)
    if arguments.output:
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
