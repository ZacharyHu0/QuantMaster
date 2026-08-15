from __future__ import annotations

import json
import re
from pathlib import Path

import numpy as np
import pandas as pd

from scripts.dev.benchmark_kernels import (
    evaluate_retention,
    run_rotation_backend,
    summarize_samples,
)


def _timing(p50_ms: float, peak_bytes: int) -> dict[str, float | int]:
    return {"p50_ms": p50_ms, "peak_memory_max_bytes": peak_bytes}


def test_retention_thresholds_fail_closed_at_the_public_report_seam() -> None:
    report = {
        "comparisons": {
            "scipy": {
                "parity": {"exact": True},
                "backends": {
                    "scipy": {
                        "cold": _timing(100.0, 100),
                        "warm": _timing(100.0, 100),
                    },
                    "numpy": {
                        "cold": _timing(120.0, 125),
                        "warm": _timing(120.0, 125),
                    },
                },
            },
            "rust": {
                "parity": {"equivalent": True},
                "backends": {
                    "python": {
                        "cold": _timing(100.0, 100),
                        "warm": _timing(100.0, 100),
                    },
                    "rust": {
                        "cold": _timing(80.0, 100),
                        "warm": _timing(80.0, 100),
                    },
                },
            },
        },
    }

    accepted = evaluate_retention(report)
    assert accepted["scipy"]["decision"] == "delete"
    assert accepted["rust"]["decision"] == "retain"

    report["comparisons"]["scipy"]["backends"]["numpy"]["warm"]["p50_ms"] = 120.01
    report["comparisons"]["rust"]["backends"]["rust"]["cold"]["p50_ms"] = 80.01
    rejected = evaluate_retention(report)
    assert rejected["scipy"]["decision"] == "retain"
    assert rejected["rust"]["decision"] == "delete"


def test_numpy_adapter_matches_scipy_through_rotation_application_seam() -> None:
    dates = pd.bdate_range("2026-01-02", periods=80)
    symbols = [f"{index:06d}.SZ" for index in range(24)]
    rng = np.random.default_rng(84)
    close = pd.DataFrame(
        10.0 * np.exp(np.cumsum(rng.normal(0.0, 0.01, (80, 24)), axis=0)),
        index=dates,
        columns=symbols,
    )
    amount = close.mul(rng.uniform(100_000, 500_000, len(symbols)), axis=1)
    groups = {
        "g1": {"code": "g1", "name": "one", "members": symbols[:16]},
        "g2": {"code": "g2", "name": "two", "members": symbols[8:]},
    }

    scipy_result = run_rotation_backend(close, amount, groups, backend="scipy")
    numpy_result = run_rotation_backend(close, amount, groups, backend="numpy")

    assert numpy_result == scipy_result


def test_sample_summary_keeps_distribution_and_native_peak_memory() -> None:
    summary = summarize_samples([
        {"elapsed_ms": 12.0, "peak_memory_bytes": 300},
        {"elapsed_ms": 10.0, "peak_memory_bytes": 500},
        {"elapsed_ms": 11.0, "peak_memory_bytes": 400},
    ])

    assert summary == {
        "runs": 3,
        "samples_ms": [12.0, 10.0, 11.0],
        "samples_peak_memory_bytes": [300, 500, 400],
        "min_ms": 10.0,
        "p50_ms": 11.0,
        "mean_ms": 11.0,
        "stdev_ms": 1.0,
        "max_ms": 12.0,
        "peak_memory_max_bytes": 500,
    }


def test_checked_in_report_is_replayable_and_safe_for_public_github() -> None:
    report = json.loads(
        (Path(__file__).parents[1] / "docs" / "baselines" / "kernel-retention-2026-08-16.json")
        .read_text(encoding="utf-8")
    )

    assert report["dataset"]["network"] == "disabled"
    assert report["machine"]["rustc"].startswith("rustc ")
    assert evaluate_retention(report) == report["decisions"]
    encoded = json.dumps(report, ensure_ascii=False)
    assert re.search(r"(?i)(?:[a-z]:\\\\|/users/|/home/)", encoded) is None
