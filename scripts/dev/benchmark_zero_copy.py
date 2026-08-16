"""Minimal zero-copy Rust kernel benchmark for issue #155.

Uses a synthetic full-style panel (760 days x N symbols) and measures
``quantmaster.research.providers.compute_core_factors`` with Python-only and
Rust (zero-copy) kernels.  This is a development signal; the final evidence for
#155 must include a full-market StockDB-backed report.
"""

from __future__ import annotations

import argparse
import statistics
import time

import numpy as np
import pandas as pd

from quantmaster.research.kernel import Kernel, KernelBackend
from quantmaster.research.providers import compute_core_factors


def make_bars(days: int = 760, symbols: int = 800, seed: int = 20260816) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(end=pd.Timestamp("2026-08-15"), periods=days)
    rows = days * symbols
    frame = pd.DataFrame(
        {
            "trade_date": np.repeat(dates, symbols),
            "symbol": np.tile([f"S{i:04d}" for i in range(symbols)], days),
        },
    )
    frame["close"] = 100 + np.cumsum(rng.normal(0, 1, rows)) * 0.01
    frame["volume"] = rng.integers(100_000, 1_000_000, rows).astype(float)
    frame["amount"] = frame["close"] * frame["volume"]
    return frame


def measure(fn, runs: int) -> tuple[list[float], float]:
    fn()  # warmup
    samples = []
    for _ in range(runs):
        started = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - started) * 1000)
    return samples, statistics.median(samples)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", type=int, default=800)
    parser.add_argument("--runs", type=int, default=3)
    args = parser.parse_args()
    bars = make_bars(symbols=args.symbols)
    python_samples, python_median = measure(
        lambda: compute_core_factors(bars, Kernel(KernelBackend.PYTHON)), args.runs,
    )
    rust_samples, rust_median = measure(
        lambda: compute_core_factors(bars, Kernel(KernelBackend.RUST)), args.runs,
    )
    speedup = python_median / rust_median if rust_median > 0 else float("nan")
    print(f"symbols={args.symbols} runs={args.runs}")
    print(f"python p50={python_median:.1f}ms samples={[round(v,1) for v in python_samples]}")
    print(f"rust   p50={rust_median:.1f}ms samples={[round(v,1) for v in rust_samples]}")
    print(f"speedup={speedup:.3f}x")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
