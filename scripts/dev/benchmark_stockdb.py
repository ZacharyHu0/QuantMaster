"""Full-market StockDB-backed benchmark for issue #155.

Requires a local free-stockdb service (default http://127.0.0.1:7899).
Measures ``quantmaster.research.providers.compute_core_factors`` with the
Python-only and zero-copy Rust kernels.
"""

from __future__ import annotations

import argparse
import os
import statistics
import time

import pandas as pd

from quantmaster.data.free_stockdb_source import FreeStockDBSource
from quantmaster.data.instruments import InstrumentStore
from quantmaster.research.kernel import Kernel, KernelBackend
from quantmaster.research.providers import compute_core_factors


def load_bars(url: str, start: str, end: str, limit: int | None = None) -> pd.DataFrame:
    os.environ.setdefault("QM_FREE_STOCKDB_URL", url)
    source = FreeStockDBSource()
    symbols = [
        item.symbol for item in InstrumentStore().list(market="CN", asset_type="stock")
        if item.tradable and item.status not in {"d", "p"}
    ]
    if limit is not None:
        symbols = symbols[:limit]
    print(f"[benchmark-stockdb] loading {len(symbols)} symbols {start}..{end}", flush=True)
    frames = []
    errors = 0
    for index, symbol in enumerate(symbols, 1):
        try:
            frame = source.daily(symbol, start, end)
            if frame.empty:
                continue
            frame = frame.reset_index()
            frame["symbol"] = symbol
            frame = frame.rename(columns={"date": "trade_date"})
            frames.append(frame[["trade_date", "symbol", "close", "volume", "amount"]])
        except Exception as exc:
            errors += 1
            if errors <= 5:
                print(f"[benchmark-stockdb] skip {symbol}: {exc}", flush=True)
        if index % 500 == 0:
            print(f"[benchmark-stockdb] loaded {index}/{len(symbols)}", flush=True)
    if not frames:
        raise RuntimeError("no StockDB daily data loaded")
    bars = pd.concat(frames, ignore_index=True)
    bars["trade_date"] = pd.to_datetime(bars["trade_date"])
    print(f"[benchmark-stockdb] loaded {len(bars)} rows, errors={errors}", flush=True)
    return bars


def measure(fn, runs: int) -> tuple[list[float], float]:
    started = time.perf_counter()
    fn()
    cold_ms = (time.perf_counter() - started) * 1000
    samples = []
    for _ in range(runs):
        started = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - started) * 1000)
    return samples, statistics.median(samples), cold_ms


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:7899")
    parser.add_argument("--start", default="2023-07-01")
    parser.add_argument("--end", default="2026-08-15")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    bars = load_bars(args.url, args.start, args.end, limit=args.limit)
    print(f"[benchmark-stockdb] rows={len(bars)}", flush=True)

    _py_samples, py_median, py_cold = measure(
        lambda: compute_core_factors(bars, Kernel(KernelBackend.PYTHON)), args.runs,
    )
    _rust_samples, rust_median, rust_cold = measure(
        lambda: compute_core_factors(bars, Kernel(KernelBackend.RUST)), args.runs,
    )
    speedup = py_median / rust_median if rust_median > 0 else float("nan")
    print(f"[benchmark-stockdb] python cold={py_cold:.1f}ms p50={py_median:.1f}ms")
    print(f"[benchmark-stockdb] rust   cold={rust_cold:.1f}ms p50={rust_median:.1f}ms")
    print(f"[benchmark-stockdb] speedup={speedup:.3f}x")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

