"""Benchmark optional numerical kernels through their production application seams."""

from __future__ import annotations

import argparse
import ctypes
import gc
import json
import os
import platform
import statistics
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd
import scipy

from quantmaster.data.free_stockdb_ingest import StockDBIngestStore
from quantmaster.research.contracts import KernelBackend
from quantmaster.research.kernel import Kernel
from quantmaster.research.providers import compute_core_factors
from quantmaster.rotation import analytics


class _DenseMatrix:
    def __init__(self, values: Any, *, shape: tuple[int, int]):
        data, (rows, columns) = values
        self.values = np.zeros(shape, dtype=np.asarray(data).dtype)
        np.add.at(self.values, (rows, columns), data)

    def __matmul__(self, values: np.ndarray) -> np.ndarray:
        return self.values @ values

    def toarray(self) -> np.ndarray:
        return self.values


class _NumPySparseAdapter:
    csr_matrix = _DenseMatrix

    @staticmethod
    def issparse(_value: Any) -> bool:
        return False


def run_rotation_backend(
    close: pd.DataFrame,
    amount: pd.DataFrame,
    groups: dict[str, dict[str, Any]],
    *,
    backend: Literal["scipy", "numpy"],
    trend: analytics.TrendMatrices | None = None,
) -> dict[str, Any]:
    """Run the real rotation interface with one membership-matrix implementation."""

    if backend == "scipy":
        return analytics.analyze_group_rotation(
            close, groups, amount=amount, kind="theme", trend=trend,
        )
    if backend != "numpy":
        raise ValueError(f"unknown rotation backend: {backend}")
    original = analytics.sparse
    try:
        analytics.sparse = _NumPySparseAdapter
        return analytics.analyze_group_rotation(
            close, groups, amount=amount, kind="theme", trend=trend,
        )
    finally:
        analytics.sparse = original


def _ratio(numerator: float, denominator: float) -> float:
    return numerator / denominator if denominator > 0 else float("inf")


def summarize_samples(samples: list[dict[str, float | int]]) -> dict[str, Any]:
    """Preserve raw samples while exposing the statistics used by the decision."""

    elapsed = [float(sample["elapsed_ms"]) for sample in samples]
    peaks = [int(sample["peak_memory_bytes"]) for sample in samples]
    if not elapsed:
        raise ValueError("benchmark requires at least one sample")
    return {
        "runs": len(elapsed),
        "samples_ms": elapsed,
        "samples_peak_memory_bytes": peaks,
        "min_ms": min(elapsed),
        "p50_ms": statistics.median(elapsed),
        "mean_ms": statistics.mean(elapsed),
        "stdev_ms": statistics.stdev(elapsed) if len(elapsed) > 1 else 0.0,
        "max_ms": max(elapsed),
        "peak_memory_max_bytes": max(peaks),
    }


def evaluate_retention(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Apply the owner-approved retention thresholds to a benchmark report."""

    comparisons = report["comparisons"]
    scipy = comparisons["scipy"]
    sparse, dense = scipy["backends"]["scipy"], scipy["backends"]["numpy"]
    scipy_time_ratio = max(
        _ratio(float(dense[phase]["p50_ms"]), float(sparse[phase]["p50_ms"]))
        for phase in ("cold", "warm")
    )
    scipy_peak_ratio = _ratio(
        max(int(dense[phase]["peak_memory_max_bytes"]) for phase in ("cold", "warm")),
        max(int(sparse[phase]["peak_memory_max_bytes"]) for phase in ("cold", "warm")),
    )
    scipy_delete = (
        scipy["parity"]["exact"]
        and scipy_time_ratio <= 1.2
        and scipy_peak_ratio <= 1.25
    )

    rust = comparisons["rust"]
    python, native = rust["backends"]["python"], rust["backends"]["rust"]
    rust_time_ratio = max(
        _ratio(float(native[phase]["p50_ms"]), float(python[phase]["p50_ms"]))
        for phase in ("cold", "warm")
    )
    rust_speedup = 1.0 - rust_time_ratio
    rust_retain = rust["parity"]["equivalent"] and rust_time_ratio <= 0.80
    return {
        "scipy": {
            "decision": "delete" if scipy_delete else "retain",
            "maximum_time_ratio": scipy_time_ratio,
            "peak_memory_ratio": scipy_peak_ratio,
            "thresholds": {"time_ratio_max": 1.2, "peak_memory_ratio_max": 1.25},
        },
        "rust": {
            "decision": "retain" if rust_retain else "delete",
            "minimum_net_speedup": rust_speedup,
            "thresholds": {"net_speedup_min": 0.20},
        },
    }


def _working_set_bytes() -> int:
    if os.name == "nt":
        size_t = ctypes.c_size_t

        class ProcessMemoryCounters(ctypes.Structure):
            _fields_ = [
                ("cb", ctypes.c_ulong),
                ("PageFaultCount", ctypes.c_ulong),
                ("PeakWorkingSetSize", size_t),
                ("WorkingSetSize", size_t),
                ("QuotaPeakPagedPoolUsage", size_t),
                ("QuotaPagedPoolUsage", size_t),
                ("QuotaPeakNonPagedPoolUsage", size_t),
                ("QuotaNonPagedPoolUsage", size_t),
                ("PagefileUsage", size_t),
                ("PeakPagefileUsage", size_t),
                ("PrivateUsage", size_t),
            ]

        counters = ProcessMemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)  # type: ignore[attr-defined]
        psapi = ctypes.WinDLL("psapi", use_last_error=True)  # type: ignore[attr-defined]
        kernel32.GetCurrentProcess.restype = ctypes.c_void_p
        psapi.GetProcessMemoryInfo.argtypes = [
            ctypes.c_void_p, ctypes.POINTER(ProcessMemoryCounters), ctypes.c_ulong,
        ]
        psapi.GetProcessMemoryInfo.restype = ctypes.c_int
        process = kernel32.GetCurrentProcess()
        ok = psapi.GetProcessMemoryInfo(
            process, ctypes.byref(counters), counters.cb,
        )
        if not ok:
            raise OSError("GetProcessMemoryInfo failed")
        return int(counters.WorkingSetSize)
    status = Path("/proc/self/statm")
    if status.is_file():
        resident_pages = int(status.read_text(encoding="ascii").split()[1])
        return resident_pages * int(os.sysconf("SC_PAGE_SIZE"))
    import resource

    peak = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    return peak if sys.platform == "darwin" else peak * 1024


def _measure(operation) -> tuple[Any, dict[str, float | int]]:
    gc.collect()
    peak = _working_set_bytes()
    stop = threading.Event()

    def sample() -> None:
        nonlocal peak
        while not stop.wait(0.005):
            peak = max(peak, _working_set_bytes())

    sampler = threading.Thread(target=sample, name="kernel-benchmark-rss", daemon=True)
    sampler.start()
    started = time.perf_counter_ns()
    try:
        result = operation()
    finally:
        elapsed_ms = round((time.perf_counter_ns() - started) / 1_000_000, 3)
        stop.set()
        sampler.join()
        peak = max(peak, _working_set_bytes())
    return result, {"elapsed_ms": elapsed_ms, "peak_memory_bytes": peak}


def _snapshot(store: StockDBIngestStore, ingest_id: str):
    snapshot = store.get(ingest_id)
    if snapshot is None:
        raise ValueError(f"StockDB ingest not found: {ingest_id}")
    required = {"stock_daily", "boards"}
    if snapshot.status != "complete" or not required.issubset(snapshot.content_hashes):
        raise ValueError(f"StockDB ingest is not a complete stock snapshot: {ingest_id}")
    return snapshot


def _latest_snapshot(store: StockDBIngestStore):
    for snapshot in store.history(100):
        if (
            snapshot.status == "complete"
            and {"stock_daily", "boards"}.issubset(snapshot.content_hashes)
        ):
            return snapshot
    raise ValueError("no complete local StockDB stock snapshot is available")


def _load_stockdb(data_root: Path, ingest_id: str) -> tuple[Any, pd.DataFrame, list[Any]]:
    store = StockDBIngestStore(data_root / "stockdb-ingest")
    snapshot = _snapshot(store, ingest_id)
    daily = store.load_frame(snapshot, "stock_daily")
    boards = store.load_json(snapshot, "boards")
    if daily.empty or not isinstance(boards, list) or not boards:
        raise ValueError("StockDB snapshot is missing market rows or board membership")
    return snapshot, daily, boards


def _rotation_inputs(
    daily: pd.DataFrame, boards: list[Any],
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, dict[str, Any]], analytics.TrendMatrices]:
    close = daily.pivot(index="date", columns="symbol", values="close").sort_index()
    amount = daily.pivot(index="date", columns="symbol", values="amount").reindex_like(close)
    groups = {
        str(item["code"]): item
        for item in boards
        if isinstance(item, dict) and item.get("code") and item.get("members")
    }
    return close, amount, groups, analytics.compute_trend_matrices(close)


def _factor_inputs(daily: pd.DataFrame) -> pd.DataFrame:
    return daily[["date", "symbol", "close", "volume", "amount"]].rename(
        columns={"date": "trade_date"},
    )


def _kernel(backend: Literal["python", "rust"]) -> Kernel:
    requested = KernelBackend.PYTHON if backend == "python" else KernelBackend.RUST
    kernel = Kernel(requested)
    if backend == "rust" and kernel.backend_used != KernelBackend.RUST:
        raise RuntimeError(kernel.fallback_reason or "Rust kernel is unavailable")
    return kernel


def _worker_measure(
    data_root: Path,
    ingest_id: str,
    mode: Literal["rotation-scipy", "rotation-numpy", "factors-python", "factors-rust"],
    *,
    runs: int,
    warmup: bool,
) -> dict[str, Any]:
    snapshot, daily, boards = _load_stockdb(data_root, ingest_id)
    if mode.startswith("rotation-"):
        close, amount, groups, trend = _rotation_inputs(daily, boards)
        backend = mode.removeprefix("rotation-")
        operation = lambda: run_rotation_backend(  # noqa: E731
            close, amount, groups, backend=backend, trend=trend,
        )
        observation = {"sessions": len(close), "symbols": len(close.columns), "groups": len(groups)}
    else:
        bars = _factor_inputs(daily)
        backend = mode.removeprefix("factors-")
        kernel = _kernel(backend)

        def operation():
            result = compute_core_factors(bars, kernel)
            if backend == "rust" and kernel.backend_used != KernelBackend.RUST:
                raise RuntimeError(kernel.fallback_reason or "Rust kernel fell back to Python")
            return result

        observation = {
            "rows": len(bars),
            "sessions": int(bars["trade_date"].nunique()),
            "symbols": int(bars["symbol"].nunique()),
            "native_version": kernel.native_version,
        }
    del daily, boards, snapshot
    if warmup:
        value = operation()
        del value
        gc.collect()
    samples: list[dict[str, float | int]] = []
    output_size = 0
    for _ in range(runs):
        value, sample = _measure(operation)
        output_size = len(value["items"]) if isinstance(value, dict) else len(value)
        samples.append(sample)
        del value
    observation["output_rows"] = output_size
    return {"samples": samples, "observation": observation}


def _worker_parity(
    data_root: Path,
    ingest_id: str,
    mode: Literal["parity-scipy", "parity-rust"],
) -> dict[str, Any]:
    _snapshot_value, daily, boards = _load_stockdb(data_root, ingest_id)
    if mode == "parity-scipy":
        close, amount, groups, trend = _rotation_inputs(daily, boards)
        sparse = run_rotation_backend(close, amount, groups, backend="scipy", trend=trend)
        dense = run_rotation_backend(close, amount, groups, backend="numpy", trend=trend)
        left = json.dumps(sparse, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        right = json.dumps(dense, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return {"exact": left == right, "output_groups": len(sparse["items"])}
    bars = _factor_inputs(daily)
    python = compute_core_factors(bars, _kernel("python"))
    rust_kernel = _kernel("rust")
    native = compute_core_factors(bars, rust_kernel)
    if rust_kernel.backend_used != KernelBackend.RUST:
        raise RuntimeError(rust_kernel.fallback_reason or "Rust kernel fell back to Python")
    keys_equal = python[["trade_date", "symbol"]].equals(native[["trade_date", "symbol"]])
    columns = [column for column in python if column not in {"trade_date", "symbol"}]
    expected = python[columns].to_numpy(dtype=float)
    actual = native[columns].to_numpy(dtype=float)
    nan_masks_equal = np.array_equal(np.isnan(actual), np.isnan(expected))
    finite = np.isfinite(actual) & np.isfinite(expected)
    max_abs_error = float(np.max(np.abs(actual[finite] - expected[finite]))) if finite.any() else 0.0
    equivalent = keys_equal and nan_masks_equal and np.allclose(
        actual, expected, atol=1e-6, rtol=1e-6, equal_nan=True,
    )
    return {
        "equivalent": bool(equivalent),
        "keys_equal": keys_equal,
        "nan_masks_equal": nan_masks_equal,
        "atol": 1e-6,
        "rtol": 1e-6,
        "max_abs_error": max_abs_error,
        "output_rows": len(python),
    }


def _child(
    data_root: Path,
    ingest_id: str,
    mode: str,
    *,
    runs: int = 1,
    warmup: bool = False,
    native_path: Path | None = None,
) -> dict[str, Any]:
    command = [
        sys.executable, "-m", "scripts.dev.benchmark_kernels",
        "--data-root", str(data_root), "--ingest-id", ingest_id, "--worker", mode,
        "--runs", str(runs),
    ]
    if warmup:
        command.append("--warmup")
    environment = os.environ.copy()
    python_paths = [str(Path(__file__).resolve().parents[2])]
    if native_path is not None:
        python_paths.insert(0, str(native_path))
    if environment.get("PYTHONPATH"):
        python_paths.append(environment["PYTHONPATH"])
    environment["PYTHONPATH"] = os.pathsep.join(python_paths)
    completed = subprocess.run(
        command, cwd=Path(__file__).resolve().parents[2], env=environment,
        capture_output=True, text=True, check=False,
    )
    if completed.returncode:
        message = completed.stderr.strip().splitlines()[-1] if completed.stderr.strip() else "unknown error"
        raise RuntimeError(f"benchmark worker {mode} failed: {message}")
    return json.loads(completed.stdout)


def _backend_report(
    data_root: Path,
    ingest_id: str,
    mode: str,
    *,
    cold_runs: int,
    warm_runs: int,
    native_path: Path | None,
) -> dict[str, Any]:
    cold: list[dict[str, float | int]] = []
    observation: dict[str, Any] = {}
    for _ in range(cold_runs):
        result = _child(
            data_root, ingest_id, mode, runs=1, warmup=False, native_path=native_path,
        )
        cold.extend(result["samples"])
        observation = result["observation"]
    warm = _child(
        data_root, ingest_id, mode, runs=warm_runs, warmup=True, native_path=native_path,
    )
    return {
        "cold": summarize_samples(cold),
        "warm": summarize_samples(warm["samples"]),
        "observation": observation,
    }


def _command_version(command: str) -> str:
    try:
        completed = subprocess.run(
            [command, "--version"], capture_output=True, text=True, timeout=10, check=False,
        )
    except OSError:
        return "unavailable"
    return completed.stdout.strip() if completed.returncode == 0 else "unavailable"


def _identity(data_root: Path, ingest_id: str) -> dict[str, Any]:
    snapshot, daily, boards = _load_stockdb(data_root, ingest_id)
    memberships = sum(
        len(item.get("members") or []) for item in boards if isinstance(item, dict)
    )
    return {
        "source": "local-stockdb-ingest",
        "network": "disabled",
        "ingest_id": snapshot.ingest_id,
        "artifact_id": snapshot.artifact_id,
        "content_ids": {
            name: snapshot.content_hashes[name] for name in ("stock_daily", "boards")
        },
        "as_of_date": snapshot.as_of_date,
        "start_date": str(daily["date"].min().date()),
        "end_date": str(daily["date"].max().date()),
        "rows": len(daily),
        "sessions": int(daily["date"].nunique()),
        "symbols": int(daily["symbol"].nunique()),
        "groups": len(boards),
        "memberships": memberships,
    }


def run_benchmark(
    data_root: Path,
    *,
    ingest_id: str | None = None,
    native_path: Path | None = None,
    cold_runs: int = 3,
    warm_runs: int = 5,
) -> dict[str, Any]:
    """Run both retention decisions without contacting a provider."""

    store = StockDBIngestStore(data_root / "stockdb-ingest")
    selected = ingest_id or _latest_snapshot(store).ingest_id
    backends = {
        "scipy": _backend_report(
            data_root, selected, "rotation-scipy", cold_runs=cold_runs,
            warm_runs=warm_runs, native_path=native_path,
        ),
        "numpy": _backend_report(
            data_root, selected, "rotation-numpy", cold_runs=cold_runs,
            warm_runs=warm_runs, native_path=native_path,
        ),
        "python": _backend_report(
            data_root, selected, "factors-python", cold_runs=cold_runs,
            warm_runs=warm_runs, native_path=native_path,
        ),
        "rust": _backend_report(
            data_root, selected, "factors-rust", cold_runs=cold_runs,
            warm_runs=warm_runs, native_path=native_path,
        ),
    }
    report: dict[str, Any] = {
        "schema_version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "machine": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor() or "unknown",
            "logical_cpus": os.cpu_count(),
            "python": platform.python_version(),
            "numpy": np.__version__,
            "pandas": pd.__version__,
            "scipy": scipy.__version__,
            "rust_kernel": backends["rust"]["observation"]["native_version"],
            "rustc": _command_version("rustc"),
            "thread_environment": {
                name: os.environ.get(name, "default")
                for name in ("RAYON_NUM_THREADS", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS")
            },
        },
        "dataset": _identity(data_root, selected),
        "method": {
            "scipy_seam": "quantmaster.rotation.analytics.analyze_group_rotation",
            "rust_seam": "quantmaster.research.providers.compute_core_factors",
            "cold": "first invocation in a fresh process after local input preparation",
            "warm": "same process after one untimed invocation",
            "timing": "time.perf_counter_ns around the application seam",
            "memory": "absolute process Working Set sampled every 5 ms",
            "rust_conversion": "Kernel public methods include Python-to-list and list-to-NumPy conversion",
            "sample_distribution": "raw samples plus min/p50/mean/stdev/max",
        },
        "comparisons": {
            "scipy": {
                "parity": _child(
                    data_root, selected, "parity-scipy", native_path=native_path,
                ),
                "backends": {"scipy": backends["scipy"], "numpy": backends["numpy"]},
            },
            "rust": {
                "parity": _child(
                    data_root, selected, "parity-rust", native_path=native_path,
                ),
                "backends": {"python": backends["python"], "rust": backends["rust"]},
            },
        },
    }
    report["decisions"] = evaluate_retention(report)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--ingest-id")
    parser.add_argument("--native-path", type=Path)
    parser.add_argument("--cold-runs", type=int, default=3)
    parser.add_argument("--warm-runs", type=int, default=5)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--worker", choices=(
        "rotation-scipy", "rotation-numpy", "factors-python", "factors-rust",
        "parity-scipy", "parity-rust",
    ), help=argparse.SUPPRESS)
    parser.add_argument("--runs", type=int, default=1, help=argparse.SUPPRESS)
    parser.add_argument("--warmup", action="store_true", help=argparse.SUPPRESS)
    arguments = parser.parse_args()
    if arguments.worker:
        if not arguments.ingest_id:
            parser.error("--worker requires --ingest-id")
        if arguments.worker.startswith("parity-"):
            result = _worker_parity(arguments.data_root, arguments.ingest_id, arguments.worker)
        else:
            result = _worker_measure(
                arguments.data_root, arguments.ingest_id, arguments.worker,
                runs=max(1, arguments.runs), warmup=arguments.warmup,
            )
    else:
        result = run_benchmark(
            arguments.data_root, ingest_id=arguments.ingest_id,
            native_path=arguments.native_path,
            cold_runs=max(1, arguments.cold_runs), warm_runs=max(1, arguments.warm_runs),
        )
    text = json.dumps(result, ensure_ascii=False, indent=2)
    if arguments.output:
        if not arguments.output.parent.is_dir():
            raise ValueError("output parent must be a task-owned existing directory")
        arguments.output.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
