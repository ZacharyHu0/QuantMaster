# ADR 0003: Retain SciPy and the Rust research kernel

- Status: Accepted
- Date: 2026-08-16
- Issue: #84
- Evidence: [kernel-retention-2026-08-16.json](../baselines/kernel-retention-2026-08-16.json)

## Context

SciPy accounts for a material share of the Windows package. It may be removed only when a
full-market NumPy implementation is exactly equivalent, no slower than `1.2x`, and uses no more
than `1.25x` peak memory. The Rust research extension is retained only when its result is
equivalent to Python and its end-to-end net speedup is at least 20 percent after conversion costs.

The benchmark uses the real application seams rather than isolated operators:

- `quantmaster.rotation.analytics.analyze_group_rotation` for SciPy versus NumPy;
- `quantmaster.research.providers.compute_core_factors` for Python versus Rust.

The fixed local StockDB ingest is `sdi_f45211392ab3079c823db962`: 983,296 market rows across 180
sessions and 5,494 symbols, plus 1,337 groups and 93,514 memberships. Its existing StockDB content
identities are recorded in the JSON evidence. No provider or network fallback is used.

Cold samples are the first seam invocation in a fresh process after input preparation. Warm samples
run in one process after one untimed invocation. Each backend has three cold and three warm samples.
Timing uses `perf_counter_ns`; memory is the absolute process Working Set sampled every 5 ms. The
Rust seam includes the facade's Python-to-list and list-to-NumPy conversion work.

## Evidence

| Backend | Cold p50 / stdev | Warm p50 / stdev | Maximum Working Set |
| --- | ---: | ---: | ---: |
| SciPy | 8,913.993 / 1,803.327 ms | 7,448.880 / 209.256 ms | 728,182,784 B |
| NumPy | 15,554.687 / 1,571.373 ms | 13,793.653 / 419.421 ms | 750,645,248 B |
| Python | 126,624.082 / 12,499.049 ms | 126,139.238 / 8,895.513 ms | 829,894,656 B |
| Rust | 15,928.222 / 243.263 ms | 23,317.953 / 3,696.430 ms | 867,930,112 B |

SciPy and NumPy produced exactly the same 973 published group records. NumPy's worst cold/warm p50
ratio is `1.851775`, which fails the `1.2` runtime threshold; its peak-memory ratio is `1.030847`,
which passes the `1.25` threshold.

Python and Rust produced the same 988,920 keys and NaN mask. The maximum absolute numerical error is
`2.531308496145357e-14` under the existing `atol=1e-6`, `rtol=1e-6` contract. Taking the worse of
cold and warm p50 ratios, Rust retains an `81.514%` net speedup, above the 20 percent threshold.

## Decision

- Retain SciPy. The NumPy alternative is numerically exact and memory-safe but materially too slow.
- Retain the Rust extension and its build lane. Its conversion-inclusive application seam remains
  equivalent and substantially faster than Python.
- Create no deletion child Issue from #84 because neither deletion gate passed.

## Consequences

The package-budget work cannot count SciPy or the Rust extension as removable payload under the
current evidence. Payload pruning remains a separate task. A future decision requires a new
full-market report on the then-current numerical implementation; a microbenchmark is insufficient.

## Reproduction

Build or install the checked-in Rust extension into an isolated task-local directory, then run:

```text
<primary-python> -m scripts.dev.benchmark_kernels \
  --data-root <existing-local-data-root> \
  --ingest-id sdi_f45211392ab3079c823db962 \
  --native-path <task-local-native-path> \
  --cold-runs 3 --warm-runs 3 \
  --output <task-local-output.json>
```

The output directory must already be owned by `tasks.py`. The report intentionally omits all local
paths and records only stable StockDB identities and environment facts.
