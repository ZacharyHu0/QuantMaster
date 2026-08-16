"""Numerical kernel facade with deterministic Python and optional Rust backends."""

from __future__ import annotations

import importlib
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import numpy as np

from quantmaster.research.contracts import KernelBackend

logger = logging.getLogger(__name__)


def _matrix(values: Any) -> np.ndarray:
    result = np.asarray(values, dtype="float64")
    if result.ndim != 2:
        raise ValueError("研究内核只接受二维矩阵")
    return result


def _python_rank(values: np.ndarray) -> np.ndarray:
    output = np.full_like(values, np.nan)
    for row_index, row in enumerate(values):
        finite = np.isfinite(row)
        count = int(finite.sum())
        if not count:
            continue
        order = np.argsort(row[finite], kind="mergesort")
        sorted_values = row[finite][order]
        ranks = np.empty(count, dtype="float64")
        start = 0
        while start < count:
            stop = start + 1
            while stop < count and sorted_values[stop] == sorted_values[start]:
                stop += 1
            ranks[order[start:stop]] = ((start + 1) + stop) / 2 / count
            start = stop
        output[row_index, finite] = ranks
    return output


def _python_robust_standardize(values: np.ndarray, k: float) -> np.ndarray:
    output = np.full_like(values, np.nan)
    for row_index, row in enumerate(values):
        finite = np.isfinite(row)
        if not finite.any():
            continue
        clean = row[finite]
        median = float(np.median(clean))
        mad = float(np.median(np.abs(clean - median))) * 1.4826
        clipped = np.clip(clean, median - k * mad, median + k * mad) if mad > 0 else clean
        std = float(np.std(clipped, ddof=1)) if len(clipped) > 1 else 0.0
        if std > 0:
            output[row_index, finite] = (clipped - float(np.mean(clipped))) / std
        else:
            output[row_index, finite] = 0.0
    return output


def _python_weighted_zscore(values: np.ndarray, weights: np.ndarray) -> np.ndarray:
    if values.shape != weights.shape:
        raise ValueError("values 和 weights 形状必须一致")
    output = np.full_like(values, np.nan)
    for index, (row, weight) in enumerate(zip(values, weights, strict=True)):
        finite = np.isfinite(row) & np.isfinite(weight) & (weight > 0)
        if not finite.any():
            continue
        clean, clean_weight = row[finite], weight[finite]
        mean = float(np.average(clean, weights=clean_weight))
        variance = float(np.average((clean - mean) ** 2, weights=clean_weight))
        output[index, finite] = (clean - mean) / np.sqrt(variance) if variance > 0 else 0.0
    return output


def _python_rolling(values: np.ndarray, window: int, operation: str) -> np.ndarray:
    if window <= 0:
        raise ValueError("window 必须大于 0")
    output = np.full_like(values, np.nan)
    minimum = max(2, window // 2)
    for column in range(values.shape[1]):
        for stop in range(values.shape[0]):
            sample = values[max(0, stop - window + 1):stop + 1, column]
            sample = sample[np.isfinite(sample)]
            if len(sample) < minimum:
                continue
            output[stop, column] = (
                float(np.mean(sample)) if operation == "mean" else float(np.std(sample, ddof=1))
            )
    return output


def _python_rolling_corr(left: np.ndarray, right: np.ndarray, window: int) -> np.ndarray:
    if left.shape != right.shape or window <= 0:
        raise ValueError("rolling_corr 参数非法")
    output = np.full_like(left, np.nan)
    minimum = max(3, window // 2)
    for column in range(left.shape[1]):
        for stop in range(left.shape[0]):
            a = left[max(0, stop - window + 1):stop + 1, column]
            b = right[max(0, stop - window + 1):stop + 1, column]
            finite = np.isfinite(a) & np.isfinite(b)
            if finite.sum() < minimum:
                continue
            a, b = a[finite], b[finite]
            if np.std(a) > 0 and np.std(b) > 0:
                output[stop, column] = float(np.corrcoef(a, b)[0, 1])
    return output


@dataclass
class Kernel:
    requested: KernelBackend = KernelBackend.AUTO

    def __post_init__(self) -> None:
        self.backend_used = KernelBackend.PYTHON
        self.fallback_reason = ""
        self._native = None
        if self.requested == KernelBackend.PYTHON:
            return
        try:
            self._native = importlib.import_module("_quantmaster_kernel")
            self.backend_used = KernelBackend.RUST
        except Exception as exc:
            self.fallback_reason = f"Rust 内核不可用: {exc}"
            if self.requested == KernelBackend.RUST:
                logger.warning("%s；回退 Python", self.fallback_reason)

    @property
    def native_version(self) -> str:
        if self._native is None:
            return ""
        try:
            return str(self._native.version())
        except Exception:
            return "unknown"

    def _call(
        self,
        native_name: str,
        native_args: tuple[Any, ...],
        fallback: Callable[[], np.ndarray],
    ) -> np.ndarray:
        if self._native is None:
            return fallback()
        try:
            return np.asarray(getattr(self._native, native_name)(*native_args), dtype="float64")
        except Exception as exc:
            self.fallback_reason = f"Rust {native_name} 失败: {exc}"
            self.backend_used = KernelBackend.PYTHON
            logger.warning("%s；本次运行回退 Python", self.fallback_reason)
            self._native = None
            return fallback()

    def cross_section_rank(self, values: Any) -> np.ndarray:
        matrix = _matrix(values)
        return self._call("cross_section_rank", (matrix,), lambda: _python_rank(matrix))

    def robust_standardize(self, values: Any, k: float = 5.0) -> np.ndarray:
        matrix = _matrix(values)
        normalized_k = float(k)
        if not np.isfinite(normalized_k) or normalized_k <= 0:
            raise ValueError("k 必须是有限正数")
        return self._call(
            "robust_standardize", (matrix, normalized_k),
            lambda: _python_robust_standardize(matrix, normalized_k),
        )

    def weighted_zscore(self, values: Any, weights: Any) -> np.ndarray:
        matrix, weight_matrix = _matrix(values), _matrix(weights)
        return self._call(
            "weighted_zscore", (matrix, weight_matrix),
            lambda: _python_weighted_zscore(matrix, weight_matrix),
        )

    def rolling_mean(self, values: Any, window: int) -> np.ndarray:
        matrix = _matrix(values)
        return self._call(
            "rolling_mean", (matrix, int(window)),
            lambda: _python_rolling(matrix, int(window), "mean"),
        )

    def rolling_std(self, values: Any, window: int) -> np.ndarray:
        matrix = _matrix(values)
        return self._call(
            "rolling_std", (matrix, int(window)),
            lambda: _python_rolling(matrix, int(window), "std"),
        )

    def rolling_corr(self, left: Any, right: Any, window: int) -> np.ndarray:
        a, b = _matrix(left), _matrix(right)
        return self._call(
            "rolling_corr", (a, b, int(window)),
            lambda: _python_rolling_corr(a, b, int(window)),
        )


def kernel_capabilities() -> dict[str, Any]:
    kernel = Kernel(KernelBackend.AUTO)
    return {
        "requested": KernelBackend.AUTO.value,
        "backend": kernel.backend_used.value,
        "native_version": kernel.native_version,
        "fallback_reason": kernel.fallback_reason,
        "operators": [
            "cross_section_rank", "robust_standardize", "weighted_zscore",
            "rolling_mean", "rolling_std", "rolling_corr",
        ],
    }
