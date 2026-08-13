"""Quant Lab 的可选机器学习后端。

核心包不强制安装 PyTorch。Ridge 使用 scikit-learn；序列模型在安装
``quantmaster[ml]`` 后启用，并共享同一套时序切分、Huber 损失和早停规则。
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import math
import os
import random
import shutil
import subprocess
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from quantmaster.config import get_config
from quantmaster.lab.cache import feature_cache_lock
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.runtime.paths import confined_path

MODEL_KINDS = ("ridge", "mlp", "tcn", "gru", "transformer", "dae")
Progress = Callable[[int, str], None]
Cancelled = Callable[[], bool]


def capabilities() -> dict[str, Any]:
    torch_available = importlib.util.find_spec("torch") is not None
    sklearn_available = importlib.util.find_spec("sklearn") is not None
    cuda = torch_available and _cuda_available()
    gpu: dict[str, Any] = {"available": cuda, "hardware_available": False}
    torch_version = ""
    cuda_runtime = ""
    if torch_available:
        try:
            import torch

            torch_version = str(torch.__version__)
            cuda_runtime = str(torch.version.cuda or "")
            if cuda:
                properties = torch.cuda.get_device_properties(0)
                gpu.update({
                    "hardware_available": True,
                    "name": properties.name,
                    "memory_gb": round(properties.total_memory / (1024 ** 3), 2),
                    "compute_capability": list(torch.cuda.get_device_capability(0)),
                    "mixed_precision": True,
                })
        except Exception:
            pass
    if not gpu["hardware_available"]:
        try:
            result = subprocess.run(
                [
                    "nvidia-smi", "--query-gpu=name,memory.total,driver_version",
                    "--format=csv,noheader,nounits",
                ],
                check=False, capture_output=True, text=True, timeout=2,
            )
            if result.returncode == 0 and result.stdout.strip():
                name, memory, driver = [part.strip() for part in result.stdout.splitlines()[0].split(",")]
                gpu.update({
                    "hardware_available": True, "name": name,
                    "memory_gb": round(float(memory) / 1024, 2), "driver": driver,
                })
        except (OSError, ValueError, subprocess.SubprocessError):
            pass
    requested = str(get_config().lab.device or "auto")
    effective = "cuda:0" if cuda and requested in {"auto", "cuda"} else "cpu"
    return {
        "available_models": [
            name for name in MODEL_KINDS
            if (name == "ridge" and sklearn_available) or (name != "ridge" and torch_available)
        ],
        "torch": torch_available,
        "torch_version": torch_version,
        "cuda_runtime": cuda_runtime,
        "torch_build": "cuda" if cuda_runtime else "cpu" if torch_available else "missing",
        "sklearn": sklearn_available,
        "requested_device": requested,
        "device": effective,
        "gpu": gpu,
        "optuna": importlib.util.find_spec("optuna") is not None,
        "multi_horizon_models": [
            name for name in ("multi-transformer", "multi-tcn", "multi-gru", "ridge")
            if (name == "ridge" and sklearn_available) or (name != "ridge" and torch_available)
        ],
    }


def _cuda_available() -> bool:
    try:
        import torch

        return bool(torch.cuda.is_available())
    except Exception:
        return False


def _engineered_features(panel: dict[str, pd.DataFrame]):
    """Yield features one at a time so a full research run stays memory bounded."""
    close = panel["close"].astype(float)
    volume = panel.get("volume", close * np.nan).astype(float)
    amount = panel.get("amount", volume * close).astype(float)
    high = panel.get("high", close).astype(float)
    low = panel.get("low", close).astype(float)
    opened = panel.get("open", close).astype(float)
    returns = close.pct_change(fill_method=None)

    for window in (1, 2, 3, 5, 10, 20, 40, 60, 120):
        yield f"return_{window}", close.pct_change(window, fill_method=None)
    for window in (3, 5, 10, 20, 40, 60, 120):
        yield f"volatility_{window}", returns.rolling(window).std()
    for window in (5, 10, 20, 60):
        yield f"mean_return_{window}", returns.rolling(window).mean()
    for window in (5, 10, 20, 60, 120):
        yield f"price_bias_{window}", close / close.rolling(window).mean() - 1
    volume_safe = volume.replace(0, np.nan)
    amount_safe = amount.replace(0, np.nan)
    for window in (3, 5, 10, 20, 60):
        yield f"volume_ratio_{window}", volume_safe / volume_safe.rolling(window).mean() - 1
    for window in (5, 10, 20, 60):
        yield f"amount_ratio_{window}", amount_safe / amount_safe.rolling(window).mean() - 1
    price_range = (high - low) / close.replace(0, np.nan)
    yield "intraday_return", close / opened.replace(0, np.nan) - 1
    yield "overnight_return", opened / close.shift(1).replace(0, np.nan) - 1
    yield "range_1", price_range
    for window in (5, 10, 20):
        yield f"range_mean_{window}", price_range.rolling(window).mean()
    yield "close_location", (
        (close - low) / (high - low).replace(0, np.nan) - 0.5
    )
    for window in (10, 20, 60):
        minimum = low.rolling(window).min()
        maximum = high.rolling(window).max()
        yield f"price_position_{window}", (
            (close - minimum) / (maximum - minimum).replace(0, np.nan) - 0.5
        )
    yield "volume_price_corr_10", returns.rolling(10).corr(
        volume_safe.pct_change(fill_method=None)
    )
    yield "volume_price_corr_20", returns.rolling(20).corr(
        volume_safe.pct_change(fill_method=None)
    )
    yield "return_skew_20", returns.rolling(20).skew()
    yield "return_kurt_20", returns.rolling(20).kurt()


def engineer_features(panel: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """从可靠日线字段构造 48 个无未来信息的模型输入特征。"""
    features = dict(_engineered_features(panel))
    if len(features) != 48:
        raise AssertionError(f"模型特征应为 48 个，实际 {len(features)}")
    return features


def normalize_features(
    features: dict[str, pd.DataFrame],
) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    """Cross-sectionally winsorize/z-score features using same-date data only.

    Missing values become the same-date cross-sectional median (zero after
    standardization).  A separate validity mask is retained so callers can
    enforce coverage rather than confusing imputation with observed data.
    """
    normalized: dict[str, pd.DataFrame] = {}
    validity: dict[str, pd.DataFrame] = {}
    for name, raw in features.items():
        values = raw.astype(float).replace([np.inf, -np.inf], np.nan)
        valid = values.notna()
        lower = values.quantile(0.01, axis=1)
        upper = values.quantile(0.99, axis=1)
        clipped = values.clip(lower=lower, upper=upper, axis=0)
        mean = clipped.mean(axis=1)
        std = clipped.std(axis=1, ddof=0).replace(0, np.nan)
        normalized[name] = clipped.sub(mean, axis=0).div(std, axis=0).fillna(0.0)
        validity[name] = valid
    return normalized, validity


def _feature_cube(
    panel: dict[str, pd.DataFrame],
) -> tuple[np.ndarray, np.ndarray, pd.DatetimeIndex, pd.Index, list[str]]:
    return _feature_cube_cached(panel)


def _feature_cube_cached(
    panel: dict[str, pd.DataFrame], *, storage_dir: str | Path | None = None,
) -> tuple[np.ndarray, np.ndarray, pd.DatetimeIndex, pd.Index, list[str]]:
    root = Path(storage_dir) if storage_dir is not None else None
    if root is None:
        return _feature_cube_cached_unlocked(panel)
    with feature_cache_lock(root):
        return _feature_cube_cached_unlocked(panel, storage_dir=root)


def _feature_cube_cached_unlocked(
    panel: dict[str, pd.DataFrame], *, storage_dir: str | Path | None = None,
) -> tuple[np.ndarray, np.ndarray, pd.DatetimeIndex, pd.Index, list[str]]:
    """Build a float32 cube and daily validity counts, optionally as a reusable memmap."""
    close = panel["close"].astype(float)
    indexes = pd.DatetimeIndex(close.index)
    columns = close.columns
    root = Path(storage_dir) if storage_dir is not None else None
    cube_file = root / "feature-cube.npy" if root is not None else None
    counts_file = root / "valid-counts.npy" if root is not None else None
    metadata_file = root / "cube.json" if root is not None else None
    if root is not None and cube_file and counts_file and metadata_file:
        try:
            metadata = json.loads(metadata_file.read_text(encoding="utf-8"))
            if (
                metadata.get("feature_version") == "lab-v3-indexed"
                and metadata.get("dates") == indexes.strftime("%Y-%m-%d").tolist()
                and metadata.get("symbols") == columns.astype(str).tolist()
            ):
                cube = np.load(cube_file, mmap_mode="r")
                valid_counts = np.load(counts_file, mmap_mode="r")
                if cube.shape[:2] == (len(indexes), len(columns)):
                    os.utime(root, None)
                    return cube, valid_counts, indexes, columns, list(metadata["features"])
        except (OSError, ValueError, KeyError, json.JSONDecodeError):
            pass

    feature_count = 48
    shape = (len(indexes), len(columns), feature_count)
    if root is None:
        cube = np.empty(shape, dtype=np.float32)
        valid_counts = np.zeros(shape[:2], dtype=np.uint8)
    else:
        root.mkdir(parents=True, exist_ok=True)
        cube_partial = root / "feature-cube.partial.npy"
        counts_partial = root / "valid-counts.partial.npy"
        cube = np.lib.format.open_memmap(cube_partial, mode="w+", dtype=np.float32, shape=shape)
        valid_counts = np.lib.format.open_memmap(
            counts_partial, mode="w+", dtype=np.uint8, shape=shape[:2],
        )
        valid_counts[:] = 0

    names: list[str] = []
    for feature_position, (name, raw) in enumerate(_engineered_features(panel)):
        if feature_position >= feature_count:
            raise AssertionError("模型特征超过 48 个")
        values = raw.astype(float).replace([np.inf, -np.inf], np.nan)
        valid = values.notna()
        lower = values.quantile(0.01, axis=1)
        upper = values.quantile(0.99, axis=1)
        clipped = values.clip(lower=lower, upper=upper, axis=0)
        mean = clipped.mean(axis=1)
        std = clipped.std(axis=1, ddof=0).replace(0, np.nan)
        normalized = clipped.sub(mean, axis=0).div(std, axis=0).fillna(0.0)
        cube[:, :, feature_position] = normalized.reindex(
            index=indexes, columns=columns,
        ).to_numpy(np.float32)
        valid_counts[:] += valid.reindex(
            index=indexes, columns=columns,
        ).fillna(False).to_numpy(np.uint8)
        names.append(name)
    if len(names) != feature_count:
        raise AssertionError(f"模型特征应为 48 个，实际 {len(names)}")

    if root is not None and cube_file and counts_file and metadata_file:
        cube.flush()
        valid_counts.flush()
        del cube, valid_counts
        os.replace(root / "feature-cube.partial.npy", cube_file)
        os.replace(root / "valid-counts.partial.npy", counts_file)
        metadata_partial = root / "cube.partial.json"
        metadata_partial.write_text(strict_json_dumps({
            "feature_version": "lab-v3-indexed",
            "dates": indexes.strftime("%Y-%m-%d").tolist(),
            "symbols": columns.astype(str).tolist(),
            "features": names,
        }, indent=2), encoding="utf-8")
        os.replace(metadata_partial, metadata_file)
        cube = np.load(cube_file, mmap_mode="r")
        valid_counts = np.load(counts_file, mmap_mode="r")
    return cube, valid_counts, indexes, columns, names


@dataclass
class IndexedSamples:
    """Compact sample index over a shared date × symbol × feature cube."""

    cube: np.ndarray
    date_positions: np.ndarray
    symbol_positions: np.ndarray
    targets: np.ndarray
    dates: pd.DatetimeIndex
    symbols: pd.Index
    feature_names: list[str]
    sequence_length: int
    horizon: int
    cache_hit: bool = False

    def __len__(self) -> int:
        return len(self.targets)

    def metadata_frame(self, positions: np.ndarray | slice | None = None) -> pd.DataFrame:
        selection = np.arange(len(self), dtype=np.int64) if positions is None else positions
        date_positions = np.asarray(self.date_positions[selection], dtype=np.int64)
        symbol_positions = np.asarray(self.symbol_positions[selection], dtype=np.int64)
        return pd.DataFrame({
            "date": self.dates.take(date_positions),
            "target_date": self.dates.take(date_positions + self.horizon),
            "symbol": self.symbols.take(symbol_positions).astype(str),
        })

    def window(self, position: int) -> np.ndarray:
        date_position = int(self.date_positions[position])
        symbol_position = int(self.symbol_positions[position])
        return np.asarray(
            self.cube[
                date_position - self.sequence_length + 1:date_position + 1,
                symbol_position,
                :,
            ],
            dtype=np.float32,
        )


def _prune_feature_cache(parent: Path, *, keep: Path) -> None:
    limit = max(0, int(get_config().lab.feature_cache_gb)) * 1024 ** 3
    if not limit or not parent.is_dir():
        return
    entries: list[tuple[int, int, Path]] = []
    total = 0
    for child in parent.iterdir():
        if not child.is_dir():
            continue
        try:
            size = sum(item.stat().st_size for item in child.rglob("*") if item.is_file())
            modified = child.stat().st_mtime_ns
        except OSError:
            continue
        total += size
        entries.append((modified, size, child))
    active_threshold = time.time_ns() - 2 * 60 * 60 * 1_000_000_000
    for modified, size, child in sorted(entries):
        if total <= limit:
            break
        if child.resolve() == keep.resolve() or modified >= active_threshold:
            continue
        shutil.rmtree(child, ignore_errors=True)
        total -= size


def _atomic_save_array(path: Path, values: np.ndarray) -> None:
    partial = path.with_name(
        f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.partial",
    )
    try:
        with partial.open("wb") as stream:
            np.save(stream, values)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(partial, path)
    finally:
        partial.unlink(missing_ok=True)


def make_indexed_samples(
    panel: dict[str, pd.DataFrame], *, horizon: int = 3, sequence_length: int = 20,
    membership: pd.DataFrame | None = None, storage_dir: str | Path | None = None,
    minimum_coverage: float = 0.80,
) -> IndexedSamples:
    """Create compact indices; sequence windows are sliced only for the active batch."""
    if horizon not in {1, 3, 5, 7, 10, 20, 30}:
        raise ValueError("horizon 只支持 1/3/5/7/10/20/30 日")
    if sequence_length < 1:
        raise ValueError("sequence_length 必须为正整数")
    root = Path(storage_dir) if storage_dir is not None else None
    cube, valid_counts, dates, symbols, names = _feature_cube_cached(
        panel, storage_dir=root,
    )
    sample_root = root / f"samples-{sequence_length}-{horizon}" if root is not None else None
    cache_files = (
        [sample_root / name for name in ("date-positions.npy", "symbol-positions.npy", "targets.npy")]
        if sample_root is not None else []
    )
    if sample_root is not None and all(path.is_file() for path in cache_files):
        result = IndexedSamples(
            cube=cube,
            date_positions=np.load(cache_files[0], mmap_mode="r"),
            symbol_positions=np.load(cache_files[1], mmap_mode="r"),
            targets=np.load(cache_files[2], mmap_mode="r"),
            dates=dates, symbols=symbols, feature_names=names,
            sequence_length=sequence_length, horizon=horizon, cache_hit=True,
        )
        os.utime(sample_root.parent, None)
        return result

    close = panel["close"].reindex(index=dates, columns=symbols).to_numpy(np.float32)
    targets_by_cell = np.full(close.shape, np.nan, dtype=np.float32)
    raw_targets = close[horizon:] / close[:-horizon] - 1
    medians = np.nanmedian(raw_targets, axis=1)
    targets_by_cell[:-horizon] = raw_targets - medians[:, None]
    cumulative: Any = np.zeros((len(dates) + 1, len(symbols)), dtype=np.int32)
    np.cumsum(valid_counts, axis=0, dtype=np.int32, out=cumulative[1:])
    rolling = cumulative[sequence_length:] - cumulative[:-sequence_length]
    candidate_dates: Any = np.arange(
        sequence_length - 1, len(dates) - horizon, dtype=np.int32,
    )
    required = math.ceil(minimum_coverage * sequence_length * len(names))
    eligible = rolling[:len(candidate_dates)] >= required
    candidate_targets = targets_by_cell[candidate_dates]
    eligible &= np.isfinite(candidate_targets)
    if membership is not None:
        members = membership.reindex(index=dates, columns=symbols).fillna(False).to_numpy(bool)
        eligible &= members[candidate_dates]
    flat_positions = np.flatnonzero(eligible.ravel())
    if not len(flat_positions):
        raise ValueError("清洗后没有可训练样本；请扩大日期范围或检查数据覆盖率")
    row_positions: Any = (flat_positions // len(symbols)).astype(np.int32)
    symbol_positions: Any = (flat_positions % len(symbols)).astype(np.int32)
    date_positions = candidate_dates[row_positions]
    targets = candidate_targets[row_positions, symbol_positions].astype(np.float32, copy=False)
    if sample_root is not None:
        sample_root.mkdir(parents=True, exist_ok=True)
        _atomic_save_array(cache_files[0], date_positions)
        _atomic_save_array(cache_files[1], symbol_positions)
        _atomic_save_array(cache_files[2], targets)
        date_positions = np.load(cache_files[0], mmap_mode="r")
        symbol_positions = np.load(cache_files[1], mmap_mode="r")
        targets = np.load(cache_files[2], mmap_mode="r")
        assert root is not None
        _prune_feature_cache(root.parent, keep=root)
    return IndexedSamples(
        cube=cube, date_positions=date_positions, symbol_positions=symbol_positions,
        targets=targets, dates=dates, symbols=symbols, feature_names=names,
        sequence_length=sequence_length, horizon=horizon,
    )


def make_samples(
    panel: dict[str, pd.DataFrame],
    *,
    horizon: int = 3,
    sequence_length: int = 20,
    membership: pd.DataFrame | None = None,
) -> tuple[np.ndarray, np.ndarray, list[dict[str, str]], list[str]]:
    """将面板转换为按日期顺序排列的截面样本，避免随机切分造成泄漏。"""
    if horizon not in {1, 3, 5, 7, 10, 20, 30}:
        raise ValueError("horizon 只支持 1/3/5/7/10/20/30 日")
    if sequence_length < 1:
        raise ValueError("sequence_length 必须为正整数")
    close = panel["close"].astype(float)
    cube, valid_counts, indexes, columns, names = _feature_cube(panel)
    raw_target = close.shift(-horizon) / close - 1
    excess_target = raw_target.sub(raw_target.median(axis=1), axis=0)
    target = excess_target.reindex(index=indexes, columns=columns).to_numpy(float)
    member_values = None
    if membership is not None:
        member_values = membership.reindex(index=indexes, columns=columns).fillna(False).to_numpy(bool)

    samples: list[np.ndarray] = []
    labels: list[float] = []
    metadata: list[dict[str, str]] = []
    start = sequence_length - 1
    for date_pos in range(start, len(indexes) - horizon):
        cross_section = cube[date_pos - sequence_length + 1:date_pos + 1]
        y_values = target[date_pos]
        for symbol_pos, symbol in enumerate(columns):
            if member_values is not None and not member_values[date_pos, symbol_pos]:
                continue
            sample = cross_section[:, symbol_pos, :]
            label = y_values[symbol_pos]
            coverage = float(
                valid_counts[
                    date_pos - sequence_length + 1:date_pos + 1, symbol_pos,
                ].sum() / (sequence_length * len(names))
            )
            if coverage >= 0.80 and np.isfinite(sample).all() and np.isfinite(label):
                samples.append(sample.astype(np.float32))
                labels.append(float(label))
                metadata.append({
                    "date": pd.Timestamp(indexes[date_pos]).strftime("%Y-%m-%d"),
                    "target_date": pd.Timestamp(indexes[date_pos + horizon]).strftime("%Y-%m-%d"),
                    "symbol": str(symbol),
                })
    if not samples:
        raise ValueError("清洗后没有可训练样本；请扩大日期范围或检查数据覆盖率")
    return np.stack(samples), np.asarray(labels, dtype=np.float32), metadata, names


def make_inference_samples(
    panel: dict[str, pd.DataFrame], *, sequence_length: int = 20,
    minimum_coverage: float = 0.80,
) -> tuple[np.ndarray, list[dict[str, str]], list[str]]:
    """Build label-free samples with the exact training preprocessing path."""
    if sequence_length < 1:
        raise ValueError("sequence_length 必须为正整数")
    cube, valid_counts, indexes, columns, names = _feature_cube(panel)
    samples: list[np.ndarray] = []
    metadata: list[dict[str, str]] = []
    for date_pos in range(sequence_length - 1, len(indexes)):
        window = cube[date_pos - sequence_length + 1:date_pos + 1]
        for symbol_pos, symbol in enumerate(columns):
            sample = window[:, symbol_pos, :]
            coverage = float(
                valid_counts[
                    date_pos - sequence_length + 1:date_pos + 1, symbol_pos,
                ].sum() / (sequence_length * len(names))
            )
            if coverage >= minimum_coverage and np.isfinite(sample).all():
                samples.append(sample.astype(np.float32))
                metadata.append({
                    "date": pd.Timestamp(indexes[date_pos]).strftime("%Y-%m-%d"),
                    "symbol": str(symbol),
                })
    if not samples:
        raise ValueError("没有满足 80% 特征覆盖率的推理样本")
    return np.stack(samples), metadata, names


def _split_by_date(metadata: list[dict[str, str]], validation_ratio: float) -> int:
    dates = sorted({item["date"] for item in metadata})
    if len(dates) < 20:
        raise ValueError("训练数据至少需要 20 个有效交易日")
    cutoff_date = dates[max(1, min(len(dates) - 1, int(len(dates) * (1 - validation_ratio))))]
    cutoff = next(index for index, item in enumerate(metadata) if item["date"] >= cutoff_date)
    return cutoff


def _metrics(actual: np.ndarray, predicted: np.ndarray) -> dict[str, float]:
    actual = np.asarray(actual, dtype=float)
    predicted = np.asarray(predicted, dtype=float)
    mse = float(np.mean((actual - predicted) ** 2))
    correlation = float(np.corrcoef(actual, predicted)[0, 1]) if len(actual) > 1 else 0.0
    if not np.isfinite(correlation):
        correlation = 0.0
    return {"mse": round(mse, 8), "correlation": round(correlation, 6)}


def _seed_everything(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)


def train(
    kind: str,
    samples: np.ndarray,
    targets: np.ndarray,
    metadata: list[dict[str, str]],
    *,
    artifact_dir: str | Path,
    config: dict[str, Any] | None = None,
    progress: Progress | None = None,
    cancelled: Cancelled | None = None,
) -> dict[str, Any]:
    """训练并保存模型工件；返回可直接写入实验账本的指标。"""
    if kind not in MODEL_KINDS:
        raise ValueError(f"未知模型: {kind}")
    config = dict(config or {})
    seed = int(config.get("seed", 42))
    _seed_everything(seed)
    cutoff = _split_by_date(metadata, float(config.get("validation_ratio", 0.2)))
    validation_start = metadata[cutoff]["date"]
    # Purge training rows whose forward-return label reaches into the OOS block.
    # This is the holding-period embargo needed to keep the saved model honest.
    training_positions = [
        index for index, item in enumerate(metadata[:cutoff])
        if item.get("target_date", item["date"]) < validation_start
    ]
    x_train, x_valid = samples[training_positions], samples[cutoff:]
    y_train, y_valid = targets[training_positions], targets[cutoff:]
    if min(len(x_train), len(x_valid)) < 10:
        raise ValueError("训练集或验证集样本不足")
    artifact_path = Path(artifact_dir)
    artifact_path.mkdir(parents=True, exist_ok=True)
    if progress:
        progress(10, "准备训练样本")

    if kind == "ridge":
        result = _train_ridge(
            x_train, y_train, x_valid, y_valid, artifact_path, config, progress, cancelled
        )
    else:
        result = _train_torch(
            kind, x_train, y_train, x_valid, y_valid, artifact_path, config, progress, cancelled
        )
    result["fit_through"] = max(
        metadata[index].get("target_date", metadata[index]["date"])
        for index in training_positions
    )
    result["validation_start"] = validation_start
    result["_validation_metadata"] = metadata[cutoff:]
    return result


def _indexed_split(
    samples: IndexedSamples, validation_ratio: float,
) -> tuple[np.ndarray, np.ndarray, pd.Timestamp, pd.Timestamp]:
    unique_dates = np.unique(np.asarray(samples.date_positions, dtype=np.int32))
    if len(unique_dates) < 20:
        raise ValueError("训练数据至少需要 20 个有效交易日")
    cutoff_position = max(
        1, min(len(unique_dates) - 1, int(len(unique_dates) * (1 - validation_ratio))),
    )
    validation_date_position = int(unique_dates[cutoff_position])
    first_validation = int(
        np.searchsorted(samples.date_positions, validation_date_position, side="left")
    )
    candidate_training: Any = np.arange(first_validation, dtype=np.int64)
    training = candidate_training[
        np.asarray(samples.date_positions[:first_validation], dtype=np.int64)
        + samples.horizon < validation_date_position
    ]
    validation: Any = np.arange(first_validation, len(samples), dtype=np.int64)
    if min(len(training), len(validation)) < 10:
        raise ValueError("训练集或验证集样本不足")
    fit_through_position = int(
        np.max(np.asarray(samples.date_positions[training], dtype=np.int64) + samples.horizon)
    )
    return (
        training, validation, samples.dates[fit_through_position],
        samples.dates[validation_date_position],
    )


def train_indexed(
    kind: str, samples: IndexedSamples, *, artifact_dir: str | Path,
    config: dict[str, Any] | None = None, progress: Progress | None = None,
    cancelled: Cancelled | None = None,
) -> dict[str, Any]:
    """Train from compact positions without materializing every sequence window."""
    if kind not in MODEL_KINDS:
        raise ValueError(f"未知模型: {kind}")
    values = dict(config or {})
    seed = int(values.get("seed", 42))
    _seed_everything(seed)
    training, validation, fit_through, validation_start = _indexed_split(
        samples, float(values.get("validation_ratio", 0.2)),
    )
    artifact_path = Path(artifact_dir)
    artifact_path.mkdir(parents=True, exist_ok=True)
    if progress:
        progress(10, f"准备 {len(samples):,} 个索引样本")
    if kind == "ridge":
        result = _train_indexed_ridge(
            samples, training, validation, artifact_path, values, progress, cancelled,
        )
    else:
        result = _train_indexed_torch(
            kind, samples, training, validation, artifact_path, values, progress, cancelled,
        )
    result.update({
        "fit_through": fit_through.strftime("%Y-%m-%d"),
        "validation_start": validation_start.strftime("%Y-%m-%d"),
        "_validation_positions": validation,
    })
    return result


def _indexed_last_values(samples: IndexedSamples, positions: np.ndarray) -> np.ndarray:
    date_positions = np.asarray(samples.date_positions[positions], dtype=np.int64)
    symbol_positions = np.asarray(samples.symbol_positions[positions], dtype=np.int64)
    return np.asarray(samples.cube[date_positions, symbol_positions, :], dtype=np.float32)


def _train_indexed_ridge(
    samples: IndexedSamples, training: np.ndarray, validation: np.ndarray,
    artifact_dir: Path, config: dict[str, Any], progress: Progress | None,
    cancelled: Cancelled | None,
) -> dict[str, Any]:
    started = time.perf_counter()
    feature_count = len(samples.feature_names)
    gram: Any = np.zeros((feature_count, feature_count), dtype=np.float64)
    feature_sum: Any = np.zeros(feature_count, dtype=np.float64)
    feature_target: Any = np.zeros(feature_count, dtype=np.float64)
    target_sum = 0.0
    chunk_size = max(4096, int(config.get("ridge_chunk_size", 65536)))
    for offset in range(0, len(training), chunk_size):
        if cancelled and cancelled():
            raise InterruptedError("训练已取消")
        positions = training[offset:offset + chunk_size]
        values: Any = _indexed_last_values(samples, positions).astype(
            np.float64, copy=False,
        )
        targets = np.asarray(samples.targets[positions], dtype=np.float64)
        gram += values.T @ values
        feature_sum += values.sum(axis=0)
        feature_target += values.T @ targets
        target_sum += float(targets.sum())
        if progress:
            progress(
                12 + int(68 * min(offset + len(positions), len(training)) / len(training)),
                "流式拟合 Ridge",
            )
    alpha = float(config.get("alpha", 1.0))
    system: Any = np.empty((feature_count + 1, feature_count + 1), dtype=np.float64)
    system[:feature_count, :feature_count] = gram
    system[:feature_count, feature_count] = feature_sum
    system[feature_count, :feature_count] = feature_sum
    system[feature_count, feature_count] = len(training)
    system[:feature_count, :feature_count] += np.eye(feature_count) * alpha
    right_hand: Any = np.append(feature_target, target_sum)
    try:
        solution = np.linalg.solve(system, right_hand)
    except np.linalg.LinAlgError:
        solution = np.linalg.lstsq(system, right_hand, rcond=None)[0]
    coefficient = solution[:-1].astype(np.float32)
    intercept = float(solution[-1])
    predicted: Any = np.empty(len(validation), dtype=np.float32)
    for offset in range(0, len(validation), chunk_size):
        positions = validation[offset:offset + chunk_size]
        predicted[offset:offset + len(positions)] = (
            _indexed_last_values(samples, positions) @ coefficient + intercept
        )
    artifact = artifact_dir / "ridge.npz"
    np.savez_compressed(
        artifact, coef=coefficient, intercept=np.asarray([intercept], dtype=np.float64),
    )
    elapsed = max(time.perf_counter() - started, 1e-9)
    if progress:
        progress(95, "保存 Ridge 工件")
    return {
        "kind": "ridge", "artifact": str(artifact.resolve()),
        "train_samples": len(training), "validation_samples": len(validation),
        "device": "cpu", "requested_device": str(config.get("device", "auto")),
        "metrics": _metrics(samples.targets[validation], predicted), "config": config,
        "telemetry": {
            "resource_class": "cpu", "requested_device": str(config.get("device", "auto")),
            "effective_device": "cpu", "cpu_bound": True,
            "elapsed_seconds": round(elapsed, 4),
            "samples_per_second": round(len(training) / elapsed, 2),
            "peak_gpu_memory_mb": 0.0,
        },
        "_predicted": predicted,
        "_actual": np.asarray(samples.targets[validation], dtype=np.float32),
    }


def _resolve_torch_device(torch, requested: str):
    from quantmaster.lab.errors import LabError

    normalized = requested.strip().lower() or "auto"
    if normalized == "auto":
        return torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
    if normalized == "cuda":
        normalized = "cuda:0"
    if normalized.startswith("cuda") and not torch.cuda.is_available():
        raise LabError(
            "CUDA_UNAVAILABLE", "任务明确要求 CUDA，但当前 PyTorch 无法使用 GPU",
            action="安装 CUDA 版 PyTorch，并运行 qm lab doctor 验证 torch.cuda.is_available()",
            context={"requested_device": requested}, status_code=409,
        )
    try:
        device = torch.device(normalized)
        if device.type == "cuda":
            torch.cuda.get_device_properties(device)
        return device
    except (AssertionError, RuntimeError, ValueError) as exc:
        raise LabError(
            "CUDA_UNAVAILABLE", f"请求的计算设备不可用：{requested}",
            action="检查 lab.device 与可见 GPU 编号", context={"requested_device": requested},
            status_code=409,
        ) from exc


def _train_indexed_torch(
    kind: str, samples: IndexedSamples, training: np.ndarray, validation: np.ndarray,
    artifact_dir: Path, config: dict[str, Any], progress: Progress | None,
    cancelled: Cancelled | None,
) -> dict[str, Any]:
    try:
        import torch
        from torch import nn
        from torch.utils.data import DataLoader, Dataset
    except ImportError as exc:
        raise RuntimeError("深度模型需要安装 PyTorch：pip install 'quantmaster[ml]'") from exc
    from quantmaster.lab.errors import LabError

    started = time.perf_counter()
    requested = str(config.get("device", get_config().lab.device or "auto"))
    device = _resolve_torch_device(torch, requested)
    torch.manual_seed(int(config.get("seed", 42)))
    if device.type == "cuda":
        torch.cuda.set_device(device)
        torch.cuda.empty_cache()
        torch.cuda.reset_peak_memory_stats(device)
        try:
            torch.cuda.set_per_process_memory_fraction(
                float(config.get("gpu_memory_fraction", get_config().lab.gpu_memory_fraction)),
                device,
            )
        except (RuntimeError, ValueError):
            pass
    model_class = _torch_models(
        samples.cube.shape[-1], samples.sequence_length,
    )[kind]
    model = model_class().to(device)
    optimizer = torch.optim.AdamW(
        model.parameters(), lr=float(config.get("learning_rate", 1e-3)), weight_decay=1e-4,
    )
    loss_function = nn.HuberLoss(delta=0.02)
    batch_size = max(1, int(config.get("batch_size", 256)))
    accumulation = max(1, int(config.get("gradient_accumulation", 1)))
    worker_count = max(0, int(config.get("loader_workers", 0)))
    # The collator closes over a memory map; Windows spawn cannot pickle it safely.
    if os.name == "nt":
        worker_count = 0

    class PositionDataset(Dataset):
        def __init__(self, positions: np.ndarray):
            self.positions = positions

        def __len__(self):
            return len(self.positions)

        def __getitem__(self, index):
            return int(self.positions[index])

    def collate(position_values):
        positions = np.asarray(position_values, dtype=np.int64)
        windows = np.empty(
            (len(positions), samples.sequence_length, samples.cube.shape[-1]),
            dtype=np.float32,
        )
        for row, position in enumerate(positions):
            windows[row] = samples.window(int(position))
        targets = np.asarray(samples.targets[positions], dtype=np.float32).copy()
        return torch.from_numpy(windows), torch.from_numpy(targets)

    loader_options: dict[str, Any] = {
        "batch_size": batch_size,
        "num_workers": worker_count,
        "pin_memory": device.type == "cuda",
        "collate_fn": collate,
    }
    if worker_count:
        loader_options.update({"prefetch_factor": 2, "persistent_workers": True})
    generator = torch.Generator().manual_seed(int(config.get("seed", 42)))
    loader = DataLoader(
        PositionDataset(training), shuffle=True, generator=generator, **loader_options,
    )
    validation_loader = DataLoader(
        PositionDataset(validation), shuffle=False, **loader_options,
    )
    use_amp = device.type == "cuda"
    use_bfloat16 = use_amp and bool(torch.cuda.is_bf16_supported())
    amp_dtype = torch.bfloat16 if use_bfloat16 else torch.float16
    amp_name = "bf16" if use_bfloat16 else "fp16" if use_amp else "off"
    scaler = torch.amp.GradScaler("cuda", enabled=use_amp and not use_bfloat16)

    try:
        probe_x, _probe_y = next(iter(loader))
        probe_x = probe_x[:batch_size].to(device, non_blocking=True)
        with torch.no_grad(), torch.autocast(
            device_type=device.type, dtype=amp_dtype, enabled=use_amp,
        ):
            model(probe_x)
        del probe_x
        if device.type == "cuda":
            torch.cuda.synchronize(device)
    except torch.cuda.OutOfMemoryError as exc:
        if device.type == "cuda":
            torch.cuda.empty_cache()
        raise LabError(
            "MEMORY_BUDGET_EXCEEDED",
            f"CUDA 批量探测失败，batch_size={batch_size} 超出显存预算",
            action="降低 batch_size 或提高 gradient_accumulation 后重试", retryable=True,
            context={"batch_size": batch_size, "device": str(device)}, status_code=409,
        ) from exc

    epochs = max(1, int(config.get("epochs", 30)))
    patience = max(2, int(config.get("patience", 5)))
    best_loss, best_state, stale = float("inf"), None, 0
    history: list[dict[str, Any]] = []
    for epoch in range(epochs):
        if cancelled and cancelled():
            raise InterruptedError("训练已取消")
        model.train()
        losses: list[float] = []
        optimizer.zero_grad(set_to_none=True)
        for step, (batch_x, batch_y) in enumerate(loader):
            batch_x = batch_x.to(device, non_blocking=True)
            batch_y = batch_y.to(device, non_blocking=True)
            with torch.autocast(device_type=device.type, dtype=amp_dtype, enabled=use_amp):
                predicted = model(batch_x)
                loss = loss_function(predicted, batch_y)
                if kind == "dae":
                    noisy = batch_x.clone()
                    noisy[:, -1] += torch.randn_like(noisy[:, -1]) * 0.05
                    loss = loss + 0.2 * nn.functional.mse_loss(
                        model.reconstruct(noisy), batch_x[:, -1],
                    )
                scaled_loss = loss / accumulation
            scaler.scale(scaled_loss).backward()
            if (step + 1) % accumulation == 0 or step + 1 == len(loader):
                scaler.unscale_(optimizer)
                nn.utils.clip_grad_norm_(model.parameters(), 2.0)
                scaler.step(optimizer)
                scaler.update()
                optimizer.zero_grad(set_to_none=True)
            losses.append(float(loss.detach().cpu()))
        model.eval()
        validation_total = 0.0
        validation_count = 0
        with torch.no_grad():
            for valid_x, valid_y in validation_loader:
                valid_x = valid_x.to(device, non_blocking=True)
                valid_y = valid_y.to(device, non_blocking=True)
                with torch.autocast(
                    device_type=device.type, dtype=amp_dtype, enabled=use_amp,
                ):
                    validation_loss = loss_function(model(valid_x), valid_y)
                validation_total += float(validation_loss.cpu()) * len(valid_x)
                validation_count += len(valid_x)
        mean_validation = validation_total / max(1, validation_count)
        history.append({
            "epoch": epoch + 1, "train_loss": round(float(np.mean(losses)), 8),
            "validation_loss": round(mean_validation, 8),
        })
        if progress:
            progress(15 + int(75 * (epoch + 1) / epochs), f"训练 {kind} · {epoch + 1}/{epochs}")
        if mean_validation < best_loss - 1e-7:
            best_loss = mean_validation
            best_state = {key: value.detach().cpu() for key, value in model.state_dict().items()}
            stale = 0
        else:
            stale += 1
            if stale >= patience:
                break
    if best_state is not None:
        model.load_state_dict(best_state)
    model.eval()
    predicted_parts: list[np.ndarray] = []
    with torch.no_grad():
        for valid_x, _valid_y in validation_loader:
            valid_x = valid_x.to(device, non_blocking=True)
            with torch.autocast(device_type=device.type, dtype=amp_dtype, enabled=use_amp):
                predicted_parts.append(model(valid_x).float().cpu().numpy())
    predicted = np.concatenate(predicted_parts).astype(np.float32, copy=False)
    model_file = artifact_dir / f"{kind}.pt"
    torch.save({
        "kind": kind, "state_dict": model.state_dict(), "config": config,
        "input_size": samples.cube.shape[-1], "sequence_length": samples.sequence_length,
    }, model_file)
    (artifact_dir / "history.json").write_text(
        strict_json_dumps(history, indent=2), encoding="utf-8",
    )
    if device.type == "cuda":
        torch.cuda.synchronize(device)
    elapsed = max(time.perf_counter() - started, 1e-9)
    peak_memory = (
        float(torch.cuda.max_memory_allocated(device) / 1024 ** 2)
        if device.type == "cuda" else 0.0
    )
    gpu_name = torch.cuda.get_device_name(device) if device.type == "cuda" else ""
    telemetry = {
        "resource_class": "gpu" if device.type == "cuda" else "cpu",
        "requested_device": requested, "effective_device": str(device),
        "gpu_name": gpu_name, "torch_version": str(torch.__version__),
        "cuda_runtime": str(torch.version.cuda or ""), "amp": amp_name,
        "batch_size": batch_size, "gradient_accumulation": accumulation,
        "loader_workers": worker_count,
        "effective_batch_size": batch_size * accumulation,
        "peak_gpu_memory_mb": round(peak_memory, 2),
        "elapsed_seconds": round(elapsed, 4),
        "samples_per_second": round(len(training) * len(history) / elapsed, 2),
    }
    return {
        "kind": kind, "artifact": str(model_file.resolve()),
        "train_samples": len(training), "validation_samples": len(validation),
        "device": str(device), "requested_device": requested,
        "epochs_completed": len(history),
        "metrics": _metrics(samples.targets[validation], predicted),
        "history": history, "config": config, "telemetry": telemetry,
        "_predicted": predicted,
        "_actual": np.asarray(samples.targets[validation], dtype=np.float32),
    }


def _train_ridge(
    x_train: np.ndarray,
    y_train: np.ndarray,
    x_valid: np.ndarray,
    y_valid: np.ndarray,
    artifact_dir: Path,
    config: dict[str, Any],
    progress: Progress | None,
    cancelled: Cancelled | None,
) -> dict[str, Any]:
    try:
        from sklearn.linear_model import Ridge
    except ImportError as exc:
        raise RuntimeError("Ridge 需要安装 scikit-learn：pip install 'quantmaster[ml]'") from exc
    if cancelled and cancelled():
        raise InterruptedError("训练已取消")
    model = Ridge(alpha=float(config.get("alpha", 1.0)))
    model.fit(x_train[:, -1, :], y_train)
    predicted = model.predict(x_valid[:, -1, :])
    np.savez_compressed(
        artifact_dir / "ridge.npz",
        coef=model.coef_, intercept=np.asarray([model.intercept_], dtype=float),
    )
    if progress:
        progress(95, "保存 Ridge 工件")
    return {
        "kind": "ridge",
        "artifact": str((artifact_dir / "ridge.npz").resolve()),
        "train_samples": len(x_train),
        "validation_samples": len(x_valid),
        "metrics": _metrics(y_valid, predicted),
        "config": config,
        "_predicted": predicted,
        "_actual": y_valid,
    }


def _torch_models(input_size: int, sequence_length: int):
    import torch
    from torch import nn

    class MLP(nn.Module):
        def __init__(self):
            super().__init__()
            self.network = nn.Sequential(
                nn.Linear(input_size, 128), nn.GELU(), nn.Dropout(0.15),
                nn.Linear(128, 64), nn.GELU(), nn.Linear(64, 1),
            )

        def forward(self, value):
            return self.network(value[:, -1]).squeeze(-1)

    class TCN(nn.Module):
        def __init__(self):
            super().__init__()
            self.network = nn.Sequential(
                nn.Conv1d(input_size, 64, 3, padding=1, dilation=1), nn.GELU(),
                nn.Conv1d(64, 64, 3, padding=2, dilation=2), nn.GELU(),
                nn.Conv1d(64, 32, 3, padding=4, dilation=4), nn.GELU(),
            )
            self.head = nn.Linear(32, 1)

        def forward(self, value):
            output = self.network(value.transpose(1, 2))
            return self.head(output[:, :, sequence_length - 1]).squeeze(-1)

    class GRU(nn.Module):
        def __init__(self):
            super().__init__()
            self.recurrent = nn.GRU(input_size, 64, num_layers=2, batch_first=True, dropout=0.15)
            self.head = nn.Linear(64, 1)

        def forward(self, value):
            output, _state = self.recurrent(value)
            return self.head(output[:, -1]).squeeze(-1)

    class Transformer(nn.Module):
        def __init__(self):
            super().__init__()
            width = 64
            self.project = nn.Linear(input_size, width)
            self.position = nn.Parameter(torch.zeros(1, sequence_length, width))
            layer = nn.TransformerEncoderLayer(
                width, nhead=4, dim_feedforward=128, dropout=0.15,
                activation="gelu", batch_first=True, norm_first=True,
            )
            self.encoder = nn.TransformerEncoder(layer, num_layers=2)
            self.head = nn.Linear(width, 1)

        def forward(self, value):
            output = self.encoder(self.project(value) + self.position[:, : value.shape[1]])
            return self.head(output[:, -1]).squeeze(-1)

    class DAE(nn.Module):
        def __init__(self):
            super().__init__()
            self.encoder = nn.Sequential(nn.Linear(input_size, 64), nn.GELU(), nn.Linear(64, 24))
            self.decoder = nn.Sequential(nn.Linear(24, 64), nn.GELU(), nn.Linear(64, input_size))
            self.head = nn.Linear(24, 1)

        def forward(self, value):
            latent = self.encoder(value[:, -1])
            return self.head(latent).squeeze(-1)

        def reconstruct(self, value):
            return self.decoder(self.encoder(value[:, -1]))

    return {"mlp": MLP, "tcn": TCN, "gru": GRU, "transformer": Transformer, "dae": DAE}


def _train_torch(
    kind: str,
    x_train: np.ndarray,
    y_train: np.ndarray,
    x_valid: np.ndarray,
    y_valid: np.ndarray,
    artifact_dir: Path,
    config: dict[str, Any],
    progress: Progress | None,
    cancelled: Cancelled | None,
) -> dict[str, Any]:
    try:
        import torch
        from torch import nn
        from torch.utils.data import DataLoader, TensorDataset
    except ImportError as exc:
        raise RuntimeError("深度模型需要安装 PyTorch：pip install 'quantmaster[ml]'") from exc

    device_name = str(config.get("device", "auto"))
    device = torch.device(
        "cuda" if device_name == "auto" and torch.cuda.is_available() else
        device_name if device_name != "auto" else "cpu"
    )
    torch.manual_seed(int(config.get("seed", 42)))
    model_class = _torch_models(x_train.shape[-1], x_train.shape[1])[kind]
    model = model_class().to(device)
    optimizer = torch.optim.AdamW(
        model.parameters(), lr=float(config.get("learning_rate", 1e-3)), weight_decay=1e-4
    )
    loss_function = nn.HuberLoss(delta=0.02)
    loader = DataLoader(
        TensorDataset(torch.from_numpy(x_train), torch.from_numpy(y_train)),
        batch_size=int(config.get("batch_size", 256)), shuffle=False,
    )
    valid_x = torch.from_numpy(x_valid).to(device)
    valid_y = torch.from_numpy(y_valid).to(device)
    epochs = max(1, int(config.get("epochs", 30)))
    patience = max(2, int(config.get("patience", 5)))
    best_loss, best_state, stale = float("inf"), None, 0
    history = []
    for epoch in range(epochs):
        if cancelled and cancelled():
            raise InterruptedError("训练已取消")
        model.train()
        losses = []
        for batch_x, batch_y in loader:
            batch_x, batch_y = batch_x.to(device), batch_y.to(device)
            optimizer.zero_grad(set_to_none=True)
            predicted = model(batch_x)
            loss = loss_function(predicted, batch_y)
            if kind == "dae":
                noisy = batch_x.clone()
                noisy[:, -1] += torch.randn_like(noisy[:, -1]) * 0.05
                loss = loss + 0.2 * nn.functional.mse_loss(
                    model.reconstruct(noisy), batch_x[:, -1]
                )
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), 2.0)
            optimizer.step()
            losses.append(float(loss.detach().cpu()))
        model.eval()
        with torch.no_grad():
            validation_loss = float(loss_function(model(valid_x), valid_y).cpu())
        history.append({
            "epoch": epoch + 1,
            "train_loss": round(float(np.mean(losses)), 8),
            "validation_loss": round(validation_loss, 8),
        })
        if progress:
            progress(15 + int(75 * (epoch + 1) / epochs), f"训练 {kind} · {epoch + 1}/{epochs}")
        if validation_loss < best_loss - 1e-7:
            best_loss = validation_loss
            best_state = {key: value.detach().cpu() for key, value in model.state_dict().items()}
            stale = 0
        else:
            stale += 1
            if stale >= patience:
                break
    if best_state is not None:
        model.load_state_dict(best_state)
    model.eval()
    with torch.no_grad():
        predicted = model(valid_x).detach().cpu().numpy()
    model_file = artifact_dir / f"{kind}.pt"
    torch.save({
        "kind": kind, "state_dict": model.state_dict(), "config": config,
        "input_size": x_train.shape[-1], "sequence_length": x_train.shape[1],
    }, model_file)
    (artifact_dir / "history.json").write_text(
        strict_json_dumps(history, indent=2), encoding="utf-8"
    )
    return {
        "kind": kind,
        "artifact": str(model_file.resolve()),
        "train_samples": len(x_train),
        "validation_samples": len(x_valid),
        "device": str(device),
        "epochs_completed": len(history),
        "metrics": _metrics(y_valid, predicted),
        "history": history,
        "config": config,
        "_predicted": predicted,
        "_actual": y_valid,
    }


def artifact_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def predict_panel(
    panel: dict[str, pd.DataFrame], model: dict[str, Any], horizon: int | None = None,
) -> pd.DataFrame:
    """Load the sole current Lab model schema and return date×symbol predictions."""
    manifest_name = str(model.get("manifest") or "")
    if not manifest_name:
        raise ValueError("学习模型没有 schema v2 推理清单")
    root = Path(get_config().data_root).resolve()
    manifest_path = confined_path(root, manifest_name, label="模型清单")
    if not manifest_path.is_file():
        raise FileNotFoundError(f"模型清单不存在：{manifest_name}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != 2:
        raise ValueError("学习模型需要一次性迁移；运行时仅接受 schema v2")
    available = [int(value) for value in manifest.get("horizons") or []]
    if not available:
        raise ValueError("schema v2 模型未声明预测周期")
    selected = horizon or (3 if 3 in available else available[0])
    if selected not in available:
        raise ValueError(f"schema v2 模型不支持 {selected} 日预测")
    from quantmaster.lab.multihorizon import predict_multi_bundle

    return predict_multi_bundle(panel, model, horizon=selected).expected_excess[selected]
