"""共享编码器的 1/3/5/7 日多任务训练、校准和推理。"""

from __future__ import annotations

import json
import math
import os
import shutil
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from quantmaster.config import get_config
from quantmaster.lab.cache import feature_cache_lock
from quantmaster.lab.ml import _engineered_features, _resolve_torch_device, artifact_sha256
from quantmaster.lab.research import HORIZONS, FeatureSetSpec, TimeFold
from quantmaster.runtime.json import strict_json_dumps
from quantmaster.runtime.paths import confined_path

Progress = Callable[[int, str], None]
Cancelled = Callable[[], bool]


@dataclass
class SampleMetadata:
    date_positions: np.ndarray
    symbol_positions: np.ndarray
    feature_coverage: np.ndarray
    dates: pd.DatetimeIndex
    symbols: pd.Index
    horizons: tuple[int, ...]

    def __len__(self) -> int:
        return len(self.date_positions)

    def __getitem__(self, position):
        if isinstance(position, slice):
            return [self[index] for index in range(*position.indices(len(self)))]
        index = int(position)
        date_position = int(self.date_positions[index])
        return {
            "date": self.dates[date_position].strftime("%Y-%m-%d"),
            "symbol": str(self.symbols[int(self.symbol_positions[index])]),
            "target_dates": {
                str(horizon): self.dates[date_position + horizon].strftime("%Y-%m-%d")
                for horizon in self.horizons
            },
            "feature_coverage": float(self.feature_coverage[index]),
        }

    def date_strings(self, positions: np.ndarray | None = None) -> np.ndarray:
        values = self.date_positions if positions is None else self.date_positions[positions]
        labels = self.dates.strftime("%Y-%m-%d").to_numpy()
        return labels[np.asarray(values, dtype=np.int64)]

    def latest_target_strings(self, positions: np.ndarray | None = None) -> np.ndarray:
        values = self.date_positions if positions is None else self.date_positions[positions]
        labels = self.dates.strftime("%Y-%m-%d").to_numpy()
        return labels[np.asarray(values, dtype=np.int64) + max(self.horizons)]


@dataclass
class MultiHorizonSamples:
    values: WindowCubeView
    excess_targets: np.ndarray
    raw_targets: np.ndarray
    metadata: SampleMetadata
    feature_names: list[str]
    horizons: tuple[int, ...]


class WindowCubeView:
    """Array-like dynamic window view backed by one shared feature cube."""

    def __init__(
        self, cube: np.ndarray, date_positions: np.ndarray,
        symbol_positions: np.ndarray, sequence_length: int,
    ):
        self.cube = cube
        self.date_positions = date_positions
        self.symbol_positions = symbol_positions
        self.sequence_length = sequence_length
        self.shape = (len(date_positions), sequence_length, cube.shape[-1])

    def __len__(self) -> int:
        return self.shape[0]

    def _window(self, position: int) -> np.ndarray:
        date_position = int(self.date_positions[position])
        symbol_position = int(self.symbol_positions[position])
        return np.asarray(
            self.cube[
                date_position - self.sequence_length + 1:date_position + 1,
                symbol_position,
                :,
            ], dtype=np.float32,
        )

    def __getitem__(self, selection):
        if isinstance(selection, tuple):
            primary, *remaining = selection
            values = self[primary]
            if not remaining:
                return values
            if np.isscalar(primary):
                return values[tuple(remaining)]
            return values[(slice(None), *remaining)]
        if np.isscalar(selection):
            return self._window(int(cast(Any, selection)))
        positions = np.arange(len(self))[selection]
        result = np.empty(
            (len(positions), self.sequence_length, self.cube.shape[-1]), dtype=np.float32,
        )
        for row, position in enumerate(positions):
            result[row] = self._window(int(position))
        return result


@dataclass
class PredictionBundle:
    expected_excess: dict[int, pd.DataFrame]
    expected_return: dict[int, pd.DataFrame]
    probability_up: dict[int, pd.DataFrame]
    probability_net_positive: dict[int, pd.DataFrame]
    quantiles: dict[int, dict[str, pd.DataFrame]]
    uncertainty: dict[int, pd.DataFrame]
    ood_score: pd.DataFrame
    degraded: pd.DataFrame


def _broadcast(series: pd.Series, columns: pd.Index) -> pd.DataFrame:
    values = np.repeat(series.to_numpy(float)[:, None], len(columns), axis=1)
    return pd.DataFrame(values, index=series.index, columns=columns)


def _engineered_research_features(
    panel: dict[str, pd.DataFrame], *, fundamentals: dict[str, pd.DataFrame] | None = None,
    spec: FeatureSetSpec | None = None,
):
    spec = spec or FeatureSetSpec(groups=("price_volume_v2",))
    close = panel["close"].astype(float)
    if "price_volume_v2" in spec.groups:
        yield from _engineered_features(panel)
    if "market_context_v1" in spec.groups:
        returns = close.pct_change(fill_method=None)
        market_return = returns.median(axis=1)
        for window in (1, 5, 20):
            series = market_return if window == 1 else market_return.rolling(window).sum()
            yield f"market_return_{window}", _broadcast(series, close.columns)
        yield "market_breadth_up", _broadcast((returns > 0).mean(axis=1), close.columns)
        moving = close.rolling(20).mean()
        yield "market_breadth_above_20", _broadcast(
            (close > moving).mean(axis=1), close.columns,
        )
        yield "market_dispersion_20", _broadcast(
            returns.rolling(20).std().median(axis=1), close.columns,
        )
        yield "market_volatility_20", _broadcast(
            market_return.rolling(20).std(), close.columns,
        )
        market_index = (1 + market_return.fillna(0)).cumprod()
        yield "market_drawdown_60", _broadcast(
            market_index / market_index.rolling(60).max() - 1, close.columns,
        )
    if "pit_fundamental_v1" in spec.groups:
        source = fundamentals or {}
        transforms = {
            "earnings_yield": ("pe_ttm", lambda value: 1 / value.replace(0, np.nan)),
            "book_to_price": ("pb", lambda value: 1 / value.replace(0, np.nan)),
            "dividend_yield": ("dv_ratio", lambda value: value / 100),
            "log_market_cap": ("total_mv", lambda value: np.log(value.where(value > 0))),
            "roe": ("roe", lambda value: value / 100),
        }
        for output, (source_name, transform) in transforms.items():
            raw = source.get(source_name, close * np.nan).reindex(
                index=close.index, columns=close.columns,
            )
            yield f"fundamental_{output}", transform(raw.astype(float))
            yield f"fundamental_{output}_observed", raw.notna().astype(float)
    if spec.include_news and "news_v1" in spec.groups:
        news = (fundamentals or {}).get("news_sentiment", close * np.nan)
        yield "news_sentiment", news.reindex_like(close)
        yield "news_sentiment_observed", news.reindex_like(close).notna().astype(float)


def engineer_research_features(
    panel: dict[str, pd.DataFrame], *, fundamentals: dict[str, pd.DataFrame] | None = None,
    spec: FeatureSetSpec | None = None,
) -> dict[str, pd.DataFrame]:
    """版本化特征注册表；所有变换仅使用当日及过去数据。"""
    features = dict(_engineered_research_features(panel, fundamentals=fundamentals, spec=spec))
    if not features:
        raise ValueError("特征注册表为空")
    return features


def _feature_cube(
    panel: dict[str, pd.DataFrame], fundamentals: dict[str, pd.DataFrame] | None,
    feature_spec: FeatureSetSpec, storage_dir: Path | None = None,
) -> tuple[np.ndarray, np.ndarray, pd.DatetimeIndex, pd.Index, list[str]]:
    if storage_dir is None:
        return _feature_cube_unlocked(panel, fundamentals, feature_spec)
    with feature_cache_lock(storage_dir):
        return _feature_cube_unlocked(
            panel, fundamentals, feature_spec, storage_dir=storage_dir,
        )


def _feature_cube_unlocked(
    panel: dict[str, pd.DataFrame], fundamentals: dict[str, pd.DataFrame] | None,
    feature_spec: FeatureSetSpec, storage_dir: Path | None = None,
) -> tuple[np.ndarray, np.ndarray, pd.DatetimeIndex, pd.Index, list[str]]:
    close = panel["close"].astype(float)
    dates, columns = pd.DatetimeIndex(close.index), close.columns
    feature_count = (
        (48 if "price_volume_v2" in feature_spec.groups else 0)
        + (8 if "market_context_v1" in feature_spec.groups else 0)
        + (10 if "pit_fundamental_v1" in feature_spec.groups else 0)
        + (2 if feature_spec.include_news and "news_v1" in feature_spec.groups else 0)
    )
    if not feature_count:
        raise ValueError("特征注册表为空")
    cube_file = storage_dir / "feature-cube.npy" if storage_dir is not None else None
    counts_file = storage_dir / "valid-counts.npy" if storage_dir is not None else None
    metadata_file = storage_dir / "cube.json" if storage_dir is not None else None
    if cube_file and counts_file and metadata_file:
        try:
            metadata = json.loads(metadata_file.read_text(encoding="utf-8"))
            if (
                metadata.get("feature_spec") == feature_spec.to_dict()
                and metadata.get("dates") == dates.strftime("%Y-%m-%d").tolist()
                and metadata.get("symbols") == columns.astype(str).tolist()
            ):
                return (
                    np.load(cube_file, mmap_mode="r"),
                    np.load(counts_file, mmap_mode="r"),
                    dates, columns, list(metadata["features"]),
                )
        except (OSError, ValueError, KeyError, json.JSONDecodeError):
            pass
    shape = (len(dates), len(columns), feature_count)
    if storage_dir is None:
        cube: Any = np.empty(shape, dtype=np.float32)
        valid_counts: Any = np.zeros(shape[:2], dtype=np.uint8)
    else:
        storage_dir.mkdir(parents=True, exist_ok=True)
        cube = np.lib.format.open_memmap(
            storage_dir / "feature-cube.partial.npy", mode="w+",
            dtype=np.float32, shape=shape,
        )
        valid_counts = np.lib.format.open_memmap(
            storage_dir / "valid-counts.partial.npy", mode="w+",
            dtype=np.uint8, shape=shape[:2],
        )
        valid_counts[:] = 0
    names: list[str] = []
    for position, (name, raw) in enumerate(_engineered_research_features(
        panel, fundamentals=fundamentals, spec=feature_spec,
    )):
        values = raw.astype(float).replace([np.inf, -np.inf], np.nan)
        valid = values.notna()
        lower = values.quantile(0.01, axis=1)
        upper = values.quantile(0.99, axis=1)
        clipped = values.clip(lower=lower, upper=upper, axis=0)
        mean = clipped.mean(axis=1)
        std = clipped.std(axis=1, ddof=0).replace(0, np.nan)
        normalized = clipped.sub(mean, axis=0).div(std, axis=0).fillna(0.0)
        cube[:, :, position] = normalized.reindex(
            index=dates, columns=columns,
        ).to_numpy(np.float32)
        valid_counts[:] += valid.reindex(
            index=dates, columns=columns,
        ).fillna(False).to_numpy(np.uint8)
        names.append(name)
    if len(names) != feature_count:
        raise AssertionError(f"研究特征数量不一致：预计 {feature_count}，实际 {len(names)}")
    if storage_dir is not None and cube_file and counts_file and metadata_file:
        cube.flush()
        valid_counts.flush()
        del cube, valid_counts
        os.replace(storage_dir / "feature-cube.partial.npy", cube_file)
        os.replace(storage_dir / "valid-counts.partial.npy", counts_file)
        metadata_file.write_text(strict_json_dumps({
            "feature_spec": feature_spec.to_dict(),
            "dates": dates.strftime("%Y-%m-%d").tolist(),
            "symbols": columns.astype(str).tolist(), "features": names,
        }, indent=2), encoding="utf-8")
        cube = np.load(cube_file, mmap_mode="r")
        valid_counts = np.load(counts_file, mmap_mode="r")
    return cube, valid_counts, dates, columns, names


def make_multi_horizon_samples(
    panel: dict[str, pd.DataFrame], *, horizons: tuple[int, ...] = HORIZONS,
    sequence_length: int = 20, membership: pd.DataFrame | None = None,
    fundamentals: dict[str, pd.DataFrame] | None = None,
    feature_spec: FeatureSetSpec | None = None,
    storage_dir: str | Path | None = None,
) -> MultiHorizonSamples:
    feature_spec = feature_spec or FeatureSetSpec(groups=("price_volume_v2",))
    if not horizons or any(value not in HORIZONS for value in horizons):
        raise ValueError("horizons 只支持 1/3/5/7 日")
    root = Path(storage_dir) if storage_dir is not None else None
    close = panel["close"].astype(float)
    cube, valid_counts, dates, columns, names = _feature_cube(
        panel, fundamentals, feature_spec, storage_dir=root,
    )
    raw_frames = [close.shift(-horizon) / close - 1 for horizon in horizons]
    excess_frames = [frame.sub(frame.median(axis=1), axis=0) for frame in raw_frames]
    raw_arrays = [frame.reindex(index=dates, columns=columns).to_numpy(float) for frame in raw_frames]
    excess_arrays = [
        frame.reindex(index=dates, columns=columns).to_numpy(float) for frame in excess_frames
    ]
    member_values = None
    if membership is not None:
        member_values = membership.reindex(index=dates, columns=columns).fillna(False).to_numpy(bool)
    maximum_horizon = max(horizons)

    def eligible_samples():
        for date_pos in range(sequence_length - 1, len(dates) - maximum_horizon):
            for symbol_pos in range(len(columns)):
                if member_values is not None and not member_values[date_pos, symbol_pos]:
                    continue
                coverage = float(
                    valid_counts[
                        date_pos - sequence_length + 1:date_pos + 1, symbol_pos
                    ].sum() / (sequence_length * len(names))
                )
                raw = np.asarray(
                    [array[date_pos, symbol_pos] for array in raw_arrays], dtype=np.float32,
                )
                excess = np.asarray(
                    [array[date_pos, symbol_pos] for array in excess_arrays], dtype=np.float32,
                )
                if (
                    coverage >= feature_spec.minimum_coverage
                    and np.isfinite(raw).all()
                    and np.isfinite(excess).all()
                ):
                    yield date_pos, symbol_pos, coverage, raw, excess

    sample_count = sum(1 for _ in eligible_samples())
    if not sample_count:
        raise ValueError("清洗后没有共享多周期训练样本")

    estimated_bytes = (
        sample_count * len(horizons) * np.dtype(np.float32).itemsize * 2
        + sample_count * 12
    )
    if root is not None:
        root.mkdir(parents=True, exist_ok=True)
        free = shutil.disk_usage(root).free
        required = int(estimated_bytes * 1.2) + 256 * 1024 * 1024
        if free < required:
            raise RuntimeError(
                f"Lab 样本盘空间不足：需要约 {required / 1024 ** 3:.1f} GiB，"
                f"可用 {free / 1024 ** 3:.1f} GiB"
            )

    def allocate(name: str, dtype, array_shape: tuple[int, ...]):
        if root is None:
            return np.empty(array_shape, dtype=dtype)
        return np.lib.format.open_memmap(
            root / f"{name}.npy", mode="w+", dtype=dtype, shape=array_shape,
        )

    raw_targets = allocate("raw-targets", np.float32, (sample_count, len(horizons)))
    excess_targets = allocate("excess-targets", np.float32, (sample_count, len(horizons)))
    date_positions = allocate("date-positions", np.int32, (sample_count,))
    symbol_positions = allocate("symbol-positions", np.int32, (sample_count,))
    coverages = allocate("feature-coverage", np.float32, (sample_count,))

    for row, (date_pos, symbol_pos, coverage, raw, excess) in enumerate(
        eligible_samples()
    ):
        raw_targets[row] = raw
        excess_targets[row] = excess
        date_positions[row] = date_pos
        symbol_positions[row] = symbol_pos
        coverages[row] = coverage
    for array in (
        raw_targets, excess_targets, date_positions, symbol_positions, coverages,
    ):
        if isinstance(array, np.memmap):
            array.flush()
    return MultiHorizonSamples(
        values=WindowCubeView(cube, date_positions, symbol_positions, sequence_length),
        excess_targets=excess_targets,
        raw_targets=raw_targets,
        metadata=SampleMetadata(
            date_positions, symbol_positions, coverages, dates, columns, tuple(horizons),
        ),
        feature_names=names, horizons=tuple(horizons),
    )


def make_multi_inference_samples(
    panel: dict[str, pd.DataFrame], *, sequence_length: int,
    fundamentals: dict[str, pd.DataFrame] | None, feature_spec: FeatureSetSpec,
) -> tuple[np.ndarray, list[dict[str, Any]], list[str]]:
    cube, valid_counts, dates, columns, names = _feature_cube(panel, fundamentals, feature_spec)
    values, metadata = [], []
    for date_pos in range(sequence_length - 1, len(dates)):
        for symbol_pos, symbol in enumerate(columns):
            sample = cube[date_pos - sequence_length + 1:date_pos + 1, symbol_pos]
            coverage = float(
                valid_counts[
                    date_pos - sequence_length + 1:date_pos + 1, symbol_pos,
                ].sum() / (sequence_length * len(names))
            )
            if coverage >= feature_spec.minimum_coverage and np.isfinite(sample).all():
                values.append(sample)
                metadata.append({
                    "date": dates[date_pos].strftime("%Y-%m-%d"),
                    "symbol": str(symbol), "feature_coverage": coverage,
                })
    if not values:
        raise ValueError("当前面板没有满足覆盖率的共享模型推理样本")
    return np.stack(values).astype(np.float32), metadata, names


def fold_positions(samples: MultiHorizonSamples, fold: TimeFold) -> tuple[np.ndarray, np.ndarray]:
    dates = samples.metadata.date_strings()
    latest_targets = samples.metadata.latest_target_strings()
    train = np.flatnonzero(
        (dates >= fold.train_start) & (dates <= fold.train_end) & (latest_targets < fold.test_start)
    )
    valid = np.flatnonzero((dates >= fold.test_start) & (dates <= fold.test_end))
    if min(len(train), len(valid)) < 20:
        raise ValueError(f"{fold.name} 的训练或验证样本不足")
    return train, valid


def _constant_logit(values: np.ndarray) -> float:
    probability = float(np.clip(np.mean(values), 1e-5, 1 - 1e-5))
    return math.log(probability / (1 - probability))


def _fit_logistic(features: np.ndarray, labels: np.ndarray) -> tuple[np.ndarray, float]:
    if len(np.unique(labels)) < 2:
        return np.zeros(features.shape[1], dtype=float), _constant_logit(labels)
    from sklearn.linear_model import LogisticRegression

    model = LogisticRegression(C=1.0, max_iter=300, random_state=42)
    model.fit(features, labels)
    return model.coef_[0].astype(float), float(model.intercept_[0])


def _sigmoid(value: np.ndarray) -> np.ndarray:
    return 1 / (1 + np.exp(-np.clip(value, -30, 30)))


def fit_probability_calibrators(
    frame: pd.DataFrame, *, roundtrip_cost: float,
) -> dict[str, dict[str, dict[str, Any]]]:
    """只用开发期 OOF 预测拟合校准器，不接触密封留出标签。"""
    from sklearn.isotonic import IsotonicRegression
    from sklearn.linear_model import LogisticRegression

    result: dict[str, dict[str, dict[str, Any]]] = {}
    targets = {
        "probability_up": lambda group: group["actual_return"].to_numpy(float) > 0,
        "probability_net_positive": lambda group: (
            group["actual_return"].to_numpy(float) > roundtrip_cost
        ),
    }
    for horizon, group in frame.groupby("horizon", sort=True):
        horizon_result: dict[str, dict[str, Any]] = {}
        for column, target in targets.items():
            usable = group[[column, "actual_return"]].dropna()
            scores = np.clip(usable[column].to_numpy(float), 1e-6, 1 - 1e-6)
            labels = target(usable).astype(int)
            if not len(scores) or len(np.unique(labels)) < 2:
                horizon_result[column] = {
                    "kind": "constant",
                    "probability": float(labels.mean()) if len(labels) else 0.5,
                }
            elif len(scores) >= 1000 and min(labels.sum(), len(labels) - labels.sum()) >= 100:
                model = IsotonicRegression(out_of_bounds="clip").fit(scores, labels)
                horizon_result[column] = {
                    "kind": "isotonic",
                    "x": model.X_thresholds_.astype(float).tolist(),
                    "y": model.y_thresholds_.astype(float).tolist(),
                }
            else:
                logits = np.log(scores / (1 - scores)).reshape(-1, 1)
                model = LogisticRegression(C=1.0, max_iter=300, random_state=42).fit(
                    logits, labels,
                )
                horizon_result[column] = {
                    "kind": "platt",
                    "coefficient": float(model.coef_[0, 0]),
                    "intercept": float(model.intercept_[0]),
                }
        result[str(int(horizon))] = horizon_result
    return result


def _apply_calibrator(values: np.ndarray, model: dict[str, Any] | None) -> np.ndarray:
    scores = np.clip(np.asarray(values, dtype=float), 1e-6, 1 - 1e-6)
    if not model:
        return scores
    kind = str(model.get("kind") or "")
    if kind == "constant":
        return np.full_like(scores, float(model.get("probability", 0.5)))
    if kind == "isotonic":
        return np.interp(scores, model.get("x") or [0, 1], model.get("y") or [0, 1])
    if kind == "platt":
        logits = np.log(scores / (1 - scores))
        return _sigmoid(
            float(model.get("coefficient", 1.0)) * logits
            + float(model.get("intercept", 0.0))
        )
    raise ValueError(f"未知概率校准器: {kind}")


def apply_probability_calibrators(
    frame: pd.DataFrame, calibrators: dict[str, dict[str, dict[str, Any]]],
) -> pd.DataFrame:
    calibrated = frame.copy()
    for horizon, positions in calibrated.groupby("horizon", sort=False).groups.items():
        models = calibrators.get(str(int(horizon)), {})
        for column in ("probability_up", "probability_net_positive"):
            calibrated.loc[positions, column] = _apply_calibrator(
                calibrated.loc[positions, column].to_numpy(float), models.get(column),
            )
    return calibrated


def calibrate_prediction_arrays(
    predictions: dict[str, np.ndarray], horizons: tuple[int, ...],
    calibrators: dict[str, dict[str, dict[str, Any]]] | None,
) -> dict[str, np.ndarray]:
    if not calibrators:
        return predictions
    result = dict(predictions)
    for key in ("probability_up", "probability_net_positive"):
        values = np.asarray(predictions[key], dtype=float).copy()
        for column, horizon in enumerate(horizons):
            values[:, column] = _apply_calibrator(
                values[:, column], calibrators.get(str(horizon), {}).get(key),
            )
        result[key] = values
    return result


def _fit_ridge(
    samples: MultiHorizonSamples, train: np.ndarray, valid: np.ndarray,
    artifact: Path, config: dict[str, Any], roundtrip_cost: float,
) -> dict[str, Any]:
    alpha = float(config.get("alpha", 1.0))

    def fit_targets(targets: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        feature_count = samples.values.shape[-1]
        target_count = targets.shape[1]
        sum_x = np.zeros(feature_count, dtype=np.float64)
        sum_y = np.zeros(target_count, dtype=np.float64)
        xtx = np.zeros((feature_count, feature_count), dtype=np.float64)
        xty = np.zeros((feature_count, target_count), dtype=np.float64)
        for start in range(0, len(train), 50_000):
            positions = train[start:start + 50_000]
            x = np.asarray(samples.values[positions, -1], dtype=np.float64)
            y = np.asarray(targets[positions], dtype=np.float64)
            sum_x += x.sum(axis=0)
            sum_y += y.sum(axis=0)
            xtx += x.T @ x
            xty += x.T @ y
        count = float(len(train))
        mean_x, mean_y = sum_x / count, sum_y / count
        centered_xtx = xtx - count * np.outer(mean_x, mean_x)
        centered_xty = xty - count * np.outer(mean_x, mean_y)
        coefficient = np.linalg.solve(
            centered_xtx + np.eye(feature_count) * alpha,
            centered_xty,
        )
        intercept = mean_y - mean_x @ coefficient
        return coefficient.T, intercept

    excess_coef, excess_intercept = fit_targets(samples.excess_targets)
    raw_coef, raw_intercept = fit_targets(samples.raw_targets)

    def fit_logistic(column: int, threshold: float) -> tuple[np.ndarray, float]:
        from sklearn.linear_model import SGDClassifier

        positive = sum(
            int((samples.raw_targets[positions, column] > threshold).sum())
            for positions in (
                train[start:start + 50_000] for start in range(0, len(train), 50_000)
            )
        )
        if positive in {0, len(train)}:
            probability = float(np.clip(positive / max(1, len(train)), 1e-5, 1 - 1e-5))
            return (
                np.zeros(samples.values.shape[-1], dtype=float),
                math.log(probability / (1 - probability)),
            )
        model = SGDClassifier(
            loss="log_loss", penalty="l2", alpha=1e-4,
            random_state=42, average=True,
        )
        classes = np.asarray([0, 1])
        first = True
        for _epoch in range(3):
            for start in range(0, len(train), 50_000):
                positions = train[start:start + 50_000]
                x = np.asarray(samples.values[positions, -1], dtype=np.float64)
                y = (samples.raw_targets[positions, column] > threshold).astype(int)
                model.partial_fit(x, y, classes=classes if first else None)
                first = False
        return model.coef_[0].astype(float), float(model.intercept_[0])

    up_coef, up_intercept, net_coef, net_intercept = [], [], [], []
    for column in range(len(samples.horizons)):
        coefficient, intercept = fit_logistic(column, 0.0)
        up_coef.append(coefficient)
        up_intercept.append(intercept)
        coefficient, intercept = fit_logistic(column, roundtrip_cost)
        net_coef.append(coefficient)
        net_intercept.append(intercept)

    stride = max(1, math.ceil(len(train) / 200_000))
    diagnostic_positions = train[::stride]
    x_diagnostic = np.asarray(samples.values[diagnostic_positions, -1], dtype=np.float64)
    raw_train_prediction = x_diagnostic @ raw_coef.T + raw_intercept
    residual = samples.raw_targets[diagnostic_positions] - raw_train_prediction
    residual_quantiles = np.quantile(residual, (0.1, 0.5, 0.9), axis=0).T
    mean = x_diagnostic.mean(axis=0)
    scale = x_diagnostic.std(axis=0)
    scale[scale < 1e-6] = 1.0
    train_ood = np.sqrt(np.mean(((x_diagnostic - mean) / scale) ** 2, axis=1))
    threshold = float(np.quantile(train_ood, 0.995))
    np.savez_compressed(
        artifact,
        excess_coef=excess_coef, excess_intercept=np.atleast_1d(excess_intercept),
        raw_coef=raw_coef, raw_intercept=np.atleast_1d(raw_intercept),
        up_coef=np.asarray(up_coef), up_intercept=np.asarray(up_intercept),
        net_coef=np.asarray(net_coef), net_intercept=np.asarray(net_intercept),
        residual_quantiles=residual_quantiles, ood_mean=mean, ood_scale=scale,
        ood_threshold=np.asarray([threshold]),
    )
    collected: dict[str, list[np.ndarray]] = {}
    for start in range(0, len(valid), 50_000):
        positions = valid[start:start + 50_000]
        predicted = _predict_ridge_artifact(
            artifact, np.asarray(samples.values[positions, -1], dtype=np.float64),
        )
        for key, value in predicted.items():
            collected.setdefault(key, []).append(value)
    return {key: np.concatenate(parts) for key, parts in collected.items()}


def _predict_ridge_artifact(artifact: Path, features: np.ndarray) -> dict[str, np.ndarray]:
    with np.load(artifact) as saved:
        excess = features @ saved["excess_coef"].T + saved["excess_intercept"]
        raw = features @ saved["raw_coef"].T + saved["raw_intercept"]
        up = _sigmoid(features @ saved["up_coef"].T + saved["up_intercept"])
        net = _sigmoid(features @ saved["net_coef"].T + saved["net_intercept"])
        quantiles = raw[:, :, None] + saved["residual_quantiles"][None, :, :]
        quantiles.sort(axis=2)
        ood = np.sqrt(np.mean(((features - saved["ood_mean"]) / saved["ood_scale"]) ** 2, axis=1))
        threshold = float(saved["ood_threshold"][0])
    return {
        "expected_excess": excess, "expected_return": raw,
        "probability_up": up, "probability_net_positive": net,
        "quantiles": quantiles, "ood_score": ood,
        "degraded": ood > threshold,
    }


def _build_torch_model(kind: str, input_size: int, sequence_length: int, outputs: int):
    import torch
    from torch import nn

    class MultiTaskNetwork(nn.Module):
        def __init__(self):
            super().__init__()
            width = 96
            self.kind = kind
            if kind == "multi-transformer":
                self.project = nn.Linear(input_size, width)
                self.position = nn.Parameter(torch.zeros(1, sequence_length, width))
                layer = nn.TransformerEncoderLayer(
                    width, 4, 192, 0.15, "gelu", batch_first=True, norm_first=True,
                )
                self.encoder = nn.TransformerEncoder(layer, 2)
            elif kind == "multi-tcn":
                self.encoder = nn.Sequential(
                    nn.Conv1d(input_size, width, 3, padding=1), nn.GELU(),
                    nn.Conv1d(width, width, 3, padding=2, dilation=2), nn.GELU(),
                    nn.Conv1d(width, width, 3, padding=4, dilation=4), nn.GELU(),
                )
            else:
                self.encoder = nn.GRU(
                    input_size, width, num_layers=2, batch_first=True, dropout=0.15,
                )
            self.head = nn.Sequential(nn.LayerNorm(width), nn.Linear(width, width), nn.GELU(),
                                      nn.Dropout(0.1), nn.Linear(width, outputs * 7))

        def encode(self, value):
            if self.kind == "multi-transformer":
                return self.encoder(
                    self.project(value) + self.position[:, : value.shape[1]],
                )[:, -1]
            elif self.kind == "multi-tcn":
                return self.encoder(value.transpose(1, 2))[:, :, -1]
            encoded, _state = self.encoder(value)
            return encoded[:, -1]

        def forward(self, value):
            encoded = self.encode(value)
            raw = self.head(encoded).reshape(-1, outputs, 7)
            return {
                "excess": raw[:, :, 0], "return": raw[:, :, 1],
                "up": raw[:, :, 2], "net": raw[:, :, 3], "quantiles": raw[:, :, 4:7],
            }

    return MultiTaskNetwork()


def _internal_early_stop_positions(
    samples: MultiHorizonSamples, train: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """从训练窗尾部切出内部验证集，外层 OOF/密封标签永不参与早停。"""
    dates = samples.metadata.date_strings(train)
    unique_dates = np.unique(dates)
    validation_days = min(63, max(20, len(unique_dates) // 8))
    if len(unique_dates) <= validation_days + max(samples.horizons) + 20:
        raise ValueError("训练窗不足以创建独立的深度模型早停区间")
    validation_start = unique_dates[-validation_days]
    latest_targets = samples.metadata.latest_target_strings(train)
    fit = train[(dates < validation_start) & (latest_targets < validation_start)]
    early_stop = train[dates >= validation_start]
    if min(len(fit), len(early_stop)) < 20:
        raise ValueError("深度模型内部训练或早停样本不足")
    return fit, early_stop


def _fit_torch(
    kind: str, samples: MultiHorizonSamples, train: np.ndarray, valid: np.ndarray,
    artifact: Path, config: dict[str, Any], roundtrip_cost: float,
    progress: Progress | None, cancelled: Cancelled | None,
) -> dict[str, Any]:
    try:
        import torch
        from torch import nn
        from torch.utils.data import DataLoader, Dataset
    except ImportError as exc:
        raise RuntimeError("共享深度模型需要安装 PyTorch：pip install 'quantmaster[ml]'") from exc
    from quantmaster.lab.errors import LabError

    started = time.perf_counter()
    device_setting = str(config.get("device", get_config().lab.device or "auto"))
    device = _resolve_torch_device(torch, device_setting)
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
    torch.manual_seed(int(config.get("seed", 42)))
    fit_positions, early_stop_positions = _internal_early_stop_positions(samples, train)
    model = _build_torch_model(
        kind, samples.values.shape[-1], samples.values.shape[1], len(samples.horizons),
    ).to(device)
    optimizer = torch.optim.AdamW(
        model.parameters(), lr=float(config.get("learning_rate", 7e-4)), weight_decay=1e-4,
    )
    batch_size = int(config.get("batch_size", 256))
    accumulation = max(1, int(config.get("gradient_accumulation", 1)))
    class PositionDataset(Dataset):
        def __init__(self, positions: np.ndarray):
            self.positions = positions

        def __len__(self):
            return len(self.positions)

        def __getitem__(self, index):
            position = int(self.positions[index])
            return (
                torch.from_numpy(np.asarray(samples.values[position]).copy()),
                torch.from_numpy(np.asarray(samples.excess_targets[position])),
                torch.from_numpy(np.asarray(samples.raw_targets[position])),
            )

    dataset = PositionDataset(fit_positions)
    generator = torch.Generator().manual_seed(int(config.get("seed", 42)))
    loader = DataLoader(
        dataset, batch_size=batch_size, shuffle=True, generator=generator,
        pin_memory=device.type == "cuda",
    )
    use_amp = device.type == "cuda"
    use_bfloat16 = use_amp and bool(torch.cuda.is_bf16_supported())
    amp_dtype = torch.bfloat16 if use_bfloat16 else torch.float16
    amp_name = "bf16" if use_bfloat16 else "fp16" if use_amp else "off"
    scaler = torch.amp.GradScaler("cuda", enabled=use_amp and not use_bfloat16)
    quantile_levels = torch.tensor([0.1, 0.5, 0.9], device=device)

    def loss_for(output, excess_target, raw_target):
        regression = nn.functional.huber_loss(output["excess"], excess_target, delta=0.02)
        raw_loss = nn.functional.huber_loss(output["return"], raw_target, delta=0.02)
        up_loss = nn.functional.binary_cross_entropy_with_logits(output["up"], (raw_target > 0).float())
        net_loss = nn.functional.binary_cross_entropy_with_logits(
            output["net"], (raw_target > roundtrip_cost).float(),
        )
        errors = raw_target[:, :, None] - output["quantiles"]
        pinball = torch.maximum(quantile_levels * errors, (quantile_levels - 1) * errors).mean()
        return regression + 0.5 * raw_loss + 0.25 * (up_loss + net_loss) + 0.5 * pinball

    epochs = max(1, int(config.get("epochs", 30)))
    patience = max(2, int(config.get("patience", 5)))
    best_loss, best_state, stale = float("inf"), None, 0
    validation_loader = DataLoader(
        PositionDataset(early_stop_positions), batch_size=batch_size, shuffle=False,
        pin_memory=device.type == "cuda",
    )
    try:
        probe_x, _probe_excess, _probe_raw = next(iter(loader))
        probe_x = probe_x.to(device, non_blocking=True)
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
    for epoch in range(epochs):
        if cancelled and cancelled():
            raise InterruptedError("训练已取消")
        model.train()
        optimizer.zero_grad(set_to_none=True)
        for step, (batch_x, batch_excess, batch_raw) in enumerate(loader):
            batch_x = batch_x.to(device, non_blocking=True)
            batch_excess = batch_excess.to(device, non_blocking=True)
            batch_raw = batch_raw.to(device, non_blocking=True)
            with torch.autocast(device_type=device.type, dtype=amp_dtype, enabled=use_amp):
                loss = loss_for(model(batch_x), batch_excess, batch_raw) / accumulation
            scaler.scale(loss).backward()
            if (step + 1) % accumulation == 0 or step + 1 == len(loader):
                scaler.unscale_(optimizer)
                nn.utils.clip_grad_norm_(model.parameters(), 2.0)
                scaler.step(optimizer)
                scaler.update()
                optimizer.zero_grad(set_to_none=True)
        model.eval()
        validation_total = 0.0
        validation_count = 0
        with torch.no_grad():
            for valid_x, valid_excess, valid_raw in validation_loader:
                valid_x = valid_x.to(device, non_blocking=True)
                valid_excess = valid_excess.to(device, non_blocking=True)
                valid_raw = valid_raw.to(device, non_blocking=True)
                with torch.autocast(
                    device_type=device.type, dtype=amp_dtype, enabled=use_amp,
                ):
                    batch_loss = loss_for(model(valid_x), valid_excess, valid_raw)
                validation_total += float(batch_loss.cpu()) * len(valid_x)
                validation_count += len(valid_x)
        validation_loss = validation_total / max(1, validation_count)
        if progress:
            progress(15 + int(70 * (epoch + 1) / epochs), f"共享多周期训练 {epoch + 1}/{epochs}")
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
    ood_positions = fit_positions
    if len(ood_positions) > 50_000:
        stride = math.ceil(len(ood_positions) / 50_000)
        ood_positions = ood_positions[::stride]
    train_latent = _latent_sample_positions(model, samples, ood_positions, device)
    ood_mean = train_latent.mean(axis=0)
    centered = train_latent - ood_mean
    covariance = np.cov(centered, rowvar=False)
    covariance = np.atleast_2d(covariance) + np.eye(centered.shape[1]) * 1e-4
    ood_precision = np.linalg.pinv(covariance, hermitian=True)
    train_ood = np.sqrt(np.maximum(
        0.0, np.einsum("ij,jk,ik->i", centered, ood_precision, centered),
    ))
    ood_threshold = float(np.quantile(train_ood, 0.995))
    torch.save({
        "schema_version": 2, "kind": kind, "state_dict": model.state_dict(),
        "input_size": samples.values.shape[-1], "sequence_length": samples.values.shape[1],
        "horizons": list(samples.horizons), "config": config,
        "ood_mean": ood_mean, "ood_precision": ood_precision,
        "ood_threshold": ood_threshold,
    }, artifact)
    predictions: dict[str, Any] = _predict_torch_sample_positions(
        model, samples, valid, device,
        ood_mean=ood_mean, ood_precision=ood_precision, ood_threshold=ood_threshold,
    )
    if device.type == "cuda":
        torch.cuda.synchronize(device)
    elapsed = max(time.perf_counter() - started, 1e-9)
    predictions["_telemetry"] = {
        "resource_class": "gpu" if device.type == "cuda" else "cpu",
        "requested_device": device_setting, "effective_device": str(device),
        "gpu_name": torch.cuda.get_device_name(device) if device.type == "cuda" else "",
        "torch_version": str(torch.__version__), "cuda_runtime": str(torch.version.cuda or ""),
        "amp": amp_name, "batch_size": batch_size,
        "gradient_accumulation": accumulation,
        "effective_batch_size": batch_size * accumulation,
        "peak_gpu_memory_mb": round(
            float(torch.cuda.max_memory_allocated(device) / 1024 ** 2), 2,
        ) if device.type == "cuda" else 0.0,
        "elapsed_seconds": round(elapsed, 4),
        "samples_per_second": round(len(fit_positions) * (epoch + 1) / elapsed, 2),
    }
    return predictions


def _latent_sample_positions(
    model, samples: MultiHorizonSamples, positions: np.ndarray, device,
    *, batch_size: int = 2048,
) -> np.ndarray:
    encoded = []
    for start in range(0, len(positions), batch_size):
        current = positions[start:start + batch_size]
        encoded.append(_latent_numpy(model, samples.values[current], device, batch_size=batch_size))
    return np.concatenate(encoded)


def _predict_torch_sample_positions(
    model, samples: MultiHorizonSamples, positions: np.ndarray, device,
    *, ood_mean: np.ndarray | None = None,
    ood_precision: np.ndarray | None = None,
    ood_threshold: float | None = None,
    batch_size: int = 2048,
) -> dict[str, np.ndarray]:
    collected: dict[str, list[np.ndarray]] = {}
    for start in range(0, len(positions), batch_size):
        current = positions[start:start + batch_size]
        predicted = _predict_torch_model(
            model, samples.values[current], device,
            ood_mean=ood_mean, ood_precision=ood_precision,
            ood_threshold=ood_threshold, batch_size=batch_size,
        )
        for key, value in predicted.items():
            collected.setdefault(key, []).append(value)
    return {key: np.concatenate(parts) for key, parts in collected.items()}


def _latent_numpy(model, values: np.ndarray, device, *, batch_size: int = 2048) -> np.ndarray:
    import torch

    model.eval()
    encoded = []
    with torch.no_grad():
        for start in range(0, len(values), batch_size):
            batch = torch.from_numpy(values[start:start + batch_size]).to(device)
            encoded.append(model.encode(batch).float().cpu().numpy())
    return np.concatenate(encoded)


def _predict_torch_model(
    model, values: np.ndarray, device, *, ood_mean: np.ndarray | None = None,
    ood_precision: np.ndarray | None = None, ood_threshold: float | None = None,
    batch_size: int = 2048,
) -> dict[str, np.ndarray]:
    import torch

    model.eval()
    collected: dict[str, list[np.ndarray]] = {
        name: [] for name in ("excess", "return", "up", "net", "quantiles")
    }
    latent = []
    with torch.no_grad():
        for start in range(0, len(values), batch_size):
            batch = torch.from_numpy(values[start:start + batch_size]).to(device)
            latent.append(model.encode(batch).float().cpu().numpy())
            output = model(batch)
            for name in collected:
                collected[name].append(output[name].float().cpu().numpy())
    output_values = {name: np.concatenate(parts) for name, parts in collected.items()}
    quantiles = np.sort(output_values["quantiles"], axis=2)
    uncertainty = quantiles[:, :, 2] - quantiles[:, :, 0]
    if ood_mean is not None and ood_precision is not None and ood_threshold is not None:
        centered = np.concatenate(latent) - np.asarray(ood_mean)
        ood_score = np.sqrt(np.maximum(
            0.0, np.einsum("ij,jk,ik->i", centered, np.asarray(ood_precision), centered),
        ))
        degraded = ood_score > float(ood_threshold)
    else:
        ood_score = uncertainty.mean(axis=1)
        degraded = np.zeros(len(values), dtype=bool)
    return {
        "expected_excess": output_values["excess"],
        "expected_return": output_values["return"],
        "probability_up": _sigmoid(output_values["up"]),
        "probability_net_positive": _sigmoid(output_values["net"]),
        "quantiles": quantiles, "ood_score": ood_score, "degraded": degraded,
    }


def _load_torch_predictions(
    kind: str, artifact: Path, values: np.ndarray, horizons: tuple[int, ...],
) -> dict[str, np.ndarray]:
    import torch

    saved = torch.load(artifact, map_location="cpu", weights_only=False)
    if saved.get("kind") != kind:
        raise ValueError("共享模型种类与工件不一致")
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = _build_torch_model(
        kind, int(saved["input_size"]), int(saved["sequence_length"]), len(horizons),
    ).to(device)
    model.load_state_dict(saved["state_dict"])
    return _predict_torch_model(
        model, values, device,
        ood_mean=saved.get("ood_mean"), ood_precision=saved.get("ood_precision"),
        ood_threshold=saved.get("ood_threshold"),
    )


def predict_multi_bundle(
    panel: dict[str, pd.DataFrame], model: dict[str, Any], *, horizon: int,
) -> PredictionBundle:
    """加载 schema v2 的最近滚动模型；过期或 OOD 样本显式降级。"""
    import hashlib

    from quantmaster.config import get_config

    root = Path(get_config().data_root).resolve()
    manifest_path = confined_path(root, model.get("manifest"), label="共享模型清单")
    if not manifest_path.is_file():
        raise FileNotFoundError("共享模型 manifest 不存在或越出数据目录")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    horizons = tuple(int(value) for value in manifest.get("horizons") or ())
    if int(manifest.get("schema_version", 0)) != 2 or horizon not in horizons:
        raise ValueError("模型不是兼容的 schema v2 多周期工件")
    live = manifest.get("live_artifact") or {}
    artifact = confined_path(root, live.get("artifact"), label="共享模型工件")
    if not artifact.is_file():
        raise FileNotFoundError("共享模型 live 工件不存在")
    if hashlib.sha256(artifact.read_bytes()).hexdigest() != live.get("artifact_sha256"):
        raise ValueError("共享模型 live 工件完整性校验失败")
    feature_spec = FeatureSetSpec.from_dict(manifest.get("features"))
    fundamentals = panel.get("fundamentals")
    if "pit_fundamental_v1" in feature_spec.groups and not isinstance(fundamentals, dict):
        from quantmaster.data.fundamentals import fundamental_panel

        close = panel["close"]
        fundamentals = fundamental_panel(
            list(close.columns), str(close.index.min().date()), str(close.index.max().date()),
        )
    values, metadata, names = make_multi_inference_samples(
        panel, sequence_length=int(manifest["sequence_length"]),
        fundamentals=fundamentals if isinstance(fundamentals, dict) else None,
        feature_spec=feature_spec,
    )
    if names != list(manifest.get("feature_names") or []):
        raise ValueError("共享模型特征模式不一致")
    kind = str(manifest.get("kind") or "")
    if kind == "ridge":
        predicted = _predict_ridge_artifact(artifact, values[:, -1].astype(float))
    else:
        predicted = _load_torch_predictions(kind, artifact, values, horizons)
    predicted = calibrate_prediction_arrays(
        predicted, horizons, manifest.get("calibration_models"),
    )
    close = panel["close"]
    trained_through = pd.Timestamp(manifest.get("trained_through"))
    age = int((pd.DatetimeIndex(close.index).normalize() > trained_through.normalize()).sum())
    if age > int(manifest.get("maximum_age_trading_days", 25)):
        raise RuntimeError(f"共享模型已过期 {age} 个交易日，等待滚动重训")

    def frames(key: str) -> dict[int, pd.DataFrame]:
        result = {}
        for column, current_horizon in enumerate(horizons):
            rows = pd.DataFrame({
                "date": pd.to_datetime([item["date"] for item in metadata]),
                "symbol": [item["symbol"] for item in metadata],
                "value": predicted[key][:, column],
            })
            result[current_horizon] = rows.pivot(
                index="date", columns="symbol", values="value",
            ).reindex_like(close)
        return result

    quantiles: dict[int, dict[str, pd.DataFrame]] = {}
    for column, current_horizon in enumerate(horizons):
        quantiles[current_horizon] = {}
        for q_index, name in enumerate(("q10", "q50", "q90")):
            rows = pd.DataFrame({
                "date": pd.to_datetime([item["date"] for item in metadata]),
                "symbol": [item["symbol"] for item in metadata],
                "value": predicted["quantiles"][:, column, q_index],
            })
            quantiles[current_horizon][name] = rows.pivot(
                index="date", columns="symbol", values="value",
            ).reindex_like(close)
    ood_rows = pd.DataFrame({
        "date": pd.to_datetime([item["date"] for item in metadata]),
        "symbol": [item["symbol"] for item in metadata],
        "ood": predicted["ood_score"], "degraded": predicted["degraded"],
    })
    ood = ood_rows.pivot(index="date", columns="symbol", values="ood").reindex_like(close)
    degraded = ood_rows.pivot(index="date", columns="symbol", values="degraded").reindex_like(close)
    expected_excess = frames("expected_excess")
    expected_excess[horizon] = expected_excess[horizon].mask(degraded.astype(bool))
    uncertainty = {
        value: quantiles[value]["q90"] - quantiles[value]["q10"] for value in horizons
    }
    return PredictionBundle(
        expected_excess=expected_excess,
        expected_return=frames("expected_return"), probability_up=frames("probability_up"),
        probability_net_positive=frames("probability_net_positive"),
        quantiles=quantiles, uncertainty=uncertainty, ood_score=ood,
        degraded=degraded.astype(bool),
    )


def fit_multi_fold(
    kind: str, samples: MultiHorizonSamples, train_positions: np.ndarray,
    valid_positions: np.ndarray, *, artifact_path: str | Path,
    config: dict[str, Any] | None = None, roundtrip_cost: float = 0.002,
    progress: Progress | None = None, cancelled: Cancelled | None = None,
) -> dict[str, Any]:
    config = dict(config or {})
    started = time.perf_counter()
    path = Path(artifact_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if kind == "ridge":
        predictions = _fit_ridge(
            samples, train_positions, valid_positions, path, config, roundtrip_cost,
        )
    elif kind in {"multi-transformer", "multi-tcn", "multi-gru"}:
        predictions = _fit_torch(
            kind, samples, train_positions, valid_positions, path, config,
            roundtrip_cost, progress, cancelled,
        )
    else:
        raise ValueError(f"未知共享模型: {kind}")
    telemetry = predictions.pop("_telemetry", None)
    if telemetry is None:
        elapsed = max(time.perf_counter() - started, 1e-9)
        telemetry = {
            "resource_class": "cpu", "requested_device": str(config.get("device", "auto")),
            "effective_device": "cpu", "cpu_bound": True,
            "peak_gpu_memory_mb": 0.0, "elapsed_seconds": round(elapsed, 4),
            "samples_per_second": round(len(train_positions) / elapsed, 2),
        }
    actual = samples.excess_targets[valid_positions]
    predicted = predictions["expected_excess"]
    correlations = []
    for column in range(actual.shape[1]):
        if np.std(actual[:, column]) > 0 and np.std(predicted[:, column]) > 0:
            correlations.append(float(np.corrcoef(actual[:, column], predicted[:, column])[0, 1]))
    return {
        "kind": kind, "artifact": str(path.resolve()),
        "artifact_sha256": artifact_sha256(path),
        "train_samples": len(train_positions), "validation_samples": len(valid_positions),
        "metrics": {"mean_correlation": float(np.mean(correlations)) if correlations else 0.0},
        "telemetry": telemetry,
        "_predictions": predictions, "_valid_positions": valid_positions,
    }


def predictions_to_frame(
    samples: MultiHorizonSamples, positions: np.ndarray, predictions: dict[str, np.ndarray],
) -> pd.DataFrame:
    rows = []
    for local, sample_position in enumerate(positions):
        meta = samples.metadata[int(sample_position)]
        for column, horizon in enumerate(samples.horizons):
            rows.append({
                "date": meta["date"], "symbol": meta["symbol"], "horizon": horizon,
                "actual_excess": float(samples.excess_targets[sample_position, column]),
                "actual_return": float(samples.raw_targets[sample_position, column]),
                "expected_excess": float(predictions["expected_excess"][local, column]),
                "expected_return": float(predictions["expected_return"][local, column]),
                "probability_up": float(predictions["probability_up"][local, column]),
                "probability_net_positive": float(
                    predictions["probability_net_positive"][local, column]
                ),
                "q10": float(predictions["quantiles"][local, column, 0]),
                "q50": float(predictions["quantiles"][local, column, 1]),
                "q90": float(predictions["quantiles"][local, column, 2]),
                "ood_score": float(predictions["ood_score"][local]),
                "degraded": bool(predictions["degraded"][local]),
            })
    return pd.DataFrame(rows)


def probability_diagnostics(frame: pd.DataFrame) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for horizon, group in frame.groupby("horizon"):
        actual = (group["actual_return"].to_numpy(float) > 0).astype(float)
        probability = group["probability_up"].to_numpy(float).clip(0, 1)
        brier = float(np.mean((probability - actual) ** 2))
        bins = np.minimum((probability * 10).astype(int), 9)
        ece = 0.0
        for number in range(10):
            selected = bins == number
            if selected.any():
                ece += float(selected.mean()) * abs(
                    float(probability[selected].mean() - actual[selected].mean())
                )
        coverage = float(((group["actual_return"] >= group["q10"])
                          & (group["actual_return"] <= group["q90"])).mean())
        result[str(int(horizon))] = {
            "brier": round(brier, 6), "ece": round(ece, 6),
            "interval_80_coverage": round(coverage, 6),
        }
    return result


def write_manifest(path: str | Path, payload: dict[str, Any]) -> str:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(strict_json_dumps(payload, indent=2), encoding="utf-8")
    return str(target.resolve())
