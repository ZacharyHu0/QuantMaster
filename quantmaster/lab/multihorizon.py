"""共享编码器的 1/3/5/7 日多任务训练、校准和推理。"""

from __future__ import annotations

import json
import math
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from quantmaster.lab.ml import artifact_sha256, engineer_features, normalize_features
from quantmaster.lab.research import HORIZONS, FeatureSetSpec, TimeFold

Progress = Callable[[int, str], None]
Cancelled = Callable[[], bool]


@dataclass
class MultiHorizonSamples:
    values: np.ndarray
    excess_targets: np.ndarray
    raw_targets: np.ndarray
    metadata: list[dict[str, Any]]
    feature_names: list[str]
    horizons: tuple[int, ...]


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


def engineer_research_features(
    panel: dict[str, pd.DataFrame], *, fundamentals: dict[str, pd.DataFrame] | None = None,
    spec: FeatureSetSpec | None = None,
) -> dict[str, pd.DataFrame]:
    """版本化特征注册表；所有变换仅使用当日及过去数据。"""
    spec = spec or FeatureSetSpec(groups=("price_volume_v2",))
    close = panel["close"].astype(float)
    features: dict[str, pd.DataFrame] = {}
    if "price_volume_v2" in spec.groups:
        features.update(engineer_features(panel))
    if "market_context_v1" in spec.groups:
        returns = close.pct_change(fill_method=None)
        market_return = returns.median(axis=1)
        for window in (1, 5, 20):
            series = market_return if window == 1 else market_return.rolling(window).sum()
            features[f"market_return_{window}"] = _broadcast(series, close.columns)
        features["market_breadth_up"] = _broadcast((returns > 0).mean(axis=1), close.columns)
        moving = close.rolling(20).mean()
        features["market_breadth_above_20"] = _broadcast(
            (close > moving).mean(axis=1), close.columns,
        )
        features["market_dispersion_20"] = _broadcast(
            returns.rolling(20).std().median(axis=1), close.columns,
        )
        features["market_volatility_20"] = _broadcast(
            market_return.rolling(20).std(), close.columns,
        )
        market_index = (1 + market_return.fillna(0)).cumprod()
        features["market_drawdown_60"] = _broadcast(
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
            features[f"fundamental_{output}"] = transform(raw.astype(float))
            features[f"fundamental_{output}_observed"] = raw.notna().astype(float)
    if spec.include_news and "news_v1" in spec.groups:
        news = (fundamentals or {}).get("news_sentiment", close * np.nan)
        features["news_sentiment"] = news.reindex_like(close)
        features["news_sentiment_observed"] = news.reindex_like(close).notna().astype(float)
    if not features:
        raise ValueError("特征注册表为空")
    return features


def _feature_cube(
    panel: dict[str, pd.DataFrame], fundamentals: dict[str, pd.DataFrame] | None,
    feature_spec: FeatureSetSpec,
) -> tuple[np.ndarray, np.ndarray, pd.DatetimeIndex, pd.Index, list[str]]:
    raw = engineer_research_features(panel, fundamentals=fundamentals, spec=feature_spec)
    normalized, validity = normalize_features(raw)
    close = panel["close"].astype(float)
    dates, columns = pd.DatetimeIndex(close.index), close.columns
    names = list(normalized)
    cube = np.stack([
        normalized[name].reindex(index=dates, columns=columns).to_numpy(np.float32)
        for name in names
    ], axis=-1)
    valid = np.stack([
        validity[name].reindex(index=dates, columns=columns).fillna(False).to_numpy(bool)
        for name in names
    ], axis=-1)
    return cube, valid, dates, columns, names


def make_multi_horizon_samples(
    panel: dict[str, pd.DataFrame], *, horizons: tuple[int, ...] = HORIZONS,
    sequence_length: int = 20, membership: pd.DataFrame | None = None,
    fundamentals: dict[str, pd.DataFrame] | None = None,
    feature_spec: FeatureSetSpec | None = None,
) -> MultiHorizonSamples:
    feature_spec = feature_spec or FeatureSetSpec(groups=("price_volume_v2",))
    if not horizons or any(value not in HORIZONS for value in horizons):
        raise ValueError("horizons 只支持 1/3/5/7 日")
    close = panel["close"].astype(float)
    cube, validity, dates, columns, names = _feature_cube(
        panel, fundamentals, feature_spec,
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
    values, raw_targets, excess_targets, metadata = [], [], [], []
    maximum_horizon = max(horizons)
    for date_pos in range(sequence_length - 1, len(dates) - maximum_horizon):
        target_dates = {
            str(horizon): dates[date_pos + horizon].strftime("%Y-%m-%d") for horizon in horizons
        }
        for symbol_pos, symbol in enumerate(columns):
            if member_values is not None and not member_values[date_pos, symbol_pos]:
                continue
            sample = cube[date_pos - sequence_length + 1:date_pos + 1, symbol_pos]
            coverage = float(
                validity[date_pos - sequence_length + 1:date_pos + 1, symbol_pos].mean()
            )
            raw = np.asarray([array[date_pos, symbol_pos] for array in raw_arrays], dtype=np.float32)
            excess = np.asarray(
                [array[date_pos, symbol_pos] for array in excess_arrays], dtype=np.float32,
            )
            if (coverage >= feature_spec.minimum_coverage and np.isfinite(sample).all()
                    and np.isfinite(raw).all() and np.isfinite(excess).all()):
                values.append(sample)
                raw_targets.append(raw)
                excess_targets.append(excess)
                metadata.append({
                    "date": dates[date_pos].strftime("%Y-%m-%d"),
                    "symbol": str(symbol), "target_dates": target_dates,
                    "feature_coverage": coverage,
                })
    if not values:
        raise ValueError("清洗后没有共享多周期训练样本")
    return MultiHorizonSamples(
        values=np.stack(values).astype(np.float32),
        excess_targets=np.stack(excess_targets).astype(np.float32),
        raw_targets=np.stack(raw_targets).astype(np.float32),
        metadata=metadata, feature_names=names, horizons=tuple(horizons),
    )


def make_multi_inference_samples(
    panel: dict[str, pd.DataFrame], *, sequence_length: int,
    fundamentals: dict[str, pd.DataFrame] | None, feature_spec: FeatureSetSpec,
) -> tuple[np.ndarray, list[dict[str, Any]], list[str]]:
    cube, validity, dates, columns, names = _feature_cube(panel, fundamentals, feature_spec)
    values, metadata = [], []
    for date_pos in range(sequence_length - 1, len(dates)):
        for symbol_pos, symbol in enumerate(columns):
            sample = cube[date_pos - sequence_length + 1:date_pos + 1, symbol_pos]
            coverage = float(
                validity[date_pos - sequence_length + 1:date_pos + 1, symbol_pos].mean()
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
    dates = np.asarray([item["date"] for item in samples.metadata])
    latest_targets = np.asarray([
        max(item["target_dates"].values()) for item in samples.metadata
    ])
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
    from sklearn.linear_model import Ridge

    x_train = samples.values[train, -1].astype(float)
    x_valid = samples.values[valid, -1].astype(float)
    alpha = float(config.get("alpha", 1.0))
    excess = Ridge(alpha=alpha).fit(x_train, samples.excess_targets[train])
    raw = Ridge(alpha=alpha).fit(x_train, samples.raw_targets[train])
    up_coef, up_intercept, net_coef, net_intercept = [], [], [], []
    for column in range(len(samples.horizons)):
        coefficient, intercept = _fit_logistic(
            x_train, (samples.raw_targets[train, column] > 0).astype(int),
        )
        up_coef.append(coefficient)
        up_intercept.append(intercept)
        coefficient, intercept = _fit_logistic(
            x_train, (samples.raw_targets[train, column] > roundtrip_cost).astype(int),
        )
        net_coef.append(coefficient)
        net_intercept.append(intercept)
    raw_train_prediction = raw.predict(x_train)
    residual = samples.raw_targets[train] - raw_train_prediction
    residual_quantiles = np.quantile(residual, (0.1, 0.5, 0.9), axis=0).T
    mean = x_train.mean(axis=0)
    scale = x_train.std(axis=0)
    scale[scale < 1e-6] = 1.0
    train_ood = np.sqrt(np.mean(((x_train - mean) / scale) ** 2, axis=1))
    threshold = float(np.quantile(train_ood, 0.995))
    np.savez_compressed(
        artifact,
        excess_coef=excess.coef_, excess_intercept=np.atleast_1d(excess.intercept_),
        raw_coef=raw.coef_, raw_intercept=np.atleast_1d(raw.intercept_),
        up_coef=np.asarray(up_coef), up_intercept=np.asarray(up_intercept),
        net_coef=np.asarray(net_coef), net_intercept=np.asarray(net_intercept),
        residual_quantiles=residual_quantiles, ood_mean=mean, ood_scale=scale,
        ood_threshold=np.asarray([threshold]),
    )
    return _predict_ridge_artifact(artifact, x_valid)


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
    dates = np.asarray([samples.metadata[int(position)]["date"] for position in train])
    unique_dates = np.unique(dates)
    validation_days = min(63, max(20, len(unique_dates) // 8))
    if len(unique_dates) <= validation_days + max(samples.horizons) + 20:
        raise ValueError("训练窗不足以创建独立的深度模型早停区间")
    validation_start = unique_dates[-validation_days]
    latest_targets = np.asarray([
        max(samples.metadata[int(position)]["target_dates"].values()) for position in train
    ])
    fit = train[(dates < validation_start) & (latest_targets < validation_start)]
    early_stop = train[dates >= validation_start]
    if min(len(fit), len(early_stop)) < 20:
        raise ValueError("深度模型内部训练或早停样本不足")
    return fit, early_stop


def _fit_torch(
    kind: str, samples: MultiHorizonSamples, train: np.ndarray, valid: np.ndarray,
    artifact: Path, config: dict[str, Any], roundtrip_cost: float,
    progress: Progress | None, cancelled: Cancelled | None,
) -> dict[str, np.ndarray]:
    try:
        import torch
        from torch import nn
        from torch.utils.data import DataLoader, TensorDataset
    except ImportError as exc:
        raise RuntimeError("共享深度模型需要安装 PyTorch：pip install 'quantmaster[ml]'") from exc
    device_setting = str(config.get("device", "auto"))
    device = torch.device(
        "cuda" if device_setting == "auto" and torch.cuda.is_available()
        else device_setting if device_setting != "auto" else "cpu"
    )
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
    dataset = TensorDataset(
        torch.from_numpy(samples.values[fit_positions]),
        torch.from_numpy(samples.excess_targets[fit_positions]),
        torch.from_numpy(samples.raw_targets[fit_positions]),
    )
    generator = torch.Generator().manual_seed(int(config.get("seed", 42)))
    loader = DataLoader(
        dataset, batch_size=batch_size, shuffle=True, generator=generator,
        pin_memory=device.type == "cuda",
    )
    use_amp = device.type == "cuda"
    scaler = torch.amp.GradScaler("cuda", enabled=use_amp)
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
    valid_x = torch.from_numpy(samples.values[early_stop_positions]).to(device)
    valid_excess = torch.from_numpy(samples.excess_targets[early_stop_positions]).to(device)
    valid_raw = torch.from_numpy(samples.raw_targets[early_stop_positions]).to(device)
    for epoch in range(epochs):
        if cancelled and cancelled():
            raise InterruptedError("训练已取消")
        model.train()
        optimizer.zero_grad(set_to_none=True)
        for step, (batch_x, batch_excess, batch_raw) in enumerate(loader):
            batch_x = batch_x.to(device, non_blocking=True)
            batch_excess = batch_excess.to(device, non_blocking=True)
            batch_raw = batch_raw.to(device, non_blocking=True)
            with torch.autocast(device_type=device.type, dtype=torch.float16, enabled=use_amp):
                loss = loss_for(model(batch_x), batch_excess, batch_raw) / accumulation
            scaler.scale(loss).backward()
            if (step + 1) % accumulation == 0 or step + 1 == len(loader):
                scaler.unscale_(optimizer)
                nn.utils.clip_grad_norm_(model.parameters(), 2.0)
                scaler.step(optimizer)
                scaler.update()
                optimizer.zero_grad(set_to_none=True)
        model.eval()
        with torch.no_grad(), torch.autocast(device_type=device.type, dtype=torch.float16, enabled=use_amp):
            validation_loss = float(loss_for(model(valid_x), valid_excess, valid_raw).cpu())
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
    train_latent = _latent_numpy(model, samples.values[ood_positions], device)
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
    return _predict_torch_model(
        model, samples.values[valid], device,
        ood_mean=ood_mean, ood_precision=ood_precision, ood_threshold=ood_threshold,
    )


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
    manifest_path = (root / str(model.get("manifest") or "")).resolve()
    if not manifest_path.is_relative_to(root) or not manifest_path.is_file():
        raise FileNotFoundError("共享模型 manifest 不存在或越出数据目录")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    horizons = tuple(int(value) for value in manifest.get("horizons") or ())
    if int(manifest.get("schema_version", 0)) != 2 or horizon not in horizons:
        raise ValueError("模型不是兼容的 schema v2 多周期工件")
    live = manifest.get("live_artifact") or {}
    artifact = (root / str(live.get("artifact") or "")).resolve()
    if not artifact.is_relative_to(root) or not artifact.is_file():
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
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(target.resolve())
