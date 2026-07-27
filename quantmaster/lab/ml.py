"""Quant Lab 的可选机器学习后端。

核心包不强制安装 PyTorch。Ridge 使用 scikit-learn；序列模型在安装
``quantmaster[ml]`` 后启用，并共享同一套时序切分、Huber 损失和早停规则。
"""

from __future__ import annotations

import importlib.util
import json
import random
from collections.abc import Callable
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

MODEL_KINDS = ("ridge", "mlp", "tcn", "gru", "transformer", "dae")
Progress = Callable[[int, str], None]
Cancelled = Callable[[], bool]


def capabilities() -> dict[str, Any]:
    torch_available = importlib.util.find_spec("torch") is not None
    sklearn_available = importlib.util.find_spec("sklearn") is not None
    return {
        "available_models": [
            name for name in MODEL_KINDS
            if (name == "ridge" and sklearn_available) or (name != "ridge" and torch_available)
        ],
        "torch": torch_available,
        "sklearn": sklearn_available,
        "device": "cuda" if torch_available and _cuda_available() else "cpu",
    }


def _cuda_available() -> bool:
    try:
        import torch

        return bool(torch.cuda.is_available())
    except Exception:
        return False


def engineer_features(panel: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """从可靠日线字段构造 48 个无未来信息的模型输入特征。"""
    close = panel["close"].astype(float)
    volume = panel.get("volume", close * np.nan).astype(float)
    amount = panel.get("amount", volume * close).astype(float)
    high = panel.get("high", close).astype(float)
    low = panel.get("low", close).astype(float)
    opened = panel.get("open", close).astype(float)
    returns = close.pct_change(fill_method=None)
    features: dict[str, pd.DataFrame] = {}

    for window in (1, 2, 3, 5, 10, 20, 40, 60, 120):
        features[f"return_{window}"] = close.pct_change(window, fill_method=None)
    for window in (3, 5, 10, 20, 40, 60, 120):
        features[f"volatility_{window}"] = returns.rolling(window).std()
    for window in (5, 10, 20, 60):
        features[f"mean_return_{window}"] = returns.rolling(window).mean()
    for window in (5, 10, 20, 60, 120):
        features[f"price_bias_{window}"] = close / close.rolling(window).mean() - 1
    volume_safe = volume.replace(0, np.nan)
    amount_safe = amount.replace(0, np.nan)
    for window in (3, 5, 10, 20, 60):
        features[f"volume_ratio_{window}"] = volume_safe / volume_safe.rolling(window).mean() - 1
    for window in (5, 10, 20, 60):
        features[f"amount_ratio_{window}"] = amount_safe / amount_safe.rolling(window).mean() - 1
    price_range = (high - low) / close.replace(0, np.nan)
    features["intraday_return"] = close / opened.replace(0, np.nan) - 1
    features["overnight_return"] = opened / close.shift(1).replace(0, np.nan) - 1
    features["range_1"] = price_range
    for window in (5, 10, 20):
        features[f"range_mean_{window}"] = price_range.rolling(window).mean()
    features["close_location"] = (
        (close - low) / (high - low).replace(0, np.nan) - 0.5
    )
    for window in (10, 20, 60):
        minimum = low.rolling(window).min()
        maximum = high.rolling(window).max()
        features[f"price_position_{window}"] = (
            (close - minimum) / (maximum - minimum).replace(0, np.nan) - 0.5
        )
    features["volume_price_corr_10"] = returns.rolling(10).corr(
        volume_safe.pct_change(fill_method=None)
    )
    features["volume_price_corr_20"] = returns.rolling(20).corr(
        volume_safe.pct_change(fill_method=None)
    )
    features["return_skew_20"] = returns.rolling(20).skew()
    features["return_kurt_20"] = returns.rolling(20).kurt()
    if len(features) != 48:
        raise AssertionError(f"模型特征应为 48 个，实际 {len(features)}")
    return features


def make_samples(
    panel: dict[str, pd.DataFrame],
    *,
    horizon: int = 3,
    sequence_length: int = 20,
    membership: pd.DataFrame | None = None,
) -> tuple[np.ndarray, np.ndarray, list[dict[str, str]], list[str]]:
    """将面板转换为按日期顺序排列的截面样本，避免随机切分造成泄漏。"""
    if horizon not in {1, 3, 5, 7}:
        raise ValueError("horizon 只支持 1/3/5/7 日")
    if sequence_length < 1:
        raise ValueError("sequence_length 必须为正整数")
    features = engineer_features(panel)
    names = list(features)
    close = panel["close"].astype(float)
    indexes = close.index.intersection(next(iter(features.values())).index)
    columns = close.columns
    arrays = [value.reindex(index=indexes, columns=columns).to_numpy(float) for value in features.values()]
    cube = np.stack(arrays, axis=-1)
    target = (close.shift(-horizon) / close - 1).reindex(index=indexes, columns=columns).to_numpy(float)
    member_values = None
    if membership is not None:
        member_values = membership.reindex(index=indexes, columns=columns).fillna(False).to_numpy(bool)

    samples, labels, metadata = [], [], []
    start = sequence_length - 1
    for date_pos in range(start, len(indexes) - horizon):
        cross_section = cube[date_pos - sequence_length + 1:date_pos + 1]
        y_values = target[date_pos]
        for symbol_pos, symbol in enumerate(columns):
            if member_values is not None and not member_values[date_pos, symbol_pos]:
                continue
            sample = cross_section[:, symbol_pos, :]
            label = y_values[symbol_pos]
            if np.isfinite(sample).all() and np.isfinite(label):
                samples.append(sample.astype(np.float32))
                labels.append(float(label))
                metadata.append({
                    "date": pd.Timestamp(indexes[date_pos]).strftime("%Y-%m-%d"),
                    "symbol": str(symbol),
                })
    if not samples:
        raise ValueError("清洗后没有可训练样本；请扩大日期范围或检查数据覆盖率")
    return np.stack(samples), np.asarray(labels, dtype=np.float32), metadata, names


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
    x_train, x_valid = samples[:cutoff], samples[cutoff:]
    y_train, y_valid = targets[:cutoff], targets[cutoff:]
    if min(len(x_train), len(x_valid)) < 10:
        raise ValueError("训练集或验证集样本不足")
    artifact_path = Path(artifact_dir)
    artifact_path.mkdir(parents=True, exist_ok=True)
    if progress:
        progress(10, "准备训练样本")

    if kind == "ridge":
        return _train_ridge(
            x_train, y_train, x_valid, y_valid, artifact_path, config, progress, cancelled
        )
    return _train_torch(
        kind, x_train, y_train, x_valid, y_valid, artifact_path, config, progress, cancelled
    )


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
        json.dumps(history, ensure_ascii=False, indent=2), encoding="utf-8"
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
    }
