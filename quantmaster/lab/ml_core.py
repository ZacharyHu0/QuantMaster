"""Small, dependency-light ML primitives shared by the Lab backends."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


def engineered_features(panel: dict[str, pd.DataFrame]) -> Iterator[tuple[str, pd.Series]]:
    """Yield model features without importing either training backend."""
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


def resolve_torch_device(torch: Any, requested: str) -> Any:
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


def artifact_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
