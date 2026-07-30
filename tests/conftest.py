"""测试夹具：合成行情面板 + 隔离的临时数据目录（不触网）。"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from quantmaster.config import Config, set_config


@pytest.fixture(autouse=True)
def isolated_config(tmp_path):
    """每个测试用独立数据目录，避免污染真实数据。"""
    from quantmaster.data.repair import reset_data_repair_manager_for_tests
    from quantmaster.rotation.service import reset_rotation_runtime_for_tests
    from quantmaster.runtime.sqlite import reset_sqlite_runtime_for_tests

    reset_data_repair_manager_for_tests()
    reset_rotation_runtime_for_tests()
    reset_sqlite_runtime_for_tests()
    cfg = Config()
    cfg.data.root = str(tmp_path / "data")
    set_config(cfg)
    yield cfg
    reset_data_repair_manager_for_tests()
    reset_rotation_runtime_for_tests()
    set_config(None)
    reset_sqlite_runtime_for_tests()


def make_panel(days: int = 150, n: int = 8, seed: int = 7) -> dict[str, pd.DataFrame]:
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2023-01-02", periods=days)
    symbols = [f"60000{i}.SH" for i in range(n)]
    log_returns = rng.normal(0.0003, 0.02, (days, n))
    close = pd.DataFrame(
        100 * np.exp(np.cumsum(log_returns, axis=0)), index=dates, columns=symbols
    )
    open_px = close * (1 + rng.normal(0, 0.003, (days, n)))
    high = np.maximum(close, open_px) * (1 + abs(rng.normal(0, 0.005, (days, n))))
    low = np.minimum(close, open_px) * (1 - abs(rng.normal(0, 0.005, (days, n))))
    volume = pd.DataFrame(rng.uniform(1e6, 1e7, (days, n)), index=dates, columns=symbols)
    return {
        "open": open_px, "high": pd.DataFrame(high, index=dates, columns=symbols),
        "low": pd.DataFrame(low, index=dates, columns=symbols),
        "close": close, "volume": volume,
        "amount": volume * close, "turnover": volume / 1e8,
    }


@pytest.fixture
def panel() -> dict[str, pd.DataFrame]:
    return make_panel()
