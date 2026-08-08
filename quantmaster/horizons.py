"""QuantMaster 研究、回测与决策共用的预测周期。"""

from __future__ import annotations

SUPPORTED_HORIZONS: tuple[int, ...] = (1, 3, 5, 7, 10, 20, 30)
MAX_HORIZON = max(SUPPORTED_HORIZONS)


def require_supported_horizon(value: int) -> int:
    horizon = int(value)
    if horizon not in SUPPORTED_HORIZONS:
        choices = "/".join(str(item) for item in SUPPORTED_HORIZONS)
        raise ValueError(f"预测周期只支持 {choices} 日")
    return horizon
