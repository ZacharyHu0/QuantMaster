"""市场状态：牛熊、趋势、板块宽度与短周期展望。"""

from quantmaster.market.fear_greed import classify_opportunity, load_cnn_fear_greed
from quantmaster.market.regime import (
    analyze_bars,
    analyze_market,
    analyze_sectors,
    indicator_frame,
)

__all__ = [
    "analyze_bars",
    "analyze_market",
    "analyze_sectors",
    "classify_opportunity",
    "indicator_frame",
    "load_cnn_fear_greed",
]
