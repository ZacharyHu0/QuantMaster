"""大盘 RSI 与 CNN 恐贪的轻量契约测试。"""

import threading

import numpy as np
import pandas as pd

from quantmaster.market import classify_opportunity, indicator_frame
from quantmaster.market.fear_greed import CnnFearGreedRefresher, parse_cnn_fear_greed


def test_rsi_and_fear_greed_opportunity_contract():
    dates = pd.bdate_range("2026-01-02", periods=40)
    falling = pd.DataFrame({"close": np.linspace(100, 60, len(dates))}, index=dates)
    rsi = float(indicator_frame(falling)["rsi_14"].iloc[-1])

    assert rsi < 22
    assert classify_opportunity(rsi)["code"] == "rsi_oversold"
    assert classify_opportunity(rsi, 9.9)["code"] == "rare_bottom"

    parsed = parse_cnn_fear_greed(
        {
            "fear_and_greed": {
                "score": 9.9,
                "rating": "extreme fear",
                "timestamp": "2026-08-06T08:00:00Z",
            },
            "fear_and_greed_historical": {
                "data": [
                    {"x": 1785974400000, "y": 8.5, "rating": "extreme fear"},
                    {"x": 1786060800000, "y": 9.9, "rating": "extreme fear"},
                ],
            },
        }
    )
    assert parsed["score"] == 9.9
    assert parsed["rating_label"] == "极度恐惧"
    assert parsed["history"] == [
        {"date": "2026-08-06", "score": 8.5, "rating": "extreme fear", "rating_label": "极度恐惧"},
        {"date": "2026-08-07", "score": 9.9, "rating": "extreme fear", "rating_label": "极度恐惧"},
    ]
    assert parsed["thresholds"] == {"rsi_add": 22.0, "fear_greed_rare": 10.0}


def test_cnn_refresher_runs_immediately_and_retries_stale_result():
    calls = 0
    retried = threading.Event()

    def refresh():
        nonlocal calls
        calls += 1
        if calls == 2:
            retried.set()
        return {"status": "stale"}

    refresher = CnnFearGreedRefresher(
        refresh, interval_seconds=10, retry_seconds=0.01,
    )
    try:
        assert refresher.start()
        assert not refresher.start()
        assert retried.wait(timeout=1)
    finally:
        refresher.stop()
    assert calls >= 2


def test_cnn_refresher_uses_normal_interval_after_success():
    called = threading.Event()

    def refresh():
        called.set()
        return {"status": "ready"}

    refresher = CnnFearGreedRefresher(
        refresh, interval_seconds=10, retry_seconds=0.01,
    )
    try:
        assert refresher.start()
        assert called.wait(timeout=1)
        called.clear()
        assert not called.wait(timeout=0.03)
    finally:
        refresher.stop()
