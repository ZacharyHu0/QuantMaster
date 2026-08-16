"""大盘 RSI 与 A 股 FundDB 恐贪的轻量契约测试。"""

import base64
import threading

import numpy as np
import pandas as pd
import pytest

from quantmaster.data.resilience import ProviderContractChanged
from quantmaster.market import classify_opportunity, indicator_frame
from quantmaster.market.ashare_fear_greed import (
    _FUNDB_AES_IV,
    _FUNDB_AES_KEY,
    AShareFearGreedRefresher,
    _funddb_decrypt,
    _funddb_payload_to_frame,
    _funddb_signed_body,
    parse_ashare_fear_greed,
)
from quantmaster.market.fear_greed import CnnFearGreedRefresher, parse_cnn_fear_greed


def test_rsi_and_ashare_fear_greed_opportunity_contract():
    dates = pd.bdate_range("2026-01-02", periods=40)
    falling = pd.DataFrame({"close": np.linspace(100, 60, len(dates))}, index=dates)
    rsi = float(indicator_frame(falling)["rsi_14"].iloc[-1])

    assert rsi < 22
    assert classify_opportunity(rsi)["code"] == "rsi_oversold"
    assert classify_opportunity(rsi, 9.9)["code"] == "rare_bottom"

    parsed = parse_ashare_fear_greed(
        pd.DataFrame(
            {
                "date": ["2026-08-06", "2026-08-07"],
                "fear": [8.5, 9.9],
                "index": [3_500.12, 3_512.34],
            }
        ),
        symbol="上证指数",
    )
    assert parsed["symbol"] == "上证指数"
    assert parsed["score"] == 9.9
    assert parsed["rating_label"] == "极度恐惧"
    assert parsed["benchmark_value"] == 3512.34
    assert parsed["history"] == [
        {"date": "2026-08-06", "score": 8.5, "benchmark": 3500.12},
        {"date": "2026-08-07", "score": 9.9, "benchmark": 3512.34},
    ]
    assert parsed["thresholds"] == {"rsi_add": 22.0, "fear_greed_rare": 10.0}


def test_cnn_fear_greed_parser_contract():
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


def test_ashare_fear_greed_rejects_invalid_contract():
    with pytest.raises(ValueError, match="缺少字段"):
        parse_ashare_fear_greed(
            pd.DataFrame({"date": ["2026-08-07"], "fear": [33.0]}),
            symbol="上证指数",
        )

    with pytest.raises(ValueError, match="超出 0-100"):
        parse_ashare_fear_greed(
            pd.DataFrame(
                {"date": ["2026-08-07"], "fear": [101.0], "index": [3_500.0]}
            ),
            symbol="上证指数",
        )


def test_funddb_public_payload_and_request_contract():
    body = _funddb_signed_body({"gu_code": "000001.SH", "time": -1})
    assert body["type"] == "pc"
    assert body["version"] == "2.2.7"
    assert isinstance(body["act_time"], int)
    assert len([key for key in body if key.startswith(("tir", "abi", "u54", "kf54"))]) == 4

    frame = _funddb_payload_to_frame(
        {
            "code": 0,
            "data": {
                "xAxis": {"categories": ["2026-08-06", "2026-08-07"]},
                "series": [
                    {"name": "恐惧贪婪", "data": [8.5, 9.9]},
                    {"name": "上证指数(点击隐藏)", "data": [3500.12, 3512.34]},
                ],
            },
        },
        symbol="上证指数",
    )
    assert frame.to_dict(orient="records") == [
        {"date": "2026-08-06", "fear": 8.5, "index": 3500.12},
        {"date": "2026-08-07", "fear": 9.9, "index": 3512.34},
    ]

    with pytest.raises(ProviderContractChanged, match="序列长度不一致"):
        _funddb_payload_to_frame(
            {
                "code": 0,
                "data": {
                    "xAxis": {"categories": ["2026-08-07"]},
                    "series": [
                        {"name": "恐惧贪婪", "data": [33.0, 34.0]},
                        {"name": "上证指数(点击隐藏)", "data": [3500.0]},
                    ],
                },
            },
            symbol="上证指数",
        )


def test_funddb_decrypts_current_repeated_padding_contract():
    pytest.importorskip("Crypto")
    from Crypto.Cipher import AES

    padded = b"{}" + bytes([30]) * 30
    encrypted = AES.new(
        (_FUNDB_AES_KEY + "ll1").encode("utf-8"),
        AES.MODE_CBC,
        (_FUNDB_AES_IV + "ll1")[:16].encode("utf-8"),
    ).encrypt(padded)
    assert _funddb_decrypt(base64.b64encode(encrypted).decode("ascii")) == {}


def test_ashare_refresher_runs_immediately_and_retries_stale_result():
    calls = 0
    retried = threading.Event()

    def refresh():
        nonlocal calls
        calls += 1
        if calls == 2:
            retried.set()
        return {"status": "stale"}

    refresher = AShareFearGreedRefresher(
        refresh, interval_seconds=10, retry_seconds=0.01,
    )
    try:
        assert refresher.start()
        assert not refresher.start()
        assert retried.wait(timeout=1)
    finally:
        refresher.stop()
    assert calls >= 2


def test_ashare_refresher_uses_normal_interval_after_success():
    called = threading.Event()

    def refresh():
        called.set()
        return {"status": "ready"}

    refresher = AShareFearGreedRefresher(
        refresh, interval_seconds=10, retry_seconds=0.01,
    )
    try:
        assert refresher.start()
        assert called.wait(timeout=1)
        called.clear()
        assert not called.wait(timeout=0.03)
    finally:
        refresher.stop()
