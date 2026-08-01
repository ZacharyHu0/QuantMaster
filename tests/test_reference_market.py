from __future__ import annotations

import pandas as pd

from quantmaster.data.reference_market import fetch_reference


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.bdate_range("2026-07-20", periods=5),
            "open": [100, 101, 102, 103, 104],
            "high": [101, 102, 103, 104, 105],
            "low": [99, 100, 101, 102, 103],
            "close": [100.5, 101.5, 102.5, 103.5, 104.5],
        }
    )


def test_reference_market_prefers_validated_sina_route(monkeypatch):
    monkeypatch.setattr(
        "quantmaster.data.reference_market._akshare_route",
        lambda symbol, start: ("sina:test", _frame),
    )
    monkeypatch.setattr(
        "quantmaster.data.yfinance_source.YFinanceSource.daily",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("Yahoo 不应被调用")),
    )

    result = fetch_reference("^GSPC.US", "2026-07-01", "2026-07-31")

    assert result.source == "sina:test"
    assert list(result.frame.columns) == ["open", "high", "low", "close", "volume"]
    assert result.attempts == ()


def test_reference_market_falls_back_per_symbol_and_records_attempt(monkeypatch):
    def failed_sina():
        raise ConnectionError("Sina offline")

    yahoo = _frame().set_index("date")
    monkeypatch.setattr(
        "quantmaster.data.reference_market._akshare_route",
        lambda symbol, start: ("sina:test", failed_sina),
    )
    monkeypatch.setattr(
        "quantmaster.data.yfinance_source.YFinanceSource.daily",
        lambda *args, **kwargs: yahoo,
    )

    result = fetch_reference("^GSPC.US", "2026-07-01", "2026-07-31")

    assert result.source == "yfinance"
    assert result.attempts[0]["source"] == "sina:test"
    assert result.frame.index.max() == pd.Timestamp("2026-07-24")


def test_reference_market_normalizes_tushare_fx_trade_date(monkeypatch):
    raw = pd.DataFrame([{
        "trade_date": "20260730",
        "bid_open": 6.74,
        "bid_high": 6.76,
        "bid_low": 6.73,
        "bid_close": 6.75,
    }])
    monkeypatch.setattr(
        "quantmaster.data.reference_market._akshare_route", lambda symbol, start: None,
    )
    monkeypatch.setattr(
        "quantmaster.data.reference_market._tushare_cny", lambda start, end: raw,
    )

    result = fetch_reference("CNY=X.US", "2026-07-01", "2026-07-31")

    assert result.source == "tushare:fx"
    assert result.frame.index.tolist() == [pd.Timestamp("2026-07-30")]
    assert result.frame.iloc[0]["close"] == 6.75


def test_dollar_index_uses_akshare_before_yahoo(monkeypatch):
    import sys
    from types import SimpleNamespace

    calls = []
    raw = pd.DataFrame({
        "日期": ["2024-01-02", "2024-01-03"],
        "开盘": [102.0, 102.5], "最高": [103.0, 103.5],
        "最低": [101.0, 101.5], "收盘": [102.5, 103.0],
    })

    def global_index(**kwargs):
        calls.append(kwargs)
        return raw

    monkeypatch.setitem(sys.modules, "akshare", SimpleNamespace(
        index_global_hist_em=global_index,
    ))
    result = fetch_reference("DX-Y.NYB.US", "2024-01-01", "2024-01-05")
    assert result.source == "akshare:global-index"
    assert calls == [{"symbol": "美元指数"}]
    assert result.frame["close"].iloc[-1] == 103.0
