from types import SimpleNamespace

import pandas as pd
import pytest

from quantmaster.data.akshare_source import AkshareSource
from quantmaster.data.yfinance_source import YFinanceSource


def _bars() -> pd.DataFrame:
    return pd.DataFrame({
        "date": ["2026-01-02"],
        "open": [10.0],
        "high": [11.0],
        "low": [9.0],
        "close": [10.5],
        "volume": [100.0],
    })


@pytest.mark.parametrize(
    ("symbol", "instrument_type", "function_name"),
    [
        ("600519.SH", "stock", "stock_zh_a_hist"),
        ("510300.SH", "etf", "fund_etf_hist_em"),
        ("00700.HK", "stock", "stock_hk_hist"),
    ],
)
def test_akshare_daily_requests_unadjusted_prices(
    monkeypatch, symbol, instrument_type, function_name,
):
    observed = {}

    def endpoint(**kwargs):
        observed.update(kwargs)
        return _bars()

    monkeypatch.setattr(
        "quantmaster.data.akshare_source._require_akshare",
        lambda: SimpleNamespace(**{function_name: endpoint}),
    )
    monkeypatch.setattr(
        "quantmaster.data.akshare_source._instrument_type",
        lambda _symbol: instrument_type,
    )
    monkeypatch.setattr(
        "quantmaster.data.akshare_source.akshare_call",
        lambda _label, function, **kwargs: function(**kwargs),
    )

    AkshareSource().daily(symbol, "2026-01-01", "2026-01-03")

    assert observed["adjust"] == ""


def test_yfinance_daily_and_batch_disable_implicit_adjustment(monkeypatch):
    observed = []

    class Ticker:
        def history(self, **kwargs):
            observed.append(kwargs)
            return _bars().set_index("date")

    class YF:
        exceptions = SimpleNamespace(YFException=RuntimeError)

        @staticmethod
        def Ticker(_ticker):
            return Ticker()

        @staticmethod
        def download(_tickers, **kwargs):
            observed.append(kwargs)
            return _bars().set_index("date")

    monkeypatch.setattr("quantmaster.data.yfinance_source._require_yfinance", lambda: YF)
    monkeypatch.setattr(
        "quantmaster.data.yfinance_source.to_yahoo_symbol",
        lambda symbol, **_kwargs: symbol.removesuffix(".US"),
    )
    monkeypatch.setattr(
        "quantmaster.data.yfinance_source.provider_call",
        lambda _provider, _key, function, **_kwargs: function(),
    )

    source = YFinanceSource()
    source.daily("MSFT.US", "2026-01-01", "2026-01-03")
    source.daily_many(["MSFT.US"], "2026-01-01", "2026-01-03")

    assert [item["auto_adjust"] for item in observed] == [False, False]
