"""yfinance 数据源（免费）：美股 / 日经 / 韩国 KOSPI / 全球指数 / 部分商品。

用于「参考市场」：美/日/韩/港指数与大宗商品走势，作为 A 股策略的外部信号。
"""

from __future__ import annotations

import pandas as pd

from quantmaster.data.base import DataCapability, DataSource, Market, normalize_daily
from quantmaster.data.resilience import provider_call


def _require_yfinance():
    try:
        import yfinance as yf
        return yf
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            "未安装 yfinance。请执行: pip install yfinance 或 pip install 'quantmaster[data]'"
        ) from e


def to_yahoo_symbol(symbol: str, *, as_of: str = "") -> str:
    """Resolve a verified Yahoo alias; suffix concatenation is intentionally forbidden."""
    from quantmaster.data.instruments import InstrumentStore

    return InstrumentStore().provider_alias(symbol, "yahoo", as_of=as_of).provider_symbol


class YFinanceSource(DataSource):
    name = "yfinance"
    markets = (Market.US, Market.JP, Market.KR, Market.HK, Market.INDEX, Market.FUTURES)
    capabilities = frozenset({DataCapability.DAILY})

    def daily(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        yf = _require_yfinance()
        ticker = to_yahoo_symbol(symbol, as_of=end)
        inclusive_end = str((pd.Timestamp(end) + pd.Timedelta(days=1)).date())
        key = f"history:{ticker}:{start}:{inclusive_end}"

        def fetch():
            return yf.Ticker(ticker).history(
                start=start, end=inclusive_end, auto_adjust=True, actions=False,
                raise_errors=True,
            )

        try:
            raw = provider_call("yahoo", key, fetch, empty_opens=True)
        except yf.exceptions.YFException as exc:
            raise RuntimeError(str(exc).strip() or f"Yahoo {ticker} 请求失败") from exc
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)
        return normalize_daily(raw)

    def daily_many(
        self, symbols: list[str], start: str, end: str,
    ) -> dict[str, pd.DataFrame]:
        """用一次 Yahoo 批量下载完成全球参考标的同步。"""
        if not symbols:
            return {}
        yf = _require_yfinance()
        mapping = {symbol: to_yahoo_symbol(symbol, as_of=end) for symbol in symbols}
        inclusive_end = str((pd.Timestamp(end) + pd.Timedelta(days=1)).date())
        key = f"batch:{','.join(sorted(mapping.values()))}:{start}:{inclusive_end}"

        def fetch():
            return yf.download(
                list(mapping.values()), start=start, end=inclusive_end,
                progress=False, auto_adjust=True, actions=False, threads=False,
                group_by="ticker",
            )

        try:
            raw = provider_call("yahoo", key, fetch, empty_opens=True)
        except yf.exceptions.YFException as exc:
            raise RuntimeError(str(exc).strip() or "Yahoo 批量请求失败") from exc
        result: dict[str, pd.DataFrame] = {}
        for symbol, ticker in mapping.items():
            try:
                if isinstance(raw.columns, pd.MultiIndex):
                    first = set(map(str, raw.columns.get_level_values(0)))
                    frame = raw[ticker] if ticker in first else raw.xs(ticker, axis=1, level=1)
                elif len(mapping) == 1:
                    frame = raw
                else:
                    continue
                normalized = normalize_daily(frame.dropna(how="all"))
                if not normalized.empty:
                    result[symbol] = normalized.loc[start:end]
            except (KeyError, TypeError, ValueError):
                continue
        return result


# Global references use local display symbols; provider symbols live in aliases.
GLOBAL_REFS = {
    "SPX.INDEX": ("^GSPC", "标普500"),
    "IXIC.INDEX": ("^IXIC", "纳斯达克"),
    "DJI.INDEX": ("^DJI", "道琼斯"),
    "N225.INDEX": ("^N225", "日经225"),
    "KS11.INDEX": ("^KS11", "韩国KOSPI"),
    "HSI.INDEX": ("^HSI", "恒生指数"),
    "HSTECH.INDEX": ("^HSTECH", "恒生科技"),
    "GC.CONTINUOUS": ("GC=F", "COMEX黄金"),
    "CL.CONTINUOUS": ("CL=F", "WTI原油"),
    "HG.CONTINUOUS": ("HG=F", "COMEX铜"),
    "DXY.INDEX": ("DX-Y.NYB", "美元指数"),
    "USD-CNY.FX": ("CNY=X", "美元兑人民币"),
    "US10Y.RATE": ("^TNX", "美债10年收益率"),
}

REFERENCE_IDENTITIES = {
    "SPX.INDEX": {"market": "US", "exchange": "S&P DJI", "asset_type": "index", "currency": "USD"},
    "IXIC.INDEX": {"market": "US", "exchange": "NASDAQ", "asset_type": "index", "currency": "USD"},
    "DJI.INDEX": {"market": "US", "exchange": "S&P DJI", "asset_type": "index", "currency": "USD"},
    "N225.INDEX": {"market": "JP", "exchange": "JPX", "asset_type": "index", "currency": "JPY"},
    "KS11.INDEX": {"market": "KR", "exchange": "KRX", "asset_type": "index", "currency": "KRW"},
    "HSI.INDEX": {"market": "HK", "exchange": "HKEX", "asset_type": "index", "currency": "HKD"},
    "HSTECH.INDEX": {"market": "HK", "exchange": "HKEX", "asset_type": "index", "currency": "HKD"},
    "GC.CONTINUOUS": {
        "market": "FUT", "exchange": "COMEX", "asset_type": "future_continuous",
        "currency": "USD", "contract_kind": "provider_current_active_series",
        "product_code": "GC", "multiplier": "100 troy ounces",
        "quote_unit": "USD/troy ounce", "timezone": "America/New_York",
        "roll_rule": "provider_undocumented", "adjustment": "provider_undocumented",
    },
    "CL.CONTINUOUS": {
        "market": "FUT", "exchange": "NYMEX", "asset_type": "future_continuous",
        "currency": "USD", "contract_kind": "provider_current_active_series",
        "product_code": "CL", "multiplier": "1000 barrels",
        "quote_unit": "USD/barrel", "timezone": "America/New_York",
        "roll_rule": "provider_undocumented", "adjustment": "provider_undocumented",
    },
    "HG.CONTINUOUS": {
        "market": "FUT", "exchange": "COMEX", "asset_type": "future_continuous",
        "currency": "USD", "contract_kind": "provider_current_active_series",
        "product_code": "HG", "multiplier": "25000 pounds",
        "quote_unit": "USD/pound", "timezone": "America/New_York",
        "roll_rule": "provider_undocumented", "adjustment": "provider_undocumented",
    },
    "DXY.INDEX": {"market": "US", "exchange": "ICE", "asset_type": "index", "currency": "USD"},
    "USD-CNY.FX": {
        "market": "FX", "exchange": "OTC", "asset_type": "forex", "currency": "CNY",
        "base_currency": "USD", "quote_currency": "CNY", "timezone": "UTC",
    },
    "US10Y.RATE": {"market": "US", "exchange": "US TREASURY", "asset_type": "index", "currency": "USD"},
}
