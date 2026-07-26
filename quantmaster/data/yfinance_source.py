"""yfinance 数据源（免费）：美股 / 日经 / 韩国 KOSPI / 全球指数 / 部分商品。

用于「参考市场」：美/日/韩/港指数与大宗商品走势，作为 A 股策略的外部信号。
"""

from __future__ import annotations

import pandas as pd

from quantmaster.data.base import DataSource, Market, normalize_daily


def _require_yfinance():
    try:
        import yfinance as yf
        return yf
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            "未安装 yfinance。请执行: pip install yfinance 或 pip install 'quantmaster[data]'"
        ) from e


def to_yahoo_symbol(symbol: str) -> str:
    """QuantMaster 统一符号 -> Yahoo 符号。"""
    if symbol in GLOBAL_REFS:
        return GLOBAL_REFS[symbol][0]
    code, _, suffix = symbol.partition(".")
    suffix = suffix.upper()
    if suffix == "US":
        return code
    if suffix == "HK":
        return f"{code.lstrip('0').zfill(4)}.HK"
    if suffix == "JP":
        return code if code.startswith("^") else f"{code}.T"
    if suffix == "KR":
        return code if code.startswith("^") else f"{code}.KS"
    return code


class YFinanceSource(DataSource):
    name = "yfinance"
    markets = (Market.US, Market.JP, Market.KR, Market.HK, Market.INDEX, Market.FUTURES)

    def daily(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        yf = _require_yfinance()
        ticker = to_yahoo_symbol(symbol)
        raw = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
        if raw is None or raw.empty:
            return pd.DataFrame()
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)
        return normalize_daily(raw)


# 全球参考标的：统一符号 -> (yahoo 符号, 中文名)
GLOBAL_REFS = {
    "^GSPC.US": ("^GSPC", "标普500"),
    "^IXIC.US": ("^IXIC", "纳斯达克"),
    "^DJI.US": ("^DJI", "道琼斯"),
    "^N225.JP": ("^N225", "日经225"),
    "^KS11.KR": ("^KS11", "韩国KOSPI"),
    "^HSI.HK": ("^HSI", "恒生指数"),
    "^HSTECH.HK": ("^HSTECH", "恒生科技"),
    "GC=F.US": ("GC=F", "COMEX黄金"),
    "CL=F.US": ("CL=F", "WTI原油"),
    "HG=F.US": ("HG=F", "COMEX铜"),
    "DX-Y.NYB.US": ("DX-Y.NYB", "美元指数"),
    "CNY=X.US": ("CNY=X", "美元兑人民币"),
    "^TNX.US": ("^TNX", "美债10年收益率"),
}
