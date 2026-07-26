"""AKShare 数据源（免费，无需 token）：A 股 / 港股 / 商品期货 / A股指数。

AKShare 聚合了东方财富、新浪财经等公开接口，是 A 股免费数据的事实标准。
安装：pip install "quantmaster[data]"
"""

from __future__ import annotations

import pandas as pd

from quantmaster.data.base import DataSource, Market, guess_market, normalize_daily


def _require_akshare():
    try:
        import akshare as ak  # noqa: PLC0415
        return ak
    except ImportError as e:  # pragma: no cover
        raise ImportError(
            "未安装 akshare。请执行: pip install akshare 或 pip install 'quantmaster[data]'"
        ) from e


def _split(symbol: str) -> tuple[str, str]:
    code, _, suffix = symbol.partition(".")
    return code, suffix.upper()


class AkshareSource(DataSource):
    name = "akshare"
    markets = (Market.CN, Market.HK, Market.FUTURES, Market.INDEX)

    def daily(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        ak = _require_akshare()
        code, suffix = _split(symbol)
        market = guess_market(symbol)
        start_c, end_c = start.replace("-", ""), end.replace("-", "")

        # A 股指数判断：SH 后缀 000 开头是指数（股票为 6/9 开头）；SZ 后缀 399 开头是指数
        is_index = (
            symbol in A_SHARE_INDEXES
            or (suffix == "SH" and code.startswith("000"))
            or (suffix == "SZ" and code.startswith("399"))
        )
        if market == Market.CN and is_index:
            raw = ak.index_zh_a_hist(symbol=code, period="daily", start_date=start_c, end_date=end_c)
        elif market == Market.CN:
            raw = ak.stock_zh_a_hist(
                symbol=code, period="daily", start_date=start_c, end_date=end_c, adjust="qfq"
            )
        elif market == Market.HK:
            raw = ak.stock_hk_hist(
                symbol=code, period="daily", start_date=start_c, end_date=end_c, adjust="qfq"
            )
        elif market == Market.FUTURES:
            # 期货主力连续合约，如 AU0 -> 沪金主力
            raw = ak.futures_zh_daily_sina(symbol=code)
        else:
            raise NotImplementedError(f"akshare 不支持该市场: {symbol}")

        df = normalize_daily(raw)
        return df.loc[start:end]

    def spot(self, symbols: list[str]) -> pd.DataFrame:  # pragma: no cover - 网络
        ak = _require_akshare()
        raw = ak.stock_zh_a_spot_em()
        raw = raw.rename(columns={"代码": "code", "名称": "name", "最新价": "price", "涨跌幅": "change_pct"})
        codes = {s.split(".")[0] for s in symbols}
        rows = raw[raw["code"].isin(codes)][["code", "name", "price", "change_pct"]]
        return rows.reset_index(drop=True)

    def index_members(self, index_symbol: str) -> list[str]:  # pragma: no cover - 网络
        """指数成分股。支持 000300.SH(沪深300)、000905.SH(中证500) 等。"""
        ak = _require_akshare()
        code, _ = _split(index_symbol)
        raw = ak.index_stock_cons_csindex(symbol=code)
        result = []
        for c in raw["成分券代码"].astype(str).str.zfill(6):
            suffix = "SH" if c.startswith(("6", "9")) else ("BJ" if c.startswith(("4", "8")) else "SZ")
            result.append(f"{c}.{suffix}")
        return result


# 常用 A 股指数代码（index_zh_a_hist 接口）
A_SHARE_INDEXES = {
    "000001.SH": "上证综指",
    "000300.SH": "沪深300",
    "000905.SH": "中证500",
    "000852.SH": "中证1000",
    "399001.SZ": "深证成指",
    "399006.SZ": "创业板指",
    "000688.SH": "科创50",
}

# 商品期货主力连续（新浪接口代码）
FUTURES_MAIN = {
    "AU0.SHF": "沪金主力",
    "AG0.SHF": "沪银主力",
    "CU0.SHF": "沪铜主力",
    "RB0.SHF": "螺纹钢主力",
    "SC0.INE": "原油主力",
    "M0.DCE": "豆粕主力",
    "TA0.CZC": "PTA主力",
    "IF0.CFX": "沪深300期指",
}
