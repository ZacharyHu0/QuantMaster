"""AKShare 数据源（免费，无需 token）：A 股 / 港股 / 商品期货 / A股指数。

AKShare 聚合了东方财富、新浪财经等公开接口，是 A 股免费数据的事实标准。
安装：pip install "quantmaster[data]"
"""

from __future__ import annotations

import pandas as pd

from quantmaster.data.base import (
    DataCapability,
    DataSource,
    Market,
    guess_market,
    normalize_bars,
    normalize_daily,
    validate_frequency,
)
from quantmaster.data.resilience import akshare_call


def _require_akshare():
    try:
        import akshare as ak
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
    capabilities = frozenset({
        DataCapability.DAILY,
        DataCapability.INTRADAY,
        DataCapability.SPOT,
        DataCapability.INDEX_MEMBERS,
    })

    def daily(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        ak = _require_akshare()
        code, suffix = _split(symbol)
        market = guess_market(symbol)
        start_c, end_c = start.replace("-", ""), end.replace("-", "")
        instrument_type = _instrument_type(symbol)

        # A 股指数判断：SH 后缀 000 开头是指数（股票为 6/9 开头）；SZ 后缀 399 开头是指数
        is_index = (
            symbol in A_SHARE_INDEXES
            or (suffix == "SH" and code.startswith("000"))
            or (suffix == "SZ" and code.startswith("399"))
            or instrument_type == "index"
        )
        if market == Market.CN and is_index:
            raw = akshare_call(
                f"index_zh_a_hist({symbol})", ak.index_zh_a_hist,
                symbol=code, period="daily", start_date=start_c, end_date=end_c,
            )
        elif market == Market.CN and instrument_type in {"etf", "fund"}:
            raw = akshare_call(
                f"fund_etf_hist_em({symbol})", ak.fund_etf_hist_em,
                symbol=code, period="daily", start_date=start_c, end_date=end_c,
                adjust="",
            )
        elif market == Market.CN:
            raw = akshare_call(
                f"stock_zh_a_hist({symbol})", ak.stock_zh_a_hist,
                symbol=code, period="daily", start_date=start_c, end_date=end_c,
                adjust="",
            )
        elif market == Market.HK:
            raw = akshare_call(
                f"stock_hk_hist({symbol})", ak.stock_hk_hist,
                symbol=code, period="daily", start_date=start_c, end_date=end_c,
                adjust="",
            )
        elif market == Market.FUTURES:
            # 期货主力连续合约，如 AU0 -> 沪金主力
            raw = akshare_call(
                f"futures_zh_daily_sina({symbol})", ak.futures_zh_daily_sina,
                symbol=code, lane="akshare:sina",
            )
        else:
            raise NotImplementedError(f"akshare 不支持该市场: {symbol}")

        df = normalize_daily(raw)
        return df.loc[start:end]

    def intraday(
        self, symbol: str, start: str, end: str, frequency: str = "5m"
    ) -> pd.DataFrame:
        """A 股/港股/指数分钟线。

        1m 数据源只提供近期。所有分钟频率均保存不复权价格，确保每日增量
        归档不会因前复权基准变化产生接缝跳空；日线研究仍使用前复权数据。
        """
        ak = _require_akshare()
        frequency = validate_frequency(frequency)
        if frequency == "1d":
            return self.daily(symbol, start, end)
        period = frequency[:-1]
        code, suffix = _split(symbol)
        market = guess_market(symbol)
        instrument_type = _instrument_type(symbol)
        is_index = (
            symbol in A_SHARE_INDEXES
            or (suffix == "SH" and code.startswith("000"))
            or (suffix == "SZ" and code.startswith("399"))
            or instrument_type == "index"
        )
        if market == Market.CN and is_index:
            raw = akshare_call(
                f"index_zh_a_hist_min_em({symbol},{frequency})",
                ak.index_zh_a_hist_min_em,
                symbol=code, period=period, start_date=start, end_date=end,
            )
        elif market == Market.CN and instrument_type in {"etf", "fund"}:
            raw = akshare_call(
                f"fund_etf_hist_min_em({symbol},{frequency})",
                ak.fund_etf_hist_min_em,
                symbol=code, period=period, start_date=start, end_date=end,
                adjust="",
            )
        elif market == Market.CN:
            raw = akshare_call(
                f"stock_zh_a_hist_min_em({symbol},{frequency})",
                ak.stock_zh_a_hist_min_em,
                symbol=code, period=period, start_date=start, end_date=end,
                adjust="",
            )
        elif market == Market.HK:
            raw = akshare_call(
                f"stock_hk_hist_min_em({symbol},{frequency})",
                ak.stock_hk_hist_min_em,
                symbol=code, period=period, start_date=start, end_date=end,
                adjust="",
            )
        else:
            raise NotImplementedError(f"akshare 不支持该市场分钟线: {symbol}")
        bars = normalize_bars(raw)
        if period == "1" and "open" in bars:
            # 东财近期 1m 历史有时将非最新交易日开盘价置 0；用同日上一根
            # 收盘修复，日内首根则回退到本根收盘，避免伪造 -100% 跳空。
            previous = bars["close"].groupby(bars.index.normalize()).shift(1)
            bars["open"] = bars["open"].where(bars["open"] > 0, previous)
            bars["open"] = bars["open"].fillna(bars["close"])
        return bars.loc[start:end]

    def spot(self, symbols: list[str]) -> pd.DataFrame:  # pragma: no cover - 网络
        ak = _require_akshare()
        raw = akshare_call(
            "stock_zh_a_spot_em", ak.stock_zh_a_spot_em,
            lane="akshare:eastmoney-spot",
        )
        raw = raw.rename(columns={"代码": "code", "名称": "name", "最新价": "price", "涨跌幅": "change_pct"})
        codes = {s.split(".")[0] for s in symbols}
        rows = raw[raw["code"].isin(codes)][["code", "name", "price", "change_pct"]]
        return rows.reset_index(drop=True)

    def index_members(self, index_symbol: str) -> list[str]:  # pragma: no cover - 网络
        """指数成分股。支持 000300.SH(沪深300)、000905.SH(中证500) 等。"""
        ak = _require_akshare()
        code, _ = _split(index_symbol)
        raw = None
        try:
            raw = akshare_call(
                f"index_stock_cons_csindex({index_symbol})",
                ak.index_stock_cons_csindex, symbol=code, lane="akshare:csindex",
            )
        except Exception:
            # 深交所自编指数未必出现在中证目录，继续尝试通用成分接口。
            pass
        if raw is None or raw.empty:
            raw = akshare_call(
                f"index_stock_cons({index_symbol})",
                ak.index_stock_cons, symbol=code, lane="akshare:index-cons",
            )
        member_column = next(
            (column for column in ("成分券代码", "品种代码", "证券代码", "代码")
             if column in raw),
            None,
        )
        if member_column is None:
            raise RuntimeError(f"{index_symbol} 成分响应缺少证券代码列")
        result = []
        seen = set()
        exchange_column = next(
            (column for column in ("交易所", "所属交易所") if column in raw), None,
        )
        if exchange_column is None:
            raise RuntimeError(
                f"{index_symbol} 成分响应缺少交易所；不能按代码首位猜市场"
            )
        exchanges = {
            "上海证券交易所": "SH", "上交所": "SH", "SSE": "SH", "SH": "SH",
            "深圳证券交易所": "SZ", "深交所": "SZ", "SZSE": "SZ", "SZ": "SZ",
            "北京证券交易所": "BJ", "北交所": "BJ", "BSE": "BJ", "BJ": "BJ",
        }
        for _, row in raw[[member_column, exchange_column]].dropna().iterrows():
            c = str(row[member_column]).strip().split(".", 1)[0].zfill(6)
            if not c.isdigit() or len(c) != 6 or c in seen:
                continue
            suffix = exchanges.get(str(row[exchange_column]).strip().upper())
            if suffix is None:
                raise RuntimeError(f"{index_symbol} 成分包含未识别交易所: {row[exchange_column]}")
            seen.add(c)
            result.append(f"{c}.{suffix}")
        if not result:
            raise RuntimeError(f"{index_symbol} 没有可用成分")
        return result


# 常用 A 股指数代码（index_zh_a_hist 接口）
A_SHARE_INDEXES = {
    "000001.SH": "上证综指",
    "000300.SH": "沪深300",
    "000905.SH": "中证500",
    "000852.SH": "中证1000",
    "399001.SZ": "深证成指",
    "399006.SZ": "创业板指",
    "399673.SZ": "创业板50",
    "000688.SH": "科创50",
    "000698.SH": "科创100",
}


def _instrument_type(symbol: str) -> str:
    try:
        from quantmaster.data.instruments import InstrumentStore

        instrument = InstrumentStore().get(symbol)
        return instrument.asset_type if instrument else ""
    except Exception:
        return ""

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
