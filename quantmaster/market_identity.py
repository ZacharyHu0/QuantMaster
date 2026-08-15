"""Neutral market identity primitives shared across product boundaries."""

from __future__ import annotations

import enum


class Market(enum.StrEnum):
    CN = "cn"          # A 股
    HK = "hk"          # 港股
    US = "us"          # 美股
    JP = "jp"          # 日本
    KR = "kr"          # 韩国
    FUTURES = "fut"    # 商品期货/期指
    INDEX = "idx"      # 指数
    FOREX = "fx"       # 外汇货币对


def guess_market(symbol: str) -> Market:
    """Classify a fully-qualified local symbol; never guess a bare code."""
    suffix = symbol.rsplit(".", 1)[-1].upper() if "." in symbol else ""
    market = {
        "CSI": Market.INDEX, "INDEX": Market.INDEX,
        "SH": Market.CN, "SZ": Market.CN, "BJ": Market.CN,
        "HK": Market.HK, "US": Market.US, "JP": Market.JP, "KR": Market.KR,
        "FX": Market.FOREX,
        "CONTINUOUS": Market.FUTURES, "SHF": Market.FUTURES,
        "INE": Market.FUTURES, "DCE": Market.FUTURES, "CZC": Market.FUTURES,
        "CFX": Market.FUTURES, "CFFEX": Market.FUTURES,
    }.get(suffix)
    if market is None:
        raise ValueError(f"标的缺少已确认市场身份: {symbol}")
    return market


__all__ = ["Market", "guess_market"]
