"""Static identities for non-CN reference instruments."""

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
