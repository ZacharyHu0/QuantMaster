"""Tushare 数据源（可选，需注册 token，基础额度免费）。

Tushare Pro 的日线/财务数据质量较好，注册后在 config.yaml 或环境变量
TUSHARE_TOKEN 中配置 token 即可启用。
"""

from __future__ import annotations

import pandas as pd

from quantmaster.config import get_config
from quantmaster.data.base import DataSource, Market, normalize_daily


def _require_tushare():
    try:
        import tushare as ts  # noqa: PLC0415
    except ImportError as e:  # pragma: no cover
        raise ImportError("未安装 tushare。请执行: pip install tushare") from e
    token = get_config().data.tushare_token
    if not token:
        raise RuntimeError("未配置 TUSHARE_TOKEN（config.yaml 的 data.tushare_token 或环境变量）")
    ts.set_token(token)
    return ts.pro_api()


class TushareSource(DataSource):
    name = "tushare"
    markets = (Market.CN, Market.INDEX)

    def daily(self, symbol: str, start: str, end: str) -> pd.DataFrame:
        pro = _require_tushare()
        raw = pro.daily(
            ts_code=symbol,
            start_date=start.replace("-", ""),
            end_date=end.replace("-", ""),
        )
        if raw is None or raw.empty:
            return pd.DataFrame()
        raw = raw.rename(columns={"trade_date": "date", "vol": "volume"})
        return normalize_daily(raw)
