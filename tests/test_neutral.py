"""行业中性化测试（合成映射，离线）。"""

import numpy as np
import pandas as pd

from quantmaster.data.industry import load_industry_map, save_industry_map
from quantmaster.factors.neutral import industry_neutralize


def _values(symbols, days=10, seed=5):
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2024-01-02", periods=days)
    return pd.DataFrame(rng.normal(0, 1, (days, len(symbols))),
                        index=dates, columns=symbols)


MAPPING = {
    "600000.SH": "银行", "600016.SH": "银行", "601398.SH": "银行",
    "300750.SZ": "电池", "002594.SZ": "电池",
    "600519.SH": "白酒",   # 单成员行业
}


class TestIndustryNeutralize:
    def test_within_industry_mean_zero(self):
        values = _values(list(MAPPING))
        result = industry_neutralize(values, MAPPING)
        banks = ["600000.SH", "600016.SH", "601398.SH"]
        assert result[banks].mean(axis=1).abs().max() < 1e-12
        batteries = ["300750.SZ", "002594.SZ"]
        assert result[batteries].mean(axis=1).abs().max() < 1e-12

class TestIndustryMapCache:
    def test_save_and_load_roundtrip(self):
        save_industry_map(MAPPING)
        assert load_industry_map() == MAPPING

    def test_partial_refresh_merges_without_deleting_old_blocks(self, monkeypatch):
        """刷新只取得一个板块时，旧缓存的其他完整板块仍然可用。"""
        from quantmaster.data import industry as mod

        save_industry_map(MAPPING)
        partial = {"600000.SH": "银行（新口径）", "000001.SZ": "银行（新口径）"}
        monkeypatch.setattr(mod, "fetch_industry_map", lambda: partial)

        result = load_industry_map(refresh=True)
        assert result["600000.SH"] == "银行（新口径）"
        assert result["000001.SZ"] == "银行（新口径）"
        assert result["300750.SZ"] == "电池"
        assert result["600519.SH"] == "白酒"
