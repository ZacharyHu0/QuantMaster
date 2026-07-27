"""行业中性化测试（合成映射，离线）。"""

import json

import numpy as np
import pandas as pd
import pytest

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

    def test_single_member_industry_unchanged(self):
        """单成员行业不调整（减自身均值会抹成 0，损失信息）。"""
        values = _values(list(MAPPING))
        result = industry_neutralize(values, MAPPING)
        pd.testing.assert_series_equal(result["600519.SH"], values["600519.SH"])

    def test_unmapped_symbol_unchanged(self):
        symbols = [*MAPPING, "999999.SZ"]
        values = _values(symbols)
        result = industry_neutralize(values, MAPPING)
        pd.testing.assert_series_equal(result["999999.SZ"], values["999999.SZ"])

    def test_empty_mapping_noop(self):
        values = _values(["600000.SH", "300750.SZ"])
        result = industry_neutralize(values, {})
        pd.testing.assert_frame_equal(result, values)

    def test_nan_members_excluded_from_mean(self):
        """某成员当日缺值：行业均值按其余成员算，缺值处保持 NaN。"""
        values = _values(list(MAPPING))
        values.iloc[0, values.columns.get_loc("600000.SH")] = np.nan
        result = industry_neutralize(values, MAPPING)
        others = ["600016.SH", "601398.SH"]
        assert result.iloc[0][others].mean() == pytest.approx(0.0, abs=1e-12)
        assert pd.isna(result.iloc[0]["600000.SH"])


class TestIndustryMapCache:
    def test_save_and_load_roundtrip(self):
        save_industry_map(MAPPING)
        assert load_industry_map() == MAPPING

    def test_corrupt_cache_returns_empty(self, isolated_config, monkeypatch):
        from quantmaster.data import industry as mod

        path = mod._cache_path()
        path.write_text("not json{{{", encoding="utf-8")
        monkeypatch.setattr(mod, "fetch_industry_map",
                            lambda: (_ for _ in ()).throw(RuntimeError("offline")))
        assert load_industry_map() == {}

    def test_stale_cache_fallback_when_fetch_fails(self, monkeypatch):
        from quantmaster.data import industry as mod

        save_industry_map(MAPPING)
        # 手动把缓存改旧
        path = mod._cache_path()
        data = json.loads(path.read_text(encoding="utf-8"))
        data["updated_at"] = 0
        path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        monkeypatch.setattr(mod, "fetch_industry_map",
                            lambda: (_ for _ in ()).throw(RuntimeError("offline")))
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
