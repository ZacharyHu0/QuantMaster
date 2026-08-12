"""行业中性化测试（合成映射，离线）。"""

import json
from datetime import date

import numpy as np
import pandas as pd
import pytest

from quantmaster.data.industry import (
    load_cached_industry_map,
    load_industry_analysis_context,
    load_industry_evidence,
    load_industry_map,
    save_industry_map,
)
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
_TRUSTED_UNIVERSES = {}


def _trust_industry_universe(monkeypatch, symbols):
    from quantmaster.data import industry as industry_module
    from quantmaster.data import instrument_snapshots

    fixture_date = date(2026, 8, 9)
    expected = set(symbols)
    snapshot_id = "fixture:" + ",".join(sorted(expected))
    _TRUSTED_UNIVERSES[snapshot_id] = expected
    evidence = {
        "snapshot_id": snapshot_id,
        "snapshot_sha256": snapshot_id,
        "expected_count": len(expected),
        "as_of": "2026-08-09",
        "acquired_at": "2026-08-09T07:00:00+00:00",
        "source": "tushare:catalog",
    }
    monkeypatch.setattr(
        industry_module,
        "_active_cn_universe",
        lambda **_kwargs: (expected, evidence),
    )
    monkeypatch.setattr(industry_module, "market_date", lambda: fixture_date)
    monkeypatch.setattr(
        instrument_snapshots,
        "verify_instrument_catalog_evidence",
        lambda value, **_kwargs: (None, _TRUSTED_UNIVERSES[value["snapshot_id"]]),
    )
    return evidence


class TestIndustryNeutralize:
    def test_within_industry_mean_zero(self):
        values = _values(list(MAPPING))
        result = industry_neutralize(values, MAPPING)
        banks = ["600000.SH", "600016.SH", "601398.SH"]
        assert result[banks].mean(axis=1).abs().max() < 1e-12
        batteries = ["300750.SZ", "002594.SZ"]
        assert result[batteries].mean(axis=1).abs().max() < 1e-12

class TestIndustryMapCache:
    def test_save_and_load_roundtrip(self, monkeypatch):
        from quantmaster.data import industry as mod

        _trust_industry_universe(monkeypatch, MAPPING)
        monkeypatch.setattr(mod, "_active_cn_symbols", lambda: set(MAPPING))
        save_industry_map(MAPPING)
        assert load_industry_map() == MAPPING

    def test_partial_refresh_merges_without_deleting_old_blocks(self, monkeypatch):
        """刷新只取得一个板块时，旧缓存的其他完整板块仍然可用。"""
        from quantmaster.data import industry as mod

        _trust_industry_universe(monkeypatch, MAPPING)
        save_industry_map(MAPPING)
        partial = {"600000.SH": "银行（新口径）", "000001.SZ": "银行（新口径）"}
        monkeypatch.setattr(mod, "fetch_industry_map", lambda: partial)
        _trust_industry_universe(monkeypatch, set(MAPPING) | {"000001.SZ"})
        monkeypatch.setattr(mod, "_active_cn_symbols", lambda: set(MAPPING) | {"000001.SZ"})

        with pytest.raises(RuntimeError, match="degraded"):
            load_industry_map(refresh=True)
        assert load_cached_industry_map() == MAPPING
        with pytest.raises(RuntimeError, match="degraded"):
            load_industry_map()
        history = list((mod._history_root()).glob("*.json"))
        assert any("degraded_merged_partial" in path.read_text(encoding="utf-8") for path in history)

    def test_complete_refresh_can_remove_obsolete_members(self, monkeypatch):
        from quantmaster.data import industry as mod

        _trust_industry_universe(monkeypatch, {"600000.SH", "000001.SZ"})
        save_industry_map({"600000.SH": "银行", "000001.SZ": "旧分类"})
        monkeypatch.setattr(
            mod, "fetch_industry_map",
            lambda: ({"600000.SH": "银行（完整新口径）"}, True, "test:complete"),
        )
        _trust_industry_universe(monkeypatch, {"600000.SH"})
        monkeypatch.setattr(mod, "_active_cn_symbols", lambda: {"600000.SH"})

        result = load_industry_map(refresh=True)
        assert result == {"600000.SH": "银行（完整新口径）"}

    def test_stale_cache_is_not_returned_when_refresh_fails(self, monkeypatch):
        from quantmaster.data import industry as mod

        _trust_industry_universe(monkeypatch, MAPPING)
        save_industry_map(MAPPING, observed_at="2026-08-09T07:00:00+00:00")
        monkeypatch.setattr(mod.time, "time", lambda: 1_800_000_000.0)

        def fail_fetch():
            raise OSError("provider offline")

        monkeypatch.setattr(mod, "fetch_industry_map", fail_fetch)
        with pytest.raises(RuntimeError, match="拒绝返回"):
            load_industry_map()

        mapping, evidence = load_industry_analysis_context()
        assert mapping == MAPPING
        assert evidence["status"] == "degraded"
        assert evidence["formal_eligible"] is False
        assert evidence["preview_symbols"] == len(MAPPING)
        assert evidence["issues"][0] == (
            "正式行业证据不可用；结果已降级为研究预览，详细信息已写入本机日志"
        )

    def test_analysis_context_keeps_loader_exception_out_of_public_evidence(
        self, monkeypatch, caplog,
    ):
        from quantmaster.data import industry as mod

        secret = "token=industry-secret"

        def fail_strict_load(**_kwargs):
            raise RuntimeError(secret)

        monkeypatch.setattr(mod, "load_industry_map", fail_strict_load)
        monkeypatch.setattr(mod, "load_cached_industry_map", lambda **_kwargs: {})
        monkeypatch.setattr(
            mod,
            "load_industry_evidence",
            lambda **_kwargs: (_ for _ in ()).throw(FileNotFoundError()),
        )

        with caplog.at_level("ERROR", logger="quantmaster.data.industry"):
            mapping, evidence = load_industry_analysis_context()

        assert mapping == {}
        assert evidence["issues"] == [
            "正式行业证据不可用；结果已降级为研究预览，详细信息已写入本机日志",
        ]
        assert secret not in str(evidence)
        assert secret in caplog.text

    def test_historical_lookup_never_backdates_current_mapping(self, monkeypatch):
        _trust_industry_universe(monkeypatch, MAPPING)
        save_industry_map(
            MAPPING,
            effective_as_of="2026-08-09",
            observed_at="2026-08-09T07:00:00+00:00",
            expected_symbols=len(MAPPING),
        )
        assert load_industry_map(as_of="2026-08-09") == MAPPING
        with pytest.raises(RuntimeError, match="拒绝用当前行业映射"):
            load_industry_map(as_of="2026-08-08")

    def test_unmigrated_current_mapping_is_not_loaded(
        self, isolated_config, monkeypatch,
    ):
        from quantmaster.data import industry as mod
        from quantmaster.data.industry import LegacyIndustrySnapshotError

        (isolated_config.data_root / "industry_map.json").write_text(
            '{"updated_at": 1, "mapping": {"600519.SH": "食品饮料"}}',
            encoding="utf-8",
        )
        monkeypatch.setattr(
            mod, "fetch_industry_map", lambda: (_ for _ in ()).throw(OSError("offline")),
        )

        with pytest.raises(LegacyIndustrySnapshotError, match="旧格式"):
            load_cached_industry_map()
        mapping, evidence = load_industry_analysis_context()
        historical, historical_evidence = load_industry_analysis_context(
            as_of="2026-08-08",
        )

        assert mapping == {}
        assert evidence["source"] == "unavailable"
        assert evidence["formal_eligible"] is False
        assert historical == {}
        assert historical_evidence["formal_eligible"] is False

    def test_explicit_migration_is_current_only_and_has_no_content_hash(
        self, isolated_config, monkeypatch,
    ):
        from quantmaster.data import industry as mod
        from quantmaster.data.legacy_migrations import migrate_industry_current_projection

        projection = isolated_config.data_root / "industry_map.json"
        projection.write_text(
            '{"updated_at": 1785119503.0, "mapping": {'
            '"600519.SH": "食品饮料", "000001.sz": "银行"}}',
            encoding="utf-8",
        )
        result = migrate_industry_current_projection(isolated_config.data_root)
        migrated = json.loads(projection.read_text(encoding="utf-8"))

        assert result[0]["outcome"] == "converted"
        assert "content_sha256" not in migrated
        assert migrated["projection"] == "current_only"
        assert migrated["updated_at"] == 1785119503.0
        assert load_industry_map() == {
            "600519.SH": "食品饮料", "000001.SZ": "银行",
        }
        evidence = load_industry_evidence()
        assert evidence["status"] == "degraded"
        assert evidence["content_hash"] == ""
        mapping, context = load_industry_analysis_context()
        assert mapping == load_industry_map()
        assert context["formal_eligible"] is False
        with pytest.raises(RuntimeError, match="拒绝用当前行业映射"):
            load_industry_map(as_of="2026-07-27")
        assert not list(mod._history_root().glob("*.json"))

    def test_industry_history_tamper_is_rejected(self, monkeypatch, isolated_config):
        from quantmaster.data.industry import IndustrySnapshotIntegrityError

        _trust_industry_universe(monkeypatch, MAPPING)
        save_industry_map(
            MAPPING,
            effective_as_of="2026-08-09",
            observed_at="2026-08-09T07:00:00+00:00",
            expected_symbols=len(MAPPING),
        )
        snapshot = next(
            (isolated_config.data_root / "industry_map_history").glob("*.json")
        )
        snapshot.write_text(
            snapshot.read_text(encoding="utf-8").replace("银行", "伪造行业", 1),
            encoding="utf-8",
        )
        with pytest.raises(IndustrySnapshotIntegrityError, match="内容哈希"):
            load_industry_map(as_of="2026-08-09")


def test_industry_manifest_freezes_shared_catalog_evidence(monkeypatch):
    from quantmaster.data import instrument_snapshots
    from quantmaster.data.instrument_snapshots import (
        TUSHARE_CATALOG_QUERY,
        freeze_instrument_catalog,
        load_instrument_catalog_snapshot,
    )
    from tests.catalog_evidence_helpers import bound_tushare_catalog

    monkeypatch.setattr(
        instrument_snapshots, "TUSHARE_MINIMUM_ASSET_COUNTS", {"CN:stock": 2},
    )
    records = [{
        "symbol": symbol,
        "market": "CN",
        "asset_type": "stock",
        "list_date": "20200101",
        "delist_date": "",
        "status": "L",
    } for symbol in ("600000.SH", "000001.SZ")]
    catalog_records, catalog_outcomes = bound_tushare_catalog(records)
    snapshot = freeze_instrument_catalog(
        catalog_records,
        source="tushare:catalog",
        query=TUSHARE_CATALOG_QUERY,
        request_outcomes=catalog_outcomes,
        acquired_at="2026-08-09T07:00:00+00:00",
    )
    _loaded, symbols, evidence = load_instrument_catalog_snapshot(
        as_of="2026-08-09", market="CN", asset_type="stock",
    )
    mapping = {symbol: "银行" for symbol in symbols}
    save_industry_map(
        mapping,
        effective_as_of="2026-08-09",
        observed_at="2026-08-09T07:00:00+00:00",
        expected_symbols=2,
        universe_evidence=evidence,
    )
    industry_evidence = load_industry_evidence(as_of="2026-08-09")
    assert industry_evidence["universe_evidence"]["snapshot_id"] == snapshot.snapshot_id
    assert industry_evidence["expected_symbols"] == 2


def test_yesterday_catalog_cannot_be_relabelled_as_today_industry_universe(monkeypatch):
    from quantmaster.data import instrument_snapshots
    from quantmaster.data.industry import IndustrySnapshotIncomplete, _active_cn_universe
    from quantmaster.data.instrument_snapshots import (
        TUSHARE_CATALOG_QUERY,
        freeze_instrument_catalog,
    )
    from tests.catalog_evidence_helpers import bound_tushare_catalog

    monkeypatch.setattr(
        instrument_snapshots, "TUSHARE_MINIMUM_ASSET_COUNTS", {"CN:stock": 1},
    )
    records, outcomes = bound_tushare_catalog([{
        "symbol": "600000.SH", "market": "CN", "asset_type": "stock",
        "status": "L", "list_date": "2020-01-01", "delist_date": "",
    }])
    freeze_instrument_catalog(
        records,
        source="tushare:catalog",
        query=TUSHARE_CATALOG_QUERY,
        request_outcomes=outcomes,
        acquired_at="2026-08-08T07:01:00+00:00",
    )
    monkeypatch.setattr("quantmaster.data.industry.market_date", lambda: date(2026, 8, 9))

    with pytest.raises(IndustrySnapshotIncomplete, match="不可变证券目录不可用"):
        _active_cn_universe()


def test_industry_rejects_wrong_day_or_catalog_acquired_after_observation(monkeypatch):
    evidence = _trust_industry_universe(monkeypatch, MAPPING)
    wrong_day = {**evidence, "as_of": "2026-08-08"}
    with pytest.raises(RuntimeError, match="as_of 不一致"):
        save_industry_map(
            MAPPING,
            effective_as_of="2026-08-09",
            observed_at="2026-08-09T07:00:00+00:00",
            universe_evidence=wrong_day,
        )
    with pytest.raises(RuntimeError, match="早于证券目录"):
        save_industry_map(
            MAPPING,
            effective_as_of="2026-08-09",
            observed_at="2026-08-09T06:59:00+00:00",
            universe_evidence=evidence,
        )
