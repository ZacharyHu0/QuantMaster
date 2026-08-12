from __future__ import annotations

import json
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from quantmaster.data.universe import (
    index_universe,
    list_universes,
    load_universe,
    load_universe_analysis_snapshot,
    load_universe_snapshot,
    save_universe,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")


def test_index_universe_uses_complete_local_snapshot_before_remote(
    isolated_config, monkeypatch,
):
    import pandas as pd

    records = pd.DataFrame({
        "index_code": ["000300.SH"] * 300,
        "symbol": [f"{600000 + value:06d}.SH" for value in range(300)],
        "trade_date": [pd.Timestamp("2026-08-01")] * 300,
        "acquired_at": [pd.Timestamp("2026-08-01", tz="UTC")] * 300,
    })
    monkeypatch.setattr(
        "quantmaster.data.index_membership.load_cached_csi800_records",
        lambda *_args, **_kwargs: records,
    )
    monkeypatch.setattr(
        "quantmaster.data.tushare_source.TushareSource.index_weights",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("不应联网")),
    )
    monkeypatch.setattr(
        "quantmaster.data.akshare_source.AkshareSource.index_members",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("不应联网")),
    )

    assert len(index_universe("000300.SH")) == 300


def test_custom_universe_history_is_point_in_time_and_content_hashed(isolated_config):
    save_universe(
        "重点候选",
        ["600000.SH"],
        observed_at=datetime(2026, 7, 1, 14, 0, tzinfo=SHANGHAI),
    )
    first = load_universe_snapshot("重点候选", as_of="2026-07-01")
    save_universe(
        "重点候选",
        ["000001.SZ"],
        observed_at=datetime(2026, 8, 1, 14, 0, tzinfo=SHANGHAI),
    )

    assert load_universe("重点候选") == ["000001.SZ"]
    assert load_universe("重点候选", as_of="2026-07-01") == ["600000.SH"]
    assert first.content_hash == load_universe_snapshot(
        "重点候选", as_of="2026-07-01",
    ).content_hash
    with pytest.raises(RuntimeError, match="没有可验证快照"):
        load_universe("重点候选", as_of="2026-06-30")


def test_legacy_universe_list_is_not_silently_reinterpreted(isolated_config):
    root = isolated_config.data_root / "universe"
    root.mkdir(parents=True)
    (root / "legacy.json").write_text(
        json.dumps(["600000.SH"]), encoding="utf-8",
    )

    with pytest.raises(ValueError, match="v2"):
        load_universe("legacy")

    preview = load_universe_analysis_snapshot("legacy")
    assert preview.symbols == ("600000.SH",)
    assert preview.source == "legacy-custom-preview"
    assert preview.formal_eligible is False
    assert preview.to_dict()["formal_eligible"] is False
    assert any(item["name"] == "legacy" for item in list_universes())
    with pytest.raises(RuntimeError, match="没有可验证快照"):
        load_universe_analysis_snapshot("legacy", as_of="2026-08-09")


def test_custom_universe_rejects_tampered_observation_time(isolated_config):
    save_universe(
        "时间证据",
        ["600000.SH"],
        observed_at=datetime(2026, 8, 2, 14, 0, tzinfo=SHANGHAI),
    )
    history = next(
        (isolated_config.data_root / "universe" / "history" / "时间证据").glob("*.json")
    )
    payload = json.loads(history.read_text(encoding="utf-8"))
    payload["observed_at"] = "2026-07-01T06:00:00+00:00"
    payload["effective_as_of"] = "2026-07-01"
    history.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    with pytest.raises(RuntimeError, match="历史证据损坏"):
        load_universe("时间证据", as_of="2026-07-01")


def test_same_universe_observation_identity_is_immutable(isolated_config):
    observed = datetime(2026, 8, 2, 14, 0, tzinfo=SHANGHAI)
    save_universe("不可改写", ["600000.SH"], observed_at=observed)

    with pytest.raises(RuntimeError, match="同一 observed_at"):
        save_universe("不可改写", ["000001.SZ"], observed_at=observed)

    assert load_universe("不可改写") == ["600000.SH"]
