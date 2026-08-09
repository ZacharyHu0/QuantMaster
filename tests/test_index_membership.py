from __future__ import annotations

import pandas as pd
import pytest

from quantmaster.data.index_membership import (
    CSI800_INDEXES,
    cache_csi800_records,
    load_cached_csi800_members_as_of,
)
from quantmaster.research.lake import ResearchLake


def _small_expected(monkeypatch, count: int) -> None:
    monkeypatch.setattr(
        "quantmaster.data.index_membership.EXPECTED_INDEX_MEMBERS",
        {"000300.SH": count, "000905.SH": count},
    )


class _Source:
    def __init__(self, acquired_at: str = "2026-08-05T06:59:00+00:00") -> None:
        self.calls: list[str] = []
        self.acquired_at = acquired_at

    def index_weights(self, index_code: str, start: str, end: str) -> pd.DataFrame:
        self.calls.append(index_code)
        symbol = "600001.SH" if index_code == "000300.SH" else "000001.SZ"
        return pd.DataFrame([{
            "index_code": index_code,
            "symbol": symbol,
            "trade_date": "2026-07-31",
            "weight": 1.0,
            "acquired_at": self.acquired_at,
            "snapshot_expected_count": 1,
        }])


def test_csi800_membership_is_cached_by_effective_date_and_reused(
    tmp_path, monkeypatch,
) -> None:
    _small_expected(monkeypatch, 1)
    lake = ResearchLake(tmp_path / "lake")
    source = _Source()

    pulled = load_cached_csi800_members_as_of(
        "2026-08-05", source=source, lake=lake,
    )
    reused = load_cached_csi800_members_as_of(
        "2026-08-05", pull=True, source=source, lake=lake,
    )

    assert source.calls == list(CSI800_INDEXES)
    assert pulled["symbols"] == ["000001.SZ", "600001.SH"]
    assert reused == pulled
    assert set(pulled["snapshot_dates"].values()) == {"2026-07-31"}
    assert pulled["requested_as_of"] == "2026-08-05"
    assert pulled["effective_as_of"] == "2026-07-31"
    assert pulled["lag_days"] == {"000300.SH": 5, "000905.SH": 5}
    assert set(pulled["snapshot_acquired_at"].values()) == {
        "2026-08-05T06:59:00+00:00",
    }
    assert next((tmp_path / "lake").rglob("*.parquet")).is_file()


def test_csi800_membership_rejects_stale_snapshot(tmp_path, monkeypatch) -> None:
    _small_expected(monkeypatch, 1)
    lake = ResearchLake(tmp_path / "lake")
    load_cached_csi800_members_as_of("2026-08-05", source=_Source(), lake=lake)

    with pytest.raises(RuntimeError, match="过旧"):
        load_cached_csi800_members_as_of(
            "2026-12-15", pull=False, lake=lake, max_snapshot_age_days=45,
        )


def test_csi800_membership_rejects_evidence_acquired_after_signal_cutoff(
    tmp_path, monkeypatch,
) -> None:
    _small_expected(monkeypatch, 1)
    lake = ResearchLake(tmp_path / "lake")
    with pytest.raises(RuntimeError, match="上海 15:00"):
        load_cached_csi800_members_as_of(
            "2026-08-05",
            source=_Source("2026-08-05T07:01:00+00:00"),
            lake=lake,
        )


def test_partial_later_acquisition_cannot_replace_complete_snapshot(
    tmp_path, monkeypatch,
) -> None:
    _small_expected(monkeypatch, 10)
    lake = ResearchLake(tmp_path / "lake")
    complete = pd.DataFrame([
        {
            "index_code": index_code,
            "symbol": f"{offset:06d}.{'SH' if index_code == '000300.SH' else 'SZ'}",
            "trade_date": "2026-07-31",
            "weight": 1.0,
            "acquired_at": "2026-08-05T05:00:00+00:00",
            "snapshot_expected_count": 10,
        }
        for index_code in CSI800_INDEXES
        for offset in range(10)
    ])
    assert cache_csi800_records(complete, lake=lake) == 20
    partial = complete.iloc[[0]].copy()
    partial["acquired_at"] = "2026-08-05T06:00:00+00:00"
    assert cache_csi800_records(partial, lake=lake) == 0

    result = load_cached_csi800_members_as_of("2026-08-05", pull=False, lake=lake)
    assert len(result["symbols"]) == 20
    assert result["snapshot_acquired_at"]["000300.SH"] == "2026-08-05T05:00:00+00:00"


def test_known_index_cannot_self_declare_one_member_as_complete(tmp_path) -> None:
    lake = ResearchLake(tmp_path / "lake")
    forged = pd.DataFrame([{
        "index_code": "000300.SH",
        "symbol": "600000.SH",
        "trade_date": "2026-07-31",
        "weight": 100.0,
        "acquired_at": "2026-08-05T05:00:00+00:00",
        "snapshot_expected_count": 1,
    }])

    assert cache_csi800_records(forged, lake=lake) == 0


def test_csi800_requires_disjoint_complete_subindexes(tmp_path, monkeypatch) -> None:
    _small_expected(monkeypatch, 2)
    lake = ResearchLake(tmp_path / "lake")
    overlapping = pd.DataFrame([
        {
            "index_code": index_code,
            "symbol": symbol,
            "trade_date": "2026-07-31",
            "weight": 50.0,
            "acquired_at": "2026-08-05T05:00:00+00:00",
            "snapshot_expected_count": 2,
        }
        for index_code in CSI800_INDEXES
        for symbol in ("600000.SH", "600001.SH")
    ])
    assert cache_csi800_records(overlapping, lake=lake) == 4

    with pytest.raises(RuntimeError, match="重叠标的"):
        load_cached_csi800_members_as_of(
            "2026-08-05",
            pull=False,
            lake=lake,
        )
