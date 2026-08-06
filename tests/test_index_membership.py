from __future__ import annotations

import pandas as pd

from quantmaster.data.index_membership import (
    CSI800_INDEXES,
    load_cached_csi800_members_as_of,
)
from quantmaster.research.lake import ResearchLake


class _Source:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def index_weights(self, index_code: str, start: str, end: str) -> pd.DataFrame:
        self.calls.append(index_code)
        symbol = "600001.SH" if index_code == "000300.SH" else "000001.SZ"
        return pd.DataFrame([{
            "index_code": index_code,
            "symbol": symbol,
            "trade_date": "2026-07-31",
            "weight": 1.0,
        }])


def test_csi800_membership_is_cached_by_effective_date_and_reused(tmp_path) -> None:
    lake = ResearchLake(tmp_path / "lake")
    source = _Source()

    pulled = load_cached_csi800_members_as_of(
        "2026-08-05", source=source, lake=lake,
    )
    reused = load_cached_csi800_members_as_of(
        "2026-08-05", pull=False, lake=lake,
    )

    assert source.calls == list(CSI800_INDEXES)
    assert pulled["symbols"] == ["000001.SZ", "600001.SH"]
    assert reused == pulled
    assert set(pulled["snapshot_dates"].values()) == {"2026-07-31"}
    assert next((tmp_path / "lake").rglob("*.parquet")).is_file()
