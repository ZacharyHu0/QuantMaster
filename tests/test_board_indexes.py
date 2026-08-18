from __future__ import annotations

import pandas as pd

from quantmaster.rotation.board_indexes import (
    BOARD_INDEX_MEMBERSHIP,
    BOARD_INDEX_METHODS,
    build_board_index_data,
)


class _StockDB:
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[str, ...], int]] = []

    def boards(self):
        return [
            {
                "code": "801010.SI", "name": "农林牧渔", "category": "申万一级",
                "symbols": ["000001.SZ", "600000.SH"],
            },
            {
                "code": "801011.SI", "name": "林业", "category": "申万二级",
                "symbols": ["000001.SZ"],
            },
            {
                "code": "300001.TI", "name": "测试题材", "category": "概念",
                "symbols": ["600000.SH"],
            },
            {
                "code": "300002.TI", "name": "未入选题材", "category": "概念",
                "symbols": ["600000.SH"],
            },
        ]

    def native_board_index(self, symbols, _start, _end, *, method, base):
        assert base == 1000
        self.calls.append((tuple(symbols), method))
        return [
            {
                "date": 20260810 + offset,
                "open": 1000 + offset,
                "high": 1002 + offset,
                "low": 999 + offset,
                "close": 1000 + offset + method,
                "pct_chg": 0.1,
                "volume": 100,
                "amount": 200,
                "stock_count": len(symbols),
            }
            for offset in range(6)
        ]


def test_board_index_snapshot_uses_current_membership_and_all_five_methods():
    source = _StockDB()
    dates = pd.date_range("2026-08-10", periods=3, freq="B")
    close = pd.DataFrame(
        {"000001.SZ": [10.0, 10.5, 11.0], "600000.SH": [8.0, 8.1, 8.2]},
        index=dates,
    )
    amount = pd.DataFrame(
        {"000001.SZ": [1, 2, 3], "600000.SH": [4, 5, 6]}, index=dates,
    )
    progress = []

    data, quality = build_board_index_data(
        source,
        close=close,
        amount=amount,
        names={"000001.SZ": "平安银行", "600000.SH": "浦发银行"},
        as_of="2026-08-17",
        selected_l2={"801011.SI"},
        theme_codes={"300001.TI"},
        progress=lambda *value: progress.append(value),
        cancelled=lambda: False,
    )

    assert [item["category"] for item in data["items"]] == ["sw1", "sw2", "theme"]
    assert len(source.calls) == 3 * len(BOARD_INDEX_METHODS)
    assert quality["status"] == "complete"
    detail = data["details"]["SW1:801010.SI"]
    assert detail["membership_semantics"] == BOARD_INDEX_MEMBERSHIP
    assert set(detail["series"]) == set(BOARD_INDEX_METHODS)
    assert detail["constituents"][0]["name"] == "平安银行"
    assert data["definition"]["frequency"] == "1d"
    assert progress[-1][0] == 95


def test_board_index_snapshot_marks_one_method_unavailable_without_fabricating_data():
    source = _StockDB()
    original = source.native_board_index

    def calculate(symbols, start, end, *, method, base):
        if method == 2:
            raise RuntimeError("缺少流通市值")
        return original(symbols, start, end, method=method, base=base)

    source.native_board_index = calculate
    dates = pd.date_range("2026-08-10", periods=2, freq="B")
    close = pd.DataFrame({"000001.SZ": [10.0, 10.5]}, index=dates)
    amount = pd.DataFrame({"000001.SZ": [1, 2]}, index=dates)

    data, quality = build_board_index_data(
        source,
        close=close,
        amount=amount,
        names={},
        as_of="2026-08-17",
        selected_l2=set(),
        theme_codes=set(),
        progress=lambda *_args: None,
        cancelled=lambda: False,
    )

    method = data["details"]["SW1:801010.SI"]["methods"]["float_mv"]
    assert method == {
        "status": "unavailable",
        "last": None,
        "changes": {"1": None, "3": None, "5": None, "20": None},
        "sessions": 0,
        "reason": "缺少流通市值",
    }
    assert quality["status"] == "partial"
