from __future__ import annotations

from collections import defaultdict
from typing import Any

from quantmaster.data.instrument_snapshots import (
    TUSHARE_CATALOG_REQUESTS,
    tushare_catalog_partition_evidence,
    tushare_catalog_request_params,
)


def bound_tushare_catalog(records: list[dict[str, Any]]) -> tuple[list[dict], list[dict]]:
    """Turn concise test rows into the same replayable evidence emitted in production."""
    partitions: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    index_markets = iter(("CSI", "SSE", "SZSE"))
    for item in records:
        row = dict(item)
        symbol = str(row["symbol"]).upper()
        market = str(row.get("market") or "CN").upper()
        asset_type = str(row.get("asset_type") or "stock").lower()
        raw_status = str(row.get("status") or "L")
        status = {
            "listed": "L", "active": "L", "delisted": "D",
            "terminated": "D", "paused": "P",
        }.get(raw_status.lower(), raw_status.upper())
        listed = str(row.get("list_date") or "20200101").replace("-", "")
        delisted = str(row.get("delist_date") or "").replace("-", "")
        name = str(row.get("name") or symbol)
        if market == "HK":
            key = ("hk_basic", "list_status", status)
            code = symbol.partition(".")[0]
            partitions[key].append({
                "ts_code": str(row.get("provider_symbol") or symbol), "symbol": code,
                "name": name, "fullname": str(row.get("full_name") or ""),
                "enname": str(row.get("en_name") or ""), "list_status": status,
                "list_date": listed, "delist_date": delisted,
            })
        elif asset_type == "stock":
            key = ("stock_basic", "list_status", status)
            partitions[key].append({
                "ts_code": symbol, "symbol": symbol.partition(".")[0], "name": name,
                "fullname": str(row.get("full_name") or ""),
                "enname": str(row.get("en_name") or ""),
                "exchange": str(row.get("exchange") or symbol.rsplit(".", 1)[-1]),
                "curr_type": str(row.get("currency") or "CNY"), "list_status": status,
                "list_date": listed, "delist_date": delisted,
            })
        elif asset_type in {"etf", "fund"}:
            if status not in {"L", "D"}:
                raise ValueError("fund_basic test evidence only permits L/D")
            key = ("fund_basic", "status", status)
            partitions[key].append({
                "ts_code": symbol, "name": name,
                "fund_type": "ETF" if asset_type == "etf" else "混合型",
                "status": status, "list_date": listed, "delist_date": delisted,
            })
        elif asset_type == "index":
            partition = str(row.get("catalog_market") or next(index_markets))
            key = ("index_basic", "market", partition)
            partitions[key].append({
                "ts_code": symbol, "name": name,
                "fullname": str(row.get("full_name") or ""), "market": partition,
            })

    placeholders = {
        ("stock_basic", "list_status", "L"): {
            "ts_code": "430000.BJ", "symbol": "430000", "name": "占位股票",
            "fullname": "", "enname": "", "exchange": "BSE", "curr_type": "CNY",
            "list_status": "L", "list_date": "20200101", "delist_date": "",
        },
        ("stock_basic", "list_status", "D"): {
            "ts_code": "430001.BJ", "symbol": "430001", "name": "历史退市股票",
            "fullname": "", "enname": "", "exchange": "BSE", "curr_type": "CNY",
            "list_status": "D", "list_date": "20100101", "delist_date": "20200101",
        },
        ("stock_basic", "list_status", "P"): {
            "ts_code": "430002.BJ", "symbol": "430002", "name": "暂停上市股票",
            "fullname": "", "enname": "", "exchange": "BSE", "curr_type": "CNY",
            "list_status": "P", "list_date": "20100101", "delist_date": "",
        },
        ("fund_basic", "status", "L"): {
            "ts_code": "588999.SH", "name": "占位ETF", "fund_type": "ETF",
            "status": "L", "list_date": "20200101", "delist_date": "",
        },
        ("fund_basic", "status", "D"): {
            "ts_code": "159999.SZ", "name": "历史退市ETF", "fund_type": "ETF",
            "status": "D", "list_date": "20100101", "delist_date": "20200101",
        },
    }
    for index, market in enumerate(("CSI", "SSE", "SZSE"), start=1):
        placeholders[("index_basic", "market", market)] = {
            "ts_code": f"{index:06d}.{market}", "name": f"{market}指数",
            "fullname": "", "market": market,
        }
    for index, status in enumerate(("L", "D", "P"), start=1):
        placeholders[("hk_basic", "list_status", status)] = {
            "ts_code": f"0000{index}.HK", "symbol": f"0000{index}",
            "name": f"港股{status}", "fullname": "", "enname": "",
            "list_status": status, "list_date": "20100101",
            "delist_date": "20200101" if status == "D" else "",
        }
    normalized: list[dict] = []
    outcomes: list[dict] = []
    for endpoint, key, value in TUSHARE_CATALOG_REQUESTS:
        identity = (endpoint, key, value)
        raw = partitions[identity] or [placeholders[identity]]
        partition, evidence = tushare_catalog_partition_evidence(
            endpoint, key, value,
            params=tushare_catalog_request_params(endpoint, key, value),
            raw_records=raw, raw_columns=raw[0],
        )
        normalized.extend(partition)
        outcomes.append(evidence)
    return normalized, outcomes
