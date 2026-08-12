"""生成随安装包发布的证券主数据快照。

这是维护工具，不在应用启动时运行。主数据来自 Tushare 的内地、香港目录和
Nasdaq Trader 官方美国上市目录；输出是可重复导入的 gzip JSON。
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from quantmaster.config import get_config  # noqa: E402
from quantmaster.data.akshare_source import FUTURES_MAIN  # noqa: E402
from quantmaster.data.tushare_source import _require_tushare  # noqa: E402
from quantmaster.data.universe import DEMO_STOCK_NAMES  # noqa: E402
from quantmaster.data.yfinance_source import GLOBAL_REFS  # noqa: E402


def _romanize(name: str) -> tuple[str, str]:
    try:
        from pypinyin import Style, lazy_pinyin
    except ImportError:
        return "", ""
    syllables = lazy_pinyin(name, style=Style.NORMAL, errors="ignore")
    return "".join(syllables).lower(), "".join(item[:1] for item in syllables).lower()


def _base(
    symbol: str, name: str, *, market: str, exchange: str, asset_type: str,
    provider_symbol: str = "", full_name: str = "", en_name: str = "",
    currency: str = "", status: str = "listed", list_date: str = "",
    delist_date: str = "", source: str,
) -> dict:
    code = symbol.rsplit(".", 1)[0]
    pinyin, initials = _romanize(name)
    return {
        "symbol": symbol.upper(), "provider_symbol": provider_symbol or symbol.upper(),
        "code": code.upper(), "name": name.strip(), "full_name": full_name.strip(),
        "en_name": en_name.strip(), "pinyin": pinyin, "pinyin_initials": initials,
        "market": market, "exchange": exchange, "asset_type": asset_type,
        "currency": currency, "status": status.lower(), "list_date": list_date,
        "delist_date": delist_date, "source": source, "source_priority": 10,
        "observed_at": time.time(),
    }


def _tushare_records() -> list[dict]:
    api = _require_tushare()
    records: list[dict] = []
    for list_status in ("L", "P", "D"):
        frame = api.stock_basic(
            exchange="", list_status=list_status,
            fields="ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date",
        )
        for row in frame.to_dict("records"):
            ts_code = str(row.get("ts_code") or "").upper()
            if not ts_code:
                continue
            records.append(_base(
                ts_code, str(row.get("name") or ""), market="CN",
                exchange=ts_code.rsplit(".", 1)[-1], asset_type="stock",
                full_name=str(row.get("fullname") or ""), en_name=str(row.get("enname") or ""),
                currency=str(row.get("curr_type") or "CNY"),
                status=str(row.get("list_status") or list_status),
                list_date=str(row.get("list_date") or ""),
                delist_date=str(row.get("delist_date") or ""), source="tushare:stock_basic",
            ))
    for status in ("L", "D", "I"):
        try:
            frame = api.fund_basic(market="E", status=status)
        except Exception:
            continue
        for row in frame.to_dict("records"):
            ts_code = str(row.get("ts_code") or "").upper()
            name = str(row.get("name") or "").strip()
            if not ts_code or not name:
                continue
            kind = str(row.get("fund_type") or "").upper()
            records.append(_base(
                ts_code, name, market="CN", exchange=ts_code.rsplit(".", 1)[-1],
                asset_type="etf" if "ETF" in kind or "ETF" in name.upper() or "交易型" in kind else "fund",
                currency="CNY", status=status, list_date=str(row.get("list_date") or ""),
                delist_date=str(row.get("delist_date") or ""), source="tushare:fund_basic",
            ))
    for market in ("CSI", "SSE", "SZSE"):
        try:
            frame = api.index_basic(market=market)
        except Exception:
            continue
        for row in frame.to_dict("records"):
            ts_code = str(row.get("ts_code") or "").upper()
            name = str(row.get("name") or "").strip()
            if not ts_code or not name:
                continue
            records.append(_base(
                ts_code, name, market="CN", exchange=ts_code.rsplit(".", 1)[-1],
                asset_type="index", full_name=str(row.get("fullname") or ""),
                currency="CNY", status="listed", source=f"tushare:index_basic:{market.lower()}",
            ))
    for status in ("L", "D", "P"):
        try:
            frame = api.hk_basic(list_status=status)
        except Exception:
            continue
        for row in frame.to_dict("records"):
            raw_symbol = str(row.get("ts_code") or "").upper()
            code = str(row.get("symbol") or raw_symbol.partition(".")[0]).zfill(5)
            name = str(row.get("name") or "").strip()
            if not code or not name:
                continue
            records.append(_base(
                f"{code}.HK", name, market="HK", exchange="HKEX", asset_type="stock",
                provider_symbol=raw_symbol, full_name=str(row.get("fullname") or ""),
                en_name=str(row.get("enname") or ""), currency="HKD", status=status,
                list_date=str(row.get("list_date") or ""),
                delist_date=str(row.get("delist_date") or ""), source="tushare:hk_basic",
            ))
    return records


def _download_pipe(url: str) -> list[dict[str, str]]:
    response = httpx.get(url, timeout=30, follow_redirects=True)
    response.raise_for_status()
    lines = [line.strip() for line in response.text.splitlines() if line.strip()]
    headers = lines[0].split("|")
    return [dict(zip(headers, line.split("|"), strict=False)) for line in lines[1:]
            if not line.startswith("File Creation Time")]


def _us_records() -> list[dict]:
    rows = []
    sources = (
        ("https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt", "NASDAQ", "Symbol"),
        ("https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt", "", "ACT Symbol"),
    )
    exchange_names = {"A": "NYSE American", "N": "NYSE", "P": "NYSE Arca", "Z": "Cboe"}
    for url, default_exchange, field in sources:
        for row in _download_pipe(url):
            ticker = str(row.get(field) or "").strip().upper()
            name = str(row.get("Security Name") or "").strip()
            if not ticker or not name or row.get("Test Issue") == "Y":
                continue
            exchange = default_exchange or exchange_names.get(str(row.get("Exchange")), "US")
            rows.append(_base(
                f"{ticker}.US", name, market="US", exchange=exchange,
                asset_type="etf" if row.get("ETF") == "Y" else "stock",
                provider_symbol=ticker.replace(".", "-"), en_name=name, currency="USD",
                status="listed", source="nasdaq:symbol_directory",
            ))
    return rows


def _static_records() -> list[dict]:
    rows = []
    for symbol, name in DEMO_STOCK_NAMES.items():
        rows.append(_base(
            symbol, name, market="CN", exchange=symbol.rsplit(".", 1)[-1],
            asset_type="stock", currency="CNY", source="built_in",
        ))
    for symbol, name in FUTURES_MAIN.items():
        rows.append(_base(
            symbol, name, market="FUT", exchange=symbol.rsplit(".", 1)[-1],
            asset_type="future", currency="CNY", source="built_in",
        ))
    for symbol, (provider, name) in GLOBAL_REFS.items():
        rows.append(_base(
            symbol, name, market=symbol.rsplit(".", 1)[-1],
            exchange=symbol.rsplit(".", 1)[-1],
            asset_type="index" if provider.startswith("^") else "future",
            provider_symbol=provider, source="built_in",
        ))
    return rows


def build(output: Path) -> dict:
    combined = [*_tushare_records(), *_us_records(), *_static_records()]
    unique: dict[str, dict] = {}
    for record in combined:
        old = unique.get(record["symbol"])
        if old is None or (not old.get("name") and record.get("name")):
            unique[record["symbol"]] = record
    payload = {
        "format": 1, "generated_at": int(time.time()),
        "instruments": sorted(unique.values(), key=lambda item: item["symbol"]),
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(output, "wt", encoding="utf-8", compresslevel=9) as handle:
        json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))
    return {"path": str(output), "records": len(unique), "bytes": output.stat().st_size}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output", type=Path,
        default=ROOT / "quantmaster" / "data" / "security_master.json.gz",
    )
    args = parser.parse_args()
    if not get_config().data.tushare_token:
        raise SystemExit("需要在 config.yaml 或 TUSHARE_TOKEN 配置 Tushare token")
    print(json.dumps(build(args.output), ensure_ascii=False))


if __name__ == "__main__":
    main()
