"""券商成交 CSV 的编码识别、列映射、逐行校验与重复识别。"""

from __future__ import annotations

import hashlib
import io
import json
import math
import re
from dataclasses import asdict, dataclass
from typing import Any

import pandas as pd

from quantmaster.data.universe import normalize_symbol

MAX_CSV_BYTES = 20 * 1024 * 1024

ALIASES = {
    "date": {"date", "trade_date", "成交日期", "交易日期", "发生日期", "日期"},
    "symbol": {"symbol", "code", "ticker", "证券代码", "股票代码", "代码"},
    "side": {"side", "direction", "bs", "买卖方向", "操作", "业务名称", "委托方向"},
    "price": {"price", "trade_price", "成交价格", "成交价", "价格"},
    "shares": {"shares", "quantity", "qty", "volume", "成交数量", "成交股数", "数量"},
    "note": {"note", "remark", "备注", "摘要"},
}
FEE_ALIASES = {
    "fee", "fees", "commission", "tax", "stamp_tax", "transfer_fee",
    "手续费", "佣金", "印花税", "过户费", "规费", "其他费", "费用", "交易费",
}
BUY_VALUES = {"buy", "b", "1", "买", "买入", "证券买入", "融资买入"}
SELL_VALUES = {"sell", "s", "2", "卖", "卖出", "证券卖出", "卖券还款"}


def _key(value: Any) -> str:
    return re.sub(r"[\s_\-（）()]+", "", str(value).strip().lower())


def decode_csv(content: bytes) -> tuple[str, str]:
    if not content:
        raise ValueError("CSV 文件为空")
    if len(content) > MAX_CSV_BYTES:
        raise ValueError("CSV 文件超过 20MB 限制")
    for encoding in ("utf-8-sig", "utf-8", "gb18030"):
        try:
            return content.decode(encoding), encoding
        except UnicodeDecodeError:
            continue
    raise ValueError("无法识别编码；仅支持 UTF-8、UTF-8-SIG、GB18030")


def read_csv(content: bytes) -> tuple[pd.DataFrame, str]:
    text, encoding = decode_csv(content)
    try:
        frame = pd.read_csv(io.StringIO(text), dtype=str, keep_default_na=False)
    except Exception as exc:
        raise ValueError(f"CSV 解析失败: {exc}") from exc
    frame.columns = [str(column).strip().lstrip("\ufeff") for column in frame.columns]
    if not len(frame.columns) or len(frame.columns) > 200:
        raise ValueError("CSV 列数异常")
    if len(frame) > 200_000:
        raise ValueError("单次最多导入 200000 行")
    return frame, encoding


def suggest_mapping(columns: list[str]) -> dict[str, Any]:
    normalized = {_key(column): column for column in columns}
    mapping: dict[str, Any] = {}
    for canonical, aliases in ALIASES.items():
        for alias in aliases:
            if _key(alias) in normalized:
                mapping[canonical] = normalized[_key(alias)]
                break
    mapping["fees"] = [column for column in columns if _key(column) in {_key(a) for a in FEE_ALIASES}]
    return mapping


def validate_mapping(columns: list[str], mapping: dict[str, Any]) -> dict[str, Any]:
    required = {"date", "symbol", "side", "price", "shares"}
    missing = sorted(key for key in required if not mapping.get(key))
    if missing:
        raise ValueError(f"缺少列映射: {', '.join(missing)}")
    allowed = set(columns)
    for key in required | {"note"}:
        if mapping.get(key) and mapping[key] not in allowed:
            raise ValueError(f"映射列不存在: {mapping[key]}")
    fees = mapping.get("fees", mapping.get("fee", []))
    if isinstance(fees, str):
        fees = [fees] if fees else []
    if not isinstance(fees, list) or any(column not in allowed for column in fees):
        raise ValueError("费用列映射非法")
    return {**mapping, "fees": list(dict.fromkeys(fees))}


def _number(value: Any, label: str, *, positive: bool = True) -> float:
    raw = str(value).strip().replace(",", "").replace("￥", "")
    if not positive and not raw:
        return 0.0
    try:
        number = float(raw)
    except ValueError as exc:
        raise ValueError(f"{label}不是数字") from exc
    if not math.isfinite(number) or (positive and number <= 0) or (not positive and number < 0):
        raise ValueError(f"{label}必须为{'正数' if positive else '非负数'}")
    return number


def _side(value: Any) -> str:
    normalized = _key(value)
    if normalized in {_key(v) for v in BUY_VALUES}:
        return "buy"
    if normalized in {_key(v) for v in SELL_VALUES}:
        return "sell"
    raise ValueError(f"无法识别买卖方向: {value}")


def trade_fingerprint(record: dict[str, Any]) -> str:
    stable = "|".join((
        record["date"], record["symbol"], record["side"],
        f"{float(record['price']):.8f}", f"{float(record['shares']):.8f}",
        f"{float(record['fee']):.8f}",
    ))
    return hashlib.sha256(stable.encode("utf-8")).hexdigest()


@dataclass
class ParsedRow:
    row_number: int
    raw: dict[str, Any]
    record: dict[str, Any] | None
    errors: list[str]
    duplicate: bool = False

    def public(self) -> dict:
        return asdict(self)


@dataclass
class ParsedCSV:
    encoding: str
    columns: list[str]
    mapping: dict[str, Any]
    file_hash: str
    rows: list[ParsedRow]

    @property
    def valid_rows(self) -> list[ParsedRow]:
        return [row for row in self.rows if not row.errors]

    def preview(self, batch_duplicate: bool = False, limit: int = 50) -> dict[str, Any]:
        return {
            "encoding": self.encoding,
            "columns": self.columns,
            "suggested_mapping": self.mapping,
            "file_hash": self.file_hash,
            "batch_duplicate": batch_duplicate,
            "total_rows": len(self.rows),
            "valid_count": len(self.valid_rows),
            "error_count": sum(bool(row.errors) for row in self.rows),
            "duplicate_count": sum(row.duplicate and not row.errors for row in self.rows),
            "rows": [row.public() for row in self.rows[:limit]],
        }


def parse_broker_csv(
    content: bytes,
    mapping: dict[str, Any] | str | None = None,
    existing_fingerprints: set[str] | None = None,
) -> ParsedCSV:
    frame, encoding = read_csv(content)
    if isinstance(mapping, str):
        try:
            mapping = json.loads(mapping)
        except json.JSONDecodeError as exc:
            raise ValueError("列映射不是合法 JSON") from exc
    selected = validate_mapping(list(frame.columns), mapping or suggest_mapping(list(frame.columns)))
    known = set(existing_fingerprints or ())
    within_file: set[str] = set()
    rows: list[ParsedRow] = []
    for index, row in frame.iterrows():
        raw = {str(key): str(value) for key, value in row.items()}
        errors: list[str] = []
        record: dict[str, Any] | None = None
        duplicate = False
        try:
            date = str(pd.to_datetime(row[selected["date"]], errors="raise").date())
            record = {
                "date": date,
                "symbol": normalize_symbol(row[selected["symbol"]]),
                "side": _side(row[selected["side"]]),
                "price": _number(row[selected["price"]], "成交价"),
                "shares": _number(row[selected["shares"]], "成交数量"),
                "fee": sum(_number(row[column] or 0, column, positive=False)
                           for column in selected.get("fees", [])),
                "note": str(row[selected["note"]]).strip() if selected.get("note") else "",
            }
            record["fingerprint"] = trade_fingerprint(record)
            duplicate = record["fingerprint"] in known or record["fingerprint"] in within_file
            within_file.add(record["fingerprint"])
        except (ValueError, TypeError, OverflowError) as exc:
            errors.append(str(exc))
        rows.append(ParsedRow(row_number=int(index) + 2, raw=raw, record=record,
                              errors=errors, duplicate=duplicate))
    return ParsedCSV(encoding=encoding, columns=list(frame.columns), mapping=selected,
                     file_hash=hashlib.sha256(content).hexdigest(), rows=rows)
