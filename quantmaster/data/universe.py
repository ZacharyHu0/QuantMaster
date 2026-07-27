"""候选管理：指数成分、自定义列表。"""

from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path

from quantmaster.config import get_config

# 内置示例候选：沪深各行业代表性大盘股（便于开箱即用地跑通流程）
DEMO_STOCK_NAMES = {
    "600519.SH": "贵州茅台",
    "601318.SH": "中国平安",
    "600036.SH": "招商银行",
    "601899.SH": "紫金矿业",
    "600900.SH": "长江电力",
    "688981.SH": "中芯国际",
    "000333.SZ": "美的集团",
    "000858.SZ": "五粮液",
    "300750.SZ": "宁德时代",
    "002594.SZ": "比亚迪",
    "300059.SZ": "东方财富",
    "002230.SZ": "科大讯飞",
}
DEMO_UNIVERSE = list(DEMO_STOCK_NAMES)


def _universe_dir() -> Path:
    p = get_config().data_root / "universe"
    p.mkdir(parents=True, exist_ok=True)
    return p


_NAME_RE = re.compile(r"[A-Za-z0-9_\-\u4e00-\u9fff]{1,40}")
SYSTEM_UNIVERSES = {"demo", "csi800"}


def validate_universe_name(name: str, *, allow_demo: bool = False) -> str:
    """候选名直接映射文件名，必须先严格过滤以阻止路径穿越。"""
    value = str(name).strip()
    if not _NAME_RE.fullmatch(value) or value in {".", ".."}:
        raise ValueError("候选名称仅支持 1–40 位中英文、数字、下划线和连字符")
    normalized = value.lower()
    if normalized == "demo" and allow_demo:
        return "demo"
    if normalized in SYSTEM_UNIVERSES:
        label = "内置 demo 候选" if normalized == "demo" else "动态 csi800 候选"
        raise ValueError(f"{label}只读，请复制后再编辑")
    return value


def normalize_symbol(symbol: str) -> str:
    """兼容旧调用的单值规范化。

    新增交互应使用 ``resolve_instrument(s)`` 暴露歧义。这里仅对历史上明确约定
    为 A 股的六位裸代码保留交易所推断，避免已有配置升级后无法读取。
    """
    from quantmaster.data.instruments import InstrumentStore

    raw = str(symbol).strip()
    store = InstrumentStore()
    result = store.resolve(raw)
    if result["status"] == "resolved":
        return result["instrument"]["symbol"]

    value = re.sub(r"\s+", "", raw).upper()
    if re.fullmatch(r"\d{6}", value) and result["status"] in {"ambiguous", "unresolved"}:
        if value.startswith(("4", "8", "92")):
            preferred = f"{value}.BJ"
        elif value.startswith(("0", "2", "3")):
            preferred = f"{value}.SZ"
        else:
            preferred = f"{value}.SH"
        if result["status"] == "unresolved" or any(
            item["symbol"] == preferred for item in result["candidates"]
        ):
            return preferred

    # 旧版曾把所有 9xxxxx 指数误写为 .SH；有且只有一个同码 CSI 指数时迁移。
    wrong_csi = re.fullmatch(r"(\d{6})\.SH", value)
    if wrong_csi:
        corrected = store.get(f"{wrong_csi.group(1)}.CSI")
        if corrected and corrected.asset_type == "index":
            return corrected.symbol
    message = result.get("message") or f"证券代码或名称无法识别: {symbol}"
    raise ValueError(message)


def normalize_symbols(symbols: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        normalized = normalize_symbol(symbol)
        if normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    if not result:
        raise ValueError("候选至少需要一个有效代码")
    if len(result) > 10_000:
        raise ValueError("单个候选最多 10000 只标的")
    return result


def _atomic_json(path: Path, value: object) -> None:
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temp = Path(temp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def save_universe(name: str, symbols: list[str]) -> None:
    safe_name = validate_universe_name(name)
    _atomic_json(_universe_dir() / f"{safe_name}.json", normalize_symbols(symbols))


def load_universe(name: str) -> list[str]:
    if str(name).lower() == "demo":
        return list(DEMO_UNIVERSE)
    safe_name = validate_universe_name(name)
    path = _universe_dir() / f"{safe_name}.json"
    if not path.exists():
        raise FileNotFoundError(f"候选不存在: {name}（可用 save_universe 创建，或使用 'demo'）")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, list):
        raise ValueError(f"候选文件格式错误: {safe_name}")
    return normalize_symbols([str(item) for item in value])


def list_universes() -> list[dict]:
    items = [{"name": "demo", "count": len(DEMO_UNIVERSE), "readonly": True}]
    for path in sorted(_universe_dir().glob("*.json"), key=lambda item: item.stem.casefold()):
        try:
            name = validate_universe_name(path.stem)
            symbols = load_universe(name)
            items.append({"name": name, "count": len(symbols), "readonly": False})
        except (OSError, ValueError, json.JSONDecodeError):
            continue
    return items


def delete_universe(name: str) -> None:
    safe_name = validate_universe_name(name)
    path = _universe_dir() / f"{safe_name}.json"
    if not path.is_file():
        raise FileNotFoundError(f"候选不存在: {safe_name}")
    path.unlink()


def rename_universe(name: str, new_name: str) -> None:
    old = validate_universe_name(name)
    new = validate_universe_name(new_name)
    source, target = _universe_dir() / f"{old}.json", _universe_dir() / f"{new}.json"
    if not source.is_file():
        raise FileNotFoundError(f"候选不存在: {old}")
    if target.exists():
        raise FileExistsError(f"候选已存在: {new}")
    os.replace(source, target)


def index_universe(index_symbol: str = "000300.SH") -> list[str]:  # pragma: no cover - 网络
    """从指数成分构建候选（如沪深300）。"""
    from quantmaster.data.akshare_source import AkshareSource

    return AkshareSource().index_members(index_symbol)
