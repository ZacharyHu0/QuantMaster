"""候选管理：指数成分、自定义列表。"""

from __future__ import annotations

import json
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from quantmaster.config import get_config
from quantmaster.research.contracts import content_hash
from quantmaster.trading_sessions import daily_signal_cutoff, market_date

if TYPE_CHECKING:
    from quantmaster.data.instruments import InstrumentStore

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

# 新建候选时可直接读取的常用指数。科技成长方向优先排列；这些条目只是
# 当前成分的快捷入口，保存后仍会成为普通的本地固定候选。
INDEX_UNIVERSE_PRESETS = (
    {
        "name": "科创50", "symbol": "000688.SH", "category": "科技成长",
        "description": "科创板大市值核心", "preferred": True,
    },
    {
        "name": "科创100", "symbol": "000698.SH", "category": "科技成长",
        "description": "科创板中盘成长", "preferred": True,
    },
    {
        "name": "科创创业50", "symbol": "931643.CSI", "category": "科技成长",
        "description": "科创板与创业板龙头", "preferred": True,
    },
    {
        "name": "半导体材料设备", "symbol": "931743.CSI", "category": "科技成长",
        "description": "半导体材料与设备", "preferred": True,
    },
    {
        "name": "创业板指", "symbol": "399006.SZ", "category": "科技成长",
        "description": "创业板核心成长", "preferred": True,
    },
    {
        "name": "创业板50", "symbol": "399673.SZ", "category": "科技成长",
        "description": "创业板高流动性龙头", "preferred": True,
    },
    {
        "name": "沪深300", "symbol": "000300.SH", "category": "主流宽基",
        "description": "沪深大盘核心", "preferred": False,
    },
    {
        "name": "中证500", "symbol": "000905.SH", "category": "主流宽基",
        "description": "中盘代表", "preferred": False,
    },
    {
        "name": "中证1000", "symbol": "000852.SH", "category": "主流宽基",
        "description": "小盘成长代表", "preferred": False,
    },
)


def _universe_dir() -> Path:
    p = get_config().data_root / "universe"
    p.mkdir(parents=True, exist_ok=True)
    return p


_NAME_RE = re.compile(r"[A-Za-z0-9_\-\u4e00-\u9fff]{1,40}")
SYSTEM_UNIVERSES = {"demo", "csi800"}
UNIVERSE_SCHEMA_VERSION = 2


@dataclass(frozen=True)
class UniverseSnapshot:
    name: str
    symbols: tuple[str, ...]
    observed_at: str
    effective_as_of: str
    content_hash: str
    source: str
    formal_eligible: bool = True
    issues: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_version": UNIVERSE_SCHEMA_VERSION,
            "name": self.name,
            "symbols": list(self.symbols),
            "observed_at": self.observed_at,
            "effective_as_of": self.effective_as_of,
            "content_hash": self.content_hash,
            "source": self.source,
        }
        # Keep the persisted v2 contract byte-for-byte stable. Preview-only flags are
        # derived from legacy evidence and are emitted only for analytical consumers.
        if not self.formal_eligible or self.issues:
            payload["formal_eligible"] = self.formal_eligible
            payload["issues"] = list(self.issues)
        return payload


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


def normalize_symbol(symbol: str, *, store: InstrumentStore | None = None) -> str:
    """兼容旧调用的单值规范化。

    新增交互应使用 ``resolve_instrument(s)`` 暴露歧义。这里仅对历史上明确约定
    为 A 股的六位裸代码保留交易所推断，避免已有配置升级后无法读取。
    """
    from quantmaster.data.instruments import InstrumentStore

    raw = str(symbol).strip()
    instrument_store = store if store is not None else InstrumentStore()
    value = re.sub(r"\s+", "", raw).upper()
    direct = instrument_store.get(value)
    if direct is not None:
        return direct.symbol

    result = instrument_store.resolve(raw)
    if result["status"] == "resolved":
        return result["instrument"]["symbol"]

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
        corrected = instrument_store.get(f"{wrong_csi.group(1)}.CSI")
        if corrected and corrected.asset_type == "index":
            return corrected.symbol
    message = result.get("message") or f"证券代码或名称无法识别: {symbol}"
    raise ValueError(message)


def normalize_symbols(symbols: list[str]) -> list[str]:
    from quantmaster.data.instruments import InstrumentStore

    raw_values = [str(symbol).strip() for symbol in symbols]
    store = InstrumentStore()
    canonical_values = [re.sub(r"\s+", "", value).upper() for value in raw_values]
    known = store.get_many(canonical_values)
    result: list[str] = []
    seen: set[str] = set()
    for raw, canonical in zip(raw_values, canonical_values, strict=True):
        direct = known.get(canonical)
        normalized = direct.symbol if direct is not None else normalize_symbol(raw, store=store)
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


def _universe_history_dir(name: str) -> Path:
    path = _universe_dir() / "history" / name
    path.mkdir(parents=True, exist_ok=True)
    return path


def _snapshot_content_hash(
    *, name: str, symbols: tuple[str, ...], observed_at: str,
    effective_as_of: str, source: str,
) -> str:
    return content_hash({
        "schema_version": UNIVERSE_SCHEMA_VERSION,
        "name": name,
        "symbols": sorted(symbols),
        "observed_at": observed_at,
        "effective_as_of": effective_as_of,
        "source": source,
    })


def _history_filename(snapshot: UniverseSnapshot) -> str:
    observed = datetime.fromisoformat(snapshot.observed_at).astimezone(UTC)
    return f"{observed.strftime('%Y%m%dT%H%M%S%fZ')}--{snapshot.content_hash[:16]}.json"


def universe_snapshot_from_payload(
    payload: object, *, expected_name: str,
) -> UniverseSnapshot:
    if not isinstance(payload, dict) or payload.get("schema_version") != UNIVERSE_SCHEMA_VERSION:
        raise ValueError("候选文件缺少可回放的 v2 时间与内容哈希；请在候选管理中重新保存")
    name = str(payload.get("name") or "")
    raw_symbols = payload.get("symbols")
    observed_raw = str(payload.get("observed_at") or "")
    effective = str(payload.get("effective_as_of") or "")
    source = str(payload.get("source") or "")
    if name != expected_name or not isinstance(raw_symbols, list) or not raw_symbols:
        raise ValueError("候选快照身份或标的列表无效")
    try:
        observed = datetime.fromisoformat(observed_raw.replace("Z", "+00:00"))
        date.fromisoformat(effective)
    except ValueError as exc:
        raise ValueError("候选快照缺少有效的 observed_at/effective_as_of") from exc
    if observed.tzinfo is None:
        raise ValueError("候选快照 observed_at 必须包含时区")
    symbols = tuple(dict.fromkeys(str(item).upper() for item in raw_symbols if str(item)))
    observed_iso = observed.astimezone(UTC).isoformat()
    digest = _snapshot_content_hash(
        name=name,
        symbols=symbols,
        observed_at=observed_iso,
        effective_as_of=effective,
        source=source,
    )
    if not symbols or not source or digest != str(payload.get("content_hash") or ""):
        raise ValueError("候选快照内容哈希不匹配")
    return UniverseSnapshot(
        name=name,
        symbols=symbols,
        observed_at=observed_iso,
        effective_as_of=effective,
        content_hash=digest,
        source=source,
    )


def save_universe(
    name: str,
    symbols: list[str],
    *,
    observed_at: datetime | None = None,
    effective_as_of: str | date | None = None,
) -> None:
    safe_name = validate_universe_name(name)
    normalized = tuple(normalize_symbols(symbols))
    observed = observed_at or datetime.now(UTC)
    if observed.tzinfo is None:
        raise ValueError("候选 observed_at 必须包含时区")
    observed = observed.astimezone(UTC)
    effective = (
        effective_as_of.isoformat()
        if isinstance(effective_as_of, date)
        else str(effective_as_of or market_date(observed).isoformat())
    )
    date.fromisoformat(effective)
    source = "custom-local"
    observed_iso = observed.isoformat()
    snapshot = UniverseSnapshot(
        name=safe_name,
        symbols=normalized,
        observed_at=observed_iso,
        effective_as_of=effective,
        content_hash=_snapshot_content_hash(
            name=safe_name,
            symbols=normalized,
            observed_at=observed_iso,
            effective_as_of=effective,
            source=source,
        ),
        source=source,
    )
    payload = snapshot.to_dict()
    stamp = observed.strftime("%Y%m%dT%H%M%S%fZ")
    history = _universe_history_dir(safe_name)
    target = history / _history_filename(snapshot)
    siblings = list(history.glob(f"{stamp}--*.json"))
    if siblings:
        if len(siblings) != 1 or siblings[0].name != target.name:
            raise RuntimeError("同一 observed_at 已存在不同内容的候选快照，拒绝改写历史")
        existing = json.loads(siblings[0].read_text(encoding="utf-8"))
        if existing != payload:
            raise RuntimeError("同一候选快照身份对应不同内容，拒绝改写历史")
        universe_snapshot_from_payload(existing, expected_name=safe_name)
    else:
        _atomic_json(target, payload)
    _atomic_json(_universe_dir() / f"{safe_name}.json", payload)


def load_universe_snapshot(name: str, *, as_of: str | None = None) -> UniverseSnapshot:
    normalized_name = str(name).lower()
    if normalized_name == "demo":
        symbols = tuple(DEMO_UNIVERSE)
        return UniverseSnapshot(
            name="demo",
            symbols=symbols,
            observed_at="1970-01-01T00:00:00+00:00",
            effective_as_of="1970-01-01",
            content_hash=content_hash({"name": "demo", "symbols": sorted(symbols)}),
            source="bundled-demo",
        )
    if normalized_name == "csi800":
        from quantmaster.data.index_membership import load_cached_csi800_members_as_of

        target = as_of or market_date().isoformat()
        evidence = load_cached_csi800_members_as_of(target)
        symbols = tuple(evidence["symbols"])
        acquired = max(evidence.get("snapshot_acquired_at", {}).values())
        return UniverseSnapshot(
            name="csi800",
            symbols=symbols,
            observed_at=acquired,
            effective_as_of=str(evidence["effective_as_of"]),
            content_hash=str(evidence["content_hash"]),
            source=str(evidence["source"]),
        )
    safe_name = validate_universe_name(name)
    current = _universe_dir() / f"{safe_name}.json"
    if not current.exists():
        raise FileNotFoundError(f"候选不存在: {name}（可用 save_universe 创建，或使用 'demo'）")
    if not as_of:
        return universe_snapshot_from_payload(
            json.loads(current.read_text(encoding="utf-8")), expected_name=safe_name,
        )
    target = date.fromisoformat(as_of)
    cutoff = daily_signal_cutoff(target).astimezone(UTC)
    candidates: list[UniverseSnapshot] = []
    for path in _universe_history_dir(safe_name).glob("*.json"):
        try:
            snapshot = universe_snapshot_from_payload(
                json.loads(path.read_text(encoding="utf-8")), expected_name=safe_name,
            )
            if path.name != _history_filename(snapshot):
                raise ValueError("候选历史文件名与内容身份不一致")
            observed = datetime.fromisoformat(snapshot.observed_at)
            if date.fromisoformat(snapshot.effective_as_of) <= target and observed <= cutoff:
                candidates.append(snapshot)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"候选 {safe_name} 的历史证据损坏：{path.name}") from exc
    if not candidates:
        raise RuntimeError(
            f"候选 {safe_name} 在 {target.isoformat()} 上海 15:00 前没有可验证快照"
        )
    return max(candidates, key=lambda item: item.observed_at)


def load_universe(name: str, *, as_of: str | None = None) -> list[str]:
    return list(load_universe_snapshot(name, as_of=as_of).symbols)


def load_universe_analysis_snapshot(
    name: str, *, as_of: str | None = None,
) -> UniverseSnapshot:
    """Load a universe for analysis without weakening the formal snapshot contract.

    Pre-v2 JSON lists are useful owner-curated candidates, but they have no observed
    time or immutable history.  They are therefore admitted only for a current
    sandbox calculation.  Historical queries and formal persistence continue to use
    :func:`load_universe_snapshot` and fail closed.
    """
    try:
        return load_universe_snapshot(name, as_of=as_of)
    except ValueError:
        if as_of is not None:
            raise

    safe_name = validate_universe_name(name)
    current = _universe_dir() / f"{safe_name}.json"
    try:
        payload = json.loads(current.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise FileNotFoundError(
            f"候选不存在: {name}（可用 save_universe 创建，或使用 'demo'）"
        ) from None
    if not isinstance(payload, list):
        raise ValueError("候选文件无效；只有旧版代码列表可用于 sandbox 预览")
    symbols = tuple(normalize_symbols([str(item) for item in payload]))
    observed = datetime.fromtimestamp(current.stat().st_mtime, tz=UTC)
    observed_iso = observed.isoformat()
    effective = market_date(observed).isoformat()
    digest = content_hash({
        "schema": "legacy-universe-preview-v1",
        "name": safe_name,
        "symbols": sorted(symbols),
        "file_observed_at": observed_iso,
        "source": "legacy-custom-preview",
    })
    return UniverseSnapshot(
        name=safe_name,
        symbols=symbols,
        observed_at=observed_iso,
        effective_as_of=effective,
        content_hash=digest,
        source="legacy-custom-preview",
        formal_eligible=False,
        issues=("旧候选缺少可回放时间与来源；仅用于当前 sandbox 分析",),
    )


def load_universe_analysis(name: str, *, as_of: str | None = None) -> list[str]:
    return list(load_universe_analysis_snapshot(name, as_of=as_of).symbols)


def list_universes() -> list[dict]:
    items = [{
        "name": "demo",
        "count": len(DEMO_UNIVERSE),
        "readonly": True,
        "formal_eligible": True,
        "issues": [],
    }]
    for path in sorted(_universe_dir().glob("*.json"), key=lambda item: item.stem.casefold()):
        try:
            name = validate_universe_name(path.stem)
            snapshot = load_universe_analysis_snapshot(name)
            items.append({
                "name": name,
                "count": len(snapshot.symbols),
                "readonly": False,
                "formal_eligible": snapshot.formal_eligible,
                "issues": list(snapshot.issues),
            })
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
    symbols = load_universe_analysis(old)
    save_universe(new, symbols)
    source.unlink()


def index_universe(index_symbol: str = "000300.SH") -> list[str]:  # pragma: no cover - 网络
    """从指数成分构建候选（如沪深300）。"""
    from quantmaster.data.akshare_source import AkshareSource

    return AkshareSource().index_members(index_symbol)
