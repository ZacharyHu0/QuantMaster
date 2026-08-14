"""行业分类：股票 -> 行业 的映射，用于因子行业中性化与持仓行业分布。

数据源：优先使用所选 free-stockdb 本地申万一级板块，随后回退 Tushare 和
东方财富。当前决策每天重新核对在市证券覆盖；历史读取只接受带完整性分母
和精确观测时点的不可变快照。
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal

from quantmaster.config import get_config
from quantmaster.data.resilience import akshare_call
from quantmaster.trading_sessions import daily_signal_cutoff, market_date

logger = logging.getLogger(__name__)

CACHE_TTL_DAYS = 1
CURRENT_ONLY_SCHEMA_VERSION = 3
CURRENT_ONLY_SOURCE = "current-only-migration"
IndustryReadMode = Literal["formal", "sandbox_current"]


class IndustrySnapshotIncomplete(RuntimeError):
    """A fetched classification was persisted as evidence but is not usable."""


class IndustrySnapshotIntegrityError(RuntimeError):
    """An immutable industry artifact failed its self-hash or file identity."""


class LegacyIndustrySnapshotError(IndustrySnapshotIntegrityError):
    """The current projection predates the immutable snapshot contract."""


def _cache_path() -> Path:
    return get_config().data_root / "industry_map.json"


def _history_root() -> Path:
    return get_config().data_root / "industry_map_history"


def _block_cache_path() -> Path:
    return get_config().data_root / "industry_blocks.json"


def _load_industry_blocks() -> dict[str, dict]:
    path = _block_cache_path()
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data.get("blocks", {}) if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        return {}


def _save_industry_blocks(blocks: dict[str, dict]) -> None:
    """成功一个板块就原子落盘，后续板块失败也不影响已取得的数据。"""
    path = _block_cache_path()
    temp = path.with_suffix(".json.tmp")
    temp.write_text(
        json.dumps({"updated_at": time.time(), "blocks": blocks}, ensure_ascii=False),
        encoding="utf-8",
    )
    temp.replace(path)


def _free_stockdb_industry_map() -> dict[str, str]:  # pragma: no cover - 本地外部服务
    if get_config().data.primary_provider != "free-stockdb":
        return {}
    try:
        from quantmaster.data.free_stockdb_source import FreeStockDBSource

        mapping = FreeStockDBSource().industry_map()
        if not mapping:
            logger.warning("free-stockdb 申万一级行业映射为空，继续使用备用源")
        return mapping
    except (ImportError, OSError, RuntimeError, TypeError, ValueError) as exc:
        logger.warning("free-stockdb 行业映射不可用，继续使用备用源: %s", exc)
        return {}


def _fallback_industry_map() -> dict[str, str]:  # pragma: no cover - 网络
    tushare_mapping: dict[str, str] = {}
    if get_config().data.tushare_token:
        try:
            from quantmaster.data.tushare_source import TushareSource

            tushare_mapping = TushareSource().industry_map()
            # A 股在市公司通常远超 3000；低于该值多半是个别行业请求失败。
            if len(tushare_mapping) >= 3000:
                return tushare_mapping
            logger.warning(
                "Tushare 申万行业映射仅 %s 条，继续用 AKShare 补全", len(tushare_mapping))
        except Exception as e:
            logger.warning("Tushare 申万行业映射失败，降级 AKShare: %s", e)

    import akshare as ak

    boards = akshare_call(
        "stock_board_industry_name_em", ak.stock_board_industry_name_em)
    blocks = _load_industry_blocks()
    for _, row in boards.iterrows():
        board = str(row["板块名称"])
        try:
            cons = akshare_call(
                f"stock_board_industry_cons_em({board})",
                ak.stock_board_industry_cons_em, symbol=board,
            )
        except Exception as e:
            logger.warning("行业 %s 成分获取失败: %s", board, e)
            continue
        block_mapping: dict[str, str] = {}
        for code in cons["代码"].astype(str).str.zfill(6):
            suffix = "SH" if code.startswith(("6", "9")) else (
                "BJ" if code.startswith(("4", "8")) else "SZ")
            block_mapping[f"{code}.{suffix}"] = board
        # 空响应同样视为不完整，不用它覆盖以前抓到的完整板块。
        if block_mapping:
            blocks[board] = {"updated_at": time.time(), "mapping": block_mapping}
            _save_industry_blocks(blocks)
    mapping: dict[str, str] = {}
    for block in blocks.values():
        if isinstance(block, dict):
            mapping.update(block.get("mapping", {}))
    # 对同一股票优先采用申万 2021 一级行业口径。
    return {**mapping, **tushare_mapping}


def fetch_industry_map() -> dict[str, str]:  # pragma: no cover - 网络
    """按设置优先使用 free-stockdb，失败后回退 Tushare/东方财富。"""
    return _free_stockdb_industry_map() or _fallback_industry_map()


def _active_cn_universe(*, as_of: str | None = None) -> tuple[set[str], dict[str, object]]:
    """Return a denominator exclusively from an immutable catalog object."""
    try:
        from quantmaster.data.instrument_snapshots import load_instrument_catalog_snapshot

        effective = as_of or market_date().isoformat()
        _snapshot, symbols, evidence = load_instrument_catalog_snapshot(
            as_of=effective, market="CN", asset_type="stock",
        )
        return symbols, evidence
    except (ImportError, OSError, RuntimeError, TypeError, ValueError) as exc:
        raise IndustrySnapshotIncomplete(
            f"不可变证券目录不可用，无法证明行业快照分母：{exc}"
        ) from exc


def _active_cn_symbols() -> set[str]:
    """Compatibility-free public denominator accessor backed by immutable evidence."""
    return _active_cn_universe()[0]


def _industry_payload_hash(payload: dict) -> str:
    body = {key: value for key, value in payload.items() if key != "content_sha256"}
    return hashlib.sha256(
        json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _atomic_json(path: Path, serialized: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with staged.open("w", encoding="utf-8", newline="\n") as stream:
            stream.write(serialized)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(staged, path)
    finally:
        staged.unlink(missing_ok=True)


def _current_only_payload(payload: dict, *, history: bool) -> dict:
    """Accept an explicitly migrated projection for *current* classification only.

    This is deliberately not an immutable/PIT evidence contract.  A migrated
    mapping retains one useful observation timestamp, but has no
    complete catalog denominator and therefore must never become a historical
    replay candidate.  The migration contains no derived content hash: the
    operator has explicitly declared the local source trusted.
    """
    if history:
        raise LegacyIndustrySnapshotError("受信任旧行业导入不能作为历史快照")
    if payload.get("schema_version") != CURRENT_ONLY_SCHEMA_VERSION:
        raise LegacyIndustrySnapshotError("行业快照为旧格式")
    try:
        float(payload["updated_at"])
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise IndustrySnapshotIntegrityError("当前行业投影 updated_at 非法") from exc
    if payload.get("projection") != "current_only":
        raise IndustrySnapshotIntegrityError("当前行业投影标记非法")
    mapping = payload.get("mapping")
    if not isinstance(mapping, dict) or not mapping:
        raise IndustrySnapshotIntegrityError("当前行业投影缺少映射")
    normalized = {
        str(symbol).upper(): str(industry).strip()
        for symbol, industry in mapping.items()
        if str(symbol).strip() and str(industry).strip()
    }
    if not normalized:
        raise IndustrySnapshotIntegrityError("当前行业投影没有有效映射")
    return {**payload, "mapping": normalized}


def _exact_instant(value: object, *, field: str, required: bool = True) -> datetime | None:
    """Parse an exact provider/observation instant without local-time guessing."""
    if value in (None, ""):
        if required:
            raise IndustrySnapshotIntegrityError(f"行业快照缺少 {field}")
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise IndustrySnapshotIntegrityError(f"行业快照 {field} 非法") from exc
    if parsed.tzinfo is None:
        raise IndustrySnapshotIntegrityError(f"行业快照 {field} 必须包含时区")
    return parsed.astimezone(UTC)


def _temporal_contract(payload: dict) -> tuple[date, datetime]:
    """Validate canonical effective/knowledge fields for formal PIT use."""
    try:
        effective = date.fromisoformat(str(payload["effective_session_date"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise IndustrySnapshotIntegrityError(
            "行业快照缺少有效的 effective_session_date"
        ) from exc
    first_observed = _exact_instant(
        payload.get("first_observed_at"), field="first_observed_at",
    )
    assert first_observed is not None
    announced = _exact_instant(
        payload.get("announced_at"), field="announced_at", required=False,
    )
    published = _exact_instant(
        payload.get("published_at"), field="published_at", required=False,
    )
    if announced is not None and published is not None and published < announced:
        raise IndustrySnapshotIntegrityError(
            "行业快照 published_at 早于 announced_at"
        )
    if published is not None and first_observed < published:
        raise IndustrySnapshotIntegrityError(
            "行业快照 first_observed_at 早于 published_at"
        )
    return effective, first_observed


def _verified_industry_payload(path: Path, *, history: bool = False) -> dict:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise IndustrySnapshotIntegrityError(f"行业快照不可读: {path.name}") from exc
    if not isinstance(payload, dict):
        raise IndustrySnapshotIntegrityError(f"行业快照结构非法: {path.name}")
    if payload.get("schema_version") == CURRENT_ONLY_SCHEMA_VERSION:
        return _current_only_payload(payload, history=history)
    expected = str(payload.get("content_sha256") or "")
    if payload.get("schema_version") != 2 or not expected:
        raise LegacyIndustrySnapshotError(f"行业快照为旧格式: {path.name}")
    actual = _industry_payload_hash(payload)
    if expected != actual:
        raise IndustrySnapshotIntegrityError(f"行业快照内容哈希失败: {path.name}")
    if history and path.stem.rsplit("--", 1)[-1] != expected:
        raise IndustrySnapshotIntegrityError(f"行业快照文件名哈希失败: {path.name}")
    if payload.get("snapshot_complete"):
        effective_date, first_observed = _temporal_contract(payload)
        effective = effective_date.isoformat()
        evidence = dict(payload.get("universe_evidence") or {})
        if str(evidence.get("as_of") or "") != effective:
            raise IndustrySnapshotIntegrityError(
                f"行业快照与证券目录 as_of 不一致: {path.name}"
            )
        try:
            catalog_acquired = datetime.fromisoformat(
                str(evidence.get("acquired_at") or "").replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise IndustrySnapshotIntegrityError(
                f"行业快照时间证据非法: {path.name}"
            ) from exc
        if (
            catalog_acquired.tzinfo is None
            or catalog_acquired.astimezone(UTC) > first_observed
        ):
            raise IndustrySnapshotIntegrityError(
                f"行业快照早于证券目录采集时间: {path.name}"
            )
        try:
            from quantmaster.data.instrument_snapshots import (
                verify_instrument_catalog_evidence,
            )

            _snapshot, symbols = verify_instrument_catalog_evidence(
                dict(payload.get("universe_evidence") or {}),
                market="CN",
                asset_type="stock",
            )
        except (ImportError, OSError, RuntimeError, TypeError, ValueError) as exc:
            raise IndustrySnapshotIntegrityError(
                f"行业快照的证券目录证据不可恢复: {path.name}"
            ) from exc
        if symbols != set(payload.get("mapping") or {}):
            raise IndustrySnapshotIntegrityError(
                f"行业快照映射集合与证券目录分母不一致: {path.name}"
            )
    return payload


def save_industry_map(
    mapping: dict[str, str], *, effective_session_date: str | date,
    first_observed_at: str | datetime,
    announced_at: str | datetime | None = None,
    published_at: str | datetime | None = None,
    snapshot_complete: bool = True,
    source: str = "manual",
    expected_symbols: int | None = None,
    missing_symbols: list[str] | tuple[str, ...] = (),
    universe_evidence: dict[str, object] | None = None,
) -> None:
    """Persist an explicitly timed industry observation and current pointer.

    ``effective_session_date`` is the membership's business date.
    ``first_observed_at`` is when this installation first had the information.
    Provider announcement/publication instants stay optional rather than being
    guessed; neither effective nor observed time is ever filled from today/now.
    """
    effective = (
        effective_session_date.isoformat()
        if isinstance(effective_session_date, date)
        else str(effective_session_date)
    )
    try:
        effective = date.fromisoformat(effective).isoformat()
    except ValueError as exc:
        raise ValueError("行业快照日期需要使用 YYYY-MM-DD 格式") from exc
    if isinstance(first_observed_at, datetime):
        observed = first_observed_at
    else:
        try:
            observed = datetime.fromisoformat(
                str(first_observed_at).replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise ValueError(
                "行业快照 first_observed_at 需要使用带时区的 ISO 时间"
            ) from exc
    if observed.tzinfo is None:
        raise ValueError("行业快照 first_observed_at 必须包含时区")
    observed = observed.astimezone(UTC)
    temporal_values: dict[str, str | None] = {}
    for field, value in (("announced_at", announced_at), ("published_at", published_at)):
        if value is None:
            temporal_values[field] = None
            continue
        try:
            parsed = value if isinstance(value, datetime) else datetime.fromisoformat(
                str(value).replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise ValueError(f"行业快照 {field} 需要使用带时区的 ISO 时间") from exc
        if parsed.tzinfo is None:
            raise ValueError(f"行业快照 {field} 必须包含时区")
        temporal_values[field] = parsed.astimezone(UTC).isoformat()
    if (
        temporal_values["announced_at"]
        and temporal_values["published_at"]
        and str(temporal_values["published_at"]) < str(temporal_values["announced_at"])
    ):
        raise ValueError("行业快照 published_at 不能早于 announced_at")
    if (
        temporal_values["published_at"]
        and observed < datetime.fromisoformat(str(temporal_values["published_at"]))
    ):
        raise ValueError("行业快照 first_observed_at 不能早于 published_at")
    if snapshot_complete:
        if universe_evidence is None:
            universe_symbols, universe_evidence = _active_cn_universe(as_of=effective)
        else:
            from quantmaster.data.instrument_snapshots import (
                verify_instrument_catalog_evidence,
            )

            _snapshot, universe_symbols = verify_instrument_catalog_evidence(
                universe_evidence, market="CN", asset_type="stock",
            )
        if str((universe_evidence or {}).get("as_of") or "") != effective:
            raise IndustrySnapshotIncomplete(
                "行业 effective_session_date 与证券目录 as_of 不一致"
            )
        try:
            catalog_acquired = datetime.fromisoformat(
                str((universe_evidence or {}).get("acquired_at") or "").replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise IndustrySnapshotIncomplete("证券目录 acquired_at 非法") from exc
        if catalog_acquired.tzinfo is None or catalog_acquired.astimezone(UTC) > observed:
            raise IndustrySnapshotIncomplete(
                "行业 first_observed_at 早于证券目录 acquired_at"
            )
        evidence_count = int(str((universe_evidence or {}).get("expected_count") or 0))
        expected_value = int(expected_symbols) if expected_symbols is not None else len(universe_symbols)
        if (
            not universe_evidence
            or not str(universe_evidence.get("snapshot_id") or "")
            or evidence_count <= 0
            or expected_value != evidence_count
            or set(mapping) != universe_symbols
            or missing_symbols
        ):
            raise IndustrySnapshotIncomplete(
                "行业完整快照的分母与不可变证券目录证据不一致"
            )
        expected_symbols = evidence_count
    payload = {
        "schema_version": 2,
        "announced_at": temporal_values["announced_at"],
        "published_at": temporal_values["published_at"],
        "first_observed_at": observed.isoformat(),
        "effective_session_date": effective,
        "snapshot_complete": bool(snapshot_complete),
        "quality": "verified_complete" if snapshot_complete else "degraded_merged_partial",
        "source": source,
        "expected_symbols": expected_symbols,
        "observed_symbols": (
            max(0, int(expected_symbols) - len(missing_symbols))
            if expected_symbols is not None
            else len(mapping)
        ),
        "missing_symbols": list(missing_symbols),
        "universe_evidence": universe_evidence or {},
        "mapping": mapping,
    }
    payload["content_sha256"] = _industry_payload_hash(payload)
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    path = _cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    if snapshot_complete:
        _atomic_json(path, serialized)
    history = _history_root()
    history.mkdir(parents=True, exist_ok=True)
    digest = payload["content_sha256"]
    snapshot = history / f"{effective}--{digest}.json"
    if snapshot.is_file():
        _verified_industry_payload(snapshot, history=True)
        return
    _atomic_json(snapshot, serialized)


def _load_snapshot_payload(as_of: str) -> dict:
    try:
        target = date.fromisoformat(as_of)
    except ValueError as exc:
        raise ValueError("行业查看日期需要使用 YYYY-MM-DD 格式") from exc
    cutoff = daily_signal_cutoff(target).astimezone(UTC)
    candidates: list[tuple[date, datetime, dict]] = []
    history = _history_root()
    paths: list[Path] = []
    if history.is_dir():
        paths.extend(history.rglob("*.json"))
    current = _cache_path()
    if current.is_file():
        paths.append(current)
    for path in paths:
        try:
            payload = _verified_industry_payload(
                path, history=path.parent == history,
            )
            if payload.get("schema_version") == CURRENT_ONLY_SCHEMA_VERSION:
                continue
            effective, observed = _temporal_contract(payload)
            mapping = payload.get("mapping", {}) if isinstance(payload, dict) else {}
            expected_count = int(payload.get("expected_symbols") or 0)
            observed_count = int(payload.get("observed_symbols") or 0)
            complete = bool(
                payload.get("snapshot_complete")
                and expected_count > 0
                and observed_count == expected_count
                and not payload.get("missing_symbols")
            )
            if mapping and complete and effective <= target and observed <= cutoff:
                candidates.append((effective, observed, payload))
        except IndustrySnapshotIntegrityError:
            raise
        except (TypeError, ValueError):
            continue
    if candidates:
        return max(candidates, key=lambda item: (item[0], item[1]))[2]
    raise RuntimeError(
        f"{target.isoformat()} 上海 15:00 前没有带 first_observed_at 的行业分类快照；"
        "拒绝用当前行业映射重算历史结果"
    )


def load_industry_map(
    refresh: bool = False, *, as_of: str | None = None,
    mode: IndustryReadMode = "formal",
) -> dict[str, str]:
    """Read current classification or a strictly dated historical snapshot."""
    if mode not in {"formal", "sandbox_current"}:
        raise ValueError("行业读取模式必须是 formal 或 sandbox_current")
    if as_of and mode == "sandbox_current":
        raise ValueError("sandbox_current 不能用于历史行业读取")
    if as_of:
        payload = _load_snapshot_payload(as_of)
        return {
            str(key): str(value)
            for key, value in (payload.get("mapping") or {}).items()
            if value
        }
    path = _cache_path()
    cached: dict[str, str] = {}
    fresh = False
    if path.exists():
        try:
            data = _verified_industry_payload(path)
            cached = data.get("mapping", {})
            if data.get("schema_version") == CURRENT_ONLY_SCHEMA_VERSION:
                if mode == "sandbox_current" and not refresh:
                    return cached
                if not refresh:
                    raise LegacyIndustrySnapshotError(
                        "current-only 行业投影仅允许显式 sandbox_current 预览"
                    )
                # A deliberate refresh may still replace this import with a
                # provider observation, but it never rewrites it as history.
                cached = {}
            active_symbols = _active_cn_symbols()
            missing_current = active_symbols - set(cached)
            complete = bool(
                data.get("snapshot_complete")
                and active_symbols
                and not missing_current
            )
            if data.get("schema_version") == CURRENT_ONLY_SCHEMA_VERSION:
                observed_epoch = float(data.get("updated_at") or 0)
            else:
                _effective, first_observed = _temporal_contract(data)
                observed_epoch = first_observed.timestamp()
            age_seconds = time.time() - observed_epoch
            fresh = (
                complete
                and 0 <= age_seconds < CACHE_TTL_DAYS * 86400
            )
        except LegacyIndustrySnapshotError:
            # A pre-v2 projection is useful only as a degraded live preview.
            # Current refresh treats it as no formal cache and replaces it only
            # after a complete provider observation passes the new contract.
            if not refresh:
                raise
        except IndustrySnapshotIntegrityError:
            raise
    if cached and fresh and not refresh:
        return cached
    try:
        fetched = fetch_industry_map()
        if not isinstance(fetched, tuple) or len(fetched) != 4:
            raise IndustrySnapshotIncomplete(
                "行业 provider 缺少 effective_session_date/first_observed_at 时间证据"
            )
        mapping, provider_complete, source, temporal_evidence = fetched
        if not isinstance(temporal_evidence, dict):
            raise IndustrySnapshotIncomplete("行业 provider 时间证据结构非法")
        if mapping:
            active_symbols, universe_evidence = _active_cn_universe()
            observed_mapping = {
                symbol: str(mapping[symbol])
                for symbol in active_symbols
                if str(mapping.get(symbol) or "")
            }
            missing_symbols = sorted(active_symbols - set(observed_mapping))
            complete = bool(
                provider_complete and active_symbols and not missing_symbols
            )
            # Only a provider-declared complete snapshot may express removals.
            # Partial refreshes merge with the previous projection and stay
            # explicitly degraded in the immutable observation manifest.
            resolved = observed_mapping if complete else {**cached, **observed_mapping}
            save_industry_map(
                resolved,
                effective_session_date=temporal_evidence.get("effective_session_date"),
                first_observed_at=temporal_evidence.get("first_observed_at"),
                announced_at=temporal_evidence.get("announced_at"),
                published_at=temporal_evidence.get("published_at"),
                snapshot_complete=bool(complete),
                source=str(source),
                expected_symbols=len(active_symbols) if active_symbols else None,
                missing_symbols=missing_symbols,
                universe_evidence=universe_evidence,
            )
            if complete:
                return resolved
            reason = (
                f"行业映射缺少 {len(missing_symbols)}/{len(active_symbols)} 个在市标的"
                if active_symbols else "证券主表为空，无法证明行业映射完整性"
            )
            raise IndustrySnapshotIncomplete(
                f"行业分类快照仅作为 degraded 证据保存：{reason}"
            )
    except IndustrySnapshotIncomplete:
        raise
    except Exception as e:
        logger.warning("行业映射抓取失败: %s", e)
        raise RuntimeError(
            "行业映射刷新失败；拒绝返回未重新验证的新鲜度未知缓存"
        ) from e
    raise RuntimeError("行业映射源未返回可验证的完整快照")


def load_cached_industry_map(
    *, as_of: str | None = None, mode: IndustryReadMode = "formal",
) -> dict[str, str]:
    """只读本地白名单缓存；AutoMiner 构造特征时绝不隐式触网。"""
    if as_of:
        if mode == "sandbox_current":
            raise ValueError("sandbox_current 不能用于历史行业读取")
        payload = _load_snapshot_payload(as_of)
        return {
            str(key): str(value)
            for key, value in (payload.get("mapping") or {}).items()
            if value
        }
    path = _cache_path()
    if not path.is_file():
        return {}
    try:
        payload = _verified_industry_payload(path)
        if (
            payload.get("schema_version") == CURRENT_ONLY_SCHEMA_VERSION
            and mode != "sandbox_current"
        ):
            raise LegacyIndustrySnapshotError(
                "current-only 行业投影仅允许显式 sandbox_current 预览"
            )
        mapping = payload.get("mapping", {}) if isinstance(payload, dict) else {}
        return {str(key): str(value) for key, value in mapping.items() if value}
    except IndustrySnapshotIntegrityError:
        raise


def load_industry_evidence(*, as_of: str | None = None) -> dict[str, object]:
    """Return the exact manifest supporting a previously validated mapping."""
    if as_of:
        payload = _load_snapshot_payload(as_of)
    else:
        path = _cache_path()
        if not path.is_file():
            raise RuntimeError("当前行业分类没有可验证清单")
        try:
            payload = _verified_industry_payload(path)
        except IndustrySnapshotIntegrityError as exc:
            raise RuntimeError("当前行业分类清单不可读") from exc
    mapping = payload.get("mapping") if isinstance(payload, dict) else None
    if not isinstance(mapping, dict) or not mapping:
        raise RuntimeError("行业分类清单缺少映射内容")
    return {
        "status": (
            "degraded"
            if payload.get("schema_version") == CURRENT_ONLY_SCHEMA_VERSION
            else "verified" if payload.get("snapshot_complete") else "degraded"
        ),
        "source": str(payload.get("source") or CURRENT_ONLY_SOURCE),
        "announced_at": str(payload.get("announced_at") or ""),
        "published_at": str(payload.get("published_at") or ""),
        "first_observed_at": str(payload.get("first_observed_at") or ""),
        "effective_session_date": str(payload.get("effective_session_date") or ""),
        "expected_symbols": payload.get("expected_symbols"),
        "observed_symbols": payload.get("observed_symbols"),
        "missing_symbols": list(payload.get("missing_symbols") or []),
        "content_hash": str(payload.get("content_sha256") or ""),
        "universe_evidence": dict(payload.get("universe_evidence") or {}),
    }


def load_industry_analysis_context(
    *, as_of: str | None = None, mode: IndustryReadMode = "formal",
) -> tuple[dict[str, str], dict[str, object]]:
    """Return the best PIT-safe mapping for analysis plus its formal eligibility.

    A stale or partial local mapping can still label an exploratory result.  It
    must never be silently promoted to a production input, so callers receive a
    separate ``formal_eligible`` flag and the strict-load failure reason.
    Historical calls only use snapshots that already pass the target-date
    cutoff; they never fall back to today's projection.
    """
    try:
        mapping = load_industry_map(as_of=as_of, mode=mode)
        evidence = load_industry_evidence(as_of=as_of)
        return mapping, {
            **evidence,
            "formal_eligible": evidence.get("status") in {"verified", "trusted"},
            "issues": [],
        }
    except (
        FileNotFoundError,
        IndustrySnapshotIncomplete,
        IndustrySnapshotIntegrityError,
        OSError,
        RuntimeError,
        TypeError,
        ValueError,
    ):
        logger.exception("严格行业证据加载失败，降级到研究预览")
        strict_issue = (
            "正式行业证据不可用；结果已降级为研究预览，详细信息已写入本机日志"
        )
    try:
        mapping = load_cached_industry_map(as_of=as_of, mode=mode)
    except (
        FileNotFoundError,
        IndustrySnapshotIncomplete,
        IndustrySnapshotIntegrityError,
        OSError,
        RuntimeError,
        TypeError,
        ValueError,
    ):
        mapping = {}
    try:
        evidence = load_industry_evidence(as_of=as_of)
    except (
        FileNotFoundError,
        IndustrySnapshotIncomplete,
        IndustrySnapshotIntegrityError,
        OSError,
        RuntimeError,
        TypeError,
        ValueError,
    ):
        evidence = {
            "source": "unavailable",
            "announced_at": "",
            "published_at": "",
            "first_observed_at": "",
            "effective_session_date": str(as_of or ""),
            "expected_symbols": None,
            "observed_symbols": len(mapping),
            "missing_symbols": [],
            "content_hash": "",
            "universe_evidence": {},
        }
    return mapping, {
        **evidence,
        "status": "degraded",
        "formal_eligible": False,
        "issues": [
            strict_issue,
        ],
        "preview_symbols": len(mapping),
    }
