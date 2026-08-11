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

from quantmaster.config import get_config
from quantmaster.data.resilience import akshare_call
from quantmaster.trading_sessions import daily_signal_cutoff, market_date

logger = logging.getLogger(__name__)

CACHE_TTL_DAYS = 1


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


def _verified_industry_payload(path: Path, *, history: bool = False) -> dict:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise IndustrySnapshotIntegrityError(f"行业快照不可读: {path.name}") from exc
    if not isinstance(payload, dict):
        raise IndustrySnapshotIntegrityError(f"行业快照结构非法: {path.name}")
    expected = str(payload.get("content_sha256") or "")
    if payload.get("schema_version") != 2 or not expected:
        raise LegacyIndustrySnapshotError(f"行业快照为旧格式: {path.name}")
    actual = _industry_payload_hash(payload)
    if expected != actual:
        raise IndustrySnapshotIntegrityError(f"行业快照内容哈希失败: {path.name}")
    if history and path.stem.rsplit("--", 1)[-1] != expected:
        raise IndustrySnapshotIntegrityError(f"行业快照文件名哈希失败: {path.name}")
    if payload.get("snapshot_complete"):
        effective = str(payload.get("effective_as_of") or "")
        evidence = dict(payload.get("universe_evidence") or {})
        if str(evidence.get("as_of") or "") != effective:
            raise IndustrySnapshotIntegrityError(
                f"行业快照与证券目录 as_of 不一致: {path.name}"
            )
        try:
            catalog_acquired = datetime.fromisoformat(
                str(evidence.get("acquired_at") or "").replace("Z", "+00:00")
            )
            industry_observed = datetime.fromisoformat(
                str(payload.get("observed_at") or "").replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise IndustrySnapshotIntegrityError(
                f"行业快照时间证据非法: {path.name}"
            ) from exc
        if (
            catalog_acquired.tzinfo is None
            or industry_observed.tzinfo is None
            or catalog_acquired > industry_observed
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
    mapping: dict[str, str], *, effective_as_of: str | date | None = None,
    observed_at: str | datetime | None = None,
    snapshot_complete: bool = True,
    source: str = "manual",
    expected_symbols: int | None = None,
    missing_symbols: list[str] | tuple[str, ...] = (),
    universe_evidence: dict[str, object] | None = None,
) -> None:
    """Persist a dated immutable observation as well as the current pointer."""
    effective = (
        effective_as_of.isoformat()
        if isinstance(effective_as_of, date)
        else str(effective_as_of or market_date().isoformat())
    )
    try:
        effective = date.fromisoformat(effective).isoformat()
    except ValueError as exc:
        raise ValueError("行业快照日期需要使用 YYYY-MM-DD 格式") from exc
    if observed_at is None:
        observed = datetime.now(UTC)
    elif isinstance(observed_at, datetime):
        observed = observed_at
    else:
        try:
            observed = datetime.fromisoformat(str(observed_at).replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("行业快照 observed_at 需要使用带时区的 ISO 时间") from exc
    if observed.tzinfo is None:
        raise ValueError("行业快照 observed_at 必须包含时区")
    observed = observed.astimezone(UTC)
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
            raise IndustrySnapshotIncomplete("行业 effective_as_of 与证券目录 as_of 不一致")
        try:
            catalog_acquired = datetime.fromisoformat(
                str((universe_evidence or {}).get("acquired_at") or "").replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise IndustrySnapshotIncomplete("证券目录 acquired_at 非法") from exc
        if catalog_acquired.tzinfo is None or catalog_acquired.astimezone(UTC) > observed:
            raise IndustrySnapshotIncomplete("行业 observed_at 早于证券目录 acquired_at")
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
        "updated_at": observed.timestamp(),
        "observed_at": observed.isoformat(),
        "effective_as_of": effective,
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
            effective = date.fromisoformat(str(payload.get("effective_as_of") or ""))
            observed = datetime.fromisoformat(
                str(payload.get("observed_at") or "").replace("Z", "+00:00")
            )
            if observed.tzinfo is None:
                continue
            observed = observed.astimezone(UTC)
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
        f"{target.isoformat()} 上海 15:00 前没有带 observed_at 的行业分类快照；"
        "拒绝用当前行业映射重算历史结果"
    )


def load_industry_map(
    refresh: bool = False, *, as_of: str | None = None,
) -> dict[str, str]:
    """Read current classification or a strictly dated historical snapshot."""
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
            active_symbols = _active_cn_symbols()
            missing_current = active_symbols - set(cached)
            complete = bool(
                data.get("snapshot_complete")
                and active_symbols
                and not missing_current
            )
            age_seconds = time.time() - float(data.get("updated_at") or 0)
            fresh = (
                complete
                and 0 <= age_seconds < CACHE_TTL_DAYS * 86400
            )
        except LegacyIndustrySnapshotError:
            # A pre-v2 projection is useful only as a degraded live preview.
            # Current refresh treats it as no formal cache and replaces it only
            # after a complete provider observation passes the new contract.
            pass
        except IndustrySnapshotIntegrityError:
            raise
    if cached and fresh and not refresh:
        return cached
    try:
        fetched = fetch_industry_map()
        if isinstance(fetched, tuple):
            mapping, provider_complete, source = fetched
        else:
            mapping = fetched
            provider_complete = True
            source = "provider:unverified-completeness"
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


def load_cached_industry_map(*, as_of: str | None = None) -> dict[str, str]:
    """只读本地白名单缓存；AutoMiner 构造特征时绝不隐式触网。"""
    if as_of:
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
        "status": "verified" if payload.get("snapshot_complete") else "degraded",
        "source": str(payload.get("source") or "unknown"),
        "observed_at": str(payload.get("observed_at") or ""),
        "effective_as_of": str(payload.get("effective_as_of") or ""),
        "expected_symbols": payload.get("expected_symbols"),
        "observed_symbols": payload.get("observed_symbols"),
        "missing_symbols": list(payload.get("missing_symbols") or []),
        "content_hash": str(payload.get("content_sha256") or ""),
        "universe_evidence": dict(payload.get("universe_evidence") or {}),
    }


def load_industry_analysis_context(
    *, as_of: str | None = None,
) -> tuple[dict[str, str], dict[str, object]]:
    """Return the best PIT-safe mapping for analysis plus its formal eligibility.

    A stale or partial local mapping can still label an exploratory result.  It
    must never be silently promoted to a production input, so callers receive a
    separate ``formal_eligible`` flag and the strict-load failure reason.
    Historical calls only use snapshots that already pass the target-date
    cutoff; they never fall back to today's projection.
    """
    try:
        mapping = load_industry_map(as_of=as_of)
        evidence = load_industry_evidence(as_of=as_of)
        return mapping, {
            **evidence,
            "formal_eligible": evidence.get("status") == "verified",
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
    legacy_preview = False
    try:
        mapping = load_cached_industry_map(as_of=as_of)
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
    if not mapping and as_of is None:
        # Personal installations can have a large pre-v2 mapping that remains
        # useful for exploratory sector labels.  Read it only for the live
        # preview tier; historical/PIT and formal consumers never accept it.
        try:
            raw_preview = json.loads(_cache_path().read_text(encoding="utf-8"))
            raw_mapping = raw_preview.get("mapping") if isinstance(raw_preview, dict) else None
            if (
                isinstance(raw_preview, dict)
                and raw_preview.get("schema_version") != 2
                and isinstance(raw_mapping, dict)
            ):
                mapping = {
                    str(key): str(value)
                    for key, value in raw_mapping.items()
                    if str(key) and str(value)
                }
                legacy_preview = bool(mapping)
        except (AttributeError, FileNotFoundError, json.JSONDecodeError, OSError, TypeError):
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
            "source": "legacy-local-preview" if legacy_preview else "unavailable",
            "observed_at": "",
            "effective_as_of": str(as_of or ""),
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
            *(["旧行业映射仅用于当前 sandbox 标签"] if legacy_preview else []),
        ],
        "preview_symbols": len(mapping),
    }
