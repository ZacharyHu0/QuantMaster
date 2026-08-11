"""CNN Fear & Greed 市场背景与 RSI 机会分级。"""

from __future__ import annotations

import json
import logging
import math
import os
import tempfile
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import httpx

from quantmaster.config import get_config

CNN_GRAPH_URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
CNN_PAGE_URL = "https://edition.cnn.com/markets/fear-and-greed"
CACHE_TTL_SECONDS = 30 * 60
MAX_RESPONSE_BYTES = 512 * 1024
MAX_HISTORY_POINTS = 370
RSI_ADD_THRESHOLD = 22.0
FEAR_GREED_RARE_THRESHOLD = 10.0

_CACHE_LOCK = threading.Lock()
logger = logging.getLogger(__name__)

_RATING_LABELS = {
    "extreme fear": "极度恐惧",
    "fear": "恐惧",
    "neutral": "中性",
    "greed": "贪婪",
    "extreme greed": "极度贪婪",
}


def classify_opportunity(rsi: object, fear_greed: object = None) -> dict[str, object]:
    """按用户经验阈值分级；结果是提示，不是自动交易指令。"""

    rsi_value = _float_or_nan(rsi)
    fear_value = _float_or_nan(fear_greed)

    if not math.isfinite(rsi_value):
        code, label = "unavailable", "RSI 暂缺"
    elif rsi_value < RSI_ADD_THRESHOLD and (
        math.isfinite(fear_value) and fear_value < FEAR_GREED_RARE_THRESHOLD
    ):
        code, label = "rare_bottom", "罕见大底机会"
    elif rsi_value < RSI_ADD_THRESHOLD:
        code, label = "rsi_oversold", "加仓抄底观察"
    else:
        code, label = "neutral", "暂无极端信号"
    return {
        "code": code,
        "label": label,
        "rsi_threshold": RSI_ADD_THRESHOLD,
        "fear_greed_threshold": FEAR_GREED_RARE_THRESHOLD,
    }


def _float_or_nan(value: object) -> float:
    try:
        return float(cast(Any, value))
    except (TypeError, ValueError):
        return math.nan


def _cache_path() -> Path:
    return Path(get_config().data.root) / "cache" / "cnn_fear_greed.json"


def _iso_timestamp(value: object) -> str:
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        seconds = float(value) / 1000 if float(value) > 10_000_000_000 else float(value)
        return datetime.fromtimestamp(seconds, tz=UTC).isoformat()
    return str(value or "")[:80]


def _history(payload: dict[str, object]) -> list[dict[str, object]]:
    historical = payload.get("fear_and_greed_historical")
    if not isinstance(historical, dict) or not isinstance(historical.get("data"), list):
        return []
    points: list[dict[str, object]] = []
    for raw in historical["data"][-MAX_HISTORY_POINTS:]:
        if not isinstance(raw, dict):
            continue
        score = _float_or_nan(raw.get("y"))
        if not math.isfinite(score) or not 0 <= score <= 100:
            continue
        rating = str(raw.get("rating") or "").strip().lower()[:40]
        points.append(
            {
                "date": _iso_timestamp(raw.get("x"))[:10],
                "score": round(score, 2),
                "rating": rating,
                "rating_label": _RATING_LABELS.get(rating, rating or "未分类"),
            }
        )
    return points


def parse_cnn_fear_greed(payload: object) -> dict[str, object]:
    """校验 CNN graphdata 响应并收敛为稳定的小型契约。"""
    if not isinstance(payload, dict):
        raise ValueError("CNN 恐贪响应不是对象")
    current = payload.get("fear_and_greed")
    if not isinstance(current, dict):
        raise ValueError("CNN 恐贪响应缺少当前值")
    score = _float_or_nan(current.get("score"))
    if not math.isfinite(score):
        raise ValueError("CNN 恐贪响应缺少有效分数") from None
    if not 0 <= score <= 100:
        raise ValueError("CNN 恐贪分数超出 0-100")
    rating = str(current.get("rating") or "").strip().lower()[:40]
    return {
        "status": "ready",
        "score": round(score, 2),
        "rating": rating,
        "rating_label": _RATING_LABELS.get(rating, rating or "未分类"),
        "as_of": _iso_timestamp(current.get("timestamp")),
        "history": _history(payload),
        "fetched_at": datetime.now(UTC).isoformat(),
        "source": "CNN Fear & Greed Index",
        "source_url": CNN_PAGE_URL,
        "scope": "美国市场风险情绪（作为全球背景参考，不代表 A 股或具体板块）",
        "thresholds": {
            "rsi_add": RSI_ADD_THRESHOLD,
            "fear_greed_rare": FEAR_GREED_RARE_THRESHOLD,
        },
    }


def _read_cache(path: Path) -> dict[str, Any] | None:
    try:
        if not path.is_file() or path.stat().st_size > MAX_RESPONSE_BYTES:
            return None
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) and value.get("score") is not None else None


def _cache_is_fresh(value: dict[str, Any]) -> bool:
    if not isinstance(value.get("history"), list):
        return False
    try:
        fetched_at = datetime.fromisoformat(str(value["fetched_at"]).replace("Z", "+00:00"))
    except (KeyError, TypeError, ValueError):
        return False
    if fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=UTC)
    return (datetime.now(UTC) - fetched_at).total_seconds() < CACHE_TTL_SECONDS


def _write_cache(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
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


def _unavailable() -> dict[str, object]:
    return {
        "status": "unavailable",
        "score": None,
        "rating": "",
        "rating_label": "暂不可用",
        "as_of": "",
        "history": [],
        "source": "CNN Fear & Greed Index",
        "source_url": CNN_PAGE_URL,
        "scope": "美国市场风险情绪（作为全球背景参考，不代表 A 股或具体板块）",
        "warning": "CNN 指数暂不可达；RSI 仍可独立使用。",
        "thresholds": {
            "rsi_add": RSI_ADD_THRESHOLD,
            "fear_greed_rare": FEAR_GREED_RARE_THRESHOLD,
        },
    }


def read_cnn_fear_greed() -> dict[str, object]:
    """Return only the last local CNN snapshot; never make an HTTP request."""

    with _CACHE_LOCK:
        cached = _read_cache(_cache_path())
    if cached is None:
        return _unavailable()
    if _cache_is_fresh(cached):
        return dict(cached)
    return {
        **cached,
        "status": "stale",
        "warning": "CNN 指数快照已过期；后台刷新完成前正在展示最近一次本地结果。",
    }


def load_cnn_fear_greed(*, force: bool = False) -> dict[str, object]:
    """读取 CNN 当前指数；短缓存优先，失败时安全降级到最近成功值。"""
    path = _cache_path()
    with _CACHE_LOCK:
        cached = _read_cache(path)
        if cached is not None and not force and _cache_is_fresh(cached):
            return dict(cached)
        try:
            response = httpx.get(
                CNN_GRAPH_URL,
                timeout=8.0,
                follow_redirects=True,
                headers={
                    "Accept": "application/json",
                    "Referer": "https://edition.cnn.com/",
                    "User-Agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/131.0.0.0 Safari/537.36"
                    ),
                },
            )
            response.raise_for_status()
            if len(response.content) > MAX_RESPONSE_BYTES:
                raise ValueError("CNN 恐贪响应过大")
            result = parse_cnn_fear_greed(response.json())
            try:
                _write_cache(path, result)
            except OSError as exc:
                logger.debug("CNN 恐贪缓存写入失败: %s", exc)
            return result
        except (httpx.HTTPError, ValueError) as exc:
            logger.info("CNN 恐贪指数刷新失败: %s", exc)
            if cached is not None:
                return {
                    **cached,
                    "status": "stale",
                    "warning": "CNN 指数刷新失败，正在使用最近一次本地缓存。",
                }
            return _unavailable()
