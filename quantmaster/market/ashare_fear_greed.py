"""FundDB A 股恐贪指数的公开数据适配与本地快照。"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import logging
import os
import tempfile
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pandas as pd

from quantmaster.config import get_config
from quantmaster.data.resilience import (
    ProviderCapabilityMissing,
    ProviderContractChanged,
    provider_call,
)

FUNDB_FEAR_GREED_URL = "https://www.funddb.cn/tool/fear"
FUNDB_API_URL = "https://api.jiucaishuo.com/v2/kjtl/kjtlconnect"
ASHARE_FEAR_GREED_SYMBOLS = ("上证指数", "沪深300")
CACHE_TTL_SECONDS = 24 * 60 * 60
REFRESH_RETRY_SECONDS = 60
MAX_CACHE_BYTES = 2 * 1024 * 1024
MAX_HISTORY_POINTS = 370
RSI_ADD_THRESHOLD = 22.0
FEAR_GREED_RARE_THRESHOLD = 10.0

_CACHE_LOCK = threading.Lock()
_REFRESH_LOCK = threading.Lock()
logger = logging.getLogger(__name__)

_RATING_BANDS = (
    (25.0, "extreme_fear", "极度恐惧"),
    (45.0, "fear", "恐惧"),
    (55.0, "neutral", "中立"),
    (75.0, "greed", "贪婪"),
    (101.0, "extreme_greed", "极度贪婪"),
)
_CACHE_NAMES = {
    "上证指数": "ashare_fear_greed_sh.json",
    "沪深300": "ashare_fear_greed_hs300.json",
}
_FUNDB_SYMBOL_CODES = {"上证指数": "000001.SH", "沪深300": "000300.SH"}
_FUNDB_VERSION = "2.2.7"
_FUNDB_REQUEST_KEY = "EWf45rlv#kfsr@k#gfksgkr"
_FUNDB_AES_KEY = "eveqocftukbotqjcequcnkrqlw1oi"
_FUNDB_AES_IV = "bvroqevdjqibsdkq"


def _symbol_label(symbol: str) -> str:
    if symbol not in _CACHE_NAMES:
        supported = "、".join(ASHARE_FEAR_GREED_SYMBOLS)
        raise ValueError(f"不支持的 A 股恐贪口径: {symbol}（可选：{supported}）")
    return symbol


def _cache_path(symbol: str) -> Path:
    return Path(get_config().data.root) / "cache" / _CACHE_NAMES[_symbol_label(symbol)]


def _rating(score: float) -> tuple[str, str]:
    for upper, code, label in _RATING_BANDS:
        if score < upper:
            return code, label
    return "extreme_greed", "极度贪婪"


def _thresholds() -> dict[str, float]:
    return {
        "rsi_add": RSI_ADD_THRESHOLD,
        "fear_greed_rare": FEAR_GREED_RARE_THRESHOLD,
    }


def parse_ashare_fear_greed(
    payload: object,
    *,
    symbol: str,
) -> dict[str, object]:
    """校验 FundDB 数据并收敛为稳定的小型契约。"""

    symbol = _symbol_label(symbol)
    if not isinstance(payload, pd.DataFrame):
        raise ValueError("A 股恐贪响应不是 DataFrame")
    required = {"date", "fear", "index"}
    missing = sorted(required.difference(str(column) for column in payload.columns))
    if missing:
        raise ValueError(f"A 股恐贪响应缺少字段: {', '.join(missing)}")
    if payload.empty:
        raise ValueError("A 股恐贪响应为空")

    frame = payload.loc[:, ["date", "fear", "index"]].copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["score"] = pd.to_numeric(frame["fear"], errors="coerce")
    frame["benchmark"] = pd.to_numeric(frame["index"], errors="coerce")
    if frame[["date", "score", "benchmark"]].isna().any().any():
        raise ValueError("A 股恐贪响应包含无效日期或数值")
    if not frame["score"].between(0, 100).all():
        raise ValueError("A 股恐贪分数超出 0-100")
    if not (frame["benchmark"] > 0).all():
        raise ValueError("A 股恐贪响应缺少有效指数点位")
    if frame["date"].duplicated().any():
        raise ValueError("A 股恐贪响应包含重复日期")

    frame = frame.sort_values("date", kind="stable")
    history = [
        {
            "date": point.date().isoformat(),
            "score": round(float(score), 2),
            "benchmark": round(float(benchmark), 2),
        }
        for point, score, benchmark in zip(
            frame["date"].iloc[-MAX_HISTORY_POINTS:],
            frame["score"].iloc[-MAX_HISTORY_POINTS:],
            frame["benchmark"].iloc[-MAX_HISTORY_POINTS:],
            strict=True,
        )
    ]
    current = history[-1]
    score = float(current["score"])
    rating, rating_label = _rating(score)
    return {
        "status": "ready",
        "symbol": symbol,
        "symbol_label": symbol,
        "score": current["score"],
        "rating": rating,
        "rating_label": rating_label,
        "benchmark_value": current["benchmark"],
        "benchmark_label": f"{symbol}点位",
        "as_of": current["date"],
        "history": history,
        "fetched_at": datetime.now(UTC).isoformat(),
        "source": "FundDB A 股恐贪指数（公开接口）",
        "source_url": FUNDB_FEAR_GREED_URL,
        "scope": "A 股市场情绪；分数和指数点位采用 FundDB 口径，不代表自动交易信号。",
        "thresholds": _thresholds(),
    }


def _read_cache(path: Path, symbol: str) -> dict[str, Any] | None:
    try:
        if not path.is_file() or path.stat().st_size > MAX_CACHE_BYTES:
            return None
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict) or value.get("symbol") != symbol:
        return None
    if value.get("status") != "ready" or value.get("score") is None:
        return None
    if not isinstance(value.get("history"), list) or value.get("benchmark_value") is None:
        return None
    return value


def _cache_is_fresh(value: dict[str, Any]) -> bool:
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


def _unavailable(symbol: str, warning: str | None = None) -> dict[str, object]:
    symbol = _symbol_label(symbol)
    return {
        "status": "unavailable",
        "symbol": symbol,
        "symbol_label": symbol,
        "score": None,
        "rating": "",
        "rating_label": "暂不可用",
        "benchmark_value": None,
        "benchmark_label": f"{symbol}点位",
        "as_of": "",
        "history": [],
        "source": "FundDB A 股恐贪指数（公开接口）",
        "source_url": FUNDB_FEAR_GREED_URL,
        "scope": "A 股市场情绪；分数和指数点位采用 FundDB 口径，不代表自动交易信号。",
        "warning": warning or "A 股恐贪指数暂不可达；RSI 仍可独立使用。",
        "thresholds": _thresholds(),
    }


def read_ashare_fear_greed(
    symbol: str,
) -> dict[str, object]:
    """只读最近的本地 A 股恐贪快照；不会在页面请求中访问网络。"""

    symbol = _symbol_label(symbol)
    with _CACHE_LOCK:
        cached = _read_cache(_cache_path(symbol), symbol)
    if cached is None:
        return _unavailable(symbol)
    if _cache_is_fresh(cached):
        return dict(cached)
    return {
        **cached,
        "status": "stale",
        "warning": "A 股恐贪指数快照已过期；后台刷新完成前正在展示最近一次本地结果。",
    }


def _funddb_signed_body(data: dict[str, object]) -> dict[str, object]:
    """Build the request fields required by FundDB's public web client."""

    body = {
        **data,
        "type": "pc",
        "version": _FUNDB_VERSION,
        "authtoken": "",
        "act_time": int(time.time() * 1000),
    }
    signing_text = "".join(
        str(body[key])
        for key in sorted(body)
        if body[key] not in (None, "", False) and not isinstance(body[key], (dict, list))
    )
    # This MD5 is FundDB's upstream request signature, not a cache/integrity hash.
    digest = hashlib.md5(
        (signing_text + _FUNDB_REQUEST_KEY).encode("utf-8"),
        usedforsecurity=False,
    ).hexdigest()
    parts = (
        digest[29:31], digest[2:4], digest[5], digest[26], digest[6:8], digest[1],
        digest[0:2], digest[6:8], digest[8], digest[30], digest[11:14], digest[11],
        digest[2:5], digest[9:11], digest[23:25], digest[31], digest[25:27],
        digest[9:11], digest[27:29], digest[17:19], digest[26], digest[12:14],
        digest[25], digest[16:19], digest[17:21], digest[18], digest[21:23],
        digest[14:16], digest[29:32], digest[21:23], digest[24:26], digest[16],
    )
    names = (
        "e", "n", "a", "i", "o", "r", "u", "l", "c", "s", "d", "_", "f",
        "h", "p", "m", "g", "v", "y", "b", "k", "w", "x", "j", "P", "z",
        "q", "E", "H", "O", "A", "C",
    )
    values = dict(zip(names, parts, strict=True))
    fields = {
        "tirgkjfs": "f", "abiokytke": "_", "u54rg5d": "e", "kf54ge7": "q",
        "tiklsktr4": "d", "lksytkjh": "P", "sbnoywr": "z", "bgd7h8tyu54": "w",
        "y654b5fs3tr": "O", "bioduytlw": "n", "bd4uy742": "j", "h67456y": "r",
        "bvytikwqjk": "s", "ngd4uy551": "b", "bgiuytkw": "v", "nd354uy4752": "g",
        "ghtoiutkmlg": "x", "bd24y6421f": "a", "tbvdiuytk": "u", "ibvytiqjek": "p",
        "jnhf8u5231": "C", "fjlkatj": "A", "hy5641d321t": "E", "iogojti": "o",
        "ngd4yut78": "i", "nkjhrew": "c", "yt447e13f": "H", "n3bf4uj7y7": "k",
        "nbf4uj7y432": "h", "yi854tew": "l", "h13ey474": "m", "quikgdky": "y",
    }
    body.update({key: values[name] for key, name in fields.items()})
    return body


def _funddb_decrypt(payload: object) -> object:
    if not isinstance(payload, str):
        return payload
    try:
        from Crypto.Cipher import AES

        encrypted = base64.b64decode(payload, validate=True)
        cipher = AES.new(
            (_FUNDB_AES_KEY + "ll1").encode("utf-8"),
            AES.MODE_CBC,
            (_FUNDB_AES_IV + "ll1")[:16].encode("utf-8"),
        )
        padded = cipher.decrypt(encrypted)
        padding = padded[-1]
        # FundDB's browser client removes the repeated trailing byte literally;
        # its current response uses a value larger than one AES block.
        if not 1 <= padding <= len(padded) or padded[-padding:] != bytes([padding]) * padding:
            raise ValueError("invalid FundDB response padding")
        return json.loads(padded[:-padding].decode("utf-8"))
    except (
        ImportError,
        IndexError,
        binascii.Error,
        ValueError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        if isinstance(exc, ImportError):
            raise ProviderCapabilityMissing("缺少 FundDB 响应解密依赖") from exc
        raise ProviderContractChanged("FundDB 恐贪响应无法解密") from exc


def _funddb_payload_to_frame(payload: object, *, symbol: str) -> pd.DataFrame:
    decoded = _funddb_decrypt(payload)
    if not isinstance(decoded, dict) or decoded.get("code") != 0:
        message = decoded.get("message", "未知错误") if isinstance(decoded, dict) else "响应不是对象"
        raise ProviderContractChanged(f"FundDB 恐贪响应失败: {message}")
    data = decoded.get("data")
    x_axis = data.get("xAxis") if isinstance(data, dict) else None
    categories = x_axis.get("categories") if isinstance(x_axis, dict) else None
    series = data.get("series") if isinstance(data, dict) else None
    if not isinstance(categories, list) or not isinstance(series, list):
        raise ProviderContractChanged("FundDB 恐贪响应缺少图表序列")
    fear_series = next(
        (
            item
            for item in series
            if isinstance(item, dict) and str(item.get("name", "")).startswith("恐惧贪婪")
        ),
        None,
    )
    benchmark_series = next(
        (item for item in series if isinstance(item, dict) and str(item.get("name", "")).startswith(symbol)),
        None,
    )
    scores = fear_series.get("data") if isinstance(fear_series, dict) else None
    benchmarks = benchmark_series.get("data") if isinstance(benchmark_series, dict) else None
    if not isinstance(scores, list) or not isinstance(benchmarks, list):
        raise ProviderContractChanged("FundDB 恐贪响应缺少分数或指数序列")
    if not len(categories) == len(scores) == len(benchmarks):
        raise ProviderContractChanged("FundDB 恐贪图表序列长度不一致")
    return pd.DataFrame({"date": categories, "fear": scores, "index": benchmarks})


def _fetch_ashare_fear_greed(symbol: str) -> dict[str, object]:
    try:
        code = _FUNDB_SYMBOL_CODES[_symbol_label(symbol)]
    except KeyError as exc:
        raise ValueError(f"不支持的 FundDB 恐贪指数口径: {symbol}") from exc

    def request() -> object:
        body = _funddb_signed_body({"gu_code": code, "time": -1})
        with httpx.Client(timeout=30.0, follow_redirects=False) as client:
            response = client.post(
                FUNDB_API_URL,
                json=body,
                headers={
                    "Origin": "https://www.funddb.cn",
                    "Referer": FUNDB_FEAR_GREED_URL,
                },
            )
            response.raise_for_status()
            return response.json()

    payload = provider_call(
        "funddb",
        f"kjtlconnect:{code}",
        request,
    )
    return parse_ashare_fear_greed(
        _funddb_payload_to_frame(payload, symbol=symbol),
        symbol=symbol,
    )


def load_ashare_fear_greed(
    symbol: str,
    *,
    force: bool = False,
) -> dict[str, object]:
    """读取 A 股恐贪指数；日缓存优先，失败时安全降级到最近成功值。"""

    symbol = _symbol_label(symbol)
    path = _cache_path(symbol)
    with _CACHE_LOCK:
        cached = _read_cache(path, symbol)
    if cached is not None and not force and _cache_is_fresh(cached):
        return dict(cached)
    with _REFRESH_LOCK:
        # 后台刷新可能在本调用等待期间完成，避免重复请求。
        with _CACHE_LOCK:
            cached = _read_cache(path, symbol)
        if cached is not None and not force and _cache_is_fresh(cached):
            return dict(cached)
        try:
            result = _fetch_ashare_fear_greed(symbol)
            try:
                with _CACHE_LOCK:
                    _write_cache(path, result)
            except OSError as exc:
                logger.debug("A 股恐贪缓存写入失败: %s", exc)
            return result
        except (
            ArithmeticError,
            AttributeError,
            ImportError,
            LookupError,
            OSError,
            httpx.HTTPError,
            RuntimeError,
            TypeError,
            ValueError,
        ) as exc:
            logger.info("A 股恐贪指数刷新失败（%s）: %s", symbol, exc)
            if cached is not None:
                return {
                    **cached,
                    "status": "stale",
                    "warning": "A 股恐贪指数刷新失败，正在使用最近一次本地缓存。",
                }
            return _unavailable(symbol)


def _refresh_all_ashare_fear_greed() -> dict[str, object]:
    results = {
        symbol: load_ashare_fear_greed(symbol)
        for symbol in ASHARE_FEAR_GREED_SYMBOLS
    }
    ready = all(result.get("status") == "ready" for result in results.values())
    return {"status": "ready" if ready else "stale", "results": results}


class AShareFearGreedRefresher:
    """在持久后台执行器中刷新两个 A 股恐贪口径。"""

    def __init__(
        self,
        refresh: Callable[[], dict[str, object]] | None = None,
        *,
        interval_seconds: float = CACHE_TTL_SECONDS,
        retry_seconds: float = REFRESH_RETRY_SECONDS,
    ) -> None:
        self._refresh = refresh or _refresh_all_ashare_fear_greed
        self._interval_seconds = interval_seconds
        self._retry_seconds = retry_seconds
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> bool:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return False
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run,
                name="ashare-fear-greed-refresh",
                daemon=True,
            )
            self._thread.start()
            return True

    def stop(self, timeout: float = 10.0) -> None:
        with self._lock:
            thread = self._thread
            self._stop.set()
        if thread is not None:
            thread.join(timeout=timeout)
        with self._lock:
            if self._thread is thread and (thread is None or not thread.is_alive()):
                self._thread = None

    def _run(self) -> None:
        while not self._stop.is_set():
            started = time.monotonic()
            try:
                result = self._refresh()
                ready = result.get("status") == "ready"
            except (OSError, RuntimeError, TypeError, ValueError):
                ready = False
                logger.exception("A 股恐贪后台刷新异常")
            delay = self._interval_seconds if ready else self._retry_seconds
            elapsed = time.monotonic() - started
            if self._stop.wait(max(0.0, delay - elapsed)):
                return


_REFRESHER = AShareFearGreedRefresher()


def get_ashare_fear_greed_refresher() -> AShareFearGreedRefresher:
    return _REFRESHER
