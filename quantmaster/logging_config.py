"""QuantMaster 的终端与文件日志配置。

终端只承担快速判断和定位，完整异常始终写入轮转文件。模块保持在 CLI
入口层使用，业务模块继续使用标准库 ``logging.getLogger(__name__)``。
"""

from __future__ import annotations

import gzip
import json
import logging
import logging.handlers
import os
import re
import shutil
import sys
import threading
import time
import traceback
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import TracebackType
from typing import Literal

LOG_FILENAME = "quantmaster.log"
LOG_MAX_BYTES = 20 * 1024 * 1024
LOG_BACKUP_COUNT = 7
LOG_TOTAL_MAX_BYTES = 200 * 1024 * 1024
REPEAT_WINDOW_SECONDS = 10 * 60
MAX_REPEAT_FINGERPRINTS = 256
MAX_CONSOLE_MESSAGE = 700
MAX_KEY_FRAMES = 4
MAX_VERBOSE_FRAMES = 50

TracebackPolicy = Literal["always", "first", "never"]

_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "WARN": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
    "FATAL": logging.CRITICAL,
}
_LEVEL_LABELS = {
    logging.DEBUG: "DEBUG",
    logging.INFO: "INFO",
    logging.WARNING: "WARN",
    logging.ERROR: "ERROR",
    logging.CRITICAL: "FATAL",
}
_LEVEL_STYLES = {
    logging.DEBUG: "dim cyan",
    logging.INFO: "cyan",
    logging.WARNING: "yellow",
    logging.ERROR: "bold red",
    logging.CRITICAL: "bold white on red",
}
_SECRET_PATTERNS = (
    (
        re.compile(
            r"(?i)((?:api[_-]?key|token|authorization|app[_-]?secret|secret|password)"
            r"\s*[=:]\s*)[^\s,;]+"
        ),
        r"\1***",
    ),
    (re.compile(r"(?i)(bearer\s+)[^\s,;]+"), r"\1***"),
    (re.compile(r"\bsk-[A-Za-z0-9_-]{8,}\b"), "sk-***"),
    (re.compile(r"(?i)((?:cookie|set-cookie|x-api-key)\s*[:=]\s*)[^\r\n]+"), r"\1***"),
    (re.compile(r"(?i)([?&](?:token|api_key|apikey|key|secret|password|signature)=)[^&#\s]+"), r"\1***"),
    (re.compile(r"(?i)(://[^:/\s]+:)[^@/\s]+(@)"), r"\1***\2"),
)
_LOCAL_PATH_PATTERN = re.compile(
    r"""(?ix)
    (?<![A-Z0-9_])
    (?:
        [A-Z]:[\\/]+[^,\s"'<>]+
        |\\\\[^,\s"'<>]+(?:[\\/]+[^,\s"'<>]+)+
        |/(?:Users|home|private|var/folders|tmp|runner|workspace)
            (?:/[^,\s"'<>]+)+
    )
    """
)
_SECRET_FIELD = re.compile(
    r"(?i)(?:api.?key|authorization|cookie|app.?secret|tenant.*token|token|"
    r"password|passwd|credential|database.?url|dsn|prompt|model.?response|response.?body)"
)
_CONFIG_LOCK = threading.RLock()
_RECORD_FACTORY = logging.getLogRecordFactory()
_factory_installed = False


@dataclass(frozen=True)
class LoggingState:
    """当前进程的日志状态，供启动摘要和测试读取。"""

    level: int
    verbose: bool
    log_path: Path | None


@dataclass
class _RepeatState:
    first_seen: float
    last_seen: float
    count: int
    affected_items: set[str] | None = None


@dataclass(frozen=True)
class _FrameInfo:
    filename: str
    lineno: int
    function: str
    module: str
    application: bool


_state = LoggingState(level=logging.INFO, verbose=False, log_path=None)


def redact_sensitive_text(value: object) -> str:
    """遮蔽凭据与本地绝对路径，避免诊断文本越过公开边界。"""
    text = str(value)
    text = _LOCAL_PATH_PATTERN.sub("<local-path>", text)
    for pattern, replacement in _SECRET_PATTERNS:
        text = pattern.sub(replacement, text)
    return text


def redact_sensitive_value(value: object, *, key: str = "", depth: int = 0) -> object:
    """Recursively redact values before any QuantMaster handler sees them."""
    if _SECRET_FIELD.search(key):
        return "***"
    if depth >= 8:
        return "<max-depth>"
    if isinstance(value, str):
        return redact_sensitive_text(value)
    if isinstance(value, Mapping):
        return {
            redact_sensitive_text(name): redact_sensitive_value(item, key=str(name), depth=depth + 1)
            for name, item in value.items()
        }
    if isinstance(value, tuple):
        return tuple(redact_sensitive_value(item, depth=depth + 1) for item in value)
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return [redact_sensitive_value(item, depth=depth + 1) for item in value]
    if isinstance(value, (bytes, bytearray)):
        return f"<{type(value).__name__}:{len(value)} bytes>"
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return redact_sensitive_text(value)


def _install_redacting_record_factory() -> None:
    global _factory_installed
    if _factory_installed:
        return

    def factory(*args, **kwargs):
        record = _RECORD_FACTORY(*args, **kwargs)
        record.msg = redact_sensitive_value(record.msg)
        record.args = redact_sensitive_value(record.args)
        return record

    logging.setLogRecordFactory(factory)
    _factory_installed = True


def current_logging_state() -> LoggingState:
    return _state


def current_log_path() -> Path | None:
    return _state.log_path


def is_verbose_logging() -> bool:
    return _state.verbose


def _compact_text(value: object, limit: int = MAX_CONSOLE_MESSAGE) -> str:
    text = redact_sensitive_text(value)
    text = " ↵ ".join(part.strip() for part in text.splitlines() if part.strip())
    if len(text) > limit:
        return text[: limit - 1] + "…"
    return text


def _component(logger_name: str) -> str:
    if logger_name == "Lark" or logger_name.startswith("lark"):
        return "feishu"
    if logger_name.startswith("uvicorn"):
        return "server"
    if logger_name.startswith("apscheduler"):
        return "scheduler"
    if logger_name == "py.warnings":
        return "python"
    if logger_name.startswith("quantmaster."):
        return logger_name.split(".", 2)[1]
    return logger_name.split(".", 1)[0] or "runtime"


def _exception_chain(exc: BaseException) -> list[BaseException]:
    chain: list[BaseException] = []
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        chain.append(current)
        seen.add(id(current))
        if current.__cause__ is not None:
            current = current.__cause__
        elif not current.__suppress_context__:
            current = current.__context__
        else:
            current = None
    return chain


def _root_exception(exc: BaseException) -> BaseException:
    return _exception_chain(exc)[-1]


def _frames(
    exc: BaseException, fallback_traceback: TracebackType | None = None,
) -> list[_FrameInfo]:
    result: list[_FrameInfo] = []
    for index, item in enumerate(_exception_chain(exc)):
        tb = item.__traceback__
        if tb is None and index == 0:
            tb = fallback_traceback
        if tb is None:
            continue
        for frame, lineno in traceback.walk_tb(tb):
            module = str(frame.f_globals.get("__name__", ""))
            result.append(_FrameInfo(
                filename=frame.f_code.co_filename,
                lineno=lineno,
                function=frame.f_code.co_name,
                module=module,
                application=module == "quantmaster" or module.startswith("quantmaster."),
            ))
    return result


def _display_frame(frame: _FrameInfo) -> str:
    path = Path(frame.filename)
    if frame.application:
        try:
            path = path.resolve().relative_to(Path(__file__).resolve().parent)
        except (OSError, ValueError):
            pass
    return f"{path}:{frame.lineno}  {frame.function}"


def _key_frames(
    exc: BaseException, fallback_traceback: TracebackType | None = None,
) -> list[_FrameInfo]:
    frames = _frames(exc, fallback_traceback)
    application = [frame for frame in frames if frame.application]
    return (application[-MAX_KEY_FRAMES:] if application else frames[-2:])


def _fingerprint(
    record: logging.LogRecord,
    exc: BaseException,
    fallback_traceback: TracebackType | None,
) -> tuple[str, str, str, int, str]:
    root = _root_exception(exc)
    frames = _frames(exc, fallback_traceback)
    application = [frame for frame in frames if frame.application]
    deepest = (application or frames)[-1] if frames else None
    return (
        record.name,
        f"{type(root).__module__}.{type(root).__qualname__}",
        deepest.filename if deepest else "",
        deepest.lineno if deepest else 0,
        _compact_text(root, limit=300),
    )


def _format_full_traceback(
    exc_info: tuple[type[BaseException], BaseException, TracebackType | None],
) -> str:
    rendered = "".join(traceback.TracebackException(
        exc_info[0], exc_info[1], exc_info[2],
        limit=MAX_VERBOSE_FRAMES, capture_locals=False,
    ).format(chain=True))
    return redact_sensitive_text(rendered).rstrip()


def _coerce_exc_info(
    value: object,
) -> tuple[type[BaseException], BaseException, TracebackType | None] | None:
    if not isinstance(value, tuple) or len(value) != 3:
        return None
    exc_type, exc, tb = value
    if not isinstance(exc, BaseException) or not isinstance(exc_type, type):
        return None
    return exc_type, exc, tb if isinstance(tb, TracebackType) else None


def _display_log_path(path: Path | None) -> str:
    if path is None:
        return ""
    try:
        return Path(os.path.relpath(path, Path.cwd())).as_posix()
    except ValueError:
        return str(path)


class RedactingFileFormatter(logging.Formatter):
    """Emit one redacted JSON document per record for safe aggregation."""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        return datetime.fromtimestamp(record.created).astimezone().isoformat(timespec="milliseconds")

    def format(self, record: logging.LogRecord) -> str:
        document = {
            "timestamp": self.formatTime(record),
            "level": _LEVEL_LABELS.get(record.levelno, record.levelname),
            "logger": record.name,
            "component": getattr(record, "component", _component(record.name)),
            "event": getattr(record, "event", "log_record"),
            "error_code": getattr(record, "error_code", ""),
            "diagnostic_id": getattr(record, "diagnostic_id", ""),
            "operation_id": getattr(record, "operation_id", ""),
            "item_type": getattr(record, "item_type", ""),
            "item_id": getattr(record, "item_id", ""),
            "symbol": getattr(record, "symbol", ""),
            "business_date": getattr(record, "business_date", ""),
            "attempt": getattr(record, "attempt", None),
            "retryable": getattr(record, "retryable", None),
            "next_retry_at": getattr(record, "next_retry_at", ""),
            "impact": getattr(record, "impact", ""),
            "first_seen_at": getattr(record, "first_seen_at", ""),
            "last_seen_at": getattr(record, "last_seen_at", ""),
            "suppressed_count": getattr(record, "suppressed_count", 0),
            "affected_count": getattr(record, "affected_count", 0),
            "affected_items": getattr(record, "affected_items", []),
            "pid": record.process,
            "thread": record.threadName,
            "message": record.getMessage(),
        }
        if record.exc_info:
            exc_info = _coerce_exc_info(record.exc_info)
            if exc_info:
                document["traceback"] = _format_full_traceback(exc_info)
        safe = redact_sensitive_value(document)
        return json.dumps(safe, ensure_ascii=False, separators=(",", ":"), default=str)


class _ProcessSafeRotatingFileHandler(logging.handlers.RotatingFileHandler):
    """Rotate a process-owned file and enforce a bounded shared log directory."""

    def __init__(self, *args, clock=time.time, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.clock = clock
        self._repeat: dict[tuple[object, ...], _RepeatState] = {}
        self.namer = lambda name: name + ".gz"
        self.rotator = self._compress

    @staticmethod
    def _compress(source: str, destination: str) -> None:
        temporary = destination + ".tmp"
        with open(source, "rb") as raw, gzip.open(temporary, "wb") as compressed:
            shutil.copyfileobj(raw, compressed)
        os.replace(temporary, destination)
        os.unlink(source)

    def handleError(self, record: logging.LogRecord) -> None:
        """Never recurse through logging when rotation or disk writes fail."""
        try:
            emergency = Path(self.baseFilename).parent / "emergency"
            emergency.mkdir(parents=True, exist_ok=True)
            path = emergency / f"quantmaster-{os.getpid()}.log"
            message = redact_sensitive_text(record.getMessage())[:1000]
            with path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps({
                    "timestamp": datetime.now().astimezone().isoformat(),
                    "event": "log_write_failed", "level": record.levelname,
                    "logger": record.name, "message": message,
                }, ensure_ascii=False) + "\n")
            sys.stderr.write("QuantMaster 日志写入失败；已切换脱敏应急日志\n")
        except OSError:
            try:
                sys.stderr.write("QuantMaster 日志与应急日志均不可写\n")
            except OSError:
                pass

    @staticmethod
    def _structured_signature(record: logging.LogRecord) -> tuple[object, ...]:
        exc_info = _coerce_exc_info(record.exc_info)
        exc_type = ""
        frame = ""
        if exc_info:
            root = _root_exception(exc_info[1])
            exc_type = f"{type(root).__module__}.{type(root).__qualname__}"
            frames = _frames(exc_info[1], exc_info[2])
            deepest = frames[-1] if frames else None
            frame = f"{deepest.filename}:{deepest.lineno}" if deepest else ""
        error_code = getattr(record, "error_code", "")
        return (
            getattr(record, "event", "log_record"),
            getattr(record, "component", _component(record.name)),
            error_code,
            getattr(record, "item_type", ""),
            getattr(record, "item_id", ""),
            getattr(record, "symbol", ""),
            getattr(record, "business_date", ""),
            getattr(record, "security_event_kind", ""),
            getattr(record, "corruption_id", ""),
            getattr(record, "source", ""),
            getattr(record, "lane", ""),
            exc_type,
            frame,
            "" if error_code else _compact_text(record.getMessage(), 300),
            record.name,
        )

    def _summary(self, record: logging.LogRecord, state: _RepeatState) -> logging.LogRecord:
        summary = logging.makeLogRecord({
            "name": record.name, "levelno": record.levelno, "levelname": record.levelname,
            "msg": "同类问题聚合摘要", "args": (), "exc_info": None,
            "event": "problem_repeat_summary",
            "component": getattr(record, "component", _component(record.name)),
            "error_code": getattr(record, "error_code", ""),
            "diagnostic_id": getattr(record, "diagnostic_id", ""),
            "operation_id": getattr(record, "operation_id", ""),
            "first_seen_at": datetime.fromtimestamp(state.first_seen).astimezone().isoformat(),
            "last_seen_at": datetime.fromtimestamp(state.last_seen).astimezone().isoformat(),
            "suppressed_count": state.count - 1,
            "affected_count": len(state.affected_items or ()),
            "affected_items": sorted(state.affected_items or ())[:20],
        })
        return summary

    def emit(self, record: logging.LogRecord) -> None:
        policy = getattr(record, "traceback_policy", "first")
        if record.exc_info and policy == "first":
            now = self.clock()
            signature = self._structured_signature(record)
            state = self._repeat.get(signature)
            item = str(getattr(record, "item_id", "") or getattr(record, "symbol", ""))
            if state is not None and now - state.last_seen < REPEAT_WINDOW_SECONDS:
                state.last_seen = now
                state.count += 1
                if item and state.affected_items is not None:
                    state.affected_items.add(item)
                if state.count == 10 or state.count % 50 == 0:
                    super().emit(self._summary(record, state))
                return
            if state is not None and state.count > 1:
                super().emit(self._summary(record, state))
            self._repeat[signature] = _RepeatState(
                first_seen=now, last_seen=now, count=1,
                affected_items={item} if item else set(),
            )
        elif getattr(record, "state", "") == "recovered":
            signature = self._structured_signature(record)
            state = self._repeat.pop(signature, None)
            if state is not None and state.count > 1:
                super().emit(self._summary(record, state))
        super().emit(record)

    def doRollover(self) -> None:
        super().doRollover()
        self._enforce_total_cap()

    def _enforce_total_cap(self) -> None:
        directory = Path(self.baseFilename).parent
        candidates = sorted(
            (path for path in directory.glob("*.log*") if path.is_file()),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        total = 0
        for path in candidates:
            try:
                size = path.stat().st_size
                total += size
                if total > LOG_TOTAL_MAX_BYTES and path != Path(self.baseFilename):
                    path.unlink(missing_ok=True)
            except OSError:
                continue


class CompactConsoleHandler(logging.Handler):
    """串行渲染紧凑终端记录，并按异常指纹折叠重复栈。"""

    def __init__(
        self,
        *,
        level: int,
        verbose: bool,
        log_path: Path | None,
        stream=None,
        force_terminal: bool | None = None,
        clock=time.monotonic,
    ) -> None:
        super().__init__(level=level)
        self.verbose = verbose
        self.log_path = log_path
        self.stream = stream or sys.stderr
        self.clock = clock
        self._repeat: dict[tuple[str, str, str, int, str], _RepeatState] = {}
        self._rich_console = None
        self._rich_text = None
        self._rich_syntax = None
        try:
            from rich.console import Console
            from rich.syntax import Syntax
            from rich.text import Text

            self._rich_console = Console(
                file=self.stream, force_terminal=force_terminal,
                color_system="auto", highlight=False,
            )
            self._rich_text = Text
            self._rich_syntax = Syntax
        except ImportError:  # pragma: no cover - rich 是正式依赖，仅保留安全降级
            pass

    def _repeat_count(
        self,
        record: logging.LogRecord,
        exc: BaseException,
        tb: TracebackType | None,
    ) -> int:
        now = self.clock()
        expired = [key for key, state in self._repeat.items()
                   if now - state.last_seen >= REPEAT_WINDOW_SECONDS]
        for key in expired:
            self._repeat.pop(key, None)
        if len(self._repeat) >= MAX_REPEAT_FINGERPRINTS:
            oldest = min(self._repeat, key=lambda key: self._repeat[key].last_seen)
            self._repeat.pop(oldest, None)
        key = _fingerprint(record, exc, tb)
        state = self._repeat.get(key)
        if state is None:
            self._repeat[key] = _RepeatState(first_seen=now, last_seen=now, count=1)
            return 1
        state.last_seen = now
        state.count += 1
        return state.count

    def _header(
        self,
        record: logging.LogRecord,
        message: str,
        exc: BaseException | None,
        repeat_count: int,
    ):
        label = _LEVEL_LABELS.get(record.levelno, record.levelname)
        component = _component(record.name)
        if exc is not None:
            root = _root_exception(exc)
            root_text = _compact_text(root, limit=300)
            root_description = type(root).__name__ + (f": {root_text}" if root_text else "")
            message = f"{message} · {root_description}" if message else root_description
        if repeat_count > 1:
            message += (
                f" · 同类错误 10 分钟内第 {repeat_count} 次；完整 traceback 已记录"
            )
        prefix = f"{datetime.fromtimestamp(record.created).strftime('%H:%M:%S')}  "
        if self._rich_text is None:
            return f"{prefix}{label:<5} {_component(record.name):<12} {message}"
        text = self._rich_text()
        text.append(prefix, style="dim")
        text.append(f"{label:<5}", style=_LEVEL_STYLES.get(record.levelno, "bold"))
        text.append(" ")
        text.append(f"{component:<12}", style="dim")
        text.append(" ")
        text.append(message)
        return text

    def _write(self, value) -> None:
        if self._rich_console is not None:
            self._rich_console.print(value, soft_wrap=False)
        else:
            self.stream.write(str(value) + "\n")
            self.stream.flush()

    def _write_detail(self, text: str, *, path: bool = False) -> None:
        prefix = " " * 29
        if self._rich_text is None:
            self._write(prefix + text)
            return
        value = self._rich_text(prefix)
        value.append(text, style="cyan" if path else "dim")
        self._write(value)

    def _write_exception(
        self,
        exc_info: tuple[type[BaseException], BaseException, TracebackType | None],
        policy: TracebackPolicy,
        repeat_count: int,
    ) -> None:
        exc, tb = exc_info[1], exc_info[2]
        if self.verbose or self.log_path is None:
            rendered = _format_full_traceback(exc_info)
            if self._rich_syntax is not None:
                self._write(self._rich_syntax(
                    rendered, "pytb", word_wrap=True,
                    background_color="default", padding=(0, 0),
                ))
            else:
                self._write(rendered)
            return
        show_key_frames = policy == "always" or (policy == "first" and repeat_count == 1)
        if show_key_frames:
            for frame in _key_frames(exc, tb):
                self._write_detail("↳ " + _display_frame(frame), path=True)
        log_path = _display_log_path(self.log_path)
        if log_path and (show_key_frames or policy == "never"):
            self._write_detail(f"完整日志 → {log_path}")

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = _compact_text(record.getMessage())
            exc_info = _coerce_exc_info(record.exc_info)
            exc = exc_info[1] if exc_info else None
            tb = exc_info[2] if exc_info else None
            policy = getattr(record, "traceback_policy", "first")
            if policy not in {"always", "first", "never"}:
                policy = "first"
            repeat_count = 1
            if exc is not None and policy == "first" and not self.verbose and self.log_path:
                repeat_count = self._repeat_count(record, exc, tb)

            self._write(self._header(record, message, exc, repeat_count))
            if exc_info is None:
                return

            self._write_exception(exc_info, policy, repeat_count)
        except Exception:
            try:
                self.stream.write(
                    f"{record.levelname} {_compact_text(record.getMessage())}\n"
                )
                self.stream.flush()
            except Exception:  # pragma: no cover - 解释器关闭阶段
                pass


def normalize_third_party_logger(name: str, level: int = logging.WARNING) -> None:
    """移除第三方 SDK 自带 handler，使其统一写入 QuantMaster 日志。"""
    logger = logging.getLogger(name)
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()
    logger.setLevel(level)
    logger.propagate = True


def _resolve_level(verbose: bool) -> tuple[int, bool, str]:
    if verbose:
        return logging.DEBUG, True, ""
    raw = os.environ.get("QM_LOG_LEVEL", "INFO").strip().upper()
    level = _LEVELS.get(raw)
    if level is None:
        return logging.INFO, False, raw
    return level, level == logging.DEBUG, ""


def _thread_exception_hook(args: threading.ExceptHookArgs) -> None:
    if args.exc_type is SystemExit:
        return
    logging.getLogger("quantmaster.runtime").error(
        "后台线程 %s 未处理异常", args.thread.name if args.thread else "unknown",
        exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
        extra={"traceback_policy": "always"},
    )


def configure_logging(
    *, verbose: bool = False, data_root: Path | None = None,
) -> LoggingState:
    """为当前 CLI 进程配置统一日志；重复调用不会叠加自有 handler。"""
    global _state
    with _CONFIG_LOCK:
        _install_redacting_record_factory()
        level, resolved_verbose, invalid_level = _resolve_level(verbose)
        if data_root is None:
            from quantmaster.config import get_config

            data_root = get_config().data_root
        root = logging.getLogger()
        for handler in list(root.handlers):
            if getattr(handler, "_quantmaster_owned", False):
                root.removeHandler(handler)
                handler.close()

        role = "compute" if os.environ.get("QM_COMPUTE_CHILD") == "1" else (
            "web" if os.environ.get("QM_WEB_PROCESS") == "1" else "runtime"
        )
        session = os.environ.setdefault("QM_LOG_SESSION_ID", uuid.uuid4().hex[:12])
        filename = f"quantmaster-{role}-{session}-{os.getpid()}.log"
        log_path: Path | None = Path(data_root) / "logs" / filename
        file_handler: logging.Handler | None = None
        file_error = ""
        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            file_handler = _ProcessSafeRotatingFileHandler(
                log_path, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT,
                encoding="utf-8", delay=True,
            )
            file_handler.setLevel(level)
            file_handler.setFormatter(RedactingFileFormatter())
            file_handler._quantmaster_owned = True  # type: ignore[attr-defined]
        except OSError as exc:
            file_error = _compact_text(exc)
            log_path = None

        console_handler = CompactConsoleHandler(
            level=level, verbose=resolved_verbose, log_path=log_path,
        )
        console_handler._quantmaster_owned = True  # type: ignore[attr-defined]
        root.addHandler(console_handler)
        if file_handler is not None:
            root.addHandler(file_handler)
        root.setLevel(level)

        for name in (
            "apscheduler", "httpx", "httpcore", "websockets",
            "watchfiles", "python_multipart", "multipart", "yfinance",
        ):
            normalize_third_party_logger(
                name, logging.CRITICAL if name == "yfinance" else logging.WARNING,
            )
        for name in ("uvicorn", "uvicorn.error", "uvicorn.asgi"):
            normalize_third_party_logger(name, logging.WARNING)
        access_logger = logging.getLogger("uvicorn.access")
        normalize_third_party_logger("uvicorn.access", logging.CRITICAL)
        access_logger.propagate = False

        logging.captureWarnings(True)
        threading.excepthook = _thread_exception_hook
        _state = LoggingState(level=level, verbose=resolved_verbose, log_path=log_path)

        logger = logging.getLogger(__name__)
        if invalid_level:
            logger.warning(
                "QM_LOG_LEVEL=%s 无效，已使用 INFO；可选 DEBUG/INFO/WARNING/ERROR/CRITICAL",
                invalid_level,
            )
        if file_error:
            logger.warning(
                "日志文件不可用，将在终端保留完整 traceback: %s", file_error,
            )
        return _state


def shutdown_logging() -> None:
    """关闭 QuantMaster 自有 handler；主要用于测试和嵌入式调用。"""
    global _state
    with _CONFIG_LOCK:
        root = logging.getLogger()
        for handler in list(root.handlers):
            if getattr(handler, "_quantmaster_owned", False):
                root.removeHandler(handler)
                handler.close()
        _state = LoggingState(level=logging.INFO, verbose=False, log_path=None)
