"""终端摘要、完整日志与 CLI 诊断入口测试。"""

from __future__ import annotations

import argparse
import io
import json
import logging
import threading

import pytest

from quantmaster import cli
from quantmaster import logging_config as qm_logging


@pytest.fixture(autouse=True)
def restore_logging_state():
    root = logging.getLogger()
    old_level = root.level
    old_thread_hook = threading.excepthook
    yield
    qm_logging.shutdown_logging()
    root.setLevel(old_level)
    threading.excepthook = old_thread_hook
    logging.captureWarnings(False)


def _application_failure():
    namespace = {"__name__": "quantmaster.fake_component"}
    exec(compile(
        "def fail():\n    raise RuntimeError('请求失败 token=very-secret-value')\n",
        "quantmaster/fake_component.py", "exec",
    ), namespace)
    namespace["fail"]()


def _isolated_logger(name: str, handler: logging.Handler) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.handlers = [handler]
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    return logger


def test_console_shows_key_frame_then_folds_repeated_traceback(tmp_path):
    stream = io.StringIO()
    clock = iter((1.0, 2.0, 603.0))
    handler = qm_logging.CompactConsoleHandler(
        level=logging.INFO, verbose=False,
        log_path=tmp_path / "logs" / "quantmaster.log",
        stream=stream, force_terminal=False, clock=lambda: next(clock),
    )
    logger = _isolated_logger("quantmaster.automation.service", handler)

    for _ in range(3):
        try:
            _application_failure()
        except RuntimeError:
            logger.exception("盘中监控失败")

    output = stream.getvalue()
    assert "盘中监控失败" in output
    assert "RuntimeError: 请求失败 token=***" in output
    assert "fake_component.py:2" in output
    assert output.count("↳") == 2
    assert "同类错误 10 分钟内第 2 次" in output
    assert "very-secret-value" not in output
    assert "\x1b[" not in output


def test_verbose_console_expands_every_traceback_and_redacts(tmp_path):
    stream = io.StringIO()
    handler = qm_logging.CompactConsoleHandler(
        level=logging.DEBUG, verbose=True,
        log_path=tmp_path / "logs" / "quantmaster.log",
        stream=stream, force_terminal=False,
    )
    logger = _isolated_logger("quantmaster.lab.worker", handler)

    for _ in range(2):
        try:
            _application_failure()
        except RuntimeError:
            logger.exception("研究任务失败")

    output = stream.getvalue()
    assert output.count("Traceback (most recent call last)") == 2
    assert "token=***" in output
    assert "very-secret-value" not in output
    assert "同类错误" not in output


def test_configured_logging_separates_stdout_and_writes_full_file(tmp_path, capsys):
    qm_logging.configure_logging(data_root=tmp_path)
    try:
        _application_failure()
    except RuntimeError:
        logging.getLogger("quantmaster.server.app").exception("接口处理失败")
    print('{"status":"ok"}')

    captured = capsys.readouterr()
    assert captured.out == '{"status":"ok"}\n'
    assert "接口处理失败" in captured.err
    assert "Traceback (most recent call last)" not in captured.err

    qm_logging.shutdown_logging()
    content = next((tmp_path / "logs").glob("quantmaster-*.log")).read_text(encoding="utf-8")
    assert "Traceback (most recent call last)" in content
    assert "fake_component.py" in content
    assert '"thread":"MainThread"' in content
    assert "token=***" in content
    assert "very-secret-value" not in content


def test_log_file_rotates_and_configuration_is_idempotent(tmp_path, monkeypatch):
    monkeypatch.setattr(qm_logging, "LOG_MAX_BYTES", 240)
    qm_logging.configure_logging(data_root=tmp_path)
    qm_logging.configure_logging(data_root=tmp_path)
    owned = [handler for handler in logging.getLogger().handlers
             if getattr(handler, "_quantmaster_owned", False)]
    assert len(owned) == 2

    logger = logging.getLogger("quantmaster.data.registry")
    for index in range(20):
        logger.info("轮转测试 %s %s", index, "x" * 80)
    for handler in owned:
        handler.flush()

    log_path = next((tmp_path / "logs").glob("quantmaster-*.log"))
    assert log_path.is_file()
    assert log_path.with_name(log_path.name + ".1.gz").is_file()


def test_structured_file_record_deeply_redacts_and_keeps_runtime_ids(tmp_path):
    qm_logging.configure_logging(data_root=tmp_path)
    logging.getLogger("httpx").warning(
        "request %s", {"Authorization": "Bearer private", "nested": {
            "cookie": "session=private", "url": "https://user:pass@host/x?token=private",
        }}, extra={"event": "provider_request_failed", "error_code": "tls_failed",
                  "diagnostic_id": "diag-1", "operation_id": "op-1"},
    )
    qm_logging.shutdown_logging()
    document = json.loads(next((tmp_path / "logs").glob("quantmaster-*.log")).read_text(encoding="utf-8"))
    assert document["event"] == "provider_request_failed"
    assert document["error_code"] == "tls_failed"
    assert document["diagnostic_id"] == "diag-1"
    assert document["operation_id"] == "op-1"
    assert "private" not in str(document)


def test_structured_file_suppresses_repeated_tracebacks_and_keeps_items_separate(tmp_path):
    qm_logging.configure_logging(data_root=tmp_path)
    logger = logging.getLogger("quantmaster.data.registry")
    for item_id in ("600000.SH", "600000.SH", "000001.SZ"):
        try:
            raise RuntimeError("provider unavailable")
        except RuntimeError:
            logger.exception(
                "行情失败", extra={"event": "bar_load_failed", "error_code": "provider_timeout",
                                   "item_type": "symbol", "item_id": item_id},
            )
    qm_logging.shutdown_logging()
    documents = [json.loads(line) for line in next(
        (tmp_path / "logs").glob("quantmaster-*.log")
    ).read_text(encoding="utf-8").splitlines()]
    assert len(documents) == 2
    assert sum("traceback" in document for document in documents) == 2
    assert {document["item_id"] for document in documents} == {"600000.SH", "000001.SZ"}


def test_log_level_environment_and_verbose_precedence(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("QM_LOG_LEVEL", "ERROR")
    state = qm_logging.configure_logging(data_root=tmp_path)
    assert state.level == logging.ERROR
    assert state.verbose is False

    state = qm_logging.configure_logging(verbose=True, data_root=tmp_path)
    assert state.level == logging.DEBUG
    assert state.verbose is True

    monkeypatch.setenv("QM_LOG_LEVEL", "invalid")
    state = qm_logging.configure_logging(data_root=tmp_path)
    assert state.level == logging.INFO
    assert "QM_LOG_LEVEL=INVALID 无效" in capsys.readouterr().err


def test_unwritable_log_path_falls_back_to_full_terminal(tmp_path, capsys):
    blocked_root = tmp_path / "blocked"
    blocked_root.write_text("not a directory", encoding="utf-8")
    state = qm_logging.configure_logging(data_root=blocked_root)
    assert state.log_path is None

    try:
        _application_failure()
    except RuntimeError:
        logging.getLogger("quantmaster.cli").exception("命令失败")

    output = capsys.readouterr().err
    assert "日志文件不可用" in output
    assert "Traceback (most recent call last)" in output
    assert "token=***" in output
    assert "very-secret-value" not in output


def test_verbose_flag_is_accepted_anywhere_and_respects_separator():
    assert cli._extract_verbose(["--verbose", "serve"]) == (["serve"], True)
    assert cli._extract_verbose(["lab", "worker", "--verbose"]) == (
        ["lab", "worker"], True,
    )
    assert cli._extract_verbose(["factor-test", "--", "--verbose"]) == (
        ["factor-test", "--", "--verbose"], False,
    )


def test_cli_returns_one_and_logs_uncaught_error(tmp_path, monkeypatch, capsys):
    def parser() -> argparse.ArgumentParser:
        value = argparse.ArgumentParser(prog="qm-test")
        sub = value.add_subparsers(dest="command", required=True)
        command = sub.add_parser("boom")

        def fail(_args):
            raise RuntimeError("boom token=very-secret-value")

        command.set_defaults(func=fail)
        return value

    real_configure = qm_logging.configure_logging
    monkeypatch.setattr(cli, "build_parser", parser)
    monkeypatch.setattr(
        qm_logging, "configure_logging",
        lambda *, verbose=False: real_configure(verbose=verbose, data_root=tmp_path),
    )

    assert cli.main(["boom", "--verbose"]) == 1
    output = capsys.readouterr().err
    assert "命令执行失败" in output
    assert "Traceback (most recent call last)" in output
    assert "token=***" in output
    assert "very-secret-value" not in output


def test_third_party_logger_handler_is_removed():
    logger = logging.getLogger("Lark")
    logger.handlers = [logging.StreamHandler(io.StringIO())]
    logger.propagate = False

    qm_logging.normalize_third_party_logger("Lark")

    assert logger.handlers == []
    assert logger.propagate is True
    assert logger.level == logging.WARNING
