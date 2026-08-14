from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import pytest

from quantmaster.cli import build_parser

REMOVED_RESEARCH_COMMANDS = ("validate", "grid", "fund-test", "mine", "mine-llm")
SUPPORTED_RESEARCH_COMMANDS = ("factor-test", "backtest", "lab")
ROOT = Path(__file__).resolve().parents[1]
REMOVED_INVOCATION = re.compile(
    r"(?m)(?:^\s*qm\s+(?:validate|grid|fund-test|mine(?:-llm)?)(?=\s|$)"
    r"|`qm\s+(?:validate|grid|fund-test|mine(?:-llm)?)(?=[\s`]|$)[^`]*`)"
)

def test_top_level_research_command_choices_match_the_supported_interface() -> None:
    parser = build_parser()
    subparsers = next(
        action for action in parser._actions if isinstance(action, argparse._SubParsersAction)
    )
    commands = set(subparsers.choices)

    assert commands.isdisjoint(REMOVED_RESEARCH_COMMANDS)
    assert commands.issuperset(SUPPORTED_RESEARCH_COMMANDS)


def test_removed_top_level_research_command_is_rejected_by_argparse() -> None:
    with pytest.raises(SystemExit) as error:
        build_parser().parse_args(["mine", "--help"])

    assert error.value.code == 2


def test_current_markdown_only_advertises_supported_research_commands() -> None:
    markdown = []
    for path in ROOT.rglob("*.md"):
        relative = path.relative_to(ROOT)
        if path.name == "CHANGELOG.md" or {".artifacts", ".worktrees"} & set(relative.parts):
            continue
        markdown.append(path.read_text(encoding="utf-8"))

    assert REMOVED_INVOCATION.search("\n".join(markdown)) is None


def test_markdown_contract_catches_command_lines_and_inline_code() -> None:
    for markdown in ("  qm fund-test ep", "Use `qm mine-llm --rounds 2` here."):
        assert REMOVED_INVOCATION.search(markdown) is not None


def test_supported_research_command_remains_available() -> None:
    with pytest.raises(SystemExit) as result:
        build_parser().parse_args(["lab", "--help"])

    assert result.value.code == 0


def test_factor_test_cli_only_parses_and_renders_the_shared_result(monkeypatch, capsys) -> None:
    observed = []

    def run_factor_test(**kwargs):
        observed.append(kwargs)
        return {
            "summary": {"name": "mom_20d", "ic_mean": 0.0312},
            "universe_evidence": {"formal_eligible": False},
            "data_quality": {"status": "degraded"},
            "neutralized": False,
            "industry_evidence": {"status": "degraded"},
        }

    monkeypatch.setattr("quantmaster.factors.run_factor_test", run_factor_test)
    args = build_parser().parse_args([
        "factor-test",
        "mom_20d",
        "--universe", "csi800",
        "--start", "2023-01-02",
        "--end", "2023-07-28",
        "--quantiles", "4",
        "--neutralize",
    ])

    assert args.func(args) is None
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {"name": "mom_20d", "ic_mean": 0.0312}
    assert captured.err.splitlines() == [
        "⚠️ Sandbox：结果不可进入正式研究",
        "⚠️ 行情数据已降级",
        "⚠️ 行业中性化未执行",
        "⚠️ 行业证据已降级",
    ]
    assert observed == [{
        "expression": "mom_20d",
        "universe": "csi800",
        "start": "2023-01-02",
        "end": "2023-07-28",
        "quantiles": 4,
        "neutralize": True,
        "refresh": True,
    }]
