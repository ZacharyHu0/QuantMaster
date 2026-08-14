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


def test_backtest_cli_maps_to_shared_execution_without_a_job_store(
    monkeypatch, capsys,
) -> None:
    observed = []

    def execute_backtest(spec, **kwargs):
        observed.append((spec, kwargs))
        return {
            "manifest": {
                "research_tier": "sandbox",
                "formal_eligible": False,
                "warnings": [{"code": "fixed_universe"}],
            },
            "summary": {},
            "artifact": {
                "metrics": {"total_return": 0.125},
                "yearly": {"2023": 0.125},
                "monthly": {"2023": {"1": 0.01}},
            },
        }

    monkeypatch.setattr(
        "quantmaster.backtest.application.execute_backtest", execute_backtest,
    )
    args = build_parser().parse_args([
        "backtest", "--factor", "ts_corr(rank(volume), rank(close), 20), mom_20d",
        "--universe", "demo", "--start", "2023-01-02", "--end", "2023-07-28",
        "--top", "4", "--rebalance", "M", "--weighting", "ic",
        "--capital", "200000", "--stop-loss", "0.08", "--take-profit", "0.25",
    ])

    assert args.func(args) is None
    spec, kwargs = observed[0]
    assert spec.model_dump(mode="json") == {
        "name": "",
        "strategy": {
            "kind": "factor",
            "factor": "ts_corr(rank(volume), rank(close), 20), mom_20d",
            "top_n": 4,
            "rebalance": "M",
            "weighting": "ic",
            "cap_weight": 0.35,
        },
        "universe": "demo",
        "start": "2023-01-02",
        "end": "2023-07-28",
        "benchmark": "000300.SH",
        "initial_capital": 200000.0,
        "stop_loss": 0.08,
        "take_profit": 0.25,
        "allow_partial": False,
        "research_tier": "auto",
    }
    assert kwargs == {}
    assert json.loads(capsys.readouterr().out) == {
        "total_return": 0.125,
        "research_tier": "sandbox",
        "formal_eligible": False,
        "warnings": [{"code": "fixed_universe"}],
    }
