from __future__ import annotations

import os
import re
import subprocess
import sys
import time
from pathlib import Path

import pytest

REMOVED_RESEARCH_COMMANDS = ("validate", "grid", "fund-test", "mine", "mine-llm")
SUPPORTED_RESEARCH_HELP = (
    ("factor-test", "--help"),
    ("backtest", "--help"),
    ("lab", "--help"),
)
ROOT = Path(__file__).resolve().parents[1]


def _qm(*args: str) -> subprocess.CompletedProcess[str]:
    environment = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    return subprocess.run(
        [sys.executable, "-m", "quantmaster.cli", *args],
        capture_output=True,
        check=False,
        env=environment,
        text=True,
        timeout=10,
    )


@pytest.mark.parametrize("command", REMOVED_RESEARCH_COMMANDS)
def test_removed_top_level_research_commands_are_unknown(command: str) -> None:
    result = _qm(command, "--help")

    assert result.returncode == 2
    assert "invalid choice" in result.stderr


def test_help_and_guides_only_advertise_supported_research_commands() -> None:
    help_result = _qm("--help")
    tracked_guidance = "\n".join(
        [
            (ROOT / "quantmaster" / "cli.py").read_text(encoding="utf-8"),
            (ROOT / "README.md").read_text(encoding="utf-8"),
            (ROOT / "docs" / "guide.md").read_text(encoding="utf-8"),
        ]
    )

    assert help_result.returncode == 0
    for command in REMOVED_RESEARCH_COMMANDS:
        assert f"\n    {command}" not in help_result.stdout
    assert re.search(
        r"(?m)^\s*qm\s+(?:validate|grid|fund-test|mine(?:-llm)?)\b",
        tracked_guidance,
    ) is None


@pytest.mark.parametrize("args", SUPPORTED_RESEARCH_HELP)
def test_supported_research_commands_remain_available(args: tuple[str, ...]) -> None:
    result = _qm(*args)

    assert result.returncode == 0


def test_cli_help_stays_within_startup_budget() -> None:
    started = time.perf_counter()
    result = _qm("--help")
    elapsed = time.perf_counter() - started

    assert result.returncode == 0
    assert elapsed <= 1.5
