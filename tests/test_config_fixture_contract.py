"""Contracts for opting a module out of per-test Config replacement."""

from __future__ import annotations

import os

import pytest

pytest_plugins = ("pytester",)


def test_default_config_is_per_test_but_marker_keeps_one_module_config(pytester):
    pytester.makeini("[pytest]\nmarkers = module_isolated_config")
    pytester.makeconftest('pytest_plugins = ("tests.conftest",)')
    pytester.makepyfile(
        test_default="""
        from quantmaster.config import get_config

        roots = []

        def test_first():
            roots.append(str(get_config().data_root))

        def test_second():
            assert str(get_config().data_root) not in roots
        """,
        test_module="""
        import pytest
        from quantmaster.config import Config, get_config, set_config

        pytestmark = pytest.mark.module_isolated_config

        @pytest.fixture(scope="module", autouse=True)
        def fixed_config(tmp_path_factory):
            cfg = Config()
            cfg.data.root = str(tmp_path_factory.mktemp("fixed") / "data")
            set_config(cfg)
            yield cfg
            set_config(None)

        roots = []

        def test_first():
            roots.append(str(get_config().data_root))

        def test_second():
            assert str(get_config().data_root) == roots[0]
        """,
    )
    result = pytester.runpytest("-p", "scripts.dev.pytest_windows_acl")
    assert result.ret == pytest.ExitCode.OK
    result.assert_outcomes(passed=4)
    if os.name == "nt":
        from quantmaster.runtime.storage_governance import inspect_acl

        assert inspect_acl(pytester.path.parent / "basetemp").inherited is True
