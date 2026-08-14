from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

import quantmaster.config as config_module
from quantmaster.config import Config, load_config, set_config
from quantmaster.data.free_stockdb_runtime import FreeStockDBRuntime
from quantmaster.data.free_stockdb_source import resolve_free_stockdb_sdk_path
from quantmaster.settings import ConfigManager

_INSTANCE_ENV = ("QM_CONFIG_PATH", "QM_DATA_ROOT", "QM_FREE_STOCKDB_ROOT")


@pytest.fixture
def packaged_windows(tmp_path, monkeypatch):
    previous_paths = list(config_module.DEFAULT_CONFIG_PATHS)
    previous_defaults = config_module._installed_data_defaults
    monkeypatch.setattr(config_module, "_is_packaged_windows", lambda: True)
    monkeypatch.setenv("APPDATA", str(tmp_path / "roaming"))
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "local"))
    for name in _INSTANCE_ENV:
        monkeypatch.delenv(name, raising=False)
    yield
    config_module.DEFAULT_CONFIG_PATHS[:] = previous_paths
    config_module._installed_data_defaults = previous_defaults
    set_config(None)


def test_packaged_windows_defaults_are_external_without_creating_paths(
    tmp_path, monkeypatch, packaged_windows,
):
    appdata = tmp_path / "roaming"
    local_appdata = tmp_path / "local"
    home = tmp_path / "home"
    cwd = tmp_path / "cwd"
    home.mkdir()
    cwd.mkdir()
    (home / "config.yaml").write_text("data:\n  root: ignored-home\n", encoding="utf-8")
    (cwd / "config.yaml").write_text("data:\n  root: ignored-cwd\n", encoding="utf-8")
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.chdir(cwd)

    config_module.configure_installed_instance()

    expected_config = appdata / "QuantMaster" / "config.yaml"
    expected_data = local_appdata / "QuantMaster" / "data"
    expected_stockdb = local_appdata / "QuantMaster" / "runtime" / "free-stockdb"
    assert not any(name in os.environ for name in _INSTANCE_ENV)
    assert ConfigManager().path == expected_config
    cfg = load_config()
    assert cfg.data_root == expected_data
    assert cfg.free_stockdb_root == expected_stockdb
    assert not expected_config.parent.exists()
    assert not expected_data.exists()
    assert not expected_stockdb.exists()


def test_packaged_windows_yaml_and_absolute_overrides_win(
    tmp_path, monkeypatch, packaged_windows,
):
    default_config = tmp_path / "roaming" / "QuantMaster" / "config.yaml"
    default_config.parent.mkdir(parents=True)
    default_config.write_text(
        yaml.safe_dump({
            "data": {
                "root": "yaml-data",
                "free_stockdb_root": "yaml-runtime/free-stockdb",
            },
        }),
        encoding="utf-8",
    )
    overrides = {
        "QM_CONFIG_PATH": tmp_path / "custom" / "config.yaml",
        "QM_DATA_ROOT": tmp_path / "custom-data",
        "QM_FREE_STOCKDB_ROOT": tmp_path / "custom-runtime" / "free-stockdb",
    }
    config_module.configure_installed_instance()
    yaml_cfg = load_config()
    assert yaml_cfg.data_root == default_config.parent / "yaml-data"
    assert yaml_cfg.free_stockdb_root == default_config.parent / "yaml-runtime/free-stockdb"

    overrides["QM_CONFIG_PATH"].parent.mkdir(parents=True)
    overrides["QM_CONFIG_PATH"].write_text("{}", encoding="utf-8")
    for name, value in overrides.items():
        monkeypatch.setenv(name, str(value))

    cfg = load_config()

    assert {name: os.environ[name] for name in _INSTANCE_ENV} == {
        name: str(value) for name, value in overrides.items()
    }
    assert cfg.config_path == overrides["QM_CONFIG_PATH"]
    assert cfg.data_root == overrides["QM_DATA_ROOT"]
    assert cfg.free_stockdb_root == overrides["QM_FREE_STOCKDB_ROOT"]


@pytest.mark.parametrize("invalid_name", _INSTANCE_ENV)
def test_packaged_windows_rejects_relative_override_without_partial_defaults(
    tmp_path, monkeypatch, packaged_windows, invalid_name,
):
    monkeypatch.setenv(invalid_name, "relative/path")
    previous_paths = list(config_module.DEFAULT_CONFIG_PATHS)

    with pytest.raises(RuntimeError, match=invalid_name):
        config_module.configure_installed_instance()

    assert {
        name: os.environ[name] for name in _INSTANCE_ENV if name in os.environ
    } == {invalid_name: "relative/path"}
    assert config_module.DEFAULT_CONFIG_PATHS == previous_paths
    assert config_module._installed_data_defaults is None


@pytest.mark.parametrize("invalid_root", ["APPDATA", "LOCALAPPDATA"])
@pytest.mark.parametrize("value", [None, "relative/path"])
def test_packaged_windows_requires_absolute_default_roots(
    monkeypatch, packaged_windows, invalid_root, value,
):
    if value is None:
        monkeypatch.delenv(invalid_root)
    else:
        monkeypatch.setenv(invalid_root, value)
    previous_paths = list(config_module.DEFAULT_CONFIG_PATHS)

    with pytest.raises(RuntimeError, match=invalid_root):
        config_module.configure_installed_instance()

    assert not any(name in os.environ for name in _INSTANCE_ENV)
    assert config_module.DEFAULT_CONFIG_PATHS == previous_paths
    assert config_module._installed_data_defaults is None


def test_source_instance_path_bootstrap_is_noop(monkeypatch):
    previous_paths = list(config_module.DEFAULT_CONFIG_PATHS)
    monkeypatch.setattr(config_module, "_is_packaged_windows", lambda: False)

    config_module.configure_installed_instance()

    assert config_module.DEFAULT_CONFIG_PATHS == previous_paths
    assert config_module._installed_data_defaults is None


def test_packaged_help_does_not_create_installed_paths(tmp_path, packaged_windows):
    from quantmaster.cli import main

    config_module.configure_installed_instance()

    with pytest.raises(SystemExit) as stopped:
        main(["--help"])

    assert stopped.value.code == 0
    assert not (tmp_path / "roaming" / "QuantMaster").exists()
    assert not (tmp_path / "local" / "QuantMaster").exists()


def test_relative_instance_paths_are_stable_across_cwd(tmp_path, monkeypatch):
    monkeypatch.delenv("QM_DATA_ROOT", raising=False)
    monkeypatch.delenv("QM_FREE_STOCKDB_ROOT", raising=False)
    monkeypatch.delenv("QM_FREE_STOCKDB_SDK_PATH", raising=False)
    instance = tmp_path / "instance"
    config_path = instance / "config.yaml"
    config_path.parent.mkdir()
    config_path.write_text(yaml.safe_dump({
        "data": {
            "root": "data",
            "free_stockdb_root": "runtime/free-stockdb",
            "free_stockdb_sdk_path": "sdk",
        },
    }), encoding="utf-8")
    first_cwd = tmp_path / "first"
    second_cwd = tmp_path / "second"
    first_cwd.mkdir()
    second_cwd.mkdir()

    cfg = load_config(config_path)
    set_config(cfg)
    monkeypatch.chdir(first_cwd)
    first = (
        cfg.data_root,
        FreeStockDBRuntime._root(),
        FreeStockDBRuntime._vendor_cache_path(),
        resolve_free_stockdb_sdk_path(),
    )
    monkeypatch.chdir(second_cwd)
    second = (
        cfg.data_root,
        FreeStockDBRuntime._root(),
        FreeStockDBRuntime._vendor_cache_path(),
        resolve_free_stockdb_sdk_path(),
    )

    assert first == second
    assert cfg.data_root == instance / "data"
    assert FreeStockDBRuntime._root() == instance / "runtime" / "free-stockdb"
    assert FreeStockDBRuntime._vendor_cache_path() == instance / "data" / (
        "free_stockdb_vendor_notice.json"
    )
    assert resolve_free_stockdb_sdk_path() == instance / "sdk" / "stock_sdk.py"


def test_config_without_file_anchors_defaults_to_explicit_workspace(tmp_path):
    cfg = Config(workspace_root=tmp_path.resolve())

    assert cfg.data_root == tmp_path / "data"
    assert cfg.free_stockdb_root == tmp_path / "runtime" / "free-stockdb"


def test_worktree_default_does_not_search_home_or_parent_checkout(tmp_path, monkeypatch):
    workspace = tmp_path / "task-worktree"
    home = tmp_path / "home"
    parent = tmp_path / "primary"
    workspace.mkdir()
    (home / ".quantmaster").mkdir(parents=True)
    parent.mkdir()
    (home / ".quantmaster" / "config.yaml").write_text(
        yaml.safe_dump({"data": {"root": str(tmp_path / "home-data")}}),
        encoding="utf-8",
    )
    (parent / "config.yaml").write_text(
        yaml.safe_dump({"data": {"root": str(tmp_path / "primary-data")}}),
        encoding="utf-8",
    )
    monkeypatch.setattr("quantmaster.config.WORKSPACE_ROOT", workspace.resolve())
    monkeypatch.setattr(
        "quantmaster.config.DEFAULT_CONFIG_PATHS", [workspace.resolve() / "config.yaml"],
    )
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.chdir(parent)
    monkeypatch.delenv("QM_CONFIG_PATH", raising=False)
    monkeypatch.delenv("QM_DATA_ROOT", raising=False)

    cfg = load_config()

    assert cfg.config_path is None
    assert cfg.data_root == workspace / "data"
    assert cfg.data_root not in {tmp_path / "home-data", tmp_path / "primary-data"}


def test_explicit_qm_paths_remain_isolated_and_are_anchored_to_config(tmp_path, monkeypatch):
    instance = tmp_path / "instance"
    instance.mkdir()
    config_path = instance / "config.yaml"
    config_path.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("QM_CONFIG_PATH", str(config_path))
    monkeypatch.setenv("QM_DATA_ROOT", "task-data")
    monkeypatch.setenv("QM_FREE_STOCKDB_ROOT", "task-runtime/free-stockdb")

    cfg = load_config()

    assert cfg.data_root == instance / "task-data"
    assert cfg.free_stockdb_root == instance / "task-runtime" / "free-stockdb"


def test_relative_explicit_config_path_is_workspace_anchored_not_cwd(tmp_path, monkeypatch):
    workspace = tmp_path / "workspace"
    elsewhere = tmp_path / "elsewhere"
    workspace.mkdir()
    elsewhere.mkdir()
    config_path = workspace / "settings" / "config.yaml"
    config_path.parent.mkdir()
    config_path.write_text(yaml.safe_dump({"data": {"root": "instance-data"}}), encoding="utf-8")
    monkeypatch.setattr("quantmaster.config.WORKSPACE_ROOT", workspace.resolve())
    monkeypatch.chdir(elsewhere)

    cfg = load_config(Path("settings/config.yaml"))

    assert cfg.config_path == config_path.resolve()
    assert cfg.data_root == config_path.parent / "instance-data"
