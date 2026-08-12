from __future__ import annotations

from pathlib import Path

import yaml

from quantmaster.config import Config, load_config, set_config
from quantmaster.data.free_stockdb_runtime import FreeStockDBRuntime
from quantmaster.data.free_stockdb_source import resolve_free_stockdb_sdk_path


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
