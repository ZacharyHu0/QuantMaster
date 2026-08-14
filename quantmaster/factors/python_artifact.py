"""受限 Python 因子工件。

候选代码只能定义 ``compute(features, params)``。代码在独立子进程中执行，
父进程只接收对齐后的 DataFrame；研究工件由内容哈希寻址，永不覆盖。
"""

from __future__ import annotations

import ast
import hashlib
import json
import math
import numbers
import os
import pickle
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from subprocess import TimeoutExpired
from types import MappingProxyType
from typing import Any

import numpy as np
import pandas as pd

from quantmaster.runtime.paths import confined_path
from quantmaster.runtime.process import (
    ProcessLimitError,
    ProcessLimits,
    run_restricted_process,
)


class PythonFactorPolicyError(ValueError):
    """候选违反静态策略或运行时输出契约。"""


_BANNED_NODES = (
    ast.Import, ast.ImportFrom, ast.For, ast.AsyncFor, ast.While, ast.With,
    ast.AsyncWith, ast.Try, ast.Raise, ast.ClassDef, ast.Lambda, ast.ListComp,
    ast.SetComp, ast.DictComp, ast.GeneratorExp, ast.Await, ast.Yield,
    ast.YieldFrom, ast.Global, ast.Nonlocal, ast.Delete,
)
_SAFE_BUILTINS = {
    "abs": abs, "bool": bool, "dict": dict, "float": float, "int": int,
    "len": len, "list": list, "max": max, "min": min, "round": round,
    "str": str, "sum": sum, "tuple": tuple,
}
_SAFE_CALLS = set(_SAFE_BUILTINS) | {"compute"}
_SAFE_NUMPY = {
    "abs", "clip", "exp", "isfinite", "isnan", "log", "log1p", "maximum",
    "minimum", "power", "sign", "sqrt", "tanh", "where",
}
_SAFE_PANDAS = {"DataFrame", "Series", "concat", "isna", "notna"}
_SAFE_METHODS = {
    "abs", "add", "clip", "corr", "corrwith", "cummax", "cummin", "cumprod",
    "cumsum", "diff", "div", "ewm", "expanding", "fillna", "mask", "max",
    "mean", "median", "min", "mul", "notna", "pct_change", "pow", "quantile",
    "rank", "reindex", "replace", "rolling", "shift", "std", "sub", "sum",
    "where", "winsorize",
}
_CAUSAL_PERIOD_METHODS = {"diff", "pct_change", "shift"}


class _PolicyVisitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.functions: list[str] = []
        self.period_params: dict[str, set[str]] = {}

    def generic_visit(self, node: ast.AST) -> None:
        if isinstance(node, _BANNED_NODES):
            raise PythonFactorPolicyError(f"禁止使用 {type(node).__name__}")
        super().generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        if node.name != "compute" or self.functions:
            raise PythonFactorPolicyError("只能定义一个 compute(features, params) 函数")
        if [item.arg for item in node.args.args] != ["features", "params"]:
            raise PythonFactorPolicyError("compute 参数必须严格为 features, params")
        if node.decorator_list or node.args.vararg or node.args.kwarg:
            raise PythonFactorPolicyError("compute 不允许装饰器或可变参数")
        self.functions.append(node.name)
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        if node.id.startswith("_") or node.id in {
            "open", "exec", "eval", "compile", "input", "help", "globals",
            "locals", "vars", "getattr", "setattr", "delattr", "__import__",
        }:
            raise PythonFactorPolicyError(f"禁止访问名称 {node.id}")

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if node.attr.startswith("_"):
            raise PythonFactorPolicyError("禁止访问私有或反射属性")
        allowed = _SAFE_METHODS
        if isinstance(node.value, ast.Name) and node.value.id == "np":
            allowed = _SAFE_NUMPY
        elif isinstance(node.value, ast.Name) and node.value.id == "pd":
            allowed = _SAFE_PANDAS
        if node.attr not in allowed:
            raise PythonFactorPolicyError(f"方法或属性不在白名单: {node.attr}")
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        if isinstance(node.func, ast.Name) and node.func.id not in _SAFE_CALLS:
            raise PythonFactorPolicyError(f"函数不在白名单: {node.func.id}")
        if isinstance(node.func, ast.Attribute) and node.func.attr in _CAUSAL_PERIOD_METHODS:
            method = node.func.attr
            periods = node.args[0] if node.args else next(
                (item.value for item in node.keywords if item.arg == "periods"), None,
            )
            if periods is not None:
                if _non_negative_integer_literal(periods):
                    pass
                else:
                    parameter = _params_string_key(periods)
                    if parameter is None:
                        raise PythonFactorPolicyError(
                            f"{method} 周期必须是非负整数常量或 params 参数"
                        )
                    self.period_params.setdefault(method, set()).add(parameter)
        self.generic_visit(node)


def _non_negative_integer_literal(node: ast.AST) -> bool:
    """Return whether an AST node is a literal, non-negative integer period."""
    if not isinstance(node, ast.Constant) or isinstance(node.value, bool):
        return False
    return isinstance(node.value, int) and node.value >= 0


def _params_string_key(node: ast.AST) -> str | None:
    if not isinstance(node, ast.Subscript):
        return None
    if not isinstance(node.value, ast.Name) or node.value.id != "params":
        return None
    value = node.slice.value if isinstance(node.slice, ast.Index) else node.slice
    if isinstance(value, ast.Constant) and isinstance(value.value, str):
        return value.value
    return None


def _validate_period_parameters(policy: dict[str, Any], params: dict | None) -> None:
    """Keep parameterized temporal operators causal at execution time."""
    values = params or {}
    for method, names in (policy.get("period_params") or {}).items():
        for name in names:
            value = values.get(name)
            if (
                isinstance(value, bool)
                or not isinstance(value, numbers.Real)
                or not math.isfinite(float(value))
                or float(value) < 0
                or int(value) != float(value)
            ):
                raise PythonFactorPolicyError(
                    f"{method} 周期参数 {name} 必须是非负整数"
                )


def validate_python_factor(source: str) -> dict[str, Any]:
    if not source.strip() or len(source) > 12_000:
        raise PythonFactorPolicyError("候选代码必须为 1–12000 个字符")
    try:
        tree = ast.parse(source, mode="exec")
    except SyntaxError as exc:
        raise PythonFactorPolicyError(f"Python 语法错误: {exc.msg}") from exc
    visitor = _PolicyVisitor()
    visitor.visit(tree)
    if visitor.functions != ["compute"]:
        raise PythonFactorPolicyError("必须定义 compute(features, params)")
    top_level = [node for node in tree.body if not isinstance(node, ast.FunctionDef)]
    if top_level:
        raise PythonFactorPolicyError("顶层只能包含 compute 函数定义")
    digest = hashlib.sha256(source.encode("utf-8")).hexdigest()
    features = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Subscript):
            continue
        if not isinstance(node.value, ast.Name) or node.value.id != "features":
            continue
        value = node.slice.value if isinstance(node.slice, ast.Index) else node.slice
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            features.add(value.value)
        else:
            raise PythonFactorPolicyError("features 只能使用静态字符串键访问")
    return {
        "sha256": digest, "ast_nodes": sum(1 for _ in ast.walk(tree)),
        "features": sorted(features),
        "shift_params": sorted(visitor.period_params.get("shift", set())),
        "period_params": {
            method: sorted(names) for method, names in sorted(visitor.period_params.items())
        },
    }


def _normalize_output(value: Any, reference: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(value, pd.DataFrame):
        raise PythonFactorPolicyError("compute 必须返回 pandas.DataFrame")
    if value.index.has_duplicates or value.columns.has_duplicates:
        raise PythonFactorPolicyError("输出索引或列不能重复")
    if not value.index.equals(reference.index) or not value.columns.equals(reference.columns):
        raise PythonFactorPolicyError("输出必须与 features['close'] 的索引和列严格对齐")
    result = value.apply(pd.to_numeric, errors="coerce").astype(float)
    return result.replace([np.inf, -np.inf], np.nan)


def execute_in_process(source: str, features: dict[str, pd.DataFrame], params: dict) -> pd.DataFrame:
    policy = validate_python_factor(source)
    _validate_period_parameters(policy, params)
    if "close" not in features:
        raise PythonFactorPolicyError("特征包缺少 close")
    namespace: dict[str, Any] = {
        "__builtins__": MappingProxyType(_SAFE_BUILTINS), "np": np, "pd": pd,
    }
    exec(compile(source, "<restricted-factor>", "exec"), namespace, namespace)
    value = namespace["compute"](MappingProxyType(features), MappingProxyType(dict(params)))
    return _normalize_output(value, features["close"])


def _worker(request_path: str, response_path: str) -> int:
    response: dict[str, Any]
    try:
        request = json.loads(Path(request_path).read_text(encoding="utf-8"))
        memory_mb = max(128, int(request.get("memory_mb", 768)))
        if os.name != "nt":
            try:
                import resource
                limit = memory_mb * 1024 * 1024
                resource.setrlimit(resource.RLIMIT_AS, (limit, limit))
            except (ImportError, OSError, ValueError):
                pass
        with Path(request["input"]).open("rb") as stream:
            payload = pickle.load(stream)  # trusted parent-owned temporary file
        result = execute_in_process(request["source"], payload["features"], payload["params"])
        with Path(request["output"]).open("wb") as stream:
            pickle.dump(result, stream, protocol=pickle.HIGHEST_PROTOCOL)
        response = {"ok": True, "rows": len(result), "columns": len(result.columns)}
    except Exception as exc:
        response = {"ok": False, "error": str(exc)[:1000], "error_type": type(exc).__name__}
    Path(response_path).write_text(json.dumps(response, ensure_ascii=False), encoding="utf-8")
    return 0 if response.get("ok") else 2


@dataclass(frozen=True)
class RestrictedPythonRunner:
    timeout_seconds: float = 20.0
    memory_mb: int = 1024
    cpu_seconds: int = 15
    output_mb: int = 2
    result_mb: int = 64

    def execute(
        self, source: str, features: dict[str, pd.DataFrame], params: dict | None = None,
    ) -> pd.DataFrame:
        policy = validate_python_factor(source)
        _validate_period_parameters(policy, params)
        with tempfile.TemporaryDirectory(prefix="qm-factor-") as directory:
            root = Path(directory)
            input_path, output_path = root / "input.pkl", root / "output.pkl"
            request_path, response_path = root / "request.json", root / "response.json"
            with input_path.open("wb") as stream:
                pickle.dump({"features": features, "params": params or {}}, stream,
                            protocol=pickle.HIGHEST_PROTOCOL)
            request_path.write_text(json.dumps({
                "source": source, "input": str(input_path), "output": str(output_path),
                "memory_mb": self.memory_mb,
            }, ensure_ascii=False), encoding="utf-8")
            command = [sys.executable]
            if getattr(sys, "frozen", False):
                command += ["__factor-runner", str(request_path), str(response_path)]
            else:
                command += ["-m", "quantmaster.factors.python_artifact", "--worker",
                            str(request_path), str(response_path)]
            env = {key: value for key, value in os.environ.items() if key.upper() in {
                "PATH", "PYTHONPATH", "SYSTEMROOT", "WINDIR", "TEMP", "TMP", "LANG",
                "LC_ALL", "APPDATA", "LOCALAPPDATA", "PROGRAMDATA", "USERPROFILE",
            }}
            env["PYTHONIOENCODING"] = "utf-8"
            try:
                completed = run_restricted_process(
                    command,
                    limits=ProcessLimits(
                        memory_bytes=max(128, self.memory_mb) * 1024 * 1024,
                        cpu_seconds=max(1, self.cpu_seconds),
                        output_bytes=max(1, self.output_mb) * 1024 * 1024,
                        max_processes=1,
                        file_bytes=max(1, self.result_mb) * 1024 * 1024,
                    ),
                    timeout=self.timeout_seconds,
                    env=env,
                )
            except TimeoutExpired as exc:
                raise PythonFactorPolicyError(
                    f"候选执行超过 {self.timeout_seconds:g} 秒，已终止"
                ) from exc
            except ProcessLimitError as exc:
                raise PythonFactorPolicyError(str(exc)) from exc
            if not response_path.exists():
                detail = (completed.stderr or completed.stdout).strip()[:500]
                raise PythonFactorPolicyError(f"隔离进程异常退出: {detail or completed.returncode}")
            response = json.loads(response_path.read_text(encoding="utf-8"))
            if not response.get("ok"):
                raise PythonFactorPolicyError(str(response.get("error") or "候选执行失败"))
            if output_path.stat().st_size > max(1, self.result_mb) * 1024 * 1024:
                raise PythonFactorPolicyError("候选结果超过安全上限")
            with output_path.open("rb") as stream:
                return pickle.load(stream)


def write_python_factor_artifact(
    data_root: str | Path, *, source: str, params: dict[str, Any], manifest: dict[str, Any],
) -> dict[str, Any]:
    policy = validate_python_factor(source)
    payload = {**manifest, "schema_version": 1, "source_sha256": policy["sha256"],
               "parameters": params}
    artifact_hash = hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    relative_root = Path("lab_artifacts") / "python_factors" / artifact_hash
    root = Path(data_root).resolve() / relative_root
    root.mkdir(parents=True, exist_ok=True)
    source_path, manifest_path = root / "factor.py", root / "manifest.json"
    if source_path.exists() and source_path.read_text(encoding="utf-8") != source:
        raise RuntimeError("内容寻址工件冲突")
    source_path.write_text(source, encoding="utf-8")
    manifest_path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8",
    )
    return {
        "hash": artifact_hash, "manifest": manifest_path.relative_to(Path(data_root)).as_posix(),
        "source": source_path.relative_to(Path(data_root)).as_posix(),
        "source_sha256": policy["sha256"], "parameters": params,
    }


def execute_python_factor_artifact(
    data_root: str | Path, artifact: dict[str, Any], features: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    root = Path(data_root).resolve()
    try:
        source_path = confined_path(root, artifact.get("source"), label="Python 因子工件")
    except ValueError as exc:
        raise PythonFactorPolicyError("工件路径越界") from exc
    source = source_path.read_text(encoding="utf-8")
    expected = str(artifact.get("source_sha256") or "")
    if not expected:
        raise PythonFactorPolicyError("Python 因子工件缺少 source_sha256，拒绝执行")
    actual = hashlib.sha256(source.encode("utf-8")).hexdigest()
    if actual != expected:
        raise PythonFactorPolicyError("Python 因子工件完整性校验失败")
    return RestrictedPythonRunner().execute(source, features, artifact.get("parameters") or {})


if __name__ == "__main__":  # pragma: no cover - exercised through parent process
    if len(sys.argv) == 4 and sys.argv[1] == "--worker":
        raise SystemExit(_worker(sys.argv[2], sys.argv[3]))
    raise SystemExit("This module is an internal restricted-factor worker.")
