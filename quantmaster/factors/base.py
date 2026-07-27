"""因子定义与安全的表达式引擎。

表达式因子示例（Alpha101 风格）：
    "rank(-delta(close, 5))"          5 日反转
    "-ts_corr(rank(volume), rank(close), 10)"
    "ts_zscore(turnover, 20)"

表达式通过 Python AST 白名单解析执行——只允许注册过的算子、字段名与
四则运算，杜绝任意代码执行（LLM 生成的表达式也能安全评估）。
"""

from __future__ import annotations

import ast
from abc import ABC, abstractmethod
from collections.abc import Callable

import pandas as pd

from quantmaster.factors.ops import OPERATORS

Panel = pd.DataFrame
PanelDict = dict[str, pd.DataFrame]

# 表达式中可引用的行情字段
FIELDS = ("open", "high", "low", "close", "volume", "amount", "turnover", "vwap", "returns")


class Factor(ABC):
    """因子基类：输入行情面板字典，输出因子值面板（date × symbol）。

    因子值仅使用当日及更早的数据（不允许未来函数）；分析与回测阶段
    统一做 shift(1) 对齐，确保「今天收盘算因子，明天开盘交易」。
    """

    name: str = "factor"
    description: str = ""

    @abstractmethod
    def compute(self, panel: PanelDict) -> Panel:
        ...


class FuncFactor(Factor):
    """用普通函数定义的因子。"""

    def __init__(self, name: str, func: Callable[[PanelDict], Panel], description: str = ""):
        self.name = name
        self.func = func
        self.description = description

    def compute(self, panel: PanelDict) -> Panel:
        return self.func(panel)


class ExpressionFactor(Factor):
    """表达式因子：字符串表达式 -> 因子。"""

    def __init__(self, expression: str, name: str | None = None, description: str = ""):
        self.expression = expression
        self.name = name or expression
        self.description = description
        self._tree = parse_expression(expression)  # 构造时即校验合法性

    def compute(self, panel: PanelDict) -> Panel:
        return eval_expression(self._tree, panel)

    def __repr__(self) -> str:
        return f"ExpressionFactor({self.expression!r})"


class ExpressionError(ValueError):
    pass


def parse_expression(expression: str) -> ast.expression:
    """解析并校验表达式，返回 AST。非法节点直接抛 ExpressionError。"""
    try:
        tree = ast.parse(expression, mode="eval")
    except SyntaxError as e:
        raise ExpressionError(f"表达式语法错误: {expression!r}: {e}") from e
    _validate(tree.body)
    return tree


_ALLOWED_BINOPS = (ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Pow)
_ALLOWED_UNARY = (ast.USub, ast.UAdd)


def _validate(node: ast.AST) -> None:
    if isinstance(node, ast.Expression):
        _validate(node.body)
    elif isinstance(node, ast.BinOp):
        if not isinstance(node.op, _ALLOWED_BINOPS):
            raise ExpressionError(f"不支持的运算符: {ast.dump(node.op)}")
        _validate(node.left)
        _validate(node.right)
    elif isinstance(node, ast.UnaryOp):
        if not isinstance(node.op, _ALLOWED_UNARY):
            raise ExpressionError(f"不支持的一元运算符: {ast.dump(node.op)}")
        _validate(node.operand)
    elif isinstance(node, ast.Call):
        if not isinstance(node.func, ast.Name) or node.func.id not in OPERATORS:
            raise ExpressionError(f"未注册的算子调用: {ast.unparse(node)}")
        if node.keywords:
            raise ExpressionError("表达式不支持关键字参数")
        _func, arity = OPERATORS[node.func.id]
        if len(node.args) != arity:
            raise ExpressionError(
                f"算子 {node.func.id} 需要 {arity} 个参数，实际 {len(node.args)} 个"
            )
        for arg in node.args:
            _validate(arg)
    elif isinstance(node, ast.Name):
        if node.id not in FIELDS:
            raise ExpressionError(f"未知字段: {node.id}（可用: {', '.join(FIELDS)}）")
    elif isinstance(node, ast.Constant):
        if not isinstance(node.value, (int, float)):
            raise ExpressionError(f"只允许数字常量: {node.value!r}")
    else:
        raise ExpressionError(f"不允许的语法节点: {type(node).__name__}")


def _prepare_panel(panel: PanelDict) -> PanelDict:
    """补充派生字段 vwap / returns。"""
    out = dict(panel)
    if "vwap" not in out and "amount" in out and "volume" in out:
        out["vwap"] = out["amount"] / out["volume"].replace(0, float("nan"))
    if "returns" not in out and "close" in out:
        # 缺失收盘价代表停牌或数据缺口，不能隐式前向填充后伪造零收益。
        out["returns"] = out["close"].pct_change(fill_method=None)
    return out


def eval_expression(tree: ast.expression, panel: PanelDict) -> Panel:
    data = _prepare_panel(panel)

    def _eval(node: ast.AST):
        if isinstance(node, ast.Expression):
            return _eval(node.body)
        if isinstance(node, ast.BinOp):
            left, right = _eval(node.left), _eval(node.right)
            if isinstance(node.op, ast.Add):
                return left + right
            if isinstance(node.op, ast.Sub):
                return left - right
            if isinstance(node.op, ast.Mult):
                return left * right
            if isinstance(node.op, ast.Div):
                return left / right
            return left ** right
        if isinstance(node, ast.UnaryOp):
            operand = _eval(node.operand)
            return -operand if isinstance(node.op, ast.USub) else operand
        if isinstance(node, ast.Call):
            func, _ = OPERATORS[node.func.id]  # type: ignore[union-attr]
            return func(*[_eval(a) for a in node.args])
        if isinstance(node, ast.Name):
            if node.id not in data:
                raise ExpressionError(f"数据缺少字段: {node.id}")
            return data[node.id]
        if isinstance(node, ast.Constant):
            return node.value
        raise ExpressionError(f"不允许的语法节点: {type(node).__name__}")

    result = _eval(tree)
    if not isinstance(result, pd.DataFrame):
        raise ExpressionError("表达式结果必须是面板数据（提示：表达式需引用行情字段）")
    return result
