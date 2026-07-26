"""遗传规划（Genetic Programming）因子挖掘。

思路（无需高深数学背景）：
1. 随机生成一批「表达式树」作为初始因子种群，如 rank(-delta(close, 5))；
2. 用历史数据计算每个因子的适应度 = |RankIC均值| × 稳定性 - 复杂度惩罚；
3. 适应度高的因子有更大概率被选中「繁殖」：
   - 交叉：交换两棵表达式树的子树
   - 变异：随机替换某个子树 / 参数
4. 重复若干代，输出适应度最高的因子表达式。

自实现的轻量版（不依赖 gplearn），表达式与 ExpressionFactor 完全兼容，
挖出的因子可直接进因子库、跑分层回测。
"""

from __future__ import annotations

import random
from dataclasses import dataclass

import pandas as pd

from quantmaster.factors.analysis import forward_returns, information_coefficient
from quantmaster.factors.base import ExpressionFactor, PanelDict

# 挖掘用的算子集合：(名称, 参数模式)。"p" = 面板参数, "n" = 窗口常数
_GP_OPERATORS = [
    ("rank", ["p"]),
    ("zscore", ["p"]),
    ("sign", ["p"]),
    ("log", ["p"]),
    ("delay", ["p", "n"]),
    ("delta", ["p", "n"]),
    ("pct_change", ["p", "n"]),
    ("ts_mean", ["p", "n"]),
    ("ts_std", ["p", "n"]),
    ("ts_rank", ["p", "n"]),
    ("ts_zscore", ["p", "n"]),
    ("ts_max", ["p", "n"]),
    ("ts_min", ["p", "n"]),
    ("ts_corr", ["p", "p", "n"]),
]
_WINDOWS = [3, 5, 10, 20, 40, 60]
_FIELDS = ["open", "high", "low", "close", "volume", "returns"]


@dataclass
class Node:
    op: str | None            # None 表示叶子
    children: list            # Node 或常数
    field: str | None = None  # 叶子的字段名

    def to_expression(self) -> str:
        if self.op is None:
            return self.field or "close"
        args = []
        for child in self.children:
            args.append(child.to_expression() if isinstance(child, Node) else str(child))
        return f"{self.op}({', '.join(args)})"

    def size(self) -> int:
        return 1 + sum(c.size() for c in self.children if isinstance(c, Node))

    def clone(self) -> Node:
        return Node(
            op=self.op,
            children=[c.clone() if isinstance(c, Node) else c for c in self.children],
            field=self.field,
        )


@dataclass
class MinedFactor:
    expression: str
    fitness: float
    ic_mean: float
    icir: float


class GeneticMiner:
    def __init__(
        self,
        population: int = 60,
        generations: int = 8,
        max_depth: int = 4,
        seed: int = 42,
        elite_ratio: float = 0.15,
        mutation_rate: float = 0.3,
        complexity_penalty: float = 0.002,
        fields: list[str] | None = None,
    ):
        self.population_size = population
        self.generations = generations
        self.max_depth = max_depth
        self.rng = random.Random(seed)
        self.elite_ratio = elite_ratio
        self.mutation_rate = mutation_rate
        self.complexity_penalty = complexity_penalty
        self.fields = fields or _FIELDS

    # ---- 表达式树的生成 ----

    def _random_leaf(self) -> Node:
        return Node(op=None, children=[], field=self.rng.choice(self.fields))

    def _random_tree(self, depth: int = 0) -> Node:
        if depth >= self.max_depth or (depth > 0 and self.rng.random() < 0.3):
            return self._random_leaf()
        op, pattern = self.rng.choice(_GP_OPERATORS)
        children: list = []
        for kind in pattern:
            if kind == "p":
                children.append(self._random_tree(depth + 1))
            else:
                children.append(self.rng.choice(_WINDOWS))
        return Node(op=op, children=children)

    # ---- 适应度 ----

    def _fitness(self, node: Node, panel: PanelDict, fwd: pd.DataFrame) -> tuple[float, float, float]:
        expr = node.to_expression()
        try:
            import warnings

            factor = ExpressionFactor(expr)
            with warnings.catch_warnings():
                # 随机表达式常产生全 NaN 列，触发无害的 RuntimeWarning
                warnings.simplefilter("ignore", RuntimeWarning)
                values = factor.compute(panel)
                ic = information_coefficient(values, fwd)
        except Exception:
            return -1.0, 0.0, 0.0
        if len(ic) < 20:
            return -1.0, 0.0, 0.0
        ic_mean = float(ic.mean())
        ic_std = float(ic.std())
        icir = ic_mean / ic_std if ic_std > 0 else 0.0
        # 适应度 = |IC| 主导 + 稳定性加成 - 复杂度惩罚
        fitness = abs(ic_mean) + 0.1 * abs(icir) - self.complexity_penalty * node.size()
        return fitness, ic_mean, icir

    # ---- 遗传操作 ----

    def _collect_subtrees(self, node: Node) -> list[Node]:
        result = [node]
        for child in node.children:
            if isinstance(child, Node):
                result.extend(self._collect_subtrees(child))
        return result

    def _crossover(self, a: Node, b: Node) -> Node:
        child = a.clone()
        targets = self._collect_subtrees(child)
        donor_parts = self._collect_subtrees(b)
        target = self.rng.choice(targets)
        donor = self.rng.choice(donor_parts).clone()
        target.op, target.children, target.field = donor.op, donor.children, donor.field
        return child

    def _mutate(self, node: Node) -> Node:
        mutant = node.clone()
        targets = self._collect_subtrees(mutant)
        target = self.rng.choice(targets)
        if target.children and self.rng.random() < 0.5:
            # 参数变异：换窗口常数
            for i, child in enumerate(target.children):
                if not isinstance(child, Node):
                    target.children[i] = self.rng.choice(_WINDOWS)
        else:
            fresh = self._random_tree(depth=self.max_depth - 1)
            target.op, target.children, target.field = fresh.op, fresh.children, fresh.field
        return mutant

    # ---- 主流程 ----

    def mine(self, panel: PanelDict, top_n: int = 10, periods: int = 1,
             progress: bool = True) -> list[MinedFactor]:
        close = panel["close"]
        fwd = forward_returns(close, periods=periods)
        population = [self._random_tree() for _ in range(self.population_size)]
        best: dict[str, MinedFactor] = {}

        for gen in range(self.generations):
            scored = []
            for node in population:
                fitness, ic_mean, icir = self._fitness(node, panel, fwd)
                scored.append((fitness, node, ic_mean, icir))
            scored.sort(key=lambda x: x[0], reverse=True)

            for fitness, node, ic_mean, icir in scored:
                if fitness <= 0:
                    continue
                expr = node.to_expression()
                if expr not in best or best[expr].fitness < fitness:
                    best[expr] = MinedFactor(expr, fitness, ic_mean, icir)

            if progress:
                top = scored[0]
                print(f"[gen {gen + 1}/{self.generations}] "
                      f"best fitness={top[0]:.4f} ic={top[2]:.4f} expr={top[1].to_expression()}")

            # 精英保留 + 锦标赛选择繁殖
            elite_count = max(2, int(self.population_size * self.elite_ratio))
            elites = [node.clone() for _, node, _, _ in scored[:elite_count]]
            next_gen = list(elites)
            candidates = [node for _, node, _, _ in scored[: max(10, self.population_size // 2)]]
            while len(next_gen) < self.population_size:
                if self.rng.random() < self.mutation_rate:
                    next_gen.append(self._mutate(self.rng.choice(candidates)))
                else:
                    a, b = self.rng.sample(candidates, 2)
                    next_gen.append(self._crossover(a, b))
            population = next_gen

        result = sorted(best.values(), key=lambda f: f.fitness, reverse=True)
        return result[:top_n]
