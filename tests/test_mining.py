"""因子挖掘测试（遗传规划为本地计算，可离线测试）。"""

from quantmaster.factors.base import ExpressionFactor
from quantmaster.factors.mining import GeneticMiner


class TestGeneticMiner:
    def test_mining_produces_valid_factors(self, panel):
        miner = GeneticMiner(population=20, generations=2, seed=11)
        mined = miner.mine(panel, top_n=5, progress=False)
        assert mined, "至少应挖出一个正适应度因子"
        for m in mined:
            # 所有产出的表达式必须能通过表达式引擎校验并计算
            values = ExpressionFactor(m.expression).compute(panel)
            assert values.shape == panel["close"].shape
            assert -1 <= m.ic_mean <= 1

    def test_deterministic_with_seed(self, panel):
        a = GeneticMiner(population=16, generations=2, seed=5).mine(
            panel, top_n=3, progress=False)
        b = GeneticMiner(population=16, generations=2, seed=5).mine(
            panel, top_n=3, progress=False)
        assert [x.expression for x in a] == [x.expression for x in b]

    def test_results_sorted_by_fitness(self, panel):
        mined = GeneticMiner(population=16, generations=2, seed=9).mine(
            panel, top_n=10, progress=False)
        fitness = [m.fitness for m in mined]
        assert fitness == sorted(fitness, reverse=True)
