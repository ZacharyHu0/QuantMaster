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
