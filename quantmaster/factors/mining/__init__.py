from quantmaster.factor_mining_access import register_factor_miners
from quantmaster.factors.mining.genetic import GeneticMiner, MinedFactor
from quantmaster.factors.mining.llm_miner import LLMFactorMiner
from quantmaster.factors.mining.python_miner import PythonFactorMiner, PythonMiningCandidate

register_factor_miners(
    genetic=GeneticMiner,
    llm=LLMFactorMiner,
    python=PythonFactorMiner,
)

__all__ = [
    "GeneticMiner", "LLMFactorMiner", "MinedFactor", "PythonFactorMiner",
    "PythonMiningCandidate",
]
