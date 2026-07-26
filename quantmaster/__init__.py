"""QuantMaster — 面向 A 股的量化研究与实盘记录平台。

模块总览：
- data      多市场行情数据（A股/港股/美日韩/商品期货），本地缓存
- factors   因子库、表达式引擎、IC/分层分析、因子挖掘（遗传规划 + LLM）
- ai        统一 LLM 客户端（OpenAI/Anthropic 兼容）、AI 爬虫、舆情
- backtest  向量化回测引擎（A股交易规则）、模拟盘
- portfolio 实盘交易账本与收益统计
- server    FastAPI 本地服务 + Web 仪表盘
"""

__version__ = "0.1.0"
