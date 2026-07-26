# 架构设计

## 总览

```
┌─────────────────────────────────────────────────────────┐
│                 Web 仪表盘 (ECharts)  /  CLI (qm)        │
├─────────────────────────────────────────────────────────┤
│                  FastAPI 本地服务 (server/)              │
├───────────┬───────────┬───────────┬───────────┬─────────┤
│  factors/ │ backtest/ │    ai/    │ portfolio/│  data/  │
│  因子引擎  │  回测引擎  │ LLM+爬虫  │  实盘账本  │ 数据层   │
├───────────┴───────────┴───────────┴───────────┴─────────┤
│      本地存储：Parquet 行情缓存 + SQLite（新闻/账本）        │
└─────────────────────────────────────────────────────────┘
```

所有模块都可以脱离 Web 界面在 Python / CLI 中独立使用；Web 层只是薄薄的
一层 JSON API。

## 数据层（data/）

- **统一符号**：`600519.SH`、`00700.HK`、`^N225.JP`、`AU0.SHF`……后缀决定市场，
  `guess_market()` 据此路由数据源。
- **统一数据结构**：所有数据源输出同一种日线 DataFrame
  （index=交易日，columns=open/high/low/close/volume/amount/turnover），
  上层模块完全不感知数据来自哪家。
- **数据源优先级与降级**：每个市场配置一列数据源（如 A 股 = [AKShare, Tushare]），
  逐个尝试，全部失败时回退本地缓存。新增数据源只需实现 `DataSource.daily()`
  并注册到 `registry._factories()`。
- **缓存**：每标的一个 Parquet 文件，SQLite 记录覆盖区间与更新时间；
  命中覆盖区间或缓存足够新（`cache_days`）就不触网。

## 因子层（factors/）

- **面板范式**：因子的输入输出都是「面板」= `DataFrame(交易日 × 股票)`。
  这与 Qlib / Alpha101 的研究范式一致，向量化计算快且代码短。
- **表达式引擎**（`base.py`）：`rank(-delta(close, 5))` 这类字符串经
  Python AST 解析，只允许白名单内的算子/字段/四则运算——不存在 `eval`
  注入风险，因此 LLM 生成的表达式可以直接安全验证。
- **约定**：因子值只用当日及以前的数据；分析和回测统一按
  「T 日收盘算因子 → T+1 开盘交易」对齐，杜绝未来函数。
- **挖掘**：
  - 遗传规划（`mining/genetic.py`）：表达式树 随机生成→按 |RankIC| 适应度
    选择/交叉/变异。轻量自实现，产物与表达式引擎完全兼容。
  - LLM（`mining/llm_miner.py`）：把字段/算子/上一轮验证结果发给大模型，
    让它提出新表达式，本地验证后再反馈迭代。

## 回测层（backtest/）

- **引擎**（`engine.py`）：按目标权重调仓的日线回测。A 股规则内置：
  T+1（每日只在开盘交易一次，天然满足）、开盘涨跌停禁止买/卖
  （主板 ±10%，创业/科创 ±20%，北交所 ±30%）、佣金+印花税+过户费+滑点、
  100 股整手。
- **策略**（`strategy.py`）：`Strategy.target_weights(panel)` 返回
  信号日为行的权重矩阵。内置 `FactorStrategy`（因子 top-N 等权，周/月调仓）
  与 `BuyAndHold` 基准。自定义策略只需实现这一个方法。
- **模拟盘**（`paper.py`）：同一策略代码在「现在」运行，虚拟资金记账，
  与实盘账本共用 Ledger 结构。

## AI 层（ai/）

- **llm.py**：直接 httpx 调 REST，`provider` 三选一：`anthropic` /
  `openai` / `openai-compatible`（DeepSeek、通义、Kimi、GLM、Ollama 等
  一切 OpenAI 协议网关，改 `base_url` 即可）。
- **crawler.py**：`fetch（免费快讯接口）→ extract（LLM 批量结构化：相关股票/
  事件类型/情绪分/摘要）→ store（SQLite 去重入库）`。
- **sentiment.py**：把新闻情绪按股票聚合成因子面板（指数半衰衰减），
  可与量价因子直接合成。

## 实盘层（portfolio/）

- **ledger.py**：成交/出入金/分红三类记录，SQLite 存储；FIFO 批次配对
  计算持仓成本与已实现盈亏；支持券商导出 CSV 导入。
- **performance.py**：TWR（策略视角）与 XIRR（资金视角）两种收益率、
  持仓浮盈、费用统计。

## 为什么不直接用 vn.py / Qlib / backtrader？

| 项目 | 借鉴了什么 | 为什么不直接用 |
| --- | --- | --- |
| Qlib | 表达式因子 + IC 分析范式 | 安装重（C 扩展/数据格式绑定），学习曲线陡 |
| vn.py | 无（定位不同） | 面向实盘交易网关/CTP，研究功能弱 |
| backtrader | 事件驱动思想 | 逐 bar 事件循环慢，且无 A 股规则；保留为可选依赖 |
| AKShare | 整个数据层直接复用 | —— 直接作为依赖使用 |

自研向量化回测 + 表达式引擎合计不到一千行，换来的是：安装只需
numpy/pandas、A 股规则完整、LLM 可安全对接。

## 扩展点

1. **新数据源**：实现 `DataSource.daily()`，注册进 `registry._factories()`。
2. **新因子**：`ExpressionFactor("...")` 一行；需要财务数据的因子用
   `FuncFactor` 包任意函数。
3. **新策略**：继承 `Strategy` 实现 `target_weights()`。
4. **新爬虫源**：写一个返回 `list[NewsItem]` 的函数，登记进 `crawler.SOURCES`。
5. **桌面打包**：Web 界面即 UI，后续可用 Tauri/Electron/pywebview 包一层壳，
   或 PyInstaller 打包 `qm serve`。
