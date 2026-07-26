# QuantMaster

面向 **中国 A 股** 的开源量化研究与实盘记录平台。

为已开户的个人投资者设计：假定你有不错的编程能力（计算机本科水平），
数学与金融只需本科基础——文档与代码注释会把用到的量化概念讲清楚。

## 能做什么

| 模块 | 说明 |
| --- | --- |
| 📡 多市场数据 | A 股/港股为主，参考美/日/韩指数与大宗商品期货；免费数据源（AKShare、yfinance，可选 Tushare），本地 Parquet 缓存，自动降级 |
| 🧪 因子研究 | 内置常用量价因子库；Alpha101 风格表达式引擎（AST 白名单，安全执行）；IC/ICIR/分层回测/换手率一站式因子体检 |
| ⛏️ 因子挖掘 | 遗传规划自动搜索因子表达式；LLM 因子挖掘（大模型提出候选 → 本地数据严格验证） |
| 🤖 AI 能力 | 统一 LLM 客户端，兼容 **Anthropic / OpenAI / 任何 OpenAI 协议网关**（DeepSeek、通义、Kimi、GLM、本地 Ollama）；AI 爬虫抓取财经快讯并结构化为舆情因子 |
| 📈 回测 Lab | 向量化回测引擎，内置 A 股规则：**T+1、涨跌停、佣金/印花税/过户费、100 股整手**；净值/回撤/夏普/卡玛等完整绩效指标；对比基准指数 |
| 💰 实盘记录 | 交易账本（支持券商成交记录 CSV 导入），FIFO 成本核算，TWR 时间加权收益 / XIRR 内部收益率 |
| 🖥️ 本地 Web 界面 | FastAPI + ECharts 仪表盘，`qm serve` 一键启动，浏览器访问；Mac / Windows / Linux 跨平台 |

## 快速开始

```bash
# 环境要求 Python 3.10+
pip install -e ".[data,dev]"     # data = akshare + yfinance（推荐）

qm serve                          # 启动 Web 界面 -> http://127.0.0.1:8686
```

命令行研究流程：

```bash
qm fetch --universe demo --start 2022-01-01          # 拉取内置示例股票池行情
qm factor-test "rank(-delta(close, 5))"              # 因子体检：IC/分层/换手
qm backtest --factor mom_20d --top 5                 # 因子选股回测
qm mine --generations 8                              # 遗传规划挖因子
qm mine-llm --rounds 2                               # LLM 挖因子（需配置 API key）
qm crawl                                             # 抓取财经快讯 + LLM 情绪标注
qm ledger import my_trades.csv                       # 导入实盘成交记录
qm ledger report                                     # 实盘收益报告
```

Python API 同样直接：

```python
from quantmaster.data import load_panel
from quantmaster.factors import ExpressionFactor, analyze_factor, compute_factor

panel = load_panel(["600519.SH", "000858.SZ", "300750.SZ"], "2022-01-01", "2024-12-31")
factor = ExpressionFactor("rank(-delta(close, 5))")
report = analyze_factor(compute_factor(factor, panel), panel["close"], name="5日反转")
print(report.summary())
```

## 配置

复制 `config.example.yaml` 为 `config.yaml`（或使用环境变量）：

```yaml
llm:
  provider: anthropic          # anthropic | openai | openai-compatible
  model: claude-sonnet-5
  api_key: ""                  # 或环境变量 ANTHROPIC_API_KEY / OPENAI_API_KEY
  base_url: ""                 # openai-compatible 时填网关地址，如 https://api.deepseek.com/v1
data:
  tushare_token: ""            # 可选
```

## 设计原则

1. **站在巨人肩膀上**：数据层直接复用 [AKShare](https://github.com/akfamily/akshare)
   （A 股免费数据事实标准）与 yfinance；因子研究流程借鉴
   [Microsoft Qlib](https://github.com/microsoft/qlib) 的「表达式因子 + IC 分析」范式；
   表达式算子命名沿用 WorldQuant Alpha101 惯例，社区因子可直接迁移。
2. **回测必须像 A 股**：T+1、涨跌停买卖限制、印花税单边征收、整手交易——
   这些规则不建模，回测收益就是自欺欺人。
3. **无未来函数**：统一「T 日收盘算信号 → T+1 开盘成交」；表达式引擎只提供
   向后看的算子。
4. **LLM 输出不可信**：AI 生成的因子表达式一律经 AST 白名单校验 + 本地数据验证，
   不存在代码注入，也不盲信大模型的「故事」。
5. **本地优先**：数据、账本、研究结果全部落在本地磁盘，不依赖任何云服务。

## 项目结构

```
quantmaster/
├── config.py            配置（yaml + 环境变量）
├── data/                数据层：akshare / yfinance / tushare + Parquet 缓存
├── factors/             因子：算子库、表达式引擎、内置因子、IC/分层分析
│   └── mining/          因子挖掘：遗传规划 + LLM
├── ai/                  统一 LLM 客户端、AI 爬虫、舆情因子
├── backtest/            回测引擎（A 股规则）、策略、绩效指标、模拟盘
├── portfolio/           实盘账本（FIFO 成本、TWR/XIRR）
└── server/              FastAPI + ECharts Web 界面
```

更多文档见 [docs/](docs/)。

## 免责声明

本项目仅供学习研究，不构成任何投资建议。历史回测收益不代表未来表现，
入市有风险，投资需谨慎。

## License

MIT
