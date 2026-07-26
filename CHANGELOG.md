# Changelog

## v0.2.0（2026-07-26）

### 新增
- **基本面/价值因子**：`data/fundamentals.py`（每日 PE/PB/股息率/市值 + 季度 ROE，
  报告期 +45 天发布滞后对齐防未来函数，Parquet 缓存）；`factors/fundamental.py`
  产出 ep / bp / dividend_yield / small_cap / roe 五个因子
- **样本外验证**：`backtest/validation.py` — train/test IC 对比（含衰减度与
  稳健/衰减/疑似过拟合/失效判定）、滚动分段 walk-forward IC、参数网格扫描
- **回测报告**：`backtest/report.py` — 年度收益表、月度收益表、成交统计，
  JSON 可序列化的 full_report
- **回测止损/止盈**：开盘价触线清仓，跌停顺延、当日不回补，成交记录标注原因
- **实盘净值曲线**：`portfolio/nav.py` — 从账本逐日重建资产/现金/浮盈，
  TWR 时间加权净值（出入金不扰动收益），与基准对比
- **多因子工具**：`factors/composite.py` — 因子相关性矩阵、IC/ICIR 动态加权合成
  （权重 shift 防未来函数、负 IC 自动反向）、截面正交化、贪心去冗余选择
- **CLI**：`qm validate` / `qm grid` / `qm fund-test` / `qm ledger nav`、
  `qm backtest --full --stop-loss --take-profit`
- **仪表盘**：因子页样本外验证区块；回测页止损止盈输入与年度/月度收益表；
  实盘页 TWR 净值曲线对比基准；挖掘结果点击表达式一键送检

### 修复
- `.gitignore` 的 `data/` 模式误忽略 `quantmaster/data` 源码包（v0.1 远端仓库
  缺失整个数据层），改为 `/data/`
- 爬虫入库去重计数使用 `cursor.rowcount`
- ECharts 本地化打包，仪表盘不再依赖 CDN（离线/大陆网络可用）

### 测试
- 65 → 135 项离线测试

## v0.1.0（2026-07-26）

- 多市场数据层（AKShare / yfinance / Tushare，自动降级 + Parquet 缓存）
- 因子表达式引擎（AST 白名单）、12 个内置量价因子、IC/分层分析
- 遗传规划 + LLM 因子挖掘；统一 LLM 客户端（Anthropic/OpenAI/兼容协议）
- AI 财经快讯爬虫与舆情因子
- A 股规则回测引擎（T+1/涨跌停/费用/整手）、模拟盘
- 实盘账本（FIFO/TWR/XIRR）
- FastAPI + ECharts 仪表盘、CLI、CI
