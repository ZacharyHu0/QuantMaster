# 使用指南

## 安装

```bash
# Python 3.10+；Mac / Windows / Linux 均可
git clone https://github.com/ZacharyHu0/QuantMaster.git
cd QuantMaster
pip install -e ".[data,dev]"        # data = akshare + yfinance
```

Windows 提示：建议用 PowerShell + 官方 python.org 安装包，或直接用
[uv](https://github.com/astral-sh/uv)：`uv pip install -e ".[data,dev]"`。

## 1. 第一次跑通（10 分钟）

```bash
# 预取内置示例股票池（12 只大盘股）的行情到本地缓存
qm fetch --universe demo --start 2022-01-01

# 看看内置因子
qm factors

# 给「5日反转」做个体检
qm factor-test rev_5d
# 或任意表达式
qm factor-test "rank(-delta(close, 5))"

# 回测：20日动量选股，每周调仓，持有5只，对比沪深300
qm backtest --factor mom_20d --top 5 --rebalance W

# 打开 Web 界面看图
qm serve     # -> http://127.0.0.1:8686
```

## 2. 构建自己的股票池

```python
from quantmaster.data.universe import save_universe, index_universe

# 方式一：手动指定
save_universe("my_pool", ["600519.SH", "000858.SZ", "601318.SH"])

# 方式二：从指数成分构建（需要网络）
save_universe("hs300", index_universe("000300.SH"))
```

之后所有命令用 `--universe my_pool` 即可。
注意：股票池越大，首次拉数据越慢（免费接口有频率限制），建议先小池子迭代。

## 3. 研究自己的因子

```python
from quantmaster.data import load_panel
from quantmaster.data.universe import load_universe
from quantmaster.factors import ExpressionFactor, compute_factor, analyze_factor

panel = load_panel(load_universe("demo"), "2022-01-01", "2024-12-31")

# 表达式因子：量价背离
f = ExpressionFactor("-ts_corr(rank(volume), rank(close), 10)", name="pv_div")
report = analyze_factor(compute_factor(f, panel), panel["close"], name=f.name)
print(report.summary())
# report.ic_series / report.quantile_returns 可用 matplotlib 自行画图
```

需要财务数据的因子（如 PE/ROE）：用 `FuncFactor` 包一个自己取数的函数：

```python
from quantmaster.factors.base import FuncFactor

def my_pe_factor(panel):
    ...  # 返回 DataFrame(date × symbol)，如从 Tushare 拉 PE 后取倒数
    
f = FuncFactor("ep", my_pe_factor, description="盈利收益率 E/P")
```

## 4. 挖掘因子

```bash
# 遗传规划（本地算力，无需 key）：种群60 × 8代，约几分钟
qm mine --generations 8 --population 60 --start 2020-01-01 --end 2022-12-31

# 拿挖出的表达式做样本外验证（时间段错开！这是防过拟合的关键）
qm factor-test "<挖出的表达式>" --start 2023-01-01
```

LLM 挖掘需要先配 key（任选其一）：

```bash
export ANTHROPIC_API_KEY=sk-ant-...              # Claude
export OPENAI_API_KEY=sk-...                     # OpenAI
# 或 DeepSeek 等 OpenAI 兼容网关（config.yaml）：
#   llm: { provider: openai-compatible, model: deepseek-chat,
#          base_url: https://api.deepseek.com/v1, api_key: sk-... }

qm mine-llm --rounds 2
```

## 5. 舆情

```bash
qm crawl                 # 抓新浪/东财快讯 + LLM 标注（股票/事件/情绪）
qm crawl --skip-llm      # 没配 key：只抓取入库
```

Python 中把情绪聚合成因子面板：

```python
from quantmaster.ai.sentiment import sentiment_panel
senti = sentiment_panel()        # date × symbol，可与量价因子合成
```

## 6. 模拟盘 → 实盘

```bash
# 每个交易日收盘后跑一次（可挂 cron / 计划任务）
qm paper run --factor mom_20d --top 5
qm paper report

# 实盘：把券商 App 导出的成交记录整理成 CSV 导入
#   表头: date,symbol,side,price,shares,fee
qm ledger import my_trades.csv
qm ledger cash --amount 100000 --kind deposit --date 2024-01-02   # 别忘了入金记录
qm ledger report
```

Web 界面「实盘」页也可以逐笔录入。

## 7. 常见问题

**Q: akshare 拉数报错/很慢？**
免费接口有频控。`qm fetch` 会把数据缓存在 `data/bars/`，重跑不再触网；
偶发失败重跑一次即可，或换 Tushare（配 token 后自动作为 A 股备用源）。

**Q: 想用分钟线？**
当前版本聚焦日线研究（个人投资者最现实的频率）。数据层结构支持扩展，
欢迎 PR。

**Q: 回测收益为什么比想象低？**
默认扣了佣金/印花税/滑点，周调仓 top5 一年成本约 3-6%。这是特性不是 bug——
不扣成本的回测才是骗人的。

**Q: Mac / Windows 都能用？**
是。纯 Python + 浏览器界面，无平台绑定。后续计划提供 Tauri 桌面壳与
PyInstaller 单文件包。
