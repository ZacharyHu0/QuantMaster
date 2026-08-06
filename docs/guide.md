# 使用指南

## 安装

```bash
# Python 3.12+；Mac / Windows / Linux 均可
git clone https://github.com/ZacharyHu0/QuantMaster.git
cd QuantMaster
pip install -e ".[data,dev]"        # data = akshare + yfinance
```

Windows 提示：建议用 PowerShell + 官方 python.org 安装包，或直接用
[uv](https://github.com/astral-sh/uv)：`uv pip install -e ".[data,dev]"`。

## 1. 第一次跑通（10 分钟）

```bash
# 预取内置示例候选（12 只大盘股）的行情到本地缓存
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

## 2. 构建自己的候选

Web 界面顶部的“候选”是统一管理入口。左侧选择候选后，右侧可核对证券名称、代码、
来源、研究质量和当前使用位置。自定义候选的添加、移除和批量粘贴先进入草稿，只有点击
“保存更改”才会影响后续任务；`demo` 与按日期读取历史成分的 `csi800` 只读，可复制后再编辑。
重命名会同步更新自动化和 Quant Lab 的当前引用，删除正在使用的候选时必须先选择替代项。

```python
from quantmaster.data.universe import save_universe, index_universe

# 方式一：手动指定
save_universe("my_pool", ["600519.SH", "000858.SZ", "601318.SH"])

# 方式二：从指数成分构建（需要网络）
save_universe("hs300", index_universe("000300.SH"))
```

之后所有命令用 `--universe my_pool` 即可。
注意：候选越大，首次拉数据越慢（免费接口有频率限制），建议先从少量标的开始迭代。

## 2.5 生成六维个股分析

启动 `qm serve` 后，从顶部进入“个股分析”，输入 `600519`、`600519.SH` 或“贵州茅台”。
输入框会先用本地证券主数据确认名称和市场。“快速”研究对应原有联网深度能力：并发读取财务披露、
长周期与相对强弱、公告及事件后价格反应、资金/融资融券/龙虎榜、市场宽度和宏观政策证据，完成六维
首轮复核及一次交叉终审，通常需要 2–5 分钟。默认“深度”研究会再执行三轮定向追证、六维逐一反方
审查和独立证伪终审，通常约 8–15 分钟，最长 900 秒。每维完成后会立即显示完整结论和可点击来源；
报告同时披露逐维证据数、来源数、审查轮次及未达深度门槛的原因。可以离开或刷新页面，后台任务会按
本地保存的 job ID 恢复，也可安全取消。

已经绑定的飞书私聊或群聊也可以发送“分析贵州茅台”“六维分析 600519”或
“贵州茅台怎么样”。这些说法默认深度模式，只有明确发送“快速分析 600519”才使用快速模式。
Bot 先发一张进度卡并原位更新；最终主卡直接保留六维结论，证据超出单卡容量时按编号发送完整附录，
服务重启后仍从原 `message_id` 和附录游标继续。群聊仍需真实
`@QuantMaster`。上游 skill 的持仓监控脚本不会执行，分析只读取本次明确询问的单一标的。
本地没有财务、资讯或逐单资金流时，对应维度会标为部分数据或数据缺失，不代表现实中没有事件，
也不会用模型虚构数字。原生联网搜索不可用时会自动保留 AKShare/本地可信数据与规则版报告；设置页
“检测联网搜索”可清除旧探测结果并发出一次最小请求，失败的能力缓存也会在 5 分钟后自动复测。任何模型
主张若引用任务外 evidence ID、返回非法数值或未引用证据都会被拒绝。分析不执行交易，也不读取或发送持仓。

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

## 3.5 基本面因子与多因子合成

```bash
# 基本面因子体检（首次会拉取估值/财务数据并缓存，稍慢）
qm fund-test ep            # 盈利收益率 1/PE
qm fund-test small_cap     # 小市值因子（A股历史上最强的因子之一，注意风格切换风险）
```

Python 中做多因子合成：

```python
from quantmaster.factors import (
    BUILTIN_FACTORS, compute_factors, factor_correlation,
    ic_weighted_combine, greedy_select,
)

values = compute_factors(list(BUILTIN_FACTORS.values()), panel)
print(factor_correlation(values))              # 相关性矩阵：>0.6 的因子留一个就够
picked = greedy_select(values, panel["close"]) # 按 |IC| 贪心挑出低相关因子组
combined, weights = ic_weighted_combine(       # 滚动 IC 加权动态合成（权重已 shift 防未来）
    {k: values[k] for k in picked}, panel["close"], lookback=60)
```

## 3.8 生产跨资产研究 Artifact

先 dry-run，再生产：

```bash
qm data capabilities
qm data plan --assets stock,etf \
  --specs cross_asset_core,forward_returns,qm_style_v1 \
  --start 2022-01-01 --end 2026-07-30
qm data sync --assets stock,etf \
  --specs cross_asset_core,forward_returns,qm_style_v1 \
  --start 2022-01-01 --end 2026-07-30
qm data jobs
```

Windows PowerShell 可将换行续写改为反引号，或把命令写在同一行。`plan` 会显示
依赖、预热/前瞻窗口、缺失分区、修订范围、预估行数和权限阻塞；只有 `sync`
才会读取数据并写入研究湖。也可在“设置 → 数据与缓存 → 研究生产湖”执行
同样的流程。需要将旧按标的行情缓存暴露给新流水线时，使用 `qm data materialize`。

回测可直接锁定不可变产物：

```bash
qm backtest --factor artifact:factor:stock:cross_momentum_20d@1.0.0 --universe demo
```

学习模型训练后会以 `artifact:model:stock:<slug>@1.0.0` 发布样本外预测分区；因子、
标签、风险和模型都使用同一版本与血缘协议。完整字段口径见
[研究生产流水线](research_pipeline.md)。

## 4. 挖掘因子

### AI Quant Lab：推荐工作流

Web 顶部的 **Quant Lab** 把发现、模型实验、因子版本、统一验证、人工审批和
自动研究队列集中到一个工作台。内置目录固定为 48 个可解释起点；新表达式只能
使用安全 DSL，AI 不能执行任意 Python。

```bash
# 基础安装可直接运行 Ridge；深度模型使用可选依赖
pip install -e ".[data,ml]"

qm lab doctor
qm lab prepare-data --universe demo --start 2022-01-01
qm lab discover --method genetic --universe demo --start 2022-01-01
qm lab train --model transformer --universe demo --start 2022-01-01
qm lab jobs
```

`qm serve` 会承载本地 Worker；需要把重型训练隔离到单独进程时，运行
`qm lab worker`。自动研究只在 `lab.window_start` / `window_end` 内消费定时任务，
并受 `daily_budget_hours` 限制。生产研究应配置 Tushare token，使用从 2015 年开始的
point-in-time 中证800成分；`demo`/固定候选会被标记为 sandbox，不能绕过数据硬门槛。

AI 自动任务默认只发送表达式结构和本地验证指标。发送匿名样本必须同时开启
`allow_cloud_sample`，并在当次请求再次确认。无论来源是人工、遗传规划、LLM 还是
深度模型，版本都必须经过统一验证和人工批准后才能成为研究 Champion；该操作不连接券商。

```bash
# 遗传规划（本地算力，无需 key）：种群60 × 8代，约几分钟
qm mine --generations 8 --population 60 --start 2020-01-01 --end 2022-12-31

# 拿挖出的表达式做样本外验证（这是防过拟合的关键一步）
qm validate "<挖出的表达式>" --split 2023-01-01 --start 2020-01-01
# 输出训练期/验证期 IC 对比、衰减度、以及 稳健/衰减/疑似过拟合/失效 结论

# 参数网格扫描：别只看单点参数的好结果，稳健的策略应该在邻近参数上也不差
qm grid --factors mom_20d,rev_5d,low_vol_20d --tops 3,5,10 --rebalances W,M
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

## 5. 资讯与消息面因子

```bash
qm crawl                 # 抓取全部已启用来源 + LLM 标注（股票/板块/事件/情绪）
qm crawl --skip-llm      # 没配 key：先归档并进入待标注队列
```

Python 中把情绪聚合成因子面板：

```python
from quantmaster.ai.sentiment import NewsSentimentFactor

factor = NewsSentimentFactor()   # compute(panel) 只读本地资讯库，不会在回测时触网
```

Web「资讯」页提供可筛选事件流、标注队列、重要度、大盘情绪和申万一级板块独立分数。
事件板块由 LLM 在固定白名单内标注，并由本地股票行业映射补全；大盘和板块当前分数均按
情绪 × 置信度 × 重要度 × 来源权重计算，再按 3 个自然日半衰期衰减到 -100 至 +100。点击「处理待标注」
后，页面按每批 5 条显示真实进度与已用时间；批次一经写入便实时更新事件、统计和因子，
无需等待全部内容处理完成。来源在
「设置 → 资讯来源」管理。除内置适配器外，可添加三种声明式来源：RSS / Atom、
JSON 点号路径、HTML CSS 选择器。来源只能访问公开 `http(s)` 地址，Token 必须使用
Bearer 或自定义 Header 凭据字段，不能写进普通请求头。

来源固定分入快讯、官方、定期三组，默认分别每 10、15、60 分钟采集；启停和频率
在「自动化」页统一修改。原始响应默认保留 7 天，规范化正文与首次获取时点长期保留。
`news_sentiment` 使用情绪 × 置信度 × 重要度 × 来源权重，精确重复内容只计一次；
15:00 后首次获取的消息进入下一交易日，默认按 3 个自然日半衰期衰减。

## 6. 飞书主通道 / 微信轻量提醒

先在「设置 → 自动化」完成接入并打开运行状态；设置会自动保存，调度器与长连接立即热应用。
凭据只在设置中心维护，「自动化」页面用于绑定会话、调整推送策略和操作任务：

- 飞书：在开放平台创建企业自建应用，启用机器人、开通收发消息权限，订阅
  `im.message.receive_v1` 并选择长连接；将 App ID / App Secret 填入设置中心。最小权限建议为
  `im:message:send_as_bot`、`im:message.p2p_msg:readonly`、
  `im:message.group_msg`，配置后必须发布应用版本才会生效。若只开
  `im:message.group_at_msg:readonly`，机器人仍能响应 @消息，但无法参考普通群聊形成话题记忆。
- 飞书会话绑定：先绑定管理员私聊，再由同一管理员绑定群聊。绑定码 10 分钟失效且只能使用一次。
- 飞书群聊响应：普通消息只进入本地会话记录，不触发回复；真正 `@QuantMaster` 时才按当前问题
  检索相关话题和最近对话。记录过长后由已配置 LLM 压缩为结构化话题记忆，保留标的、观点、
  分歧、结论和待确认项，再删除已经被记忆覆盖的冗长原文。
- 飞书诊断：设置中心依次检测应用凭据、自动化运行时、WebSocket 长连接、入站消息事件和
  会话绑定。凭据已配置但总开关关闭时会明确显示“尚未监听”；只有用户点击一键启用后，
  长连接与既有定时任务才会一起启动。
- 腾讯微信 ClawBot（可选）：点击扫码授权；确认后主动给机器人发一句话，让系统取得当前
  会话的 `context_token`。微信接口只承载文本提醒和简单命令；结构化卡片与完整交互以飞书为准。

常用对话命令：

```text
把当前推送强度调成敏感
查看任务
运行收盘
暂停盘中监控
买入 600519 100股 价格1500 费用5
确认 123456
```

成交、现金流和模拟调仓不会因一句自然语言直接落账：系统先返回规范化预览，只能由已绑定管理员
在同一私聊用一次性确认码提交。用 `qm automation doctor` 可检查依赖、任务、账号与目标状态。

## 7. 模拟盘 → 实盘

```bash
# 每个交易日收盘后跑一次（可挂 cron / 计划任务）
qm paper run --factor mom_20d --top 5
qm paper report

# 实盘：把券商 App 导出的成交记录整理成 CSV 导入
#   表头: date,symbol,side,price,shares,fee
qm ledger import my_trades.csv
qm ledger cash --amount 100000 --kind deposit --date 2024-01-02   # 别忘了入金记录
qm ledger report
qm ledger nav --benchmark 000300.SH   # 每日净值（TWR）与沪深300对比
```

回测时启用止损/止盈（A 股常用纪律）：

```bash
qm backtest --factor mom_20d --stop-loss 0.08 --take-profit 0.25 --full
# --full 额外输出年度收益表和月度收益表
```

Web 界面「实盘」页既可以逐笔录入，也可以直接导入券商 CSV：选择文件后检查
自动列映射与逐行预览，再选择严格模式或仅导入有效行。疑似重复默认跳过；最终
有效记录在一个 SQLite 事务中写入，任一数据库错误都会整批回滚。

## 设置中心

顶部「设置」统一管理 LLM、Tushare、缓存、交易费率、资讯处理、消息自动化、Quant Lab
及本机服务。普通字段停顿片刻或离开字段后自动保存并热应用；API Key、Token 与 App Secret
会等待输入完成再提交，避免保存半截凭据。模型下拉来自
提供商的模型列表接口，也可保留任意手填 ID；联网检测失败只显示警告，不阻止
保存。运行状态会显示当前配置是否已应用；只有 host/port 修改需重启后生效。关闭 Lab Worker
只会停止领取新任务，正在执行的研究会安全完成。

数据根目录不要直接编辑 YAML：使用「数据与缓存 → 数据目录迁移」复制并切换。
系统会用 SQLite 备份 API 复制数据库、对其他文件校验大小与 SHA-256，全部通过
后才更新配置，旧目录不会删除。设置快照不包含密钥或行情/账本数据，回滚时也会
保留当前凭据。

市场页启动会先显示已有本地卡片，再在后台检查近期缺口；点击「同步最新行情」只
检查尾部增量。批量维护时使用同一区域的「手动增量同步行情」：先选择市场页、
指定候选或全部已缓存标的并预览影响，再明确确认。已有标的只请求最后 5 个
交易日的重叠区间，未缓存标的才按起始日期初始化。任务进度写入数据目录，可在当前
标的完成后取消，服务重启后续跑；失败不会冲掉原缓存。

## 7. 常见问题

**Q: akshare 拉数报错/很慢？**
免费接口有频控。系统按东方财富、新浪和中证等真实上游分别排队，不同 API 与 Yahoo、
Tushare 可以并行；同一请求会合并。代理错误、限流或连续失败会进入持久化冷却，
后续任务直接降级，不会在每次启动时重复打印同一批错误。短暂错误仍按配置指数退避，
随后自动降级到 Tushare（配置 `TUSHARE_TOKEN` 后启用），最后使用已有旧缓存。
`qm fetch` 会把标准化行情写入 `data/bars/`；Tushare 原始响应另存于
`data/api_cache/tushare/`，因此服务重启后重复历史区间仍不会再次调用接口。当日早盘前
产生的响应会在 15:30 后自动失效；显式手动同步也会绕过当期接口缓存。

2000 积分档默认限制为 120 次/分钟，可通过
`QM_TUSHARE_CALLS_PER_MINUTE` 调整。建议保持保守值；批量回测应先运行一次
`qm fetch` 完成本地归档，后续研究均复用 Parquet。

板块联动的“细分题材”优先读取东方财富概念；若该接口不可用，会在 Token 具备权限时
尝试 Tushare 的 DC 概念目录。该目录当前要求 6000 积分，权限不足时页面会明确显示数据
说明并保留旧快照，不会影响 2000 积分档的行情、申万行业和 ETF 数据。

轮动工作台默认观察 5 日，可切换 1、3、5、20 日：总览用于核对四维快照、行业/题材
变化榜和共振；“行业周期”是唯一展示强势占比 × 弱势占比坐标的位置；“细分题材”提供
全目录搜索、阶段筛选及每页 50 条明细；“宽基资金”同时展示窗口净流、跟踪基准聚合和
逐只 ETF 的净值/收盘价口径。生命周期阶段始终按连续 3 日判断，不随观察窗口改变。

**Q: 想用分钟线？**
已支持 `1m/5m/15m/30m/60m`，并与日线一样按频率保存为本地 Parquet：

```bash
qm fetch --universe demo --frequency 5m --start 2026-07-01
```

Web 市场页点击标的后可在日线、60 分、15 分、5 分和 1 分之间切换。
免费源的 1 分钟历史回溯有限，适合通过每日增量拉取逐步积累；长期回测应先
确认本地时间跨度完整，并把分钟级交易成本和滑点纳入假设。

**Q: 回测收益为什么比想象低？**
默认扣了佣金/印花税/滑点，周调仓 top5 一年成本约 3-6%。这是特性不是 bug——
不扣成本的回测才是骗人的。

**Q: Mac / Windows 都能用？**
是。纯 Python + 浏览器界面，无平台绑定。桌面模式：`qm app`（启动服务并自动
打开浏览器）。也可自行打包单文件可执行程序：

```bash
pip install pyinstaller
cd packaging && pyinstaller quantmaster.spec
# 产物 dist/QuantMaster(.exe)，双击即用；仓库打 v* tag 时 CI 会自动
# 构建 Mac/Windows/Linux 三平台版本并附到 GitHub Release
```
