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

## 自动化与 Bot（automation/）

- `runtime.py` 在 FastAPI lifespan 内启动 APScheduler，并通过 SQLite 租约保证多进程时只有
  一个调度器、微信长轮询和飞书长连接处于活动状态；设置热应用可单独重建调度器或某个通道，
  不会为了更新飞书凭据断开微信。
- `detector.py` 组合指数 15 分钟收益、量能、市场宽度和多指数同向性检测变盘；
  `news.py` 按持仓、自选和全市场相关性为消息评分。数据过期或证据不足时不推送。
- `store.py` 保存任务、事件、目标、策略、审计和 outbox。事件指纹、入站消息 ID、投递唯一键
  与账本意图键共同保证重启/重试不会重复执行。
- `channels/feishu.py` 使用飞书官方 Python SDK 的 WebSocket 长连接收事件，并通过消息 OpenAPI
  向 `chat_id` 发送。它是主通道：私聊和群聊各自绑定为独立目标，告警使用结构化消息卡片；
  接入诊断把凭据、运行时、长连接、入站事件和绑定状态分开呈现。
- `channels/weixin.py` 直接实现腾讯微信 ClawBot iLink 的二维码授权、`getupdates` 长轮询与
  `sendmessage`；每次回复使用对应会话的最新 `context_token`。该通道仅作为能力受限的文本提醒补充。
- `commands.py` 优先处理固定中文命令。查询只读；任务/策略变更需要管理员身份；账本和模拟调仓写入
  仅允许管理员私聊，并使用 5 分钟有效的一次性确认码。
- `analysis/stock.py` 内置 ClawHub `stock-analysis-framework` 的安全适配工作流；只接收用户明确询问的
  单一标的，组合行情、基本面、本地资讯、资金流和行业缓存生成六维 JSON 报告。Web NDJSON 与飞书
  卡片共享同一组阶段事件；飞书通过消息 PATCH 原位更新，不为每个阶段新增会话消息。
- 上游 `stock_monitor.py` / `stock_briefing.py` 不在运行时执行，因此不会隐式读取本地持仓文件或把
  持仓列表发送给新浪。逐单资金流不可用时只使用日线量价代理，并在报告中降低数据覆盖率。
- 飞书群普通消息只写入 `conversation_messages`，真实 `@QuantMaster` 才进入命令路由。未匹配
  白名单命令的问题结合相关话题、回复引用和最近轮次交给 LLM；原文过长后先压缩到
  `conversation_memories` 的结构化话题记忆，成功落库后再删除已覆盖原文。

推送策略按目标保存。三个预设只是起点，高级字段可覆盖变盘阈值、连续确认 K 线、冷却时间、
重要消息阈值和频率上限。任务失败及极高风险事件不受普通阈值过滤，但仍进入可靠发件箱留痕。

## AI Quant Lab（lab/）

- `catalog.py` 提供 48 个量价、估值、质量、行业与消息面研究起点。
- `models.py` / `store.py` 定义不可变因子版本、数据快照、验证报告、审批、部署、
  实验和可恢复任务；独立 `lab.sqlite` 使用 WAL，避免与交易账本耦合。
- `dataset.py` 按沪深300和中证500各自最近一次历史权重向前填充，再取并集，构造
  point-in-time 中证800掩码；固定候选只具备 sandbox 研究质量。
- `validation.py` 对所有因子统一执行 1/3/5/7 日 walk-forward、隔离期、IC/ICIR、
  FDR、换手/交易成本、覆盖率和已有生产因子相关性门槛。
- `ml.py` 在轻量核心外提供 Ridge 与可选 MLP、TCN、GRU、Transformer、DAE，
  共用 48 维时序特征、按日期切分、Huber 损失和早停。
- `worker.py` 既可随 Web 启动，也可独立运行；任务中断后恢复，自动任务受时间窗和
  每日计算预算约束。跨进程调度时隙由 SQLite 幂等占用。
- `server/lab.py` 只暴露安全 DSL、版本操作、任务和证据 API。人工批准是研究生产
  的强制边界，部署仅指 Champion 切换，不连接真实券商。

长耗时的市场概览与决策生成另提供 NDJSON 流式接口。数据层按标的回调真实
完成度，事件除进度外还携带可立即使用的 `partial`：市场逐标的返回卡片数据，
决策依次返回已就绪标的、牛熊、板块、候选和历史快照。最终结果用于一致性收口；
反向代理应关闭响应缓冲。

## 数据层（data/）

- **统一符号**：`600519.SH`、`00700.HK`、`^N225.JP`、`AU0.SHF`……后缀决定市场，
  `guess_market()` 据此路由数据源。
- **统一数据结构**：所有数据源输出同一种日线/分钟线 DataFrame
  （index=交易日，columns=open/high/low/close/volume/amount/turnover），
  上层模块完全不感知数据来自哪家。
- **数据源优先级与降级**：每个市场配置一列数据源（如 A 股 = [AKShare, Tushare]），
  逐个尝试，全部失败时回退本地缓存。新增数据源只需实现 `DataSource.daily()`
  并注册到 `registry._factories()`。
- **缓存**：日线每标的一个 Parquet；分钟线按 `1m/5m/15m/30m/60m`
  隔离目录并增量归档。SQLite 分别记录实际数据边界、已检查覆盖边界、检查时间、
  来源和状态。完整历史覆盖长期视为不可变；接近当前日期时按 `cache_days` 检查，
  只请求缺口及末尾 5 个交易日重叠窗口。
- **前复权增量**：以重叠收盘价的稳定中位比例校准动态 qfq 基准，只缩放 OHLC，
  不缩放 volume/amount。比例不稳定或响应内部过稀时拒绝写入并尝试备用源。显式全量
  刷新只有在未丢失已知交易日且密度校验通过后才原子替换单标的缓存。
- **并发与数据源韧性**：市场面板并发加载，但按真实上游拆分队列；东方财富、新浪、
  中证、Yahoo 和 Tushare 可互相并行，同一通道限制并发，前台任务优先于维护任务。
  请求按精确参数合并。代理错误、限流或连续失败会持久化熔断 5/15/30 分钟，冷却期
  直接降级并聚合日志。AKShare 短暂错误仍按配置指数退避；A 股日线随后
  降级到 2000 积分 Tushare 的 `daily + adj_factor`，指数使用 `index_daily`。
  Tushare 统一匀速限流，原始响应按接口和精确参数缓存为 Parquet；已结束的历史
  区间长期复用。当日收盘前产生的响应在 15:30 后失效，显式增量同步绕过当期
  接口缓存，避免把“请求到今天但只返回昨天”误认为最新行情。
- **批量与维护**：Yahoo 全球参考标的使用单次批量下载；设置中心的增量同步任务
  持久化范围、逐标的进度和失败摘要，可在标的边界取消，服务重启后手动续跑。
- **基本面与行业降级**：AKShare 估值/ROE 失败时使用 `daily_basic` /
  `fina_indicator`；行业优先使用申万 2021 `index_classify + index_member_all`，
  按一级行业分批拉取，规避单次 2000 行上限并缓存 30 天。
- **分钟线口径**：本地归档使用不复权价格，避免后续增量与变化后的前复权
  基准拼接产生假跳空；1 分钟免费源回溯有限，需要每日运行 `qm fetch` 积累。

## 研究生产层（research/）

- `contracts.py` 定义不可变 `ResearchSpec`、`ArtifactRef`、`ExecutionPlan`
  和 `RunManifest`；语义版本会进入存储列名，旧因子计算结果不会被新定义覆盖。
- `lake.py` 在 `data_root/research_lake` 中按 kind / asset / frequency / dataset /
  trade_date 组织 Zstd Parquet；SQLite 目录保存模式哈希、内容 SHA-256、输入
  血缘、规格版本和 run id。写入经过临时文件、fsync 和原子替换。
- `adapters.py` 用交易日截面接口生产 A 股、ETF 和期货基线，区分未配置、
  缺少权限与短暂失败。高级分钟接口只报能力，不会阻塞可用的日线基线。
- `engine.py` 先用官方日历和 lookback/lookahead 生成 dry-run DAG，再按 provider 合并
  公共扫描。增量运行会重算数据集的修订窗口，分区租约避免并发重复写入。
- `providers.py` 内置六个跨资产截面因子、四个前瞻标签、QM_STYLE_V1 五风格
  暴露和期货主连前比例复权。QM_STYLE_V1 是透明的本项目风格基线，不声称是
  Barra CNE6 的完整复刻。
- `kernel.py` 以 Python 结果为规范实现；可选 `_quantmaster_kernel` 用 PyO3/Rayon
  加速排名、稳健/加权标准化和滚动统计。`auto` 选择加速内核，并且仅在不可用时
  显式记录一次 Python 回退原因。
- `jobs.py` 与 `server/research.py` 提供持久化后台任务、取消、中断恢复和
  `completed_with_errors`；变更类 REST 端点继续受本机来源和 CSRF 约束。

详细操作和数据口径见 [研究生产流水线](research_pipeline.md)。

## 市场状态与决策层（market/、decision/）

- `market/regime.py`：逐日计算 MACD、资金量比、波动、牛熊分和五档趋势状态；
  候选状态叠加上涨家数、站上 MA20 比例，并按行业映射生成板块强弱。
- “未来”输出 1/3/5/7 日概率、期望收益和置信度，明确标为规则型展望，
  不把未知未来包装成事实；同时报告历史样本数、方向准确率与 Brier 概率误差，
  方便长期检验展望是否真的有用。
- `decision/swing.py`：趋势、MACD、价格位置、资金量、低波动截面合成；
  熊市自动降至约 30% 敞口，生成 1–7 日持有、止损止盈与每日候选。
- `decision/storage.py`：SQLite 保存每次真实生成的选股快照；回测研究可以对照
  “当时的信号”，避免用后来重算的结果冒充历史决策。
- Web 决策图按自然时间提供 7D、14D、1M、3M、6M、1Y、3Y、5Y、10Y
  观察窗口；最长返回 2600 个交易日并复用同一 ECharts 实例做克制更新过渡。
- `SwingStrategy` 使用同一套逐日历史分数进入 A 股规则回测，信号统一为
  T 日收盘生成、T+1 开盘成交。
- `load_bar_panel(..., frequency=...)` 为日线和分钟线提供相同的多标的面板范式，
  分钟级特征研究无需另写数据拼接代码。

## 标的列表与持仓（portfolio/）

- `AssetListStore` 用 SQLite 独立保存自选与重点关注；A 股六位代码会自动补交易所后缀。
- 持有列表直接来自 `Ledger.positions()`，不维护第二份持仓真相。
- 三类列表的报价只读 `BarStore` 本地缓存，浏览列表不会触发 AKShare 或消耗
  Tushare 调用次数；新增成交后前端会同步刷新持有列表。

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
- **news_sources.py**：持久化来源、运行记录和 HTTP 条件缓存。声明式采集只支持
  RSS、JSON 点号路径和 HTML CSS 选择器；每次请求及重定向都校验公网地址，响应限制
  5MB，鉴权凭据不会随跨域跳转或详情链接发送。
- **crawler.py**：先规范化、指纹去重并写入 SQLite，再把新资讯放入 LLM 批量标注队列。
  模型不可用不影响归档；失败按 1/5/30 分钟退避，重复资讯不重复消耗模型额度。
- **sentiment.py**：`news_sentiment` 按首次获取时点而非来源声称的发布时间对齐，聚合
  情绪、置信度、重要度与来源权重；盘后消息顺延到下一交易日并按自然日半衰衰减。
- **server/news.py**：本机只读查询、来源管理、解析预览、手动采集与重新标注 API；
  所有写操作复用设置中心的本机限制与 CSRF 防护。

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
4. **新资讯源**：优先在「设置 → 资讯来源」添加 RSS、JSON 或 HTML 声明式规则；
   只有需要专用协议的可信内置来源才新增适配器并登记进 `crawler.SOURCES`。
5. **桌面打包**：Web 界面即 UI，后续可用 Tauri/Electron/pywebview 包一层壳，
   或 PyInstaller 打包 `qm serve`。
