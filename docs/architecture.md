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
- `analysis/stock_research.py` 把单一标的研究拆成并发取证、六维独立规则/模型复核和最终交叉复核；
  快速模式执行联网首轮复核，深度模式追加三轮定向搜索、逐维反方审查和独立证伪终审，并用确定性
  完整度门槛披露证据缺口。结构化数值由本地规则计算，模型只能引用本任务的 evidence ID；面向用户的
  文案字段必须通过纯文本校验，结构化模型信封不会直接进入 Web 或飞书。`analysis/stock_jobs.py` 将它注册为
  `market.stock_analysis`，复用 `runtime/jobs.py` 的不可变规格、幂等键、租约、事件、取消、重试、
  严格 JSON 产物和损坏产物修复队列，不建立个股专属任务数据库。
- Web 通过 `/api/v1/market/stock-analyses` 提交，再轮询 `/api/v1/jobs/{job_id}` 及其事件；刷新页面只
  恢复 job ID，后台任务不依赖浏览器连接。飞书把同一 job 与原卡片 `message_id` 持久化到 Automation
  outbox，每维完成后原位更新；终态主卡保留六维结论，完整证据按不超过 28 KB 的编号附录续投。
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

长耗时的市场概览与决策生成另提供 NDJSON 流式接口；个股分析不使用流式连接，而使用可恢复的统一
任务事件。数据层按标的回调真实
完成度，事件除进度外还携带可立即使用的 `partial`：市场逐标的返回卡片数据，
决策依次返回已就绪标的、牛熊、板块、候选和历史快照。最终结果用于一致性收口；
反向代理应关闭响应缓冲。

## 数据层（data/）

- **统一符号**：`600519.SH`、`00700.HK`、`^N225.JP`、`AU0.SHF`……后缀决定市场，
  `guess_market()` 据此路由数据源。
- **统一数据结构**：所有数据源输出同一种日线/分钟线 DataFrame
  （index=交易日，columns=open/high/low/close/volume/amount/turnover），
  上层模块完全不感知数据来自哪家。
- **数据源优先级与降级**：A 股默认使用用户安装的 free-stockdb SDK 和本地数据，
  随后回退 AKShare、Tushare 与本地缓存；主源可在设置中切换。QuantMaster 不捆绑
  free-stockdb 程序、数据或同步源。free-stockdb 是 Tushare 数据的第三方本地分发，
  不作为独立上游交叉验证。数据源显式区分日线、日频截面、分钟线、收盘快照、实时 Tick、
  证券目录、复权因子、板块层级、ETF 份额与原生指标；诊断同时披露安装、连接、数据就绪、
  验证、资产类别、频率、覆盖和截至日期。调度器只调用满足请求能力的来源。新增数据源
  需要实现对应方法、声明 capability 并注册到 `registry._factories()`。能力矩阵和各市场
  实际优先级可在 `/api/v1/diagnostics` 或 `qm doctor --deep` 中查看。
- **free-stockdb 托管**：`data/free_stockdb_runtime.py` 只管理用户自行安装的完整发行包，
  不下载程序、数据或同步源。目录、启停、盘后更新时间均可在设置中调整；热重载监督
  进程通过 SQLite 邮箱独占进程控制，Web worker 只提交命令和消费幂等结果事件。自动
  更新采用真实交易日和全市场覆盖验收，失败期间先恢复服务再有限重试。控制台关闭会
  主动停止 sidecar；异常退出后的孤儿进程只有在 PID、创建时间、路径和 owner 身份都
  核验一致时才回收，外部进程绝不终止。更新期间其他请求按注册优先级降级，运行状态
  进入深度诊断。
- **盘后研究快照**：`after_close/` 以证券主数据为全 A 股入口，批量读取日频截面并执行
  最新交易日、证券/OHLCV 覆盖、板块目录和覆盖骤降门禁。板块及候选由版本化的
  QuantMaster 公式计算，正式结果以输入哈希写入不可变 SQLite 快照，同时将截面和板块
  成员关系写入研究湖。历史重放只读取冻结快照；缺少点时板块生效日期时拒绝用今天的分类
  强制重算过去。1/3/5/7 日标签只在未来交易日实际发生后生成，并显式保留全市场基线、
  中证 800 点时成员缺失、换手、集中度和容量口径。
  中证800成员通过 Tushare `index_weight` 拉取沪深300与中证500月度快照，按生效日写入
  `raw/stock/1d/csi800_membership` 研究湖分区；盘后、Quant Lab 与回放共用内容哈希和
  分区血缘，缺权限时只把该基线标为不可用。
- **free-stockdb 摄取**：`data/free_stockdb_ingest.py` 以 SDK/原生程序哈希、数据代次、
  证券主数据及证券/退市/板块目录哈希确定缓存身份，原子发布未复权日线、复权因子、ETF
  日线/分钟证据和冻结目录。最近成功清单按发布时间保留，内容块只在不再被清单引用时回收；
  研究价由冻结因子按版本化公式派生，原始价从不被覆盖。
- **ETF 研究**：`rotation/etf_research.py` 保留全部沪深可交易 ETF 并排除 LOF，按宽基、
  行业主题、策略、QDII、债券、商品、货币和其他类别分别排名。分钟证据不参与日频排名；
  stockdb 份额保留观察日期和滞后语义，当日 Tushare 份额可用时优先采用。
- **缓存**：日线每标的一个 Parquet；分钟线按 `1m/5m/15m/30m/60m`
  隔离目录并增量归档。SQLite 分别记录实际数据边界、已检查覆盖边界、检查时间、
  来源和状态。完整历史覆盖长期视为不可变；接近当前日期时按 `cache_days` 检查，
  只请求缺口及末尾 5 个交易日重叠窗口。
- **前复权增量**：以重叠收盘价的稳定中位比例校准动态 qfq 基准，只缩放 OHLC，
  不缩放 volume/amount。比例不稳定或响应内部过稀时拒绝写入并尝试备用源。显式全量
  刷新只有在未丢失已知交易日且密度校验通过后才原子替换单标的缓存。
- **并发与数据源韧性**：市场面板并发加载，但按真实上游拆分队列；东方财富、新浪、
  中证、Yahoo 和 Tushare 可互相并行，同一通道限制并发，前台任务优先于维护任务。
  请求按精确参数合并，并从首次入队起共用 `provider_timeout` 硬截止；超时调用立即
  打开熔断并降级，仍在退出的 SDK 线程迟到后不得把通道改写为成功。诊断会报告各通道
  active、waiting、expired 与累计超时。代理错误、限流或连续失败会持久化熔断
  5/15/30 分钟，冷却期直接降级并聚合日志。AKShare 短暂错误仍按配置指数退避；A 股日线随后
  降级到 2000 积分 Tushare 的 `daily + adj_factor`，指数使用 `index_daily`。
  Tushare 统一匀速限流，原始响应按接口和精确参数缓存为 Parquet；已结束的历史
  区间长期复用。当日收盘前产生的响应在 15:30 后失效，显式增量同步绕过当期
  接口缓存，避免把“请求到今天但只返回昨天”误认为最新行情。
- **批量与维护**：Yahoo 全球参考标的使用单次批量下载；设置中心的增量同步任务
  持久化范围、逐标的进度和失败摘要，可在标的边界取消，服务重启后手动续跑。
- **基本面与行业降级**：AKShare 估值/ROE 失败时使用 `daily_basic` /
  `fina_indicator`；默认从 free-stockdb 板块索引读取申万一级映射，失败后使用
  Tushare `index_classify + index_member_all` 或东方财富，并缓存 30 天。
- **题材多源**：选择 free-stockdb 为主源时，概念目录直接读取其本地板块索引；不可用时
  使用 AKShare 东方财富完整概念目录，再尝试
  Tushare `dc_index + dc_member`（当前需 6000 积分）。两套目录按整套口径切换，不混合
  板块代码；权限型接口使用独立健康通道，失败不会熔断 Tushare 核心行情，双源都失败
  时保留上次可用目录。
- **分钟线口径**：free-stockdb SDK 提供 1 分钟；HTTP 回退的 5/15/30/60 分钟由
  QuantMaster 按 A 股上午/下午交易时段聚合，绝不跨午休合并。其他来源作为后备；
  本地归档统一使用不复权价格，避免复权基准变化造成假跳空。

## 研究生产层（research/）

- `contracts.py` 定义不可变 `ResearchSpec`、`ArtifactRef`、`ExecutionPlan`
  和 `RunManifest`；语义版本会进入存储列名，旧因子计算结果不会被新定义覆盖。
- `lake.py` 在 `data_root/research_lake` 中按 kind / asset / frequency / dataset /
  trade_date 组织 Zstd Parquet；SQLite 目录保存模式哈希、内容 SHA-256、输入
  血缘、规格版本和 run id。写入经过临时文件、fsync 和原子替换。
- `adapters.py` 优先读取已验证的 stockdb `ingest_id` 生产 A 股原始行情、复权因子、
  每日指标与 ETF 行情分区，再按缺失行显式回退 Tushare 直连；两条路径都标记同一 Tushare
  上游及各自 distribution。未配置、未安装、数据未就绪、缺少权限与短暂失败分别报告。
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

- `market/regime.py`：逐日计算 RSI(14)、MACD、资金量比、波动、牛熊分和五档趋势状态；
  候选状态叠加上涨家数、站上 MA20 比例，并按行业映射生成板块强弱。
- `market/fear_greed.py` 从 CNN 官方 graphdata 读取 Fear & Greed，以 30 分钟派生缓存
  隔离弱网；失败时使用最近成功值或显式降级。该指数只作为美国/全球风险背景，
  大盘和板块分别使用自身日线 RSI；RSI<22 标记“加仓抄底观察”，再叠加 CNN<10
  标记“罕见大底机会”，两者均为经验提示而非自动交易指令。
- “未来”输出 1/3/5/7 日概率、期望收益和置信度，明确标为规则型展望，
  不把未知未来包装成事实；同时报告历史样本数、方向准确率与 Brier 概率误差，
  方便长期检验展望是否真的有用。
- `decision/hybrid.py` 提供 Hybrid 的可解释规则评分基线并融合获批 Champion：
  综合趋势、MACD、价格位置、资金量和低波动截面，统一生成策略快照。
- `decision/storage.py`：SQLite 保存每次真实生成的选股快照；回测研究可以对照
  “当时的信号”，避免用后来重算的结果冒充历史决策。
- Web 决策图按自然时间提供 7D、14D、1M、3M、6M、1Y、3Y、5Y、10Y
  观察窗口；最长返回 2600 个交易日并复用同一 ECharts 实例做克制更新过渡。
- `HybridDecisionStrategy` 固化画像与策略快照进入 A 股规则回测，信号统一为
  T 日收盘生成、T+1 开盘成交；旧 Swing 执行器已移除。
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
- **模拟盘**（`paper_accounts.py`）：同一策略代码在「现在」运行，虚拟资金记账，
  与实盘账本共用 Ledger 结构。常驻 worker 只处理共享交易日解析器确认的数据就绪日；
  `paper_auto_runs` 用随机 lease token、30 秒心跳和 fencing 防止旧 worker 写回，成交继续
  依靠 Ledger 幂等键去重。策略来源告警与瞬时运行告警分栏保存，兼容 `warning` 为汇总。
- **交易日**（`trading_sessions.py`）：优先使用上交所官方日历，其次使用已校验的研究湖
  分区或多标的行情目录；绝不以普通工作日猜测春节、国庆等休市日。冷启动没有可信证据时
  返回可行动的安全跳过原因，模拟盘和轮动新鲜度共用同一结论。

## AI 层（ai/）

- **llm.py**：通过生命周期管理的共享 httpx 连接池调 REST，`provider` 三选一：`anthropic` /
  `openai` / `openai-compatible`（DeepSeek、通义、Kimi、GLM、Ollama 等
  一切 OpenAI 协议网关，改 `base_url` 即可）。通用调用与资讯标注分别进入独立 FIFO
  并发队列，资讯可在设置中心提高并行批次数而不挤占交互分析；排队超时产生可重试结构化
  错误。连接池在配置端点切换后延迟关闭旧连接，在应用退出时统一释放。
- **news_claims.py**：可重建的 `news_analysis_claims` 只保存 owner、随机 token、任务类型、
  租约和心跳。每批在 `BEGIN IMMEDIATE` 内原子认领，完成/失败必须通过 token fencing；
  过期 worker 无权覆盖接管者结果。
- **news_sources.py**：持久化来源、运行记录和 HTTP 条件缓存。声明式采集只支持
  RSS、JSON 点号路径和 HTML CSS 选择器；每次请求及重定向都校验公网地址，响应限制
  5MB，鉴权凭据不会随跨域跳转或详情链接发送。
- **crawler.py**：先规范化、指纹去重并写入 SQLite，再把新资讯放入可独立配置并发数的
  LLM 批量标注队列。
  模型不可用不影响归档；失败按 1/5/30 分钟退避，重复资讯不重复消耗模型额度。“全部重试”
  固定启动时最大资讯 ID，再逐批认领；人工失败/死信恢复绕过自动健康与退避门禁。
- **sentiment.py**：`news_sentiment` 在处理完成后按已验证的资讯发布时间回填，聚合
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
| free-stockdb | SDK、分钟行情和行业/概念板块本地数据 | 用户自行安装和维护数据，QuantMaster 只调用 |
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
