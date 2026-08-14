# free-stockdb / StockDB 在 QuantMaster 中的可用数据与机会

研究日期：2026-08-14

## 结论

QuantMaster 已经把本机 free-stockdb 用作日线、分钟线、盘后截面、复权因子、证券目录、板块层级和原生技术指标来源，但基本面每日估值仍在基本面缓存未命中后直接访问 AKShare/Tushare。最小且高价值的接缝是：在有明确日期范围时，先读取本机 StockDB 的 `daily_cross_section`，把已验证的 `pe_ttm`、`pb`、`total_mv` 纳入现有 `fetch_daily_indicators` 合同；没有证据的 `pe`、`dv_ratio` 保持空值，不推算、不伪造。

这项改动不等于接入 StockDB 的全部 API。实时 Tick、在线行情、财务报表、期货、指数、因子和资金流在本仓库中仍应保持显式能力边界，只有在本地合同、覆盖、单位和时间语义都被验证后再分别接入。

## 证据来源

- 上游说明：[free-stockdb API 文档](https://a.123128.xyz/docs/index.html)。文档列出行情、Tick、财务、期货、指数、因子、板块和数据库能力；在线接口适合小规模、单标的或最新数据读取，不能直接假设其可以承担全市场历史回填。
- 本地接口说明：[`runtime/free-stockdb/调用方式/python/AI策略python开发接口文档.md`](../../runtime/free-stockdb/调用方式/python/AI策略python开发接口文档.md)。其中明确区分本地 `rd` 批量历史数据与在线 API，并说明 `get_data` 的日线/分钟线/周线/月线能力，以及 Tick、财务、指数、板块和因子等在线函数的限制。
- 本地 SDK：[`runtime/free-stockdb/pybao/stock_sdk.py`](../../runtime/free-stockdb/pybao/stock_sdk.py)。已发现 `get_price`、`get_bars`、`get_ticks`、`get_last_tick`、`get_fundamentals`、`get_factor_values`、指数/期货/板块函数等导出，但其中一部分是在线代理，不能仅凭函数名推断本地批量覆盖。
- QuantMaster 适配器：[`quantmaster/data/free_stockdb_source.py`](../../quantmaster/data/free_stockdb_source.py)。当前安全接缝包括 `daily_cross_section`、`daily_many`、`intraday_many`、目录、板块、复权因子、ETF 份额和原生指标。
- 本地运行状态：[`quantmaster/data/free_stockdb_vendor_notice.json`](../../quantmaster/data/free_stockdb_vendor_notice.json)。2026-08-14 的更新校验目标为 5540，观测 5491，OHLCV 比例为 1.0，但 `complete=false`，仍有 49 个缺失标的；因此不能把“服务可用”当作“全市场覆盖完成”。

## 当前能力盘点

### 已经接入并适合继续复用

1. 盘后日线：本地 SDK 支持批量日线，适合作为研究行情与收盘后任务的主要来源。
2. 日频截面：适配器请求收盘价、成交量、成交额、流通/总市值、`pe_ttm`、`pb`、涨跌幅、换手率、量比、ST 标记等字段；字段缺失会显式保留为空，并带有本地单位/原始复权属性。
3. 分钟线：已有分钟归档和 `intraday_many`，适合自选股、ETF 和盘后复盘，不应默认对全市场逐标的补拉。
4. 复权因子：已有事件与覆盖合同，适合把原始盘后截面和研究复权价格分开管理。
5. 板块/行业：已有层级、行业映射和概念映射，可支持板块宽度、涨跌家数和组合暴露。
6. 原生指标：本地 `zb` 能计算 MA、MACD、RSI、BOLL 等，适合作为已加载行情上的计算加速器，但结果仍需与 QuantMaster 的因子命名和时间合同对齐。

### 尚未被本项目本地合同证明的能力

- `SPOT` / 最新 Tick：`DataCapability` 已有枚举，但本地 `FreeStockDBSource` 没有把实时 Tick 声明为普通能力；现有 Tick 读取位于显式实验模块，默认关闭、单标的、带配额与审计。
- 财务报表与季度指标：实验模块允许有限白名单的远程单标的读取；`fundamental_panel` 的季度 ROE/PIT 滞后合同目前仍由既有 AKShare/Tushare 路径负责。
- 期货、指数、因子、资金流：SDK 导出名称已发现，但本地批量覆盖、字段单位、交易日语义和缓存合同尚未在 QuantMaster 适配器中验证。
- 全市场完整性：当前 vendor notice 的 49 个缺失标的要求盘后任务继续使用覆盖检查和可恢复的缺失清单，不能静默填充。

## 机会排序

### P1：每日估值先用本机截面（本任务）

`fundamental_panel` 已有本地基本面 BarStore；缓存未覆盖时，读取 `daily_cross_section` 可以复用盘后已落地的估值字段，减少重复触网。当前截面只把 `pe_ttm`、`pb`、`total_mv` 作为已证实字段，`pe` 与 `dv_ratio` 保持 `NaN`。读取失败、日期不覆盖或所有估值字段为空时，继续现有回退路径。

### P1：盘后完整性与调整覆盖作为任务门禁

after-close 已有 vendor notice、更新校验和缺失标的证据。后续可让研究任务在使用全市场截面前读取同一份证据，明确区分“数据源可用”“目标日期完整”“部分标的缺失”三种状态；缺失时失败或降级，不把部分数据包装成全量数据。

### P2：板块宽度与资金/成交结构

用已有行业、概念和日频截面组合出涨跌家数、成交额集中度、换手/量比分布等盘后特征。应先定义交易日、ST、停牌和板块成员快照语义，再进入因子注册；不直接把实时在线资金流函数塞入日频回测。

### P2：分钟线与 ETF/自选股复盘

利用已归档的分钟数据为 watchlist、ETF 和盘后策略提供统一读取入口。目标应是有限标的、明确频率和本地缓存命中率，不做全市场逐标的在线补拉。

### P3：原生指标加速

先做输出与 QuantMaster 因子结果的离线等价性测试，再决定是否在已有因子引擎中使用。原生指标的计算便利性不能替代未来函数检查、复权口径和缺失数据处理。

### P3：财务、期货、指数、因子和 Tick 专用适配器

这些能力可以继续利用，但每类数据都应有独立 Issue、字段白名单、时间/单位合同、覆盖检查、速率预算和回滚路径。在线代理的单标的限制不能通过隐藏循环伪装成批量能力。

## 数据完整性与回退规则

1. 基本面 BarStore 命中时不触网。
2. StockDB 读取只在有明确日期范围时执行，避免无范围调用隐式下载全历史。
3. 只有 `symbol`、日期范围和估值字段通过检查后，才返回 StockDB 结果。
4. 未被 StockDB 合同证实的字段保留 `NaN`；不以零代替，不用价格和市值臆造股息率或静态 PE。
5. StockDB 不可用、返回空集、覆盖不足、字段合同变化或没有任何可用估值字段时，沿用现有 AKShare/Tushare 回退。
6. 季度 ROE 的披露滞后和 PIT 对齐不在本次变更中，继续由 `quarterly_to_daily` 保护。

