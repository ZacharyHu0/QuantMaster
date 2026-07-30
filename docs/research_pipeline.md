# 研究生产流水线

QuantMaster 的研究生产层将“取数—因子—标签—风险—模型—回测”统一为可计划、
可续跑、可复现的本地 Artifact 流程。它与原有按标的 `BarStore` 并存：旧 API 继续服务
交互式分析，新研究湖为全市场截面计算提供按日分区和版本血缘。

## 快速开始

```bash
pip install -e ".[data,tushare,dev]"
qm data capabilities
qm data catalog
qm data plan --assets stock,etf,future \
  --specs cross_asset_core,forward_returns,qm_style_v1 \
  --start 2022-01-01 --end 2026-07-30
qm data sync --assets stock,etf,future \
  --specs cross_asset_core,forward_returns,qm_style_v1 \
  --start 2022-01-01 --end 2026-07-30 --backend auto
```

PowerShell 中请使用反引号续行，或将参数写在同一行。历史初始化用 `historical`；日常生产改用
`--mode incremental`，它会为上游修订重算最近窗口。`plan` 不执行任务，可先检查权限、
依赖、分区数和估算行数。

## 数据基线与口径

| 资产 | 默认日线数据集 | 主要口径 |
| --- | --- | --- |
| A 股 | `stock_bars`、`stock_adj_factor`、`stock_daily_basic` | 按交易日全市场；vol 从手转股，amount 从千元转元；研究价由未复权价与复权因子构造 |
| 场内 ETF | `etf_basic`、`etf_bars` | 上市基金快照 + `fund_daily`；高级复权不是启动条件 |
| 期货 | `future_contracts`、`future_bars`、`future_main_mapping` | 合约目录、日线与主力映射；换月日用前一交易日重叠价做前比例复权 |

日线默认以官方交易日历生成任务。日历接口暂时不可用但本地已有真实分区时，计划器优先
复用本地交易日；仅在两者都不可用时退到工作日并显式告警。分钟数据权限以能力徽标呈现，
不会让可用的日线任务失败。

## 版本与存储

默认路径是 `<data_root>/research_lake`：

```text
research_lake/
  raw/<asset>/<frequency>/<dataset>/<year>/YYYYMMDD.parquet
  factors/<asset>/<frequency>/<year>/YYYYMMDD.parquet
  labels/<asset>/<frequency>/<year>/YYYYMMDD.parquet
  risk/<asset>/<frequency>/QM_STYLE_V1/<year>/YYYYMMDD.parquet
  models/<asset>/<frequency>/<model>/<year>/YYYYMMDD.parquet
  runs/<run_id>/manifest.json
  _meta/research.sqlite
```

原始数值保留 float64，派生数值默认 float32，日线主键是 `(trade_date, symbol)`。每个分区的
SQLite 记录包含 schema hash、文件 SHA-256、输入哈希、spec version 和 run id。同一供应器的
多个输出共用一次面板扫描；列名携带语义版本，因此新规格不会静默改写旧实验。

## 内置派生产物

- `cross_asset_core`：20 日动量、5 日反转、20 日实现波动率、成交量比、价量相关和 Amihud 非流动性；每日截面稳健标准化。
- `forward_returns`：1/3/5/7 个交易日前瞻收益，只作标签并通过 lookahead 明示前瞻窗口。
- `qm_style_v1`：SIZE、VALUE、MOMENTUM、VOLATILITY 和 LIQUIDITY。截面先行业内补值，
  再用市场中位数兜底、5-MAD 缩尾和根号市值加权标准化，同时保留 raw 列。

QM_STYLE_V1 是用于中性化、暴露监控和归因的透明基线，不是 Barra CNE6 全部风格、行业、
因子收益与特异风险模型的复刻。原有 48 个 Quant Lab 策展因子保持可用；新流水线只为已声明
生产 provider 的规格生成新 Artifact。

每个因子生产运行可写出覆盖率、Pearson IC、RankIC、按年表现、标签衰减、分位数收益、
多空差与换手效率表，并与 run manifest 同目录保存。

## 在回测和模型中使用

Artifact 引用格式为：

```text
artifact:<factor|risk|model>:<stock|etf|future>:<id>@<semver>
```

```bash
qm backtest \
  --factor artifact:factor:stock:cross_momentum_20d@1.0.0 \
  --universe demo --start 2022-01-01 --end 2026-07-30
```

`FeatureBatchProvider` 可把多个版本锁定的 Artifact 组成长表或 `[N,T,F]` 张量，返回缺失 mask，
并用实际分区内容哈希缓存重叠请求。Quant Lab 学习模型的样本外预测会自动发布为
`artifact:model:stock:<slug>@1.0.0`，同时保留原模型 manifest 和验证状态。

## 任务、失败与恢复

- 设置中心和 `POST /api/research/data/jobs` 在后台执行已确认的计划；客户端每 2 秒读取持久化进度。
- 取消在分区边界生效，已原子落盘的分区保留；服务重启时正在运行的任务标记为 `interrupted`。
- `qm data resume <job_id>` 只重做尚未成功的任务。部分数据集失败但其他产物完成时，状态是
  `completed_with_errors`，不会伪装为全部成功。
- 完整 manifest 记录计划哈希、软件版本、内核、输入/输出分区和诊断，可用于追溯任意回测输入。

## Rust 加速

Python 是规范实现，未安装 Rust 也能完整运行。本地开发可选：

```bash
pip install -e ".[rust]"
maturin develop --release --manifest-path rust/quantmaster-kernel/Cargo.toml
qm data capabilities
```

`--backend auto` 在扩展可用时选 Rust，否则显式记录回退原因；`python` 用于对照，`rust` 则在扩展
缺失时立即报错。CI 在 Windows、macOS 和 Linux 编译扩展，并对 NaN、常数、权重与滚动边界检查
Python/Rust 奇偶性。

## 设计来源与边界

本层借鉴 [YuminQuant2026](https://github.com/YuminQuant/YuminQuant2026) 的全流程分层、版本化产物、
跨资产预留和 Rust/Python 混合计算思路，但按 QuantMaster 现有的安全边界、本地优先存储、
FastAPI/CLI 和回测契约独立实现，没有运行上游脚本或整包复制其代码。此版也不声称已迁移上游
400+ 因子、完整 CNE6 或事件驱动策略引擎；它交付的是可扩展生产底座与一组可验证基线。
