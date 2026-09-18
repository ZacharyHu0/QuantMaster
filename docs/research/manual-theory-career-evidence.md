# 手册理论深化与职业案例：来源及项目证据

本笔记为内置手册第 05—24 章的编辑证据索引。手册中的“澄川工业科技”“智维云”及其全部
财务、交易和市场数据均为教学合成例，不代表真实公司、行业预测或投资结论。外部来源用于
核对定义和方法；项目行为以代码为准。

## 数学、统计与金融理论

- 条件期望在平方损失下的最优预测，以及它与线性最小二乘投影的区别：
  [MIT Introduction to Probability 讲义](https://ocw.mit.edu/courses/res-6-012-introduction-to-probability-spring-2018/55e1f98550110cea4b1980ec9df6224d_HL7qwWvON4.pdf)、
  [MIT Linear Algebra — Projection and Least Squares](https://ocw.mit.edu/courses/18-06sc-linear-algebra-fall-2011/pages/least-squares-determinants-and-eigenvalues/projection-matrices-and-least-squares/)。
- 长期复利与对数增长：
  [Cover, Log Optimal Portfolios](https://statistics.stanford.edu/technical-reports/log-optimal-portfolios)。
  手册的 +50%/−40% 数例为本次原创推演。
- 状态价格、复制与风险中性权重：
  [MIT 15.450 Lecture 1](https://www.ocw.mit.edu/courses/15-450-analytics-of-finance-fall-2010/219406be97b96bc540e0be6aa778c0b7_MIT15_450F10_lec01.pdf)。
- 反变量应以独立配对均值估计标准误：
  [Columbia antithetic variates notes](https://www.columbia.edu/~ks20/4404-Sigman/4404-Notes-ATV.pdf)。
  手册代码按 `m` 个配对均值计算 `s_A/sqrt(m)`，不把 `2m` 个相关 payoff 当作独立样本。
- 异方差、自相关与 HAC：
  [Newey–West working paper](https://www.nber.org/papers/t0055)。
- BH 的独立检验结果及依赖扩展：
  [Benjamini–Hochberg 1995](https://rss.onlinelibrary.wiley.com/doi/abs/10.1111/j.2517-6161.1995.tb02031.x)、
  [Benjamini–Yekutieli 2001](https://projecteuclid.org/euclid.aos/1013699998)。
  项目当前多周期 p 值存在重叠标签依赖，因此手册把 q 值称为筛查统计量，没有声称严格满足
  任意依赖下的名义 FDR 控制。
- 离散损失下的一般 CVaR/ES 与优化表达：
  [Rockafellar–Uryasev](https://sites.math.washington.edu/~rtr/papers/rtr187-CVaR2.pdf)。
- YTM 实现收益的持有、违约与再投资条件：
  [CFA Institute — Interest Rate Risk and Return](https://www.cfainstitute.org/insights/professional-learning/refresher-readings/2026/interest-rate-risk-and-return)。
- CIR 零边界与 Feller 条件：
  [Federal Reserve FEDS 2008-31](https://www.federalreserve.gov/pubs/feds/2008/200831/index.html)。

## 企业财务、VC 与 PE

- 三表关系与附注阅读：
  [SEC Beginner’s Guide to Financial Statements](https://www.sec.gov/about/reports-publications/investorpubsbegfinstmtguide)。
- NOPAT、ROIC、资本成本等定义：
  [Damodaran definitions](https://pages.stern.nyu.edu/~adamodar/New_Home_Page/definitions.html)。
- FCFF/FCFE 的请求权与折现率匹配：
  [Damodaran cash-flow teaching note](https://pages.stern.nyu.edu/~adamodar/New_Home_Page/littlebook/cashflows.htm)。
- 稳定增长、再投资与超额回报：
  [Damodaran terminal-value questions](https://pages.stern.nyu.edu/~adamodar/New_Home_Page/valquestions/termvalueexreturns.htm)。
- VC 条款是交易合同选择，不是统一默认：
  [NVCA model legal documents](https://nvca.org/model-legal-documents/)。
- LP 绩效定义与报告：
  [ILPA private-equity glossary](https://ilpa.org/resources-tools/private-equity-101/private-equity-glossary/)、
  [ILPA Performance Template Guidance](https://ilpa.org/wp-content/uploads/2025/01/ILPA-Performance-Template-Suggested-Guidance-Granular-Methodology.pdf)。

## 战略、单位经济与尽调

- 行业结构与相对位置影响盈利能力：
  [HBS Institute for Strategy — Five Forces](https://www.isc.hbs.edu/strategy/business-strategy/Pages/the-five-forces.aspx)。
- 真实公司披露中的同 cohort NRR 定义：
  [Snowflake 424B4](https://www.sec.gov/Archives/edgar/data/1640147/000162828020013667/snowflake424b4.htm)。
- 商业尽调的交易论点、独立市场视角与 deal breakers：
  [Bain healthcare private-equity report](https://www.bain.com/contentassets/8fc82564bdb14099b92f64d064b36d8c/report_healthcare_private_equity_and_corporate_m26a.pdf)。
- Case 面试看结构、假设、分析、计算、沟通与判断：
  [BCG case preparation](https://careers.bcg.com/global/en/case-interview-preparation)、
  [McKinsey interviewing](https://www.mckinsey.com/careers/interviewing)。

## QuantMaster 实现边界

- `quantmaster/factors/analysis.py::forward_returns` 定义 `close[T+h]/close[T]-1`；
  `information_coefficient` 对同日横截面求 Pearson 或平均秩后的 Pearson。它是预测诊断标签。
- `quantmaster/lab/validation.py::_walk_forward_ic` 和
  `quantmaster/lab/multihorizon.py::make_multi_horizon_samples` 延续 close-to-close 标签；支持周期由
  `quantmaster/lab/horizons.py::SUPPORTED_HORIZONS` 定义为 1/3/5/7/10/20/30。
- `quantmaster/backtest/engine.py::run_backtest` 把 T 日收盘目标权重存为 pending，在下一实际交易日
  开盘执行。统计标签、首次可执行价格和持仓收益区间不可混称。
- `quantmaster/lab/validation.py::_p_value` 使用 `std/sqrt(n)` 的正态近似；随后普通 BH 不会自动
  修复重叠标签带来的序列依赖。
- `quantmaster/backtest/metrics.py::performance_metrics` 的 Sharpe 分子使用 CAGR 减年无风险利率；
  `var_95`/`cvar_95` 保留收益左尾的负号，并非一般正损失离散 ES 的完整实现。
- `quantmaster/data/free_stockdb_source.py` 的盘后截面包含行情、成交、市值及部分估值字段；
  `quantmaster/data/fundamentals.py` 标准面板覆盖 PE/PE_TTM、PB、股息率、总市值、ROE，并声明
  披露滞后与非完整 PIT 数据库限制。
- `quantmaster/analysis/stock_research.py` 可选深度基本面采集公开三表、财务指标、预告/快报和
  主营构成；`quantmaster/analysis/stock_evidence.py` 验证来源 URL、证据标识与内容哈希。
  这些能力追踪公开证据，不验证私企客户 cohort、合同真实性、cap table 或收购债条款。
- `quantmaster/analysis/stock.py` 的评分、行业相对收益、beta 与相关性用于公开市场筛查，不能
  证明产品、政策与商业结果之间的因果关系。
