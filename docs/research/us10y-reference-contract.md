# US10Y 参考收益率刷新契约

Refs #556；父任务 #517 保持开放。

## 目的与实际根因

operator 提供的既有脱敏证据中，最近五个 market 任务每次 attempt 都返回
`标的缺少已确认市场身份: US10Y.RATE`，分类为 `transient_upstream` 且可重试。
本地没有该标的行情文件或 bar_meta；内置主数据的 US 声明不构成行情单位或发布时间证据。

市场分类遗漏明确的 US10Y 身份，参考路由集合也遗漏 US10Y；确定性身份失败被误判为上游瞬时失败。
修复仅针对该参考收益率，不扩大其它市场。

## 数据契约及来源

- [东方财富官方页面脚本](https://data.eastmoney.com/newstatic/js/cjsj/zmgzsyl.js)
  将 `EMG00001310` 对应美国十年收益率，并在美国数据表头明确美联储来源、单位 `%`。
- 已装 AKShare 的 `bond_em.py` 将该字段改名为 `美国国债收益率10年`，只做数值转换，不缩放。
  [AKShare 官方接口说明](https://github.com/akfamily/akshare/blob/main/docs/data/bond/bond.md)
  列出 `bond_zh_us_rate` 日期和美国十年收益率字段。
- 因此 `4.25` 保存为 `4.25`，语义为 `percent_points`，仅存 `close` 单值列。
  不生成 OHLC/成交量，不使用缺少对应单位证明的 Yahoo `^TNX` 后备。
- [Fed H.15](https://www.federalreserve.gov/releases/h15/) 的工作日 16:15 发布计划
  不证明东方财富已发布；[Treasury 的约 15:30](https://home.treasury.gov/policy-issues/financing-the-government/interest-rate-statistics)
  是报价采集时间，也不是该 provider 发布时间。

请求日期上界保守限定为纽约已过去的自然日；不据此伪造交易日历、节假日或最新发布日。
该策略可能延后看到当天已发布的数据，但不会索取纽约尚未过去的日期。
`provider_published_at` 保持未知，实际观测日期来自响应，缓存检查时间由既有存储记录。

## 改动与验证

复用现有 reference 路由、registry、BarStore 和市场面板；收益率参考禁止正式研究及交易。
旧 OHLCV/缺契约缓存显式不可用，增量只合并同契约观测，不套用股票复权缩放。
身份缺失为不可重试 `identity_missing`；provider 不可用保留受控失败及已有缓存。

`tests/test_us10y_reference.py` 离线覆盖实际失败红→绿、字段身份/百分数、fresh cache 零远端、
源失败无股票 fallback、旧缓存拒绝、增量不缩放、纽约请求上界和面板复用。
所有 provider 返回均为隔离测试数据，不能替代生产数据真实性验收。

## 生产验收要求

由唯一 operator 安装最终合并 SHA 并统一操作；本任务不读写生产，不调用实际行情 API。
核验实际返回的字段、单位、观测日期、provider、缓存 quality/meta/events；再确认自动 market 任务
不再重试身份错误、二次 fresh cache 命中没有远端调用、UI 展示实际收益率且正式资格仍为 false。
`reference_only` 警告表示参考响应已检查，不表示数据已到最新或完整发布日历已证实。
