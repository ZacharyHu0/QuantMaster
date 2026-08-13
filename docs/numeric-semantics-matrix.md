# QuantMaster 数值语义矩阵

本矩阵记录可进入 provider 边界合同的证据。空缺表示未获官方定义；不得按字段名或
数值大小补猜。普通页面可显示已确认字段和诊断原因，正式研究、组合换算与模拟撮合
必须等待全部必需语义一致。

| 来源/接口 | price type | currency / unit | volume / amount | ratio | 时间/因子/用途 |
|---|---|---|---|---|---|
| StockDB raw ingest | `raw` | A股证据为 CNY/share | share / CNY | pct/amplitude/turnover 为 percent points | 983,296 行实例；127 标的缺因子；`pre_close` 定义冲突，禁止正式依赖 |
| StockDB daily fq | requested qfq，尚未获完整 provider 定义 | A股 CNY/share | share / CNY | 同上 | 因子仅事件行，缺发布时间、公司行为范围与每标的完整性；只预览 |
| Tushare `daily` | `raw` | A股价格随 instrument=CNY | 手→provider边界转股；千元→元 | `pct_chg` percent points | 交易日；停牌无行 |
| Tushare `pro_bar` | qfq=`forward_adjusted`; hfq=`backward_adjusted`/分红再投语义 | 同 daily | 同 daily | endpoint-specific | qfq 锚点为请求 end_date；不得跨锚点拼接 |
| Tushare `adj_factor` | factor | 无 | 无 | ratio | 适用交易日明确；09:15–09:20 更新窗口；公司行为范围/修订字段缺失 |
| Tushare `fut_daily` | raw close 与 settlement 分列 | 合约规格决定 | 手 / 万元 | endpoint-specific | 具体合约；不可沿用股票单位 |
| Yahoo chart/history | raw OHLC；`adjclose` 为 split+cash-dividend adjusted | `meta.currency` | volume 单位未定义；无 amount | 无统一字段 | timestamp+exchange timezone；FX base/quote 没有独立字段；正式研究停用缺失维度 |
| AKShare A股东财 | raw/qfq/hfq | 成交额/涨跌额元；OHLC币种需 instrument 证据 | 手 / 元 | 振幅/涨跌幅/换手 percent points | qfq/hfq 历史会变化；成交额是否调整未定义 |
| AKShare 港股东财 | raw/qfq/hfq | HKD | 股 / HKD | percent points | 港股日线时区未定义 |
| AKShare 美股东财 | raw；requested qfq/hfq 未验证 | USD | 股 / USD | percent points | 官方警告复权参数可能不生效，正式 adjusted 序列阻断 |
| AKShare `forex_hist_em` | 未说明 bid/ask/mid/fixing | 未定义 base/quote 方向 | 无 | 未定义 | 日线截点/时区未定义，禁止组合 FX 换算 |
| AKShare `*0` 连续期货 | `continuous_futures` | 需交易所规格 | 未定义 | 未定义 | roll/adjustment 未定义；仅研究候选，绝不撮合 |

## 已确认交易所规格示例

| instrument family | exchange | quote unit | multiplier | tick | currency |
|---|---|---:|---:|---:|---|
| AU | SHFE | CNY/gram | 1000 gram/lot | 0.02 | CNY |
| RB | SHFE | CNY/tonne | 10 tonne/lot | 1 | CNY |
| SR / RM | CZCE | CNY/tonne | 10 tonne/lot | 1 | CNY |
| C | DCE | CNY/tonne | 10 tonne/lot | 1 | CNY |
| LH | DCE | CNY/tonne | 16 tonne/lot | 5 | CNY |
| IF | CFFEX | index point | CNY 300/point | 0.2 point | CNY |
| IM | CFFEX | index point | CNY 200/point | 0.2 point | CNY |

规格必须按交易所、具体合约及适用日版本化连接；family 行不能直接证明任意历史合约。
`close`、daily settlement 与 delivery settlement 独立保存。volume 还必须携带
single/double-side counting，amount 携带 currency、scale 和 counting。

## 财报合同

财报按 statement/table/field 保留 provider 原始 currency 与 scale。SEC XBRL 事实以
unitRef 绑定 USD/shares 等单位；Apple 同表存在 USD×1e6、shares×1e3、USD/share。
HKEX 披露可为 HKD'000 或 RMB'000，EPS 又可为 HK cents。A股披露可逐表明确人民币元。
上市地和数量级均不是换算依据。

## 官方证据

- Tushare: `daily` doc 27、`adj_factor` doc 28、`pro_bar` doc 146、期货日线 doc 138、外汇 doc 179。
- Yahoo Finance Help: adjusted close SLN28256；chart 返回的一手 `meta.currency`/timezone/adjclose 结构。
- AKShare 官方 stock/futures/fx 文档及 GitHub 源文件。
- SHFE AU/RB 产品规则；CZCE SR/RM 规则；DCE C/LH 规则；CFFEX IF/IM/T/TL 产品页。
- SEC XBRL Guide 2026 与 Apple 2025 10-K；HKEX 2025/2026 官方披露；证监会年报格式准则与上交所官方披露。
