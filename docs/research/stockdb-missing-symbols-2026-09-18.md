# StockDB missing-symbol audit — 2026-09-18

Checked on 2026-09-19 against the accepted native StockDB snapshot, existing local caches, and fresh Tushare `daily`, `stock_basic(list_status=L)`, and `suspend_d` responses. All requests returned provider code 0.

StockDB covered 5482 of 5531 expected trading securities. All 49 missing securities were present in the current listed catalog and had positive volume on 2026-09-18; none appeared in the target-session suspension response. They must not be excluded as delisted or suspended. All listed between April and August 2026. The pattern suggests incomplete native coverage of recent listings; it does not prove the upstream implementation cause.

The 12-stock demo refresh is a separate problem from this 49-symbol native gap. Native StockDB recovery restored the demo account proposal and completed the active daily momentum account’s previously pending orders. Formal research admission remains separate from successful daily preparation.

| Code | Current name | Listed | Close 2026-09-18 (CNY) | Volume (lots) |
|---|---|---|---:|---:|
| 001232.SZ | 嘉立创 | 2026-08-04 | 195.8 | 48532.09 |
| 001237.SZ | 惠康科技 | 2026-05-22 | 52.59 | 33884.15 |
| 001248.SZ | 华润新能源 | 2026-07-02 | 12.06 | 900494.17 |
| 001365.SZ | 天海电子 | 2026-05-18 | 38.32 | 195303.96 |
| 001393.SZ | 维通利 | 2026-05-15 | 43.9 | 54280.84 |
| 001399.SZ | 惠科股份 | 2026-06-26 | 23.17 | 646537.7 |
| 301531.SZ | 春光集团 | 2026-05-11 | 54.87 | 59840.87 |
| 301583.SZ | 托伦斯 | 2026-07-10 | 142.56 | 126896.92 |
| 301599.SZ | 理奇智能 | 2026-04-30 | 33.77 | 61547.13 |
| 301669.SZ | 高特电子 | 2026-06-09 | 26.7 | 91095.92 |
| 301677.SZ | 欣兴工具 | 2026-07-30 | 53.35 | 57143.19 |
| 301707.SZ | 展芯股份 | 2026-08-07 | 75.4 | 87219.3 |
| 301717.SZ | 超纯应材 | 2026-08-11 | 371.7 | 33234.11 |
| 603407.SH | 长裕集团 | 2026-05-11 | 58.58 | 62289.01 |
| 603435.SH | 嘉德利 | 2026-05-22 | 50.32 | 54061.62 |
| 603468.SH | 津富士达 | 2026-08-06 | 22.79 | 69807.26 |
| 688635.SH | 长进光子 | 2026-05-27 | 295.03 | 25458.17 |
| 688797.SH | 臻宝科技 | 2026-06-24 | 297.0 | 51248.99 |
| 688806.SH | 泰诺麦博 | 2026-07-21 | 22.7 | 79309.63 |
| 688808.SH | 联讯仪器 | 2026-04-24 | 2468.0 | 9399.43 |
| 688825.SH | 长鑫科技 | 2026-07-27 | 55.54 | 2626513.31 |
| 688828.SH | 国仪公司 | 2026-08-11 | 96.1 | 32405.6 |
| 920038.BJ | 森合高科 | 2026-08-05 | 38.6 | 44326.11 |
| 920065.BJ | 千岸科技 | 2026-07-29 | 38.72 | 70926.3 |
| 920072.BJ | 科莱瑞迪 | 2026-06-29 | 23.5 | 12439.43 |
| 920079.BJ | 乔路铭 | 2026-07-22 | 13.02 | 57171.72 |
| 920081.BJ | 欧伦电气 | 2026-07-10 | 42.29 | 9239.46 |
| 920083.BJ | 金戈新材 | 2026-06-11 | 38.36 | 46846.32 |
| 920096.BJ | 嘉晨智能 | 2026-05-18 | 31.97 | 8422.69 |
| 920117.BJ | 龙鑫智能 | 2026-07-16 | 28.81 | 43294.83 |
| 920126.BJ | 永大股份 | 2026-06-15 | 10.03 | 21380.82 |
| 920136.BJ | 永励精密 | 2026-07-09 | 22.05 | 24139.71 |
| 920138.BJ | 杰理科技 | 2026-08-12 | 35.29 | 73234.59 |
| 920156.BJ | 海昌智能 | 2026-04-27 | 36.28 | 8335.72 |
| 920161.BJ | 龙辰科技 | 2026-05-27 | 21.33 | 34879.36 |
| 920165.BJ | 珈凯生物 | 2026-08-11 | 35.6 | 11650.47 |
| 920176.BJ | 维琪科技 | 2026-07-27 | 68.68 | 24982.29 |
| 920178.BJ | 锐翔智能 | 2026-05-15 | 93.66 | 10802.61 |
| 920186.BJ | 中科仪 | 2026-04-29 | 73.2 | 49897.23 |
| 920189.BJ | 康美特 | 2026-07-08 | 21.02 | 62466.15 |
| 920193.BJ | 吉和昌 | 2026-07-02 | 30.58 | 32871.26 |
| 920200.BJ | 振宏股份 | 2026-05-07 | 31.8 | 49931.57 |
| 920206.BJ | 彩客科技 | 2026-06-08 | 39.47 | 22480.47 |
| 920211.BJ | 新睿电子 | 2026-06-05 | 95.75 | 3667.88 |
| 920218.BJ | 新天力 | 2026-05-29 | 15.11 | 15008.13 |
| 920220.BJ | 朗信电气 | 2026-05-22 | 42.91 | 15818.62 |
| 920222.BJ | 益坤电气 | 2026-06-30 | 27.67 | 12884.16 |
| 920238.BJ | 长鹰硬科 | 2026-07-24 | 68.9 | 43826.29 |
| 920258.BJ | 聚仁新材 | 2026-08-03 | 10.71 | 32686.28 |

Volume follows the Tushare daily API unit (lots), not the native StockDB share unit. The prices and volumes above are evidence of trading activity, not a recommendation.
