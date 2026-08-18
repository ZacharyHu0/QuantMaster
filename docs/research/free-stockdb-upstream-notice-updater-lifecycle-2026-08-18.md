# free-stockdb 通知页、数据分区与更新器生命周期

研究日期：2026-08-18

范围：只使用 free-stockdb 官方站点及其直接链接的文档和 v0.3.1 客户端包。本文只保留本次三个 Issue 所需结论；“官方包静态证据”不视为稳定公开 API。

## 结论

1. **公告正文已迁到子页面。** 当前首页只是动态 Tab 容器，默认用 `fetch('./tabs/notice.html?...')` 加载公告。适配器应读取 [`/tabs/notice.html`](https://a.123128.xyz/tabs/notice.html)，或从[官方首页](https://a.123128.xyz/)发现并跟随该模块。
2. **“更新至: 2026-08-17”是公告页日期，不是行情数据日期。** 页面同时展示不同日期的功能公告和客户端下载卡片，且没有 `latest_market_trade_date` 字段。目标交易日应来自可信交易日历；官方 SDK 提供 `get_trade_days(end_date=..., count=1)`，但它只能给出日历目标，最终完成仍须验证本地数据。[官方动态页](https://a.123128.xyz/tabs/notice.html) · [官方 Python 文档：交易日接口](https://a.123128.xyz/docs/AI策略python开发接口文档.md#125-%E4%BA%A4%E6%98%93%E6%97%A5%E5%92%8C%E8%AF%81%E5%88%B8%E4%BF%A1%E6%81%AF)
3. **`data ... dataN` 是官方数据分区合同。** 上游把这些目录定义为多数据源、多分卷历史行情，由数据更新程序维护并只读使用；`mydb` 才是用户私有写入空间。QuantMaster 不应只假定 `data`，也不应直接改写这些分区。[官方 AI Python 开发页：本地目录职责](https://a.123128.xyz/tabs/ai-py.html)
4. **网页只引导双击，随包说明还公开了定时 `-run`。** 快速开始页的本地流程是双击“数据更新”，再双击 `stockdb`；官方包内说明另给出 `数据更新.exe -run 15:50:00`。因此不是“只有双击”，但公开的命令行能力只有定时运行，未发现受支持的一次性静默模式。[官方快速开始页](https://a.123128.xyz/tabs/start.html) · [官方 v0.3.1 客户端包及随包说明](https://a.123128.xyz/downloads/stockdb.zip?v=0.31)
5. **没有公开的完成/退出 API。** 官方资料未提供 `--headless`、`--quiet`、`--once`、状态端点、稳定退出码、完成事件或“同步完自动退出”保证；`-run` 的官方二进制文本显示它会等待下一次计划同步，因此它不是一次性无窗口模式。[官方 v0.3.1 客户端包](https://a.123128.xyz/downloads/stockdb.zip?v=0.31)
6. **可以自动关窗，但必须由 QuantMaster 自己判定数据完成。** 启动时记录子进程 PID；本地目标日期、覆盖率和关键字段通过后，只向该 PID 的窗口发送正常关闭请求并等待退出。窗口关闭、进程退出或上游内部 marker 都不能单独代表数据成功；写入状态不明时不应强杀。
7. **当前最低兼容版本是 v0.3.1。** 动态页称低于 v0.3.1 无法接收最新行情，并要求遇到 `unsafe path`、`error` 等旧版问题时升级；当前发布标识为 `v0.3.1-online-more-power`。[官方动态页](https://a.123128.xyz/tabs/notice.html) · [官方下载](https://a.123128.xyz/downloads/stockdb.zip?v=0.31)

## Issue 1：通知页与目标交易日

建议把上游信息拆成三个字段，禁止互相代用：

| 字段 | 来源 | 语义 |
| --- | --- | --- |
| `notice_updated_on` | `tabs/notice.html` 的“更新至” | 公告页新鲜度 |
| `calendar_target_date` | 可信日历或 `get_trade_days` | 应有交易日 |
| `local_verified_date` | 本地 `rd` 完整性检查 | 已经证实可用的数据日 |

通知解析器还可从[官方动态页](https://a.123128.xyz/tabs/notice.html)提取：

- `minimum_client_version = 0.3.1`
- `release_label = v0.3.1-online-more-power`
- `download_url = https://a.123128.xyz/downloads/stockdb.zip?v=0.31`

如果日历来源不可用，应明确报告“目标交易日未知”；不能把 `notice_updated_on` 代入。官方文档要求本地行情优先使用 `rd`，在线接口只补本地没有的数据，并要求检查在线错误字典。[官方 Python 文档：使用规则](https://a.123128.xyz/docs/AI策略python开发接口文档.md#1-ai-%E5%BF%85%E9%A1%BB%E9%81%B5%E5%AE%88%E7%9A%84%E8%A7%84%E5%88%99)

## Issue 2：`data ... dataN` 分区

官方页面的原文语义是：

- `./data ～ ./dataN`：多数据源分区存储，底层并行挂载多分卷历史行情与多源数据，只读管理，由数据更新程序维护。
- `./mydb`：私有写入空间，用于策略状态、因子和缓存。

来源：[官方 AI Python 开发页](https://a.123128.xyz/tabs/ai-py.html) · [官方 Python 文档：文件与运行环境](https://a.123128.xyz/docs/AI策略python开发接口文档.md#21-%E6%96%87%E4%BB%B6%E4%B8%8E%E8%BF%90%E8%A1%8C%E7%8E%AF%E5%A2%83)

直接工程含义：

- 发现和校验不能硬编码只有 `data`；应接受 `data` 与编号分卷 `dataN`。
- 上游没有公开 N 的上限、编号是否连续或底层文件格式合同；优先通过 `stock_sdk`/`rd` 读取，不直接遍历或修改分区内部文件。
- 更新完成验证应覆盖所有实际挂载分区产生的统一 SDK 视图，而不是只检查一个目录的时间戳。

## Issue 3：更新器完成与自动关窗

### 公开合同

官方本地流程是：更新数据，然后启动 `stockdb.exe`；本地服务默认监听 `127.0.0.1:7899`，Python 使用 `stock_sdk`/`rd` 访问。[官方快速开始页](https://a.123128.xyz/tabs/start.html) · [官方 Python 文档：安装、导入和连接](https://a.123128.xyz/docs/AI策略python开发接口文档.md#2-%E5%AE%89%E8%A3%85%E5%AF%BC%E5%85%A5%E5%92%8C%E8%BF%9E%E6%8E%A5)

随包说明公开了立即双击和 `-run HH:MM:SS` 定时两种入口，并称可退出/重启后继续同步；没有公开静默执行或机器可读完成协议。[官方 v0.3.1 客户端包](https://a.123128.xyz/downloads/stockdb.zip?v=0.31)

### 不能依赖的内部实现

官方包静态可见 `.sync_manifest.json`、`.part`、`disable`、`Success: 100%` 等文本。`sync_url.txt` 同时配置普通 once 源和带 `always` 的持续源，`disable` 只表示某个 once 源曾完成，不能证明持续源已经同步到目标交易日；其余文件也没有公开 schema 或原子性承诺。[官方 v0.3.1 客户端包](https://a.123128.xyz/downloads/stockdb.zip?v=0.31)

### 推荐流程

1. 更新前记录 `calendar_target_date`、`local_verified_date` 和现有覆盖。
2. 启动更新器并保存此次启动的 PID，不按进程名处理其他实例。
3. 周期性运行现有本地完整性检查；至少验证目标日期、标的覆盖和关键 OHLCV 字段，并要求结果短暂稳定。
4. 数据门通过后，向该 PID 的窗口发送正常 `WM_CLOSE`/等价关闭请求，限时等待进程退出。
5. 正常关闭失败时报告“数据已验证、更新器未能自动关闭”并保留窗口；数据门未通过时不得因窗口显示 100% 或进程退出而报告成功。

这套流程让“自动关窗”只负责 UI 收尾，数据正确性仍由 QuantMaster 自己的本地证据决定。

## 状态文案

上游没有“第 1/1 次”的概念。QuantMaster 应明确维度：

- 重试显示 `尝试 1/3`；
- 分片显示 `分片 1/4`；
- 单一任务且没有重试时省略计数；
- 等待第三方更新器时显示 `正在等待本地数据验证`。

