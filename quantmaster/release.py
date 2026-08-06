"""QuantMaster 发布元数据。

这里是应用版本号、发布日期和前端最近 10 条更新日志的唯一运行时来源。
每次仓库修改都必须递增版本，并同步更新保留完整历史的根目录 CHANGELOG.md。
"""

VERSION = "0.17.0"
RELEASE_DATE = "2026-08-06"
RELEASE_HISTORY_URL = "https://github.com/ZacharyHu0/QuantMaster/blob/main/CHANGELOG.md"

RELEASES = (
    {
        "version": VERSION,
        "date": RELEASE_DATE,
        "sections": (
            {
                "title": "运行时发布元数据瘦身",
                "items": (
                    (
                        "release.py 只保留应用实际展示的最近 10 个版本，完整发布历史"
                        "继续由 CHANGELOG 保存，源码从 2784 行降至约 200 行。"
                    ),
                    (
                        "版本接口与发布钩子契约保持不变，不再解析或加载 100 余份已经"
                        "退出支持范围的历史 Python 字面量。"
                    ),
                ),
            },
        ),
    },
    {
        "version": "0.16.1",
        "date": "2026-08-06",
        "sections": (
            {
                "title": "Windows 一键启动",
                "items": (
                    (
                        "仓库根目录新增 qm-serve.cmd，固定使用项目 .venv 中的 Python "
                        "启动服务，避免 Conda base 或旧版全局 qm 命令干扰。"
                    ),
                ),
            },
            {
                "title": "数值测试去重",
                "items": (
                    (
                        "本地快速集从 249 项收敛到 175 项，资讯与 LLM 集成测试转入"
                        "完整 lane，默认开发反馈不再加载重型边界。"
                    ),
                    (
                        "合并净值、行业中性、样本外验证、绩效和遗传因子挖掘的重复"
                        "等价用例，移除未再使用的 Hypothesis 依赖。"
                    ),
                ),
            },
        ),
    },
    {
        "version": "0.16.0",
        "date": RELEASE_DATE,
        "sections": (
            {
                "title": "首轮测试瘦身",
                "items": (
                    (
                        "浏览器回归从 15 条收敛到 8 条核心产品流程，删除重复的错误恢复、"
                        "动画细节和内部状态测试，减少 Chromium 重复启动。"
                    ),
                    (
                        "移除发布工作流与 Quant Lab 静态字符串镜像测试，并将帮助页验收"
                        "收敛为导航、搜索、计算器和响应式主路径。"
                    ),
                ),
            },
        ),
    },
    {
        "version": "0.15.0",
        "date": "2026-08-06",
        "sections": (
            {
                "title": "模拟账户策略管理闭环",
                "items": (
                    (
                        "模拟账户详情明确展示策略可编辑、历史锁定或只读归档状态，"
                        "产生历史前可直接编辑，产生历史后可复制策略继续探索。"
                    ),
                    (
                        "账户 API 增加稳定管理能力字段，删除继续采用可恢复归档并保留"
                        "策略快照、订单与成交账本。"
                    ),
                ),
            },
        ),
    },
    {
        "version": "0.14.5",
        "date": "2026-08-06",
        "sections": (
            {
                "title": "健康探针异常边界",
                "items": (
                    (
                        "后台健康探针保留完整本机日志，但公开诊断仅返回稳定恢复提示，"
                        "关闭最后一条异常栈暴露告警。"
                    ),
                ),
            },
        ),
    },
    {
        "version": "0.14.4",
        "date": RELEASE_DATE,
        "sections": (
            {
                "title": "代码扫描残余收敛",
                "items": (
                    (
                        "文件缓存安全字符集恢复基本面派生键与 Yahoo 期货代码使用的 #、=，"
                        "继续拒绝路径分隔符和目录跳转。"
                    ),
                    (
                        "行情缓存统一经过受限路径解析，运行诊断不再向 API 返回底层异常文本。"
                    ),
                ),
            },
        ),
    },
    {
        "version": "0.14.3",
        "date": RELEASE_DATE,
        "sections": (
            {
                "title": "代码扫描安全边界收敛",
                "items": (
                    (
                        "限制 GitHub Actions 默认权限，集中加固文件路径边界，并阻止内部异常"
                        "文本进入资讯、诊断、决策和板块 API。"
                    ),
                ),
            },
        ),
    },
    {
        "version": "0.14.2",
        "date": RELEASE_DATE,
        "sections": (
            {
                "title": "数据源边界收敛",
                "items": (
                    (
                        "将 free-stockdb 行业降级与诊断探针拆为独立边界，保持 v0.14 功能不变，"
                        "恢复复杂度质量基线。"
                    ),
                ),
            },
        ),
    },
    {
        "version": "0.14.1",
        "date": RELEASE_DATE,
        "sections": (
            {
                "title": "free-stockdb 首个可发布补丁",
                "items": (
                    (
                        "保留 v0.14 的 SDK 分钟行情与行业/概念板块接入，并将外部故障处理"
                        "收敛为明确异常类型。"
                    ),
                    (
                        "恢复宽异常质量门禁，不扩大技术债基线。"
                    ),
                ),
            },
        ),
    },
    {
        "version": "0.14.0",
        "date": RELEASE_DATE,
        "sections": (
            {
                "title": "free-stockdb 本地行情与板块数据",
                "items": (
                    (
                        "默认复用用户安装的 free-stockdb 成熟 SDK 与本地数据，补充前复权日线、"
                        "1/5/15/30/60 分钟线、行情快照、申万行业和概念板块。"
                    ),
                    (
                        "主数据源、SDK 目录和服务地址均可设置；不捆绑 free-stockdb 程序、"
                        "数据包或上游同步源，不可用时自动回退现有来源。"
                    ),
                    (
                        "GitHub Release 仅展示当前版本变更，仓库 CHANGELOG 继续保留完整历史。"
                    ),
                ),
            },
        ),
    },
)
