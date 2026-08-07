"""QuantMaster 发布元数据。

这里是应用版本号、发布日期和前端最近 10 条更新日志的唯一运行时来源。
每次仓库修改都必须递增版本，并同步更新保留完整历史的根目录 CHANGELOG.md。
"""

VERSION = "1.3.13"
RELEASE_DATE = "2026-08-07"
RELEASE_HISTORY_URL = "https://github.com/ZacharyHu0/QuantMaster/blob/main/CHANGELOG.md"

RELEASES = (
    {
        "version": VERSION,
        "date": RELEASE_DATE,
        "sections": (
            {
                "title": "候选日期选择器重建",
                "items": (
                    "候选历史日期改为项目内月历弹层，彻底移除浏览器原生日期输入的上下微调和滚轮行为。",
                ),
            },
        ),
    },
    {
        "version": "1.3.12",
        "date": RELEASE_DATE,
        "sections": (
            {
                "title": "候选日期控件外观修正",
                "items": (
                    "保留日历选择能力，移除浏览器原生日期输入可能显示的上下微调器，并继续禁止滚轮误改日期。",
                ),
            },
        ),
    },
    {
        "version": "1.3.11",
        "date": RELEASE_DATE,
        "sections": (
            {
                "title": "发布质量修正",
                "items": (
                    (
                        "修正数据研究模块的变量遮蔽，登记资讯抓取故障边界，并收紧发布文案"
                        "格式以通过发布前 Ruff 检查。"
                    ),
                ),
            },
        ),
    },
    {
        "version": "1.3.10",
        "date": RELEASE_DATE,
        "sections": (
            {
                "title": "Windows 进程品牌化",
                "items": (
                    (
                        "一键启动自动生成基于项目虚拟环境的 QuantMaster.exe，主站监督进程与"
                        "热更新 worker 不再显示为 python.exe。"
                    ),
                    (
                        "命名启动器嵌入项目标识、QuantMaster 产品信息和当前发布版本；正式"
                        "PyInstaller 构建同步使用同一图标。"
                    ),
                ),
            },
        ),
    },
    {
        "version": "1.3.9",
        "date": RELEASE_DATE,
        "sections": (
            {
                "title": "图表滚轮语义修正",
                "items": (
                    (
                        "普通滚轮沿对应轴平移已缩放的数据窗口；仅按住 Ctrl 时沿该轴缩放，"
                        "图内 Ctrl 滚轮会拦截浏览器页面缩放。"
                    ),
                ),
            },
        ),
    },
    {
        "version": "1.3.8",
        "date": RELEASE_DATE,
        "sections": (
            {
                "title": "主站安全热更新",
                "items": (
                    (
                        "Windows 启动脚本默认监视主站代码并安全替换 Web worker；热更新期间"
                        "FreeStockDB 持续运行，前端资源刷新页面即可生效。"
                    ),
                    "退出启动器时仍执行完整的主站清理并停止其托管的 FreeStockDB，避免残留后台进程。",
                ),
            },
        ),
    },
    {
        "version": "1.3.7",
        "date": RELEASE_DATE,
        "sections": (
            {
                "title": "数据图表轴向滚轮缩放",
                "items": (
                    (
                        "支持在可缩放数据图内以纵向滚轮缩放纵轴、横向滚轮或 Shift 加滚轮"
                        "缩放横轴；页面其余区域保持正常滚动。"
                    ),
                ),
            },
        ),
    },
    {
        "version": "1.3.6",
        "date": RELEASE_DATE,
        "sections": (
            {
                "title": "候选列表宽度与日期交互修正",
                "items": (
                    "候选日期输入不再响应鼠标滚轮；成员表按可用宽度分配列宽，消除中等宽度下的无效横向滚动与留白。",
                ),
            },
        ),
    },
    {
        "version": "1.3.5",
        "date": RELEASE_DATE,
        "sections": (
            {
                "title": "页面内选择器统一",
                "items": (
                    "时间窗口、行情周期与推送强度等互斥选择器统一为二级导航的底部蓝色当前位置标记，取消圆角底色。",
                ),
            },
        ),
    },
    {
        "version": "1.3.4",
        "date": RELEASE_DATE,
        "sections": (
            {
                "title": "观察页签选中态收口",
                "items": (
                    "观察二级页签取消圆角底色，改为文字强调与底部蓝色当前位置标记，悬停不再产生按钮式背景。",
                ),
            },
        ),
    },
)
