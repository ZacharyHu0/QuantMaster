# PyInstaller 打包配置：pyinstaller packaging/quantmaster.spec
# 产物：dist/QuantMaster(.exe) 单文件；双击运行 = qm app（启动服务并打开浏览器），
# 命令行带参数运行则等价于 qm <参数>。
# 显式声明静态资源（collect_data_files 对 editable 安装不可靠）
from PyInstaller.utils.hooks import collect_submodules

datas = [
    ("../quantmaster/server/static", "quantmaster/server/static"),
    ("../quantmaster/data/security_master.json.gz", "quantmaster/data"),
    ("../quantmaster/skills/stock-analysis-framework", "quantmaster/skills/stock-analysis-framework"),
]
optional_hidden = (
    collect_submodules("keyring.backends") + collect_submodules("multipart") +
    collect_submodules("apscheduler") + collect_submodules("lark_oapi") +
    collect_submodules("qrcode")
)

a = Analysis(
    ["entry.py"],
    pathex=[".."],
    datas=datas,
    hiddenimports=[
        "uvicorn.logging", "uvicorn.loops.auto", "uvicorn.protocols.http.auto",
        "uvicorn.protocols.websockets.auto", "uvicorn.lifespan.on",
    ] + optional_hidden,
    excludes=["tkinter", "matplotlib"],
)
pyz = PYZ(a.pure)
exe = EXE(
    pyz, a.scripts, a.binaries, a.datas,
    name="QuantMaster",
    console=True,           # 保留控制台便于查看日志；不想要黑窗口可改 False
    upx=False,
)
