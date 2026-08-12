# PyInstaller 打包配置：pyinstaller packaging/quantmaster.spec
# 产物：dist/QuantMaster(.exe) 单文件；双击运行 = qm app（启动服务并打开浏览器），
# 命令行带参数运行则等价于 qm <参数>。
# 显式声明静态资源（collect_data_files 对 editable 安装不可靠）
from pathlib import Path

from PyInstaller.utils.hooks import collect_submodules
from PyInstaller.utils.win32.versioninfo import (
    FixedFileInfo, StringFileInfo, StringStruct, StringTable, VarFileInfo,
    VarStruct, VSVersionInfo,
)

release_scope = {}
exec((Path(SPECPATH).parent / "quantmaster" / "release.py").read_text(encoding="utf-8"), release_scope)
version = release_scope["VERSION"]
version_tuple = tuple(int(part) for part in version.split(".")) + (0,)
version_info = VSVersionInfo(
    ffi=FixedFileInfo(
        filevers=version_tuple, prodvers=version_tuple, mask=0x3F, flags=0x0,
        OS=0x40004, fileType=0x1, subtype=0x0, date=(0, 0),
    ),
    kids=[
        StringFileInfo([StringTable("080404B0", [
            StringStruct("CompanyName", "QuantMaster"),
            StringStruct("FileDescription", "QuantMaster A股量化研究平台"),
            StringStruct("FileVersion", version),
            StringStruct("InternalName", "QuantMaster"),
            StringStruct("OriginalFilename", "QuantMaster.exe"),
            StringStruct("ProductName", "QuantMaster"),
            StringStruct("ProductVersion", version),
        ])]),
        VarFileInfo([VarStruct("Translation", [2052, 1200])]),
    ],
)

datas = [
    ("../quantmaster/server/static", "quantmaster/server/static"),
    ("../quantmaster/data/security_master.json.gz", "quantmaster/data"),
    ("../quantmaster/skills/stock-analysis-framework", "quantmaster/skills/stock-analysis-framework"),
]
optional_hidden = (
    collect_submodules("keyring.backends") + collect_submodules("multipart") +
    collect_submodules("apscheduler") + collect_submodules("lark_oapi") +
    collect_submodules("qrcode") +
    collect_submodules("scipy._external.array_api_compat")
)

a = Analysis(
    ["entry.py"],
    pathex=[".."],
    datas=datas,
    hiddenimports=[
        "uvicorn.logging", "uvicorn.loops.auto", "uvicorn.protocols.http.auto",
        "uvicorn.protocols.websockets.auto", "uvicorn.lifespan.on", "_quantmaster_kernel",
    ] + optional_hidden,
    excludes=["tkinter", "matplotlib"],
)
pyz = PYZ(a.pure)
exe = EXE(
    pyz, a.scripts, a.binaries, a.datas,
    name="QuantMaster",
    icon="quantmaster.ico",
    version=version_info,
    console=True,           # 保留控制台便于查看日志；不想要黑窗口可改 False
    upx=False,
)
