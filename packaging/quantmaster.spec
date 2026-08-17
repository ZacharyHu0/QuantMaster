# PyInstaller 打包配置：pyinstaller packaging/quantmaster.spec
# 默认产物为 QuantMaster onefile；Windows 可显式测量 onedir。
# 命令行带参数运行则等价于 qm <参数>。
# 显式声明静态资源（collect_data_files 对 editable 安装不可靠）
import os
import runpy
import sys
from pathlib import Path

from PyInstaller.utils.hooks import collect_submodules

package_layout = os.environ.get("QM_DESKTOP_LAYOUT", "onefile")
if package_layout not in {"onefile", "onedir-measurement"}:
    raise ValueError(f"unsupported QM_DESKTOP_LAYOUT: {package_layout}")
if package_layout == "onedir-measurement" and sys.platform != "win32":
    raise ValueError("QM_DESKTOP_LAYOUT=onedir-measurement is Windows-only")
onedir_measurement = package_layout == "onedir-measurement"

project_root = Path(SPECPATH).parent
packaged_build_sha = runpy.run_path(
    str(project_root / "scripts/release/check_desktop_artifact.py")
)["packaged_build_sha"]
release_scope = {}
exec(
    (project_root / "quantmaster" / "release" / "history.py").read_text(encoding="utf-8"),
    release_scope,
)
version = release_scope["VERSION"]
build_sha = packaged_build_sha(project_root)
runtime_identity_hook = Path(workpath) / "quantmaster_runtime_identity.py"
runtime_identity_hook.write_text(
    "from quantmaster.runtime.identity import bind_packaged_build\n"
    f"bind_packaged_build({build_sha!r})\n",
    encoding="utf-8",
)
version_info = None
if sys.platform == "win32":
    from PyInstaller.utils.win32.versioninfo import (
        FixedFileInfo, StringFileInfo, StringStruct, StringTable, VarFileInfo,
        VarStruct, VSVersionInfo,
    )

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
    (str(project_root / "quantmaster/server/static"), "quantmaster/server/static"),
    (str(project_root / "quantmaster/data/security_master.json.gz"), "quantmaster/data"),
    (str(project_root / "quantmaster/skills/stock-analysis-framework"), "quantmaster/skills/stock-analysis-framework"),
]
optional_hidden = (
    collect_submodules("keyring.backends") + collect_submodules("multipart") +
    collect_submodules("apscheduler") +
    collect_submodules("qrcode", filter=lambda name: not name.startswith("qrcode.tests"))
)
lark_oapi_hidden = [
    "lark_oapi",
    "lark_oapi.api.im.v1",
    "lark_oapi.channel",
    "lark_oapi.channel.events",
    "lark_oapi.core",
    "lark_oapi.ws",
    "lark_oapi.ws.client",
]
pyarrow_unused_hidden = [
    "pyarrow.acero",
    "pyarrow.cuda",
    "pyarrow.dataset",
    "pyarrow.feather",
    "pyarrow.flight",
    "pyarrow.gandiva",
    "pyarrow.json",
    "pyarrow.substrait",
    "pyarrow.parquet.encryption",
]

a = Analysis(
    [str(project_root / "packaging/entry.py")],
    pathex=[str(project_root)],
    datas=datas,
    runtime_hooks=[str(runtime_identity_hook)],
    hiddenimports=[
        "uvicorn.logging", "uvicorn.loops.auto", "uvicorn.protocols.http.auto",
        "uvicorn.protocols.websockets.auto", "uvicorn.lifespan.on", "_quantmaster_kernel",
    ] + optional_hidden + lark_oapi_hidden,
    excludes=[
        "tkinter", "matplotlib", "torch", "sklearn", "scipy", "dask", "pytest", "_pytest",
        "qrcode.tests", "lark_oapi.adapter",
        *pyarrow_unused_hidden,
    ],
)
pyz = PYZ(a.pure)
splash = None
if sys.platform == "win32" and not onedir_measurement:
    splash = Splash(
        project_root / "packaging/quantmaster-splash.png",
        binaries=a.binaries,
        datas=a.datas,
        text_pos=(32, 270),
        text_size=11,
        text_color="#f4f7fb",
        text_default="正在准备 QuantMaster",
        full_tk=False,
        always_on_top=True,
    )
splash_inputs = [] if splash is None else [splash, splash.binaries]
onefile_inputs = [] if onedir_measurement else [a.binaries, a.datas]
exe = EXE(
    pyz, a.scripts, *splash_inputs, *onefile_inputs,
    exclude_binaries=onedir_measurement,
    name="QuantMaster",
    icon=str(project_root / "packaging/quantmaster.ico"),
    version=version_info,
    console=True,           # 保留控制台便于查看日志；不想要黑窗口可改 False
    upx=False,
)
if onedir_measurement:
    collect = COLLECT(
        exe, a.binaries, a.datas,
        name="QuantMaster",
        upx=False,
    )
