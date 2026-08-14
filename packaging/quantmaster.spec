# PyInstaller 打包配置：pyinstaller packaging/quantmaster.spec
# 产物：.artifacts/packages/desktop/QuantMaster(.exe) 单文件；双击运行 = qm app，
# 命令行带参数运行则等价于 qm <参数>。
# 显式声明静态资源（collect_data_files 对 editable 安装不可靠）
import subprocess
import sys
from pathlib import Path

from PyInstaller.utils.hooks import collect_submodules

project_root = Path(SPECPATH).parent
release_scope = {}
exec((project_root / "quantmaster" / "release.py").read_text(encoding="utf-8"), release_scope)
version = release_scope["VERSION"]
git_status = subprocess.run(
    ["git", "status", "--porcelain", "--untracked-files=normal"],
    cwd=project_root,
    check=True,
    capture_output=True,
    text=True,
).stdout
if git_status:
    raise SystemExit("QuantMaster packages require a clean Git worktree")
build_sha = subprocess.run(
    ["git", "rev-parse", "--verify", "HEAD"],
    cwd=project_root,
    check=True,
    capture_output=True,
    text=True,
).stdout.strip()
if len(build_sha) != 40 or any(character not in "0123456789abcdef" for character in build_sha):
    raise SystemExit("QuantMaster packages require a full lowercase Git SHA")
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
    collect_submodules("lark_oapi", filter=lambda name: name != "lark_oapi.adapter.flask") +
    collect_submodules("qrcode", filter=lambda name: not name.startswith("qrcode.tests"))
)
scipy_array_api_hidden = [
    "scipy._external.array_api_compat.common._fft",
    "scipy._external.array_api_compat.numpy.fft",
]

a = Analysis(
    [str(project_root / "packaging/entry.py")],
    pathex=[str(project_root)],
    datas=datas,
    runtime_hooks=[str(runtime_identity_hook)],
    hiddenimports=[
        "uvicorn.logging", "uvicorn.loops.auto", "uvicorn.protocols.http.auto",
        "uvicorn.protocols.websockets.auto", "uvicorn.lifespan.on", "_quantmaster_kernel",
    ] + optional_hidden + scipy_array_api_hidden,
    excludes=[
        "tkinter", "matplotlib", "torch", "dask", "pytest", "_pytest",
        "qrcode.tests", "lark_oapi.adapter.flask",
    ],
)
pyz = PYZ(a.pure)
exe = EXE(
    pyz, a.scripts, a.binaries, a.datas,
    name="QuantMaster",
    icon=str(project_root / "packaging/quantmaster.ico"),
    version=version_info,
    console=True,           # 保留控制台便于查看日志；不想要黑窗口可改 False
    upx=False,
)
