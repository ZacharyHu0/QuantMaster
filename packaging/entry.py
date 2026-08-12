"""PyInstaller 打包入口：双击即启动 QuantMaster 桌面模式。"""

import sys

from quantmaster.cli import main
from quantmaster.runtime.windows_app import initialize_windows_app_process

if __name__ == "__main__":
    initialize_windows_app_process(root=True)
    sys.exit(main(["app"]) if len(sys.argv) == 1 else main(sys.argv[1:]))
