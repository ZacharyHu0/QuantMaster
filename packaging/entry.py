"""PyInstaller 打包入口：双击即启动 QuantMaster 桌面模式。"""

import multiprocessing
import sys

if __name__ == "__main__":
    # Frozen multiprocessing children re-enter this executable with
    # ``--multiprocessing-fork``. Dispatch them before importing application
    # modules, otherwise a renamed worker can start another desktop server.
    multiprocessing.freeze_support()

    from quantmaster.config import configure_installed_instance

    configure_installed_instance()

    from quantmaster.cli import main
    from quantmaster.runtime.windows_app import initialize_windows_app_process

    initialize_windows_app_process(root=True)
    sys.exit(main(["app"]) if len(sys.argv) == 1 else main(sys.argv[1:]))
