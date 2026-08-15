"""PyInstaller 打包入口：双击即启动 QuantMaster 桌面模式。"""

import multiprocessing
import sys

if __name__ == "__main__":
    # Frozen multiprocessing children re-enter this executable with
    # ``--multiprocessing-fork``. Dispatch them before importing application
    # modules, otherwise a renamed worker can start another desktop server.
    multiprocessing.freeze_support()
    sys.stdout.reconfigure(encoding="utf-8")

    from quantmaster.runtime.splash import close_splash, update_splash

    try:
        update_splash("正在加载本地配置")
        from quantmaster.config import configure_installed_instance

        configure_installed_instance()
        update_splash("正在装配命令入口")

        from quantmaster.cli import main
        from quantmaster.runtime.windows_app import initialize_windows_app_process

        initialize_windows_app_process(root=True)
        arguments = ["app"] if len(sys.argv) == 1 else sys.argv[1:]
        command = next((argument for argument in arguments if not argument.startswith("-")), "")
        reload_requested = False
        if command == "serve":
            for argument in arguments:
                if argument == "--reload":
                    reload_requested = True
                elif argument == "--no-reload":
                    reload_requested = False
        web_command = command == "app" or (command == "serve" and not reload_requested)
        update_splash(
            "正在启动 Web 服务"
            if web_command
            else "正在执行命令"
        )
        if not web_command:
            close_splash()
        sys.exit(main(arguments))
    finally:
        close_splash()
