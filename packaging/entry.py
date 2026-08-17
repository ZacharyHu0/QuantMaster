"""PyInstaller 打包入口：双击即启动 QuantMaster 桌面模式。"""

import multiprocessing
import sys

if __name__ == "__main__":
    from quantmaster.config import configure_installed_instance

    configure_installed_instance()
    # Frozen multiprocessing children re-enter this executable with
    # ``--multiprocessing-fork``. Bind the installed instance before dispatch,
    # but still dispatch before importing the application command surface.
    multiprocessing.freeze_support()
    sys.stdout.reconfigure(encoding="utf-8")

    from quantmaster.runtime.splash import close_splash, update_splash

    try:
        update_splash("正在加载本地配置")
        update_splash("正在装配命令入口")

        from quantmaster.runtime.windows_app import initialize_windows_app_process
        from quantmaster.server.cli import main

        arguments = ["app"] if len(sys.argv) == 1 else sys.argv[1:]
        command = next((argument for argument in arguments if not argument.startswith("-")), "")
        # The activation/setup helpers are short-lived control commands. They
        # must not create an application root Job Object before handing off to
        # the stable immutable-slot process.
        if command not in {"activate", "setup-shortcut"}:
            initialize_windows_app_process(root=True)
        web_command = command == "app" or command == "serve"
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
