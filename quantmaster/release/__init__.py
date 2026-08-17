"""QuantMaster 发布元数据公开入口。

这是应用版本号、发布日期和前端最近 10 条更新日志的唯一运行时来源
（``quantmaster.release``）。数据与更新日志查询逻辑位于 ``history`` 子模块，
发布预检位于 ``validate``，打包辅助位于 ``packaging``。

保持 ``from quantmaster.release import VERSION, RELEASE_DATE, RELEASES``
这一公开契约不变。版本变更由 owner 要求时在单独版本 PR 完成，并同步更新
根目录 CHANGELOG.md；任务分支的 checkpoint 提交不得修改版本元数据。
"""

from __future__ import annotations

from quantmaster.release.history import RELEASE_DATE, RELEASES, VERSION

__all__ = ("RELEASES", "RELEASE_DATE", "VERSION")
