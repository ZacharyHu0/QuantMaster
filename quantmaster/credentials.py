"""系统凭据库适配器。

keyring 是可选运行时依赖：桌面安装包会携带它，但源码精简安装可能没有。
调用方必须显式处理 :class:`CredentialError`，不得静默把密钥写回配置文件。
"""

from __future__ import annotations

import hashlib


class CredentialError(RuntimeError):
    """系统凭据库不可用或操作失败。"""


class CredentialStore:
    SERVICE = "QuantMaster"

    @staticmethod
    def llm_target(provider: str, base_url: str) -> str:
        endpoint = (base_url or "official").strip().lower().rstrip("/")
        digest = hashlib.sha256(endpoint.encode("utf-8")).hexdigest()[:20]
        return f"llm:{provider.strip().lower()}:{digest}"

    @staticmethod
    def tushare_target() -> str:
        return "data:tushare"

    @staticmethod
    def weixin_target(account_id: str) -> str:
        return f"bot:weixin:{account_id.strip()}"

    @staticmethod
    def feishu_target(app_id: str) -> str:
        return f"bot:feishu:{app_id.strip()}"

    @staticmethod
    def news_source_target(source_id: str) -> str:
        """动态资讯来源的凭据槽；来源 ID 不可变，避免改名后遗留密钥。"""
        normalized = source_id.strip().lower()
        digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:24]
        return f"news:source:{digest}"

    def _backend(self):
        try:
            import keyring
            import keyring.errors
        except ImportError as exc:  # pragma: no cover - 由无 optional dependency 环境覆盖
            raise CredentialError("未安装 keyring，无法使用系统凭据库") from exc
        try:
            backend = keyring.get_keyring()
            priority = getattr(backend, "priority", 0)
            if not priority:
                raise CredentialError("系统没有可用的凭据库后端")
            return keyring, keyring.errors
        except CredentialError:
            raise
        except Exception as exc:  # pragma: no cover - 平台 keyring 差异
            raise CredentialError("无法初始化系统凭据库") from exc

    def get(self, target: str) -> str | None:
        keyring, errors = self._backend()
        try:
            return keyring.get_password(self.SERVICE, target)
        except errors as exc:
            raise CredentialError("读取系统凭据失败") from exc

    def set(self, target: str, value: str) -> None:
        if not value:
            raise CredentialError("不能保存空凭据")
        keyring, errors = self._backend()
        try:
            keyring.set_password(self.SERVICE, target, value)
        except errors as exc:
            raise CredentialError("写入系统凭据失败") from exc

    def delete(self, target: str) -> None:
        keyring, errors = self._backend()
        try:
            keyring.delete_password(self.SERVICE, target)
        except errors.PasswordDeleteError:
            return
        except errors as exc:
            raise CredentialError("删除系统凭据失败") from exc
