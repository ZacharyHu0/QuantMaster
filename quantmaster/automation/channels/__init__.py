"""微信 ClawBot iLink 与飞书应用 Bot 的直连适配器。"""

from quantmaster.automation.channels.feishu import FeishuBotClient
from quantmaster.automation.channels.weixin import WeixinClawBotClient

__all__ = ["FeishuBotClient", "WeixinClawBotClient"]
