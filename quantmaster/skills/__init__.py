"""随 QuantMaster 发布或经校验缓存的研究工作流资源。"""

from __future__ import annotations


def load_xiaoshi_quant_skill() -> str:
    """Load the last verified Xiaoshi quant workflow before finance reasoning."""
    from quantmaster.data.xiaoshi_source import XiaoshiPublicationStore

    return XiaoshiPublicationStore().skill_text()
