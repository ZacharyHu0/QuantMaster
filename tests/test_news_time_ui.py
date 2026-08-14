from pathlib import Path


def test_news_ui_labels_event_and_observation_times_separately() -> None:
    script = (
        Path(__file__).parents[1] / "quantmaster" / "server" / "static" / "news.js"
    ).read_text(encoding="utf-8")
    assert "localDate(item.published_at)" in script
    assert "发布 · Asia/Shanghai" in script
    assert "首次观测 · Asia/Shanghai" in script
    assert "最近抓取 · Asia/Shanghai" in script
    assert "内容版本进入系统 · Asia/Shanghai" in script
    assert "TIME_UNINTERPRETABLE" in script
    assert "localDate(item.first_seen_at || item.published_at)" not in script
