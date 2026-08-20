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


def test_news_ui_distinguishes_queue_status_from_symbol_association() -> None:
    script = (
        Path(__file__).parents[1] / "quantmaster" / "server" / "static" / "news.js"
    ).read_text(encoding="utf-8")
    assert "|| status || '待标注'" not in script
    assert "NEWS_STATUS_LABELS[value] || '状态未知';" in script
    assert "status === 'complete' && symbols.length === 0" in script
    assert "未关联直接标的" in script
