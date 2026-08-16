"""爬虫存储与舆情聚合测试（不触网）。"""

import pytest

from quantmaster.ai.crawler import AICrawler, NewsItem, NewsStore
from quantmaster.ai.news_contracts import FetchBatch, FetchedArticle, NewsContractError
from quantmaster.ai.sentiment import sentiment_panel


def _item(title: str, published: str = "2024-05-06 09:30:00", **kw) -> NewsItem:
    return NewsItem(source="test", title=title, content=title,
                    published_at=published, **kw)


class TestNewsStore:
    def test_news_identity_accepts_utf8_bytes(self):
        text_item = NewsItem(source="test", title="标题", content="正文")
        bytes_item = NewsItem(
            source="test", title="标题".encode(), content="正文".encode(),
        )

        assert NewsStore.fingerprint(bytes_item) == NewsStore.fingerprint(text_item)
        assert NewsStore.content_hash(bytes_item) == NewsStore.content_hash(text_item)

    def test_save_and_dedup(self, tmp_path):
        store = NewsStore(path=tmp_path / "news.sqlite")
        items = [_item("新闻A"), _item("新闻B")]
        assert store.save(items) == 2
        # 重复入库应被去重，计数为 0
        assert store.save(items) == 0
        assert len(store.recent()) == 2

    def test_symbols_roundtrip(self, tmp_path):
        store = NewsStore(path=tmp_path / "news.sqlite")
        store.save([_item("利好", symbols=["600519.SH"], sentiment=0.8,
                          event_type="业绩", summary="摘要")])
        row = store.recent()[0]
        assert row["symbols"] == ["600519.SH"]
        assert row["sentiment"] == 0.8


class TestCrawlerSkipLLM:
    def test_fetched_bytes_are_decoded_before_storage(self, tmp_path, monkeypatch):
        from quantmaster.ai import crawler as crawler_mod

        c = AICrawler(store=NewsStore(path=tmp_path / "news.sqlite"))
        source = c.source_store.create({
            "name": "字节来源", "kind": "rss", "group_name": "periodic",
            "url": "https://example.test/feed", "max_age_hours": 24,
        })
        monkeypatch.setattr(
            crawler_mod,
            "fetch_declarative_source",
            lambda *args, **kwargs: FetchBatch(
                source_id=source["id"], articles=[FetchedArticle(
                    source=source["id"], title="字节标题".encode(),
                    content="字节正文".encode(), provider_item_id="bytes-1",
                )], watermark="bytes-1",
            ),
        )

        result = c.run(sources=[source["id"]], skip_llm=True)

        assert result["errors"] == {}
        row = c.store.recent()[0]
        assert row["title"] == "字节标题"
        assert row["content"] == "字节正文"

    def test_invalid_fetched_bytes_fail_with_contract_error(self):
        with pytest.raises(NewsContractError) as error:
            FetchedArticle(
                source="test", title=b"\xff", content="正文", provider_item_id="bad-1",
            )

        assert error.value.code == "invalid_text_encoding"

    def test_run_with_fake_source(self, tmp_path, monkeypatch):
        from quantmaster.ai import crawler as crawler_mod

        c = AICrawler(store=NewsStore(path=tmp_path / "news.sqlite"))
        source = c.source_store.create({
            "name": "测试来源", "kind": "rss", "group_name": "fast",
            "url": "https://example.test/feed", "is_official": False,
        })
        monkeypatch.setattr(
            crawler_mod,
            "fetch_declarative_source",
            lambda *args, **kwargs: FetchBatch(source_id=source["id"], articles=[
                FetchedArticle(
                    source=source["id"], title="快讯1", content="快讯1",
                    provider_item_id="1",
                ),
                FetchedArticle(
                    source=source["id"], title="快讯2", content="快讯2",
                    provider_item_id="2",
                ),
            ], watermark="1"),
        )
        result = c.run(sources=[source["id"]], skip_llm=True)
        assert result["fetched"] == 2
        assert result["saved"] == 2
        assert not result["errors"]

    def test_custom_source_304_returns_batch_without_index_error(self, tmp_path, monkeypatch):
        c = AICrawler(store=NewsStore(path=tmp_path / "news.sqlite"))
        source = c.source_store.create({
            "name": "自定义 RSS", "kind": "rss", "group_name": "periodic",
            "url": "https://example.test/feed", "is_official": False,
            "max_age_hours": 24,
        })
        latest = 1786240800.0
        c.source_store.record_batch(FetchBatch(
            source_id=source["id"], watermark="custom-watermark",
            latest_published_at=latest,
        ))
        monkeypatch.setattr(
            "quantmaster.ai.news_sources._fetch_bytes",
            lambda *args, **kwargs: (None, source["url"], ""),
        )
        monkeypatch.setattr(
            "quantmaster.ai.news_sources.time.time", lambda: latest + 3600,
        )

        result = c.run(sources=[source["id"]], skip_llm=True)

        assert result["fetched"] == 0
        assert result["errors"] == {}
        assert result["sources"][0]["health"] == "not_modified"

    def test_incomplete_batch_run_status_is_degraded(self, tmp_path, monkeypatch):
        from quantmaster.ai import crawler as crawler_mod

        c = AICrawler(store=NewsStore(path=tmp_path / "news.sqlite"))
        source = c.source_store.create({
            "name": "缺口 RSS", "kind": "rss", "group_name": "periodic",
            "url": "https://example.test/feed", "max_age_hours": 24,
        })
        monkeypatch.setattr(
            crawler_mod,
            "fetch_declarative_source",
            lambda *args, **kwargs: FetchBatch(
                source_id=source["id"], watermark="old", previous_watermark="old",
                pending_watermark="new", complete=False, health="degraded",
                error_code="watermark_not_reached",
            ),
        )
        result = c.run(sources=[source["id"]], skip_llm=True)
        listed = next(item for item in c.source_store.list() if item["id"] == source["id"])
        assert result["sources"][0]["health"] == "degraded"
        assert listed["last_status"] == "degraded"
        assert listed["health"] == "degraded"


class TestSentimentPanel:
    def test_aggregation_and_decay(self, tmp_path):
        store = NewsStore(path=tmp_path / "news.sqlite")
        store.save([
            _item("大利好", "2024-05-06 09:00:00", symbols=["600519.SH"], sentiment=0.9),
            _item("小利空", "2024-05-06 10:00:00", symbols=["600519.SH"], sentiment=-0.1),
            _item("其他", "2024-05-08 09:00:00", symbols=["000858.SZ"], sentiment=0.5),
        ])
        panel = sentiment_panel(store=store, halflife_days=1.0)
        assert set(panel.columns) == {"600519.SH", "000858.SZ"}
        day1 = panel.loc["2024-05-06", "600519.SH"]
        assert day1 == (0.9 - 0.1) / 2   # 同日取均值
        day2 = panel.loc["2024-05-07", "600519.SH"]
        assert abs(day2 - day1 * 0.5) < 1e-9   # 半衰期 1 天 -> 次日减半
