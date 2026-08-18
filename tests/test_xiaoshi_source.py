from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace

import httpx
import pytest

from quantmaster.ai.news_providers import fetch_xiaoshi
from quantmaster.data.xiaoshi_source import (
    API_BASE,
    XiaoshiClient,
    XiaoshiError,
    XiaoshiPublicationStore,
)


class _ReadyPublications:
    def __init__(self):
        self.calls = 0

    def ensure_current(self):
        self.calls += 1
        return {"manifest_version": "test"}


def _publication_manifest(bodies: dict[str, bytes]) -> dict:
    return {
        "manifest_version": "1",
        "prompt_version": "1",
        "skill_version": "1",
        "prompt_url": f"{API_BASE}/prompt",
        "skill_url": f"{API_BASE}/skill",
        "api_schema_url": f"{API_BASE}/schema",
        "checksums": {
            "prompt_sha256": hashlib.sha256(bodies["/prompt"]).hexdigest(),
            "skill_sha256": hashlib.sha256(bodies["/skill"]).hexdigest(),
            "api_schema_sha256": hashlib.sha256(bodies["/schema"]).hexdigest(),
        },
    }


def test_publications_check_manifest_once_and_skip_unchanged_bodies(tmp_path):
    bodies = {"/prompt": b"prompt", "/skill": b"skill", "/schema": b"schema"}
    manifest = _publication_manifest(bodies)
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        assert request.headers["cache-control"] == "no-store, no-cache"
        if request.url.path == "/api/v3/manifest":
            return httpx.Response(200, json=manifest)
        return httpx.Response(200, content=bodies[request.url.path])

    client = httpx.Client(transport=httpx.MockTransport(handler))
    first = XiaoshiPublicationStore(tmp_path, client)
    first.ensure_current()
    first.ensure_current()
    second_task = XiaoshiPublicationStore(tmp_path, client)
    second_task.ensure_current()

    assert calls.count("/api/v3/manifest") == 2
    assert calls.count("/prompt") == 1
    assert calls.count("/skill") == 1
    assert calls.count("/schema") == 1
    assert next(tmp_path.glob("*.SKILL.md")).read_bytes() == b"skill"


def test_publication_failure_keeps_last_verified_version(tmp_path):
    bodies = {"/prompt": b"prompt", "/skill": b"skill", "/schema": b"schema"}
    first_manifest = _publication_manifest(bodies)
    state = {"manifest": first_manifest}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v3/manifest":
            return httpx.Response(200, json=state["manifest"])
        if request.url.path == "/changed-skill":
            return httpx.Response(200, content=b"wrong bytes")
        return httpx.Response(200, content=bodies[request.url.path])

    client = httpx.Client(transport=httpx.MockTransport(handler))
    XiaoshiPublicationStore(tmp_path, client).ensure_current()
    changed = json.loads(json.dumps(first_manifest))
    changed["skill_version"] = "2"
    changed["skill_url"] = f"{API_BASE}/changed-skill"
    changed["checksums"]["skill_sha256"] = hashlib.sha256(b"expected").hexdigest()
    state["manifest"] = changed

    returned = XiaoshiPublicationStore(tmp_path, client).ensure_current()

    assert returned["skill_version"] == "1"
    assert next(tmp_path.glob("*.SKILL.md")).read_bytes() == b"skill"


def test_client_authenticates_before_quote_and_validates_identity():
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        assert request.headers["authorization"] == "Bearer secret-value"
        if request.url.path.endswith("api-key/check"):
            return httpx.Response(200, json={"valid": True})
        return httpx.Response(200, json={
            "market": "CN", "instrument": "stock", "symbol": "600519",
            "name": "贵州茅台", "price": 1,
        })

    publications = _ReadyPublications()
    client = XiaoshiClient(
        "secret-value",
        client=httpx.Client(base_url=API_BASE, transport=httpx.MockTransport(handler)),
        publications=publications,
    )

    quote = client.quote(
        "600519", market="CN", instrument="stock", expected_name="贵州茅台",
    )

    assert quote["name"] == "贵州茅台"
    assert paths == ["/api/v3/auth/api-key/check", "/api/v3/market/quote/600519"]
    assert publications.calls == 1
    with pytest.raises(XiaoshiError, match="名称"):
        client.quote("600519", market="CN", instrument="stock", expected_name="平安银行")


def test_client_waits_for_retry_after_without_reporting_a_bug():
    attempts = 0
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        if request.url.path.endswith("api-key/check"):
            return httpx.Response(200, json={"valid": True})
        attempts += 1
        if attempts == 1:
            return httpx.Response(429, headers={"Retry-After": "2"}, json={})
        return httpx.Response(200, json={
            "market": "US", "instrument": "stock", "symbol": "AAPL",
            "name": "Apple", "price": 1,
        })

    client = XiaoshiClient(
        "secret-value",
        client=httpx.Client(base_url=API_BASE, transport=httpx.MockTransport(handler)),
        publications=_ReadyPublications(),
        sleeper=sleeps.append,
    )

    assert client.quote("AAPL", market="US", instrument="stock")["name"] == "Apple"
    assert sleeps == [2.0]


def test_r2_download_has_no_api_authorization_header(monkeypatch):
    body = b"verified parquet bytes"
    observed: dict = {}

    def fake_get(url, **kwargs):
        observed.update(kwargs)
        return httpx.Response(200, content=body, request=httpx.Request("GET", url))

    monkeypatch.setattr(httpx, "get", fake_get)
    client = XiaoshiClient("secret-value", publications=_ReadyPublications())
    session = {"files": {
        "url": "https://bucket.r2.cloudflarestorage.com/file",
        "size": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
    }}

    assert client.download_files(session)[0][1] == body
    assert "headers" not in observed


def test_history_and_timeline_contracts_reject_guessed_dimensions():
    client = XiaoshiClient("secret-value", publications=_ReadyPublications())

    with pytest.raises(ValueError, match="数据集"):
        client.history_session("guessed-dataset")
    with pytest.raises(ValueError, match="31 天"):
        client.financial_timeline(since="2026-01-01", to="2026-03-01")
    with pytest.raises(ValueError, match="market"):
        client.quote("600519", market="", instrument="stock")


def test_news_provider_advances_only_the_returned_after_id(monkeypatch):
    calls: list[dict] = []

    class FakeClient:
        @staticmethod
        def news(**kwargs):
            calls.append(kwargs)
            return {
                "next_after_id": 124,
                "has_more": True,
                "data": [{
                    "id": 124,
                    "title": "金融事件",
                    "content": "事件摘要",
                    "pub_time": "2026-08-18T12:00:00+08:00",
                    "original_url": "https://example.test/news/124",
                }],
            }

    class FakeStore:
        @staticmethod
        def save_response(*_args, **_kwargs):
            return "sha256:raw"

    monkeypatch.setattr(
        "quantmaster.config.get_config",
        lambda: SimpleNamespace(data=SimpleNamespace(xiaoshi_news_enabled=True)),
    )
    monkeypatch.setattr(
        "quantmaster.data.xiaoshi_source.get_xiaoshi_client", lambda: FakeClient(),
    )
    batch = fetch_xiaoshi(
        {
            "id": "xiaoshi",
            "name": "小石",
            "url": f"{API_BASE}/api/v3/news",
            "item_limit": 100,
            "max_age_hours": 6,
        },
        FakeStore(),
        "123",
        100,
    )

    assert calls == [{"after_id": 123, "page_size": 100}]
    assert batch.watermark == "124"
    assert batch.complete is True
    assert [item.provider_item_id for item in batch.articles] == ["124"]
