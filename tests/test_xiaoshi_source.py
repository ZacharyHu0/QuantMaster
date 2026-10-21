from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pandas as pd
import pytest

from quantmaster.ai.news_providers import fetch_xiaoshi
from quantmaster.data.xiaoshi_source import (
    API_BASE,
    XiaoshiBulkDownloadRequired,
    XiaoshiClient,
    XiaoshiError,
    XiaoshiPublicationStore,
    XiaoshiRateLimited,
    XiaoshiSource,
)


def _v3_manifest(capabilities: bytes) -> dict:
    return {
        "contract": "xiaoshi-agent-contract/v1",
        "platform_version": "2026-09-05.4",
        "prompt_version": "2026-09-06.1",
        "capabilities_url": f"{API_BASE}/api/v3/capabilities",
        "bootstrap_url": f"{API_BASE}/api/v3/bootstrap",
        "capabilities_sha256": hashlib.sha256(capabilities).hexdigest(),
    }


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


def test_v3_manifest_stores_hashed_capabilities_and_bootstrap(tmp_path):
    capabilities = json.dumps({"contract": "xiaoshi-agent-contract/v1"}).encode()
    manifest = _v3_manifest(capabilities)
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path == "/api/v3/manifest":
            return httpx.Response(200, json=manifest)
        if request.url.path == "/api/v3/capabilities":
            return httpx.Response(200, content=capabilities)
        return httpx.Response(200, json={"contract": "xiaoshi-agent-bootstrap/v1"})

    store = XiaoshiPublicationStore(
        tmp_path,
        httpx.Client(transport=httpx.MockTransport(handler)),
    )
    assert store.capabilities()["contract"] == "xiaoshi-agent-contract/v1"
    assert store.bootstrap()["contract"] == "xiaoshi-agent-bootstrap/v1"
    assert calls.count("/api/v3/manifest") == 1
    assert calls.count("/api/v3/capabilities") == 1


def test_default_rate_limit_stops_without_retry():
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if request.url.path.endswith("api-key/check"):
            return httpx.Response(200, json={"valid": True})
        return httpx.Response(429, headers={"Retry-After": "4"}, json={})

    client = XiaoshiClient(
        "secret-value",
        client=httpx.Client(base_url=API_BASE, transport=httpx.MockTransport(handler)),
        publications=_ReadyPublications(),
    )
    with pytest.raises(XiaoshiRateLimited) as error:
        client.quote("AAPL", market="US", instrument="stock")
    assert error.value.retry_after == 4
    assert calls == 2


def test_bulk_download_required_is_a_cli_handoff():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("api-key/check"):
            return httpx.Response(200, json={"valid": True})
        return httpx.Response(409, json={"code": "bulk_download_required"})

    client = XiaoshiClient(
        "secret-value",
        client=httpx.Client(base_url=API_BASE, transport=httpx.MockTransport(handler)),
        publications=_ReadyPublications(),
    )
    with pytest.raises(XiaoshiBulkDownloadRequired, match="xiaoshi-data"):
        client.query_dataset("daily")


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


def test_history_uses_cli_and_requires_verified_provenance(tmp_path, monkeypatch):
    import quantmaster.data.xiaoshi_source as module

    monkeypatch.setattr(module, "get_config", lambda: SimpleNamespace(
        data_root=tmp_path, data=SimpleNamespace(provider_timeout=30),
    ))
    calls = []
    frame = pd.DataFrame({"symbol": ["600519"], "close": [1263.0]})

    def cli(*args):
        calls.append(args)
        if args[0] == "verify":
            root = Path(args[args.index("--data-dir") + 1])
            root.mkdir(parents=True, exist_ok=True)
            frame.to_parquet(root / "data.parquet")
            (root / "verify.json").write_text(json.dumps({
                "valid": True, "provenance_verified": True,
                "files": [{"path": "data.parquet"}],
            }))
        return ""

    client = XiaoshiClient(api_key="", publications=_ReadyPublications())
    monkeypatch.setattr(client, "_history_cli", cli)
    result = client.history_frame("daily-stock", market="CN", code="600519", year=2026, adjust="qfq")
    assert result["close"].tolist() == [1263.0]
    assert [call[0] for call in calls] == ["download", "verify"]
    assert all("--api-key" not in call for call in calls)
    report = Path(calls[-1][calls[-1].index("--output") + 1])
    report.write_text(json.dumps({"valid": True, "provenance_verified": False}))
    monkeypatch.setattr(client, "_history_cli", lambda *_args: "")
    with pytest.raises(XiaoshiError, match="校验"):
        client.history_frame("daily-stock", market="CN", code="000001", year=2026, adjust="qfq")


def test_cli_passes_secret_only_in_environment(tmp_path, monkeypatch):
    import quantmaster.data.xiaoshi_source as module

    monkeypatch.setattr(module.Path, "home", lambda: tmp_path)
    command = tmp_path / ".local/share/xiaoshi-agent-tools-managed/bin" / (
        "xiaoshi-data.cmd" if module.os.name == "nt" else "xiaoshi-data"
    )
    command.parent.mkdir(parents=True)
    command.touch()
    client = XiaoshiClient(api_key=bytes(range(65, 73)).decode(), publications=_ReadyPublications())
    observed = {}

    def run(argv, **kwargs):
        observed.update(argv=argv, **kwargs)
        return SimpleNamespace(returncode=0, stdout="ok")

    monkeypatch.setattr(module.subprocess, "run", run)
    assert client._history_cli("verify", "--data-dir", str(tmp_path)) == "ok"
    assert client.api_key not in " ".join(observed["argv"])
    assert observed["env"]["XIAOSHI_API_KEY"] == client.api_key
    assert observed["timeout"] == 180


@pytest.mark.parametrize("minute", [False, True])
def test_canonical_history_filters_identity_adjustment_and_time(monkeypatch, minute):
    import quantmaster.data.xiaoshi_source as module

    monkeypatch.setattr(module, "get_config", lambda: SimpleNamespace(data=SimpleNamespace(
        xiaoshi_history_enabled=True, xiaoshi_minute_enabled=True,
    )))
    date = "2026-10-09 09:31:00+08:00" if minute else "2026-10-09"
    frame = pd.DataFrame({
        "canonical_schema_version": ["history-canonical-v2"] * 3,
        "symbol": ["600519", "000001", "600519"], "market": ["CN"] * 3,
        "adjustment": ["raw" if minute else "qfq", "raw" if minute else "qfq", "hfq"],
        "trade_time" if minute else "trade_date": [date] * 3,
        "open": [10, 20, 30], "high": [11, 21, 31], "low": [9, 19, 29],
        "close": [10.5, 20.5, 30.5], "volume": [100, 200, 300],
    })
    calls = []

    def history(dataset, **dimensions):
        calls.append((dataset, dimensions))
        return frame

    source = XiaoshiSource(SimpleNamespace(history_frame=history))
    result = (source.intraday("600519.SH", "2026-10-09 09:30", "2026-10-09 10:00", "1m")
              if minute else source.daily("600519.SH", "2026-10-09", "2026-10-09"))
    assert result["close"].tolist() == [10.5]
    assert result.index[0] == pd.Timestamp("2026-10-09 09:31" if minute else "2026-10-09")
    assert calls[0][0] == ("cn-minute" if minute else "daily-stock")
    if minute:
        assert calls[0][1] == {"code": "600519"}
    frame.drop(columns="canonical_schema_version", inplace=True)
    with pytest.raises(XiaoshiError, match="canonical"):
        source.daily("600519.SH", "2026-10-09", "2026-10-09")


def test_history_and_timeline_contracts_reject_guessed_dimensions():
    client = XiaoshiClient("secret-value", publications=_ReadyPublications())

    with pytest.raises(ValueError, match="数据集"):
        client.history_frame("guessed-dataset")
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
