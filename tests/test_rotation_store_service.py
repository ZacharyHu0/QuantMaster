from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from quantmaster.rotation.analytics import compute_etf_capital_evidence
from quantmaster.rotation.contracts import RotationJobSpec
from quantmaster.rotation.service import (
    RotationService,
    RotationWorker,
    _overlay_stockdb_etf_prices,
)
from quantmaster.rotation.store import (
    RotationIntegrityError,
    RotationStore,
)
from quantmaster.runtime.jobs import JobOutcome, UnifiedJobStore
from quantmaster.runtime.sqlite import connect_sqlite


def _market(days: int = 100, symbols: int = 40):
    dates = pd.bdate_range("2025-03-03", periods=days)
    rng = np.random.default_rng(11)
    returns = rng.normal(0.0004, 0.012, (days, symbols))
    close = pd.DataFrame(
        30 * np.exp(np.cumsum(returns, axis=0)),
        index=dates,
        columns=[f"{600000 + index:06d}.SH" for index in range(symbols)],
    )
    return close, close * 800_000


def test_market_etf_evidence_overlays_stockdb_prices_for_unpriced_share_rows(
    monkeypatch,
):
    observations = pd.DataFrame([
        {
            "trade_date": "2026-08-07", "symbol": "510300.SH",
            "shares": 100.0, "nav": 4.0, "close": 4.0,
        },
        {
            "trade_date": "2026-08-10", "symbol": "510300.SH",
            "shares": 110.0, "nav": None, "close": None,
        },
    ])
    daily = pd.DataFrame([
        {"symbol": "510300.SH", "date": "2026-08-10", "close": 4.2},
    ])
    snapshot = SimpleNamespace(
        status="complete",
        assets={"etf": {"daily_rows": 1}},
        content_hashes={"etf_daily": "etf-daily-digest"},
        start_date="2026-08-01",
        end_date="2026-08-10",
        as_of_date="2026-08-10",
    )

    class FakeIngestStore:
        def history(self, _limit):
            return [snapshot]

        def load_frame(self, _snapshot, name):
            assert name == "etf_daily"
            return daily

    monkeypatch.setattr(
        "quantmaster.data.free_stockdb_ingest.StockDBIngestStore",
        FakeIngestStore,
    )
    enriched, source = _overlay_stockdb_etf_prices(
        observations,
        as_of="2026-08-10",
    )

    assert source == "local:stockdb:etf_daily"
    assert enriched.loc[1, "close"] == 4.2
    evidence = compute_etf_capital_evidence(
        enriched,
        as_of="2026-08-10",
        window=1,
        lookback=10,
        min_history=1,
        min_funds=1,
    )
    assert evidence["available"] is True
    assert evidence["as_of"] == "2026-08-10"


def test_rotation_store_round_trips_snapshots_preferences_and_auxiliary_data(tmp_path):
    store = RotationStore(tmp_path / "rotation")
    payload = {
        "meta": {
            "snapshot_id": "sample", "as_of": "2026-07-30",
            "generated_at": "2026-07-30T10:00:00+00:00",
        },
        "data": {"current": {"temperature": 42.5}},
    }
    store.save_snapshots({"temperature": payload})

    assert store.snapshot("temperature") == payload
    assert store.snapshots()[0]["snapshot_id"] == "sample"
    saved = store.save_preferences({"l2_codes": ["801081.SI", "801081.SI"], "theme_limit": 20})
    assert saved["l2_codes"] == ["801081.SI"]
    assert store.preferences()["theme_limit"] == 20

    store.replace_taxonomy_nodes([
        {"code": "801081.SI", "name": "半导体", "level": "L2", "parent_code": "801080.SI"}
    ])
    assert store.taxonomy_nodes("L2")[0]["name"] == "半导体"
    store.replace_themes([
        {"code": "BK1001", "name": "机器人", "members": ["600000.SH"]}
    ])
    assert store.themes()[0]["code"] == "BK1001"
    store.set_runtime_state("scheduled_close", "2026-07-30")
    assert store.runtime_state("scheduled_close") == "2026-07-30"


def test_etf_metadata_history_is_idempotent_and_rejects_identity_conflicts(tmp_path):
    store = RotationStore(tmp_path / "rotation")
    frame = pd.DataFrame(
        [
            {
                "symbol": "510300.SH",
                "name": "沪深300ETF",
                "observed_at": "2026-08-09T06:59:00+00:00",
                "effective_date": "2026-08-09",
            }
        ]
    )

    store.save_etf_metadata(frame)
    manifest_before = store.etf_metadata_history_manifest_path.read_bytes()
    store.save_etf_metadata(frame.copy())

    history = store.etf_metadata_history()
    assert len(history) == 1
    assert history["observation_id"].nunique() == 1
    assert history["observation_integrity"].eq("verified").all()
    assert store.etf_metadata_history_manifest_path.read_bytes() == manifest_before

    conflict = frame.assign(name="被改写的名称")
    with pytest.raises(RotationIntegrityError, match="观察身份出现冲突内容"):
        store.save_etf_metadata(conflict)
    assert store.etf_metadata_history().iloc[0]["name"] == "沪深300ETF"


def test_etf_metadata_history_file_and_manifest_are_tamper_evident(tmp_path):
    store = RotationStore(tmp_path / "rotation")
    frame = pd.DataFrame(
        [
            {
                "symbol": "510300.SH",
                "name": "沪深300ETF",
                "observed_at": "2026-08-09T06:59:00+00:00",
            }
        ]
    )
    store.save_etf_metadata(frame)
    tampered = pd.read_parquet(store.etf_metadata_history_path).assign(name="篡改名称")
    tampered.to_parquet(store.etf_metadata_history_path, index=False)

    with pytest.raises(RotationIntegrityError, match="文件哈希与 manifest 不匹配"):
        store.etf_metadata_history()

    clean = RotationStore(tmp_path / "clean-rotation")
    clean.save_etf_metadata(frame)
    manifest = json.loads(clean.etf_metadata_history_manifest_path.read_text(encoding="utf-8"))
    manifest["row_count"] = 999
    clean.etf_metadata_history_manifest_path.write_text(
        json.dumps(manifest), encoding="utf-8"
    )

    with pytest.raises(RotationIntegrityError, match="manifest 哈希不匹配"):
        clean.etf_metadata_history()


def test_legacy_etf_metadata_history_without_manifest_fails_closed(tmp_path):
    store = RotationStore(tmp_path / "rotation")
    pd.DataFrame(
        [
            {
                "symbol": "510300.SH",
                "name": "旧历史",
                "observed_at": "2026-08-09T06:59:00+00:00",
            }
        ]
    ).to_parquet(store.etf_metadata_history_path, index=False)

    with pytest.raises(RotationIntegrityError, match="缺少完整性 manifest"):
        store.etf_metadata_history()
    with pytest.raises(RotationIntegrityError, match="缺少完整性 manifest"):
        store.save_etf_metadata(
            pd.DataFrame(
                [
                    {
                        "symbol": "510500.SH",
                        "name": "新观察",
                        "observed_at": "2026-08-10T06:59:00+00:00",
                    }
                ]
            )
        )


def test_rotation_overview_cache_invalidates_on_snapshot_id(tmp_path, monkeypatch):
    store = RotationStore(tmp_path / "rotation")
    service = RotationService(store, UnifiedJobStore(tmp_path / "jobs.sqlite"))

    def payload(kind, snapshot_id):
        data = {"as_of": "2026-08-04", "items": []}
        if kind == "temperature":
            data.update({"current": {}, "history": []})
        elif kind == "structure":
            data["current"] = {}
        elif kind == "etf_flows":
            data.update({"summary": {}, "benchmarks": []})
        return {
            "meta": {
                "snapshot_id": snapshot_id, "as_of": "2026-08-04",
                "generated_at": "2026-08-04T10:00:00+00:00",
                "algorithm_version": "QM_ROTATION_V2", "sources": ["unit"],
                "quality": {"status": "complete", "issues": []},
            },
            "data": data,
        }

    store.save_snapshots({kind: payload(kind, "one") for kind in (
        "temperature", "structure", "industries", "themes", "etf_flows",
    )})
    from quantmaster.rotation import service as service_module

    calls = []
    original = service_module._rank_window
    monkeypatch.setattr(
        service_module, "_rank_window",
        lambda *args, **kwargs: calls.append(1) or original(*args, **kwargs),
    )
    service.overview()
    first_count = len(calls)
    assert service.snapshot("themes")["meta"]["quality"]["upgrade_pending"] is True
    service.overview()
    assert len(calls) == first_count
    store.save_snapshots({"themes": payload("themes", "two")})
    service.overview()
    assert len(calls) > first_count


def test_theme_staging_keeps_old_catalog_until_atomic_quality_commit(tmp_path):
    store = RotationStore(tmp_path / "rotation")
    old = {
        "code": "OLD", "name": "旧目录", "members": ["600000.SH"],
        "source": "eastmoney-concept",
    }
    store.replace_themes([old])
    staging = store.begin_theme_sync("ths:concept", "directory-hash", 1)
    assert staging["attempted_count"] == 0
    fresh = {
        "code": "301558", "name": "新目录", "members": ["920130.BJ"],
        "source": "ths:concept",
    }
    store.save_theme_sync_item(
        staging["run_id"], fresh["code"], fresh["name"], payload=fresh, pages=2,
    )

    assert store.themes() == [old]

    store.commit_theme_sync(staging["run_id"], [fresh], [])
    assert store.themes() == [fresh]
    resumed = store.begin_theme_sync("ths:concept", "directory-hash", 1)
    assert resumed["attempted_count"] == 1


def test_rotation_charts_use_adaptive_axes_and_scoped_zoom():
    script = (
        Path(__file__).parents[1] / "quantmaster" / "server" / "static" / "rotation.js"
    ).read_text(encoding="utf-8")
    stylesheet = (
        Path(__file__).parents[1] / "quantmaster" / "server" / "static" / "rotation.css"
    ).read_text(encoding="utf-8")

    assert "const padding = Math.max(3,span * .16)" in script
    assert "min:xRange.min,max:xRange.max" in script
    assert "min:yRange.min,max:yRange.max" in script
    assert script.count('id="rotation-industry-scatter"') == 1
    assert "rotation-radar-scatter" not in script
    assert "周期坐标与 ${activeWindow} 日轨迹" in script
    assert "name:'趋势向上占比'" in script
    assert "item.positive_ratio" in script
    assert "currentSignal.positive_change_pp" in script
    assert "评分为 75% 绝对结构与 25% 同层级相对证据" in script
    assert "scoreEvidenceMarkup(score)" in script
    assert "?window=${activeWindow}" in script
    assert "dataZoom:chartZoom(history.length,{initialPoints:252,initialYEnd:60})" in script
    assert "grid:{left:46,right:80,top:38,bottom:58}" in script
    assert "xAxis:{...timeAxis(),splitNumber:12}" in script
    assert "start:0,end:initialYEnd" in script
    assert script.count('id="rotation-temperature-recent-chart"') == 1
    assert script.count("slice(-15)") >= 2
    assert "近 15 日温度路径" in script
    assert "index % 3 === 0" in script
    assert "const padding = Math.max(3, (maximum - minimum) * .15)" in script
    assert "min:lower,max:upper > lower ? upper : Math.min(100,lower + 5)" in script
    assert "recentTemperatureChart(recent)" in script
    assert script.count('id="rotation-evidence-radar"') == 1
    assert "const currentComplete = evidence.every" in script
    assert "const previousComplete = evidence.every" in script
    assert "currentComplete && previousComplete" in script
    assert "type:'dashed'" in script
    assert "等待完整五维证据" in script
    assert 'data-temperature-window="${window}"' in script
    assert "TEMPERATURE_WINDOW_KEY" in script
    assert "rotation-meter-reference" in script
    assert "dataZoom:chartZoom(points.length)" in script
    assert "dataZoom:chartZoom(daily.length,{initialPoints:260})" in script
    assert "dataZoom:chartZoom(daily.length,{yAxisIndex:[0,1]" not in script
    assert "orient:'vertical',yAxisIndex" in script
    assert script.count('class="rotation-layout two rotation-temperature-layout"') == 1
    assert 'class="rotation-regime" data-regime="${esc(current.regime || \'unavailable\')}"' in script
    assert "每只股票只归入一档，四档合计等于参与计算的股票总数" in script
    assert "同一有效样本互斥归类，家数严格守恒" not in script
    assert ".rotation-layout.two.rotation-temperature-layout" in stylesheet
    assert "grid-template-columns:minmax(0,2.3fr) minmax(240px,.5fr)" in stylesheet
    assert ".rotation-temperature-recent-chart { height:136px; }" in stylesheet
    assert ".rotation-evidence-radar { height:240px; }" in stylesheet
    assert ".rotation-meter-reference" in stylesheet
    assert "grid-template-columns:minmax(220px,.7fr) minmax(0,1.3fr)" in stylesheet
    assert ".rotation-state-row { grid-template-columns:52px minmax(0,1fr) 92px; gap:8px; }" in stylesheet
    assert '.rotation-regime[data-regime="ice"] { color:var(--s1); }' in stylesheet
    assert '.rotation-regime[data-regime="contraction"] { color:var(--down); }' in stylesheet
    assert '.rotation-regime[data-regime="expansion"] { color:var(--up); }' in stylesheet
    assert '.rotation-regime[data-regime="overheat"] { color:var(--s4); }' in stylesheet
    assert ".rotation-table.rotation-ranking-table" in stylesheet
    assert "width:100%; min-width:0; table-layout:fixed;" in stylesheet
    assert ".rotation-ranking-table th:nth-child(1)" in stylesheet
    assert ".rotation-ranking-table th:nth-child(2)" in stylesheet
    assert ".rotation-ranking-table th:nth-child(n+3)" in stylesheet


def test_market_style_confirmation_path_uses_compact_chart_layout():
    script = (
        Path(__file__).parents[1] / "quantmaster" / "server" / "static" / "rotation.js"
    ).read_text(encoding="utf-8")
    stylesheet = (
        Path(__file__).parents[1] / "quantmaster" / "server" / "static" / "rotation.css"
    ).read_text(encoding="utf-8")

    assert script.count('id="rotation-style-path-chart"') == 1
    assert "structurePathChart(data.history || [])" in script
    assert "step:'end'" in script
    assert "const structureBarPoint = (row, key, direction) =>" in script
    assert "direction * Math.abs(rawReturn)" in script
    assert "name:'强势样本',type:'bar'" in script
    assert "name:'低位样本',type:'bar'" in script
    assert "barGap:'-100%'" in script
    assert "itemStyle:{color:CHART_COLORS.up,borderRadius:[2,2,0,0]}" in script
    assert "itemStyle:{color:CHART_COLORS.down,borderRadius:[0,0,2,2]}" in script
    assert "name:'强弱差',type:'line',showSymbol:false" in script
    assert "lineStyle:{color:CHART_COLORS.primary,width:1.8,type:'solid'}" in script
    assert "min:-structureExtent,max:structureExtent" in script
    assert "rgba(201,150,66,.07)" in script
    assert "data:[[{yAxis:-.0025},{yAxis:.0025}]]" in script
    assert "const levelStyles = {'-1':'weak','0':'balanced','1':'strong'};" in script
    assert "rgba(36,160,107,.055)" in script
    assert "rgba(79,143,216,.055)" in script
    assert "rgba(230,103,103,.055)" in script
    assert 'data-style="${esc(style)}" data-confirmation="${confirmation}"' in script
    assert 'data-state="${esc(row.state || \'unavailable\')}"' in script
    assert 'class="rotation-structure-aside"' in script
    assert "rotation-path-strip" not in script
    assert ".rotation-layout.two.rotation-style-layout" in stylesheet
    assert "grid-template-columns:minmax(0,1.7fr) minmax(300px,.7fr)" in stylesheet
    assert ".rotation-style-path-chart { height:136px; }" in stylesheet
    assert ".rotation-style-current-kpi[data-style=\"strong_dominant\"]" in stylesheet
    assert ".rotation-style-current-kpi[data-style=\"weak_rebound\"]" in stylesheet
    assert ".rotation-style-current-kpi[data-style=\"balanced\"]" in stylesheet
    assert "background:color-mix(in oklch,var(--style-tone) 7%,var(--page));" in stylesheet
    assert ".rotation-style-current-kpi[data-confirmation=\"pending\"]" in stylesheet
    assert ".rotation-style-distribution .rotation-state-row[data-state=\"strong_up\"]" in stylesheet
    assert ".rotation-style-distribution .rotation-state-row[data-state=\"range\"]" in stylesheet
    assert ".rotation-style-distribution .rotation-state-row[data-state=\"weak\"]" in stylesheet
    assert ".rotation-style-heading[data-tone=\"strong\"]" in stylesheet
    assert ".rotation-style-heading[data-tone=\"weak\"]" in stylesheet


def test_rotation_jobs_use_the_unified_lease_ledger(tmp_path):
    jobs = UnifiedJobStore(tmp_path / "jobs.sqlite")
    spec = {"scope": "all", "mode": "incremental", "source": "local"}
    created, created_new = jobs.submit("rotation.refresh", spec, input_fingerprint="v1")
    duplicate, duplicate_new = jobs.submit("rotation.refresh", spec, input_fingerprint="v1")
    assert created_new is True
    assert duplicate_new is False
    assert duplicate["id"] == created["id"]

    assert jobs.claim(created["id"], "worker-one", lease_seconds=5)
    claimed = jobs.get(created["id"])
    jobs.progress(
        claimed["id"], "worker-one", claimed["lease_token"], 50, "计算中", "一半",
    )
    jobs.finish(
        claimed["id"], "worker-one", JobOutcome("completed", "done"),
        lease_token=claimed["lease_token"],
    )
    completed = jobs.get(claimed["id"])
    assert completed["spec"] == created["spec"]
    assert completed["status"] == "completed"

    retried = jobs.retry(claimed["id"])
    assert retried["id"] == claimed["id"]
    assert retried["spec"] == claimed["spec"]
    assert any(event["type"] == "job_retried" for event in jobs.events(retried["id"]))


def test_rotation_worker_pause_interrupts_owned_job_without_cancelling_job(tmp_path):
    jobs = UnifiedJobStore(tmp_path / "jobs.sqlite")
    service = RotationService(RotationStore(tmp_path / "rotation"), jobs)
    worker = RotationWorker(service, isolated=False)
    created, _ = jobs.submit(
        "rotation.refresh",
        {"scope": "themes", "mode": "incremental", "source": "auto"},
    )
    assert jobs.claim(created["id"], worker.identity.value)
    worker.stop()

    handed_off = jobs.get(created["id"])
    assert handed_off["status"] == "interrupted"
    assert handed_off["cancel_requested"] is False
    assert handed_off["lease_expires"] == 0
    assert jobs.events(created["id"])[-1]["type"] == "job_interrupted"
    assert jobs.claim(created["id"], "worker-next")
    reclaimed = jobs.get(created["id"])
    assert reclaimed["attempt"] == 2
    worker.shutdown()


def test_rotation_schedule_marks_success_only_after_fresh_completion(tmp_path):
    store = RotationStore(tmp_path / "rotation")
    service = RotationService(store, UnifiedJobStore(tmp_path / "jobs.sqlite"))
    worker = RotationWorker(service, isolated=False)
    spec = RotationJobSpec(scope="close", source="auto")
    date_key = str(pd.Timestamp.now(tz="Asia/Shanghai").date())

    worker._record_scheduled_result(spec, succeeded=False)

    retry = store.runtime_state("scheduled_close_retry").split("|")
    assert retry[:2] == [date_key, "1"]
    assert store.runtime_state("scheduled_close") == ""
    assert worker._scheduled_retry_due("close", date_key) is False

    worker._record_scheduled_result(spec, succeeded=True)
    assert store.runtime_state("scheduled_close") == date_key
    assert store.runtime_state("scheduled_close_retry") == ""


def test_rotation_service_builds_coherent_views_from_local_matrices(tmp_path, monkeypatch):
    store = RotationStore(tmp_path / "rotation")
    jobs = UnifiedJobStore(tmp_path / "rotation-jobs.sqlite")
    service = RotationService(store, jobs)
    close, amount = _market()
    names = {symbol: f"股票{index}" for index, symbol in enumerate(close.columns)}
    from quantmaster.rotation import service as service_module

    trend_calls = []
    real_compute_trend = service_module.compute_trend_matrices

    def counted_trend(values):
        trend_calls.append(len(values))
        return real_compute_trend(values)

    monkeypatch.setattr(service_module, "compute_trend_matrices", counted_trend)
    monkeypatch.setattr(
        service_module,
        "compute_etf_capital_evidence",
        lambda *_args, as_of, **_kwargs: {
            "available": True,
            "score": 18.45,
            "as_of": str(as_of),
            "note": "近 5 日净申购率 -3.98%",
            "fund_count": 20,
            "reference_windows": 252,
        },
    )
    sentiment_calls: list[tuple[str, float | None]] = []
    monkeypatch.setattr(
        service_module,
        "_news_sentiment_evidence",
        lambda as_of, **kwargs: (
            sentiment_calls.append((as_of, kwargs.get("knowledge_as_of")))
            or {
                "available": True,
                "score": 53.95,
                "as_of": as_of,
                "note": "中性 +7.90",
                "event_count": 120,
            }
        ),
    )

    def load_values(*, progress, cancelled):
        assert not cancelled()
        progress(20, "测试行情", "已准备")
        return close, amount, names, len(close.columns), ["test:local"]

    service.loader.market_matrices = load_values
    monkeypatch.setattr(
        service_module,
        "_expected_market_session",
        lambda: str(close.index[-1].date()),
    )
    industry_names = ("电子", "计算机", "机械设备", "医药生物")
    industry_map = {
        symbol: industry_names[index // 10]
        for index, symbol in enumerate(close.columns)
    }
    monkeypatch.setattr(
        "quantmaster.rotation.service.load_cached_industry_map",
        lambda: industry_map,
    )
    store.replace_themes([
        {
            "code": "BK1001", "name": "主题一",
            "members": list(close.columns[:16]),
        },
        {
            "code": "BK1002", "name": "主题一别名",
            "members": list(close.columns[:15]),
        },
    ])
    etf = pd.DataFrame([
        {"trade_date": "2026-07-29", "symbol": "510300.SH", "shares": 100, "nav": 4.0},
        {"trade_date": "2026-07-30", "symbol": "510300.SH", "shares": 105, "nav": 4.1},
    ])
    store.save_etf_observations(etf)
    updates: list[tuple[int, str]] = []
    result = service.build(
        RotationJobSpec(source="local"),
        progress=lambda value, phase, detail: updates.append((value, phase)),
        cancelled=lambda: False,
    )

    assert result["tracked_count"] == 40
    assert set(result["updated"]) == {
        "etf_flows", "industries", "structure", "taxonomy", "temperature", "themes",
    }
    assert len(sentiment_calls) == 5
    knowledge_cutoffs = {value for _, value in sentiment_calls}
    assert len(knowledge_cutoffs) == 1
    assert next(iter(knowledge_cutoffs)) is not None
    temperature = service.snapshot("temperature")
    industries = service.snapshot("industries")
    themes = service.snapshot("themes")
    etf_flows = service.snapshot("etf_flows")
    assert temperature["meta"]["batch_id"] == industries["meta"]["batch_id"]
    assert temperature["data"]["evidence"]["available_weight"] == 100
    evidence = {
        item["id"]: item for item in temperature["data"]["evidence"]["items"]
    }
    assert evidence["etf_capital"]["score"] == 18.45
    assert evidence["sentiment"]["score"] == 53.95
    changes = temperature["data"]["change_windows"]
    assert changes["default_window"] == 5
    assert set(changes["windows"]) == {"1", "3", "5", "20"}
    assert changes["windows"]["5"]["evidence"]["comparable_count"] == 5
    assert changes["windows"]["5"]["reference_as_of"] == str(close.index[-6].date())
    assert "tushare:fund_share" in temperature["meta"]["sources"]
    assert "local:news" in temperature["meta"]["sources"]
    assert len(industries["data"]["items"]) == 4
    assert len(themes["data"]["items"]) == 1
    assert set(industries["data"]["items"][0]["signals"]) == {"1", "3", "5", "20"}
    assert themes["data"]["items"][0]["primary_industry"] is not None
    assert "tushare:fund_nav" in etf_flows["meta"]["sources"]
    assert etf_flows["meta"]["quality"]["status"] == "complete"
    overview = service.overview()
    assert overview["data"]["temperature"] is not None
    assert overview["data"]["rankings"]["5"]["industries"]["available"] == 4
    assert len(overview["data"]["resonance"]["5"]) == 4
    assert overview["data"]["etf_context"]["summary"]["windows"]["1"]["net_flow"] == 20.5
    assert updates[-1][0] == 96
    assert trend_calls == [len(close)]

    close_fingerprints = service.snapshot_input_fingerprints(
        RotationJobSpec(scope="close", source="local"),
    )
    assert all(
        service.snapshot_header(kind)["meta"].get("input_fingerprint")
        == close_fingerprints[kind]
        for kind in ("temperature", "structure", "industries", "themes", "taxonomy")
    )
    close_result = service.build(
        RotationJobSpec(scope="close", source="local"),
        progress=lambda *_: None,
        cancelled=lambda: False,
    )
    assert close_result["updated"] == []
    assert close_result["outcome"] == "unchanged"
    # A narrower task against the same published input must short-circuit
    # before loading matrices or rebuilding trend state.
    assert trend_calls == [len(close)]


def test_partial_theme_provider_uses_catalog_denominator_and_deduplicates_issues(
    tmp_path, monkeypatch,
):
    store = RotationStore(tmp_path / "rotation")
    service = RotationService(store, UnifiedJobStore(tmp_path / "jobs.sqlite"))
    close, amount = _market()
    names = {symbol: f"股票{index}" for index, symbol in enumerate(close.columns)}
    monkeypatch.setattr(
        "quantmaster.rotation.service._expected_market_session",
        lambda: str(close.index[-1].date()),
    )
    service.loader.market_matrices = lambda **_: (
        close, amount, names, len(close.columns), ["test:local"],
    )
    store.replace_themes([{
        "code": "THS_SAMPLE",
        "name": "受限题材",
        "members": list(close.columns[:16]),
        "source": "ths:concept",
    }])

    class PartialProvider:
        def __init__(self, _store):
            pass

        @staticmethod
        def sync_market_history(progress, cancelled, *, rebuild):
            return {"expected_as_of": str(close.index[-1].date()), "issues": []}

        @staticmethod
        def sync_themes(progress, cancelled):
            return {
                "catalog": 100,
                "available": 75,
                "quality_status": "partial",
                "issues": ["受限目录", "受限目录"],
            }

    monkeypatch.setattr("quantmaster.rotation.provider.RotationProvider", PartialProvider)
    service.build(
        RotationJobSpec(scope="themes", source="auto"),
        progress=lambda *_: None,
        cancelled=lambda: False,
    )

    quality = service.snapshot("themes")["meta"]["quality"]
    assert quality["status"] == "partial"
    assert quality["eligible_count"] == 1
    assert quality["expected_count"] == 100
    assert quality["coverage"] == 0.01
    assert quality["issues"].count("受限目录") == 1


def test_rotation_job_cancel_queued_is_terminal(tmp_path):
    jobs = UnifiedJobStore(tmp_path / "jobs.sqlite")
    created, _ = jobs.submit(
        "rotation.refresh", {"scope": "etf", "mode": "incremental", "source": "local"},
    )
    cancelled = jobs.cancel(created["id"])
    assert cancelled["status"] == "cancelled"
    assert not jobs.claim(created["id"], "worker")
    assert jobs.get(created["id"])["cancel_requested"] is True
    assert jobs.events(created["id"])[-1]["type"] == "job_cancel_requested"


def test_rotation_overview_reports_dimensions_without_fabricating_zero_coverage(
    tmp_path,
):
    store = RotationStore(tmp_path / "rotation")
    service = RotationService(store, UnifiedJobStore(tmp_path / "jobs.sqlite"))
    meta = {
        "snapshot_id": "sample",
        "as_of": "2026-07-30",
        "generated_at": "2026-07-30T10:00:00+00:00",
        "sources": ["test:local"],
    }
    store.save_snapshots({
        "temperature": {
            "meta": {**meta, "quality": {"status": "complete", "issues": []}},
            "data": {"current": {"temperature": 42.0}},
        },
        "industries": {
            "meta": {**meta, "quality": {"status": "partial", "issues": []}},
            "data": {"items": []},
        },
        "etf_flows": {
            "meta": {**meta, "quality": {"status": "complete", "issues": []}},
            "data": {"summary": {"status": "ready"}, "items": []},
        },
    })

    quality = service.overview()["meta"]["quality"]

    assert quality["status"] == "partial"
    assert quality["coverage"] is None
    assert quality["eligible_count"] is None
    assert quality["expected_count"] is None
    assert quality["available_dimensions"] == 3
    assert quality["total_dimensions"] == 4
    assert quality["dimension_statuses"]["细分题材"] == "cold"
    assert "3/4 个维度可用" in quality["issues"][0]


def test_rotation_snapshot_hash_failure_is_exposed_as_corrupt(tmp_path):
    store = RotationStore(tmp_path / "rotation")
    store.save_snapshots({
        "temperature": {
            "meta": {"snapshot_id": "ok", "as_of": "2026-07-30", "generated_at": "now"},
            "data": {"current": {"temperature": 30}},
        }
    })
    with connect_sqlite(store.cache_path, policy="cache") as connection:
        connection.execute(
            "UPDATE snapshots SET payload_json=? WHERE kind='temperature'",
            ('{"data":{"current":{"temperature":99}}}',),
        )

    with pytest.raises(RotationIntegrityError, match="哈希不匹配"):
        store.snapshot("temperature")
    public = RotationService(store, UnifiedJobStore(tmp_path / "jobs.sqlite")).snapshot(
        "temperature"
    )
    assert public["meta"]["quality"]["status"] == "corrupt"
    assert "损坏内容不会参与计算" in public["meta"]["quality"]["issues"][-1]
    assert "哈希不匹配" not in str(public)


def test_rotation_etf_file_corruption_is_not_treated_as_empty(tmp_path):
    store = RotationStore(tmp_path / "rotation")
    store.etf_path.write_bytes(b"not-a-parquet-file")

    with pytest.raises(RotationIntegrityError, match="ETF 观察文件损坏"):
        store.etf_observations()


def test_rotation_worker_bootstrap_is_explicit_and_close_scoped(tmp_path, monkeypatch):
    service = RotationService(
        RotationStore(tmp_path / "rotation"),
        UnifiedJobStore(tmp_path / "jobs.sqlite"),
    )

    class Morning:
        @staticmethod
        def now(_timezone):
            return pd.Timestamp("2026-07-30 10:00:00", tz="Asia/Shanghai")

    monkeypatch.setattr("quantmaster.rotation.service.datetime", Morning)
    ordinary = RotationWorker(service, isolated=False)
    monkeypatch.setattr(ordinary, "_run", lambda: ordinary._stop.wait())
    ordinary.start()
    assert service.jobs.list() == []
    ordinary.stop()

    bootstrap = RotationWorker(service, isolated=False)
    monkeypatch.setattr(bootstrap, "_run", lambda: bootstrap._stop.wait())
    bootstrap.start(bootstrap_local=True)
    specs = [item["spec"] for item in service.jobs.list()]
    assert {
        "scope": "close", "mode": "incremental", "source": "local", "as_of": "",
    } in specs
    assert {
        "scope": "themes", "mode": "incremental", "source": "auto", "as_of": "",
    } in specs
    bootstrap.stop()

