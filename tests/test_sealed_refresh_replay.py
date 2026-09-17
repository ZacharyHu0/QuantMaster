import io
import json
import os
import zipfile
from pathlib import Path

import pandas as pd
import pytest

from quantmaster.data import registry
from quantmaster.data.instruments import InstrumentStore
from quantmaster.data.storage import BarStore
from quantmaster.data.tushare_source import TushareSource


@pytest.mark.parametrize(
    "symbol",
    [
        "000333.SZ",
        "000338.SZ",
        "000723.SZ",
        "000725.SZ",
        "002028.SZ",
        "002064.SZ",
        "600000.SH",
        "600497.SH",
        "600704.SH",
        "600900.SH",
        "601608.SH",
        "603160.SH",
        "688065.SH",
        "601238.SH",
    ],
)
def test_sealed_replay(symbol, isolated_config, tmp_path, monkeypatch):
    if "SEALED_ROOT" not in os.environ:
        pytest.skip("requires privately sealed installed samples")
    root = Path(os.environ["SEALED_ROOT"])
    with zipfile.ZipFile(root / "r4-all14-source-candidates.zip") as archive:
        evidence = json.loads(archive.read("evidence.json"))
    with zipfile.ZipFile(root / "r4-bars-before-private.zip") as archive:
        original_bytes = archive.read(f"{symbol}.parquet")
        old = pd.read_parquet(io.BytesIO(original_bytes))
    item = json.loads((root / "r4-before-private.json").read_text(encoding="utf-8"))["bars"][symbol]
    InstrumentStore().upsert(
        [evidence["identity"][symbol]], source="sealed-tushare-catalog", source_priority=100
    )
    dates = json.loads((root / "r4-candidate-isolated-replay.json").read_text(encoding="utf-8"))[
        "calendar_dates"
    ]
    sessions = pd.DatetimeIndex(pd.to_datetime(dates))
    monkeypatch.setattr(
        registry,
        "_local_sessions",
        lambda start, end: (sessions[(sessions >= start) & (sessions <= end)], "research_lake"),
    )
    monkeypatch.setattr(
        registry, "market_now", lambda: pd.Timestamp("2026-09-17T20:00:00+08:00").to_pydatetime()
    )
    monkeypatch.setattr(registry, "market_date", lambda: pd.Timestamp("2026-09-17").date())
    start, end = min("2015-01-01", str(old.index.min().date())), "2026-09-17"
    with zipfile.ZipFile(root / "r4-all14-source-candidates.zip") as archive:

        def endpoint(name):
            match = next(
                item
                for item in evidence["lookups"]
                if item.get("exists")
                and item["endpoint"] == name
                and item["params"]["ts_code"] == symbol
                and item["params"]["start_date"] == start.replace("-", "")
            )
            return pd.read_parquet(io.BytesIO(archive.read(match["artifact"])))

        raw, factors = endpoint("daily"), endpoint("adj_factor")
    source = TushareSource()
    with zipfile.ZipFile(root / "r4-calendar-raw-supplement.zip") as archive:
        calendar = pd.read_parquet(io.BytesIO(archive.read("calendar-1.parquet")))

    def calendar_call(endpoint, ttl, **params):
        assert endpoint == "trade_cal"
        assert params["start_date"] == "20150101" and params["end_date"] == "20260917"
        return calendar.copy()

    monkeypatch.setattr(source, "_call", calendar_call)
    frame = source._qfq_frame(symbol, start, end, raw, factors)
    monkeypatch.setattr(source, "cached_daily", lambda *args: frame.copy())
    monkeypatch.setattr(source, "daily", lambda *args: pytest.fail("unexpected remote request"))
    monkeypatch.setattr(registry, "_request_factories", lambda **kw: {registry.Market.CN: [lambda: source]})
    store = BarStore(tmp_path / "bars")
    store.put(
        symbol,
        old,
        replace=True,
        replace_coverage=True,
        source="tushare",
        request_start=item["metadata"]["coverage_start"],
        request_end=end,
        quality=json.loads(item["metadata"]["quality_json"]),
    )
    store.mark_status(symbol, "stale")
    before = store._path(symbol).read_bytes(), store.metadata(symbol)
    quality = registry._assess_daily_frame(frame, start, end, symbol=symbol, source="tushare")
    report = {
        "symbol": symbol,
        "raw_rows": len(raw),
        "old_rows": len(old),
        "candidate_rows": len(frame),
        "lost_dates": old.index.difference(frame.index).strftime("%Y-%m-%d").tolist(),
        "added_dates": frame.index.difference(old.index).strftime("%Y-%m-%d").tolist(),
        "candidate_quality": quality.to_dict(),
    }
    try:
        registry.refresh_history(symbol, "2015-01-01", end, store=store, mode="auto")
    except Exception as exc:
        report["rejection"] = str(exc)
        assert (store._path(symbol).read_bytes(), store.metadata(symbol)) == before
    else:
        saved = store.get(symbol)
        report["published_rows"] = len(saved)
        report["published_end"] = str(saved.index.max().date())
        report["saved_quality"] = json.loads(store.metadata(symbol)["quality_json"])
        assert old.index.difference(saved.index).empty
        assert report["published_end"] == end
        assert not registry.read_history(symbol, "2015-01-01", end, store).quality.formal_eligible
        if symbol == "600704.SH":
            from quantmaster.data.maintenance import DataRefreshManager

            assert store.metadata(symbol)["coverage_start"] == "2015-02-13"
            monkeypatch.setattr(registry, "_request_factories", lambda **kw: pytest.fail("repeat source"))
            outcome = DataRefreshManager._refresh_one(store, symbol, "2015-01-01", end)
            report["wide_outcome"] = outcome
            assert outcome["code"] == "historical_start_incomplete" and not outcome["retryable"]
            narrow = registry.read_history(symbol, "2026-09-10", end, store)
            assert narrow.quality.status == "degraded" and not narrow.quality.formal_eligible
            assert not any(issue.startswith("响应起点 ") for issue in narrow.quality.issues)
    output = Path(os.environ["REPLAY_OUTPUT"]) / (symbol + ".json")
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    if symbol == "601238.SH":
        assert "rejection" in report
        from quantmaster.data.instrument_snapshots import freeze_suspension_snapshot
        from quantmaster.data.maintenance import DataRefreshManager

        assert DataRefreshManager._refresh_one(store, symbol, "2015-01-01", end)["code"] == (
            "history_repair_rejected"
        )
        with zipfile.ZipFile(root / "r4-601238-authorized-supplement.zip") as archive:
            for day in ("14", "15", "16", "17"):
                freeze_suspension_snapshot(json.loads(archive.read(f"official-2026-09-{day}.json")))
        outcome = DataRefreshManager._refresh_one(store, symbol, "2015-01-01", end)
        assert outcome["code"] == "history_repair_rejected" and "warning" not in outcome
        assert (store._path(symbol).read_bytes(), store.metadata(symbol)) == before
        report["strict_suspension_outcome"] = outcome
        # The sealed Sep 15 raw response has BOTH S and R. Legacy normalized
        # symbols drop the R row; they cannot prove a full day without trading.
        from quantmaster.data.instrument_snapshots import load_suspension_snapshot

        receipts = [load_suspension_snapshot(f"2026-09-{day}") for day in ("14", "15", "16", "17")]
        assert [r["trade_date"] for r in receipts if symbol not in r["full_day_symbols"]] == ["2026-09-15"]
        report["unconfirmed_full_day_dates"] = ["2026-09-15"]
        output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        assert "rejection" not in report, report.get("rejection")
