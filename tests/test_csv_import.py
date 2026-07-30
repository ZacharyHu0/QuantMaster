"""券商 CSV 编码、映射、容错、重复与事务测试。"""

from __future__ import annotations

import json
import sqlite3

import pytest
from fastapi.testclient import TestClient

from quantmaster.portfolio import Ledger
from quantmaster.portfolio.csv_import import parse_broker_csv
from quantmaster.server.app import app

CSV_TEXT = (
    "成交日期,证券代码,买卖方向,成交价格,成交数量,佣金,印花税\n"
    "2024-01-08,600519,买入,1620,100,5,0\n"
    "2024-02-01,600519.SH,卖出,1700,100,5,8.5\n"
)


@pytest.mark.parametrize("encoding", ["utf-8", "utf-8-sig", "gb18030"])
def test_encoding_auto_mapping_side_and_fee_sum(encoding):
    parsed = parse_broker_csv(CSV_TEXT.encode(encoding))
    assert parsed.encoding in {"utf-8", "utf-8-sig", "gb18030"}
    assert [row.record["side"] for row in parsed.rows] == ["buy", "sell"]
    assert parsed.rows[1].record["fee"] == pytest.approx(13.5)
    assert parsed.rows[0].record["symbol"] == "600519.SH"


def test_bad_rows_and_duplicates_are_reported(tmp_path):
    content = (CSV_TEXT + "bad-date,600519,买入,x,100,0,0\n").encode()
    ledger = Ledger(path=tmp_path / "ledger.sqlite")
    first = parse_broker_csv(content, existing_fingerprints=ledger.fingerprints())
    assert len(first.valid_rows) == 2
    assert len([row for row in first.rows if row.errors]) == 1
    ledger.import_records([row.record for row in first.valid_rows], first.file_hash, "x.csv", first.encoding)
    second = parse_broker_csv(content, existing_fingerprints=ledger.fingerprints())
    assert all(row.duplicate for row in second.valid_rows)
    assert ledger.has_import_hash(first.file_hash)


@pytest.mark.parametrize("value", ["NaN", "Inf", "-Inf"])
def test_nonfinite_csv_numbers_are_rejected(value):
    parsed = parse_broker_csv(
        f"date,symbol,side,price,shares\n2024-01-02,600519,buy,{value},100\n".encode()
    )
    assert parsed.rows[0].record is None
    assert "必须为正数" in parsed.rows[0].errors[0]


def test_import_records_rolls_back_whole_transaction(tmp_path):
    ledger = Ledger(path=tmp_path / "ledger.sqlite")
    with sqlite3.connect(ledger.path) as connection:
        connection.execute(
            "CREATE TRIGGER reject_bad BEFORE INSERT ON trades WHEN NEW.symbol='000002.SZ' "
            "BEGIN SELECT RAISE(ABORT, 'reject'); END"
        )
    records = [
        {"date": "2024-01-01", "symbol": "000001.SZ", "side": "buy", "price": 10,
         "shares": 100, "fee": 1, "note": "", "fingerprint": "a"},
        {"date": "2024-01-01", "symbol": "000002.SZ", "side": "buy", "price": 10,
         "shares": 100, "fee": 1, "note": "", "fingerprint": "b"},
    ]
    with pytest.raises(sqlite3.IntegrityError):
        ledger.import_records(records, "hash", "test.csv", "utf-8")
    assert ledger.trades().empty
    assert not ledger.has_import_hash("hash")


def test_multipart_preview_strict_and_tolerant_submit():
    client = TestClient(app)
    settings = client.get("/api/settings").json()
    headers = {"X-CSRF-Token": settings["csrf_token"]}
    content = (
        b"date,symbol,side,price,shares,fee\n"
        b"2024-01-02,600519,buy,10,100,5\n"
        b"bad,000001,sell,x,100,5\n"
    )
    preview = client.post(
        "/api/ledger/import/preview", headers=headers,
        files={"file": ("broker.csv", content, "text/csv")},
    )
    assert preview.status_code == 200
    assert preview.json()["valid_count"] == 1
    mapping = json.dumps(preview.json()["suggested_mapping"])
    strict = client.post(
        "/api/ledger/import/submit", headers=headers,
        files={"file": ("broker.csv", content, "text/csv")},
        data={"mapping": mapping, "strict": "true", "include_duplicates": "false"},
    )
    assert strict.status_code == 422
    assert strict.json()["detail"]["failed_rows"]
    tolerant = client.post(
        "/api/ledger/import/submit", headers=headers,
        files={"file": ("broker.csv", content, "text/csv")},
        data={"mapping": mapping, "strict": "false", "include_duplicates": "false"},
    )
    assert tolerant.status_code == 200
    assert tolerant.json()["imported"] == 1
    assert tolerant.json()["skipped_invalid"] == 1
