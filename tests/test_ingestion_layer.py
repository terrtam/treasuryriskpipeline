from __future__ import annotations

from datetime import date
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq

from src.ingestion import (
    ingest_fx_files,
    ingest_transaction_files,
    load_parquet_rows,
    validate_fx_batch,
    validate_transaction_batch,
)


def test_fx_duplicate_key_rejection():
    valid_rows, rejections = validate_fx_batch(
        [
            {
                "date": date(2026, 6, 7),
                "base_currency": "USD",
                "quote_currency": "EUR",
                "fx_rate": "1.10",
            },
            {
                "date": date(2026, 6, 7),
                "base_currency": "USD",
                "quote_currency": "EUR",
                "fx_rate": "1.11",
            },
        ]
    )

    assert valid_rows == []
    assert len(rejections.rejections) == 2
    assert all("duplicate" in rejection.reason for rejection in rejections.rejections)


def test_fx_rejects_non_usd_base_currency():
    valid_rows, rejections = validate_fx_batch(
        [
            {
                "date": date(2026, 6, 7),
                "base_currency": "CAD",
                "quote_currency": "EUR",
                "fx_rate": "1.10",
            }
        ]
    )

    assert valid_rows == []
    assert len(rejections.rejections) == 1
    assert "USD" in rejections.rejections[0].reason


def test_load_parquet_rows_reads_valid_file(tmp_path):
    path = tmp_path / "daily_transactions_001.parquet"
    table = pa.table(
        {
            "transaction_id": ["T1"],
            "timestamp": ["2026-06-07T10:00:00Z"],
            "legal_entity_id": ["LE1"],
            "currency": ["USD"],
            "amount": [Decimal("10.00")],
            "direction": ["INBOUND"],
        }
    )
    pq.write_table(table, path)

    rows = load_parquet_rows(path)

    assert rows[0]["transaction_id"] == "T1"
    assert rows[0]["amount"] == Decimal("10.00")


def test_ingest_transaction_files_records_file_errors(tmp_path):
    good_path = tmp_path / "daily_transactions_001.parquet"
    bad_path = tmp_path / "daily_transactions_002.parquet"
    pq.write_table(
        pa.table(
            {
                "transaction_id": ["T1"],
                "timestamp": ["2026-06-07T10:00:00Z"],
                "legal_entity_id": ["LE1"],
                "currency": ["USD"],
                "amount": [Decimal("10.00")],
                "direction": ["INBOUND"],
            }
        ),
        good_path,
    )
    bad_path.write_text("not parquet", encoding="utf-8")

    result = ingest_transaction_files(tmp_path)

    assert len(result.records) == 1
    assert result.records[0].transaction_id == "T1"
    assert result.file_errors


def test_transaction_validation_rejects_invalid_direction():
    valid_rows, rejections = validate_transaction_batch(
        [
            {
                "transaction_id": "T1",
                "timestamp": "2026-06-07T10:00:00Z",
                "legal_entity_id": "LE1",
                "currency": "USD",
                "amount": "10.00",
                "direction": "IN",
            }
        ]
    )

    assert valid_rows == []
    assert len(rejections.rejections) == 1
    assert "INBOUND or OUTBOUND" in rejections.rejections[0].reason
