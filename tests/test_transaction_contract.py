from __future__ import annotations

from decimal import Decimal

from src.ingestion.validator import validate_transaction_batch


def test_invalid_timestamp_rejection():
    valid_rows, rejections = validate_transaction_batch(
        [
            {
                "transaction_id": "T1",
                "timestamp": "not-a-timestamp",
                "legal_entity_id": "LE1",
                "currency": "USD",
                "amount": "10.00",
                "direction": "INBOUND",
            }
        ]
    )

    assert valid_rows == []
    assert len(rejections.rejections) == 1
    assert "timestamp" in rejections.rejections[0].reason


def test_negative_amount_rejection():
    valid_rows, rejections = validate_transaction_batch(
        [
            {
                "transaction_id": "T2",
                "timestamp": "2026-06-07T10:00:00Z",
                "legal_entity_id": "LE1",
                "currency": "USD",
                "amount": "-1.00",
                "direction": "OUTBOUND",
            }
        ]
    )

    assert valid_rows == []
    assert len(rejections.rejections) == 1
    assert "non-negative" in rejections.rejections[0].reason


def test_invalid_currency_rejection():
    valid_rows, rejections = validate_transaction_batch(
        [
            {
                "transaction_id": "T3",
                "timestamp": "2026-06-07T10:00:00Z",
                "legal_entity_id": "LE1",
                "currency": "usd",
                "amount": "1.00",
                "direction": "INBOUND",
            }
        ]
    )

    assert valid_rows == []
    assert len(rejections.rejections) == 1
    assert "currency" in rejections.rejections[0].reason


def test_duplicate_transaction_id_rejection():
    valid_rows, rejections = validate_transaction_batch(
        [
            {
                "transaction_id": "T4",
                "timestamp": "2026-06-07T10:00:00Z",
                "legal_entity_id": "LE1",
                "currency": "USD",
                "amount": "1.00",
                "direction": "INBOUND",
            },
            {
                "transaction_id": "T4",
                "timestamp": "2026-06-07T11:00:00Z",
                "legal_entity_id": "LE2",
                "currency": "EUR",
                "amount": "2.00",
                "direction": "OUTBOUND",
            },
        ]
    )

    assert valid_rows == []
    assert len(rejections.rejections) == 2
    assert all("duplicate" in rejection.reason for rejection in rejections.rejections)


def test_valid_mixed_batch():
    valid_rows, rejections = validate_transaction_batch(
        [
            {
                "transaction_id": "T5",
                "timestamp": "2026-06-07T10:00:00Z",
                "legal_entity_id": "LE1",
                "currency": "USD",
                "amount": Decimal("100.00"),
                "direction": "INBOUND",
            },
            {
                "transaction_id": "T6",
                "timestamp": "bad-timestamp",
                "legal_entity_id": "LE2",
                "currency": "EUR",
                "amount": Decimal("25.00"),
                "direction": "OUTBOUND",
            },
        ]
    )

    assert len(valid_rows) == 1
    assert valid_rows[0].transaction_id == "T5"
    assert valid_rows[0].amount == Decimal("100.00")
    assert len(rejections.rejections) == 1
    assert rejections.rejections[0].row["transaction_id"] == "T6"
