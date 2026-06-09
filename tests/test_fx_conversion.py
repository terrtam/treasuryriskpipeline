from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from src.fx import FxDatasetError, convert_transactions_to_usd
from src.ingestion.validator import ValidatedFXRate, ValidatedTransaction


def _txn(transaction_id: str, timestamp: datetime, currency: str, amount: str) -> ValidatedTransaction:
    return ValidatedTransaction(
        transaction_id=transaction_id,
        timestamp=timestamp,
        legal_entity_id="LE1",
        currency=currency,
        amount=Decimal(amount),
        direction="INBOUND",
    )


def _fx(fx_date: date, quote_currency: str, rate: str) -> ValidatedFXRate:
    return ValidatedFXRate(
        date=fx_date,
        base_currency="USD",
        quote_currency=quote_currency,
        fx_rate=Decimal(rate),
    )


def test_converts_usd_transactions_with_identity_rate():
    transactions = [_txn("T1", datetime(2026, 6, 7, 10, 0, tzinfo=timezone.utc), "USD", "10.00")]
    fx_rates = [_fx(date(2026, 6, 7), "USD", "1.0")]

    converted, rejections = convert_transactions_to_usd(transactions, fx_rates)

    assert rejections.rejections == []
    assert len(converted) == 1
    assert converted[0].fx_rate_applied == Decimal("1.0")
    assert converted[0].amount_usd == Decimal("10.00")


def test_uses_event_date_not_processing_order_for_fx_lookup():
    transactions = [
        _txn("T1", datetime(2026, 6, 8, 1, 0, tzinfo=timezone.utc), "EUR", "10.00"),
        _txn("T2", datetime(2026, 6, 7, 23, 0, tzinfo=timezone.utc), "EUR", "10.00"),
    ]
    fx_rates = [
        _fx(date(2026, 6, 7), "USD", "1.0"),
        _fx(date(2026, 6, 7), "EUR", "1.10"),
        _fx(date(2026, 6, 8), "USD", "1.0"),
        _fx(date(2026, 6, 8), "EUR", "1.20"),
    ]

    converted, rejections = convert_transactions_to_usd(transactions, fx_rates)

    assert rejections.rejections == []
    assert [record.transaction_id for record in converted] == ["T1", "T2"]
    assert converted[0].amount_usd == Decimal("12.0000")
    assert converted[1].amount_usd == Decimal("11.0000")


def test_missing_non_usd_rate_rejects_only_the_affected_transaction():
    transactions = [
        _txn("T1", datetime(2026, 6, 7, 10, 0, tzinfo=timezone.utc), "EUR", "10.00"),
        _txn("T2", datetime(2026, 6, 7, 10, 0, tzinfo=timezone.utc), "USD", "25.00"),
    ]
    fx_rates = [
        _fx(date(2026, 6, 7), "USD", "1.0"),
    ]

    converted, rejections = convert_transactions_to_usd(transactions, fx_rates)

    assert [record.transaction_id for record in converted] == ["T2"]
    assert len(rejections.rejections) == 1
    assert rejections.rejections[0].row["transaction_id"] == "T1"
    assert "missing fx rate" in rejections.rejections[0].reason


def test_missing_usd_identity_rate_is_systemic():
    transactions = [_txn("T1", datetime(2026, 6, 7, 10, 0, tzinfo=timezone.utc), "EUR", "10.00")]
    fx_rates = [_fx(date(2026, 6, 7), "EUR", "1.10")]

    with pytest.raises(FxDatasetError, match="USD identity rate"):
        convert_transactions_to_usd(transactions, fx_rates)


def test_duplicate_fx_rates_are_systemic():
    transactions = [_txn("T1", datetime(2026, 6, 7, 10, 0, tzinfo=timezone.utc), "EUR", "10.00")]
    fx_rates = [
        _fx(date(2026, 6, 7), "USD", "1.0"),
        _fx(date(2026, 6, 7), "EUR", "1.10"),
        _fx(date(2026, 6, 7), "EUR", "1.11"),
    ]

    with pytest.raises(FxDatasetError, match="duplicate fx key"):
        convert_transactions_to_usd(transactions, fx_rates)
