from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Iterable, Mapping

from src.ingestion.rejections import RejectionReport
from src.ingestion.validator import ValidatedFXRate, ValidatedTransaction

USD = "USD"
USD_IDENTITY_RATE = Decimal("1.0")


class FxConversionError(ValueError):
    """Base error for FX conversion failures."""


class FxDatasetError(FxConversionError):
    """Raised when the FX reference set is internally inconsistent."""


@dataclass(frozen=True)
class USDNormalizedTransaction:
    transaction_id: str
    timestamp: datetime
    legal_entity_id: str
    currency: str
    amount: Decimal
    direction: str
    fx_rate_applied: Decimal
    amount_usd: Decimal


def _utc_date(value: datetime) -> date:
    ts = value
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).date()


def _materialize_fx_lookup(fx_rates: Iterable[ValidatedFXRate]) -> dict[tuple[date, str], Decimal]:
    lookup: dict[tuple[date, str], Decimal] = {}
    for fx_rate in fx_rates:
        fx_date = fx_rate.date
        if not isinstance(fx_date, date):
            raise FxDatasetError("fx date must be a date")
        if isinstance(fx_date, datetime):
            raise FxDatasetError("fx date must not be a datetime")
        if fx_rate.base_currency != USD:
            raise FxDatasetError("base_currency must be USD")
        key = (fx_date, fx_rate.quote_currency)
        if key in lookup:
            raise FxDatasetError("duplicate fx key")
        lookup[key] = fx_rate.fx_rate
    return lookup


def _validate_usd_identity_rates(
    fx_lookup: Mapping[tuple[date, str], Decimal],
    transaction_dates: set[date],
) -> None:
    for transaction_date in sorted(transaction_dates):
        rate = fx_lookup.get((transaction_date, USD))
        if rate is None:
            raise FxDatasetError(f"missing USD identity rate for {transaction_date.isoformat()}")
        if rate != USD_IDENTITY_RATE:
            raise FxDatasetError(f"USD identity rate must be exactly 1.0 for {transaction_date.isoformat()}")


def convert_transactions_to_usd(
    transactions: Iterable[ValidatedTransaction],
    fx_rates: Iterable[ValidatedFXRate],
) -> tuple[list[USDNormalizedTransaction], RejectionReport]:
    """
    Convert validated transactions to USD using the FX rate for the event date.

    Non-USD transactions without a matching FX rate are rejected downstream.
    A missing or incorrect USD identity rate is treated as a systemic dataset error.
    """

    materialized_transactions = list(transactions)
    materialized_fx_rates = list(fx_rates)

    fx_lookup = _materialize_fx_lookup(materialized_fx_rates)
    transaction_dates = {_utc_date(transaction.timestamp) for transaction in materialized_transactions}
    fx_dates = {fx_date for fx_date, _ in fx_lookup.keys()}
    _validate_usd_identity_rates(fx_lookup, transaction_dates | fx_dates)

    converted_transactions: list[USDNormalizedTransaction] = []
    rejection_report = RejectionReport()

    for row_index, transaction in enumerate(materialized_transactions):
        transaction_date = _utc_date(transaction.timestamp)
        if transaction.currency == USD:
            fx_rate_applied = USD_IDENTITY_RATE
        else:
            fx_rate_applied = fx_lookup.get((transaction_date, transaction.currency))
            if fx_rate_applied is None:
                rejection_report.add(
                    row_index,
                    {
                        "transaction_id": transaction.transaction_id,
                        "timestamp": transaction.timestamp,
                        "legal_entity_id": transaction.legal_entity_id,
                        "currency": transaction.currency,
                        "amount": transaction.amount,
                        "direction": transaction.direction,
                    },
                    f"missing fx rate for {transaction.currency} on {transaction_date.isoformat()}",
                )
                continue

        amount_usd = transaction.amount * fx_rate_applied
        converted_transactions.append(
            USDNormalizedTransaction(
                transaction_id=transaction.transaction_id,
                timestamp=transaction.timestamp,
                legal_entity_id=transaction.legal_entity_id,
                currency=transaction.currency,
                amount=transaction.amount,
                direction=transaction.direction,
                fx_rate_applied=fx_rate_applied,
                amount_usd=amount_usd,
            )
        )

    return converted_transactions, rejection_report
