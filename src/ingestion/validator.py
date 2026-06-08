from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
import re
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping

from .rejections import RejectionReport

ISO_4217_PATTERN = re.compile(r"^[A-Z]{3}$")
VALID_DIRECTIONS = {"INBOUND", "OUTBOUND"}


@dataclass(frozen=True)
class ValidatedTransaction:
    transaction_id: str
    timestamp: datetime
    legal_entity_id: str
    currency: str
    amount: Decimal
    direction: str


def _parse_timestamp(value: Any) -> datetime:
    if value is None or isinstance(value, bool):
        raise ValueError("timestamp missing or invalid")
    if isinstance(value, datetime):
        ts = value
    elif isinstance(value, str):
        try:
            ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("timestamp missing or invalid") from exc
    else:
        raise ValueError("timestamp missing or invalid")

    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    ts = ts.astimezone(timezone.utc)
    return ts


def _parse_amount(value: Any) -> Decimal:
    if value is None or isinstance(value, bool):
        raise ValueError("amount missing or invalid")
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError("amount missing or invalid") from exc
    if amount < 0:
        raise ValueError("amount must be non-negative")
    return amount


def _validate_currency(value: Any) -> str:
    if not isinstance(value, str) or not ISO_4217_PATTERN.match(value):
        raise ValueError("currency must be uppercase ISO 4217")
    return value


def _validate_direction(value: Any) -> str:
    if value not in VALID_DIRECTIONS:
        raise ValueError("direction must be INBOUND or OUTBOUND")
    return value


def validate_transaction_batch(rows: Iterable[Mapping[str, Any]]) -> tuple[list[ValidatedTransaction], RejectionReport]:
    materialized = [dict(row) for row in rows]
    rejection_report = RejectionReport()
    seen_ids: Counter[str] = Counter()

    for row in materialized:
        transaction_id = row.get("transaction_id")
        if isinstance(transaction_id, str) and transaction_id:
            seen_ids[transaction_id] += 1

    valid_rows: list[ValidatedTransaction] = []
    for row_index, row in enumerate(materialized):
        try:
            transaction_id = row.get("transaction_id")
            if not isinstance(transaction_id, str) or not transaction_id:
                raise ValueError("transaction_id missing or invalid")
            if seen_ids[transaction_id] > 1:
                raise ValueError("duplicate transaction_id")

            timestamp = _parse_timestamp(row.get("timestamp"))

            legal_entity_id = row.get("legal_entity_id")
            if not isinstance(legal_entity_id, str) or not legal_entity_id:
                raise ValueError("legal_entity_id missing or invalid")

            currency = _validate_currency(row.get("currency"))
            amount = _parse_amount(row.get("amount"))
            direction = _validate_direction(row.get("direction"))

            valid_rows.append(
                ValidatedTransaction(
                    transaction_id=transaction_id,
                    timestamp=timestamp,
                    legal_entity_id=legal_entity_id,
                    currency=currency,
                    amount=amount,
                    direction=direction,
                )
            )
        except ValueError as exc:
            rejection_report.add(row_index, row, str(exc))

    return valid_rows, rejection_report

