from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
import re
from typing import Any, Iterable, Mapping

import pyarrow.parquet as pq

from .rejections import RejectionReport

ISO_4217_PATTERN = re.compile(r"^[A-Z]{3}$")
VALID_DIRECTIONS = {"INBOUND", "OUTBOUND"}
TRANSACTION_FILE_PATTERN = "daily_transactions_*.parquet"
FX_FILE_PATTERN = "fx_rates_*.parquet"


@dataclass(frozen=True)
class ValidatedTransaction:
    transaction_id: str
    timestamp: datetime
    legal_entity_id: str
    currency: str
    amount: Decimal
    direction: str


@dataclass(frozen=True)
class ValidatedFXRate:
    date: Any
    base_currency: str
    quote_currency: str
    fx_rate: Decimal


@dataclass(frozen=True)
class IngestionBatchResult:
    records: list[Any]
    rejections: RejectionReport
    file_errors: list[str]
    file_error_sources: list[tuple[str, str]] = field(default_factory=list)
    status: str = "SUCCESS"


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


def discover_ingestion_files(data_feeds_dir: str | Path, dataset: str) -> list[Path]:
    root = Path(data_feeds_dir)
    pattern = TRANSACTION_FILE_PATTERN if dataset == "transactions" else FX_FILE_PATTERN
    if root.is_file():
        return [root] if root.match(pattern) else []
    return sorted(root.glob(pattern))


def load_parquet_rows(path: str | Path) -> list[dict[str, Any]]:
    parquet_path = Path(path)
    try:
        table = pq.read_table(parquet_path)
    except Exception as exc:  # pragma: no cover - exercised via tests
        raise ValueError(f"unable to read parquet file: {parquet_path}") from exc
    return table.to_pylist()


def _collect_rows(file_paths: Iterable[Path]) -> tuple[list[dict[str, Any]], list[str], list[tuple[str, str]]]:
    rows: list[dict[str, Any]] = []
    file_errors: list[str] = []
    file_error_sources: list[tuple[str, str]] = []

    for file_path in file_paths:
        try:
            rows.extend(load_parquet_rows(file_path))
        except ValueError as exc:
            message = str(exc)
            file_errors.append(message)
            file_error_sources.append((str(file_path), message))

    return rows, file_errors, file_error_sources


def _derive_transaction_status(valid_row_count: int, rejection_count: int, file_error_count: int) -> str:
    if file_error_count:
        return "FAILED"
    if rejection_count and valid_row_count:
        return "DEGRADED"
    if rejection_count:
        return "FAILED"
    return "SUCCESS"


def _derive_fx_status(valid_row_count: int, rejection_count: int, file_error_count: int) -> str:
    if file_error_count or rejection_count:
        return "FAILED"
    if valid_row_count:
        return "SUCCESS"
    return "FAILED"


def ingest_transaction_files(data_feeds_dir: str | Path) -> IngestionBatchResult:
    rows, file_errors, file_error_sources = _collect_rows(discover_ingestion_files(data_feeds_dir, "transactions"))
    records, rejections = validate_transaction_batch(rows)
    status = _derive_transaction_status(len(records), len(rejections), len(file_errors))
    return IngestionBatchResult(
        records=records,
        rejections=rejections,
        file_errors=file_errors,
        file_error_sources=file_error_sources,
        status=status,
    )


def ingest_fx_files(data_feeds_dir: str | Path) -> IngestionBatchResult:
    rows, file_errors, file_error_sources = _collect_rows(discover_ingestion_files(data_feeds_dir, "fx"))
    records, rejections = validate_fx_batch(rows)
    status = _derive_fx_status(len(records), len(rejections), len(file_errors))
    return IngestionBatchResult(
        records=records,
        rejections=rejections,
        file_errors=file_errors,
        file_error_sources=file_error_sources,
        status=status,
    )


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


def _parse_fx_rate(value: Any) -> Decimal:
    if value is None or isinstance(value, bool):
        raise ValueError("fx_rate missing or invalid")
    try:
        fx_rate = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError("fx_rate missing or invalid") from exc
    if fx_rate <= 0:
        raise ValueError("fx_rate must be positive")
    return fx_rate


def _validate_fx_currency(value: Any, field_name: str) -> str:
    if field_name == "base_currency" and value == "USD":
        return "USD"
    if not isinstance(value, str) or not ISO_4217_PATTERN.match(value):
        raise ValueError(f"{field_name} must be uppercase ISO 4217")
    return value


def validate_fx_batch(rows: Iterable[Mapping[str, Any]]) -> tuple[list[ValidatedFXRate], RejectionReport]:
    materialized = [dict(row) for row in rows]
    rejection_report = RejectionReport()
    seen_keys: Counter[tuple[Any, Any, Any]] = Counter()

    for row in materialized:
        key = (row.get("date"), row.get("base_currency"), row.get("quote_currency"))
        if all(part is not None for part in key):
            seen_keys[key] += 1

    valid_rows: list[ValidatedFXRate] = []
    for row_index, row in enumerate(materialized):
        try:
            date = row.get("date")
            if date is None:
                raise ValueError("date missing or invalid")
            base_currency = _validate_fx_currency(row.get("base_currency"), "base_currency")
            if base_currency != "USD":
                raise ValueError("base_currency must be USD")
            quote_currency = _validate_fx_currency(row.get("quote_currency"), "quote_currency")
            fx_rate = _parse_fx_rate(row.get("fx_rate"))
            key = (date, base_currency, quote_currency)
            if seen_keys[key] > 1:
                raise ValueError("duplicate fx key")
            valid_rows.append(
                ValidatedFXRate(
                    date=date,
                    base_currency=base_currency,
                    quote_currency=quote_currency,
                    fx_rate=fx_rate,
                )
            )
        except ValueError as exc:
            rejection_report.add(row_index, row, str(exc))

    return valid_rows, rejection_report
