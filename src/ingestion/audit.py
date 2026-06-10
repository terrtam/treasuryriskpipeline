from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any, Mapping
from uuid import NAMESPACE_URL, UUID, uuid5

from .rejections import Rejection, RejectionReport
from src.liquidity.window import LiquiditySnapshot

AUDIT_NAMESPACE = uuid5(NAMESPACE_URL, "treasury-pipeline/ingestion-audit")


@dataclass(frozen=True)
class AuditEvent:
    event_id: str
    event_type: str
    run_id: str
    pipeline_version: str
    dataset_version: str
    source_file: str | None
    transaction_id: str | None
    legal_entity_id: str | None
    event_timestamp_utc: datetime
    processing_timestamp_utc: datetime
    currency: str | None
    amount_original: Decimal | None
    fx_rate_applied: Decimal | None
    amount_usd: Decimal | None
    direction: str | None
    window_start_utc: datetime | None
    window_end_utc: datetime | None
    status: str
    error_code: str | None = None
    error_message: str | None = None


def _utc_datetime(value: Any, fallback: datetime) -> datetime:
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    if isinstance(value, date):
        return datetime.combine(value, time.min, tzinfo=timezone.utc)
    return fallback


def _make_event_id(*parts: str) -> str:
    payload = "|".join(parts)
    return str(uuid5(AUDIT_NAMESPACE, payload))


def _build_rejection_event(
    rejection: Rejection,
    *,
    run_id: str,
    pipeline_version: str,
    dataset_version: str,
    source_file: str | None,
    processing_timestamp_utc: datetime,
    event_type: str,
    status: str,
) -> AuditEvent:
    row = dict(rejection.row)
    transaction_id = row.get("transaction_id") if isinstance(row.get("transaction_id"), str) else None
    legal_entity_id = row.get("legal_entity_id") if isinstance(row.get("legal_entity_id"), str) else None
    currency = row.get("currency") if isinstance(row.get("currency"), str) else None
    direction = row.get("direction") if isinstance(row.get("direction"), str) else None
    event_timestamp_utc = _utc_datetime(row.get("timestamp") or row.get("date"), processing_timestamp_utc)

    amount_value = row.get("amount")
    amount_original = amount_value if isinstance(amount_value, Decimal) else None
    fx_rate_value = row.get("fx_rate")
    fx_rate_applied = fx_rate_value if isinstance(fx_rate_value, Decimal) else None

    event_id = _make_event_id(
        run_id,
        pipeline_version,
        dataset_version,
        source_file or "",
        str(rejection.row_index),
        event_type,
        rejection.reason,
        transaction_id or "",
        legal_entity_id or "",
    )

    return AuditEvent(
        event_id=event_id,
        event_type=event_type,
        run_id=run_id,
        pipeline_version=pipeline_version,
        dataset_version=dataset_version,
        source_file=source_file,
        transaction_id=transaction_id,
        legal_entity_id=legal_entity_id,
        event_timestamp_utc=event_timestamp_utc,
        processing_timestamp_utc=processing_timestamp_utc,
        currency=currency,
        amount_original=amount_original,
        fx_rate_applied=fx_rate_applied,
        amount_usd=None,
        direction=direction,
        window_start_utc=None,
        window_end_utc=None,
        status=status,
        error_code="INGESTION_REJECTION",
        error_message=rejection.reason,
    )


def build_transaction_processed_audit_events(
    transactions: list[Any],
    *,
    run_id: str,
    pipeline_version: str,
    dataset_version: str,
    processing_timestamp_utc: datetime,
    source_file: str | None = None,
) -> list[AuditEvent]:
    events: list[AuditEvent] = []
    for transaction in transactions:
        transaction_id = transaction.transaction_id if isinstance(transaction.transaction_id, str) else None
        legal_entity_id = transaction.legal_entity_id if isinstance(transaction.legal_entity_id, str) else None
        currency = transaction.currency if isinstance(transaction.currency, str) else None
        direction = transaction.direction if isinstance(transaction.direction, str) else None
        event_id = _make_event_id(
            run_id,
            pipeline_version,
            dataset_version,
            source_file or "",
            transaction_id or "",
            legal_entity_id or "",
            "transaction_processed",
        )
        events.append(
            AuditEvent(
                event_id=event_id,
                event_type="transaction_processed",
                run_id=run_id,
                pipeline_version=pipeline_version,
                dataset_version=dataset_version,
                source_file=source_file,
                transaction_id=transaction_id,
                legal_entity_id=legal_entity_id,
                event_timestamp_utc=_utc_datetime(transaction.timestamp, processing_timestamp_utc),
                processing_timestamp_utc=processing_timestamp_utc,
                currency=currency,
                amount_original=transaction.amount if isinstance(transaction.amount, Decimal) else None,
                fx_rate_applied=None,
                amount_usd=None,
                direction=direction,
                window_start_utc=None,
                window_end_utc=None,
                status="SUCCESS",
                error_code=None,
                error_message=None,
            )
        )
    return events


def build_rejection_audit_events(
    rejection_report: RejectionReport,
    *,
    run_id: str,
    pipeline_version: str,
    dataset_version: str,
    source_file: str | None,
    processing_timestamp_utc: datetime,
    event_type: str,
    status: str,
) -> list[AuditEvent]:
    return [
        _build_rejection_event(
            rejection,
            run_id=run_id,
            pipeline_version=pipeline_version,
            dataset_version=dataset_version,
            source_file=source_file,
            processing_timestamp_utc=processing_timestamp_utc,
            event_type=event_type,
            status=status,
        )
        for rejection in rejection_report.rejections
    ]


def build_file_failure_event(
    *,
    run_id: str,
    pipeline_version: str,
    dataset_version: str,
    source_file: str | None,
    processing_timestamp_utc: datetime,
    error_message: str,
    event_type: str = "sink_failed",
) -> AuditEvent:
    event_id = _make_event_id(
        run_id,
        pipeline_version,
        dataset_version,
        source_file or "",
        error_message,
        event_type,
    )
    return AuditEvent(
        event_id=event_id,
        event_type=event_type,
        run_id=run_id,
        pipeline_version=pipeline_version,
        dataset_version=dataset_version,
        source_file=source_file,
        transaction_id=None,
        legal_entity_id=None,
        event_timestamp_utc=processing_timestamp_utc,
        processing_timestamp_utc=processing_timestamp_utc,
        currency=None,
        amount_original=None,
        fx_rate_applied=None,
        amount_usd=None,
        direction=None,
        window_start_utc=None,
        window_end_utc=None,
        status="FAILED",
        error_code="INGESTION_FILE_READ_ERROR",
        error_message=error_message,
    )


def build_snapshot_written_audit_events(
    snapshots: list[LiquiditySnapshot],
    *,
    run_id: str,
    pipeline_version: str,
    dataset_version: str,
    processing_timestamp_utc: datetime,
    source_file: str | None = None,
) -> list[AuditEvent]:
    events: list[AuditEvent] = []
    for snapshot in snapshots:
        event_id = _make_event_id(
            run_id,
            pipeline_version,
            dataset_version,
            str(snapshot.snapshot_date),
            snapshot.legal_entity_id,
            snapshot.window_start_utc.isoformat(),
            snapshot.window_end_utc.isoformat(),
            str(snapshot.net_liquidity_usd),
            "snapshot_written",
        )
        events.append(
            AuditEvent(
                event_id=event_id,
                event_type="snapshot_written",
                run_id=run_id,
                pipeline_version=pipeline_version,
                dataset_version=dataset_version,
                source_file=source_file,
                transaction_id=None,
                legal_entity_id=snapshot.legal_entity_id,
                event_timestamp_utc=snapshot.window_end_utc,
                processing_timestamp_utc=processing_timestamp_utc,
                currency=snapshot.currency,
                amount_original=None,
                fx_rate_applied=None,
                amount_usd=snapshot.net_liquidity_usd,
                direction=None,
                window_start_utc=snapshot.window_start_utc,
                window_end_utc=snapshot.window_end_utc,
                status="SUCCESS",
                error_code=None,
                error_message=None,
            )
        )
    return events
