from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from .audit import AuditEvent, build_file_failure_event, build_rejection_audit_events
from src.fx.conversion import FxDatasetError, convert_transactions_to_usd
from .rejections import RejectionReport
from .sinks import IngestionSinks, create_default_ingestion_sinks
from .validator import IngestionBatchResult, ingest_fx_files, ingest_transaction_files


@dataclass(frozen=True)
class IngestionBatchConfig:
    data_feeds_dir: str | Path
    run_id: str
    pipeline_version: str
    dataset_version: str
    processing_timestamp_utc: datetime | None = None

    def resolved_processing_timestamp_utc(self) -> datetime:
        if self.processing_timestamp_utc is not None:
            ts = self.processing_timestamp_utc
            if ts.tzinfo is None:
                return ts.replace(tzinfo=timezone.utc)
            return ts.astimezone(timezone.utc)
        return datetime.now(timezone.utc)


@dataclass(frozen=True)
class SinkWriteResult:
    sink_name: str
    status: str
    row_count: int
    error_message: str | None = None


@dataclass
class IngestionRunResult:
    transaction_batch: IngestionBatchResult
    fx_batch: IngestionBatchResult
    usd_batch: IngestionBatchResult
    audit_events: list[AuditEvent] = field(default_factory=list)
    sink_writes: list[SinkWriteResult] = field(default_factory=list)

    @property
    def has_failures(self) -> bool:
        return any(write.status != "SUCCESS" for write in self.sink_writes)


def _write_sink(name: str, writer, payload) -> SinkWriteResult:
    try:
        writer(payload)
    except Exception as exc:  # pragma: no cover - exercised in integration-style tests
        return SinkWriteResult(sink_name=name, status="FAILED", row_count=0, error_message=str(exc))
    return SinkWriteResult(sink_name=name, status="SUCCESS", row_count=len(payload))


def _empty_batch(status: str = "SKIPPED") -> IngestionBatchResult:
    return IngestionBatchResult(records=[], rejections=RejectionReport(), file_errors=[], file_error_sources=[], status=status)


def run_ingestion_batch(
    config: IngestionBatchConfig,
    sinks: IngestionSinks | None = None,
) -> IngestionRunResult:
    sinks = sinks or create_default_ingestion_sinks()
    processing_timestamp_utc = config.resolved_processing_timestamp_utc()
    data_feeds_dir = Path(config.data_feeds_dir)

    if data_feeds_dir.is_file():
        if "daily_transactions_" in data_feeds_dir.name:
            transaction_batch = ingest_transaction_files(data_feeds_dir)
            fx_batch = _empty_batch()
        elif "fx_rates_" in data_feeds_dir.name:
            transaction_batch = _empty_batch()
            fx_batch = ingest_fx_files(data_feeds_dir)
        else:
            raise ValueError(f"unable to infer dataset type from file name: {data_feeds_dir.name}")
    else:
        transaction_batch = ingest_transaction_files(data_feeds_dir)
        fx_batch = ingest_fx_files(data_feeds_dir)

    usd_batch = _empty_batch()
    if transaction_batch.status != "SKIPPED" and fx_batch.status == "SUCCESS":
        try:
            usd_records, usd_rejections = convert_transactions_to_usd(transaction_batch.records, fx_batch.records)
            usd_batch = IngestionBatchResult(
                records=usd_records,
                rejections=usd_rejections,
                file_errors=[],
                file_error_sources=[],
                status="SUCCESS" if not usd_rejections.rejections else "DEGRADED",
            )
        except FxDatasetError as exc:
            usd_batch = IngestionBatchResult(
                records=[],
                rejections=RejectionReport(),
                file_errors=[str(exc)],
                file_error_sources=[],
                status="FAILED",
            )

    audit_events = [
        *build_rejection_audit_events(
            transaction_batch.rejections,
            run_id=config.run_id,
            pipeline_version=config.pipeline_version,
            dataset_version=config.dataset_version,
            source_file=None,
            processing_timestamp_utc=processing_timestamp_utc,
            event_type="transaction_rejected",
            status="REJECTED",
        ),
        *build_rejection_audit_events(
            fx_batch.rejections,
            run_id=config.run_id,
            pipeline_version=config.pipeline_version,
            dataset_version=config.dataset_version,
            source_file=None,
            processing_timestamp_utc=processing_timestamp_utc,
            event_type="fx_missing",
            status="REJECTED",
        ),
    ]

    for source_file, file_error in transaction_batch.file_error_sources:
        audit_events.append(
            build_file_failure_event(
                run_id=config.run_id,
                pipeline_version=config.pipeline_version,
                dataset_version=config.dataset_version,
                source_file=source_file,
                processing_timestamp_utc=processing_timestamp_utc,
                error_message=file_error,
                event_type="sink_failed",
            )
        )

    for source_file, file_error in fx_batch.file_error_sources:
        audit_events.append(
            build_file_failure_event(
                run_id=config.run_id,
                pipeline_version=config.pipeline_version,
                dataset_version=config.dataset_version,
                source_file=source_file,
                processing_timestamp_utc=processing_timestamp_utc,
                error_message=file_error,
                event_type="sink_failed",
            )
        )

    sink_writes = []
    if transaction_batch.status != "SKIPPED":
        sink_writes.append(_write_sink("transactions", sinks.transactions.write_transactions, transaction_batch.records))
    if usd_batch.status == "SUCCESS" or usd_batch.status == "DEGRADED":
        sink_writes.append(_write_sink("usd", sinks.usd.write_usd_transactions, usd_batch.records))
    if fx_batch.status == "SUCCESS":
        sink_writes.append(_write_sink("fx_rates", sinks.fx_rates.write_fx_rates, fx_batch.records))
    sink_writes.append(_write_sink("audit", sinks.audit.write_audit_events, audit_events))

    return IngestionRunResult(
        transaction_batch=transaction_batch,
        fx_batch=fx_batch,
        usd_batch=usd_batch,
        audit_events=audit_events,
        sink_writes=sink_writes,
    )
