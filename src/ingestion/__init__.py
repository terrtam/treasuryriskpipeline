"""Ingestion validation utilities for canonical transaction and FX records."""

from .audit import AuditEvent, build_file_failure_event, build_rejection_audit_events, build_snapshot_written_audit_events
from .rejections import Rejection, RejectionReport
from .sinks import (
    AuditEventSink,
    FXRecordSink,
    IngestionSinks,
    InMemoryAuditSink,
    InMemoryFXSink,
    InMemoryLiquiditySink,
    InMemoryUSDSink,
    InMemoryTransactionSink,
    LiquiditySnapshotSink,
    USDRecordSink,
    TransactionRecordSink,
    create_default_ingestion_sinks,
)
from .validator import (
    IngestionBatchResult,
    ValidatedFXRate,
    ValidatedTransaction,
    ingest_fx_files,
    ingest_transaction_files,
    discover_ingestion_files,
    load_parquet_rows,
    validate_fx_batch,
    validate_transaction_batch,
)

__all__ = [
    "IngestionBatchResult",
    "AuditEvent",
    "AuditEventSink",
    "FXRecordSink",
    "Rejection",
    "RejectionReport",
    "IngestionSinks",
    "InMemoryAuditSink",
    "InMemoryFXSink",
    "InMemoryLiquiditySink",
    "InMemoryUSDSink",
    "InMemoryTransactionSink",
    "LiquiditySnapshotSink",
    "ValidatedFXRate",
    "ValidatedTransaction",
    "USDRecordSink",
    "TransactionRecordSink",
    "build_file_failure_event",
    "build_rejection_audit_events",
    "build_snapshot_written_audit_events",
    "create_default_ingestion_sinks",
    "ingest_fx_files",
    "ingest_transaction_files",
    "discover_ingestion_files",
    "load_parquet_rows",
    "validate_fx_batch",
    "validate_transaction_batch",
]
