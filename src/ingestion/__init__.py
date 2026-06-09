"""Ingestion validation utilities for canonical transaction and FX records."""

from .audit import AuditEvent, build_file_failure_event, build_rejection_audit_events
from .batch import IngestionBatchConfig, IngestionRunResult, SinkWriteResult, run_ingestion_batch
from .rejections import Rejection, RejectionReport
from .sinks import (
    AuditEventSink,
    FXRecordSink,
    IngestionSinks,
    InMemoryAuditSink,
    InMemoryFXSink,
    InMemoryTransactionSink,
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
    "IngestionBatchConfig",
    "IngestionRunResult",
    "SinkWriteResult",
    "AuditEvent",
    "AuditEventSink",
    "FXRecordSink",
    "Rejection",
    "RejectionReport",
    "IngestionSinks",
    "InMemoryAuditSink",
    "InMemoryFXSink",
    "InMemoryTransactionSink",
    "ValidatedFXRate",
    "ValidatedTransaction",
    "TransactionRecordSink",
    "build_file_failure_event",
    "build_rejection_audit_events",
    "create_default_ingestion_sinks",
    "run_ingestion_batch",
    "ingest_fx_files",
    "ingest_transaction_files",
    "discover_ingestion_files",
    "load_parquet_rows",
    "validate_fx_batch",
    "validate_transaction_batch",
]
