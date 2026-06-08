"""Ingestion validation utilities for canonical transaction and FX records."""

from .rejections import Rejection, RejectionReport
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
    "Rejection",
    "RejectionReport",
    "ValidatedFXRate",
    "ValidatedTransaction",
    "ingest_fx_files",
    "ingest_transaction_files",
    "discover_ingestion_files",
    "load_parquet_rows",
    "validate_fx_batch",
    "validate_transaction_batch",
]
