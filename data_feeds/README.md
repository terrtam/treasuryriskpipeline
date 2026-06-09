# Ingestion Fixture Data

This folder contains sample parquet inputs for the ingestion pipeline.

Files:

- `daily_transactions_001.parquet`
- `fx_rates_001.parquet`

The transaction file includes 5 rows total:

- 4 valid rows that should load into PostgreSQL
- 1 invalid row that should be rejected and produce an audit event

The FX file includes valid USD-based FX reference rows so the full ingestion batch can run.
