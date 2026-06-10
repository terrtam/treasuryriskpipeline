# Ingestion Fixture Data

This folder is the landing zone for generated parquet inputs consumed by the ingestion pipeline.

Generated artifacts:

- `daily_transactions_YYYYMMDD.parquet`
- `daily_transactions_errors_YYYYMMDD.parquet`
- `fx_rates_YYYYMMDD.parquet`

The parquet files are ignored by git so local demo datasets can be regenerated freely.

To create a richer liquidity test set with multiple business days, currencies, and legal entities:

```powershell
python -m src.data_generation --output-dir data_feeds --seed 42 --start-date 2026-01-01 --days 90 --transactions 1500 --entities 25 --currencies 20 --transaction-error-rows 8
```

The generated dataset is designed to exercise:

- multi-entity aggregation
- multi-currency FX normalization
- business-day Parquet publication
- trailing 30-day rolling liquidity windows
- deterministic reruns with the same seed and configuration
- deterministic rejection coverage from invalid transaction rows
