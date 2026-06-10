# Ingestion Fixture Data

This folder is the landing zone for generated parquet inputs consumed by the ingestion pipeline.

Generated artifacts:

- `daily_transactions_*.parquet`
- `daily_transactions_errors_*.parquet`
- `fx_rates_*.parquet`

The parquet files are ignored by git so local demo datasets can be regenerated freely.

To create a richer liquidity test set with multiple dates, currencies, and legal entities:

```powershell
python -m src.data_generation --output-dir data_feeds --seed 42 --start-date 2026-01-01 --days 90 --transactions 1500 --entities 25 --currencies 20 --transaction-files 3 --fx-files 3 --transaction-error-files 1 --transaction-error-rows 8
```

The generated dataset is designed to exercise:

- multi-entity aggregation
- multi-currency FX normalization
- trailing 30-day rolling liquidity windows
- deterministic reruns with the same seed and configuration
- deterministic rejection coverage from invalid transaction rows
