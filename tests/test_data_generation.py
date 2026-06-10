from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta

from src.data_generation import DataGenerationConfig, generate_demo_datasets
from src.ingestion import InMemoryAuditSink, InMemoryFXSink, InMemoryLiquiditySink, InMemoryUSDSink, InMemoryTransactionSink, IngestionSinks, ingest_fx_files, ingest_transaction_files
from src.ingestion import load_parquet_rows
from src.ingestion.batch import IngestionBatchConfig, run_ingestion_batch


def _business_days(start_date: date, days: int) -> list[date]:
    return [
        start_date + timedelta(days=offset)
        for offset in range(days)
        if (start_date + timedelta(days=offset)).weekday() < 5
    ]


def _date_from_filename(path_name: str) -> date:
    return datetime.strptime(path_name.rsplit("_", 1)[-1].removesuffix(".parquet"), "%Y%m%d").date()


def test_generate_demo_datasets_is_reproducible(tmp_path):
    config = DataGenerationConfig(
        output_dir=tmp_path / "one",
        seed=7,
        start_date=date(2026, 1, 1),
        days=45,
        transaction_count=120,
        entity_count=6,
        currency_count=5,
        dataset_version="demo-test",
    )

    first = generate_demo_datasets(config)
    second = generate_demo_datasets(replace(config, output_dir=tmp_path / "two"))
    business_days = _business_days(config.start_date, config.days)

    assert first.transaction_count == second.transaction_count == 120
    assert first.fx_row_count == second.fx_row_count == len(business_days) * 5
    assert first.transaction_error_count == second.transaction_error_count
    assert [path.name for path in first.transaction_files] == [f"daily_transactions_{day:%Y%m%d}.parquet" for day in business_days]
    assert [path.name for path in first.fx_files] == [f"fx_rates_{day:%Y%m%d}.parquet" for day in business_days]
    assert [path.name for path in first.transaction_error_files] == [f"daily_transactions_errors_{day:%Y%m%d}.parquet" for day in business_days]

    assert [path.name for path in first.transaction_files] == [path.name for path in second.transaction_files]
    assert [path.name for path in first.fx_files] == [path.name for path in second.fx_files]
    assert [path.name for path in first.transaction_error_files] == [path.name for path in second.transaction_error_files]

    first_txn = ingest_transaction_files(first.output_dir)
    second_txn = ingest_transaction_files(second.output_dir)
    first_fx = ingest_fx_files(first.output_dir)
    second_fx = ingest_fx_files(second.output_dir)

    assert first_txn.records == second_txn.records
    assert first_fx.records == second_fx.records

    for first_path, second_path in zip(first.transaction_files, second.transaction_files):
        assert load_parquet_rows(first_path) == load_parquet_rows(second_path)

    for first_path, second_path in zip(first.fx_files, second.fx_files):
        assert load_parquet_rows(first_path) == load_parquet_rows(second_path)

    for first_path, second_path in zip(first.transaction_error_files, second.transaction_error_files):
        assert load_parquet_rows(first_path) == load_parquet_rows(second_path)


def test_generate_demo_datasets_uses_business_day_files_and_rows(tmp_path):
    manifest = generate_demo_datasets(
        DataGenerationConfig(
            output_dir=tmp_path,
            seed=11,
            start_date=date(2026, 5, 1),
            days=5,
            transaction_count=20,
            entity_count=4,
            currency_count=4,
            transaction_error_rows=4,
            dataset_version="demo-business-days",
        )
    )

    expected_days = _business_days(date(2026, 5, 1), 5)

    assert [path.name for path in manifest.transaction_files] == [f"daily_transactions_{day:%Y%m%d}.parquet" for day in expected_days]
    assert [path.name for path in manifest.transaction_error_files] == [f"daily_transactions_errors_{day:%Y%m%d}.parquet" for day in expected_days]
    assert [path.name for path in manifest.fx_files] == [f"fx_rates_{day:%Y%m%d}.parquet" for day in expected_days]

    for path in manifest.transaction_files:
        file_date = _date_from_filename(path.name)
        rows = load_parquet_rows(path)
        assert file_date.weekday() < 5
        assert all(row["timestamp"].date() == file_date for row in rows)

    for path in manifest.fx_files:
        file_date = _date_from_filename(path.name)
        rows = load_parquet_rows(path)
        assert file_date.weekday() < 5
        assert all(row["date"] == file_date for row in rows)

    for path in manifest.transaction_error_files:
        file_date = _date_from_filename(path.name)
        rows = load_parquet_rows(path)
        assert file_date.weekday() < 5
        assert all(row["timestamp"] is None or row["timestamp"].date() == file_date for row in rows)


def test_generated_inputs_drive_liquidity_snapshots(tmp_path):
    manifest = generate_demo_datasets(
        DataGenerationConfig(
            output_dir=tmp_path,
            seed=11,
            start_date=date(2026, 5, 1),
            days=40,
            transaction_count=160,
            entity_count=4,
            currency_count=6,
            transaction_error_rows=8,
            dataset_version="demo-liquidity",
        )
    )

    sinks = IngestionSinks(
        transactions=InMemoryTransactionSink(),
        usd=InMemoryUSDSink(),
        liquidity=InMemoryLiquiditySink(),
        fx_rates=InMemoryFXSink(),
        audit=InMemoryAuditSink(),
    )
    result = run_ingestion_batch(
        IngestionBatchConfig(
            data_feeds_dir=manifest.output_dir,
            run_id="run-001",
            pipeline_version="1.0.0",
            dataset_version="demo-liquidity",
        ),
        sinks=sinks,
    )

    assert result.usd_batch.status in {"SUCCESS", "DEGRADED"}
    assert result.liquidity_batch.status == "SUCCESS"
    assert len(sinks.liquidity.records) > 0
    assert any(event.event_type == "snapshot_written" for event in result.audit_events)
