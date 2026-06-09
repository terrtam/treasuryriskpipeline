from __future__ import annotations

from dataclasses import replace
from datetime import date

from src.data_generation import DataGenerationConfig, generate_demo_datasets
from src.ingestion import InMemoryAuditSink, InMemoryFXSink, InMemoryLiquiditySink, InMemoryUSDSink, InMemoryTransactionSink, IngestionSinks, ingest_fx_files, ingest_transaction_files
from src.ingestion.batch import IngestionBatchConfig, run_ingestion_batch


def test_generate_demo_datasets_is_reproducible(tmp_path):
    config = DataGenerationConfig(
        output_dir=tmp_path / "one",
        seed=7,
        start_date=date(2026, 1, 1),
        days=45,
        transaction_count=120,
        entity_count=6,
        currency_count=5,
        transaction_files=2,
        fx_files=2,
        dataset_version="demo-test",
    )

    first = generate_demo_datasets(config)
    second = generate_demo_datasets(replace(config, output_dir=tmp_path / "two"))

    assert first.transaction_count == second.transaction_count == 120
    assert first.fx_row_count == second.fx_row_count == 225
    assert [path.name for path in first.transaction_files] == ["daily_transactions_001.parquet", "daily_transactions_002.parquet"]
    assert [path.name for path in first.fx_files] == ["fx_rates_001.parquet", "fx_rates_002.parquet"]

    first_txn = ingest_transaction_files(first.output_dir)
    second_txn = ingest_transaction_files(second.output_dir)
    first_fx = ingest_fx_files(first.output_dir)
    second_fx = ingest_fx_files(second.output_dir)

    assert first_txn.records == second_txn.records
    assert first_fx.records == second_fx.records


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
            transaction_files=2,
            fx_files=2,
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
