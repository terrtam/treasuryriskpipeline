from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import date, datetime, time, timezone
from pathlib import Path

from src.config.db_config import load_database_config, load_elasticsearch_config
from .batch import IngestionBatchConfig, run_ingestion_batch
from .external_sinks import ElasticsearchSinkConfig, PostgresSinkConfig
from .sinks import IngestionSinks, create_postgres_ingestion_sinks


def _load_psycopg_connection_factory(dsn: str):
    try:
        import psycopg

        return lambda: psycopg.connect(dsn)
    except ImportError:
        try:
            import psycopg2

            return lambda: psycopg2.connect(dsn)
        except ImportError as exc:  # pragma: no cover - environment dependent
            raise RuntimeError("psycopg or psycopg2 is required for PostgreSQL writes") from exc


def build_production_sinks() -> IngestionSinks:
    db_config = load_database_config()
    es_config = load_elasticsearch_config()
    connection_factory = _load_psycopg_connection_factory(db_config.to_dsn())
    postgres_config = PostgresSinkConfig()
    return create_postgres_ingestion_sinks(
        connection_factory,
        es_config.base_url,
        postgres_config=postgres_config,
        elasticsearch_config=ElasticsearchSinkConfig(
            index_name=es_config.index_name,
            failure_log_path=es_config.failure_log_path,
        ),
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Treasury ingestion pipeline on real parquet inputs.")
    parser.add_argument("input_path", help="A parquet file or a directory containing transaction and FX parquet files")
    parser.add_argument("--run-id", help="Stable run identifier")
    parser.add_argument("--pipeline-version", default="1.0.0", help="Pipeline version string")
    parser.add_argument("--dataset-version", default="demo-2026-01-01-v1", help="Dataset version string")
    parser.add_argument("--business-date", help="Optional business date in YYYY-MM-DD used to select one day's inputs")
    parser.add_argument("--processing-timestamp-utc", help="Optional RFC3339 UTC timestamp")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    sinks = build_production_sinks()
    business_date = date.fromisoformat(args.business_date) if args.business_date else None
    processing_timestamp = datetime.fromisoformat(args.processing_timestamp_utc.replace("Z", "+00:00")) if args.processing_timestamp_utc else None
    if processing_timestamp is None and business_date is not None:
        processing_timestamp = datetime.combine(business_date, time(23, 59, 59), tzinfo=timezone.utc)
    run_id = args.run_id or (f"liquidity-{business_date:%Y%m%d}" if business_date is not None else f"liquidity-{datetime.now(timezone.utc):%Y%m%d}")
    config = IngestionBatchConfig(
        data_feeds_dir=Path(args.input_path),
        run_id=run_id,
        pipeline_version=args.pipeline_version,
        dataset_version=args.dataset_version,
        business_date=business_date,
        processing_timestamp_utc=processing_timestamp,
    )
    result = run_ingestion_batch(config, sinks=sinks)
    print(json.dumps(asdict(result), default=str, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
