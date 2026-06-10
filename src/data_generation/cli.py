from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from .generator import DataGenerationConfig, generate_demo_datasets


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate deterministic Treasury demo parquet inputs.")
    parser.add_argument(
        "--output-dir",
        default="data_feeds",
        help="Directory where date-keyed parquet files will be written",
    )
    parser.add_argument("--seed", type=int, default=42, help="Deterministic random seed")
    parser.add_argument("--start-date", default="2026-01-01", help="Dataset start date in YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=90, help="Number of calendar days to generate")
    parser.add_argument("--transactions", type=int, default=1500, help="Total transaction rows to generate")
    parser.add_argument("--entities", type=int, default=25, help="Number of legal entities to synthesize")
    parser.add_argument("--currencies", type=int, default=20, help="Number of currencies to synthesize")
    parser.add_argument("--transaction-error-rows", type=int, default=8, help="Number of invalid transaction rows to generate")
    parser.add_argument("--dataset-version", default="demo-2026-01-01-v1", help="Dataset version label")
    parser.add_argument("--generator-version", default="1.0.0", help="Generator version label")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    config = DataGenerationConfig(
        output_dir=Path(args.output_dir),
        seed=args.seed,
        start_date=date.fromisoformat(args.start_date),
        days=args.days,
        transaction_count=args.transactions,
        entity_count=args.entities,
        currency_count=args.currencies,
        transaction_error_rows=args.transaction_error_rows,
        generator_version=args.generator_version,
        dataset_version=args.dataset_version,
    )
    manifest = generate_demo_datasets(config)
    print(f"wrote {manifest.transaction_count} transaction rows to {len(manifest.transaction_files)} business-day files")
    print(f"wrote {manifest.transaction_error_count} invalid transaction rows to {len(manifest.transaction_error_files)} business-day files")
    print(f"wrote {manifest.fx_row_count} fx rows to {len(manifest.fx_files)} business-day files")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
