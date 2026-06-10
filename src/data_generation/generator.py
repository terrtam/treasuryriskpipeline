from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
import math
import random
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq


TRANSACTION_SCHEMA = pa.schema(
    [
        ("transaction_id", pa.string()),
        ("timestamp", pa.timestamp("us", tz="UTC")),
        ("legal_entity_id", pa.string()),
        ("currency", pa.string()),
        ("amount", pa.decimal128(20, 6)),
        ("direction", pa.string()),
    ]
)

FX_SCHEMA = pa.schema(
    [
        ("date", pa.date32()),
        ("base_currency", pa.string()),
        ("quote_currency", pa.string()),
        ("fx_rate", pa.decimal128(20, 10)),
    ]
)

DEFAULT_CURRENCIES = [
    "USD",
    "EUR",
    "GBP",
    "CAD",
    "AUD",
    "NZD",
    "JPY",
    "CHF",
    "SEK",
    "NOK",
    "DKK",
    "PLN",
    "CZK",
    "HUF",
    "MXN",
    "BRL",
    "ZAR",
    "SGD",
    "HKD",
    "INR",
]


@dataclass(frozen=True)
class DataGenerationConfig:
    output_dir: str | Path
    seed: int = 42
    start_date: date = date(2026, 1, 1)
    days: int = 90
    transaction_count: int = 1500
    entity_count: int = 25
    currency_count: int = 20
    transaction_files: int = 3
    fx_files: int = 3
    transaction_error_files: int = 1
    transaction_error_rows: int = 8
    generator_version: str = "1.0.0"
    dataset_version: str = "demo-2026-01-01-v1"


@dataclass(frozen=True)
class GeneratedDatasetManifest:
    output_dir: Path
    transaction_files: list[Path]
    transaction_error_files: list[Path]
    fx_files: list[Path]
    transaction_count: int
    transaction_error_count: int
    fx_row_count: int


def _validate_config(config: DataGenerationConfig) -> None:
    if config.days <= 0:
        raise ValueError("days must be positive")
    if config.transaction_count <= 0:
        raise ValueError("transaction_count must be positive")
    if config.entity_count <= 0:
        raise ValueError("entity_count must be positive")
    if not 1 <= config.currency_count <= len(DEFAULT_CURRENCIES):
        raise ValueError("currency_count must be between 1 and 20")
    if config.transaction_files <= 0:
        raise ValueError("transaction_files must be positive")
    if config.fx_files <= 0:
        raise ValueError("fx_files must be positive")
    if config.transaction_error_files <= 0:
        raise ValueError("transaction_error_files must be positive")
    if config.transaction_error_rows <= 0:
        raise ValueError("transaction_error_rows must be positive")


def _currency_universe(currency_count: int) -> list[str]:
    return DEFAULT_CURRENCIES[:currency_count]


def _entity_ids(entity_count: int) -> list[str]:
    return [f"LE{i:03d}" for i in range(1, entity_count + 1)]


def _make_timestamp(day: date, rng: random.Random) -> datetime:
    return datetime.combine(
        day,
        time(
            hour=rng.randint(0, 23),
            minute=rng.randint(0, 59),
            second=rng.randint(0, 59),
            tzinfo=timezone.utc,
        ),
    )


def _weighted_choices(values: list[str], weights: list[int], rng: random.Random) -> str:
    return rng.choices(values, weights=weights, k=1)[0]


def _quantize_decimal(value: Decimal, scale: str) -> Decimal:
    return value.quantize(Decimal(scale))


def _transaction_rows(config: DataGenerationConfig) -> list[dict[str, object]]:
    rng = random.Random(config.seed)
    start_date = config.start_date
    currencies = _currency_universe(config.currency_count)
    entities = _entity_ids(config.entity_count)

    day_weights = [8 if (index % 7) < 5 else 3 for index in range(config.days)]
    entity_weights = [25 - min(index, 20) if index < 5 else 5 for index in range(len(entities))]
    currency_weights = [12 if currency == "USD" else max(2, 20 - idx) for idx, currency in enumerate(currencies)]
    direction_weights = [58, 42]

    rows: list[dict[str, object]] = []
    for row_index in range(config.transaction_count):
        day_index = rng.choices(range(config.days), weights=day_weights, k=1)[0]
        transaction_day = start_date + timedelta(days=day_index)
        timestamp = _make_timestamp(transaction_day, rng)
        legal_entity_id = _weighted_choices(entities, entity_weights, rng)
        currency = _weighted_choices(currencies, currency_weights, rng)
        direction = _weighted_choices(["INBOUND", "OUTBOUND"], direction_weights, rng)

        base_cents = rng.randint(5_000, 5_000_000)
        amount = (Decimal(base_cents) / Decimal("100")).quantize(Decimal("0.000001"))

        rows.append(
            {
                "transaction_id": f"TX-{timestamp:%Y%m%d}-{row_index:07d}",
                "timestamp": timestamp,
                "legal_entity_id": legal_entity_id,
                "currency": currency,
                "amount": amount,
                "direction": direction,
            }
        )

    rows.sort(key=lambda row: (row["timestamp"], row["legal_entity_id"], row["transaction_id"]))
    return rows


def _transaction_error_rows(
    config: DataGenerationConfig,
    transaction_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    if not transaction_rows:
        return []

    reference_row = transaction_rows[0]
    rng = random.Random(config.seed + 19)
    error_rows: list[dict[str, object]] = []
    base_timestamp = reference_row["timestamp"]
    base_currency = reference_row["currency"]
    base_entity = reference_row["legal_entity_id"]
    base_amount = reference_row["amount"]
    base_direction = reference_row["direction"]

    templates = [
        {
            "transaction_id": reference_row["transaction_id"],
            "timestamp": base_timestamp,
            "legal_entity_id": base_entity,
            "currency": base_currency,
            "amount": base_amount,
            "direction": "SIDEWAYS",
        },
        {
            "transaction_id": None,
            "timestamp": base_timestamp,
            "legal_entity_id": base_entity,
            "currency": base_currency,
            "amount": base_amount,
            "direction": base_direction,
        },
        {
            "transaction_id": f"TX-ERR-{config.seed:04d}-03",
            "timestamp": None,
            "legal_entity_id": base_entity,
            "currency": "US",
            "amount": base_amount,
            "direction": base_direction,
        },
        {
            "transaction_id": f"TX-ERR-{config.seed:04d}-04",
            "timestamp": base_timestamp,
            "legal_entity_id": None,
            "currency": base_currency,
            "amount": -abs(base_amount),
            "direction": base_direction,
        },
        {
            "transaction_id": reference_row["transaction_id"],
            "timestamp": base_timestamp + timedelta(minutes=1),
            "legal_entity_id": base_entity,
            "currency": base_currency,
            "amount": base_amount,
            "direction": base_direction,
        },
        {
            "transaction_id": f"TX-ERR-{config.seed:04d}-06",
            "timestamp": base_timestamp + timedelta(minutes=2),
            "legal_entity_id": base_entity,
            "currency": base_currency,
            "amount": base_amount,
            "direction": "IN",
        },
        {
            "transaction_id": f"TX-ERR-{config.seed:04d}-07",
            "timestamp": base_timestamp + timedelta(minutes=3),
            "legal_entity_id": "",
            "currency": base_currency,
            "amount": base_amount,
            "direction": base_direction,
        },
        {
            "transaction_id": f"TX-ERR-{config.seed:04d}-08",
            "timestamp": base_timestamp + timedelta(minutes=4),
            "legal_entity_id": base_entity,
            "currency": "usd",
            "amount": Decimal("0.000000"),
            "direction": base_direction,
        },
    ]

    for index in range(config.transaction_error_rows):
        template = dict(templates[index % len(templates)])
        if template["transaction_id"] is not None and index >= len(templates):
            template["transaction_id"] = f"{template['transaction_id']}-{rng.randint(100, 999)}"
        error_rows.append(template)

    error_rows.sort(key=lambda row: (row["timestamp"] is None, row["timestamp"], str(row["transaction_id"])))
    return error_rows


def _fx_rows(config: DataGenerationConfig) -> list[dict[str, object]]:
    currencies = _currency_universe(config.currency_count)
    rows: list[dict[str, object]] = []

    for day_index in range(config.days):
        current_day = config.start_date + timedelta(days=day_index)
        for currency_index, currency in enumerate(currencies):
            if currency == "USD":
                rate = Decimal("1.0000000000")
            else:
                currency_rng = random.Random(config.seed * 10_000 + currency_index * 1_000 + day_index)
                base_rate = Decimal(currency_rng.randint(6_000, 24_000)) / Decimal("10000")
                daily_drift = Decimal(0)
                for _ in range(day_index + 1):
                    daily_drift += Decimal(currency_rng.randint(-180, 180)) / Decimal("10000")
                rate = (base_rate + daily_drift).quantize(Decimal("0.0000000001"))
                if rate <= Decimal("0.1000"):
                    rate = Decimal("0.1000")

            rows.append(
                {
                    "date": current_day,
                    "base_currency": "USD",
                    "quote_currency": currency,
                    "fx_rate": rate,
                }
            )

    rows.sort(key=lambda row: (row["date"], row["quote_currency"]))
    return rows


def _atomic_write_parquet(rows: list[dict[str, object]], schema: pa.Schema, target_path: Path) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target_path.with_name(f"{target_path.stem}.{uuid4().hex}.tmp")
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, temp_path)
    temp_path.replace(target_path)


def _split_rows(rows: list[dict[str, object]], file_count: int) -> list[list[dict[str, object]]]:
    if not rows:
        return [[] for _ in range(file_count)]
    chunk_size = math.ceil(len(rows) / file_count)
    return [rows[index : index + chunk_size] for index in range(0, len(rows), chunk_size)]


def generate_demo_datasets(config: DataGenerationConfig) -> GeneratedDatasetManifest:
    _validate_config(config)
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    transaction_rows = _transaction_rows(config)
    transaction_error_rows = _transaction_error_rows(config, transaction_rows)
    fx_rows = _fx_rows(config)

    transaction_files: list[Path] = []
    for index, rows in enumerate(_split_rows(transaction_rows, config.transaction_files), start=1):
        path = output_dir / f"daily_transactions_{index:03d}.parquet"
        _atomic_write_parquet(rows, TRANSACTION_SCHEMA, path)
        transaction_files.append(path)

    transaction_error_files: list[Path] = []
    for index, rows in enumerate(_split_rows(transaction_error_rows, config.transaction_error_files), start=1):
        path = output_dir / f"daily_transactions_errors_{index:03d}.parquet"
        _atomic_write_parquet(rows, TRANSACTION_SCHEMA, path)
        transaction_error_files.append(path)

    fx_files: list[Path] = []
    for index, rows in enumerate(_split_rows(fx_rows, config.fx_files), start=1):
        path = output_dir / f"fx_rates_{index:03d}.parquet"
        _atomic_write_parquet(rows, FX_SCHEMA, path)
        fx_files.append(path)

    return GeneratedDatasetManifest(
        output_dir=output_dir,
        transaction_files=transaction_files,
        transaction_error_files=transaction_error_files,
        fx_files=fx_files,
        transaction_count=len(transaction_rows),
        transaction_error_count=len(transaction_error_rows),
        fx_row_count=len(fx_rows),
    )
