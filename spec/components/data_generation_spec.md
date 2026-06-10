# Data Generation Component Specification

## Purpose
The data generation module creates deterministic synthetic transaction and FX datasets for local or distributed execution.

It exists to replace external banking datasets with reproducible, versioned inputs that exercise the full pipeline end to end.

## Outputs
The module produces three Parquet dataset families under `data_feeds/`, one file per business day in the configured date window.

### Transaction Dataset
| Field | Type | Requirements |
|---|---|---|
| `transaction_id` | string | Unique within the generated dataset. |
| `timestamp` | timestamp | UTC event time. |
| `legal_entity_id` | string | Required. |
| `currency` | string | Uppercase ISO 4217 code. |
| `amount` | decimal | Non-negative. |
| `direction` | string | Must be `INBOUND` or `OUTBOUND`. |

### FX Dataset
| Field | Type | Requirements |
|---|---|---|
| `date` | date | UTC business date. |
| `base_currency` | string | Must be `USD` for v1. |
| `quote_currency` | string | Uppercase ISO 4217 code. |
| `fx_rate` | decimal | Positive. |

## Rules
- Generation must be deterministic for the same seed, generator version, and configuration.
- Transaction and FX generation must be independent of external APIs or live market sources.
- Currency coverage and entity activity must be realistic enough to exercise distributed processing behavior.
- Output files must be date-keyed and emitted only for business days in scope.
- FX output must include explicit USD/USD = 1.0 rows for every business day in scope.
- The generator must publish outputs atomically so partially written files are never treated as valid inputs.
- The generator must not depend on Spark jobs for correctness.

## Data Volume Assumptions
| Dataset | Volume |
|---|---:|
| Transactions | 500,000 records |
| Legal Entities | 25 |
| Currencies | 20 |
| Activity Window | 90 days |
| FX Rates | Business-day rates |

## File Naming
- Transaction files follow `daily_transactions_YYYYMMDD.parquet`.
- Invalid transaction files follow `daily_transactions_errors_YYYYMMDD.parquet`.
- FX files follow `fx_rates_YYYYMMDD.parquet`.

## Failure Behavior
- If generation is rerun with the same seed and configuration, the logical dataset must remain identical.
- If output publication is interrupted, the partial result must not be treated as valid input.
- If the generator configuration is invalid, generation must fail rather than producing approximate data.

## Relationship to the Pipeline
- The ingestion layer consumes the generated Parquet artifacts.
- Spark performs FX conversion and liquidity aggregation after generation.
- PostgreSQL and Elasticsearch consume only downstream processed outputs, not generator internals.
