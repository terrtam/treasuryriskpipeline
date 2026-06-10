# TreasuryPipeline

## 1. Project Overview

TreasuryPipeline is a Python data pipeline for treasury-style liquidity analytics. It generates deterministic Parquet inputs, validates transaction and FX feeds, converts transactions to USD, computes 30-day liquidity snapshots, and writes relational and search-oriented outputs.

The repository also includes audit-event generation for rejected rows, file errors, and snapshot writes.

## 2. Architecture Diagram

```text
                +-------------------+
                | Transaction Files |
                |    (Parquet)      |
                +---------+---------+
                          |
                          v
                +-----------------------------+
                | Input Validation +          |
                | Ingestion Layer             |
                +---------+-------------------+
                          |
                          v
                +-------------------+
                | Python Pipeline   |
                | FX + Liquidity    |
                +---------+---------+
                          |
          +---------------+---------------+
          |                               |
          v                               v
+-------------------+         +----------------------+
|   PostgreSQL      |         |   Elasticsearch      |
| Liquidity Reports |         | Audit Search        |
+-------------------+         +----------------------+
```

## 3. Technology Stack

| Component | Technology |
| --- | --- |
| Runtime | Python |
| Parquet I/O | PyArrow |
| Reporting Store | PostgreSQL |
| Audit/Search Store | Elasticsearch |
| Local Services | Docker Compose |

## 4. Input Data

### Transaction Ledger Feed

Files:

```text
daily_transactions_YYYYMMDD.parquet
```

Purpose: validated cash movement events used for USD conversion and liquidity aggregation.

| Column | Type |
| --- | --- |
| `transaction_id` | string |
| `timestamp` | ISO-8601 timestamp |
| `legal_entity_id` | string |
| `direction` | `INBOUND` or `OUTBOUND` |
| `currency` | string |
| `amount` | decimal |

### FX Reference Matrix

Files:

```text
fx_rates_YYYYMMDD.parquet
```

Purpose: daily FX lookup data keyed by date and currency.

| Column | Type |
| --- | --- |
| `date` | date |
| `base_currency` | string |
| `quote_currency` | string |
| `fx_rate` | decimal |

### Invalid Input Files

The generator can also emit invalid transaction files so rejection paths and audit events can be tested.

## 5. Output Data

### PostgreSQL

Tables created by `sql/schema.sql`:

| Table | Purpose |
| --- | --- |
| `treasury.transactions` | Raw validated transactions |
| `treasury.usd` | USD-normalized transactions |
| `treasury.fx_rates` | FX reference rows |
| `treasury.liquidity_snapshots` | 30-day liquidity snapshots |
| `treasury.audit_events` | Rejection, file-failure, and snapshot audit events |

The schema also defines the required enums, constraints, and indexes for these tables.

### Elasticsearch

| Index | Purpose |
| --- | --- |
| `treasury_audit_logs` | Audit events for search and compliance investigation |

Elasticsearch write failures are recorded to `logs/elasticsearch_audit_failures.jsonl`.

## 6. Repository Structure

```text
TreasuryPipeline/
|-- data_feeds/                   # Generated parquet inputs
|-- infra/docker/docker-compose.yml
|-- examples/elasticsearch_smoke_test.py
|-- sql/schema.sql
|-- src/
|   |-- config/                   # Environment and connection settings
|   |-- data_generation/          # Demo dataset generator
|   |-- fx/                       # FX normalization logic
|   |-- ingestion/                # Validation, audit events, and sinks
|   `-- liquidity/                # Rolling liquidity snapshots
|-- tests/                        # Unit and integration-style tests
|-- spec/                         # Architecture, component, and contract docs
`-- README.md
```

Key files:

| File | Role |
| --- | --- |
| `src/data_generation/cli.py` | Generates demo Parquet inputs |
| `src/ingestion/cli.py` | Runs the ingestion pipeline |
| `src/ingestion/batch.py` | Orchestrates validation, conversion, and sink writes |
| `src/ingestion/external_sinks.py` | PostgreSQL and Elasticsearch sink implementations |
| `src/fx/conversion.py` | Converts validated transactions to USD |
| `src/liquidity/window.py` | Computes 30-day liquidity snapshots |
| `sql/schema.sql` | Creates schema, enums, tables, and indexes |

## 7. Running the Project

1. Install Python dependencies.

```bash
pip install -r requirements.txt
```

2. Start PostgreSQL and Elasticsearch.

Use the Docker Compose file in `infra/docker/docker-compose.yml`, or connect to existing services.

3. Create the database schema.

```sql
\i sql/schema.sql
```

4. Generate demo inputs.

```bash
python -m src.data_generation
```

5. Run ingestion.

```bash
python -m src.ingestion data_feeds --run-id ("daily-" + (Get-Date -Format yyyyMMdd)) --pipeline-version 1.0.0 --dataset-version demo-2026-01-01-v1
```

For a daily scheduled run, let the pipeline use the current UTC timestamp by default:

```powershell
python -m src.ingestion data_feeds --run-id ("daily-" + (Get-Date -Format yyyyMMdd)) --pipeline-version 1.0.0 --dataset-version demo-2026-01-01-v1
```

For a backfill rerun, point the command at a folder that contains only the historical day you want to replay, or pass a single Parquet file:

```powershell
python -m src.ingestion .\backfill_20260607 --run-id daily-20260607-rerun1 --pipeline-version 1.0.0 --dataset-version demo-2026-01-01-v1
```

```powershell
python -m src.ingestion .\backfill_20260607\daily_transactions_20260607.parquet --run-id daily-20260607-rerun1 --pipeline-version 1.0.0 --dataset-version demo-2026-01-01-v1
```

The pipeline reads PostgreSQL and Elasticsearch settings from environment variables or a local `.env` file.

## 8. Key Engineering Decisions

* Date-keyed FX lookup with explicit validation that USD identity rates exist and equal `1.0`.
* 30-day trailing liquidity snapshots computed from USD-normalized transactions.
* Decimal-backed monetary values and `NUMERIC` database columns to avoid floating-point drift.
* Deterministic audit event IDs for rejected rows, file failures, and snapshot writes.
* Audit writes run after the core transaction, USD, and liquidity writes. PostgreSQL output is not rolled back if Elasticsearch fails, and Elasticsearch failures are written to a local JSONL log.

The current implementation uses an in-memory keyed FX lookup instead of a distributed join.

## 9. Skills Demonstrated

* Data pipeline design
* Parquet ingestion and validation
* FX normalization
* Rolling window aggregation
* Relational schema design
* Audit-event modeling
* Search indexing and failure logging
* Deterministic testable data generation
