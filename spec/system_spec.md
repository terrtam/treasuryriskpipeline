# TreasuryPipeline System Specification

## Assumptions Used in This Spec
- All timestamps are interpreted in UTC.
- All monetary values use fixed-point decimal arithmetic only.
- Rolling liquidity is computed as a trailing 30 calendar-day window, inclusive of the snapshot date.
- The system produces daily snapshot outputs per legal entity.
- The project is intentionally minimal: synthetic data generation, Spark ingestion, FX normalization, liquidity aggregation, PostgreSQL reporting, and Elasticsearch audit output.

## 1. System Overview
TreasuryPipeline is a distributed treasury monitoring engine that simulates a global bank environment across multiple legal entities and currencies.

It has three core responsibilities:
- Generate synthetic treasury transaction data and FX reference data in a deterministic, versioned way.
- Ingest Parquet-based transaction and FX datasets, normalize all transaction values into USD, and compute rolling 30-day liquidity by legal entity.
- Persist reporting outputs to PostgreSQL and compliance-grade audit logs to Elasticsearch.

Synthetic data generation is a first-class subsystem. It replaces external banking datasets so the full pipeline can be executed locally or in a cluster without privacy, access, or dependency concerns.

End-to-end lifecycle:
- Synthetic generator creates transaction and FX datasets.
- Datasets are written to `data_feeds/` as Parquet artifacts.
- Spark-based processing ingests the generated files.
- Every transaction is converted to USD before any aggregation.
- Rolling 30-day liquidity snapshots are computed per legal entity.
- Aggregated snapshots are written to PostgreSQL.
- Full audit events are written to Elasticsearch without allowing Elasticsearch failures to break the main processing flow.

## 2. Canonical Data Contracts

### 2.1 Transaction Dataset
Exact schema:

| Field | Type | Requirements |
|---|---|---|
| `transaction_id` | string | Unique within the generated dataset. Stable across regeneration with the same seed and version. |
| `timestamp` | timestamp | Event time in UTC. Must be parseable deterministically and represent the actual transaction event time. |
| `legal_entity_id` | string | Synthetic legal entity identifier. |
| `currency` | string | ISO 4217 currency code. Must be uppercase. |
| `amount` | decimal | Non-negative nominal amount in the transaction currency. High precision required. |
| `direction` | string | One of `INBOUND` or `OUTBOUND`. |

Transaction semantics:
- `amount` is always stored as an absolute nominal value.
- The economic sign is derived from `direction`, not embedded in the amount.
- `timestamp` is the authoritative event time.
- `transaction_id` must be stable and reproducible for a given seed, configuration, and generator version.

### 2.2 FX Rates Dataset
Exact schema:

| Field | Type | Requirements |
|---|---|---|
| `date` | date | UTC calendar date of the FX rate observation. |
| `base_currency` | string | Must be `USD` for v1. |
| `quote_currency` | string | ISO 4217 currency code. |
| `fx_rate` | decimal | High precision fixed-point rate. Must be positive. |

FX semantics:
- `fx_rate` expresses the USD value of one unit of `quote_currency`.
- For USD itself, the generator must emit explicit USD/USD = 1.0 rows for every date.
- Rates are daily reference rates, not intraday ticks.

## 3. Data Generation System

### 3.1 Purpose
The synthetic data generation subsystem exists to:
- Provide self-contained reproducibility with no external dataset dependency.
- Simulate realistic treasury cash flows across entities, currencies, and transaction directions.
- Validate distributed processing behavior under realistic data volume and entity skew.
- Preserve deterministic outputs for testing, regression checks, and repeatable demos.

### 3.2 Generation Rules
The synthetic generator must obey these rules:

- Transaction timestamps must be distributed across the configured historical window using a deterministic, controlled distribution.
- Currency selection must be weighted. Uniform currency selection is allowed only if explicitly configured.
- Legal entity activity must vary. The population should include active entities and low-activity entities so the dataset exercises skewed workloads.
- INBOUND and OUTBOUND should be broadly balanced, but not perfectly symmetric.
- FX rates must evolve smoothly over time with bounded day-to-day movement unless a volatility override is explicitly configured.
- All random choices must be seeded.
- The same seed, generator version, schema version, and configuration must produce the same logical dataset.
- Transaction and FX generation must be independent of any external API or live market source.

### 3.3 Data Volume Assumptions

| Dataset | Volume |
|---|---:|
| Transactions | 500,000 records |
| Legal Entities | 25 |
| Currencies | 20 |
| Activity Window | 90 days |
| FX Rates | Daily rates |

These are demo-scale values intended to fit local execution while still demonstrating distributed processing behavior.

## 4. Data Inputs (Pipeline Consumption Layer)

### Transaction Parquet Ingestion Rules
- The pipeline must ingest all matching transaction Parquet artifacts under `data_feeds/`.
- Transaction input must conform exactly to the transaction schema defined above.
- Required fields must be non-null.
- Currency codes must be uppercase ISO codes.
- `direction` must be restricted to `INBOUND` and `OUTBOUND`.
- `amount` must be non-negative.
- `transaction_id` must be unique within the combined input set.

### FX Rate Ingestion Rules
- The pipeline must ingest all matching FX Parquet artifacts under `data_feeds/`.
- FX input must conform exactly to the FX schema defined above.
- A USD/USD rate of exactly 1.0 must exist for every date in scope.
- FX rates must be positive.
- Duplicate `(date, base_currency, quote_currency)` combinations are invalid.

### Event-Time Handling Rules
- Transaction `timestamp` is the source of truth for all time-based logic.
- All timestamps are interpreted in UTC.
- FX matching is done by transaction event date in UTC.
- Rolling liquidity windows are computed in event time, not processing time.
- Late or out-of-order file arrival must not change the logical result as long as the input set is identical.

## 5. Data Outputs

### PostgreSQL Table Schema
The reporting output is a liquidity snapshot table with one row per legal entity per snapshot date.

| Field | Type | Requirements |
|---|---|---|
| `snapshot_date` | date | UTC snapshot date. |
| `legal_entity_id` | string | Legal entity key. |
| `window_start_utc` | timestamp | Inclusive start of the trailing 30-day window. |
| `window_end_utc` | timestamp | Inclusive end of the trailing 30-day window. |
| `currency` | string | Must be `USD`. |
| `transaction_count` | bigint | Count of transactions in the window. |
| `inbound_count` | bigint | Count of inbound transactions in the window. |
| `outbound_count` | bigint | Count of outbound transactions in the window. |
| `total_inbound_usd` | decimal | Sum of inbound USD-normalized amounts. |
| `total_outbound_usd` | decimal | Sum of outbound USD-normalized amounts. |
| `net_liquidity_usd` | decimal | Net liquidity for the window after sign is applied. |
| `run_id` | string | Identifier for the pipeline run. |
| `pipeline_version` | string | Version of the processing logic. |
| `dataset_version` | string | Version of the generated input set. |

Reporting semantics:
- One row represents one legal entity and one snapshot date.
- Liquidity is defined from rolling USD-normalized cash flows, not from an opening balance model.
- The persisted schema must preserve fixed-point precision.

### Elasticsearch Document Structure
Audit logs are written as documents intended for compliance, traceability, and search.

| Field | Type | Requirements |
|---|---|---|
| `event_id` | string | Unique audit event identifier. |
| `event_type` | string | Examples include `transaction_processed`, `transaction_rejected`, `fx_missing`, `snapshot_written`, `sink_failed`. |
| `run_id` | string | Pipeline run identifier. |
| `pipeline_version` | string | Processing version. |
| `dataset_version` | string | Input dataset version. |
| `source_file` | string | Source artifact path or logical filename. |
| `transaction_id` | string | Present when the event relates to a transaction. |
| `legal_entity_id` | string | Present when applicable. |
| `event_timestamp_utc` | timestamp | Business event time. |
| `processing_timestamp_utc` | timestamp | Time the event was observed by the pipeline. |
| `currency` | string | Original transaction currency, when relevant. |
| `amount_original` | decimal | Original nominal amount, when relevant. |
| `fx_rate_applied` | decimal | FX rate used in conversion, when relevant. |
| `amount_usd` | decimal | USD-normalized amount, when relevant. |
| `direction` | string | Original direction, when relevant. |
| `window_start_utc` | timestamp | Window start for snapshot-related events. |
| `window_end_utc` | timestamp | Window end for snapshot-related events. |
| `status` | string | Success, rejected, degraded, or failed. |
| `error_code` | string | Present when applicable. |
| `error_message` | string | Present when applicable. |

Audit semantics:
- Audit documents must capture both normal processing and exceptional conditions.
- Audit logging must be complete enough to reconstruct what happened for each transaction or batch.

## 6. Transformation Rules

Processing must follow this order and these rules:

1. Ingest the raw transaction and FX datasets.
2. Validate schemas, required fields, and key integrity.
3. Join each transaction to the correct FX rate using transaction event date in UTC.
4. Apply FX conversion before any aggregation.
5. Convert every transaction to USD using fixed-point decimal arithmetic.
6. Apply direction sign:
- `INBOUND` contributes positively.
- `OUTBOUND` contributes negatively.
7. Aggregate by legal entity and trailing 30-day event-time window.
8. Persist the daily liquidity snapshot to PostgreSQL.
9. Emit audit events to Elasticsearch.

Window rules:
- The trailing window is inclusive of the snapshot date.
- The default definition is 30 calendar days in UTC.
- If fewer than 30 days of history exist at the beginning of the dataset, the window uses all available prior data rather than inventing synthetic history.
- Aggregation is based on event time, not ingestion order or processing time.

Precision rules:
- No floating point arithmetic is allowed for money or FX values.
- Intermediate and final monetary values must remain in fixed-point decimal form.
- Any rounding must be deterministic and must not depend on execution order.

## 7. System Architecture Rules

### What Runs in Spark
- Input ingestion from Parquet.
- Schema and business-rule validation.
- FX lookup and currency normalization.
- Rolling 30-day aggregation.
- Construction of reporting rows.
- Creation of audit events for downstream sinks.

### What Belongs to the Data Generation Layer
- Synthetic transaction creation.
- Synthetic FX rate creation.
- Seed management.
- Dataset versioning.
- Manifest and checksum creation.
- Atomic publication of generated artifacts.

### What Belongs to the Ingestion Layer
- Discovering input artifacts.
- Reading Parquet datasets.
- Enforcing schema compatibility.
- Rejecting malformed rows or files according to the failure rules.
- Preparing validated records for transformation.

### What Belongs to the Sink Layer
- Writing liquidity snapshots to PostgreSQL.
- Writing audit documents to Elasticsearch.
- Managing sink-specific retries or degradation behavior.
- Preserving idempotency of published outputs.

### Required Data Flow Order
Generator -> Parquet -> Spark Ingestion -> Transform -> Sinks

That order is mandatory. No aggregation may happen before FX normalization.

## 8. Failure Handling Rules

### Missing FX Data Handling
- If a required non-USD FX rate is missing for a transaction date, the affected transaction must be rejected from aggregation and an audit event must be emitted.
- If the USD identity rate is missing, the FX dataset is invalid and the batch must fail because the pipeline cannot safely infer conversion without violating determinism.
- The system must never silently substitute a guessed FX rate.

### Malformed Transaction Handling
- A malformed transaction row must be rejected deterministically.
- The rejection must be auditable.
- Other valid records in the same batch may continue processing unless the corruption is systemic.

### Partial Dataset Corruption Handling
- If a Parquet file is unreadable or structurally corrupted, the affected input batch is invalid.
- The pipeline must not publish a partial financial snapshot as if the input were complete.
- The batch must fail in a controlled way and record the failure in audit output.
- Corruption must never be silently skipped.

### PostgreSQL Failure Behavior
- PostgreSQL is the system of record for reporting output.
- If the PostgreSQL write fails, the snapshot batch is not considered complete.
- The write must be idempotent on retry.
- Partial committed state must not produce duplicate logical rows.

### Elasticsearch Failure Isolation Rules
- Elasticsearch failure must not break main processing.
- A failure in the audit sink must not prevent PostgreSQL snapshots from being produced.
- Audit sink failure should degrade audit completeness only, not liquidity computation.
- The pipeline must surface the sink degradation in run metadata and logs.

## 9. Determinism Rules

Reproducibility must be guaranteed by the following rules:

- All randomness must be seeded.
- Seed plus generator version plus configuration must uniquely define the generated dataset.
- Synthetic FX generation must be stable for a given date and currency pair.
- Synthetic transaction ordering must be stable and deterministic.
- The pipeline must not depend on current time, wall-clock ordering, or external APIs.
- Spark execution order must not affect results.
- Grouping and aggregation results must be stable regardless of partition count or task retries.
- Output ordering for persisted results must be canonical and reproducible.
- All file names, manifests, and version tags must be deterministic for a given run configuration.

## 10. Non-Functional Requirements

### Scalability Expectations
- The system must support distributed execution using Spark.
- The logical design must scale horizontally as transaction volume increases.
- The demo configuration should run comfortably on a laptop or single-node Spark setup.
- The same pipeline design must also support cluster execution without semantic changes.

### Performance Assumptions
- The demo-scale dataset is intentionally small enough for local execution while still being useful for distributed testing.
- Larger datasets should remain feasible by increasing cluster resources and input partitioning.
- The system should favor deterministic correctness over aggressive but non-reproducible optimization.

### Data Volume Assumptions
- Demo scale is 500,000 transactions, 25 legal entities, 20 currencies, and 90 days of history.
- Scaled-up projection should support substantially larger volumes, such as tens of millions of transactions, more entities, and longer history windows, without changing the logical contract.

### Local vs Distributed Execution Behavior
- Local execution and distributed execution must be functionally identical.
- The only acceptable differences are performance characteristics and physical partitioning.
- Output values, row counts, and audit semantics must remain the same for the same inputs.
