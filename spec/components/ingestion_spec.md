# Ingestion Component Specification

## Purpose
The ingestion module is responsible for discovering raw Parquet inputs, validating them against the canonical source schemas, and producing clean records for downstream Spark transformations.

This module is the first protection layer in the pipeline. It must reject malformed, duplicate, or schema-incompatible data before any financial transformation occurs.

## Inputs
The ingestion module accepts the following raw datasets from `data_feeds/`:

### Raw Transaction Dataset
| Field | Type | Requirements |
|---|---|---|
| `transaction_id` | string | Required. Unique within the full input set. |
| `timestamp` | timestamp | Required. UTC event time. |
| `legal_entity_id` | string | Required. |
| `currency` | string | Required. Uppercase ISO 4217 code. |
| `amount` | decimal | Required. Non-negative. |
| `direction` | string | Required. Must be `INBOUND` or `OUTBOUND`. |

### Raw FX Rates Dataset
| Field | Type | Requirements |
|---|---|---|
| `date` | date | Required. UTC business date. |
| `base_currency` | string | Required. Must be `USD` for v1. |
| `quote_currency` | string | Required. Uppercase ISO 4217 code. |
| `fx_rate` | decimal | Required. Positive. |

## Outputs
The ingestion module produces validated canonical records only. Rejected rows are not passed downstream; they are emitted to audit logging as failure events.

### Validated Transaction Record
| Field | Type | Requirements |
|---|---|---|
| `transaction_id` | string | Required. Unique. |
| `timestamp` | timestamp | Required. UTC event time. |
| `legal_entity_id` | string | Required. |
| `currency` | string | Required. Uppercase ISO code. |
| `amount` | decimal | Required. Non-negative. |
| `direction` | string | Required. `INBOUND` or `OUTBOUND`. |

### Validated FX Rate Record
| Field | Type | Requirements |
|---|---|---|
| `date` | date | Required. |
| `base_currency` | string | Required. Must be `USD` for v1. |
| `quote_currency` | string | Required. |
| `fx_rate` | decimal | Required. Positive. |

## Rules
- Read only Parquet files that match the expected naming pattern for the current dataset version.
- Reject any file that cannot be parsed as Parquet.
- Reject rows with missing required fields.
- Reject rows with duplicate `transaction_id` values in the transaction dataset.
- Reject FX rows with duplicate `(date, base_currency, quote_currency)` keys.
- Reject transactions with null or invalid timestamps.
- Reject transactions with negative `amount` values.
- Reject transactions whose `direction` is not exactly `INBOUND` or `OUTBOUND`.
- Reject currency codes that are not uppercase ISO codes.
- Preserve input values exactly for accepted rows.
- Emit deterministic audit records for every rejection.
- Do not perform FX conversion, liquidity logic, or aggregation in this module.

## Edge Cases
- Missing FX rows are not resolved here; they are surfaced later by the FX conversion module.
- Null timestamps cause the transaction row to be rejected.
- Negative values cause the transaction row to be rejected.
- Duplicate transaction IDs cause all colliding rows to be rejected deterministically.
- Duplicate FX keys cause the affected FX rows to be rejected deterministically.
- Partially corrupted Parquet files must be treated as ingestion failures for the affected file.

## Failure Behavior
- If a file is unreadable or structurally corrupted, the ingestion batch fails for that input file and records an audit failure.
- If transaction schema validation fails, the affected rows are rejected and the batch continues only if the corruption is row-local and not systemic.
- If FX schema validation fails, the affected FX batch is rejected because downstream conversion cannot be trusted without a valid rate set.
- Ingestion failures must never silently coerce, impute, or guess missing values.
- Ingestion must not write any reporting outputs.
