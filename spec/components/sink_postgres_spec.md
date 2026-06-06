# PostgreSQL Sink Component Specification

## Purpose
The PostgreSQL sink persists liquidity snapshots for reporting and downstream analytical access.

This sink is the system of record for the aggregated liquidity results.

## Inputs
The sink accepts liquidity snapshot records.

### Liquidity Snapshot Record
| Field | Type | Requirements |
|---|---|---|
| `snapshot_date` | date | Required. |
| `legal_entity_id` | string | Required. |
| `window_start_utc` | timestamp | Required. |
| `window_end_utc` | timestamp | Required. |
| `currency` | string | Required. Must be `USD`. |
| `transaction_count` | bigint | Required. |
| `inbound_count` | bigint | Required. |
| `outbound_count` | bigint | Required. |
| `total_inbound_usd` | decimal | Required. |
| `total_outbound_usd` | decimal | Required. |
| `net_liquidity_usd` | decimal | Required. |
| `run_id` | string | Required. |
| `pipeline_version` | string | Required. |
| `dataset_version` | string | Required. |

## Outputs
The sink produces persisted PostgreSQL rows and a deterministic write acknowledgment.

### Persisted PostgreSQL Row
The persisted row schema must match the input liquidity snapshot schema exactly.

### Write Acknowledgment
| Field | Type | Requirements |
|---|---|---|
| `run_id` | string | Required. |
| `target_table` | string | Required. |
| `row_count` | bigint | Required. |
| `write_status` | string | Required. Must be `SUCCESS` or `FAILED`. |
| `committed_at_utc` | timestamp | Required when the write succeeds. |
| `pipeline_version` | string | Required. |

## Rules
- Write rows only after the liquidity snapshot module has completed successfully.
- Use idempotent upsert or equivalent deterministic deduplication semantics.
- Preserve exact decimal precision in storage.
- Enforce a stable primary key, at minimum `(snapshot_date, legal_entity_id, run_id)`, unless the schema design specifies a stricter unique key.
- Do not alter liquidity values during persistence.
- Preserve row-level reproducibility across retries.
- Batch writes should be atomic at the logical record level as far as the database contract allows.

## Edge Cases
- Duplicate snapshot rows must resolve deterministically according to the pipeline's idempotency rule.
- Null or malformed timestamps in the payload must cause the write batch to fail.
- Negative values are not expected in this sink input unless they are the result of a valid net liquidity calculation.
- Partial reruns must not create duplicate logical snapshots.

## Failure Behavior
- If PostgreSQL is unavailable, the batch must fail its storage step and mark the run incomplete.
- If a write partially succeeds, the retry path must not duplicate committed logical rows.
- If schema constraints are violated, the sink must fail and surface a deterministic error.
- PostgreSQL failure must prevent the reporting batch from being considered complete.
- PostgreSQL failure must not corrupt the input dataset or the upstream computation state.
