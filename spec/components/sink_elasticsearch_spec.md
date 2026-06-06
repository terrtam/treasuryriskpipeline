# Elasticsearch Sink Component Specification

## Purpose
The Elasticsearch sink persists audit events for compliance, traceability, and search.

This sink is intentionally isolated from the financial reporting path. It must never block the main liquidity computation if it fails.

## Inputs
The sink accepts audit event records.

### Audit Event Record
| Field | Type | Requirements |
|---|---|---|
| `event_id` | string | Required. Unique. |
| `event_type` | string | Required. |
| `run_id` | string | Required. |
| `pipeline_version` | string | Required. |
| `dataset_version` | string | Required. |
| `source_file` | string | Required when applicable. |
| `transaction_id` | string | Nullable depending on event type. |
| `legal_entity_id` | string | Nullable depending on event type. |
| `event_timestamp_utc` | timestamp | Required. |
| `processing_timestamp_utc` | timestamp | Required. |
| `currency` | string | Nullable depending on event type. |
| `amount_original` | decimal | Nullable depending on event type. |
| `fx_rate_applied` | decimal | Nullable depending on event type. |
| `amount_usd` | decimal | Nullable depending on event type. |
| `direction` | string | Nullable depending on event type. |
| `window_start_utc` | timestamp | Nullable depending on event type. |
| `window_end_utc` | timestamp | Nullable depending on event type. |
| `status` | string | Required. Must be `SUCCESS`, `REJECTED`, `DEGRADED`, or `FAILED`. |
| `error_code` | string | Nullable. |
| `error_message` | string | Nullable. |

## Outputs
The sink produces indexed Elasticsearch documents and a write acknowledgment.

### Indexed Audit Document
The indexed document schema must match the input audit event schema exactly.

### Write Acknowledgment
| Field | Type | Requirements |
|---|---|---|
| `run_id` | string | Required. |
| `target_index` | string | Required. |
| `document_count` | bigint | Required. |
| `write_status` | string | Required. Must be `SUCCESS`, `PARTIAL`, or `FAILED`. |
| `committed_at_utc` | timestamp | Required when the write succeeds. |

## Rules
- Preserve every audit event that reaches the sink.
- Use deterministic document identifiers so retries do not create duplicate logical documents.
- Index normal processing events and rejection events alike.
- Do not modify the semantic content of audit events during indexing.
- Keep the sink logically independent from PostgreSQL persistence.
- If Elasticsearch is degraded, the pipeline must still produce the financial output.

## Edge Cases
- Missing optional payload fields are allowed when the event type does not require them.
- Null timestamps are not allowed for `event_timestamp_utc` or `processing_timestamp_utc`.
- Duplicate audit `event_id` values must be handled deterministically.
- Oversized error messages may need truncation only if the sink contract requires it, and that truncation must be deterministic.

## Failure Behavior
- Elasticsearch failure must not stop the main processing flow.
- If indexing fails, the pipeline must record a sink failure event locally or in another durable log path.
- If the sink is unavailable, audit completeness degrades but liquidity snapshots must still be written to PostgreSQL.
- Partial indexing must be detectable through write acknowledgment metadata.
- The sink must never silently drop events.
