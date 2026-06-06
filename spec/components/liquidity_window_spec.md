# Liquidity Window Component Specification

## Purpose
The liquidity window module computes rolling 30-day USD liquidity by legal entity from converted transaction data.

Its responsibility is to transform transaction-level USD flows into daily liquidity snapshots that can be reported, stored, and audited.

## Inputs
The module accepts USD-normalized transaction records.

### USD-Normalized Transaction Record
| Field | Type | Requirements |
|---|---|---|
| `transaction_id` | string | Required. |
| `timestamp` | timestamp | Required. UTC event time. |
| `legal_entity_id` | string | Required. |
| `currency` | string | Required. Must be `USD`. |
| `amount` | decimal | Required. |
| `direction` | string | Required. |
| `fx_rate_applied` | decimal | Required. |
| `amount_usd` | decimal | Required. |

## Outputs
The module produces daily liquidity snapshot records.

### Liquidity Snapshot Record
| Field | Type | Requirements |
|---|---|---|
| `snapshot_date` | date | Required. UTC snapshot date. |
| `legal_entity_id` | string | Required. |
| `window_start_utc` | timestamp | Required. Inclusive trailing window start. |
| `window_end_utc` | timestamp | Required. Inclusive trailing window end. |
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

## Rules
- Use event-time semantics exclusively.
- Compute a trailing 30 calendar-day window inclusive of the snapshot date.
- Use all available prior data if fewer than 30 days of history exist.
- Apply the transaction direction after FX normalization.
- Treat `INBOUND` as positive contribution.
- Treat `OUTBOUND` as negative contribution.
- Aggregate by `legal_entity_id` and `snapshot_date`.
- Preserve deterministic ordering of output rows.
- Never perform FX lookup in this module.
- Never write directly to sinks in this module.

## Edge Cases
- Null timestamps are invalid and should not reach this module.
- Transactions outside the active window must be excluded deterministically.
- Multiple transactions for the same legal entity on the same timestamp must be included in the window sum normally.
- Duplicate `transaction_id` values should not appear here because they are rejected upstream.
- If a legal entity has no transactions in the window, the module may omit the row unless the pipeline contract requires explicit zero rows.

## Failure Behavior
- If input data is not already USD-normalized, the module must fail rather than infer conversion.
- If timestamps are malformed or missing, the input batch is invalid.
- If rolling windows cannot be computed deterministically, the batch fails.
- If an intermediate aggregation state is corrupted, the affected batch must be rejected rather than producing partial liquidity output.
- The module must not write to PostgreSQL or Elasticsearch.
