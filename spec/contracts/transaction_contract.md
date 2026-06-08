# Transaction Data Contract

This contract defines the canonical transaction input accepted by the pipeline.

## Column Names
| Column | Notes |
|---|---|
| `transaction_id` | Unique transaction identifier. |
| `timestamp` | Event time in UTC. |
| `legal_entity_id` | Legal entity identifier. |
| `currency` | ISO 4217 currency code. |
| `amount` | Nominal transaction amount in source currency. |
| `direction` | `INBOUND` or `OUTBOUND`. |

## Data Types
| Column | Data Type |
|---|---|
| `transaction_id` | string |
| `timestamp` | timestamp |
| `legal_entity_id` | string |
| `currency` | string |
| `amount` | decimal |
| `direction` | string |

## Nullability Rules
| Column | Nullability |
|---|---|
| `transaction_id` | Not null |
| `timestamp` | Not null |
| `legal_entity_id` | Not null |
| `currency` | Not null |
| `amount` | Not null |
| `direction` | Not null |

## Validation Rules
- `transaction_id` must be unique within the combined input set.
- `timestamp` must be a valid UTC event timestamp and must parse deterministically.
- `legal_entity_id` must be present.
- `currency` must be an uppercase ISO 4217 code.
- `amount` must be a non-negative fixed-point decimal value.
- `direction` must be exactly `INBOUND` or `OUTBOUND`.
- Any row that violates these rules is rejected before downstream transformation.

## Rejection Handling
- Invalid rows are excluded from downstream processing.
- Rejections must be deterministic so the same input set produces the same accepted and rejected row sets.
- Rejected rows are surfaced through audit handling outside this contract; the contract itself does not define sink mechanics.

## Ordering Guarantees
- No physical row order is guaranteed in source files.
- Downstream processing must treat the dataset as unordered.
- `transaction_id` must be globally unique within the combined input set, but the contract does not require sorted order by `transaction_id` or `timestamp`.
- Any output ordering derived from this contract must be imposed explicitly by the pipeline, not assumed from the source files.
