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

## Ordering Guarantees
- No physical row order is guaranteed in source files.
- Downstream processing must treat the dataset as unordered.
- `transaction_id` must be globally unique within the combined input set, but the contract does not require sorted order by `transaction_id` or `timestamp`.
- Any output ordering derived from this contract must be imposed explicitly by the pipeline, not assumed from the source files.
