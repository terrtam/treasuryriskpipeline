# Liquidity Snapshot Data Contract

This contract defines the canonical PostgreSQL reporting output produced by the pipeline.

## Column Names
| Column | Notes |
|---|---|
| `snapshot_date` | UTC date of the liquidity snapshot. |
| `legal_entity_id` | Legal entity identifier. |
| `window_start_utc` | Inclusive start of the rolling window. |
| `window_end_utc` | Inclusive end of the rolling window. |
| `currency` | Reporting currency, always `USD`. |
| `transaction_count` | Number of transactions in the window. |
| `inbound_count` | Number of inbound transactions in the window. |
| `outbound_count` | Number of outbound transactions in the window. |
| `total_inbound_usd` | Total inbound USD amount. |
| `total_outbound_usd` | Total outbound USD amount, stored consistently as either positive magnitude or negative signed total. |
| `net_liquidity_usd` | Net liquidity in USD. |
| `run_id` | Pipeline run identifier. |
| `pipeline_version` | Processing version. |
| `dataset_version` | Input dataset version. |

## Data Types
| Column | Data Type |
|---|---|
| `snapshot_date` | date |
| `legal_entity_id` | string |
| `window_start_utc` | timestamp |
| `window_end_utc` | timestamp |
| `currency` | string |
| `transaction_count` | bigint |
| `inbound_count` | bigint |
| `outbound_count` | bigint |
| `total_inbound_usd` | decimal |
| `total_outbound_usd` | decimal |
| `net_liquidity_usd` | decimal |
| `run_id` | string |
| `pipeline_version` | string |
| `dataset_version` | string |

## Nullability Rules
| Column | Nullability |
|---|---|
| `snapshot_date` | Not null |
| `legal_entity_id` | Not null |
| `window_start_utc` | Not null |
| `window_end_utc` | Not null |
| `currency` | Not null |
| `transaction_count` | Not null |
| `inbound_count` | Not null |
| `outbound_count` | Not null |
| `total_inbound_usd` | Not null |
| `total_outbound_usd` | Not null |
| `net_liquidity_usd` | Not null |
| `run_id` | Not null |
| `pipeline_version` | Not null |
| `dataset_version` | Not null |

## Ordering Guarantees
- The contract does not guarantee any physical row order in PostgreSQL storage.
- Logical uniqueness must be preserved for the reporting key defined by the sink implementation, typically at minimum `(snapshot_date, legal_entity_id, run_id)`.
- If ordered results are needed for reporting, they must be requested explicitly via query ordering and not assumed from storage order.
