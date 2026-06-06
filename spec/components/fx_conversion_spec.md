# FX Conversion Component Specification

## Purpose
The FX conversion module normalizes every accepted transaction into USD using the appropriate daily FX reference rate before any aggregation occurs.

This module is financially critical. It is the only place where transaction amounts are converted from source currency into the reporting currency.

## Inputs
The module accepts validated transaction records and validated FX rate records.

### Validated Transaction Record
| Field | Type | Requirements |
|---|---|---|
| `transaction_id` | string | Required. |
| `timestamp` | timestamp | Required. UTC event time. |
| `legal_entity_id` | string | Required. |
| `currency` | string | Required. |
| `amount` | decimal | Required. Non-negative. |
| `direction` | string | Required. |

### Validated FX Rate Record
| Field | Type | Requirements |
|---|---|---|
| `date` | date | Required. |
| `base_currency` | string | Required. Must be `USD` for v1. |
| `quote_currency` | string | Required. |
| `fx_rate` | decimal | Required. Positive. |

## Outputs
The module produces USD-normalized transaction records.

### USD-Normalized Transaction Record
| Field | Type | Requirements |
|---|---|---|
| `transaction_id` | string | Required. |
| `timestamp` | timestamp | Required. |
| `legal_entity_id` | string | Required. |
| `currency` | string | Required. Original source currency. |
| `amount` | decimal | Required. Original nominal amount. |
| `direction` | string | Required. |
| `fx_rate_applied` | decimal | Required. FX rate used for conversion. |
| `amount_usd` | decimal | Required. USD-normalized amount before direction sign is applied. |

## Rules
- Join each transaction to the FX rate for the transaction event date in UTC.
- Use the transaction date, not processing date, for FX lookup.
- Convert before any aggregation or rolling window calculation.
- Use fixed-point decimal arithmetic only.
- Preserve full precision through conversion and apply only deterministic rounding rules if the target scale requires it.
- For USD transactions, the applied FX rate must be exactly 1.0.
- Conversion must be deterministic for the same inputs, seed, and dataset version.
- Transactions cannot be aggregated until conversion succeeds.

## Edge Cases
- Missing FX data for a non-USD transaction date causes that transaction to be rejected from downstream aggregation.
- A missing USD identity rate is a fatal FX dataset error.
- Null timestamps prevent FX lookup and cause rejection.
- Negative transaction amounts are invalid and must never be converted.
- Duplicate FX rates for the same date and currency pair must be treated as invalid input.
- Transactions that arrive out of order must still convert identically because conversion is based on event date only.

## Failure Behavior
- If an FX rate is missing, the affected transaction is rejected and an audit event is emitted.
- If the FX reference set is incomplete or internally inconsistent, the batch fails rather than guessing rates.
- If the conversion logic encounters any non-deterministic condition, the batch must fail.
- Conversion failures must not produce partial reporting rows.
- Conversion failures must not stop unrelated valid transactions from being converted unless the FX input defect is systemic.
