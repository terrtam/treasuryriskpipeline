# FX Rates Data Contract

This contract defines the canonical FX reference data accepted by the pipeline.

## Column Names
| Column | Notes |
|---|---|
| `date` | FX reference date in UTC. |
| `base_currency` | Base currency. Must be `USD` for v1. |
| `quote_currency` | Currency being priced against the base currency. |
| `fx_rate` | FX rate expressed as USD value per unit of quote currency. |

## Data Types
| Column | Data Type |
|---|---|
| `date` | date |
| `base_currency` | string |
| `quote_currency` | string |
| `fx_rate` | decimal |

## Nullability Rules
| Column | Nullability |
|---|---|
| `date` | Not null |
| `base_currency` | Not null |
| `quote_currency` | Not null |
| `fx_rate` | Not null |

## Ordering Guarantees
- No physical row order is guaranteed in source files.
- Downstream processing must treat the dataset as unordered.
- The contract requires uniqueness of `(date, base_currency, quote_currency)` but does not require any sorted ordering of FX rows.
- The pipeline must explicitly select the correct FX row by key, not by file order.
