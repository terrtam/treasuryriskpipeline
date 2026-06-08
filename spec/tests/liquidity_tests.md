# Liquidity Behavior Tests

This document defines expected system behavior before implementation.

These are not code-level unit tests. They are executable requirements written in plain English and structured tables so implementation can be verified against deterministic outcomes.

## Component Mapping Rules

- ingestion = raw data correctness
- fx_conversion = currency normalization + FX dependencies
- liquidity_window = aggregation + rolling window logic
- sinks = persistence + failure handling
- system-wide = determinism + reproducibility

## Test Format
Each behavior test specifies:
- Test case name
- Input conditions
- Expected output or system behavior
- Failure expectation when applicable

All tests assume:
- UTC timestamps
- Fixed-point decimal arithmetic
- Deterministic seeded synthetic data
- FX conversion happens before aggregation
- Rolling liquidity uses a trailing 30 calendar-day window inclusive of the snapshot date

## Ingestion

### 1. Null Timestamp Rejection
| Input Transactions | Expected Output |
|---|---|
| Transaction row with null `timestamp` | Row is rejected and written to audit output as malformed input. |
| Transaction row with malformed timestamp string | Row is rejected and the batch continues only if the defect is row-local. |

Expected behavior:
- Event time is mandatory.
- No liquidity record may be derived from a transaction with missing or malformed event time.

### 2. Negative Amount Rejection
| Input Transactions | Expected Output |
|---|---|
| Transaction with `amount = -100.00` | Row is rejected. |
| Mixed batch with valid and negative-amount transactions | Valid rows continue; invalid rows are audited and excluded. |

Expected behavior:
- Transaction amounts must be non-negative in the source dataset.
- Negative amounts are not corrected automatically.

### 3. Duplicate Transaction IDs
| Input Transactions | Expected Output |
|---|---|
| Two rows with the same `transaction_id` | Duplicate is rejected deterministically. |
| Same duplicate appears across two input files | Duplicate is still rejected consistently across the combined input set. |

Expected behavior:
- Transaction IDs are globally unique within the dataset.
- Duplicate identity collisions must not produce double counting.

### 4. Duplicate FX Keys
| Input FX Rows | Expected Output |
|---|---|
| Two rows with the same `(date, base_currency, quote_currency)` | FX input is invalid for that key and must be rejected deterministically. |

Expected behavior:
- There is exactly one usable FX rate per date and currency pair.
- Duplicate FX keys are not resolved by last-write-wins behavior.

## FX Conversion

### 1. FX Conversion Before Aggregation
| Input Transactions | FX Data | Expected Output |
|---|---|---|
| Mixed USD and non-USD transactions for the same entity | Valid daily FX rates for the non-USD currencies | All non-USD transactions are converted to USD before any liquidity aggregation occurs. |
| Same transactions, same FX inputs, different Spark partitioning | Same valid FX data | The USD-normalized totals are identical regardless of partition count or processing order. |

Expected behavior:
- Currency normalization happens before sums, counts, or rolling windows are computed.
- The aggregation layer never sees source-currency amounts as the basis for liquidity totals.

### 2. Missing FX Rate
| Input Transactions | FX Data | Expected Output |
|---|---|---|
| A non-USD transaction on a date with no matching FX rate | FX file missing that dateâ€™s rate | The transaction is excluded from liquidity aggregation and an audit event is written indicating missing FX. |
| A USD transaction on a date with no foreign FX rates | FX file otherwise valid, but only USD identity rates available | The USD transaction is still processed because USD/USD = 1.0 is explicit. |
| Any transaction when USD/USD rate is missing | FX file missing USD identity rate | The FX reference set is invalid and the batch fails. |

Expected behavior:
- Missing non-USD FX data is handled as a transaction-level rejection.
- Missing USD identity rates are treated as a systemic input failure.
- No guessed or interpolated FX rates are allowed.

### 3. Mixed Currency Normalization
| Input Transactions | FX Data | Expected Output |
|---|---|---|
| 100 EUR, 200 GBP, 300 USD | Valid FX for EUR and GBP | All totals are reported in USD after conversion. |
| Same amounts but source currency order changes | Same FX data | Result is identical. |

Expected behavior:
- The output currency is always USD.
- Source currency ordering does not affect totals.

### 4. Inbound and Outbound Sign Handling
| Input Transactions | Expected Output |
|---|---|
| `INBOUND` transaction | Contributes positively to net liquidity. |
| `OUTBOUND` transaction | Contributes negatively to net liquidity. |
| Equal inbound and outbound values in USD | Net liquidity is zero for the window. |

Expected behavior:
- Direction determines the sign after FX conversion.
- The sign convention is stable and must not vary by sink or report type.

## Liquidity Window

### 1. Rolling 30-Day Liquidity
| Input Transactions | Expected Output |
|---|---|
| Transactions for one legal entity spread across 35 days | Only the most recent 30 calendar days are included in the liquidity snapshot for each snapshot date. |
| Transactions on day 1 through day 35 with one daily snapshot per day | The snapshot on day 35 excludes day 1 and includes days 6 through 35, assuming a 30-day inclusive window definition. |
| Multiple transactions on the same day | All same-day transactions are included if they fall within the trailing window. |

Expected behavior:
- The rolling window is event-time based.
- The snapshot for a given date includes all qualifying transactions whose event timestamps fall inside the trailing 30-day interval.
- Older transactions outside the window are excluded deterministically.

### 2. Entity-Level Aggregation
| Input Transactions | Expected Output |
|---|---|
| Transactions from 5 legal entities | One liquidity snapshot series per entity. |
| Mixed entity IDs in the same batch | Aggregation groups records by legal entity first, then by snapshot date. |

Expected behavior:
- Entity boundaries are never crossed during aggregation.
- Each legal entity receives independent liquidity calculation.

### 3. Late or Out-of-Order Input Files
| Input Conditions | Expected Output |
|---|---|
| Files arrive in different physical order | Final logical output is unchanged. |
| Transactions are not sorted by timestamp within a file | Output remains the same after deterministic ingestion and processing. |

Expected behavior:
- Physical input order does not affect results.
- The pipeline behavior is based on logical content, not file arrival sequence.

## Sinks

### 1. Elasticsearch Failure Isolation
| Input Conditions | Expected Output |
|---|---|
| PostgreSQL write succeeds, Elasticsearch unavailable | Liquidity snapshots are still written to PostgreSQL. Audit sink failure is recorded separately. |
| Elasticsearch indexing times out | Main financial output remains successful if PostgreSQL succeeds. |

Expected behavior:
- Elasticsearch failure never blocks the core liquidity computation or PostgreSQL persistence.
- Audit completeness may degrade, but financial output must remain available.

### 2. PostgreSQL Write Failure
| Input Conditions | Expected Output |
|---|---|
| Valid liquidity snapshots, PostgreSQL unavailable | The reporting batch is not considered complete. |
| Retry after transient PostgreSQL outage | Data is written once without duplicate logical rows. |

Expected behavior:
- PostgreSQL is the authoritative reporting sink.
- Writes must be idempotent across retries.

## System-Wide

### 1. Deterministic Regeneration
| Input Conditions | Expected Output |
|---|---|
| Same seed, same generator version, same configuration | Identical synthetic datasets are produced. |
| Same input datasets, different Spark partitioning | Identical output snapshots and audit semantics are produced. |

Expected behavior:
- The system is reproducible from the same inputs.
- Runtime partitioning does not change business outputs.

### 2. Demo-Scale Dataset Size
| Input Conditions | Expected Output |
|---|---|
| 500,000 transactions, 25 legal entities, 20 currencies, 90-day window | Pipeline completes within demo-scale expectations and produces the full set of outputs. |

Expected behavior:
- The demo dataset is large enough to validate distributed patterns.
- The system remains usable on local or modest distributed execution.

## Acceptance Rules
- A test passes only if the observed behavior matches the expected behavior exactly.
- Any silent coercion, guessed FX value, duplicate double-counting, or non-deterministic output is a failure.
- Behavior tests are the authoritative contract until implementation exists.

## Implementation Mapping

### ingestion
- Source module: `src/ingestion`
- Plan file: `spec/components/ingestion_plan.md`

### fx_conversion
- Source module: `src/fx`
- Plan file: `spec/components/fx_conversion_plan.md`

### liquidity_window
- Source module: `src/liquidity`
- Plan file: `spec/components/liquidity_window_plan.md`

### sinks
- Source module: `src/sinks`
- Plan file: `spec/components/sink_postgres_plan.md` and `spec/components/sink_elasticsearch_plan.md`

### system-wide
- Source module: cross-cutting across `src/ingestion`, `src/fx`, `src/liquidity`, and `src/sinks`
- Plan file: `spec/components/data_generation_plan.md` and the relevant component plans above
