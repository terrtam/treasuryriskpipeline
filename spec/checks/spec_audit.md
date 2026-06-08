# Spec Audit

This file defines system-wide correctness rules used after implementation to validate that the system matches its specifications.

It is a verification checklist, not executable code.

## 1. Global System Rules

- All pipeline outputs must match their defined contracts exactly
- No silent schema inference is allowed
- All transformations must be deterministic
- Invalid data must be explicitly rejected, never dropped silently

## 2. Transaction Contract

Verify that transaction records satisfy all of the following:

- Schema correctness: all required fields exist
- Timestamp format: ISO-8601 UTC
- Currency validation: ISO-4217 only
- Amount validity: numeric and non-negative
- Direction validity: `INBOUND` or `OUTBOUND` only

## 3. Ingestion Layer

- Must validate all incoming records against the transaction contract
- Must separate valid vs rejected records explicitly
- Rejected records must include deterministic rejection reasons

## 4. FX Conversion

- All outputs must be normalized to USD
- Exchange rates must be deterministic per date
- No implicit currency assumptions are allowed

## 5. Liquidity Window

- Must use a 30-day rolling window per entity
- Must not leak data across entities
- Must be time-window deterministic

## 6. Sinks (Postgres + Elasticsearch)

- Output schemas must match the snapshot contract exactly
- No extra fields are allowed unless defined in the contract

## 7. Failure Categories

Classify audit failures into one or more of the following categories:

- Schema mismatch
- Behavioral drift from spec
- Non-deterministic output
- Missing edge case handling
- Invalid data propagation

## 8. Audit Execution Model

- This file is used after implementation
- It is not generated from code or plans
- It is used to verify correctness of implemented systems
- If audit fails:
  - either fix code
  - or update the spec
  - never silently ignore the failure
