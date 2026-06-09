# PostgreSQL Sink Component Implementation Plan

## Summary
Implement the PostgreSQL sink as the system-of-record output for liquidity snapshots with deterministic, idempotent persistence.

## Steps
1. [x] Confirm the target table shape, keys, and precision expectations.
   - Status: `done`
   - Completion: the storage contract matches the reporting schema.
2. [x] Implement idempotent writes and stable deduplication semantics.
   - Status: `done`
   - Completion: repeated runs do not create duplicate logical rows.
3. [x] Implement atomic batch persistence and explicit failure handling.
   - Status: `done`
   - Completion: write success and failure are reported deterministically.
4. [x] Add verification coverage for retries, partial writes, and schema violations.
   - Status: `done`
   - Completion: sink behavior is testable under failure conditions.

## Update Rule
When a step is started, change its status to `in_progress`. When it is finished and verified, change it to `done`.
