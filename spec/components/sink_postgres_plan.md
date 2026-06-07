# PostgreSQL Sink Component Implementation Plan

## Summary
Implement the PostgreSQL sink as the system-of-record output for liquidity snapshots with deterministic, idempotent persistence.

## Steps
1. [ ] Confirm the target table shape, keys, and precision expectations.
   - Status: `pending`
   - Completion: the storage contract matches the reporting schema.
2. [ ] Implement idempotent writes and stable deduplication semantics.
   - Status: `pending`
   - Completion: repeated runs do not create duplicate logical rows.
3. [ ] Implement atomic batch persistence and explicit failure handling.
   - Status: `pending`
   - Completion: write success and failure are reported deterministically.
4. [ ] Add verification coverage for retries, partial writes, and schema violations.
   - Status: `pending`
   - Completion: sink behavior is testable under failure conditions.

## Update Rule
When a step is started, change its status to `in_progress`. When it is finished and verified, change it to `done`.
