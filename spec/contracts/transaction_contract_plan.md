# Transaction Contract Implementation Plan

## Summary
Implement the transaction contract as the source foundation for the pipeline so ingestion, validation, and downstream transformation can rely on one stable record definition.

## Steps
1. [ ] Confirm the canonical transaction schema and required source fields.
   - Status: `pending`
   - Completion: the transaction record definition is unambiguous.
2. [ ] Implement validation rules for timestamps, direction, amount, and currency shape.
   - Status: `pending`
   - Completion: malformed or invalid transaction rows are rejected consistently.
3. [ ] Define how rejected rows are surfaced for audit and downstream safety.
   - Status: `pending`
   - Completion: rejection behavior is explicit and deterministic.
4. [ ] Add verification coverage for duplicates, malformed rows, and invalid values.
   - Status: `pending`
   - Completion: the contract can be used as an executable source-of-truth.

## Update Rule
When a step is started, change its status to `in_progress`. When it is finished and verified, change it to `done`.
