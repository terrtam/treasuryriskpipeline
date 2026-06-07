# Ingestion Component Implementation Plan

## Summary
Implement the ingestion layer so raw files are validated, cleaned, and rejected deterministically before any financial transformation occurs.

## Steps
1. [ ] Confirm the raw input discovery rules and accepted file patterns.
   - Status: `pending`
   - Completion: the ingestion entry points are defined.
2. [ ] Implement schema validation and row-level rejection for invalid transaction and FX records.
   - Status: `pending`
   - Completion: malformed or incomplete rows are filtered consistently.
3. [ ] Implement duplicate detection and audit emission for rejected rows.
   - Status: `pending`
   - Completion: duplicate and invalid records are surfaced deterministically.
4. [ ] Add verification coverage for corruption, nulls, and invalid source values.
   - Status: `pending`
   - Completion: ingestion failures can be reproduced and inspected.

## Update Rule
When a step is started, change its status to `in_progress`. When it is finished and verified, change it to `done`.
