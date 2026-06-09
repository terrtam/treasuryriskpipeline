# FX Contract Implementation Plan

## Summary
Implement the FX contract in a staged, traceable way so the conversion path can be built, verified, and updated over time.

## Steps
1. [x] Confirm the canonical FX input and output fields in the contract.
   - Status: `done`
   - Completion: the contract and downstream implementation agree on the exact FX record shape.
2. [x] Implement FX validation rules for date, currency, and rate integrity.
   - Status: `done`
   - Completion: invalid FX rows are rejected deterministically.
3. [x] Implement deterministic USD conversion behavior for transaction records.
   - Status: `done`
   - Completion: the same input always yields the same USD-normalized output.
4. [x] Add verification coverage for missing, duplicate, and malformed FX cases.
   - Status: `done`
   - Completion: edge cases are documented and testable from the plan.

## Update Rule
When a step is started, change its status to `in_progress`. When it is finished and verified, change it to `done`.
