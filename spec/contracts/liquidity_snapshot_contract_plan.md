# Liquidity Snapshot Contract Implementation Plan

## Summary
Implement the liquidity snapshot reporting contract in clear phases so the reporting schema, aggregation behavior, and sink expectations stay aligned.

## Steps
1. [x] Confirm the snapshot schema and required reporting fields.
   - Status: `done`
   - Completion: the canonical liquidity snapshot shape is fixed.
2. [x] Implement the rolling-window aggregation behavior that produces snapshot rows.
   - Status: `done`
   - Completion: the output rows match the defined liquidity window semantics.
3. [x] Implement uniqueness, ordering, and nullability expectations for the reporting key.
   - Status: `done`
   - Completion: the sink and contract agree on row identity and storage rules.
4. [x] Add verification coverage for snapshot completeness and reproducibility.
   - Status: `done`
   - Completion: the reporting contract can be validated against deterministic inputs.

## Update Rule
When a step is started, change its status to `in_progress`. When it is finished and verified, change it to `done`.
