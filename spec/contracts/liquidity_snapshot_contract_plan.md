# Liquidity Snapshot Contract Implementation Plan

## Summary
Implement the liquidity snapshot reporting contract in clear phases so the reporting schema, aggregation behavior, and sink expectations stay aligned.

## Steps
1. [ ] Confirm the snapshot schema and required reporting fields.
   - Status: `pending`
   - Completion: the canonical liquidity snapshot shape is fixed.
2. [ ] Implement the rolling-window aggregation behavior that produces snapshot rows.
   - Status: `pending`
   - Completion: the output rows match the defined liquidity window semantics.
3. [ ] Implement uniqueness, ordering, and nullability expectations for the reporting key.
   - Status: `pending`
   - Completion: the sink and contract agree on row identity and storage rules.
4. [ ] Add verification coverage for snapshot completeness and reproducibility.
   - Status: `pending`
   - Completion: the reporting contract can be validated against deterministic inputs.

## Update Rule
When a step is started, change its status to `in_progress`. When it is finished and verified, change it to `done`.
