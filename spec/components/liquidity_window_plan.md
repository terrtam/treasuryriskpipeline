# Liquidity Window Component Implementation Plan

## Summary
Implement the rolling liquidity window logic that turns USD-normalized transactions into daily reporting snapshots by legal entity.

## Steps
1. [ ] Confirm the event-time window definition and snapshot date semantics.
   - Status: `pending`
   - Completion: the trailing 30-day logic is fixed.
2. [ ] Implement aggregation by legal entity and snapshot date.
   - Status: `pending`
   - Completion: each entity’s windowed totals are computed independently.
3. [ ] Implement inbound, outbound, and net liquidity calculations in USD.
   - Status: `pending`
   - Completion: the output totals follow the contract sign rules.
4. [ ] Add verification coverage for out-of-order data and window boundaries.
   - Status: `pending`
   - Completion: the rolling window behavior is validated against edge cases.

## Update Rule
When a step is started, change its status to `in_progress`. When it is finished and verified, change it to `done`.
