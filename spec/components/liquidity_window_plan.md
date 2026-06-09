# Liquidity Window Component Implementation Plan

## Summary
Implement the rolling liquidity window logic that turns USD-normalized transactions into daily reporting snapshots by legal entity.

## Steps
1. [x] Confirm the event-time window definition and snapshot date semantics.
   - Status: `done`
   - Completion: the trailing 30-day logic is fixed.
2. [x] Implement aggregation by legal entity and snapshot date.
   - Status: `done`
   - Completion: each entity's windowed totals are computed independently.
3. [x] Implement inbound, outbound, and net liquidity calculations in USD.
   - Status: `done`
   - Completion: the output totals follow the contract sign rules.
4. [x] Add verification coverage for out-of-order data and window boundaries.
   - Status: `done`
   - Completion: the rolling window behavior is validated against edge cases.

## Update Rule
When a step is started, change its status to `in_progress`. When it is finished and verified, change it to `done`.
