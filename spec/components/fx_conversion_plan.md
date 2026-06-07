# FX Conversion Component Implementation Plan

## Summary
Implement the FX conversion component as the single point where accepted transactions are normalized into USD before any aggregation occurs.

## Steps
1. [ ] Confirm event-date FX lookup rules and USD identity handling.
   - Status: `pending`
   - Completion: the lookup policy is fixed and documented.
2. [ ] Implement deterministic conversion from source currency into USD.
   - Status: `pending`
   - Completion: valid transactions always produce the same USD result.
3. [ ] Implement handling for missing, duplicate, and malformed FX data.
   - Status: `pending`
   - Completion: invalid FX conditions are rejected without guesswork.
4. [ ] Add verification coverage for conversion order and repeatability.
   - Status: `pending`
   - Completion: conversion behavior can be rechecked from the plan.

## Update Rule
When a step is started, change its status to `in_progress`. When it is finished and verified, change it to `done`.
