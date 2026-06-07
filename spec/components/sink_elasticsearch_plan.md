# Elasticsearch Sink Component Implementation Plan

## Summary
Implement the Elasticsearch sink as the audit and observability target for pipeline events, failures, and searchable transaction history.

## Steps
1. [ ] Confirm the audit index shape and document categories to store.
   - Status: `pending`
   - Completion: the sink has a stable indexing contract.
2. [ ] Implement write behavior for audit logs and searchable operational events.
   - Status: `pending`
   - Completion: audit records are written consistently and predictably.
3. [ ] Implement failure isolation so Elasticsearch issues do not block core reporting.
   - Status: `pending`
   - Completion: search sink failures remain isolated from the main pipeline path.
4. [ ] Add verification coverage for indexing failures and retry behavior.
   - Status: `pending`
   - Completion: audit sink behavior can be checked without ambiguity.

## Update Rule
When a step is started, change its status to `in_progress`. When it is finished and verified, change it to `done`.
