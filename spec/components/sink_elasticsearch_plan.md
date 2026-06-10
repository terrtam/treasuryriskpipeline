# Elasticsearch Sink Component Implementation Plan

## Summary
Implement the Elasticsearch sink as the audit and observability target for pipeline events, failures, and searchable transaction history.

## Steps
1. [x] Confirm the audit index shape and document categories to store.
   - Status: `done`
   - Completion: the sink has a stable indexing contract.
2. [x] Implement write behavior for audit logs and searchable operational events.
   - Status: `done`
   - Completion: audit records are written consistently and predictably.
3. [x] Implement failure isolation so Elasticsearch issues do not block core reporting.
   - Status: `done`
   - Completion: search sink failures remain isolated from the main pipeline path.
4. [x] Add verification coverage for indexing failures and retry behavior.
   - Status: `done`
   - Completion: audit sink behavior can be checked without ambiguity.

## Update Rule
When a step is started, change its status to `in_progress`. When it is finished and verified, change it to `done`.
