# TreasuryPipeline Architecture Specification

## Purpose
This document defines the architectural rules for TreasuryPipeline as a layered, deterministic, Spark-based financial data system.

It is a prose specification, not a diagram. The intent is to define what belongs where, how data is allowed to move, and which parts of the system are stateful versus stateless.

## 1. Architecture Principles
- The system must be deterministic for the same inputs, seed, and versioned configuration.
- All financial conversion must happen before aggregation.
- Dependency flow must be one-way from raw input generation to ingestion to transformation to sink persistence.
- No layer may depend on a downstream layer for correctness.
- No sink may feed back into upstream computation.
- Spark is the distributed transformation engine, not the source of truth for business rules.
- Data generation, orchestration, and sink persistence are separate concerns.
- Audit delivery must be isolated from core reporting persistence.

## 2. Layer Model
TreasuryPipeline is organized into the following logical layers:

1. Data generation layer
2. Input storage layer
3. Ingestion layer
4. Spark transformation layer
5. Sink layer
6. Observability and audit layer

Each layer has a single responsibility and a defined direction of dependency.

## 3. Data Flow Rules
- The only valid primary flow is:
  Generator -> Parquet files -> Ingestion -> Spark transforms -> Sinks
- Data must be materialized as Parquet before pipeline ingestion.
- Ingestion must only consume the generated Parquet artifacts and must not call back into the generator.
- Spark transformations must only consume validated inputs from the ingestion layer.
- Sinks must only consume transformed outputs and must not influence transformation logic.
- Audit events are generated during processing but must not alter financial results.
- PostgreSQL output is the reporting sink for aggregated liquidity results.
- Elasticsearch output is the audit sink for searchable event logs.
- No reverse data flow is permitted from sinks back to Spark, ingestion, or generation.

## 4. Dependency Direction
Dependencies must point only forward in the architecture.

### Allowed dependency order
- `spec` documents may reference each other conceptually.
- Application code may depend on contracts and specs as design inputs.
- Ingestion may depend on contracts and source schemas.
- Spark transformation may depend on validated ingestion outputs.
- Sink writers may depend on transformed output contracts.

### Forbidden dependencies
- The generator must not depend on Spark jobs for correctness.
- Ingestion must not depend on PostgreSQL or Elasticsearch availability to validate input.
- Spark transformation must not depend on sink success to compute results.
- PostgreSQL persistence must not depend on Elasticsearch success.
- Elasticsearch persistence must not affect whether financial output is produced.
- Downstream outputs must not be required to reconstruct upstream raw data.

## 5. What Runs in Spark
Spark is the distributed compute layer. It is responsible for:
- Reading validated transaction and FX inputs.
- Performing schema-conforming joins between transactions and FX rates.
- Applying FX conversion in a deterministic way.
- Computing direction-adjusted USD values.
- Computing rolling 30-day liquidity by legal entity and snapshot date.
- Producing the final reporting dataset.
- Producing audit events that describe processing outcomes.

Spark is not responsible for:
- Generating synthetic input data.
- Persisting raw generated datasets.
- Managing external database connectivity policy.
- Defining business contracts for schema boundaries.
- Owning the final system-of-record state in PostgreSQL.

## 6. What Runs Outside Spark
The following logic must live outside Spark:

### Data generation
- Synthetic transaction creation.
- Synthetic FX rate creation.
- Seeded randomization and dataset versioning.
- Atomic publication of generated Parquet files.

### Ingestion orchestration
- File discovery.
- Dataset rebuild coordination.
- Input presence checks.
- File-level validation routing.

### Sink management
- PostgreSQL connection management.
- Elasticsearch indexing management.
- Retry and degradation behavior for sink writes.
- Final write acknowledgments.

### Environment and observability
- Run identifiers.
- Pipeline version labels.
- Dataset version labels.
- Operational logs that are not part of the financial audit record.

## 7. Stateful vs Stateless Responsibilities

### Stateless components
The following are stateless with respect to business data:
- Synthetic record generation logic, when driven only by explicit seed and configuration.
- Row validation rules.
- FX lookup logic for a single transaction.
- Conversion logic for a single transaction.
- Window aggregation logic for a snapshot computation.
- Audit event formatting.

These components may be executed repeatedly without changing their internal outcome for the same inputs.

### Stateful components
The following are stateful:
- Dataset generation output publication, because it manages file materialization and versioned artifacts.
- Ingestion orchestration, because it tracks which input artifacts were discovered and validated.
- PostgreSQL sink persistence, because it writes durable reporting state.
- Elasticsearch sink persistence, because it writes durable audit state.
- Run bookkeeping, because it tracks run identifiers, dataset versions, and output completion state.

### Controlled state
The system may maintain only the minimal state necessary for:
- Idempotent writes.
- Dataset version tracking.
- Run completion tracking.
- Audit completeness tracking.

The system must not maintain hidden mutable state that changes business results between runs.

## 8. Spark Boundary Rules
- Spark may hold intermediate distributed state during a job, but that state must not be treated as durable system state.
- Spark output must be deterministic for the same inputs even if partitioning changes.
- Spark tasks must be pure with respect to financial transformation logic.
- Spark may not decide whether a missing FX rate is acceptable by consulting sink state.
- Spark may not derive truth from previously written output tables unless an explicit reprocessing mode is defined in a separate spec.

## 9. File and Dataset Boundary Rules
- Parquet files are the canonical ingestion boundary.
- Generated datasets must be versioned and reproducible.
- Input file order is not part of the business contract.
- File naming is a metadata concern, not a business logic concern.
- The pipeline must tolerate physically different file layouts as long as the logical dataset is identical.

## 10. Failure Isolation Rules
- PostgreSQL failures must not alter transformation results.
- Elasticsearch failures must not block reporting writes.
- Ingestion failures must not be masked by downstream sink success.
- Transformation failures must prevent invalid outputs from being persisted.
- A partial sink failure must be visible in run status without contaminating financial computation state.

## 11. Determinism Rules
- All architecture decisions must preserve reproducibility.
- No component may depend on wall-clock time for financial logic.
- No component may depend on nondeterministic ordering for correctness.
- Any retry mechanism must produce the same logical output as the original successful execution.
- Partition count, task retry count, and cluster size must not change business semantics.

## 12. Implementation Boundaries
- Contracts define the exact input and output schemas.
- Component specs define the behavior of each module.
- Behavior tests define the expected outcomes before code exists.
- This architecture spec defines how the modules are allowed to depend on one another.

## 13. Architecture Invariants
- Raw generation precedes ingestion.
- Ingestion precedes transformation.
- Transformation precedes sink persistence.
- FX conversion precedes aggregation.
- PostgreSQL persistence is independent from Elasticsearch persistence.
- Audit failures do not invalidate reporting results.
- Reporting failures do not change transformation semantics.
- No reverse dependencies are allowed from sinks to compute layers.

