# TreasuryPipeline
# Treasury Pipeline: Distributed Liquidity Analytics Engine

## Overview

Treasury Pipeline is a distributed data engineering project that simulates how large financial institutions monitor liquidity across multiple global entities and currencies.

The system processes high-volume transactional ledger data, converts all monetary values into a common reporting currency (USD), and continuously calculates rolling 30-day liquidity positions using Apache Spark. Results are written to both a relational database for reporting and a search cluster for audit and compliance workflows.

This project demonstrates enterprise-scale ETL architecture, distributed analytics, financial data processing, window-based aggregations, and multi-sink data delivery.

---

## What This Project Does

Imagine a bank operating across multiple countries.

Every day, money moves in and out of different branches in different currencies. Treasury teams need to know:

* How much cash is currently available?
* How has liquidity changed over time?
* Are there any liquidity risks developing?
* Can regulators audit all activity if required?

This pipeline:

1. Reads transaction data from a distributed data lake.
2. Converts all currencies into USD using market FX rates.
3. Calculates a rolling 30-day liquidity position.
4. Stores summarized results in PostgreSQL for dashboards.
5. Stores audit records in Elasticsearch for compliance searches.

---

## Architecture

```text
                +-------------------+
                | Transaction Files |
                |    (Parquet)      |
                +---------+---------+
                          |
                          v
                +-----------------------------+
                | Input Ingestion &          |
                | Orchestration Layer        |
                | (Docker Compose / Bash)    |
                +---------+-------------------+
                          |
                          v
                +-------------------+
                |     Apache Spark  |
                | Distributed ETL   |
                +---------+---------+
                          |
          +---------------+---------------+
          |                               |
          v                               v
+-------------------+         +----------------------+
|   PostgreSQL      |         |   Elasticsearch      |
| Liquidity Reports |         | Compliance Auditing  |
+-------------------+         +----------------------+
```

---

## System Flow

1. Transaction and FX datasets are placed into the ingestion layer.
2. The orchestrator validates and prepares datasets for processing.
3. Apache Spark normalizes currencies, computes rolling liquidity windows, and generates aggregated metrics.
4. Outputs are split between PostgreSQL for analytics and Elasticsearch for audit/search workloads.
5. Logs and failures are captured independently for observability.
6. The DevOps layer manages lifecycle consistency across containers.

---

## Input Ingestion & Orchestration Layer

Before Spark processing begins, raw data is staged and orchestrated through a containerized ingestion layer.

This layer is responsible for:

* Coordinating daily batch ingestion jobs
* Simulating streaming-like orchestration of batch files
* Validating file availability and schema integrity
* Logging ingestion failures and retry conditions
* Passing structured input streams to downstream Spark jobs

This ensures a clear separation between raw data arrival and distributed processing execution.

---

## Technology Stack

| Component              | Technology                         |
| ---------------------- | ---------------------------------- |
| Distributed Processing | Apache Spark (PySpark)             |
| Data Format            | Apache Parquet                     |
| Relational Storage     | PostgreSQL                         |
| Search & Audit Storage | Elasticsearch                      |
| Dependency Resolution  | Apache Ivy                         |
| Language               | Python                             |
| Local Windows Support  | Winutils / Hadoop Native Libraries |

---

## Inputs

### 1. Transaction Ledger Feed

**Files**

```text
daily_transactions_*.parquet
```

**Purpose**

Contains high-volume cash movement events.

**Schema**

| Column          | Type                        |
| --------------- | --------------------------- |
| transaction_id  | String                      |
| timestamp       | ISO-8601 Timestamp          |
| legal_entity_id | String                      |
| direction       | String (INBOUND / OUTBOUND) |
| currency        | String                      |
| amount          | Decimal                     |

---

### 2. FX Reference Matrix

**Files**

```text
fx_rates_*.parquet
```

**Purpose**

Contains daily foreign exchange conversion rates used to normalize all currencies into USD.

**Schema**

| Column      | Type   |
| ----------- | ------ |
| currency    | String  |
| fx_rate     | Decimal |

---

## Outputs

### PostgreSQL

**Table**

```sql
liquidity_snapshots
```

Contains aggregated liquidity positions optimized for reporting dashboards.

Example:

| transaction_id   | snapshot_timestamp  | usd_amount | rolling_30d_liquidity_usd |
| ---------------- | ------------------- | ---------- | ------------------------- |
| TXN-20260502-001 | 2026-05-02 10:00:00 | 10800000   | 10800000                  |
| TXN-20260510-004 | 2026-05-10 14:30:00 | -2190000   | 8610000                   |
| TXN-20260604-001 | 2026-06-04 17:00:00 | 5000000    | 2810000                   |

---

### Elasticsearch

**Index**

```text
treasury_audit_logs
```

In addition to structured reporting, the system maintains a full-text searchable index of transaction and system logs.

Responsibilities:

* Index raw transaction logs for fast retrieval
* Enable compliance and audit investigations
* Provide low-latency search for operational troubleshooting
* Store pipeline execution logs and failure traces
* Support Kibana-based visualization and debugging workflows

Key Design Role:

This layer is optimized for:

* Operational visibility, not analytics
* Debugging and traceability
* Regulatory audit queries
* Fast lookup of transaction history

---

## Dual Output Architecture

The system separates outputs into two specialized data stores:

### 1. Relational Analytics Store (PostgreSQL)

* Structured financial reporting
* Aggregated liquidity metrics
* Dashboard-ready datasets
* ACID-compliant storage for correctness

### 2. Search & Audit Store (Elasticsearch)

* Raw transaction-level observability
* Full-text search capability
* Compliance and audit workflows
* Operational troubleshooting support

Design Rationale:

This separation reflects real-world treasury systems where:

* Analytics workloads require structured aggregation
* Audit workloads require fast retrieval and traceability
* A single database cannot efficiently serve both purposes

---

## Repository Structure

```text
TreasuryPipeline/
│
├── data_feeds/  
│   ├── daily_transactions_*.parquet   # Synthetic transaction dataset (demo-scale)
│   ├── fx_rates_*.parquet             # FX reference lookup table
│
├── src/
│   ├── config/                        # Runtime configuration helpers
│   │   ├── spark_config.py            # Spark session configuration
│   │   └── db_config.py               # Database connection settings
│   ├── DistributedDataTransformations.py  # Main Spark ETL pipeline
│
├── sql/
│   ├── schema.sql                      # PostgreSQL schema + indexes
│
├── tests/                             # (Optional extension layer)
│   ├── test_transformations.py        # ETL validation tests
│   ├── test_liquidity.py              # Rolling window logic tests
│
├── logs/                              # Runtime logs (local execution)
│
├── requirements.txt                   # Python dependencies
├── README.md                          # Project documentation
└── .gitignore                         # Ignored runtime / system files
```

### File Descriptions

#### DistributedDataTransformations.py

Primary Spark orchestration script.

Responsibilities:

* Load transaction feeds
* Load FX reference data
* Broadcast FX lookup tables
* Convert currencies to USD
* Calculate rolling liquidity windows
* Write results to PostgreSQL
* Write audit records to Elasticsearch

#### config/

Runtime configuration helpers live under `src/config/`.

The config helpers load a local `.env` file if present, then fall back to the current process environment.

* `spark_config.py` loads Spark session settings from environment variables
* `db_config.py` loads PostgreSQL connection settings from environment variables

Example local setup:

```powershell
Copy-Item .env.example .env
```

---

#### schema.sql

Database deployment script.

Creates:

* liquidity_snapshots table
* indexes
* data types and constraints

---

#### data_feeds/

Simulated data lake landing zone containing source datasets.

---

## Key Engineering Decisions

### Broadcast Joins

FX reference tables are small compared to transaction datasets.

The pipeline broadcasts FX rates to all Spark workers:

```python
broadcast(fx_df)
```

Benefits:

* Eliminates expensive cluster-wide shuffles
* Reduces network traffic
* Improves join performance

---

### Rolling 30-Day Window Calculations

Liquidity positions are computed using Spark window functions rather than iterative loops.

Benefits:

* Fully distributed computation
* Horizontally scalable
* Suitable for large datasets

The engine converts timestamps into Unix Epoch values and applies deterministic time-based windows.

---

### Financial Precision

Database schemas use:

```sql
NUMERIC(18,2)
```

instead of floating-point storage.

Benefits:

* Prevents floating-point precision errors
* Supports financial reporting requirements
* Maintains regulatory-grade accuracy

---

### Fault-Tolerant Dual Writes

PostgreSQL and Elasticsearch writes are isolated.

If Elasticsearch becomes unavailable:

* Liquidity reporting continues
* Errors are logged
* Core processing does not fail

This mirrors production resiliency patterns used in enterprise systems.

---

## Failure Handling & Observability

The pipeline includes a lightweight observability mechanism to capture and isolate failures during ingestion and processing.

Failure Sources:

* Ingestion validation failures
* Spark transformation errors
* Database write failures
* Elasticsearch indexing failures

Handling Strategy:

* Failures are logged independently from main data flow
* Processing continues where possible using a non-blocking design
* Error logs are persisted for debugging and replay
* Fault isolation prevents cascading pipeline collapse

This design mirrors production-grade resilience patterns used in financial data systems.

---

## DevOps & Infrastructure Layer

The pipeline is fully containerized using Docker-based infrastructure to simulate production-grade deployment environments.

Responsibilities:

* Container isolation for each service, including ETL, DB, search, and orchestration
* Multi-container coordination using Docker Compose
* Environment consistency across local and production-like setups
* Centralized execution of pipeline components
* Standardized deployment and teardown workflows

Benefits:

* Reproducible environments
* Reduced dependency drift
* Production-like execution simulation
* Easier scaling into cloud-native orchestration (ECS/Kubernetes-ready design)

---

## Scalability Considerations

The architecture is designed with horizontal scalability in mind:

* Spark cluster can scale worker nodes independently
* Partitioned transaction datasets support distributed execution
* Elasticsearch supports horizontal index scaling
* PostgreSQL handles aggregated workloads, not raw ingestion
* Containerized deployment allows independent service scaling

---

## Running the Project

### 1. Create Database

```sql
CREATE DATABASE treasury_db;
```

Run:

```sql
schema.sql
```

to create the required tables.

---

### 2. Windows Hadoop Compatibility

Place:

```text
winutils.exe
hadoop.dll
```

inside:

```text
C:\hadoop\bin
```

The application loads required native libraries automatically.

---

### 3. Execute Pipeline

```bash
python DistributedDataTransformations.py
```

Apache Ivy will automatically download required Spark connectors and dependencies.

---

## Skills Demonstrated

This project showcases:

* Distributed Data Processing
* ETL Pipeline Design
* Apache Spark Optimization
* Financial Analytics
* Window Functions
* Data Lake Processing
* Relational Database Integration
* Search Engine Integration
* Fault Tolerance
* Data Modeling
* Performance Engineering

---

## Notes

This project was designed to demonstrate enterprise-scale data engineering patterns commonly used in banking, treasury, risk management, and capital markets environments.

Key concepts include:

* Distributed computation with Apache Spark
* Time-series liquidity analytics
* Multi-currency normalization
* Broadcast join optimization
* Window-based aggregations
* Multi-destination persistence
* Financial data precision
* Fault-tolerant architecture

While the included datasets are simulated, the architectural patterns mirror real-world treasury and liquidity monitoring systems used in large financial institutions.
