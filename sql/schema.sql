-- TreasuryPipeline PostgreSQL schema
-- Canonical source, reporting, and relational audit tables.

BEGIN;

CREATE SCHEMA IF NOT EXISTS treasury;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'snapshot_write_status') THEN
        CREATE TYPE treasury.snapshot_write_status AS ENUM ('SUCCESS', 'FAILED');
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'audit_status') THEN
        CREATE TYPE treasury.audit_status AS ENUM ('SUCCESS', 'REJECTED', 'DEGRADED', 'FAILED');
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'audit_event_type') THEN
        CREATE TYPE treasury.audit_event_type AS ENUM (
            'transaction_processed',
            'transaction_rejected',
            'fx_missing',
            'snapshot_written',
            'sink_failed'
        );
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'transaction_direction') THEN
        CREATE TYPE treasury.transaction_direction AS ENUM ('INBOUND', 'OUTBOUND');
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS treasury.transactions (
    transaction_id    text                    NOT NULL,
    "timestamp"       timestamp               NOT NULL,
    legal_entity_id   text                    NOT NULL,
    currency          char(3)                 NOT NULL,
    amount            numeric(20, 6)         NOT NULL,
    direction         treasury.transaction_direction NOT NULL,
    created_at_utc    timestamp               NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    CONSTRAINT transactions_pk PRIMARY KEY (transaction_id),
    CONSTRAINT transactions_currency_chk CHECK (currency = upper(currency)),
    CONSTRAINT transactions_amount_chk CHECK (amount >= 0)
);

CREATE INDEX IF NOT EXISTS transactions_entity_ts_idx
    ON treasury.transactions (legal_entity_id, "timestamp" DESC);

CREATE INDEX IF NOT EXISTS transactions_currency_ts_idx
    ON treasury.transactions (currency, "timestamp" DESC);

CREATE TABLE IF NOT EXISTS treasury.usd (
    transaction_id       text                    NOT NULL,
    "timestamp"         timestamp               NOT NULL,
    legal_entity_id      text                    NOT NULL,
    currency             char(3)                 NOT NULL,
    amount               numeric(20, 6)          NOT NULL,
    direction            treasury.transaction_direction NOT NULL,
    fx_rate_applied      numeric(20, 10)         NOT NULL,
    amount_usd           numeric(20, 6)          NOT NULL,
    created_at_utc       timestamp               NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    CONSTRAINT usd_pk PRIMARY KEY (transaction_id),
    CONSTRAINT usd_currency_chk CHECK (currency = upper(currency)),
    CONSTRAINT usd_amount_chk CHECK (amount >= 0),
    CONSTRAINT usd_fx_rate_chk CHECK (fx_rate_applied > 0),
    CONSTRAINT usd_amount_usd_chk CHECK (amount_usd >= 0),
    CONSTRAINT usd_transaction_fk FOREIGN KEY (transaction_id)
        REFERENCES treasury.transactions (transaction_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS usd_entity_ts_idx
    ON treasury.usd (legal_entity_id, "timestamp" DESC);

CREATE INDEX IF NOT EXISTS usd_currency_ts_idx
    ON treasury.usd (currency, "timestamp" DESC);

CREATE TABLE IF NOT EXISTS treasury.fx_rates (
    date              date                    NOT NULL,
    base_currency     char(3)                 NOT NULL DEFAULT 'USD',
    quote_currency    char(3)                 NOT NULL,
    fx_rate           numeric(20, 10)        NOT NULL,
    created_at_utc    timestamp               NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    CONSTRAINT fx_rates_pk PRIMARY KEY (date, base_currency, quote_currency),
    CONSTRAINT fx_rates_base_currency_chk CHECK (base_currency = 'USD'),
    CONSTRAINT fx_rates_currency_chk CHECK (quote_currency = upper(quote_currency)),
    CONSTRAINT fx_rates_rate_chk CHECK (fx_rate > 0)
);

CREATE INDEX IF NOT EXISTS fx_rates_quote_date_idx
    ON treasury.fx_rates (quote_currency, date DESC);

CREATE TABLE IF NOT EXISTS treasury.liquidity_snapshots (
    snapshot_date       date            NOT NULL,
    legal_entity_id     text            NOT NULL,
    window_start_utc    timestamp       NOT NULL,
    window_end_utc      timestamp       NOT NULL,
    currency            char(3)         NOT NULL DEFAULT 'USD',
    transaction_count   bigint          NOT NULL CHECK (transaction_count >= 0),
    inbound_count       bigint          NOT NULL CHECK (inbound_count >= 0),
    outbound_count      bigint          NOT NULL CHECK (outbound_count >= 0),
    total_inbound_usd   numeric(20, 6)  NOT NULL,
    total_outbound_usd  numeric(20, 6)  NOT NULL,
    net_liquidity_usd   numeric(20, 6)  NOT NULL,
    run_id              text            NOT NULL,
    pipeline_version    text            NOT NULL,
    dataset_version     text            NOT NULL,
    created_at_utc      timestamp       NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    CONSTRAINT liquidity_snapshots_currency_chk CHECK (currency = 'USD'),
    CONSTRAINT liquidity_snapshots_window_chk CHECK (window_end_utc >= window_start_utc),
    CONSTRAINT liquidity_snapshots_pk PRIMARY KEY (snapshot_date, legal_entity_id, run_id)
);

CREATE INDEX IF NOT EXISTS liquidity_snapshots_legal_entity_idx
    ON treasury.liquidity_snapshots (legal_entity_id, snapshot_date DESC);

CREATE INDEX IF NOT EXISTS liquidity_snapshots_run_idx
    ON treasury.liquidity_snapshots (run_id);

CREATE TABLE IF NOT EXISTS treasury.audit_events (
    event_id                 uuid            NOT NULL DEFAULT gen_random_uuid(),
    event_type               treasury.audit_event_type NOT NULL,
    run_id                   text            NOT NULL,
    pipeline_version         text            NOT NULL,
    dataset_version          text            NOT NULL,
    source_file              text,
    transaction_id           text,
    legal_entity_id          text,
    event_timestamp_utc      timestamp       NOT NULL,
    processing_timestamp_utc timestamp       NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    currency                 char(3),
    amount_original          numeric(20, 6),
    fx_rate_applied          numeric(20, 10),
    amount_usd               numeric(20, 6),
    direction                text,
    window_start_utc         timestamp,
    window_end_utc           timestamp,
    status                   treasury.audit_status NOT NULL,
    error_code               text,
    error_message            text,
    created_at_utc           timestamp       NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    CONSTRAINT audit_events_pk PRIMARY KEY (event_id),
    CONSTRAINT audit_events_currency_chk CHECK (currency IS NULL OR currency = upper(currency)),
    CONSTRAINT audit_events_window_chk CHECK (
        window_start_utc IS NULL OR window_end_utc IS NULL OR window_end_utc >= window_start_utc
    )
);

CREATE INDEX IF NOT EXISTS audit_events_run_idx
    ON treasury.audit_events (run_id, processing_timestamp_utc DESC);

CREATE INDEX IF NOT EXISTS audit_events_event_type_idx
    ON treasury.audit_events (event_type, processing_timestamp_utc DESC);

CREATE INDEX IF NOT EXISTS audit_events_transaction_idx
    ON treasury.audit_events (transaction_id)
    WHERE transaction_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS audit_events_legal_entity_idx
    ON treasury.audit_events (legal_entity_id)
    WHERE legal_entity_id IS NOT NULL;

COMMIT;
