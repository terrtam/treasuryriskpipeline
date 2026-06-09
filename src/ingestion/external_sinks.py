from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from typing import Any, Callable, Protocol, TYPE_CHECKING
from urllib import request as urllib_request

from .audit import AuditEvent
from .validator import ValidatedFXRate, ValidatedTransaction

if TYPE_CHECKING:
    from src.fx.conversion import USDNormalizedTransaction


class SupportsCursor(Protocol):
    def execute(self, query: str, params: Any | None = None) -> Any: ...

    def executemany(self, query: str, params: list[tuple[Any, ...]]) -> Any: ...


class SupportsConnection(Protocol):
    def cursor(self) -> SupportsCursor: ...

    def commit(self) -> None: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class PostgresSinkConfig:
    transaction_table: str = "treasury.transactions"
    usd_table: str = "treasury.usd"
    fx_table: str = "treasury.fx_rates"
    audit_table: str = "treasury.audit_events"


@dataclass(frozen=True)
class ElasticsearchSinkConfig:
    index_name: str = "treasury_audit_logs"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _transaction_payload(record: ValidatedTransaction) -> tuple[Any, ...]:
    timestamp_utc = record.timestamp.astimezone(timezone.utc).replace(tzinfo=None)
    return (
        record.transaction_id,
        timestamp_utc,
        record.legal_entity_id,
        record.currency,
        record.amount,
        record.direction,
    )


def _fx_payload(record: ValidatedFXRate) -> tuple[Any, ...]:
    return (
        record.date,
        record.base_currency,
        record.quote_currency,
        record.fx_rate,
    )


def _usd_payload(record: USDNormalizedTransaction) -> tuple[Any, ...]:
    timestamp_utc = record.timestamp.astimezone(timezone.utc).replace(tzinfo=None)
    return (
        record.transaction_id,
        timestamp_utc,
        record.legal_entity_id,
        record.currency,
        record.amount,
        record.direction,
        record.fx_rate_applied,
        record.amount_usd,
    )


def _audit_timestamp(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _audit_payload(event: AuditEvent) -> tuple[Any, ...]:
    currency = event.currency.upper() if isinstance(event.currency, str) else None
    return (
        event.event_id,
        event.event_type,
        event.run_id,
        event.pipeline_version,
        event.dataset_version,
        event.source_file,
        event.transaction_id,
        event.legal_entity_id,
        _audit_timestamp(event.event_timestamp_utc),
        _audit_timestamp(event.processing_timestamp_utc),
        currency,
        event.amount_original,
        event.fx_rate_applied,
        event.amount_usd,
        event.direction,
        _audit_timestamp(event.window_start_utc) if event.window_start_utc is not None else None,
        _audit_timestamp(event.window_end_utc) if event.window_end_utc is not None else None,
        event.status,
        event.error_code,
        event.error_message,
    )


class PostgresTransactionSink:
    def __init__(self, connection_factory: Callable[[], SupportsConnection], config: PostgresSinkConfig | None = None):
        self._connection_factory = connection_factory
        self._config = config or PostgresSinkConfig()

    def write_transactions(self, records: list[ValidatedTransaction]) -> None:
        if not records:
            return
        conn = self._connection_factory()
        try:
            cur = conn.cursor()
            cur.execute(
                f"""
                create schema if not exists treasury
                """
            )
            cur.execute(
                f"""
                create table if not exists {self._config.transaction_table} (
                    transaction_id text primary key,
                    "timestamp" timestamp not null,
                    legal_entity_id text not null,
                    currency text not null,
                    amount numeric not null,
                    direction text not null
                )
                """
            )
            cur.executemany(
                f"""
                insert into {self._config.transaction_table}
                (transaction_id, "timestamp", legal_entity_id, currency, amount, direction)
                values (%s, %s, %s, %s, %s, %s)
                on conflict (transaction_id) do nothing
                """,
                [_transaction_payload(record) for record in records],
            )
            conn.commit()
        finally:
            conn.close()


class PostgresFXSink:
    def __init__(self, connection_factory: Callable[[], SupportsConnection], config: PostgresSinkConfig | None = None):
        self._connection_factory = connection_factory
        self._config = config or PostgresSinkConfig()

    def write_fx_rates(self, records: list[ValidatedFXRate]) -> None:
        if not records:
            return
        conn = self._connection_factory()
        try:
            cur = conn.cursor()
            cur.execute(
                f"""
                create schema if not exists treasury
                """
            )
            cur.execute(
                f"""
                create table if not exists {self._config.fx_table} (
                    date date not null,
                    base_currency char(3) not null,
                    quote_currency char(3) not null,
                    fx_rate numeric(20, 10) not null,
                    primary key (date, base_currency, quote_currency)
                )
                """
            )
            cur.executemany(
                f"""
                insert into {self._config.fx_table}
                (date, base_currency, quote_currency, fx_rate)
                values (%s, %s, %s, %s)
                on conflict (date, base_currency, quote_currency) do nothing
                """,
                [_fx_payload(record) for record in records],
            )
            conn.commit()
        finally:
            conn.close()


class PostgresUSDSink:
    def __init__(self, connection_factory: Callable[[], SupportsConnection], config: PostgresSinkConfig | None = None):
        self._connection_factory = connection_factory
        self._config = config or PostgresSinkConfig()

    def write_usd_transactions(self, records: list[USDNormalizedTransaction]) -> None:
        if not records:
            return
        conn = self._connection_factory()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                create schema if not exists treasury
                """
            )
            cur.execute(
                f"""
                create table if not exists {self._config.usd_table} (
                    transaction_id text not null primary key,
                    "timestamp" timestamp not null,
                    legal_entity_id text not null,
                    currency char(3) not null,
                    amount numeric(20, 6) not null,
                    direction treasury.transaction_direction not null,
                    fx_rate_applied numeric(20, 10) not null,
                    amount_usd numeric(20, 6) not null,
                    created_at_utc timestamp not null default (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
                    constraint usd_currency_chk check (currency = upper(currency)),
                    constraint usd_amount_chk check (amount >= 0),
                    constraint usd_fx_rate_chk check (fx_rate_applied > 0),
                    constraint usd_amount_usd_chk check (amount_usd >= 0),
                    constraint usd_transaction_fk foreign key (transaction_id)
                        references treasury.transactions (transaction_id)
                        on delete cascade
                )
                """
            )
            cur.executemany(
                f"""
                insert into {self._config.usd_table}
                (transaction_id, "timestamp", legal_entity_id, currency, amount, direction, fx_rate_applied, amount_usd)
                values (%s, %s, %s, %s, %s, %s, %s, %s)
                on conflict (transaction_id) do nothing
                """,
                [_usd_payload(record) for record in records],
            )
            conn.commit()
        finally:
            conn.close()


class PostgresAuditSink:
    def __init__(self, connection_factory: Callable[[], SupportsConnection], config: PostgresSinkConfig | None = None):
        self._connection_factory = connection_factory
        self._config = config or PostgresSinkConfig()

    def write_audit_events(self, events: list[AuditEvent]) -> None:
        if not events:
            return
        conn = self._connection_factory()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                create schema if not exists treasury
                """
            )
            cur.execute(
                f"""
                create table if not exists {self._config.audit_table} (
                    event_id uuid not null primary key,
                    event_type treasury.audit_event_type not null,
                    run_id text not null,
                    pipeline_version text not null,
                    dataset_version text not null,
                    source_file text,
                    transaction_id text,
                    legal_entity_id text,
                    event_timestamp_utc timestamp not null,
                    processing_timestamp_utc timestamp not null,
                    currency char(3),
                    amount_original numeric(20, 6),
                    fx_rate_applied numeric(20, 10),
                    amount_usd numeric(20, 6),
                    direction text,
                    window_start_utc timestamp,
                    window_end_utc timestamp,
                    status treasury.audit_status not null,
                    error_code text,
                    error_message text
                )
                """
            )
            cur.executemany(
                f"""
                insert into {self._config.audit_table} (
                    event_id,
                    event_type,
                    run_id,
                    pipeline_version,
                    dataset_version,
                    source_file,
                    transaction_id,
                    legal_entity_id,
                    event_timestamp_utc,
                    processing_timestamp_utc,
                    currency,
                    amount_original,
                    fx_rate_applied,
                    amount_usd,
                    direction,
                    window_start_utc,
                    window_end_utc,
                    status,
                    error_code,
                    error_message
                ) values (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                on conflict (event_id) do nothing
                """,
                [_audit_payload(event) for event in events],
            )
            conn.commit()
        finally:
            conn.close()


class ElasticsearchAuditSink:
    def __init__(
        self,
        base_url: str,
        config: ElasticsearchSinkConfig | None = None,
        opener: Callable[[urllib_request.Request], Any] | None = None,
    ):
        self._base_url = base_url.rstrip("/")
        self._config = config or ElasticsearchSinkConfig()
        self._opener = opener or urllib_request.urlopen

    def write_audit_events(self, events: list[AuditEvent]) -> None:
        if not events:
            return

        bulk_lines: list[str] = []
        for event in events:
            bulk_lines.append(json.dumps({"index": {"_index": self._config.index_name, "_id": event.event_id}}))
            bulk_lines.append(json.dumps(_audit_event_to_json(event)))

        payload = "\n".join(bulk_lines) + "\n"
        req = urllib_request.Request(
            f"{self._base_url}/_bulk",
            data=payload.encode("utf-8"),
            headers={"Content-Type": "application/x-ndjson"},
            method="POST",
        )
        response = self._opener(req)
        body = response.read().decode("utf-8") if hasattr(response, "read") else ""
        if hasattr(response, "status") and response.status >= 400:
            raise RuntimeError(f"elasticsearch bulk write failed: {body}")
        if body:
            parsed = json.loads(body)
            if parsed.get("errors"):
                raise RuntimeError("elasticsearch bulk write reported errors")


def _audit_event_to_json(event: AuditEvent) -> dict[str, Any]:
    return {
        "event_id": event.event_id,
        "event_type": event.event_type,
        "run_id": event.run_id,
        "pipeline_version": event.pipeline_version,
        "dataset_version": event.dataset_version,
        "source_file": event.source_file,
        "transaction_id": event.transaction_id,
        "legal_entity_id": event.legal_entity_id,
        "event_timestamp_utc": event.event_timestamp_utc.isoformat(),
        "processing_timestamp_utc": event.processing_timestamp_utc.isoformat(),
        "currency": event.currency,
        "amount_original": str(event.amount_original) if event.amount_original is not None else None,
        "fx_rate_applied": str(event.fx_rate_applied) if event.fx_rate_applied is not None else None,
        "amount_usd": str(event.amount_usd) if event.amount_usd is not None else None,
        "direction": event.direction,
        "window_start_utc": event.window_start_utc.isoformat() if event.window_start_utc else None,
        "window_end_utc": event.window_end_utc.isoformat() if event.window_end_utc else None,
        "status": event.status,
        "error_code": event.error_code,
        "error_message": event.error_message,
    }
