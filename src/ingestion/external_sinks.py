from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
import json
from typing import Any, Callable, Protocol, TYPE_CHECKING
from urllib import error as urllib_error
from urllib import request as urllib_request

from .audit import AuditEvent
from .validator import ValidatedFXRate, ValidatedTransaction

if TYPE_CHECKING:
    from src.fx.conversion import USDNormalizedTransaction
    from src.liquidity.window import LiquiditySnapshot


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
    liquidity_table: str = "treasury.liquidity_snapshots"
    fx_table: str = "treasury.fx_rates"
    audit_table: str = "treasury.audit_events"


@dataclass(frozen=True)
class ElasticsearchSinkConfig:
    index_name: str = "treasury_audit_logs"
    failure_log_path: Path = Path("logs/elasticsearch_audit_failures.jsonl")


@dataclass(frozen=True)
class ElasticsearchWriteAcknowledgment:
    run_id: str
    target_index: str
    document_count: int
    write_status: str
    committed_at_utc: datetime | None = None
    failed_document_ids: tuple[str, ...] = ()
    error_message: str | None = None


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


def _liquidity_payload(record: LiquiditySnapshot) -> tuple[Any, ...]:
    window_start_utc = record.window_start_utc.astimezone(timezone.utc).replace(tzinfo=None)
    window_end_utc = record.window_end_utc.astimezone(timezone.utc).replace(tzinfo=None)
    return (
        record.snapshot_date,
        record.legal_entity_id,
        window_start_utc,
        window_end_utc,
        record.currency,
        record.transaction_count,
        record.inbound_count,
        record.outbound_count,
        record.total_inbound_usd,
        record.total_outbound_usd,
        record.net_liquidity_usd,
        record.run_id,
        record.pipeline_version,
        record.dataset_version,
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


def _serialize_audit_event(event: AuditEvent) -> dict[str, Any]:
    def _decimal(value: Decimal | None) -> str | None:
        return str(value) if value is not None else None

    return {
        "event_id": event.event_id,
        "event_type": event.event_type,
        "run_id": event.run_id,
        "pipeline_version": event.pipeline_version,
        "dataset_version": event.dataset_version,
        "source_file": event.source_file,
        "transaction_id": event.transaction_id,
        "legal_entity_id": event.legal_entity_id,
        "event_timestamp_utc": event.event_timestamp_utc.astimezone(timezone.utc).isoformat(),
        "processing_timestamp_utc": event.processing_timestamp_utc.astimezone(timezone.utc).isoformat(),
        "currency": event.currency,
        "amount_original": _decimal(event.amount_original),
        "fx_rate_applied": _decimal(event.fx_rate_applied),
        "amount_usd": _decimal(event.amount_usd),
        "direction": event.direction,
        "window_start_utc": event.window_start_utc.astimezone(timezone.utc).isoformat() if event.window_start_utc else None,
        "window_end_utc": event.window_end_utc.astimezone(timezone.utc).isoformat() if event.window_end_utc else None,
        "status": event.status,
        "error_code": event.error_code,
        "error_message": event.error_message,
    }


def _es_request_json(
    opener: Callable[[urllib_request.Request], Any],
    method: str,
    url: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    body = None
    headers = {"Content-Type": "application/json"}
    if payload is not None:
        body = json.dumps(payload, sort_keys=True).encode("utf-8")

    req = urllib_request.Request(url, data=body, headers=headers, method=method)
    try:
        response = opener(req)
        raw = response.read().decode("utf-8") if hasattr(response, "read") else ""
        if hasattr(response, "status") and response.status >= 400:
            raise RuntimeError(f"{method} {url} failed with status {response.status}: {raw}")
        return json.loads(raw) if raw else {}
    except Exception as exc:
        if isinstance(exc, urllib_error.HTTPError):
            raw = exc.read().decode("utf-8")
            raise RuntimeError(f"{method} {url} failed with status {exc.code}: {raw}") from exc
        raise


def _append_failure_record(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True, default=str))
        handle.write("\n")


def _audit_index_mapping() -> dict[str, Any]:
    return {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
        },
        "mappings": {
            "dynamic": "strict",
            "properties": {
                "event_id": {"type": "keyword"},
                "event_type": {"type": "keyword"},
                "run_id": {"type": "keyword"},
                "pipeline_version": {"type": "keyword"},
                "dataset_version": {"type": "keyword"},
                "source_file": {"type": "keyword"},
                "transaction_id": {"type": "keyword"},
                "legal_entity_id": {"type": "keyword"},
                "event_timestamp_utc": {"type": "date"},
                "processing_timestamp_utc": {"type": "date"},
                "currency": {"type": "keyword"},
                "amount_original": {"type": "keyword"},
                "fx_rate_applied": {"type": "keyword"},
                "amount_usd": {"type": "keyword"},
                "direction": {"type": "keyword"},
                "window_start_utc": {"type": "date"},
                "window_end_utc": {"type": "date"},
                "status": {"type": "keyword"},
                "error_code": {"type": "keyword"},
                "error_message": {"type": "text"},
            },
        },
    }


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


class PostgresLiquiditySink:
    def __init__(self, connection_factory: Callable[[], SupportsConnection], config: PostgresSinkConfig | None = None):
        self._connection_factory = connection_factory
        self._config = config or PostgresSinkConfig()

    def write_liquidity_snapshots(self, records: list[LiquiditySnapshot]) -> None:
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
                create table if not exists {self._config.liquidity_table} (
                    snapshot_date date not null,
                    legal_entity_id text not null,
                    window_start_utc timestamp not null,
                    window_end_utc timestamp not null,
                    currency char(3) not null default 'USD',
                    transaction_count bigint not null,
                    inbound_count bigint not null,
                    outbound_count bigint not null,
                    total_inbound_usd numeric(20, 6) not null,
                    total_outbound_usd numeric(20, 6) not null,
                    net_liquidity_usd numeric(20, 6) not null,
                    run_id text not null,
                    pipeline_version text not null,
                    dataset_version text not null,
                    created_at_utc timestamp not null default (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
                    constraint liquidity_snapshots_currency_chk check (currency = 'USD'),
                    constraint liquidity_snapshots_window_chk check (window_end_utc >= window_start_utc),
                    primary key (snapshot_date, legal_entity_id, run_id)
                )
                """
            )
            cur.executemany(
                f"""
                insert into {self._config.liquidity_table} (
                    snapshot_date,
                    legal_entity_id,
                    window_start_utc,
                    window_end_utc,
                    currency,
                    transaction_count,
                    inbound_count,
                    outbound_count,
                    total_inbound_usd,
                    total_outbound_usd,
                    net_liquidity_usd,
                    run_id,
                    pipeline_version,
                    dataset_version
                ) values (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                on conflict (snapshot_date, legal_entity_id, run_id) do nothing
                """,
                [_liquidity_payload(record) for record in records],
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
        self._index_ready = False

    def _ensure_index(self) -> None:
        if self._index_ready:
            return

        index_url = f"{self._base_url}/{self._config.index_name}"
        try:
            _es_request_json(self._opener, "HEAD", index_url)
        except RuntimeError as exc:
            if "status 404" not in str(exc):
                raise
            _es_request_json(self._opener, "PUT", index_url, _audit_index_mapping())
        self._index_ready = True

    def _log_failure(self, ack: ElasticsearchWriteAcknowledgment, events: list[AuditEvent], reason: str) -> None:
        _append_failure_record(
            self._config.failure_log_path,
            {
                "captured_at_utc": _utc_now().isoformat(),
                "document_count": ack.document_count,
                "error_message": reason,
                "failed_document_ids": list(ack.failed_document_ids),
                "run_id": ack.run_id,
                "target_index": ack.target_index,
                "write_status": ack.write_status,
                "event_ids": [event.event_id for event in events],
            },
        )

    def write_audit_events(self, events: list[AuditEvent]) -> None:
        if not events:
            return

        self._ensure_index()

        bulk_lines: list[str] = []
        for event in events:
            bulk_lines.append(json.dumps({"index": {"_index": self._config.index_name, "_id": event.event_id}}, sort_keys=True))
            bulk_lines.append(json.dumps(_serialize_audit_event(event), sort_keys=True))

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
            ack = ElasticsearchWriteAcknowledgment(
                run_id=events[0].run_id,
                target_index=self._config.index_name,
                document_count=len(events),
                write_status="FAILED",
                error_message=body or f"elasticsearch bulk write failed with status {response.status}",
            )
            self._log_failure(ack, events, ack.error_message or "elasticsearch bulk write failed")
            raise RuntimeError(ack.error_message)

        parsed = json.loads(body) if body else {}
        items = parsed.get("items", [])
        failed_document_ids: list[str] = []
        for item in items:
            action = item.get("index") or item.get("create") or item.get("update")
            if not isinstance(action, dict):
                continue
            status = int(action.get("status", 0) or 0)
            if status >= 300:
                failed_document_ids.append(str(action.get("_id", "")))

        item_count_matches = not items or len(items) == len(events)
        write_success = not parsed.get("errors") and not failed_document_ids and item_count_matches
        ack = ElasticsearchWriteAcknowledgment(
            run_id=events[0].run_id,
            target_index=self._config.index_name,
            document_count=len(events),
            write_status="SUCCESS" if write_success else ("PARTIAL" if items else "FAILED"),
            committed_at_utc=_utc_now() if write_success else None,
            failed_document_ids=tuple(doc_id for doc_id in failed_document_ids if doc_id),
            error_message=None if write_success else "elasticsearch bulk write reported errors",
        )

        if ack.write_status != "SUCCESS":
            self._log_failure(ack, events, ack.error_message or "elasticsearch bulk write failed")
            raise RuntimeError(ack.error_message or "elasticsearch bulk write failed")
