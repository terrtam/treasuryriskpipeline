from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, TYPE_CHECKING

from .audit import AuditEvent
from .external_sinks import (
    ElasticsearchAuditSink,
    ElasticsearchSinkConfig,
    PostgresAuditSink,
    PostgresFXSink,
    PostgresSinkConfig,
    PostgresUSDSink,
    PostgresTransactionSink,
)
from .validator import ValidatedFXRate, ValidatedTransaction

if TYPE_CHECKING:
    from src.fx.conversion import USDNormalizedTransaction


class TransactionRecordSink(Protocol):
    def write_transactions(self, records: list[ValidatedTransaction]) -> None: ...


class FXRecordSink(Protocol):
    def write_fx_rates(self, records: list[ValidatedFXRate]) -> None: ...


class USDRecordSink(Protocol):
    def write_usd_transactions(self, records: list[USDNormalizedTransaction]) -> None: ...


class AuditEventSink(Protocol):
    def write_audit_events(self, events: list[AuditEvent]) -> None: ...


@dataclass
class InMemoryTransactionSink:
    records: list[ValidatedTransaction] = field(default_factory=list)

    def write_transactions(self, records: list[ValidatedTransaction]) -> None:
        self.records.extend(records)


@dataclass
class InMemoryFXSink:
    records: list[ValidatedFXRate] = field(default_factory=list)

    def write_fx_rates(self, records: list[ValidatedFXRate]) -> None:
        self.records.extend(records)


@dataclass
class InMemoryUSDSink:
    records: list[USDNormalizedTransaction] = field(default_factory=list)

    def write_usd_transactions(self, records: list[USDNormalizedTransaction]) -> None:
        self.records.extend(records)


@dataclass
class InMemoryAuditSink:
    events: list[AuditEvent] = field(default_factory=list)

    def write_audit_events(self, events: list[AuditEvent]) -> None:
        self.events.extend(events)


@dataclass
class CompositeAuditSink:
    sinks: list[AuditEventSink]

    def write_audit_events(self, events: list[AuditEvent]) -> None:
        errors: list[Exception] = []
        for sink in self.sinks:
            try:
                sink.write_audit_events(events)
            except Exception as exc:  # pragma: no cover - exercised in integration-style tests
                errors.append(exc)
        if errors:
            raise RuntimeError("one or more audit sinks failed") from errors[0]


@dataclass(frozen=True)
class IngestionSinks:
    transactions: TransactionRecordSink
    usd: USDRecordSink
    fx_rates: FXRecordSink
    audit: AuditEventSink


def create_default_ingestion_sinks() -> IngestionSinks:
    return IngestionSinks(
        transactions=InMemoryTransactionSink(),
        usd=InMemoryUSDSink(),
        fx_rates=InMemoryFXSink(),
        audit=InMemoryAuditSink(),
    )


def create_postgres_ingestion_sinks(
    transaction_connection_factory,
    audit_elasticsearch_base_url: str,
    *,
    postgres_config: PostgresSinkConfig | None = None,
    elasticsearch_config: ElasticsearchSinkConfig | None = None,
):
    transaction_sink = PostgresTransactionSink(transaction_connection_factory, config=postgres_config)
    usd_sink = PostgresUSDSink(transaction_connection_factory, config=postgres_config)
    fx_sink = PostgresFXSink(transaction_connection_factory, config=postgres_config)
    postgres_audit_sink = PostgresAuditSink(transaction_connection_factory, config=postgres_config)
    elasticsearch_audit_sink = ElasticsearchAuditSink(audit_elasticsearch_base_url, config=elasticsearch_config)
    audit_sink = CompositeAuditSink([postgres_audit_sink, elasticsearch_audit_sink])
    return IngestionSinks(transactions=transaction_sink, usd=usd_sink, fx_rates=fx_sink, audit=audit_sink)
