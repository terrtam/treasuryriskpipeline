from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.ingestion import (
    AuditEvent,
    InMemoryAuditSink,
    InMemoryFXSink,
    InMemoryLiquiditySink,
    InMemoryUSDSink,
    InMemoryTransactionSink,
    IngestionSinks,
    ingest_fx_files,
    ingest_transaction_files,
    load_parquet_rows,
    validate_fx_batch,
    validate_transaction_batch,
)
from src.ingestion.batch import IngestionBatchConfig, run_ingestion_batch
from src.ingestion.cli import build_production_sinks
from src.ingestion.external_sinks import (
    ElasticsearchAuditSink,
    ElasticsearchSinkConfig,
    PostgresAuditSink,
    PostgresFXSink,
    PostgresLiquiditySink,
    PostgresTransactionSink,
)


class RecordingCursor:
    def __init__(self):
        self.statements = []

    def execute(self, query, params=None):
        self.statements.append((query, params))

    def executemany(self, query, params):
        self.statements.append((query, list(params)))


class RecordingConnection:
    def __init__(self):
        self.cursor_obj = RecordingCursor()
        self.committed = False
        self.closed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


class ElasticsearchResponse:
    def __init__(self, status: int, body: dict | str | None = None):
        self.status = status
        if body is None:
            self._body = b""
        elif isinstance(body, str):
            self._body = body.encode("utf-8")
        else:
            self._body = json.dumps(body).encode("utf-8")

    def read(self):
        return self._body


def test_fx_duplicate_key_rejection():
    valid_rows, rejections = validate_fx_batch(
        [
            {
                "date": date(2026, 6, 7),
                "base_currency": "USD",
                "quote_currency": "EUR",
                "fx_rate": "1.10",
            },
            {
                "date": date(2026, 6, 7),
                "base_currency": "USD",
                "quote_currency": "EUR",
                "fx_rate": "1.11",
            },
        ]
    )

    assert valid_rows == []
    assert len(rejections.rejections) == 2
    assert all("duplicate" in rejection.reason for rejection in rejections.rejections)


def test_fx_rejects_non_usd_base_currency():
    valid_rows, rejections = validate_fx_batch(
        [
            {
                "date": date(2026, 6, 7),
                "base_currency": "CAD",
                "quote_currency": "EUR",
                "fx_rate": "1.10",
            }
        ]
    )

    assert valid_rows == []
    assert len(rejections.rejections) == 1
    assert "USD" in rejections.rejections[0].reason


def test_load_parquet_rows_reads_valid_file(tmp_path):
    path = tmp_path / "daily_transactions_001.parquet"
    table = pa.table(
        {
            "transaction_id": ["T1"],
            "timestamp": ["2026-06-07T10:00:00Z"],
            "legal_entity_id": ["LE1"],
            "currency": ["USD"],
            "amount": [Decimal("10.00")],
            "direction": ["INBOUND"],
        }
    )
    pq.write_table(table, path)

    rows = load_parquet_rows(path)

    assert rows[0]["transaction_id"] == "T1"
    assert rows[0]["amount"] == Decimal("10.00")


def test_ingest_transaction_files_records_file_errors(tmp_path):
    good_path = tmp_path / "daily_transactions_001.parquet"
    bad_path = tmp_path / "daily_transactions_002.parquet"
    pq.write_table(
        pa.table(
            {
                "transaction_id": ["T1"],
                "timestamp": ["2026-06-07T10:00:00Z"],
                "legal_entity_id": ["LE1"],
                "currency": ["USD"],
                "amount": [Decimal("10.00")],
                "direction": ["INBOUND"],
            }
        ),
        good_path,
    )
    bad_path.write_text("not parquet", encoding="utf-8")

    result = ingest_transaction_files(tmp_path)

    assert len(result.records) == 1
    assert result.records[0].transaction_id == "T1"
    assert result.file_errors
    assert result.status == "FAILED"


def test_ingest_transaction_files_ignores_transaction_error_files(tmp_path):
    transaction_path = tmp_path / "daily_transactions_20260607.parquet"
    transaction_error_path = tmp_path / "daily_transactions_errors_20260607.parquet"

    pq.write_table(
        pa.table(
            {
                "transaction_id": ["T1"],
                "timestamp": ["2026-06-07T10:00:00Z"],
                "legal_entity_id": ["LE1"],
                "currency": ["USD"],
                "amount": [Decimal("10.00")],
                "direction": ["INBOUND"],
            }
        ),
        transaction_path,
    )
    pq.write_table(
        pa.table(
            {
                "transaction_id": [None],
                "timestamp": [None],
                "legal_entity_id": [None],
                "currency": [None],
                "amount": [None],
                "direction": [None],
            }
        ),
        transaction_error_path,
    )

    result = ingest_transaction_files(tmp_path)

    assert len(result.records) == 1
    assert result.records[0].transaction_id == "T1"
    assert result.rejections.rejections == []
    assert result.status == "SUCCESS"


def test_transaction_duplicate_across_files_is_rejected(tmp_path):
    first_path = tmp_path / "daily_transactions_001.parquet"
    second_path = tmp_path / "daily_transactions_002.parquet"
    row = {
        "transaction_id": "T1",
        "timestamp": "2026-06-07T10:00:00Z",
        "legal_entity_id": "LE1",
        "currency": "USD",
        "amount": Decimal("10.00"),
        "direction": "INBOUND",
    }
    pq.write_table(pa.table({key: [value] for key, value in row.items()}), first_path)
    pq.write_table(pa.table({key: [value] for key, value in row.items()}), second_path)

    result = ingest_transaction_files(tmp_path)

    assert result.records == []
    assert len(result.rejections.rejections) == 2
    assert all("duplicate" in rejection.reason for rejection in result.rejections.rejections)
    assert result.status == "FAILED"


def test_run_ingestion_batch_writes_records_and_audit_events(tmp_path):
    transaction_path = tmp_path / "daily_transactions_001.parquet"
    fx_path = tmp_path / "fx_rates_001.parquet"

    pq.write_table(
        pa.table(
            {
                "transaction_id": ["T1"],
                "timestamp": ["2026-06-07T10:00:00Z"],
                "legal_entity_id": ["LE1"],
                "currency": ["USD"],
                "amount": [Decimal("10.00")],
                "direction": ["INBOUND"],
            }
        ),
        transaction_path,
    )
    pq.write_table(
        pa.table(
            {
                "date": [date(2026, 6, 7)],
                "base_currency": ["USD"],
                "quote_currency": ["USD"],
                "fx_rate": [Decimal("1.00")],
            }
        ),
        fx_path,
    )

    transaction_sink = InMemoryTransactionSink()
    usd_sink = InMemoryUSDSink()
    liquidity_sink = InMemoryLiquiditySink()
    fx_sink = InMemoryFXSink()
    audit_sink = InMemoryAuditSink()
    sinks = IngestionSinks(
        transactions=transaction_sink,
        usd=usd_sink,
        liquidity=liquidity_sink,
        fx_rates=fx_sink,
        audit=audit_sink,
    )

    result = run_ingestion_batch(
        IngestionBatchConfig(
            data_feeds_dir=tmp_path,
            run_id="run-001",
            pipeline_version="1.0.0",
            dataset_version="2026-06-07",
            processing_timestamp_utc=datetime(2026, 6, 7, 12, 0, tzinfo=timezone.utc),
        ),
        sinks=sinks,
    )

    assert result.transaction_batch.status == "SUCCESS"
    assert result.fx_batch.status == "SUCCESS"
    assert result.usd_batch.status == "SUCCESS"
    assert result.liquidity_batch.status == "SUCCESS"
    assert len(transaction_sink.records) == 1
    assert len(usd_sink.records) == 1
    assert usd_sink.records[0].amount_usd == Decimal("10.000000")
    assert len(liquidity_sink.records) == 1
    assert liquidity_sink.records[0].net_liquidity_usd == Decimal("10.000000")
    assert len(fx_sink.records) == 1
    assert len(result.audit_events) == 2
    assert result.audit_events[0].event_type == "transaction_processed"
    assert result.audit_events[0].transaction_id == "T1"
    assert result.audit_events[1].event_type == "snapshot_written"
    assert result.audit_events[1].legal_entity_id == "LE1"
    assert audit_sink.events == result.audit_events
    assert any(write.sink_name == "liquidity" and write.status == "SUCCESS" for write in result.sink_writes)
    assert all(write.status == "SUCCESS" for write in result.sink_writes)


def test_run_ingestion_batch_filters_by_business_date(tmp_path):
    day_one_txn = tmp_path / "daily_transactions_20260607.parquet"
    day_two_txn = tmp_path / "daily_transactions_20260608.parquet"
    day_one_fx = tmp_path / "fx_rates_20260607.parquet"
    day_two_fx = tmp_path / "fx_rates_20260608.parquet"

    pq.write_table(
        pa.table(
            {
                "transaction_id": ["T1"],
                "timestamp": ["2026-06-07T10:00:00Z"],
                "legal_entity_id": ["LE1"],
                "currency": ["USD"],
                "amount": [Decimal("10.00")],
                "direction": ["INBOUND"],
            }
        ),
        day_one_txn,
    )
    pq.write_table(
        pa.table(
            {
                "transaction_id": ["T2"],
                "timestamp": ["2026-06-08T10:00:00Z"],
                "legal_entity_id": ["LE1"],
                "currency": ["USD"],
                "amount": [Decimal("20.00")],
                "direction": ["INBOUND"],
            }
        ),
        day_two_txn,
    )
    pq.write_table(
        pa.table(
            {
                "date": [date(2026, 6, 7)],
                "base_currency": ["USD"],
                "quote_currency": ["USD"],
                "fx_rate": [Decimal("1.00")],
            }
        ),
        day_one_fx,
    )
    pq.write_table(
        pa.table(
            {
                "date": [date(2026, 6, 8)],
                "base_currency": ["USD"],
                "quote_currency": ["USD"],
                "fx_rate": [Decimal("1.00")],
            }
        ),
        day_two_fx,
    )

    sinks = IngestionSinks(
        transactions=InMemoryTransactionSink(),
        usd=InMemoryUSDSink(),
        liquidity=InMemoryLiquiditySink(),
        fx_rates=InMemoryFXSink(),
        audit=InMemoryAuditSink(),
    )
    result = run_ingestion_batch(
        IngestionBatchConfig(
            data_feeds_dir=tmp_path,
            run_id="run-002",
            pipeline_version="1.0.0",
            dataset_version="demo-test",
            business_date=date(2026, 6, 7),
        ),
        sinks=sinks,
    )

    assert [row.transaction_id for row in sinks.transactions.records] == ["T1"]
    assert [row.transaction_id for row in sinks.usd.records] == ["T1"]
    assert all(snapshot.snapshot_date == date(2026, 6, 7) for snapshot in sinks.liquidity.records)
    assert result.transaction_batch.status == "SUCCESS"
    assert result.fx_batch.status == "SUCCESS"


def test_run_ingestion_batch_includes_transaction_error_audit_for_generated_0423_feed():
    result = run_ingestion_batch(
        IngestionBatchConfig(
            data_feeds_dir=Path("data_feeds"),
            run_id="run-0423",
            pipeline_version="1.0.0",
            dataset_version="demo-2026-01-01-v1",
            business_date=date(2026, 4, 23),
        ),
        sinks=IngestionSinks(
            transactions=InMemoryTransactionSink(),
            usd=InMemoryUSDSink(),
            liquidity=InMemoryLiquiditySink(),
            fx_rates=InMemoryFXSink(),
            audit=InMemoryAuditSink(),
        ),
    )

    assert len(result.transaction_batch.records) == 8
    assert len(result.transaction_batch.rejections.rejections) == 0
    assert len(result.liquidity_batch.records) == 7
    assert len(result.audit_events) == 16
    assert sum(1 for event in result.audit_events if event.event_type == "transaction_processed") == 8
    assert sum(1 for event in result.audit_events if event.event_type == "transaction_rejected") == 1
    assert sum(1 for event in result.audit_events if event.event_type == "snapshot_written") == 7
    assert any(event.transaction_id == "TX-20260423-0000036" and event.status == "REJECTED" for event in result.audit_events)


def test_run_ingestion_batch_supports_single_transaction_file(tmp_path):
    transaction_path = tmp_path / "daily_transactions_001.parquet"
    pq.write_table(
        pa.table(
            {
                "transaction_id": ["T1"],
                "timestamp": ["2026-06-07T10:00:00Z"],
                "legal_entity_id": ["LE1"],
                "currency": ["USD"],
                "amount": [Decimal("10.00")],
                "direction": ["INBOUND"],
            }
        ),
        transaction_path,
    )

    transaction_sink = InMemoryTransactionSink()
    usd_sink = InMemoryUSDSink()
    liquidity_sink = InMemoryLiquiditySink()
    fx_sink = InMemoryFXSink()
    audit_sink = InMemoryAuditSink()

    result = run_ingestion_batch(
        IngestionBatchConfig(
            data_feeds_dir=transaction_path,
            run_id="run-002",
            pipeline_version="1.0.0",
            dataset_version="2026-06-07",
            processing_timestamp_utc=datetime(2026, 6, 7, 12, 0, tzinfo=timezone.utc),
        ),
        sinks=IngestionSinks(
            transactions=transaction_sink,
            usd=usd_sink,
            liquidity=liquidity_sink,
            fx_rates=fx_sink,
            audit=audit_sink,
        ),
    )

    assert result.transaction_batch.status == "SUCCESS"
    assert result.fx_batch.status == "SKIPPED"
    assert result.usd_batch.status == "SKIPPED"
    assert len(transaction_sink.records) == 1
    assert len(usd_sink.records) == 0
    assert len(liquidity_sink.records) == 0
    assert len(fx_sink.records) == 0
    assert len(audit_sink.events) == 1
    assert audit_sink.events[0].event_type == "transaction_processed"
    assert audit_sink.events[0].transaction_id == "T1"
    assert len(result.sink_writes) == 2


def test_postgres_liquidity_sink_records_payloads():
    connection = RecordingConnection()
    sink = PostgresLiquiditySink(lambda: connection)

    sink.write_liquidity_snapshots(
        [
            type(
                "Liquidity",
                (),
                {
                    "snapshot_date": date(2026, 6, 7),
                    "legal_entity_id": "LE1",
                    "window_start_utc": datetime(2026, 5, 9, 0, 0, tzinfo=timezone.utc),
                    "window_end_utc": datetime(2026, 6, 7, 23, 59, 59, 999999, tzinfo=timezone.utc),
                    "currency": "USD",
                    "transaction_count": 2,
                    "inbound_count": 1,
                    "outbound_count": 1,
                    "total_inbound_usd": Decimal("10.00"),
                    "total_outbound_usd": Decimal("4.00"),
                    "net_liquidity_usd": Decimal("6.00"),
                    "run_id": "run-1",
                    "pipeline_version": "1.0.0",
                    "dataset_version": "2026-06-07",
                },
            )()
        ]
    )

    assert connection.committed is True
    assert connection.closed is True
    assert any("treasury.liquidity_snapshots" in statement[0] for statement in connection.cursor_obj.statements)


def test_postgres_and_elasticsearch_sink_adapters_record_payloads(tmp_path):
    connection = RecordingConnection()
    txn_sink = PostgresTransactionSink(lambda: connection)
    fx_sink = PostgresFXSink(lambda: connection)
    audit_pg_sink = PostgresAuditSink(lambda: connection)

    txn_sink.write_transactions(
        [
            type(
                "Txn",
                (),
                {
                    "transaction_id": "T1",
                    "timestamp": datetime(2026, 6, 7, 10, 0, tzinfo=timezone.utc),
                    "legal_entity_id": "LE1",
                    "currency": "USD",
                    "amount": Decimal("10.00"),
                    "direction": "INBOUND",
                },
            )()
        ]
    )

    assert connection.committed is True
    assert connection.closed is True
    assert any("treasury.transactions" in statement[0] for statement in connection.cursor_obj.statements)
    assert any("insert into" in statement[0].lower() for statement in connection.cursor_obj.statements)

    connection = RecordingConnection()
    audit_pg_sink = PostgresAuditSink(lambda: connection)
    audit_pg_sink.write_audit_events(
        [
            AuditEvent(
                event_id="adce2db2-88ca-54a9-b7ec-4df0dcb33cb1",
                event_type="transaction_rejected",
                run_id="run-1",
                pipeline_version="1.0.0",
                dataset_version="2026-06-07",
                source_file=None,
                transaction_id="T1",
                legal_entity_id="LE1",
                event_timestamp_utc=datetime(2026, 6, 7, 10, 0, tzinfo=timezone.utc),
                processing_timestamp_utc=datetime(2026, 6, 7, 12, 0, tzinfo=timezone.utc),
                currency="USD",
                amount_original=Decimal("10.00"),
                fx_rate_applied=None,
                amount_usd=None,
                direction="INBOUND",
                window_start_utc=None,
                window_end_utc=None,
                status="REJECTED",
                error_code="INGESTION_REJECTION",
                error_message="bad row",
            )
        ]
    )

    assert any("treasury.audit_events" in statement[0] for statement in connection.cursor_obj.statements)

    es_requests = []

    def opener(req):
        es_requests.append(req)
        if req.method in {"HEAD", "PUT"}:
            if req.method == "HEAD":
                return ElasticsearchResponse(404)
            return ElasticsearchResponse(200, {"acknowledged": True})

        return ElasticsearchResponse(
            200,
            {
                "errors": False,
                "items": [
                    {"index": {"status": 201, "_id": "event-1"}},
                ],
            },
        )

    audit_sink = ElasticsearchAuditSink(
        "http://localhost:9200",
        config=ElasticsearchSinkConfig(failure_log_path=tmp_path / "test-elasticsearch-failures.jsonl"),
        opener=opener,
    )
    audit_sink.write_audit_events(
        [
            type(
                "Event",
                (),
                {
                    "event_id": "event-1",
                    "event_type": "transaction_rejected",
                    "run_id": "run-1",
                    "pipeline_version": "1.0.0",
                    "dataset_version": "2026-06-07",
                    "source_file": None,
                    "transaction_id": "T1",
                    "legal_entity_id": "LE1",
                    "event_timestamp_utc": datetime(2026, 6, 7, 10, 0, tzinfo=timezone.utc),
                    "processing_timestamp_utc": datetime(2026, 6, 7, 12, 0, tzinfo=timezone.utc),
                    "currency": "USD",
                    "amount_original": Decimal("10.00"),
                    "fx_rate_applied": None,
                    "amount_usd": None,
                    "direction": "INBOUND",
                    "window_start_utc": None,
                    "window_end_utc": None,
                    "status": "REJECTED",
                    "error_code": "INGESTION_REJECTION",
                    "error_message": "bad row",
                },
            )()
        ]
    )

    assert es_requests
    assert es_requests[0].method == "HEAD"
    assert es_requests[1].method == "PUT"
    assert es_requests[2].method == "POST"
    bulk_payload = es_requests[2].data.decode("utf-8").strip().splitlines()
    assert bulk_payload[0] == json.dumps({"index": {"_id": "event-1", "_index": "treasury_audit_logs"}}, sort_keys=True)
    assert json.loads(bulk_payload[1])["event_id"] == "event-1"
    assert not (tmp_path / "test-elasticsearch-failures.jsonl").exists()


def test_elasticsearch_sink_logs_partial_failures(tmp_path):
    failure_log_path = tmp_path / "es" / "audit-failures.jsonl"
    es_requests = []

    def opener(req):
        es_requests.append(req)
        if req.method == "HEAD":
            return ElasticsearchResponse(404)
        if req.method == "PUT":
            return ElasticsearchResponse(200, {"acknowledged": True})
        return ElasticsearchResponse(
            200,
            {
                "errors": True,
                "items": [
                    {
                        "index": {
                            "status": 409,
                            "_id": "event-1",
                            "error": {"type": "version_conflict_engine_exception", "reason": "conflict"},
                        }
                    }
                ],
            },
        )

    sink = ElasticsearchAuditSink(
        "http://localhost:9200",
        config=ElasticsearchSinkConfig(failure_log_path=failure_log_path),
        opener=opener,
    )

    with pytest.raises(RuntimeError, match="elasticsearch bulk write reported errors"):
        sink.write_audit_events(
            [
                type(
                    "Event",
                    (),
                    {
                        "event_id": "event-1",
                        "event_type": "transaction_rejected",
                        "run_id": "run-1",
                        "pipeline_version": "1.0.0",
                        "dataset_version": "2026-06-07",
                        "source_file": None,
                        "transaction_id": "T1",
                        "legal_entity_id": "LE1",
                        "event_timestamp_utc": datetime(2026, 6, 7, 10, 0, tzinfo=timezone.utc),
                        "processing_timestamp_utc": datetime(2026, 6, 7, 12, 0, tzinfo=timezone.utc),
                        "currency": "USD",
                        "amount_original": Decimal("10.00"),
                        "fx_rate_applied": None,
                        "amount_usd": None,
                        "direction": "INBOUND",
                        "window_start_utc": None,
                        "window_end_utc": None,
                        "status": "REJECTED",
                        "error_code": "INGESTION_REJECTION",
                        "error_message": "bad row",
                    },
                )()
            ]
        )

    assert es_requests[0].method == "HEAD"
    assert es_requests[1].method == "PUT"
    assert es_requests[2].method == "POST"
    assert failure_log_path.exists()
    logged = json.loads(failure_log_path.read_text(encoding="utf-8").strip())
    assert logged["write_status"] == "PARTIAL"
    assert logged["target_index"] == "treasury_audit_logs"
    assert logged["event_ids"] == ["event-1"]


def test_transaction_validation_rejects_invalid_direction():
    valid_rows, rejections = validate_transaction_batch(
        [
            {
                "transaction_id": "T1",
                "timestamp": "2026-06-07T10:00:00Z",
                "legal_entity_id": "LE1",
                "currency": "USD",
                "amount": "10.00",
                "direction": "IN",
            }
        ]
    )

    assert valid_rows == []
    assert len(rejections.rejections) == 1
    assert "INBOUND or OUTBOUND" in rejections.rejections[0].reason
