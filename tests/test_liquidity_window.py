from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import random

from src.fx.conversion import USDNormalizedTransaction
from src.liquidity import aggregate_liquidity_snapshots, compute_liquidity_snapshots


def _usd_txn(
    transaction_id: str,
    timestamp: datetime,
    legal_entity_id: str,
    amount_usd: str,
    direction: str,
) -> USDNormalizedTransaction:
    return USDNormalizedTransaction(
        transaction_id=transaction_id,
        timestamp=timestamp,
        legal_entity_id=legal_entity_id,
        currency="EUR",
        amount=Decimal("100.00"),
        direction=direction,
        fx_rate_applied=Decimal("1.10"),
        amount_usd=Decimal(amount_usd),
    )


def test_rolling_window_excludes_data_outside_30_days():
    base = datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)
    transactions = [
        _usd_txn("T1", base, "LE1", "10.00", "INBOUND"),
        _usd_txn("T2", base + timedelta(days=29), "LE1", "20.00", "OUTBOUND"),
        _usd_txn("T3", base + timedelta(days=30), "LE1", "30.00", "INBOUND"),
        _usd_txn("T4", base + timedelta(days=34), "LE1", "40.00", "INBOUND"),
    ]

    snapshots = compute_liquidity_snapshots(
        transactions,
        run_id="run-001",
        pipeline_version="1.0.0",
        dataset_version="2026-06-01",
    )

    day_30_snapshot = next(row for row in snapshots if row.snapshot_date == (base + timedelta(days=29)).date())
    day_35_snapshot = next(row for row in snapshots if row.snapshot_date == (base + timedelta(days=34)).date())

    assert day_30_snapshot.transaction_count == 2
    assert day_30_snapshot.total_inbound_usd == Decimal("10.00")
    assert day_30_snapshot.total_outbound_usd == Decimal("20.00")
    assert day_30_snapshot.net_liquidity_usd == Decimal("-10.00")
    assert day_35_snapshot.transaction_count == 3
    assert day_35_snapshot.window_start_utc == datetime(2026, 6, 6, 0, 0, tzinfo=timezone.utc)
    assert day_35_snapshot.total_inbound_usd == Decimal("70.00")
    assert day_35_snapshot.total_outbound_usd == Decimal("20.00")
    assert day_35_snapshot.net_liquidity_usd == Decimal("50.00")


def test_snapshot_window_uses_available_history_when_under_30_days():
    base = datetime(2026, 6, 10, 8, 0, tzinfo=timezone.utc)
    transactions = [
        _usd_txn("T1", base, "LE1", "15.00", "INBOUND"),
        _usd_txn("T2", base + timedelta(days=4), "LE1", "5.00", "OUTBOUND"),
    ]

    snapshots = compute_liquidity_snapshots(
        transactions,
        run_id="run-002",
        pipeline_version="1.0.0",
        dataset_version="2026-06-10",
    )

    final_snapshot = snapshots[-1]
    assert final_snapshot.snapshot_date == (base + timedelta(days=4)).date()
    assert final_snapshot.window_start_utc == datetime(2026, 6, 10, 0, 0, tzinfo=timezone.utc)
    assert final_snapshot.window_end_utc == datetime(2026, 6, 14, 23, 59, 59, 999999, tzinfo=timezone.utc)
    assert final_snapshot.total_inbound_usd == Decimal("15.00")
    assert final_snapshot.total_outbound_usd == Decimal("5.00")
    assert final_snapshot.net_liquidity_usd == Decimal("10.00")


def test_aggregates_independently_by_legal_entity_and_is_deterministic_for_input_order():
    base = datetime(2026, 6, 1, 9, 30, tzinfo=timezone.utc)
    transactions = [
        _usd_txn("T1", base, "LE2", "7.00", "INBOUND"),
        _usd_txn("T2", base, "LE1", "3.00", "OUTBOUND"),
        _usd_txn("T3", base + timedelta(days=1), "LE1", "11.00", "INBOUND"),
        _usd_txn("T4", base + timedelta(days=1), "LE2", "13.00", "OUTBOUND"),
    ]

    shuffled = list(transactions)
    random.Random(17).shuffle(shuffled)

    baseline = aggregate_liquidity_snapshots(
        transactions,
        run_id="run-003",
        pipeline_version="1.0.0",
        dataset_version="2026-06-01",
    )
    shuffled_result = aggregate_liquidity_snapshots(
        shuffled,
        run_id="run-003",
        pipeline_version="1.0.0",
        dataset_version="2026-06-01",
    )

    assert baseline == shuffled_result
    assert [(row.snapshot_date, row.legal_entity_id) for row in baseline] == [
        (base.date(), "LE1"),
        (base.date(), "LE2"),
        ((base + timedelta(days=1)).date(), "LE1"),
        ((base + timedelta(days=1)).date(), "LE2"),
    ]
    assert baseline[0].total_outbound_usd == Decimal("3.00")
    assert baseline[0].net_liquidity_usd == Decimal("-3.00")
    assert baseline[1].total_inbound_usd == Decimal("7.00")
    assert baseline[1].net_liquidity_usd == Decimal("7.00")


def test_empty_input_returns_no_snapshots():
    assert compute_liquidity_snapshots([], run_id="run-004", pipeline_version="1.0.0", dataset_version="2026-06-01") == []
