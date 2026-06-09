from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Iterable

from src.fx.conversion import USDNormalizedTransaction

USD = "USD"
WINDOW_DAYS = 30
ZERO = Decimal("0")


class LiquidityWindowError(ValueError):
    """Raised when the liquidity window batch is invalid."""


@dataclass(frozen=True)
class LiquiditySnapshot:
    snapshot_date: date
    legal_entity_id: str
    window_start_utc: datetime
    window_end_utc: datetime
    currency: str
    transaction_count: int
    inbound_count: int
    outbound_count: int
    total_inbound_usd: Decimal
    total_outbound_usd: Decimal
    net_liquidity_usd: Decimal
    run_id: str
    pipeline_version: str
    dataset_version: str


@dataclass(frozen=True)
class _WindowTransaction:
    transaction_id: str
    timestamp: datetime
    legal_entity_id: str
    direction: str
    amount_usd: Decimal
    event_date: date


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _utc_date(value: datetime) -> date:
    return _ensure_utc(value).date()


def _window_start_date(snapshot_date: date, earliest_available_date: date) -> date:
    trailing_start = snapshot_date - timedelta(days=WINDOW_DAYS - 1)
    return max(earliest_available_date, trailing_start)


def _window_bounds(snapshot_date: date, start_date: date) -> tuple[datetime, datetime]:
    window_start_utc = datetime.combine(start_date, time.min, tzinfo=timezone.utc)
    window_end_utc = datetime.combine(snapshot_date, time.max, tzinfo=timezone.utc)
    return window_start_utc, window_end_utc


def _materialize_transactions(
    transactions: Iterable[USDNormalizedTransaction],
) -> list[_WindowTransaction]:
    materialized: list[_WindowTransaction] = []
    for transaction in transactions:
        if transaction.amount_usd is None:
            raise LiquidityWindowError("amount_usd is required")
        if transaction.amount_usd < ZERO:
            raise LiquidityWindowError("amount_usd must be non-negative")
        if transaction.direction not in {"INBOUND", "OUTBOUND"}:
            raise LiquidityWindowError("direction must be INBOUND or OUTBOUND")
        if not isinstance(transaction.legal_entity_id, str) or not transaction.legal_entity_id:
            raise LiquidityWindowError("legal_entity_id must be a non-empty string")
        if not isinstance(transaction.transaction_id, str) or not transaction.transaction_id:
            raise LiquidityWindowError("transaction_id must be a non-empty string")

        materialized.append(
            _WindowTransaction(
                transaction_id=transaction.transaction_id,
                timestamp=_ensure_utc(transaction.timestamp),
                legal_entity_id=transaction.legal_entity_id,
                direction=transaction.direction,
                amount_usd=transaction.amount_usd,
                event_date=_utc_date(transaction.timestamp),
            )
        )

    return materialized


def _sum_amounts(window_transactions: list[_WindowTransaction]) -> tuple[int, int, int, Decimal, Decimal, Decimal]:
    transaction_count = len(window_transactions)
    inbound_count = 0
    outbound_count = 0
    total_inbound_usd = ZERO
    total_outbound_usd = ZERO

    for transaction in window_transactions:
        if transaction.direction == "INBOUND":
            inbound_count += 1
            total_inbound_usd += transaction.amount_usd
        else:
            outbound_count += 1
            total_outbound_usd += transaction.amount_usd

    net_liquidity_usd = total_inbound_usd - total_outbound_usd
    return transaction_count, inbound_count, outbound_count, total_inbound_usd, total_outbound_usd, net_liquidity_usd


def compute_liquidity_snapshots(
    transactions: Iterable[USDNormalizedTransaction],
    *,
    run_id: str,
    pipeline_version: str,
    dataset_version: str,
) -> list[LiquiditySnapshot]:
    """
    Compute deterministic daily liquidity snapshots from USD-normalized transactions.
    """

    if not isinstance(run_id, str) or not run_id:
        raise LiquidityWindowError("run_id must be a non-empty string")
    if not isinstance(pipeline_version, str) or not pipeline_version:
        raise LiquidityWindowError("pipeline_version must be a non-empty string")
    if not isinstance(dataset_version, str) or not dataset_version:
        raise LiquidityWindowError("dataset_version must be a non-empty string")

    materialized = _materialize_transactions(transactions)
    if not materialized:
        return []

    by_entity: dict[str, list[_WindowTransaction]] = {}
    all_dates: set[date] = set()
    for transaction in materialized:
        by_entity.setdefault(transaction.legal_entity_id, []).append(transaction)
        all_dates.add(transaction.event_date)

    for entity_transactions in by_entity.values():
        entity_transactions.sort(key=lambda row: (row.timestamp, row.transaction_id))

    snapshot_start = min(all_dates)
    snapshot_end = max(all_dates)
    snapshot_dates = [snapshot_start + timedelta(days=ordinal) for ordinal in range((snapshot_end - snapshot_start).days + 1)]

    snapshots: list[LiquiditySnapshot] = []
    for snapshot_date in snapshot_dates:
        for legal_entity_id in sorted(by_entity):
            entity_transactions = by_entity[legal_entity_id]
            earliest_available_date = entity_transactions[0].event_date
            start_date = _window_start_date(snapshot_date, earliest_available_date)

            window_transactions = [
                transaction
                for transaction in entity_transactions
                if start_date <= transaction.event_date <= snapshot_date
            ]

            if not window_transactions:
                continue

            (
                transaction_count,
                inbound_count,
                outbound_count,
                total_inbound_usd,
                total_outbound_usd,
                net_liquidity_usd,
            ) = _sum_amounts(window_transactions)
            window_start_utc, window_end_utc = _window_bounds(snapshot_date, start_date)

            snapshots.append(
                LiquiditySnapshot(
                    snapshot_date=snapshot_date,
                    legal_entity_id=legal_entity_id,
                    window_start_utc=window_start_utc,
                    window_end_utc=window_end_utc,
                    currency=USD,
                    transaction_count=transaction_count,
                    inbound_count=inbound_count,
                    outbound_count=outbound_count,
                    total_inbound_usd=total_inbound_usd,
                    total_outbound_usd=total_outbound_usd,
                    net_liquidity_usd=net_liquidity_usd,
                    run_id=run_id,
                    pipeline_version=pipeline_version,
                    dataset_version=dataset_version,
                )
            )

    return snapshots


def aggregate_liquidity_snapshots(
    transactions: Iterable[USDNormalizedTransaction],
    *,
    run_id: str,
    pipeline_version: str,
    dataset_version: str,
) -> list[LiquiditySnapshot]:
    return compute_liquidity_snapshots(
        transactions,
        run_id=run_id,
        pipeline_version=pipeline_version,
        dataset_version=dataset_version,
    )
