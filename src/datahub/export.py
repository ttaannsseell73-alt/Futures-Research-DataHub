"""Bridge verified DataHub OHLCV into strategy-runner CSV without adding server layers."""

from __future__ import annotations

import csv
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from .core import INTERVALS, safe_symbol
from .ingest import Rest, Vision
from .sync import sync, sync_month

CSV_COLUMNS = ("timestamp", "open", "high", "low", "close", "volume")


def fetch_ohlcv_csv(store, symbol: str, timeframe: str, start: int, end: int, output):
    """Ensure a closed historical OHLCV range exists, then export it to canonical CSV.

    Complete UTC calendar months use Binance Vision monthly archives; other ranges use UTC-day
    checkpoints. Re-running the same request reuses verified checkpoints on local/self-hosted
    runners. Complete months use Vision monthly archives; partial months use Vision daily archives
    with explicit REST fallback only if Vision itself fails.
    """
    safe_symbol(symbol)
    if timeframe not in INTERVALS:
        raise ValueError("Unsupported timeframe")
    if start >= end:
        raise ValueError("Require start < end")

    def month_start_ms(value: int) -> int:
        dt = datetime.fromtimestamp(value / 1000, UTC)
        return int(datetime(dt.year, dt.month, 1, tzinfo=UTC).timestamp() * 1000)

    def next_month_ms(value: int) -> int:
        dt = datetime.fromtimestamp(value / 1000, UTC)
        if dt.month == 12:
            nxt = datetime(dt.year + 1, 1, 1, tzinfo=UTC)
        else:
            nxt = datetime(dt.year, dt.month + 1, 1, tzinfo=UTC)
        return int(nxt.timestamp() * 1000)

    # Decompose arbitrary multi-month research ranges. Every complete UTC month uses
    # one Binance Vision monthly archive; only partial leading/trailing ranges use
    # Vision daily checkpoints. This avoids Binance REST geo restrictions on cloud runners while
    # still reducing a Jan-Sep request from ~269 daily archives to 8 monthly archives plus only the
    # final partial month's daily archives.
    receipt_ids = []
    cursor = start
    vision = Vision()
    rest = Rest()
    while cursor < end:
        is_month_boundary = cursor == month_start_ms(cursor)
        month_end = next_month_ms(cursor)
        if is_month_boundary and month_end <= end:
            receipt_ids.extend(
                sync_month(
                    store,
                    vision,
                    "ohlcv",
                    symbol,
                    timeframe,
                    cursor,
                    month_end,
                    fallback_adapter=rest,
                )
            )
            cursor = month_end
            continue

        stop = min(end, month_end)
        receipt_ids.extend(
            sync(
                store,
                vision,
                "ohlcv",
                symbol,
                timeframe,
                cursor,
                stop,
                fallback_adapter=rest,
            )
        )
        cursor = stop
    receipts = [store.receipt(rid) for rid in receipt_ids]
    paths = [str(store.root / receipt["path"]) for receipt in receipts]
    if not paths:
        raise ValueError("No verified partitions were produced")

    with duckdb.connect() as con:
        con.execute("SET TimeZone='UTC'")
        table = (
            con.read_parquet(paths, hive_partitioning=False)
            .filter(f"epoch_ms(timestamp) >= {int(start)} AND epoch_ms(timestamp) < {int(end)}")
            .order("timestamp")
            .to_arrow_table()
        )

    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(CSV_COLUMNS)
        for row in table.to_pylist():
            writer.writerow(
                [
                    int(row["timestamp"].timestamp() * 1000),
                    row["open"],
                    row["high"],
                    row["low"],
                    row["close"],
                    row["volume"],
                ]
            )

    return {
        "status": "READY",
        "symbol": symbol,
        "timeframe": timeframe,
        "start": start,
        "end": end,
        "rows": table.num_rows,
        "receipts": receipt_ids,
        "output": str(output),
    }
