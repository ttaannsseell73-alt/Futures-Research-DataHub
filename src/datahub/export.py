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
    runners. Vision is primary and REST/daily sync is the explicit fallback.
    """
    safe_symbol(symbol)
    if timeframe not in INTERVALS:
        raise ValueError("Unsupported timeframe")
    if start >= end:
        raise ValueError("Require start < end")

    start_dt = datetime.fromtimestamp(start / 1000, UTC)
    end_dt = datetime.fromtimestamp(end / 1000, UTC)
    if start_dt.month == 12:
        expected_month_end = datetime(start_dt.year + 1, 1, 1, tzinfo=UTC)
    else:
        expected_month_end = datetime(start_dt.year, start_dt.month + 1, 1, tzinfo=UTC)
    complete_month = (
        start_dt.day == 1
        and start_dt.hour == 0
        and start_dt.minute == 0
        and start_dt.second == 0
        and start_dt.microsecond == 0
        and end_dt == expected_month_end
    )

    if complete_month:
        receipt_ids = sync_month(
            store,
            Vision(),
            "ohlcv",
            symbol,
            timeframe,
            start,
            end,
            fallback_adapter=Rest(),
        )
    else:
        receipt_ids = sync(
            store,
            Vision(),
            "ohlcv",
            symbol,
            timeframe,
            start,
            end,
            fallback_adapter=Rest(),
        )
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
