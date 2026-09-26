"""Bridge verified DataHub OHLCV into strategy-runner CSV without adding server layers."""

from __future__ import annotations

import csv
from pathlib import Path

import duckdb

from .core import INTERVALS, safe_symbol
from .ingest import Rest, Vision
from .sync import sync


CSV_COLUMNS = ("timestamp", "open", "high", "low", "close", "volume")


def fetch_ohlcv_csv(store, symbol: str, timeframe: str, start: int, end: int, output):
    """Ensure a closed historical OHLCV range exists, then export it to canonical CSV.

    Data is synced in UTC-day checkpoints. Re-running the same request reuses verified
    checkpoints on local/self-hosted runners. Vision is primary and REST is the explicit fallback.
    """
    safe_symbol(symbol)
    if timeframe not in INTERVALS:
        raise ValueError("Unsupported timeframe")
    if start >= end:
        raise ValueError("Require start < end")

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
