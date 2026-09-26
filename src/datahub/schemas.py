from datetime import UTC, datetime

import pyarrow as pa

TS = pa.timestamp("ms", tz="UTC")
CANDLE = pa.schema(
    [
        ("timestamp", TS),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("volume", pa.float64()),
    ]
)
FUNDING = pa.schema([("timestamp", TS), ("funding_rate", pa.float64())])


def timestamp(ms):
    value = int(ms)
    if not 1_500_000_000_000 <= value < 10_000_000_000_000:
        raise ValueError("Expected Binance futures epoch milliseconds; unit/range mismatch")
    return datetime.fromtimestamp(value / 1000, UTC)


def normalize(rows, kind):
    if kind == "funding":
        records = [
            {"timestamp": timestamp(r["fundingTime"]), "funding_rate": float(r["fundingRate"])}
            for r in rows
        ]
        return pa.Table.from_pylist(records, schema=FUNDING)
    records = [
        {
            "timestamp": timestamp(r[0]),
            "open": float(r[1]),
            "high": float(r[2]),
            "low": float(r[3]),
            "close": float(r[4]),
            "volume": float(r[5]) if kind == "ohlcv" else None,
        }
        for r in rows
    ]
    return pa.Table.from_pylist(records, schema=CANDLE)
