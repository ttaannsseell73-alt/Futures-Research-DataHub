import polars as pl

from .core import INTERVALS
from .schemas import CANDLE, FUNDING


def validate(table, kind, timeframe, start, end):
    """Half-open coverage [start,end); never infer coverage from observed first/last."""
    schema = FUNDING if kind == "funding" else CANDLE
    if table.schema != schema:
        raise ValueError("Non-canonical Arrow schema")
    df = pl.from_arrow(table)
    ts = df["timestamp"].cast(pl.Int64)
    duplicate = len(ts) - ts.n_unique()
    outside = ts.is_null().sum() + ((ts < start) | (ts >= end)).sum()
    invalid = 0
    expected = missing = misaligned = None
    if kind == "funding":
        invalid = df.select(
            (pl.col("funding_rate").is_null() | ~pl.col("funding_rate").is_finite()).sum()
        ).item()
    else:
        step = INTERVALS[timeframe]
        if start >= end or start % step or end % step:
            raise ValueError("Coverage bounds must be ordered and aligned to timeframe")
        expected = (end - start) // step
        misaligned = ((ts % step) != 0).sum()
        valid_ts = ts.filter((ts >= start) & (ts < end) & ((ts % step) == 0))
        missing = expected - valid_ts.n_unique()
        price_bad = pl.any_horizontal(
            [
                pl.col(c).is_null() | ~pl.col(c).is_finite() | (pl.col(c) <= 0)
                for c in ("open", "high", "low", "close")
            ]
        )
        bad = (
            price_bad
            | (pl.col("high") < pl.max_horizontal("open", "close", "low"))
            | (pl.col("low") > pl.min_horizontal("open", "close", "high"))
        )
        if kind == "ohlcv":
            bad = (
                bad
                | pl.col("volume").is_null()
                | ~pl.col("volume").is_finite()
                | (pl.col("volume") < 0)
            )
        invalid = df.select(bad.sum()).item()
    valid = not any([duplicate, outside, invalid, missing, misaligned])
    return {
        "status": "VALID" if valid else "QUARANTINED",
        "actual_rows": len(ts),
        "expected_rows": expected,
        "missing": missing,
        "duplicates": duplicate,
        "invalid": invalid,
        "out_of_bounds": outside,
        "misaligned": misaligned,
        "first_timestamp": ts.min(),
        "last_timestamp": ts.max(),
        "coverage": "event_stream_no_fixed_cadence" if kind == "funding" else "complete_grid",
    }
