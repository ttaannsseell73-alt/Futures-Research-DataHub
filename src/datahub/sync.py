from datetime import UTC, datetime

from .core import INTERVALS, KINDS, atomic_json, fingerprint, safe_name, utcnow


def sync(store, adapter, kind, symbol, timeframe, start, end):
    safe_name(symbol)
    if kind not in KINDS or timeframe not in INTERVALS:
        raise ValueError("Unsupported dataset/timeframe")
    if start >= end or end > int(datetime.now(UTC).timestamp() * 1000):
        raise ValueError("Require a nonempty historical interval")
    if kind != "funding" and (start % INTERVALS[timeframe] or end % INTERVALS[timeframe]):
        raise ValueError("Bounds must align with timeframe; exclude open candles")
    receipts = []
    cursor = start
    while cursor < end:
        stop = min(end, (cursor // 86_400_000 + 1) * 86_400_000)
        job = {
            "source": adapter.name,
            "dataset": kind,
            "symbol": symbol,
            "timeframe": timeframe,
            "start": cursor,
            "end": stop,
            "schema_version": 1,
        }
        key = fingerprint(job)
        checkpoint = store.root / f"checkpoints/{key}.json"
        # Per-job lock prevents duplicate downloads while allowing independent jobs.
        from filelock import FileLock

        checkpoint.parent.mkdir(parents=True, exist_ok=True)
        with FileLock(str(checkpoint) + ".lock", timeout=30):
            if checkpoint.exists():
                state = store.json(f"checkpoints/{key}.json")
                if state["status"] == "COMPLETE":
                    store.receipt(state["receipt_id"])
                    receipts.append(state["receipt_id"])
                    cursor = stop
                    continue
            atomic_json(checkpoint, dict(job, status="RUNNING", updated_at=utcnow()))
            try:
                table, provenance = adapter.fetch(kind, symbol, timeframe, cursor, stop)
                rid = store.put(table, kind, symbol, timeframe, cursor, stop, provenance)
            except Exception as exc:
                atomic_json(
                    checkpoint, dict(job, status="FAILED", error=str(exc), updated_at=utcnow())
                )
                raise
            atomic_json(
                checkpoint, dict(job, status="COMPLETE", receipt_id=rid, updated_at=utcnow())
            )
            receipts.append(rid)
        cursor = stop
    return receipts
