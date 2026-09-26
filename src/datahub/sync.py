from datetime import UTC, datetime

from filelock import FileLock

from .core import INTERVALS, KINDS, atomic_json, fingerprint, safe_symbol, utcnow


def sync(store, adapter, kind, symbol, timeframe, start, end, fallback_adapter=None):
    """Sync a historical range in UTC-day chunks with an explicit, audited fallback."""
    safe_symbol(symbol)
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
        if fallback_adapter is not None:
            job["fallback_source"] = fallback_adapter.name
        key = fingerprint(job)
        checkpoint = store.root / f"checkpoints/{key}.json"
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
                source_used = adapter.name
                primary_error = None
            except Exception as primary_exc:
                if fallback_adapter is None:
                    atomic_json(
                        checkpoint,
                        dict(job, status="FAILED", error=str(primary_exc), updated_at=utcnow()),
                    )
                    raise
                primary_error = str(primary_exc)
                atomic_json(
                    checkpoint,
                    dict(
                        job,
                        status="FALLBACK",
                        primary_error=primary_error,
                        updated_at=utcnow(),
                    ),
                )
                try:
                    table, provenance = fallback_adapter.fetch(
                        kind, symbol, timeframe, cursor, stop
                    )
                    provenance = dict(
                        provenance,
                        primary_source=adapter.name,
                        fallback_from=adapter.name,
                        fallback_reason=primary_error,
                    )
                    rid = store.put(table, kind, symbol, timeframe, cursor, stop, provenance)
                    source_used = fallback_adapter.name
                except Exception as fallback_exc:
                    atomic_json(
                        checkpoint,
                        dict(
                            job,
                            status="FAILED",
                            primary_error=primary_error,
                            fallback_error=str(fallback_exc),
                            updated_at=utcnow(),
                        ),
                    )
                    raise

            complete = dict(
                job,
                status="COMPLETE",
                receipt_id=rid,
                source_used=source_used,
                updated_at=utcnow(),
            )
            if primary_error is not None:
                complete["primary_error"] = primary_error
            atomic_json(checkpoint, complete)
            receipts.append(rid)
        cursor = stop
    return receipts
