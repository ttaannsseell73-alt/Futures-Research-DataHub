"""Point-in-time lifecycle evidence. Current exchangeInfo is never historical completeness."""

from .core import fingerprint, safe_name, safe_symbol, utcnow


def capture_metadata(store, payload):
    if not isinstance(payload.get("symbols"), list) or not payload["symbols"]:
        raise ValueError("Invalid exchangeInfo")
    doc = {
        "schema_version": 1,
        "observed_at": utcnow(),
        "source": "binance_exchangeInfo",
        "historical_complete": False,
        "raw": payload,
    }
    key = fingerprint(doc)
    with store.lock():
        store.immutable(f"metadata/{key}.json", doc)
    return key


def import_lifecycle(store, evidence):
    """Import audited lifecycle intervals including delisted symbols and known-at times."""
    if not evidence.get("source") or not evidence.get("records"):
        raise ValueError("Lifecycle requires source and records")
    start, end = evidence["coverage_start"], evidence["coverage_end"]
    if start >= end or not isinstance(evidence.get("complete"), bool):
        raise ValueError("Invalid lifecycle coverage")
    seen = set()
    for row in evidence["records"]:
        safe_symbol(row["symbol"])
        identity = (row["symbol"], row["listed_at"])
        if identity in seen:
            raise ValueError("Duplicate lifecycle record")
        seen.add(identity)
        if not row.get("source") or not isinstance(row["known_at"], int):
            raise ValueError("Each lifecycle requires source and known_at")
        if row["delisted_at"] is not None and row["delisted_at"] <= row["listed_at"]:
            raise ValueError("Invalid listing/delisting interval")
    key = fingerprint(evidence)
    with store.lock():
        store.immutable(f"lifecycles/{key}.json", evidence)
    return key


def snapshot(store, name, lifecycle, as_of, known_at):
    safe_name(name)
    evidence = store.json(f"lifecycles/{safe_name(lifecycle)}.json")
    if fingerprint(evidence) != lifecycle:
        raise ValueError("Lifecycle fingerprint mismatch")
    if (
        not evidence["complete"]
        or not evidence["coverage_start"] <= as_of < evidence["coverage_end"]
    ):
        raise ValueError("Historical universe coverage is incomplete")
    if any(r["known_at"] > known_at for r in evidence["records"]):
        raise ValueError("Evidence unavailable at requested knowledge cutoff")
    symbols = sorted(
        {
            r["symbol"]
            for r in evidence["records"]
            if r["listed_at"] <= as_of and (r["delisted_at"] is None or as_of < r["delisted_at"])
        }
    )
    body = {
        "schema_version": 1,
        "as_of": as_of,
        "known_at": known_at,
        "lifecycle_fingerprint": lifecycle,
        "symbols": symbols,
        "coverage_source": evidence["source"],
        "complete": True,
    }
    doc = dict(body, fingerprint=fingerprint(body))
    with store.lock():
        store.immutable(f"universes/{name}.json", doc)
    return doc
