"""Coverage accounting over validated, content-addressed receipts."""

from .core import INTERVALS, KINDS, safe_name


def catalog_index(store):
    """Build one in-memory index of valid receipt metadata for bulk coverage scans."""
    index = {}
    for item in store.catalog():
        if item["validation"]["status"] != "VALID":
            continue
        key = (item["dataset"], item["symbol"], item["timeframe"])
        index.setdefault(key, []).append(item)
    for items in index.values():
        items.sort(key=lambda p: (p["start"], p["end"], p["receipt_id"]))
    return index


def _merge(intervals):
    merged = []
    for start, end in sorted(intervals):
        if not merged or start > merged[-1][1]:
            merged.append([start, end])
        else:
            merged[-1][1] = max(merged[-1][1], end)
    return [tuple(item) for item in merged]


def _gaps(start, end, intervals):
    gaps = []
    cursor = start
    for left, right in _merge(intervals):
        if cursor < left:
            gaps.append((cursor, left))
        cursor = max(cursor, right)
    if cursor < end:
        gaps.append((cursor, end))
    return gaps


def coverage(store, kind, symbol, timeframe, start, end, index=None):
    """Report verified local coverage for one dataset/symbol/timeframe range."""
    safe_name(symbol)
    if kind not in KINDS or timeframe not in INTERVALS:
        raise ValueError("Unsupported dataset/timeframe")
    if start >= end:
        raise ValueError("Coverage requires a nonempty interval")
    if kind != "funding" and (start % INTERVALS[timeframe] or end % INTERVALS[timeframe]):
        raise ValueError("Coverage bounds must align with timeframe")

    parts = []
    candidates = (index or catalog_index(store)).get((kind, symbol, timeframe), [])
    for item in candidates:
        if item["end"] <= start or item["start"] >= end:
            continue
        receipt = store.receipt(item["receipt_id"])
        parts.append(dict(receipt_id=item["receipt_id"], **receipt))
    parts.sort(key=lambda p: (p["start"], p["end"], p["receipt_id"]))

    conflicts = []
    for index, left in enumerate(parts):
        for right in parts[index + 1 :]:
            if right["start"] >= left["end"]:
                break
            same_object = (
                left["start"] == right["start"]
                and left["end"] == right["end"]
                and left["sha256"] == right["sha256"]
            )
            if not same_object:
                conflicts.append(
                    {
                        "left": left["receipt_id"],
                        "right": right["receipt_id"],
                        "overlap_start": max(left["start"], right["start"]),
                        "overlap_end": min(left["end"], right["end"]),
                    }
                )

    clipped = [(max(start, p["start"]), min(end, p["end"])) for p in parts]
    merged = _merge(clipped)
    if kind == "funding":
        return {
            "dataset": kind,
            "symbol": symbol,
            "timeframe": timeframe,
            "start": start,
            "end": end,
            "status": "CONFLICT" if conflicts else "EVENT_STREAM",
            "complete": None,
            "coverage_ratio": None,
            "expected_rows": None,
            "covered_rows": sum(p["validation"]["actual_rows"] for p in parts),
            "covered_intervals": [list(item) for item in merged],
            "gaps": None,
            "conflicts": conflicts,
            "receipt_ids": [p["receipt_id"] for p in parts],
        }

    gaps = _gaps(start, end, merged)
    step = INTERVALS[timeframe]
    expected = (end - start) // step
    covered = sum((right - left) // step for left, right in merged)
    status = "CONFLICT" if conflicts else ("COMPLETE" if not gaps else "GAPPED")
    return {
        "dataset": kind,
        "symbol": symbol,
        "timeframe": timeframe,
        "start": start,
        "end": end,
        "status": status,
        "complete": status == "COMPLETE",
        "coverage_ratio": covered / expected if expected else 1.0,
        "expected_rows": expected,
        "covered_rows": covered,
        "covered_intervals": [list(item) for item in merged],
        "gaps": [list(item) for item in gaps],
        "conflicts": conflicts,
        "receipt_ids": [p["receipt_id"] for p in parts],
    }
