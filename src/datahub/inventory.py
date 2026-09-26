"""Binance Vision S3 inventory and archive-observed point-in-time universes."""

import re
import xml.etree.ElementTree as ET
from datetime import UTC, datetime

import pyarrow as pa

from .core import fingerprint, safe_name, safe_symbol, utcnow
from .ingest import HTTP, Vision

S3_LIST_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
VISION_PREFIX = "data/futures/um/daily/klines/"
DAY = 86_400_000
MINUTE = 60_000


def _text(parent, name, default=None):
    node = parent.find(f"{{*}}{name}")
    return default if node is None or node.text is None else node.text


class S3Index:
    """Read-only ListObjectsV2 client for Binance's public Vision bucket."""

    def __init__(self, http=None):
        self.http = http or HTTP()

    def page(self, prefix, delimiter=None, continuation=None, start_after=None, max_keys=1000):
        params = {"list-type": "2", "prefix": prefix, "max-keys": max_keys}
        if delimiter:
            params["delimiter"] = delimiter
        if continuation:
            params["continuation-token"] = continuation
        if start_after:
            params["start-after"] = start_after
        response = self.http.get(S3_LIST_URL, params=params)
        root = ET.fromstring(response.content)
        objects = []
        for node in root.findall("{*}Contents"):
            objects.append(
                {
                    "key": _text(node, "Key", ""),
                    "etag": _text(node, "ETag", "").strip('"'),
                    "size": int(_text(node, "Size", "0")),
                    "last_modified": _text(node, "LastModified"),
                }
            )
        prefixes = [
            _text(node, "Prefix", "")
            for node in root.findall("{*}CommonPrefixes")
            if _text(node, "Prefix", "")
        ]
        truncated = _text(root, "IsTruncated", "false").lower() == "true"
        token = _text(root, "NextContinuationToken")
        if truncated and not token:
            raise ValueError("Truncated S3 listing omitted continuation token")
        return {
            "objects": objects,
            "prefixes": prefixes,
            "truncated": truncated,
            "next_token": token,
        }

    def walk(self, prefix, delimiter=None, start_after=None):
        token = None
        first = True
        while first or token:
            first = False
            page = self.page(
                prefix,
                delimiter=delimiter,
                continuation=token,
                start_after=start_after if token is None else None,
            )
            yield page
            token = page["next_token"] if page["truncated"] else None

    def common_prefixes(self, prefix):
        values = []
        for page in self.walk(prefix, delimiter="/"):
            values.extend(page["prefixes"])
        return sorted(set(values))

    def objects(self, prefix, start_after=None):
        values = []
        for page in self.walk(prefix, start_after=start_after):
            values.extend(page["objects"])
        return values


def discover_symbols(index):
    symbols = []
    for prefix in index.common_prefixes(VISION_PREFIX):
        value = prefix[len(VISION_PREFIX) :].rstrip("/")
        if value:
            symbols.append(safe_symbol(value))
    return sorted(set(symbols))


def _day_start(ms):
    return (ms // DAY) * DAY


def _date_text(ms):
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%d")


def _key(symbol, day):
    date = _date_text(day)
    return f"{VISION_PREFIX}{symbol}/1m/{symbol}-1m-{date}.zip"


def daily_archive_days(index, symbol, start, end):
    safe_symbol(symbol)
    if start >= end:
        raise ValueError("Inventory range must be nonempty")
    prefix = f"{VISION_PREFIX}{symbol}/1m/"
    previous = _day_start(start) - DAY
    start_after = _key(symbol, previous)
    expected_prefix = f"{symbol}-1m-"
    dates = []
    for obj in index.objects(prefix, start_after=start_after):
        key = obj["key"]
        name = key.rsplit("/", 1)[-1]
        if not name.endswith(".zip") or not name.startswith(expected_prefix):
            continue
        stamp = name[len(expected_prefix) : -4]
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", stamp):
            continue
        day = int(datetime.fromisoformat(stamp).replace(tzinfo=UTC).timestamp() * 1000)
        if day >= end:
            break
        if day + DAY > start:
            dates.append(day)
    return sorted(set(dates))


def _ranges(days):
    if not days:
        return []
    result = []
    left = previous = days[0]
    for day in days[1:]:
        if day == previous + DAY:
            previous = day
            continue
        result.append([left, previous + DAY])
        left = previous = day
    result.append([left, previous + DAY])
    return result


def _segments(ranges, max_gap_days):
    if not ranges:
        return [], []
    segments = [ranges[0].copy()]
    bridged = []
    max_gap = max_gap_days * DAY
    for left, right in ranges[1:]:
        gap = left - segments[-1][1]
        if 0 < gap <= max_gap:
            bridged.append([segments[-1][1], left])
            segments[-1][1] = right
        else:
            segments.append([left, right])
    return segments, bridged


def _table_bounds(table):
    if table.num_rows == 0:
        raise ValueError("Boundary archive contains no rows")
    values = table["timestamp"].cast(pa.int64()).to_pylist()
    return min(values), max(values) + MINUTE


def scan_vision_inventory(
    store,
    name,
    start,
    end,
    symbols=None,
    symbol_regex=None,
    max_gap_days=3,
    probe_boundaries=True,
    index=None,
    vision=None,
):
    """Create an immutable archive-presence inventory independent of current exchangeInfo."""
    safe_name(name)
    if start >= end:
        raise ValueError("Inventory range must be nonempty")
    if max_gap_days < 0 or max_gap_days > 31:
        raise ValueError("max_gap_days must be between 0 and 31")
    requested_symbols = sorted(set(symbols)) if symbols else None
    request = {
        "requested_start": start,
        "requested_end": end,
        "requested_symbols": requested_symbols,
        "symbol_regex": symbol_regex,
        "max_bridge_gap_days": max_gap_days,
        "boundary_probe": probe_boundaries,
    }
    existing_path = store.root / f"inventories/{name}.json"
    if existing_path.exists():
        existing = load_inventory(store, name)
        current = {
            "requested_start": existing["requested_start"],
            "requested_end": existing["requested_end"],
            "requested_symbols": existing.get("requested_symbols"),
            "symbol_regex": existing.get("symbol_regex"),
            "max_bridge_gap_days": existing["max_bridge_gap_days"],
            "boundary_probe": existing.get("boundary_probe", True),
        }
        if current != request:
            raise ValueError("Immutable inventory name already exists with different parameters")
        return existing

    index = index or S3Index()
    discovered = list(requested_symbols) if requested_symbols else discover_symbols(index)
    discovered = [safe_symbol(symbol) for symbol in discovered]
    if symbol_regex:
        pattern = re.compile(symbol_regex)
        discovered = [symbol for symbol in discovered if pattern.search(symbol)]
    discovered = sorted(set(discovered))

    vision = vision or Vision(index.http)
    records = []
    for symbol in discovered:
        days = daily_archive_days(index, symbol, start, end)
        if not days:
            continue
        archive_ranges = _ranges(days)
        day_segments, bridged = _segments(archive_ranges, max_gap_days)
        probe_cache = {}
        active_segments = []
        for seg_start, seg_end in day_segments:
            exact_start, exact_end = seg_start, seg_end
            if probe_boundaries:
                first_day = next(day for day in days if seg_start <= day < seg_end)
                last_day = next(day for day in reversed(days) if seg_start <= day < seg_end)
                for day in {first_day, last_day}:
                    if day not in probe_cache:
                        table, provenance = vision.fetch("ohlcv", symbol, "1m", day, day + DAY)
                        probe_cache[day] = (*_table_bounds(table), provenance["archive_sha256"])
                exact_start = probe_cache[first_day][0]
                exact_end = probe_cache[last_day][1]
            active_segments.append([max(start, exact_start), min(end, exact_end)])

        records.append(
            {
                "symbol": symbol,
                "object_count": len(days),
                "archive_ranges": archive_ranges,
                "bridged_missing_ranges": bridged,
                "active_segments": [
                    segment for segment in active_segments if segment[0] < segment[1]
                ],
                "boundary_probe": probe_boundaries,
            }
        )

    body = {
        "schema_version": 1,
        "inventory_id": name,
        "source": "binance_vision_s3_listobjects_v2",
        "source_endpoint": S3_LIST_URL,
        "source_prefix": VISION_PREFIX,
        "dataset": "ohlcv",
        "timeframe": "1m",
        "requested_start": start,
        "requested_end": end,
        "requested_symbols": requested_symbols,
        "symbol_regex": symbol_regex,
        "boundary_probe": probe_boundaries,
        "observed_at": utcnow(),
        "listing_scan_complete": True,
        "historical_listing_complete": False,
        "survivorship_guard": "independent_of_current_exchangeInfo",
        "membership_semantics": "archive_observed_activity_not_exchange_listing",
        "max_bridge_gap_days": max_gap_days,
        "symbols_discovered": len(discovered),
        "symbols_observed": len(records),
        "symbols": records,
    }
    document = dict(body, fingerprint=fingerprint(body))
    with store.lock():
        store.immutable(f"inventories/{name}.json", document)
    return document


def load_inventory(store, name):
    safe_name(name)
    document = store.json(f"inventories/{name}.json")
    body = {key: value for key, value in document.items() if key != "fingerprint"}
    if fingerprint(body) != document["fingerprint"]:
        raise ValueError("Inventory fingerprint mismatch")
    return document


def archive_snapshot(store, name, inventory, as_of):
    """Point-in-time universe from archive-observed activity; not an exchange listing assertion."""
    safe_name(name)
    document = load_inventory(store, inventory)
    if not document["requested_start"] <= as_of < document["requested_end"]:
        raise ValueError("Snapshot time is outside inventory range")
    symbols = sorted(
        row["symbol"]
        for row in document["symbols"]
        if any(left <= as_of < right for left, right in row["active_segments"])
    )
    body = {
        "schema_version": 1,
        "as_of": as_of,
        "inventory_id": inventory,
        "inventory_fingerprint": document["fingerprint"],
        "symbols": symbols,
        "complete": False,
        "survivorship_safe_from_current_list": True,
        "membership_semantics": document["membership_semantics"],
        "source": document["source"],
    }
    snapshot = dict(body, fingerprint=fingerprint(body))
    with store.lock():
        store.immutable(f"universes/{name}.json", snapshot)
    return snapshot
