import hashlib
import io
import urllib.parse
import zipfile

import httpx

from datahub.core import fingerprint, safe_symbol
from datahub.ingest import HTTP, Vision
from datahub.inventory import (
    S3Index,
    archive_snapshot,
    load_inventory,
    scan_vision_inventory,
)
from datahub.planning import create_inventory_backfill_plan, run_plan
from datahub.release import doctor, publish_plan
from datahub.schemas import normalize
from datahub.storage import Store

START = 1_704_067_200_000
DAY = 86_400_000


def _xml(prefixes=(), objects=(), truncated=False, token=None):
    common = "".join(
        f"<CommonPrefixes><Prefix>{value}</Prefix></CommonPrefixes>" for value in prefixes
    )
    contents = "".join(
        "<Contents>"
        f"<Key>{key}</Key><LastModified>2024-01-02T00:00:00.000Z</LastModified>"
        f'<ETag>"etag"</ETag><Size>123</Size>'
        "</Contents>"
        for key in objects
    )
    next_token = f"<NextContinuationToken>{token}</NextContinuationToken>" if token else ""
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        f"<IsTruncated>{str(truncated).lower()}</IsTruncated>"
        f"{next_token}{contents}{common}</ListBucketResult>"
    )


def test_s3_index_paginates_common_prefixes():
    calls = []

    def handler(request):
        calls.append(request)
        token = request.url.params.get("continuation-token")
        if token is None:
            return httpx.Response(
                200,
                content=_xml(
                    prefixes=["data/futures/um/daily/klines/BTCUSDT/"],
                    truncated=True,
                    token="next",
                ).encode(),
            )
        return httpx.Response(
            200,
            content=_xml(prefixes=["data/futures/um/daily/klines/DEADUSDT/"]).encode(),
        )

    index = S3Index(
        HTTP(httpx.Client(transport=httpx.MockTransport(handler)), sleep=lambda _: None)
    )
    values = index.common_prefixes("data/futures/um/daily/klines/")
    assert values == [
        "data/futures/um/daily/klines/BTCUSDT/",
        "data/futures/um/daily/klines/DEADUSDT/",
    ]
    assert len(calls) == 2


def test_inventory_discovers_archive_symbols_without_exchangeinfo(tmp_path):
    def handler(request):
        prefix = request.url.params["prefix"]
        if prefix == "data/futures/um/daily/klines/":
            return httpx.Response(
                200,
                content=_xml(
                    prefixes=[
                        "data/futures/um/daily/klines/BTCUSDT/",
                        "data/futures/um/daily/klines/DEADUSDT/",
                    ]
                ).encode(),
            )
        symbol = prefix.split("/")[-3]
        key = f"{prefix}{symbol}-1m-2024-01-01.zip"
        return httpx.Response(200, content=_xml(objects=[key]).encode())

    http = HTTP(httpx.Client(transport=httpx.MockTransport(handler)), sleep=lambda _: None)
    store = Store(tmp_path)
    document = scan_vision_inventory(
        store,
        "jan",
        START,
        START + DAY,
        probe_boundaries=False,
        index=S3Index(http),
    )
    assert document["symbols_discovered"] == 2
    assert {row["symbol"] for row in document["symbols"]} == {"BTCUSDT", "DEADUSDT"}
    assert document["historical_listing_complete"] is False
    assert load_inventory(store, "jan") == document

    snap = archive_snapshot(store, "jan_midday", "jan", START + DAY // 2)
    assert snap["symbols"] == ["BTCUSDT", "DEADUSDT"]
    assert snap["survivorship_safe_from_current_list"] is True
    assert snap["complete"] is False


def test_inventory_bridges_small_archive_hole_but_records_it(tmp_path):
    keys = [
        "data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-01.zip",
        "data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-01-03.zip",
    ]

    def handler(request):
        return httpx.Response(200, content=_xml(objects=keys).encode())

    http = HTTP(httpx.Client(transport=httpx.MockTransport(handler)), sleep=lambda _: None)
    store = Store(tmp_path)
    doc = scan_vision_inventory(
        store,
        "gap",
        START,
        START + 3 * DAY,
        symbols=["BTCUSDT"],
        max_gap_days=1,
        probe_boundaries=False,
        index=S3Index(http),
    )
    row = doc["symbols"][0]
    assert row["archive_ranges"] == [[START, START + DAY], [START + 2 * DAY, START + 3 * DAY]]
    assert row["bridged_missing_ranges"] == [[START + DAY, START + 2 * DAY]]
    assert row["active_segments"] == [[START, START + 3 * DAY]]


def test_inventory_plan_fallback_release_and_doctor(tmp_path):
    store = Store(tmp_path)
    body = {
        "schema_version": 1,
        "inventory_id": "fixture",
        "source": "binance_vision_s3_listobjects_v2",
        "source_endpoint": "fixture",
        "source_prefix": "fixture",
        "dataset": "ohlcv",
        "timeframe": "1m",
        "requested_start": START,
        "requested_end": START + DAY,
        "observed_at": "2024-01-02T00:00:00+00:00",
        "listing_scan_complete": True,
        "historical_listing_complete": False,
        "survivorship_guard": "independent_of_current_exchangeInfo",
        "membership_semantics": "archive_observed_activity_not_exchange_listing",
        "max_bridge_gap_days": 3,
        "symbols_discovered": 1,
        "symbols_observed": 1,
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "object_count": 1,
                "archive_ranges": [[START, START + DAY]],
                "bridged_missing_ranges": [],
                "active_segments": [[START, START + DAY]],
                "boundary_probe": False,
            }
        ],
    }
    inv = dict(body, fingerprint=fingerprint(body))
    store.immutable("inventories/fixture.json", inv)

    plan = create_inventory_backfill_plan(
        store,
        "fixture_plan",
        "fixture",
        ["ohlcv"],
        ["4h"],
        source_policy="vision-rest",
    )
    assert len(plan["jobs"]) == 1
    assert plan["jobs"][0]["source"] == "vision"
    assert plan["jobs"][0]["fallback_source"] == "rest"

    class BrokenVision:
        name = "vision"

        def fetch(self, kind, symbol, timeframe, start, end):
            raise ValueError("fixture archive defect")

    class GoodRest:
        name = "rest"

        def fetch(self, kind, symbol, timeframe, start, end):
            rows = [[ts, "10", "12", "9", "11", "2"] for ts in range(start, end, 14_400_000)]
            return normalize(rows, kind), {"source": "fixture_rest"}

    result = run_plan(
        store,
        "fixture_plan",
        adapters={"vision": BrokenVision(), "rest": GoodRest()},
    )
    assert result["status"] == "COMPLETE"
    receipt = store.catalog()[0]
    assert receipt["provenance"]["fallback_from"] == "vision"
    assert "fixture archive defect" in receipt["provenance"]["fallback_reason"]

    manifest = publish_plan(store, "release_v1", "fixture_plan")
    assert manifest["lineage"]["evidence_type"] == "archive_inventory"
    assert manifest["lineage"]["coverage_attestation"]["fixed_grid_complete"] is True
    assert store.verify("release_v1") == manifest

    health = doctor(store, deep=True, strict=True)
    assert health["status"] == "PASS"


def test_unicode_symbol_is_safe_and_vision_url_is_encoded():
    symbol = "龙虾USDT"
    assert safe_symbol(symbol) == symbol
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr(
            "fixture.csv",
            "\n".join(f"{ts},10,12,9,11,2" for ts in range(START, START + DAY, 14_400_000)),
        )
    raw = output.getvalue()
    checksum = hashlib.sha256(raw).hexdigest()
    seen = []

    def handler(request):
        seen.append(str(request.url))
        if str(request.url).endswith(".CHECKSUM"):
            return httpx.Response(200, text=checksum + " fixture.zip")
        return httpx.Response(200, content=raw)

    vision = Vision(
        HTTP(httpx.Client(transport=httpx.MockTransport(handler)), sleep=lambda _: None)
    )
    table, _ = vision.fetch("ohlcv", symbol, "4h", START, START + DAY)
    assert table.num_rows == 6
    encoded = urllib.parse.quote(symbol, safe="")
    assert any(encoded in url for url in seen)
