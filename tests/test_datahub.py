import hashlib
import io
import json
import subprocess
import sys
import zipfile

import httpx
import pytest

from datahub.core import data_root, millis
from datahub.ingest import HTTP, Rest, Vision
from datahub.schemas import normalize
from datahub.storage import Store
from datahub.sync import sync, sync_month
from datahub.universe import capture_metadata, import_lifecycle, snapshot
from datahub.validation import validate

START = 1_704_067_200_000


def candles(count=2, step=60_000):
    return [[START + i * step, "10", "12", "9", "11", "2"] for i in range(count)]


def test_utc_and_root(tmp_path, monkeypatch):
    assert millis("2024-01-01T03:00:00+03:00") == START
    with pytest.raises(ValueError):
        millis("2024-01-01")
    (tmp_path / ".git").mkdir()
    with pytest.raises(ValueError, match="outside"):
        data_root(tmp_path / "data")
    monkeypatch.delenv("DATAHUB_DATA_ROOT", raising=False)
    with pytest.raises(ValueError):
        data_root()


@pytest.mark.parametrize(
    "tf,step", [("1m", 60000), ("5m", 300000), ("15m", 900000), ("1h", 3600000), ("4h", 14400000)]
)
def test_timeframes(tf, step):
    report = validate(normalize(candles(step=step), "ohlcv"), "ohlcv", tf, START, START + 2 * step)
    assert report["status"] == "VALID"
    assert report["expected_rows"] == 2


@pytest.mark.parametrize(
    "case", ["duplicate", "missing", "invalid", "nan", "negative_volume", "misaligned", "outside"]
)
def test_bad_candles(case):
    rows = candles()
    if case == "duplicate":
        rows.append(rows[0])
    elif case == "missing":
        rows.pop(0)
    elif case == "invalid":
        rows[0][2] = "1"
    elif case == "nan":
        rows[0][1] = "NaN"
    elif case == "negative_volume":
        rows[0][5] = "-1"
    elif case == "misaligned":
        rows[0][0] += 1
    else:
        rows[0][0] -= 60000
    assert (
        validate(normalize(rows, "ohlcv"), "ohlcv", "1m", START, START + 120000)["status"]
        == "QUARANTINED"
    )


def test_funding_not_assumed_eight_hours():
    table = normalize(
        [
            {"fundingTime": START, "fundingRate": "-0.001"},
            {"fundingTime": START + 3600000, "fundingRate": "0.001"},
        ],
        "funding",
    )
    report = validate(table, "funding", "1m", START, START + 86400000)
    assert report["status"] == "VALID" and report["expected_rows"] is None


def test_storage_manifest_offline_and_tamper(tmp_path):
    store = Store(tmp_path)
    rid = store.put(
        normalize(candles(), "ohlcv"),
        "ohlcv",
        "BTCUSDT",
        "1m",
        START,
        START + 120000,
        {"source": "fixture"},
    )
    doc = store.manifest("v1", [rid])
    assert store.verify("v1") == doc
    assert store.scan("v1", "ohlcv").num_rows == 2
    assert store.manifest("v1", [rid]) == doc
    path = tmp_path / doc["partitions"][0]["path"]
    path.write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="SHA256"):
        store.verify("v1")


def test_quarantine(tmp_path):
    store = Store(tmp_path)
    with pytest.raises(ValueError, match="Quarantined"):
        store.put(
            normalize(candles(1), "ohlcv"), "ohlcv", "BTCUSDT", "1m", START, START + 120000, {}
        )
    rid = store.catalog()[0]["receipt_id"]
    assert list((tmp_path / "quarantine").rglob("*.parquet"))
    with pytest.raises(ValueError, match="Quarantine"):
        store.manifest("bad", [rid])


def test_immutable_and_overlap(tmp_path):
    store = Store(tmp_path)
    a = store.put(
        normalize(candles(), "ohlcv"), "ohlcv", "BTCUSDT", "1m", START, START + 120000, {}
    )
    b = store.put(
        normalize(candles(1), "ohlcv"), "ohlcv", "BTCUSDT", "1m", START, START + 60000, {}
    )
    store.manifest("v1", [a])
    with pytest.raises(ValueError, match="Immutable"):
        store.manifest("v1", [b])
    with pytest.raises(ValueError, match="Overlapping"):
        store.manifest("v2", [a, b])


def test_resume_after_second_day_failure(tmp_path):
    class Adapter:
        name = "fixture"
        calls = []
        failed = False

        def fetch(self, kind, symbol, tf, start, end):
            self.calls.append(start)
            if start > START and not self.failed:
                self.failed = True
                raise RuntimeError("interrupted")
            rows = [[t, "10", "12", "9", "11", "2"] for t in range(start, end, 14400000)]
            return normalize(rows, kind), {"source": "fixture"}

    adapter = Adapter()
    store = Store(tmp_path)
    with pytest.raises(RuntimeError):
        sync(store, adapter, "ohlcv", "BTCUSDT", "4h", START, START + 2 * 86400000)
    ids = sync(store, adapter, "ohlcv", "BTCUSDT", "4h", START, START + 2 * 86400000)
    assert adapter.calls.count(START) == 1
    assert len(ids) == 2
    assert sync(store, adapter, "ohlcv", "BTCUSDT", "4h", START, START + 2 * 86400000) == ids
    assert len(adapter.calls) == 3


def test_rest_pagination_and_index_pair():
    requests = []

    def handler(request):
        requests.append(request)
        start = int(request.url.params["startTime"])
        page = [r for r in candles() if r[0] >= start][:1]
        return httpx.Response(200, json=page)

    rest = Rest(HTTP(httpx.Client(transport=httpx.MockTransport(handler)), sleep=lambda _: None))
    table, _ = rest.fetch("index_price", "BTCUSDT", "1m", START, START + 120000)
    assert table.num_rows == 2
    assert "pair" in requests[0].url.params and "symbol" not in requests[0].url.params
    assert table["volume"].null_count == 2


def test_retry_and_ban():
    codes = iter([429, 503, 200])
    delays = []
    client = httpx.Client(
        transport=httpx.MockTransport(lambda r: httpx.Response(next(codes), json=[]))
    )
    assert HTTP(client, sleep=delays.append).get("https://example.com").status_code == 200
    assert len(delays) == 2
    client = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(418)))
    with pytest.raises(httpx.HTTPStatusError):
        HTTP(client, sleep=delays.append).get("https://example.com")
    assert len(delays) == 2


@pytest.mark.parametrize("bad_checksum", [False, True])
def test_vision_header_checksum(bad_checksum):
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as z:
        z.writestr(
            "BTCUSDT-1m-2024-01-01.csv",
            "open_time,open,high,low,close,volume\n"
            + "\n".join(",".join(map(str, row)) for row in candles()),
        )
    raw = stream.getvalue()
    checksum = "0" * 64 if bad_checksum else hashlib.sha256(raw).hexdigest()

    def handler(request):
        return (
            httpx.Response(200, text=checksum + "  file.zip")
            if str(request.url).endswith("CHECKSUM")
            else httpx.Response(200, content=raw)
        )

    vision = Vision(HTTP(httpx.Client(transport=httpx.MockTransport(handler))))
    if bad_checksum:
        with pytest.raises(ValueError, match="SHA256"):
            vision.fetch("ohlcv", "BTCUSDT", "1m", START, START + 86400000)
    else:
        table, provenance = vision.fetch("ohlcv", "BTCUSDT", "1m", START, START + 86400000)
        assert table.num_rows == 2 and provenance["archive_sha256"] == checksum


def test_lifecycle_survivorship_and_knowledge(tmp_path):
    store = Store(tmp_path)
    evidence = {
        "source": "audited_fixture",
        "complete": True,
        "coverage_start": START,
        "coverage_end": START + 86400000,
        "records": [
            {
                "symbol": "DEADUSDT",
                "listed_at": START - 86400000,
                "delisted_at": START + 60000,
                "known_at": START - 1,
                "source": "announcement_fixture",
            },
            {
                "symbol": "NEWUSDT",
                "listed_at": START + 120000,
                "delisted_at": None,
                "known_at": START - 1,
                "source": "announcement_fixture",
            },
        ],
    }
    key = import_lifecycle(store, evidence)
    assert snapshot(store, "past", key, START, START)["symbols"] == ["DEADUSDT"]
    assert snapshot(store, "later", key, START + 120000, START)["symbols"] == ["NEWUSDT"]
    with pytest.raises(ValueError, match="knowledge"):
        snapshot(store, "lookahead", key, START, START - 2)
    evidence["complete"] = False
    key = import_lifecycle(store, evidence)
    with pytest.raises(ValueError, match="incomplete"):
        snapshot(store, "incomplete", key, START, START)
    mid = capture_metadata(store, {"symbols": [{"symbol": "NEWUSDT"}]})
    assert store.json(f"metadata/{mid}.json")["historical_complete"] is False


def test_cli_real_process(tmp_path):
    proc = subprocess.run(
        [sys.executable, "-m", "datahub.cli", "--root", str(tmp_path), "init"],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0 and json.loads(proc.stdout)["data_root"] == str(tmp_path.resolve())
    proc = subprocess.run(
        [sys.executable, "-m", "datahub.cli", "--root", str(tmp_path), "verify", "missing"],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 1 and "error" in json.loads(proc.stderr)


def test_vision_monthly_archive_and_cached_sync(tmp_path):
    start = 1767225600000  # 2026-01-01T00:00:00Z
    end = 1769904000000  # 2026-02-01T00:00:00Z
    step = 900000
    rows = [[t, "10", "12", "9", "11", "2"] for t in range(start, end, step)]

    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as z:
        z.writestr(
            "BTCUSDT-15m-2026-01.csv",
            "open_time,open,high,low,close,volume\n"
            + "\n".join(",".join(map(str, row)) for row in rows),
        )
    raw = stream.getvalue()
    checksum = hashlib.sha256(raw).hexdigest()
    calls = []

    def handler(request):
        calls.append(str(request.url))
        if str(request.url).endswith(".CHECKSUM"):
            return httpx.Response(200, text=checksum + "  file.zip")
        return httpx.Response(200, content=raw)

    vision = Vision(HTTP(httpx.Client(transport=httpx.MockTransport(handler))))
    table, provenance = vision.fetch_month("ohlcv", "BTCUSDT", "15m", start, end)
    assert table.num_rows == 2976
    assert provenance["source"] == "binance_vision_monthly"
    assert "/monthly/klines/BTCUSDT/15m/" in provenance["url"]

    store = Store(tmp_path)
    ids = sync_month(store, vision, "ohlcv", "BTCUSDT", "15m", start, end)
    assert len(ids) == 1
    calls_after_first = len(calls)

    def offline(request):
        raise AssertionError("Network forbidden during cached monthly sync")

    vision.http = HTTP(httpx.Client(transport=httpx.MockTransport(offline)))
    assert sync_month(store, vision, "ohlcv", "BTCUSDT", "15m", start, end) == ids
    assert len(calls) == calls_after_first
