import csv
from datetime import UTC, datetime

import pyarrow.parquet as pq

from datahub.export import fetch_ohlcv_csv
from datahub.schemas import normalize
from datahub.storage import Store

START = 1_704_067_200_000


def test_fetch_csv_exports_verified_cached_partition(tmp_path, monkeypatch):
    store = Store(tmp_path / "data")
    rows = [
        [START, "10", "12", "9", "11", "2"],
        [START + 60_000, "11", "13", "10", "12", "3"],
    ]
    rid = store.put(
        normalize(rows, "ohlcv"),
        "ohlcv",
        "BTCUSDT",
        "1m",
        START,
        START + 120_000,
        {"source": "fixture"},
    )

    calls = []

    def fake_sync(store_arg, adapter, kind, symbol, timeframe, start, end, fallback_adapter=None):
        calls.append((adapter.name, fallback_adapter.name, kind, symbol, timeframe, start, end))
        assert store_arg is store
        return [rid]

    monkeypatch.setattr("datahub.export.sync", fake_sync)
    output = tmp_path / "btc.csv"
    result = fetch_ohlcv_csv(
        store,
        "BTCUSDT",
        "1m",
        START,
        START + 120_000,
        output,
    )

    assert result["status"] == "READY"
    assert result["rows"] == 2
    assert calls == [("vision", "rest", "ohlcv", "BTCUSDT", "1m", START, START + 120_000)]

    with output.open(newline="", encoding="utf-8") as stream:
        exported = list(csv.reader(stream))
    assert exported[0] == ["timestamp", "open", "high", "low", "close", "volume"]
    assert exported[1][0] == str(START)
    assert exported[2][0] == str(START + 60_000)


def test_fetch_csv_decomposes_multi_month_range_into_monthly_and_daily(tmp_path, monkeypatch):
    root = tmp_path / "data"
    root.mkdir()
    objects = root / "objects"
    objects.mkdir()

    jan = int(datetime(2026, 1, 1, tzinfo=UTC).timestamp() * 1000)
    feb = int(datetime(2026, 2, 1, tzinfo=UTC).timestamp() * 1000)
    mar = int(datetime(2026, 3, 1, tzinfo=UTC).timestamp() * 1000)
    mar15 = int(datetime(2026, 3, 15, tzinfo=UTC).timestamp() * 1000)

    receipts = {}
    for rid, ts in [("jan", jan), ("feb", feb), ("mar", mar)]:
        path = objects / f"{rid}.parquet"
        table = normalize([[ts, "10", "12", "9", "11", "2"]], "ohlcv")
        pq.write_table(table, path)
        receipts[rid] = {"path": f"objects/{rid}.parquet"}

    class FakeStore:
        def __init__(self, root):
            self.root = root

        def receipt(self, rid):
            return receipts[rid]

    calls = []

    def fake_month(store_arg, adapter, kind, symbol, timeframe, start, end, fallback_adapter=None):
        calls.append(("month", start, end))
        return ["jan" if start == jan else "feb"]

    def fake_day(store_arg, adapter, kind, symbol, timeframe, start, end, fallback_adapter=None):
        calls.append(("day", adapter.name, fallback_adapter.name, start, end))
        return ["mar"]

    monkeypatch.setattr("datahub.export.sync_month", fake_month)
    monkeypatch.setattr("datahub.export.sync", fake_day)

    output = tmp_path / "range.csv"
    result = fetch_ohlcv_csv(FakeStore(root), "BTCUSDT", "15m", jan, mar15, output)

    assert result["rows"] == 3
    assert calls == [
        ("month", jan, feb),
        ("month", feb, mar),
        ("day", "vision", "rest", mar, mar15),
    ]
