import csv

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
    assert calls == [
        ("vision", "rest", "ohlcv", "BTCUSDT", "1m", START, START + 120_000)
    ]

    with output.open(newline="", encoding="utf-8") as stream:
        exported = list(csv.reader(stream))
    assert exported[0] == ["timestamp", "open", "high", "low", "close", "volume"]
    assert exported[1][0] == str(START)
    assert exported[2][0] == str(START + 60_000)
