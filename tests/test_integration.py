"""Adapter -> validation -> ZSTD storage -> versioned offline DuckDB reader."""

import hashlib
import io
import zipfile

import httpx
import pyarrow.parquet as pq

from datahub.ingest import HTTP, Vision
from datahub.storage import Store
from datahub.sync import sync


def test_full_archive_pipeline_and_offline_resume(tmp_path):
    start = 1704067200000
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr(
            "fixture.csv",
            "\n".join(f"{t},10,12,9,11,2" for t in range(start, start + 86400000, 60000)),
        )
    payload = output.getvalue()
    calls = []

    def handler(request):
        calls.append(str(request.url))
        if str(request.url).endswith(".CHECKSUM"):
            return httpx.Response(200, text=hashlib.sha256(payload).hexdigest() + " fixture.zip")
        return httpx.Response(200, content=payload)

    adapter = Vision(HTTP(httpx.Client(transport=httpx.MockTransport(handler))))
    store = Store(tmp_path)
    ids = sync(store, adapter, "ohlcv", "BTCUSDT", "1m", start, start + 86400000)
    manifest = store.manifest("integration_v1", ids)
    assert len(calls) == 2
    path = tmp_path / manifest["partitions"][0]["path"]
    assert pq.ParquetFile(path).metadata.row_group(0).column(0).compression == "ZSTD"

    def offline(request):
        raise AssertionError("Network forbidden during cached sync or offline reads")

    adapter.http = HTTP(httpx.Client(transport=httpx.MockTransport(offline)))
    assert sync(store, adapter, "ohlcv", "BTCUSDT", "1m", start, start + 86400000) == ids
    table = store.scan("integration_v1", "ohlcv", "BTCUSDT", "1m")
    assert table.num_rows == 1440
    assert str(table.schema.field("timestamp").type) == "timestamp[ms, tz=UTC]"
