"""Content-addressed Parquet objects, append-only receipts, atomic publication."""

import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
from filelock import FileLock

from .core import atomic_json, fingerprint, safe_name, safe_symbol, sha256, utcnow
from .schemas import CANDLE, FUNDING
from .validation import validate


class Store:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def lock(self):
        return FileLock(str(self.root / "writer.lock"), timeout=30)

    def json(self, relative):
        return json.loads((self.root / relative).read_text())

    def immutable(self, relative, document):
        path = self.root / relative
        if path.exists():
            if json.loads(path.read_text()) != document:
                raise ValueError(f"Immutable object already exists: {relative}")
        else:
            atomic_json(path, document)

    def put(self, table, kind, symbol, timeframe, start, end, provenance):
        safe_symbol(symbol)
        safe_name(kind)
        safe_name(timeframe)
        report = validate(table, kind, timeframe, start, end)
        table = table.sort_by([("timestamp", "ascending")])
        with self.lock():
            fd, temp = tempfile.mkstemp(dir=self.root, suffix=".parquet")
            os.close(fd)
            try:
                pq.write_table(table, temp, compression="zstd", version="2.6")
                digest = sha256(temp)
                area = "objects" if report["status"] == "VALID" else "quarantine"
                date = datetime.fromtimestamp(start / 1000, UTC)
                relative = (
                    f"{area}/market=binance_usdm/dataset={kind}/timeframe={timeframe}/"
                    f"symbol={symbol}/year={date.year}/month={date.month:02d}/"
                    f"{digest}.parquet"
                )
                dest = self.root / relative
                dest.parent.mkdir(parents=True, exist_ok=True)
                if dest.exists():
                    if sha256(dest) != digest:
                        raise ValueError("Existing content-addressed object is corrupt")
                else:
                    os.replace(temp, dest)
            finally:
                Path(temp).unlink(missing_ok=True)
            receipt = {
                "schema_version": 1,
                "market": "binance_usdm",
                "dataset": kind,
                "symbol": symbol,
                "timeframe": timeframe,
                "start": start,
                "end": end,
                "sha256": digest,
                "path": relative,
                "validation": report,
                "provenance": provenance,
                "download_timestamp": utcnow(),
            }
            rid = fingerprint(receipt)
            self.immutable(f"receipts/{rid}.json", receipt)
        if report["status"] != "VALID":
            raise ValueError(f"Quarantined {rid}: {report}")
        return rid

    def receipt(self, rid):
        safe_name(rid)
        receipt = self.json(f"receipts/{rid}.json")
        if fingerprint(receipt) != rid:
            raise ValueError("Receipt fingerprint mismatch")
        path = (self.root / receipt["path"]).resolve()
        if not path.is_relative_to(self.root.resolve()) or sha256(path) != receipt["sha256"]:
            raise ValueError("Partition SHA256 mismatch or unsafe path")
        if receipt["validation"]["status"] != "VALID":
            raise ValueError("Quarantine cannot enter dataset")
        return receipt

    def manifest(self, name, receipt_ids, universe=None, lineage=None):
        safe_name(name)
        if not receipt_ids:
            raise ValueError("Cannot publish empty dataset")
        partitions = [dict(receipt_id=r, **self.receipt(r)) for r in sorted(set(receipt_ids))]
        coverage = {}
        for part in partitions:
            key = (part["dataset"], part["symbol"], part["timeframe"])
            for start, end in coverage.get(key, []):
                if part["start"] < end and part["end"] > start:
                    raise ValueError("Overlapping partitions in dataset")
            coverage.setdefault(key, []).append((part["start"], part["end"]))
        snapshot = None
        if universe:
            snapshot = self.json(f"universes/{safe_name(universe)}.json")
            body = {k: v for k, v in snapshot.items() if k != "fingerprint"}
            if fingerprint(body) != snapshot["fingerprint"]:
                raise ValueError("Universe fingerprint mismatch")
        body = {
            "schema_version": 1,
            "dataset_id": name,
            "partitions": partitions,
            "universe": snapshot,
        }
        if lineage is not None:
            body["lineage"] = lineage
        document = dict(body, fingerprint=fingerprint(body))
        with self.lock():
            self.immutable(f"manifests/{name}.json", document)
        return document

    def verify(self, name):
        doc = self.json(f"manifests/{safe_name(name)}.json")
        body = {k: v for k, v in doc.items() if k != "fingerprint"}
        if fingerprint(body) != doc["fingerprint"]:
            raise ValueError("Manifest fingerprint mismatch")
        for part in doc["partitions"]:
            receipt = self.receipt(part["receipt_id"])
            if dict(receipt_id=part["receipt_id"], **receipt) != part:
                raise ValueError("Manifest/receipt mismatch")
        return doc

    def catalog(self):
        return [
            dict(receipt_id=p.stem, **json.loads(p.read_text()))
            for p in sorted((self.root / "receipts").glob("*.json"))
        ]

    def scan(self, name, kind, symbol=None, timeframe=None):
        """Offline verified read. Exact manifest paths only; never wildcard a mutable root."""
        doc = self.verify(name)
        parts = [
            p
            for p in doc["partitions"]
            if p["dataset"] == kind
            and (symbol is None or p["symbol"] == symbol)
            and (timeframe is None or p["timeframe"] == timeframe)
        ]
        if not parts:
            raise ValueError("No matching partitions")
        if len({(p["symbol"], p["timeframe"]) for p in parts}) != 1:
            raise ValueError("Select one symbol and timeframe per scan")
        with duckdb.connect() as con:
            con.execute("SET TimeZone='UTC'")
            return (
                con.read_parquet(
                    [str(self.root / p["path"]) for p in parts], hive_partitioning=False
                )
                .order("timestamp")
                .to_arrow_table()
                .cast(FUNDING if kind == "funding" else CANDLE)
            )
