import argparse
import json
import sys
from pathlib import Path

from .core import INTERVALS, KINDS, data_root, millis
from .ingest import Rest, Vision
from .storage import Store
from .sync import sync
from .universe import capture_metadata, import_lifecycle, snapshot


def main(argv=None):
    parser = argparse.ArgumentParser(description="Versioned Binance USD-M market data")
    parser.add_argument("--root")
    parser.add_argument("--config")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init")
    s = sub.add_parser("sync")
    s.add_argument("--source", choices=["vision", "rest"], default="vision")
    s.add_argument("--dataset", choices=KINDS, default="ohlcv")
    s.add_argument("--symbol", required=True)
    s.add_argument("--timeframe", choices=INTERVALS, default="1m")
    s.add_argument("--start", required=True)
    s.add_argument("--end", required=True)
    sub.add_parser("catalog")
    m = sub.add_parser("manifest")
    m.add_argument("name")
    m.add_argument("--receipts", nargs="+", required=True)
    m.add_argument("--universe")
    v = sub.add_parser("verify")
    v.add_argument("name")
    q = sub.add_parser("inspect")
    q.add_argument("name")
    q.add_argument("--dataset", choices=KINDS, default="ohlcv")
    q.add_argument("--symbol", required=True)
    q.add_argument("--timeframe", choices=INTERVALS, required=True)
    sub.add_parser("metadata-sync")
    lifecycle_parser = sub.add_parser("lifecycle-import")
    lifecycle_parser.add_argument("file")
    u = sub.add_parser("universe")
    u.add_argument("name")
    u.add_argument("--lifecycle", required=True)
    u.add_argument("--as-of", required=True)
    u.add_argument("--known-at", required=True)
    args = parser.parse_args(argv)
    try:
        store = Store(data_root(args.root, args.config))
        if args.command == "init":
            result = {"data_root": str(store.root)}
        elif args.command == "sync":
            adapter = Vision() if args.source == "vision" else Rest()
            result = {
                "receipts": sync(
                    store,
                    adapter,
                    args.dataset,
                    args.symbol,
                    args.timeframe,
                    millis(args.start),
                    millis(args.end),
                )
            }
        elif args.command == "catalog":
            result = store.catalog()
        elif args.command == "manifest":
            result = store.manifest(args.name, args.receipts, args.universe)
        elif args.command == "verify":
            result = {"status": "PASS", "fingerprint": store.verify(args.name)["fingerprint"]}
        elif args.command == "inspect":
            table = store.scan(args.name, args.dataset, args.symbol, args.timeframe)
            result = {"rows": table.num_rows, "schema": str(table.schema)}
        elif args.command == "metadata-sync":
            result = {"metadata_id": capture_metadata(store, Rest().metadata())}
        elif args.command == "lifecycle-import":
            result = {
                "lifecycle_id": import_lifecycle(store, json.loads(Path(args.file).read_text()))
            }
        else:
            result = snapshot(
                store, args.name, args.lifecycle, millis(args.as_of), millis(args.known_at)
            )
        print(json.dumps(result, indent=2))
        return 0
    except Exception as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
