import argparse
import json
import sys
from pathlib import Path

from .core import INTERVALS, KINDS, data_root, millis
from .coverage import coverage
from .ingest import Rest, Vision
from .inventory import archive_snapshot, scan_vision_inventory
from .planning import (
    coverage_matrix,
    create_backfill_plan,
    create_inventory_backfill_plan,
    plan_status,
    run_plan,
)
from .release import doctor, publish_plan
from .remote import S3Remote
from .remote import publish_release as publish_remote_release
from .remote import pull_release as pull_remote_release
from .storage import Store
from .sync import sync
from .universe import capture_metadata, import_lifecycle, snapshot


def _add_range(parser, required=True):
    parser.add_argument("--start", required=required)
    parser.add_argument("--end", required=required)


def _add_selection(parser):
    parser.add_argument("--datasets", nargs="+", choices=KINDS, default=["ohlcv"])
    parser.add_argument("--timeframes", nargs="+", choices=INTERVALS, default=list(INTERVALS))


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
    _add_range(s)

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

    inv = sub.add_parser("inventory-sync")
    inv.add_argument("name")
    _add_range(inv)
    inv.add_argument("--symbols", nargs="+")
    inv.add_argument("--symbol-regex")
    inv.add_argument("--max-gap-days", type=int, default=3)
    inv.add_argument("--no-boundary-probe", action="store_true")

    au = sub.add_parser("archive-universe")
    au.add_argument("name")
    au.add_argument("--inventory", required=True)
    au.add_argument("--as-of", required=True)

    c = sub.add_parser("coverage")
    c.add_argument("--dataset", choices=KINDS, default="ohlcv")
    c.add_argument("--symbol", required=True)
    c.add_argument("--timeframe", choices=INTERVALS, default="1m")
    _add_range(c)

    cm = sub.add_parser("coverage-matrix")
    cm.add_argument("--lifecycle", required=True)
    _add_selection(cm)
    _add_range(cm)

    bp = sub.add_parser("backfill-plan")
    bp.add_argument("name")
    bp.add_argument("--lifecycle", required=True)
    bp.add_argument("--source-policy", choices=["auto", "vision", "rest"], default="auto")
    _add_selection(bp)
    _add_range(bp)

    ibp = sub.add_parser("inventory-backfill-plan")
    ibp.add_argument("name")
    ibp.add_argument("--inventory", required=True)
    ibp.add_argument(
        "--source-policy",
        choices=["vision-rest", "vision", "rest"],
        default="vision-rest",
    )
    _add_selection(ibp)
    _add_range(ibp, required=False)

    br = sub.add_parser("backfill-run")
    br.add_argument("name")
    br.add_argument("--max-jobs", type=int)

    bs = sub.add_parser("backfill-status")
    bs.add_argument("name")

    rel = sub.add_parser("release")
    rel.add_argument("name")
    rel.add_argument("--plan", required=True)
    rel.add_argument("--universe")

    remote_publish = sub.add_parser("remote-publish")
    remote_publish.add_argument("name")

    remote_pull = sub.add_parser("remote-pull")
    remote_pull.add_argument("name")

    doc = sub.add_parser("doctor")
    doc.add_argument("--deep", action="store_true")
    doc.add_argument("--strict", action="store_true")

    args = parser.parse_args(argv)
    exit_code = 0
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
        elif args.command == "universe":
            result = snapshot(
                store, args.name, args.lifecycle, millis(args.as_of), millis(args.known_at)
            )
        elif args.command == "inventory-sync":
            result = scan_vision_inventory(
                store,
                args.name,
                millis(args.start),
                millis(args.end),
                symbols=args.symbols,
                symbol_regex=args.symbol_regex,
                max_gap_days=args.max_gap_days,
                probe_boundaries=not args.no_boundary_probe,
            )
        elif args.command == "archive-universe":
            result = archive_snapshot(store, args.name, args.inventory, millis(args.as_of))
        elif args.command == "coverage":
            result = coverage(
                store,
                args.dataset,
                args.symbol,
                args.timeframe,
                millis(args.start),
                millis(args.end),
            )
        elif args.command == "coverage-matrix":
            result = coverage_matrix(
                store,
                args.lifecycle,
                args.datasets,
                args.timeframes,
                millis(args.start),
                millis(args.end),
            )
        elif args.command == "backfill-plan":
            result = create_backfill_plan(
                store,
                args.name,
                args.lifecycle,
                args.datasets,
                args.timeframes,
                millis(args.start),
                millis(args.end),
                args.source_policy,
            )
        elif args.command == "inventory-backfill-plan":
            if bool(args.start) != bool(args.end):
                raise ValueError("--start and --end must be supplied together")
            result = create_inventory_backfill_plan(
                store,
                args.name,
                args.inventory,
                args.datasets,
                args.timeframes,
                millis(args.start) if args.start else None,
                millis(args.end) if args.end else None,
                args.source_policy,
            )
        elif args.command == "backfill-run":
            result = run_plan(store, args.name, args.max_jobs)
            if result["status"] == "INCOMPLETE":
                exit_code = 2
        elif args.command == "backfill-status":
            result = plan_status(store, args.name)
        elif args.command == "release":
            result = publish_plan(store, args.name, args.plan, args.universe)
        elif args.command == "remote-publish":
            result = publish_remote_release(store, S3Remote.from_env(), args.name)
        elif args.command == "remote-pull":
            result = pull_remote_release(store, S3Remote.from_env(), args.name)
        else:
            result = doctor(store, deep=args.deep, strict=args.strict)
            if result["status"] != "PASS":
                exit_code = 2
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return exit_code
    except Exception as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
