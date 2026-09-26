"""Release publication and integrity/health auditing."""

import json

from .core import fingerprint, safe_name
from .coverage import catalog_index, coverage
from .planning import (
    _aligned_interval,
    inventory_intervals,
    lifecycle_intervals,
    load_plan,
    plan_receipts,
    plan_status,
)


def _release_intervals(store, plan):
    if plan.get("evidence_type") == "archive_inventory":
        _, intervals = inventory_intervals(
            store,
            plan["inventory_id"],
            plan["requested_start"],
            plan["requested_end"],
        )
        return intervals
    return lifecycle_intervals(
        store,
        plan["lifecycle_fingerprint"],
        plan["requested_start"],
        plan["requested_end"],
    )


def coverage_attestation(store, plan):
    """Recompute fixed-grid coverage after a plan reports complete."""
    index = catalog_index(store)
    intervals = _release_intervals(store, plan)
    rows = []
    for active in intervals:
        for kind in plan["datasets"]:
            selected = ("1m",) if kind == "funding" else plan["timeframes"]
            for timeframe in selected:
                left, right = _aligned_interval(kind, timeframe, active["start"], active["end"])
                if left >= right:
                    continue
                report = coverage(
                    store,
                    kind,
                    active["symbol"],
                    timeframe,
                    left,
                    right,
                    index=index,
                )
                if report["conflicts"]:
                    raise ValueError(
                        f"Release blocked by coverage conflict: "
                        f"{kind}/{active['symbol']}/{timeframe}"
                    )
                if kind != "funding" and report["status"] != "COMPLETE":
                    raise ValueError(
                        f"Release blocked by incomplete coverage: "
                        f"{kind}/{active['symbol']}/{timeframe}"
                    )
                rows.append(
                    {
                        "dataset": kind,
                        "symbol": active["symbol"],
                        "timeframe": timeframe,
                        "start": left,
                        "end": right,
                        "status": report["status"],
                    }
                )
    body = {
        "schema_version": 1,
        "plan_id": plan["plan_id"],
        "plan_fingerprint": plan["fingerprint"],
        "series": rows,
        "fixed_grid_complete": all(
            row["status"] == "COMPLETE" for row in rows if row["dataset"] != "funding"
        ),
        "funding_semantics": "event_stream_not_schedule-certified",
    }
    return dict(body, fingerprint=fingerprint(body))


def publish_plan(store, manifest_name, plan_name, universe=None):
    """Publish only after plan completion and a fresh coverage attestation."""
    safe_name(manifest_name)
    plan = load_plan(store, plan_name)
    if plan_status(store, plan_name)["status"] != "COMPLETE":
        raise ValueError("Release blocked: backfill plan is not complete")
    receipts = plan_receipts(store, plan_name, require_complete=True)
    if not receipts:
        raise ValueError("Release blocked: no verified receipts")
    attestation = coverage_attestation(store, plan)
    lineage = {
        "plan_id": plan_name,
        "plan_fingerprint": plan["fingerprint"],
        "evidence_type": plan.get("evidence_type"),
        "requested_start": plan["requested_start"],
        "requested_end": plan["requested_end"],
        "coverage_attestation": attestation,
    }
    if plan.get("inventory_id"):
        lineage["inventory_id"] = plan["inventory_id"]
        lineage["inventory_fingerprint"] = plan["inventory_fingerprint"]
    if plan.get("lifecycle_fingerprint"):
        lineage["lifecycle_fingerprint"] = plan["lifecycle_fingerprint"]
    return store.manifest(manifest_name, receipts, universe=universe, lineage=lineage)


def doctor(store, deep=False, strict=False):
    """Audit immutable control objects and summarize operational debt."""
    problems = []
    warnings = []

    manifest_count = 0
    for path in sorted((store.root / "manifests").glob("*.json")):
        manifest_count += 1
        try:
            store.verify(path.stem)
        except Exception as exc:
            problems.append({"type": "manifest", "id": path.stem, "error": str(exc)})

    inventory_count = 0
    if (store.root / "inventories").exists():
        from .inventory import load_inventory

        for path in sorted((store.root / "inventories").glob("*.json")):
            inventory_count += 1
            try:
                load_inventory(store, path.stem)
            except Exception as exc:
                problems.append({"type": "inventory", "id": path.stem, "error": str(exc)})

    plan_counts = {"COMPLETE": 0, "INCOMPLETE": 0, "PENDING": 0}
    if (store.root / "plans").exists():
        for path in sorted((store.root / "plans").glob("*.json")):
            try:
                load_plan(store, path.stem)
                status = plan_status(store, path.stem)["status"]
                plan_counts[status] = plan_counts.get(status, 0) + 1
                if status != "COMPLETE":
                    warnings.append({"type": "plan", "id": path.stem, "status": status})
            except Exception as exc:
                problems.append({"type": "plan", "id": path.stem, "error": str(exc)})

    checkpoint_counts = {}
    if (store.root / "checkpoints").exists():
        for path in (store.root / "checkpoints").glob("*.json"):
            try:
                state = json.loads(path.read_text())
                status = state.get("status", "UNKNOWN")
                checkpoint_counts[status] = checkpoint_counts.get(status, 0) + 1
            except Exception as exc:
                problems.append({"type": "checkpoint", "id": path.stem, "error": str(exc)})
    if checkpoint_counts.get("FAILED"):
        warnings.append({"type": "failed_checkpoints", "count": checkpoint_counts["FAILED"]})

    quarantine_files = len(list((store.root / "quarantine").rglob("*.parquet")))
    if quarantine_files:
        warnings.append({"type": "quarantine_files", "count": quarantine_files})

    receipt_count = len(list((store.root / "receipts").glob("*.json")))
    if deep:
        for item in store.catalog():
            if item["validation"]["status"] != "VALID":
                continue
            try:
                store.receipt(item["receipt_id"])
            except Exception as exc:
                problems.append({"type": "receipt", "id": item["receipt_id"], "error": str(exc)})

    status = "FAIL" if problems or (strict and warnings) else "PASS"
    return {
        "status": status,
        "strict": strict,
        "deep": deep,
        "manifests": manifest_count,
        "inventories": inventory_count,
        "plans": plan_counts,
        "receipts": receipt_count,
        "checkpoints": checkpoint_counts,
        "quarantine_files": quarantine_files,
        "problems": problems,
        "warnings": warnings,
    }
