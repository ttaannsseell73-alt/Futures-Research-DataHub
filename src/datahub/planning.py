"""Deterministic survivorship-safe coverage planning and resumable backfill execution."""

from pathlib import Path

from filelock import FileLock

from .core import INTERVALS, KINDS, atomic_json, fingerprint, safe_name, utcnow
from .coverage import catalog_index, coverage
from .ingest import Rest, Vision
from .sync import sync

DAY = 86_400_000


def _load_lifecycle(store, lifecycle, start, end):
    safe_name(lifecycle)
    evidence = store.json(f"lifecycles/{lifecycle}.json")
    if fingerprint(evidence) != lifecycle:
        raise ValueError("Lifecycle fingerprint mismatch")
    if not evidence["complete"] or not (
        evidence["coverage_start"] <= start < end <= evidence["coverage_end"]
    ):
        raise ValueError("Requested range lacks complete historical lifecycle coverage")
    return evidence


def lifecycle_intervals(store, lifecycle, start, end):
    evidence = _load_lifecycle(store, lifecycle, start, end)
    intervals = []
    for record in evidence["records"]:
        left = max(start, record["listed_at"])
        right = min(end, record["delisted_at"] if record["delisted_at"] is not None else end)
        if left < right:
            intervals.append({"symbol": record["symbol"], "start": left, "end": right})
    intervals.sort(key=lambda item: (item["symbol"], item["start"], item["end"]))
    return intervals


def _aligned_interval(kind, timeframe, start, end):
    if kind == "funding":
        return start, end
    step = INTERVALS[timeframe]
    left = ((start + step - 1) // step) * step
    right = (end // step) * step
    return left, right


def _split_days(start, end):
    cursor = start
    while cursor < end:
        stop = min(end, (cursor // DAY + 1) * DAY)
        yield cursor, stop
        cursor = stop


def _source(kind, start, end, policy):
    if kind == "funding":
        if policy == "vision":
            raise ValueError("Funding backfill requires REST")
        return "rest"
    if policy == "rest":
        return "rest"
    full_day = start % DAY == 0 and end - start == DAY
    if policy == "vision":
        if not full_day:
            raise ValueError("Vision-only planning cannot cover partial UTC days")
        return "vision"
    return "vision" if full_day else "rest"


def _normalized_inputs(datasets, timeframes):
    datasets = tuple(dict.fromkeys(datasets))
    timeframes = tuple(dict.fromkeys(timeframes))
    if not datasets or any(kind not in KINDS for kind in datasets):
        raise ValueError("Unsupported or empty dataset selection")
    if not timeframes or any(tf not in INTERVALS for tf in timeframes):
        raise ValueError("Unsupported or empty timeframe selection")
    return datasets, timeframes


def coverage_matrix(store, lifecycle, datasets, timeframes, start, end):
    datasets, timeframes = _normalized_inputs(datasets, timeframes)
    intervals = lifecycle_intervals(store, lifecycle, start, end)
    rows = []
    index = catalog_index(store)
    for active in intervals:
        for kind in datasets:
            selected = ("1m",) if kind == "funding" else timeframes
            for timeframe in selected:
                left, right = _aligned_interval(kind, timeframe, active["start"], active["end"])
                if left >= right:
                    continue
                report = coverage(
                    store, kind, active["symbol"], timeframe, left, right, index=index
                )
                rows.append(report)
    counts = {}
    for row in rows:
        counts[row["status"]] = counts.get(row["status"], 0) + 1
    return {
        "schema_version": 1,
        "lifecycle_fingerprint": lifecycle,
        "start": start,
        "end": end,
        "rows": rows,
        "status_counts": counts,
    }


def create_backfill_plan(
    store,
    name,
    lifecycle,
    datasets,
    timeframes,
    start,
    end,
    source_policy="auto",
):
    safe_name(name)
    if source_policy not in {"auto", "vision", "rest"}:
        raise ValueError("source_policy must be auto, vision, or rest")
    datasets, timeframes = _normalized_inputs(datasets, timeframes)
    intervals = lifecycle_intervals(store, lifecycle, start, end)
    index = catalog_index(store)
    jobs = []
    complete_series = 0
    for active in intervals:
        for kind in datasets:
            selected = ("1m",) if kind == "funding" else timeframes
            for timeframe in selected:
                left, right = _aligned_interval(kind, timeframe, active["start"], active["end"])
                if left >= right:
                    continue
                report = coverage(
                    store, kind, active["symbol"], timeframe, left, right, index=index
                )
                if report["conflicts"]:
                    raise ValueError(
                        "Coverage conflict for "
                        f"{kind}/{active['symbol']}/{timeframe}; resolve first"
                    )
                gaps = [[left, right]] if kind == "funding" else report["gaps"]
                if not gaps:
                    complete_series += 1
                    continue
                for gap_start, gap_end in gaps:
                    for job_start, job_end in _split_days(gap_start, gap_end):
                        source = _source(kind, job_start, job_end, source_policy)
                        body = {
                            "dataset": kind,
                            "symbol": active["symbol"],
                            "timeframe": timeframe,
                            "start": job_start,
                            "end": job_end,
                            "source": source,
                        }
                        jobs.append(dict(body, job_id=fingerprint(body)))
    jobs.sort(
        key=lambda job: (
            job["symbol"],
            job["dataset"],
            job["timeframe"],
            job["start"],
            job["end"],
        )
    )
    body = {
        "schema_version": 1,
        "plan_id": name,
        "lifecycle_fingerprint": lifecycle,
        "requested_start": start,
        "requested_end": end,
        "datasets": list(datasets),
        "timeframes": list(timeframes),
        "funding_timeframe_namespace": "1m",
        "source_policy": source_policy,
        "preexisting_complete_series": complete_series,
        "jobs": jobs,
    }
    document = dict(body, fingerprint=fingerprint(body))
    with store.lock():
        store.immutable(f"plans/{name}.json", document)
    return document


def load_plan(store, name):
    safe_name(name)
    plan = store.json(f"plans/{name}.json")
    body = {key: value for key, value in plan.items() if key != "fingerprint"}
    if fingerprint(body) != plan["fingerprint"]:
        raise ValueError("Backfill plan fingerprint mismatch")
    return plan


def _state_path(store, name):
    return store.root / f"plan_runs/{safe_name(name)}.json"


def _load_state(store, name, plan):
    path = _state_path(store, name)
    if not path.exists():
        return {
            "schema_version": 1,
            "plan_id": name,
            "plan_fingerprint": plan["fingerprint"],
            "jobs": {},
            "updated_at": None,
        }
    state = store.json(f"plan_runs/{name}.json")
    if state["plan_fingerprint"] != plan["fingerprint"]:
        raise ValueError("Backfill run does not match immutable plan")
    return state


def _summary(plan, state):
    counts = {"PENDING": 0, "COMPLETE": 0, "FAILED": 0}
    for job in plan["jobs"]:
        status = state["jobs"].get(job["job_id"], {}).get("status", "PENDING")
        counts[status] = counts.get(status, 0) + 1
    if not plan["jobs"] or counts["COMPLETE"] == len(plan["jobs"]):
        overall = "COMPLETE"
    elif counts["FAILED"]:
        overall = "INCOMPLETE"
    else:
        overall = "PENDING"
    return {
        "plan_id": plan["plan_id"],
        "plan_fingerprint": plan["fingerprint"],
        "status": overall,
        "counts": counts,
        "total_jobs": len(plan["jobs"]),
    }


def plan_status(store, name):
    plan = load_plan(store, name)
    state = _load_state(store, name, plan)
    return _summary(plan, state)


def run_plan(store, name, max_jobs=None, adapters=None):
    if max_jobs is not None and max_jobs <= 0:
        raise ValueError("max_jobs must be positive")
    plan = load_plan(store, name)
    state_path = _state_path(store, name)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    with FileLock(str(state_path) + ".lock", timeout=30):
        state = _load_state(store, name, plan)
        adapters = adapters or {"vision": Vision(), "rest": Rest()}
        attempted = 0
        for job in plan["jobs"]:
            job_id = job["job_id"]
            previous = state["jobs"].get(job_id, {})
            if previous.get("status") == "COMPLETE":
                try:
                    for receipt_id in previous.get("receipts", []):
                        store.receipt(receipt_id)
                    continue
                except Exception:
                    previous = dict(
                        previous, status="PENDING", error="cached receipt verification failed"
                    )
                    state["jobs"][job_id] = previous
            if max_jobs is not None and attempted >= max_jobs:
                break
            attempted += 1
            entry = {
                "status": "RUNNING",
                "attempts": previous.get("attempts", 0) + 1,
                "updated_at": utcnow(),
            }
            state["jobs"][job_id] = entry
            state["updated_at"] = entry["updated_at"]
            atomic_json(state_path, state)
            try:
                adapter = adapters[job["source"]]
                receipts = sync(
                    store,
                    adapter,
                    job["dataset"],
                    job["symbol"],
                    job["timeframe"],
                    job["start"],
                    job["end"],
                )
                entry.update(status="COMPLETE", receipts=receipts, error=None, updated_at=utcnow())
            except Exception as exc:
                entry.update(status="FAILED", error=str(exc), updated_at=utcnow())
            state["updated_at"] = entry["updated_at"]
            atomic_json(state_path, state)
        result = _summary(plan, state)
        result["attempted_this_run"] = attempted
        result["state_path"] = str(Path(state_path).relative_to(store.root))
        return result
