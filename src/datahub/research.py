"""Strategy-free persistent research-job orchestration.

Executors are external plugins (module:function). DataHub stores requests, deduplicates them by
fingerprint, and persists results, but contains no trading strategy implementation.
"""

from __future__ import annotations

import importlib
import json
import os
import sqlite3
import time
from pathlib import Path

from .core import INTERVALS, canonical, fingerprint, safe_name, utcnow


class ResearchJobs:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init()

    def _connect(self):
        con = sqlite3.connect(self.path, timeout=30)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA busy_timeout=30000")
        return con

    def _init(self):
        with self._connect() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    request_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    completed_at TEXT,
                    result_json TEXT,
                    error TEXT
                )
                """
            )

    @staticmethod
    def validate_request(request):
        if not isinstance(request, dict):
            raise ValueError("Research request must be an object")
        strategy = request.get("strategy")
        manifest = request.get("manifest")
        timeframe = request.get("timeframe")
        if not isinstance(strategy, str) or not strategy or len(strategy) > 200:
            raise ValueError("Research request requires strategy")
        safe_name(manifest)
        if timeframe not in INTERVALS:
            raise ValueError("Unsupported timeframe")
        start, end = request.get("start"), request.get("end")
        if not isinstance(start, int) or not isinstance(end, int) or start >= end:
            raise ValueError("Research request requires integer millisecond start/end")
        if not isinstance(request.get("parameters", {}), dict):
            raise ValueError("Research parameters must be an object")
        canonical(request)
        return request

    def submit(self, request):
        request = self.validate_request(request)
        job_id = fingerprint({"schema_version": 1, "request": request})
        payload = canonical(request).decode()
        with self._connect() as con:
            con.execute(
                """
                INSERT OR IGNORE INTO jobs(job_id, request_json, status, created_at)
                VALUES (?, ?, 'QUEUED', ?)
                """,
                (job_id, payload, utcnow()),
            )
        return self.get(job_id)

    def get(self, job_id):
        safe_name(job_id)
        with self._connect() as con:
            row = con.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        if row is None:
            raise KeyError(job_id)
        return self._public(row)

    def _public(self, row):
        return {
            "job_id": row["job_id"],
            "request": json.loads(row["request_json"]),
            "status": row["status"],
            "created_at": row["created_at"],
            "started_at": row["started_at"],
            "completed_at": row["completed_at"],
            "error": row["error"],
        }

    def result(self, job_id):
        safe_name(job_id)
        with self._connect() as con:
            row = con.execute(
                "SELECT status, result_json, error FROM jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
        if row is None:
            raise KeyError(job_id)
        if row["status"] != "COMPLETE":
            raise ValueError(f"Result unavailable: {row['status']}")
        return json.loads(row["result_json"])

    def claim(self):
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT * FROM jobs WHERE status = 'QUEUED' ORDER BY created_at, job_id LIMIT 1"
            ).fetchone()
            if row is None:
                con.commit()
                return None
            updated = con.execute(
                """
                UPDATE jobs SET status = 'RUNNING', started_at = ?
                WHERE job_id = ? AND status = 'QUEUED'
                """,
                (utcnow(), row["job_id"]),
            ).rowcount
            con.commit()
            return self.get(row["job_id"]) if updated else None
        finally:
            con.close()

    def complete(self, job_id, result):
        safe_name(job_id)
        payload = canonical(result).decode()
        with self._connect() as con:
            updated = con.execute(
                """
                UPDATE jobs SET status = 'COMPLETE', completed_at = ?, result_json = ?, error = NULL
                WHERE job_id = ? AND status = 'RUNNING'
                """,
                (utcnow(), payload, job_id),
            ).rowcount
        if not updated:
            raise ValueError("Job is not RUNNING")
        return self.get(job_id)

    def fail(self, job_id, error):
        safe_name(job_id)
        with self._connect() as con:
            updated = con.execute(
                """
                UPDATE jobs SET status = 'FAILED', completed_at = ?, error = ?
                WHERE job_id = ? AND status = 'RUNNING'
                """,
                (utcnow(), str(error)[:4000], job_id),
            ).rowcount
        if not updated:
            raise ValueError("Job is not RUNNING")
        return self.get(job_id)


def load_executor(spec: str):
    if ":" not in spec:
        raise ValueError("Executor must be module:function")
    module_name, function_name = spec.rsplit(":", 1)
    function = getattr(importlib.import_module(module_name), function_name)
    if not callable(function):
        raise ValueError("Executor is not callable")
    return function


def run_one(jobs: ResearchJobs, executor):
    job = jobs.claim()
    if job is None:
        return None
    try:
        result = executor(job["request"])
        if not isinstance(result, dict):
            raise ValueError("Research executor must return a JSON object")
        jobs.complete(job["job_id"], result)
    except Exception as exc:
        jobs.fail(job["job_id"], exc)
    return jobs.get(job["job_id"])


def worker_main():
    root = os.getenv("DATAHUB_DATA_ROOT")
    if not root:
        raise SystemExit("Set DATAHUB_DATA_ROOT")
    spec = os.getenv("DATAHUB_RESEARCH_EXECUTOR")
    if not spec:
        raise SystemExit("Set DATAHUB_RESEARCH_EXECUTOR=module:function")
    jobs = ResearchJobs(os.getenv("DATAHUB_JOB_DB", str(Path(root) / "research/jobs.sqlite")))
    executor = load_executor(spec)
    once = os.getenv("DATAHUB_WORKER_ONCE", "").lower() in {"1", "true", "yes"}
    poll = float(os.getenv("DATAHUB_WORKER_POLL_SECONDS", "1"))
    while True:
        result = run_one(jobs, executor)
        if once:
            return 0
        if result is None:
            time.sleep(max(0.1, poll))


if __name__ == "__main__":
    raise SystemExit(worker_main())
