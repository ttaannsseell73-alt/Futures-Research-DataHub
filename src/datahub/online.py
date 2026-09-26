"""Read-only online DataHub API plus strategy-free research job gateway."""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.ipc as ipc

from . import __version__
from .core import INTERVALS, KINDS, fingerprint, millis, safe_name, safe_symbol
from .remote import S3Remote, materialize_manifest_control, materialize_partition
from .research import ResearchJobs
from .schemas import CANDLE, FUNDING
from .storage import Store


class OnlineStore:
    """Remote-canonical, local-cache view over immutable DataHub releases."""

    def __init__(self, store: Store, remote: S3Remote | None = None):
        self.store = store
        self.remote = remote

    def manifest(self, name):
        safe_name(name)
        path = self.store.root / f"manifests/{name}.json"
        if not path.exists():
            if self.remote is None:
                raise FileNotFoundError(path)
            return materialize_manifest_control(self.store, self.remote, name)
        document = json.loads(path.read_text())
        body = {k: v for k, v in document.items() if k != "fingerprint"}
        if fingerprint(body) != document.get("fingerprint"):
            raise ValueError("Manifest fingerprint mismatch")
        return document

    def manifest_names(self):
        names = {p.stem for p in (self.store.root / "manifests").glob("*.json")}
        if self.remote is not None:
            for key in self.remote.list_keys("manifests/"):
                if key.startswith("manifests/") and key.endswith(".json"):
                    names.add(Path(key).stem)
        return sorted(names)

    def partitions(self, name, kind, symbol, timeframe, start, end, hydrate=True):
        safe_symbol(symbol)
        if kind not in KINDS or timeframe not in INTERVALS or start >= end:
            raise ValueError("Invalid query selection")
        document = self.manifest(name)
        parts = [
            part
            for part in document["partitions"]
            if part["dataset"] == kind
            and part["symbol"] == symbol
            and part["timeframe"] == timeframe
            and part["end"] > start
            and part["start"] < end
        ]
        parts.sort(key=lambda part: (part["start"], part["end"], part["receipt_id"]))
        if not parts:
            raise ValueError("No matching partitions")
        if hydrate:
            for part in parts:
                if self.remote is not None:
                    materialize_partition(self.store, self.remote, part)
                else:
                    receipt = self.store.receipt(part["receipt_id"])
                    if dict(receipt_id=part["receipt_id"], **receipt) != part:
                        raise ValueError("Manifest/receipt mismatch")
        return document, parts

    def query_plan(self, name, kind, symbol, timeframe, start, end):
        document, parts = self.partitions(name, kind, symbol, timeframe, start, end, hydrate=False)
        return {
            "manifest": name,
            "manifest_fingerprint": document["fingerprint"],
            "dataset": kind,
            "symbol": symbol,
            "timeframe": timeframe,
            "start": start,
            "end": end,
            "partitions": [
                {
                    "receipt_id": part["receipt_id"],
                    "path": part["path"],
                    "sha256": part["sha256"],
                    "start": part["start"],
                    "end": part["end"],
                    "remote_key": self.remote.key(part["path"]) if self.remote else None,
                }
                for part in parts
            ],
        }

    def coverage_summary(self, name, kind, symbol, timeframe, start, end):
        _, parts = self.partitions(name, kind, symbol, timeframe, start, end, hydrate=False)
        clipped = [
            (max(start, part["start"]), min(end, part["end"]))
            for part in parts
            if part["end"] > start and part["start"] < end
        ]
        merged = []
        for left, right in sorted(clipped):
            if not merged or left > merged[-1][1]:
                merged.append([left, right])
            else:
                merged[-1][1] = max(merged[-1][1], right)
        if kind == "funding":
            return {
                "manifest": name,
                "dataset": kind,
                "symbol": symbol,
                "timeframe": timeframe,
                "start": start,
                "end": end,
                "status": "EVENT_STREAM",
                "complete": None,
                "covered_intervals": merged,
                "expected_rows": None,
                "covered_rows": sum(part["validation"]["actual_rows"] for part in parts),
            }
        step = INTERVALS[timeframe]
        if start % step or end % step:
            raise ValueError("Coverage bounds must align with timeframe")
        cursor = start
        gaps = []
        for left, right in merged:
            if cursor < left:
                gaps.append([cursor, left])
            cursor = max(cursor, right)
        if cursor < end:
            gaps.append([cursor, end])
        expected = (end - start) // step
        covered = sum((right - left) // step for left, right in merged)
        return {
            "manifest": name,
            "dataset": kind,
            "symbol": symbol,
            "timeframe": timeframe,
            "start": start,
            "end": end,
            "status": "COMPLETE" if not gaps else "GAPPED",
            "complete": not gaps,
            "coverage_ratio": covered / expected if expected else 1.0,
            "expected_rows": expected,
            "covered_rows": covered,
            "covered_intervals": merged,
            "gaps": gaps,
        }

    def scan_range(self, name, kind, symbol, timeframe, start, end, limit=None):
        _, parts = self.partitions(name, kind, symbol, timeframe, start, end, hydrate=True)
        paths = [str(self.store.root / part["path"]) for part in parts]
        with duckdb.connect() as con:
            con.execute("SET TimeZone='UTC'")
            relation = con.read_parquet(paths, hive_partitioning=False).filter(
                f"epoch_ms(timestamp) >= {int(start)} AND epoch_ms(timestamp) < {int(end)}"
            )
            relation = relation.order("timestamp")
            if limit is not None:
                relation = relation.limit(int(limit))
            table = relation.to_arrow_table()
        return table.cast(FUNDING if kind == "funding" else CANDLE)


def _json_value(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _rows(table):
    return [{key: _json_value(value) for key, value in row.items()} for row in table.to_pylist()]


def _parse_time(value):
    return int(value) if str(value).isdigit() else millis(str(value))


def create_app(root=None, remote=None, token=None, job_db=None):
    try:
        from fastapi import FastAPI, HTTPException, Request
        from fastapi.responses import JSONResponse, Response
    except ImportError as exc:  # pragma: no cover - minimal install behavior
        raise RuntimeError("Install futures-research-datahub[online]") from exc

    raw_root = root or os.getenv("DATAHUB_DATA_ROOT")
    if not raw_root:
        raise ValueError("Set DATAHUB_DATA_ROOT")
    store = Store(Path(raw_root).expanduser().resolve())
    if remote is None and os.getenv("DATAHUB_S3_BUCKET"):
        remote = S3Remote.from_env()
    view = OnlineStore(store, remote)
    jobs = ResearchJobs(
        job_db or os.getenv("DATAHUB_JOB_DB", str(store.root / "research/jobs.sqlite"))
    )
    auth_token = token if token is not None else os.getenv("DATAHUB_API_TOKEN")

    app = FastAPI(title="Futures Research DataHub Online", version=__version__)

    @app.middleware("http")
    async def bearer_auth(request: Request, call_next):
        if auth_token and request.url.path != "/health":
            if request.headers.get("authorization") != f"Bearer {auth_token}":
                return JSONResponse({"detail": "Unauthorized"}, status_code=401)
        return await call_next(request)

    @app.get("/health")
    def health():
        return {
            "status": "PASS",
            "version": __version__,
            "remote": remote is not None,
            "research_queue": True,
        }

    @app.get("/manifests")
    def manifests():
        return {"manifests": view.manifest_names()}

    @app.get("/manifests/{name}")
    def manifest(name: str):
        try:
            return view.manifest(name)
        except (ValueError, FileNotFoundError) as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/universe")
    def universe(manifest: str):
        try:
            document = view.manifest(manifest)
        except (ValueError, FileNotFoundError) as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return document.get("universe") or {"complete": False, "symbols": []}

    @app.get("/coverage")
    def coverage_endpoint(
        manifest: str,
        dataset: str,
        symbol: str,
        timeframe: str,
        start: str,
        end: str,
    ):
        try:
            return view.coverage_summary(
                manifest,
                dataset,
                symbol,
                timeframe,
                _parse_time(start),
                _parse_time(end),
            )
        except (ValueError, FileNotFoundError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/query-plan")
    def query_plan(
        manifest: str,
        dataset: str,
        symbol: str,
        timeframe: str,
        start: str,
        end: str,
    ):
        try:
            return view.query_plan(
                manifest,
                dataset,
                symbol,
                timeframe,
                _parse_time(start),
                _parse_time(end),
            )
        except (ValueError, FileNotFoundError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/bars")
    def bars(
        manifest: str,
        dataset: str,
        symbol: str,
        timeframe: str,
        start: str,
        end: str,
        format: str = "json",
        limit: int = 10000,
    ):
        if not 1 <= limit <= 100000:
            raise HTTPException(status_code=400, detail="limit must be 1..100000")
        try:
            table = view.scan_range(
                manifest,
                dataset,
                symbol,
                timeframe,
                _parse_time(start),
                _parse_time(end),
                limit=limit,
            )
        except (ValueError, FileNotFoundError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if format == "arrow":
            sink = pa.BufferOutputStream()
            with ipc.new_stream(sink, table.schema) as writer:
                writer.write_table(table)
            return Response(
                content=sink.getvalue().to_pybytes(),
                media_type="application/vnd.apache.arrow.stream",
            )
        if format != "json":
            raise HTTPException(status_code=400, detail="format must be json or arrow")
        return {"rows": _rows(table), "count": table.num_rows}

    @app.post("/tests")
    def submit_test(request: dict):
        try:
            view.manifest(request.get("manifest", ""))
            return jobs.submit(request)
        except (ValueError, FileNotFoundError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/tests/{job_id}")
    def test_status(job_id: str):
        try:
            return jobs.get(job_id)
        except (ValueError, KeyError) as exc:
            raise HTTPException(status_code=404, detail="Unknown test") from exc

    @app.get("/tests/{job_id}/results")
    def test_result(job_id: str):
        try:
            return jobs.result(job_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Unknown test") from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    return app


def serve():
    try:
        import uvicorn
    except ImportError as exc:  # pragma: no cover
        raise SystemExit("Install futures-research-datahub[online]") from exc
    host = os.getenv("DATAHUB_API_HOST", "0.0.0.0")
    port = int(os.getenv("PORT", os.getenv("DATAHUB_API_PORT", "8080")))
    uvicorn.run(create_app(), host=host, port=port)


if __name__ == "__main__":
    serve()
