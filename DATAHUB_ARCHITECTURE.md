# DataHub architecture — v0.2

## Boundary

The canonical plan separates DataHub from Fast-Strategy-Lab. This repository implements
ingest, schemas, validation, storage, catalog, versioning, lifecycle evidence and CLI only.
Feature calculation, strategy plugins, experiment scheduling, backtests, portfolios,
rankings and research workers belong to the future consumer repository. Freqtrade-Research-Lab
and running support/resistance jobs are neither imported nor modified.

## Data path

`Vision/REST -> Arrow normalization -> Polars validation -> Parquet/ZSTD -> receipt -> manifest`

Consumers select an explicit manifest ID. The offline reader verifies manifest, receipt and
partition SHA256 before querying exact paths with DuckDB. It never discovers mutable `latest`
files and never performs network access. One scan selects one dataset/symbol/timeframe.

Modules: `ingest.py` (sources), `schemas.py`, `validation.py`, `storage.py` (objects, catalog,
versioning, offline reader), `sync.py` (incremental orchestration), `universe.py` (metadata and
historical snapshots), `coverage.py` (verified gap/conflict accounting), `planning.py`\n(backfill plans and resumable execution), `core.py` (UTC, configuration, hashes, atomic JSON),\n`cli.py`.

## Local layout

```text
DATA_ROOT/
  objects/market=binance_usdm/dataset=ohlcv/timeframe=1m/
    symbol=BTCUSDT/year=2024/month=01/<sha256>.parquet
  receipts/<receipt_fingerprint>.json
  manifests/<dataset_id>.json
  checkpoints/<request_fingerprint>.json
  quarantine/market=.../dataset=.../.../<sha256>.parquet
  metadata/<observation_fingerprint>.json
  lifecycles/<evidence_fingerprint>.json
  universes/<snapshot_id>.json
```

Daily download chunks sit inside monthly Hive-style folders; object filenames are hashes,
so a correction creates a new object rather than overwriting a referenced partition.
Metadata and control documents are canonical JSON; time-series market payloads are Parquet.
The catalog is currently append-only receipt JSON, intentionally rebuildable without a database.
Polars validates vectorized columns; PyArrow defines typed UTC schemas and writes ZSTD;
DuckDB performs offline analytical reads. Prices/volumes use float64 for research; original
exchangeInfo is preserved verbatim structurally. Exact-decimal accounting is outside this release.

## Publication and crash safety

Partition writes use temporary files and atomic rename on the same filesystem. A writer lock
serializes object/receipt/manifest publication; per-request locks avoid duplicate concurrent
downloads. Checkpoints become COMPLETE only after validated object and receipt publication.
Interrupted writes can leave unreferenced temporary objects; these cannot enter a dataset.
No automatic garbage collector deletes data. An interrupted chunk restarts at its beginning.
Only local filesystems with reliable locks/atomic rename are supported; NFS/S3 need another backend.

Immutability is enforced by the application, not OS WORM storage. Manual tampering is detected
by reader hash checks. Partition hashes describe exact Parquet bytes. Receipt fingerprints include
provenance and download time. Dataset fingerprints cover schema version, dataset ID, complete
receipts and optional universe snapshot. Identical sync retries reuse checkpoints; corrected or
differently sourced ingestion yields explicit new versions. Library upgrades can alter Parquet
bytes, so byte equality across different dependency versions is not promised.

## Validation contract

Candle timestamp is the open time, timestamp[ms, UTC]. Coverage is explicitly requested [start,end),
not inferred from the returned rows. Checks include duplicate timestamps, missing grid positions
(including first/last), misalignment, outside-range/null timestamps, non-finite/nonpositive OHLC,
high/low consistency and negative/non-finite/null traded volume. Invalid data goes to quarantine;
there is no fill, deduplication, or silent repair. Manifest creation rejects overlapping partitions.

Funding is an event stream. A universal eight-hour schedule would be wrong; event validation
checks duplicate timestamps, bounds and finite rates, and reports missing/expected as null.
This release does **not** certify funding completeness without a versioned historical funding
schedule. Empty funding responses are possible and are not evidence of no missing events.
Source ZIP checksum failures stop before normalization. Failed transport/parse/checksum attempts
retain checkpoint diagnostics, not entire rejected raw HTTP bodies.

## Universe contract

exchangeInfo observations preserve contracts, status, onboard/delivery dates and filters without
claiming to list all delisted history. Lifecycle evidence explicitly states coverage, completeness,
listing/delisting timestamps, per-record source and knowledge time. Membership uses
`listed_at <= as_of < delisted_at` (null delisting means open-ended within asserted coverage).
Knowledge cutoff rejects future evidence. Do not confuse retrospective membership with knowledge
available to a point-in-time strategy. Source completeness is an audited external prerequisite;
software cannot infer it from surviving symbols. No production historical universe is bundled.

## Coverage and backfill contract

Bulk coverage builds one receipt index per scan, then verifies only matching content-addressed
partitions before accounting. Candle coverage is fixed-grid and may be COMPLETE, GAPPED or
CONFLICT. Different objects that overlap the same series are a conflict and block new backfill
plan creation until resolved. Funding is EVENT_STREAM and never receives a fabricated gap ratio.

Backfill plans require complete lifecycle evidence across the full requested range. Jobs are
created from historical listing intervals, so symbols that are delisted today remain in past
workloads. Candle lifecycle boundaries are clipped inward to complete timeframe bars. Under the
default auto policy, complete UTC days use Vision while partial boundary/gap fragments use REST.
Funding uses one canonical `1m` namespace label and REST only; that label does not assert a
one-minute funding cadence.

The plan document is deterministic, fingerprinted and immutable. Mutable progress is stored
separately under `plan_runs/` and protected by a per-plan file lock. Each job still delegates to
the original sync/checkpoint path, so retries preserve per-day checkpoint semantics. A job marked
complete is not trusted blindly on resume: its receipts and partition hashes are re-verified first.

## HTTP and operational limits

Unauthenticated public endpoints only. REST paginates inclusive API endTime using end-1,
advances past the last timestamp, validates response ordering/range, and throttles pages.
HTTP timeouts, five-attempt transport/429/5xx retry budget and Retry-After are enforced;
418 bans fail immediately. Region/access errors are surfaced, never bypassed. Vision uses daily
archives and mandatory published SHA256. Monthly archive optimization, automatic archive symbol
inventory, raw archive retention, full historical lifecycle acquisition, global distributed rate
limiting and funding-schedule reconciliation are future work. No silent Vision-to-REST fallback.

## Acceptance

Local tests and CLI smoke are necessary but do not establish CI PASS. Delivery is complete only
after the pushed commit's GitHub Actions matrix is green. CI uses small synthetic fixtures and
builds the distribution; large data stays on the configured disk, outside Git history.
