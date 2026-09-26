# DataHub architecture — v1.0

## Boundary

DataHub is the canonical market-data substrate for Binance USD-M Futures research. It owns only:

- source adapters and archive inventory;
- canonical schemas and UTC normalization;
- validation and quarantine;
- Parquet/ZSTD storage;
- receipts, fingerprints, manifests and release lineage;
- historical-universe evidence;
- coverage/backfill orchestration;
- integrity health checks.

Strategies, indicators, features, experiment scheduling, backtests, rankings, portfolios, signals,
risk and execution remain outside this repository. Freqtrade-Research-Lab and any running S/R
process are not dependencies and are not modified.

## Data and evidence paths

Market data:

`Vision/REST -> Arrow normalization -> Polars validation -> Parquet/ZSTD -> receipt -> manifest`

Historical discovery:

`Vision S3 ListObjectsV2 -> archive inventory -> archive-observed intervals -> backfill plan`

Release:

`immutable plan + verified receipts -> fresh coverage attestation -> immutable manifest`

Offline consumers select an explicit manifest ID. DuckDB reads only exact paths stored in that
manifest after receipt and SHA256 verification; no reader discovers a mutable "latest" folder and
no offline read performs network access.

## Modules

- `core.py`: UTC parsing, canonical JSON, hashes, atomic writes and path safety.
- `ingest.py`: Binance Vision and USD-M REST adapters with bounded retry behavior.
- `schemas.py`: canonical PyArrow schemas.
- `validation.py`: fixed-grid/event-stream validation.
- `storage.py`: content-addressed Parquet, receipts, manifests and DuckDB reads.
- `sync.py`: daily checkpoints and explicit fallback semantics.
- `universe.py`: externally audited lifecycle evidence.
- `inventory.py`: public Vision S3 enumeration and archive-observed universes.
- `coverage.py`: verified interval/gap/conflict accounting.
- `planning.py`: immutable lifecycle/inventory plans and resumable execution.
- `release.py`: coverage attestation, manifest publication and doctor.
- `cli.py`: operational interface.

## Local layout

```text
DATA_ROOT/
  objects/market=binance_usdm/dataset=ohlcv/timeframe=1m/
    symbol=BTCUSDT/year=2024/month=01/<parquet_sha256>.parquet
  receipts/<receipt_fingerprint>.json
  manifests/<dataset_id>.json
  checkpoints/<request_fingerprint>.json
  quarantine/market=.../dataset=.../.../<sha256>.parquet
  metadata/<observation_fingerprint>.json
  lifecycles/<evidence_fingerprint>.json
  inventories/<inventory_id>.json
  universes/<snapshot_id>.json
  plans/<plan_id>.json
  plan_runs/<plan_id>.json
```

Large market data and all local runtime state remain outside Git history.

## Storage and immutability

Each normalized partition is written to a temporary file, ZSTD-compressed with PyArrow and moved
atomically into a content-addressed path. A receipt contains the exact Parquet SHA256, requested
coverage, validation report, source provenance and download time. A manual modification is detected
on the next receipt/manifest read.

Control documents use canonical JSON. Receipts, inventories, lifecycle imports, universes, plans
and manifests are immutable by application contract. Mutable progress is isolated in checkpoints
and `plan_runs/`.

The local backend assumes a filesystem with reliable atomic rename and file locking. Distributed
object stores require a separate storage backend rather than pretending local locking semantics.

## Timestamp and validation contract

Candle timestamps are open times using `timestamp[ms, UTC]`. All ranges are half-open
`[start, end)`. Fixed-grid validation checks:

- exact schema;
- duplicate timestamps;
- requested-range bounds;
- timeframe alignment;
- missing grid positions including first/last;
- finite positive OHLC;
- high/low consistency;
- finite nonnegative traded volume for OHLCV.

Invalid normalized data is retained in quarantine and cannot enter a manifest.

Funding is an event stream. V1 checks duplicate timestamps, bounds and finite rates but does not
assert a universal eight-hour schedule. Funding completeness therefore remains separate from
fixed-grid completeness.

## Source behavior

### Vision

Daily USD-M candle archives are fetched from `data.binance.vision`. The published `.CHECKSUM`
SHA256 is mandatory and verified before ZIP parsing. URL path segments are percent-encoded so the
adapter does not assume an ASCII-only symbol namespace.

### REST

USD-M public market-data endpoints are unauthenticated. Pagination uses inclusive API end-time
semantics carefully, with bounded retries for transport errors, 429 and 5xx responses. HTTP 418
fails immediately. No geo/access bypass is attempted.

### Explicit fallback

Inventory plans may declare `source=vision, fallback_source=rest`. The fallback is part of the
immutable plan. If Vision fails, the checkpoint records the primary error before REST is attempted;
the accepted receipt records the actual source plus `fallback_from` and `fallback_reason`.
This is auditable fallback, not silent substitution.

## Historical-universe evidence model

### Audited lifecycle

Externally supplied evidence records listing/delisting intervals, source, knowledge time and an
explicit completeness assertion. Only this evidence class can create a universe with
`complete: true`.

### Archive inventory

The inventory enumerates Binance Vision's public S3 bucket using ListObjectsV2. Symbol discovery
uses the historical daily USD-M kline prefix, not current `exchangeInfo`. For each symbol, the
inventory scans 1m daily archive presence, stores exact observed ranges and records small missing
archive ranges when they are bridged into an activity segment.

Boundary probing downloads only first/last observed 1m files for each activity segment through the
normal checksum-verified Vision adapter, producing exact first/last observed candle bounds.

This protects research from **current-list survivorship bias**, including delisted archive symbols,
but does not convert archive presence into an exchange-certified listing record. The inventory
therefore sets `historical_listing_complete: false` and archive-universe snapshots set
`complete: false`.

Long gaps are not bridged automatically; this also avoids treating a reused ticker as one continuous
contract lifetime. The permitted short-gap bridge is explicit and versioned in the inventory.

## Coverage and planning

Coverage considers only VALID receipts whose partition SHA256 still verifies. Candle series are
classified as COMPLETE, GAPPED or CONFLICT. Funding remains EVENT_STREAM.

Audited lifecycle plans can preserve exact lifecycle boundaries. Archive inventory plans instead
use observed activity segments and default to Vision with explicit REST fallback. Jobs may span
many days; `sync.py` still creates one checkpoint per UTC day, so process interruption resumes
without re-downloading verified days.

Existing valid receipts are bound into the immutable plan as preexisting receipt IDs. Plan-run state
never changes the plan fingerprint.

## Release gate

A plan-run COMPLETE flag is necessary but insufficient for publication.

`release`:

1. verifies the immutable plan;
2. verifies all planned and preexisting receipts;
3. rebuilds coverage from the current receipt catalog;
4. rejects any fixed-grid gap or overlap conflict;
5. creates a coverage-attestation fingerprint;
6. writes an immutable manifest containing plan/evidence lineage and the attestation.

Funding is explicitly labelled event-stream-not-schedule-certified in the attestation.

## Doctor

`doctor` verifies manifests and inventory fingerprints and reports plan/checkpoint/quarantine
state. `--deep` re-hashes every VALID receipt partition. `--strict` upgrades operational debt
(incomplete plans, failed checkpoints or quarantine artifacts) to a failing health result.

## Known source limitations

The upstream Binance public archive has had documented missing/duplicate files or timestamps.
DataHub treats upstream checksums as transport-integrity evidence, not proof of time-series
completeness. The validation and fallback layers exist specifically because a checksum-valid
archive can still contain a data-quality defect.

Current `exchangeInfo` is retained as a current metadata observation only and is never substituted
for historical-universe evidence.

## Acceptance

Deterministic tests, lint, format, CLI smoke and package build must pass on:

- Ubuntu / Python 3.11
- Ubuntu / Python 3.12
- Windows / Python 3.11
- Windows / Python 3.12

Large historical data is never committed to Git and bulk downloads do not run inside the required CI
matrix.
