# Futures-Research-DataHub

Canonical, strategy-free market-data layer for **Binance USD-M Futures research**.

DataHub owns ingestion, validation, provenance, immutable versioning, historical-universe evidence,
coverage, resumable backfill and release publication. It contains **no trading strategies, signals,
portfolio logic, orders or execution code**. Existing research repositories and running jobs are
outside this repository and are not modified.

## V1 guarantees

- Parquet + ZSTD market partitions; PyArrow canonical schemas, Polars validation, DuckDB reads.
- UTC canonical timestamps and half-open ranges `[start, end)`.
- OHLCV: 1m / 5m / 15m / 1h / 4h.
- Mark price, index price and funding ingestion.
- Binance Vision archive SHA256 verification before parsing.
- REST pagination with bounded retries and explicit rate-limit handling.
- Duplicate, missing, misaligned and invalid OHLC checks; rejected data is quarantined.
- Content-addressed partitions and immutable manifests/fingerprints.
- Per-day checkpoints, crash-safe resume and incremental sync.
- Historical archive inventory from Binance Vision's public S3 listing, independent of today's
  `exchangeInfo`, so delisted archive symbols are discoverable.
- Explicit Vision -> REST fallback in inventory plans. Fallback reason and actual source are stored
  in provenance; it is never silent.
- Release publication is blocked until a fresh fixed-grid coverage attestation passes.
- `doctor` audits manifests, inventories, plans, checkpoints, receipts and quarantine state.
- Large market data is excluded from Git history.

## Install

Python 3.11+ on Windows or Linux:

```sh
python -m venv .venv
# Windows: .venv\Scripts\activate
# Linux: source .venv/bin/activate
python -m pip install -e ".[dev]"
datahub --help
```

Copy `datahub.example.toml` to `datahub.local.toml` and set an absolute data-disk path.
The data root must live **outside every Git working tree**.

Priority is `--root` > `DATAHUB_DATA_ROOT` > `--config`. There is no implicit data directory.

## Canonical V1 flow

For large historical work, use bounded releases such as one calendar year at a time.

```sh
# 1. Initialize the local data root.
datahub --root D:/Futures-Research-Data init

# 2. Preserve a current contract-metadata observation.
datahub --root D:/Futures-Research-Data metadata-sync

# 3. Discover historical archive activity, including symbols no longer in today's exchangeInfo.
datahub --root D:/Futures-Research-Data inventory-sync USD_M_2024 \
  --start 2024-01-01T00:00:00Z --end 2025-01-01T00:00:00Z

# 4. Build an immutable backfill plan for the shared research datasets.
datahub --root D:/Futures-Research-Data inventory-backfill-plan USD_M_2024_FULL \
  --inventory USD_M_2024 \
  --datasets ohlcv mark_price index_price funding \
  --timeframes 1m 5m 15m 1h 4h

# 5. Execute. Re-running the same command resumes verified checkpoints.
datahub --root D:/Futures-Research-Data backfill-run USD_M_2024_FULL

# 6. Confirm plan state.
datahub --root D:/Futures-Research-Data backfill-status USD_M_2024_FULL

# 7. Publish only after the release coverage gate passes.
datahub --root D:/Futures-Research-Data release BINANCE_USDM_2024_V1 \
  --plan USD_M_2024_FULL

# 8. Verify every immutable release partition and operational state.
datahub --root D:/Futures-Research-Data doctor --deep
```

A release manifest stores plan lineage plus a new coverage attestation. Consumers should read a
specific manifest ID, never a mutable "latest" directory.

## Historical-universe evidence

DataHub keeps two evidence classes separate.

### Audited lifecycle

`lifecycle-import` accepts externally audited listing/delisting evidence with explicit coverage,
source and `known_at` timestamps. A `universe` snapshot from this evidence can claim complete
membership only when the imported evidence explicitly says coverage is complete.

### Archive-observed universe

`inventory-sync` enumerates the public Binance Vision S3 archive rather than deriving history from
the current exchange contract list. It scans USD-M daily 1m kline object presence, groups contiguous
archive ranges and can bridge only short archive holes while recording those holes explicitly.
Boundary files are checksum-verified and probed by default to obtain first/last observed candle
times. S3 objects are streamed page-by-page and scanning stops at the requested end date; boundary
archive SHA256 proofs are stored inside the immutable inventory fingerprint.

```sh
datahub --root D:/Futures-Research-Data archive-universe JAN15_2024 \
  --inventory USD_M_2024 --as-of 2024-01-15T12:00:00Z
```

An archive-observed snapshot is survivorship-safe **with respect to current-list bias**, but it is
not represented as an exchange-certified listing/delisting record. Its manifest says
`complete: false` and records the archive-presence semantics.

## Source and repair behavior

Direct `sync` remains available for one series:

```sh
datahub --root D:/Futures-Research-Data sync \
  --source vision --dataset ohlcv --symbol BTCUSDT --timeframe 1m \
  --start 2024-01-01T00:00:00Z --end 2024-01-02T00:00:00Z
```

Inventory plans default to `vision-rest`:

- complete UTC candle days attempt Binance Vision first;
- checksum, HTTP, parsing or validation failure is recorded;
- REST is then used explicitly for that same checkpoint;
- partial listing/delisting boundary days naturally fall back to REST;
- funding uses REST and remains an event stream rather than a fabricated fixed schedule.

Every accepted partition is validated before publication. Missing candles are never filled,
deduplicated or silently discarded.

## Coverage and release

```sh
datahub --root D:/Futures-Research-Data coverage \
  --dataset ohlcv --symbol BTCUSDT --timeframe 1m \
  --start 2024-01-01T00:00:00Z --end 2024-02-01T00:00:00Z
```

Fixed-grid series report `COMPLETE`, `GAPPED` or `CONFLICT`. Different overlapping objects for
the same series block planning and release. Funding reports `EVENT_STREAM`; V1 does not invent a
historical funding schedule when Binance changed interval rules.

A successful `release` requires:

1. the immutable plan is COMPLETE;
2. all referenced receipts and partition SHA256 values verify;
3. every fixed-grid interval is recomputed as COMPLETE;
4. no overlap conflict exists.

The resulting manifest is immutable and contains the coverage-attestation fingerprint.

## Integrity and health

```sh
datahub --root D:/Futures-Research-Data verify BINANCE_USDM_2024_V1
datahub --root D:/Futures-Research-Data doctor
datahub --root D:/Futures-Research-Data doctor --deep
datahub --root D:/Futures-Research-Data doctor --deep --strict
```

Normal `doctor` fails on corrupted immutable control objects. `--deep` additionally re-hashes
all VALID receipts. `--strict` also treats incomplete plans, failed checkpoints and quarantine
artifacts as release-health failures.

See [OPERATIONS.md](OPERATIONS.md) for the production sequence and recovery rules, and
[DATAHUB_ARCHITECTURE.md](DATAHUB_ARCHITECTURE.md) for invariants and evidence semantics.

## Development and CI

```sh
ruff check .
ruff format --check .
pytest -q
python -m build
datahub --help
```

The required GitHub Actions matrix runs on Ubuntu and Windows with Python 3.11 and 3.12.
Deterministic CI uses synthetic fixtures and mocked network responses; it does not bulk-download
market data. A change is not accepted until the complete matrix is green.

Official upstream references:

- Binance public historical-data repository: https://github.com/binance/binance-public-data
- Binance USD-M market-data API documentation:
  https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data
