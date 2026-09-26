# Futures-Research-DataHub

Shared, versioned Binance USD-M Futures market data for offline research. **No strategies,
signals, orders, execution engine, or Freqtrade dependencies.** Existing research repositories
and running jobs are outside this project's scope.

## Install

Python 3.11 or newer, Windows or Linux:

```sh
python -m venv .venv
# Windows: .venv\Scripts\activate
# Linux: source .venv/bin/activate
python -m pip install -e ".[dev]"
datahub --help
```

Copy `datahub.example.toml` to `datahub.local.toml` and set an absolute path on your data disk.
The data root must be **outside any Git repository**. No market-data files belong in Git.
Priority: `--root` > `DATAHUB_DATA_ROOT` > `--config` TOML. There is no hidden default path.

```sh
datahub --root D:/Futures-Research-Data init
datahub --root D:/Futures-Research-Data sync --source vision --dataset ohlcv --symbol BTCUSDT --timeframe 1m --start 2024-01-01T00:00:00Z --end 2024-01-02T00:00:00Z
datahub --root D:/Futures-Research-Data catalog
datahub --root D:/Futures-Research-Data manifest BINANCE_USDM_V1 --receipts RECEIPT_ID_FROM_SYNC
datahub --root D:/Futures-Research-Data verify BINANCE_USDM_V1
datahub --root D:/Futures-Research-Data inspect BINANCE_USDM_V1 --dataset ohlcv --symbol BTCUSDT --timeframe 1m
```

`sync` returns receipt IDs; use those exact IDs for publication. `manifest` emits the immutable
dataset fingerprint. Repeating the same sync resumes verified completed chunks without HTTP.
Extend the same day-aligned range to incrementally add new days. Each day is its own checkpoint;
an interrupted day is retried, previous days are retained. Partial REST windows should be kept
consistent across retries; overlapping receipts cannot enter one manifest.

Supported candle timeframes: **1m, 5m, 15m, 1h, 4h**, stored directly without resampling.
Supported datasets: `ohlcv`, `mark_price`, `index_price`, `funding`. Vision supports daily
candle ZIPs with mandatory upstream SHA256 checks. REST supports all four datasets with
pagination and bounded retries. Use `--source rest --dataset funding` for funding events;
the timeframe argument is a namespace label for funding and does not impose a cadence.
Mark/index volume is null because Binance's corresponding fields are placeholders.

All ranges are **[start, end)**; timestamps require an explicit timezone and are normalized
to UTC milliseconds. Candle bounds must align to the timeframe and exclude open candles.
For listing/delisting mid-day use REST with explicit lifecycle-clipped aligned bounds.
Missing candles are never silently filled or dropped. Validation failures exit nonzero and
retain the normalized rejected partition plus its validation receipt in quarantine.
Transport/checksum/parse failures retain FAILED checkpoint diagnostics and are never published.

## Contract metadata and historical universe

```sh
datahub --root D:/Futures-Research-Data metadata-sync
datahub --root D:/Futures-Research-Data lifecycle-import audited-lifecycle.json
datahub --root D:/Futures-Research-Data universe JAN_2024 --lifecycle LIFECYCLE_ID --as-of 2024-01-01T00:00:00Z --known-at 2024-01-01T00:00:00Z
```

`metadata-sync` preserves the entire exchangeInfo response (including onboardDate,
deliveryDate, contractType, status and filters) in an immutable observation. It explicitly
does **not** assert historical completeness. Historical lifecycle import requires:

```json
{
  "source": "audited announcement/archive inventory reference",
  "complete": true,
  "coverage_start": 1704067200000,
  "coverage_end": 1704153600000,
  "records": [
    {"symbol": "EXAMPLEUSDT", "listed_at": 1690000000000,
     "delisted_at": 1704100000000, "known_at": 1704000000000,
     "source": "specific lifecycle evidence reference"}
  ]
}
```

This is a schema example, **not real market evidence**. Completeness is an explicit assertion
by the evidence provider and must be audited externally. Snapshot creation rejects incomplete
coverage and evidence newer than its knowledge cutoff. Delisted symbols remain available;
current exchangeInfo is never substituted for the historical universe. A fresh installation
contains no historically complete universe. Build/import audited history before all-universe
research. Publish with `manifest ... --universe JAN_2024` to bind its fingerprint and membership.

## Coverage and controlled backfill

Coverage is computed only from VALID receipts whose partition SHA256 still verifies. Fixed-grid
candle datasets report covered intervals, gaps, conflicts and a coverage ratio. Funding remains
an event stream: the DataHub reports observed events but does not claim schedule completeness.

```sh
datahub --root D:/Futures-Research-Data coverage --dataset ohlcv --symbol BTCUSDT --timeframe 1m --start 2024-01-01T00:00:00Z --end 2024-02-01T00:00:00Z
datahub --root D:/Futures-Research-Data coverage-matrix --lifecycle LIFECYCLE_ID --datasets ohlcv mark_price index_price funding --timeframes 1m 5m 15m 1h 4h --start 2024-01-01T00:00:00Z --end 2024-02-01T00:00:00Z
datahub --root D:/Futures-Research-Data backfill-plan JAN_2024 --lifecycle LIFECYCLE_ID --datasets ohlcv mark_price index_price funding --timeframes 1m 5m 15m 1h 4h --start 2024-01-01T00:00:00Z --end 2024-02-01T00:00:00Z
datahub --root D:/Futures-Research-Data backfill-run JAN_2024 --max-jobs 100
datahub --root D:/Futures-Research-Data backfill-status JAN_2024
```

Backfill plans are immutable and require lifecycle evidence marked complete for the whole requested
range. This keeps delisted contracts in the historical workload instead of rebuilding history from
today's exchangeInfo. The default `auto` source policy uses Binance Vision for complete UTC-day
candle chunks and REST for lifecycle/coverage edge fragments; funding always uses REST. Completed
jobs are receipt-verified before they are skipped on resume. Plan-run state is mutable and guarded
by a per-plan lock, while the plan itself is fingerprinted and immutable.

## Validation and development

```sh
ruff check .
ruff format --check .
pytest -q
python -m build
```

CI runs these checks on Ubuntu and Windows, Python 3.11 and 3.12, plus the installed CLI. The acceptance gate requires the complete matrix to pass before release.
Tests are deterministic, use synthetic small fixtures, and do not depend on Binance uptime.
Integration tests exercise archive parsing through validation, Parquet/ZSTD, checkpoint resume,
manifest publication and offline DuckDB reads. A live smoke is separate from deterministic CI.
No bulk-market download runs in GitHub Actions.

See [DATAHUB_ARCHITECTURE.md](DATAHUB_ARCHITECTURE.md) for invariants and first-release limits.

Official source specifications:
[Binance public data](https://github.com/binance/binance-public-data) and
[USD-M market data REST](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data).
