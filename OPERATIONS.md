# DataHub V1 Operations

This runbook is the canonical production sequence for building a versioned Binance USD-M research
dataset. It assumes the data root is on a persistent local disk outside every Git repository.

## 1. Initialize once

```sh
datahub --root D:/Futures-Research-Data init
datahub --root D:/Futures-Research-Data metadata-sync
```

`metadata-sync` is a current observation only. It is useful for contract filters and present-day
metadata, but it is never treated as historical-universe truth.

## 2. Inventory history in bounded windows

Use calendar-year inventories for large backfills. This keeps fingerprints, recovery and releases
small enough to audit independently.

```sh
datahub --root D:/Futures-Research-Data inventory-sync USD_M_2024 \
  --start 2024-01-01T00:00:00Z \
  --end 2025-01-01T00:00:00Z
```

The default inventory:

- discovers historical symbol folders from Binance Vision S3;
- scans daily 1m OHLCV archive presence;
- records exact archive ranges and short missing-archive gaps;
- checksum-probes first/last archive days to tighten observed activity boundaries;
- writes one immutable fingerprinted inventory.

Re-running the same name with the same parameters returns the existing inventory. Reusing the name
with different parameters is rejected.

For a controlled subset during diagnostics:

```sh
datahub --root D:/Futures-Research-Data inventory-sync CHECK_2024 \
  --start 2024-01-01T00:00:00Z --end 2025-01-01T00:00:00Z \
  --symbols BTCUSDT ETHUSDT SOLUSDT
```

## 3. Build the immutable backfill plan

```sh
datahub --root D:/Futures-Research-Data inventory-backfill-plan USD_M_2024_FULL \
  --inventory USD_M_2024 \
  --datasets ohlcv mark_price index_price funding \
  --timeframes 1m 5m 15m 1h 4h
```

Default source policy is `vision-rest`.

For candle datasets the immutable plan declares Vision as primary and REST as fallback. Funding uses
REST. Existing validated receipts are incorporated so the plan only requests uncovered candle
ranges.

## 4. Execute and resume

```sh
datahub --root D:/Futures-Research-Data backfill-run USD_M_2024_FULL
```

If the process or machine stops, run the same command again. Daily checkpoints verify completed
receipt SHA256 values before skipping them.

To limit one invocation to a fixed number of high-level plan jobs:

```sh
datahub --root D:/Futures-Research-Data backfill-run USD_M_2024_FULL --max-jobs 25
```

A high-level job can span multiple UTC days; daily checkpointing still occurs inside that job.

Check state:

```sh
datahub --root D:/Futures-Research-Data backfill-status USD_M_2024_FULL
```

Do not publish while status is `PENDING` or `INCOMPLETE`.

## 5. Investigate failures without deleting evidence

```sh
datahub --root D:/Futures-Research-Data doctor
```

A Vision archive can pass its published checksum and still fail continuity validation. In an
inventory plan, the same daily checkpoint then tries REST if the plan explicitly includes the
fallback. The checkpoint and receipt retain the primary failure reason.

Quarantine is evidence. Do not delete quarantined objects merely to make a health report green.
First determine whether the accepted REST replacement covers the requested interval and whether the
quarantine is an expected upstream data-quality artifact.

## 6. Optional point-in-time universe snapshot

For archive-observed research membership:

```sh
datahub --root D:/Futures-Research-Data archive-universe U_2024_06_01 \
  --inventory USD_M_2024 \
  --as-of 2024-06-01T00:00:00Z
```

This protects against current-list survivorship bias but intentionally has `complete: false`
because archive presence is not an exchange-certified listing history.

If audited listing/delisting evidence exists, use `lifecycle-import` and `universe` instead.

## 7. Release

```sh
datahub --root D:/Futures-Research-Data release BINANCE_USDM_2024_V1 \
  --plan USD_M_2024_FULL
```

Release is blocked unless the plan is complete and a newly computed fixed-grid coverage attestation
passes. The resulting manifest binds:

- all verified receipt IDs;
- partition SHA256 values;
- plan fingerprint;
- inventory or lifecycle fingerprint;
- requested range;
- coverage attestation;
- optional universe snapshot.

The manifest ID is the consumer contract. Research code should not select files by directory glob.

## 8. Final health gate

```sh
datahub --root D:/Futures-Research-Data verify BINANCE_USDM_2024_V1
datahub --root D:/Futures-Research-Data doctor --deep
```

Use strict mode when preparing a clean archival release:

```sh
datahub --root D:/Futures-Research-Data doctor --deep --strict
```

Strict mode fails on operational debt such as incomplete plans, failed checkpoints or quarantine
artifacts. Non-strict doctor still fails on corrupted immutable objects.

## Recovery rules

1. Never overwrite a manifest, plan, inventory, lifecycle document or universe snapshot.
2. Never edit a receipt to repair a partition. Re-ingest and publish a new receipt/version.
3. Never remove a failed checkpoint before preserving the error context.
4. Never fill or interpolate missing market candles inside DataHub.
5. Never use current `exchangeInfo` as historical membership.
6. Never commit Parquet, ZIP, DuckDB, SQLite or local data roots to Git.
7. If a new upstream correction changes bytes, create a new receipt and new release manifest.
8. If evidence semantics change, create a new inventory/lifecycle ID rather than mutating the old one.

## Scaling

The expensive part is market-data volume, not control metadata. Keep releases bounded by time window
(year or quarter) and allow consumers to compose multiple manifest IDs. The content-addressed object
store deduplicates identical accepted Parquet bytes naturally, while checkpoints prevent repeated
network downloads during resume.
