# Online DataHub

Online mode makes the immutable DataHub release the canonical research source when the user's PC is
off. It keeps strategy code outside DataHub while exposing verified data and a persistent research
job contract.

## Runtime layout

```text
Binance Vision/REST -> DataHub release -> S3-compatible object storage
                                           |
                                           +-> Online API / local SSD cache
                                                   |
                                                   +-> Research workers
                                                   |
                                                   +-> ChatGPT / phone
```

The object store and workers should run in the same cloud region. Workers consume exact manifest
partitions; market data is not routed through the phone or ChatGPT.

## S3-compatible storage

Supported through the normal S3 API, including AWS S3 and compatible services such as Cloudflare R2,
Backblaze B2 S3 and MinIO.

Required environment:

```sh
DATAHUB_S3_BUCKET=research-data
DATAHUB_S3_PREFIX=binance-usdm
AWS_REGION=eu-central-1
# Optional for non-AWS S3:
DATAHUB_S3_ENDPOINT_URL=https://...
```

Normal AWS credential environment variables or workload identity are used by boto3.

After a local release has passed the normal DataHub release gate:

```sh
datahub --root /data remote-publish BINANCE_USDM_2025_V1
```

Remote writes are immutable. A key that already exists with a different SHA256 is rejected.

## Online API

Install and run:

```sh
pip install -e ".[online]"
export DATAHUB_DATA_ROOT=/cache/datahub
export DATAHUB_API_TOKEN=replace-with-a-long-random-secret
export DATAHUB_S3_BUCKET=research-data
datahub-api
```

Endpoints:

- `GET /health`
- `GET /manifests`
- `GET /manifests/{name}`
- `GET /universe?manifest=...`
- `GET /coverage?... `
- `GET /query-plan?... `
- `GET /bars?...&format=json|arrow`
- `POST /tests`
- `GET /tests/{job_id}`
- `GET /tests/{job_id}/results`

Except for `/health`, set `Authorization: Bearer <DATAHUB_API_TOKEN>` when a token is configured.

`/query-plan` is the high-throughput worker path. It returns exact object keys and SHA256 values for
only the partitions required by the requested manifest/symbol/timeframe/range. `/bars` is intended
for inspection and smaller interactive reads.

## Research queue

DataHub does not contain RSI, MACD, STR100 or other strategy logic. `POST /tests` persists and
deduplicates a canonical request by fingerprint. External research workers provide the strategy
executor.

Example request:

```json
{
  "strategy": "RSI_THRESHOLD_V1",
  "parameters": {"length": 14, "oversold": 30, "overbought": 70},
  "manifest": "BINANCE_USDM_2025_V1",
  "timeframe": "5m",
  "start": 1735689600000,
  "end": 1767225600000
}
```

Worker configuration:

```sh
DATAHUB_DATA_ROOT=/cache/datahub
DATAHUB_RESEARCH_EXECUTOR=my_research.executor:run
datahub-worker
```

The executor receives the request object and must return a JSON object. Job state is persisted in
SQLite/WAL by default at `$DATAHUB_DATA_ROOT/research/jobs.sqlite`. Re-submitting the exact same
request returns the same job ID, so identical tests are not recomputed.

## Performance invariants

- Remote object storage is canonical; worker SSD is only a verified cache.
- Parquet objects remain partitioned by dataset/timeframe/symbol/year/month.
- API queries hydrate only overlapping manifest partitions.
- Every hydrated partition is checked against its receipt and SHA256.
- Workers use `/query-plan` or the S3 store directly; they do not download the full universe through
  JSON.
- Arrow IPC is available for efficient interactive transport.
- Strategy request fingerprints are deterministic and reusable as result-cache keys.
