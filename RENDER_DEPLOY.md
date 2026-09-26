# Render deployment — FREE ONLY

This repository is intentionally configured so the Render Blueprint cannot create paid Render resources.

## What Render creates

Exactly one service:

- `futures-datahub-api`
- type: Web Service
- runtime: native Python
- plan: **Free**
- region: Frankfurt
- health check: `/health`

The Blueprint does **not** create:

- MinIO
- persistent disks
- Postgres
- Key Value / Redis
- workers
- cron jobs
- any `starter` or other paid Render plan

## Important storage rule

Render Free Web Services have an ephemeral filesystem. `DATAHUB_DATA_ROOT=/tmp/datahub` is therefore
cache-only and must never be treated as durable canonical storage.

The API can boot and pass health checks without external object storage. Until durable remote storage
is configured, `/manifests` will normally be empty after a fresh instance starts.

For persistent research data, attach a separate S3-compatible object store that has a genuinely free
tier. Configure it only through Render environment variables; do not add a Render persistent disk.

Supported variables:

```text
DATAHUB_S3_BUCKET
DATAHUB_S3_PREFIX
DATAHUB_S3_ENDPOINT_URL
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_REGION
```

No storage credentials are committed to Git.

## Security

Render generates `DATAHUB_API_TOKEN` when using the Blueprint. For direct API creation, set a
random `DATAHUB_API_TOKEN` in the Render environment. GET/HEAD requests are public because
`DATAHUB_PUBLIC_READ=true`; POST and other write requests remain bearer-token protected.

## Cost policy

**Canonical policy: no paid Render resources.**

Any future change that introduces `plan: starter`, a Render persistent `disk:`, or the old
`futures-datahub-minio` service violates the deployment policy and is guarded by an automated test.
