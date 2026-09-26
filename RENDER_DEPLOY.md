# Render deployment

This Blueprint deploys the Online DataHub in Frankfurt as two co-located services:

- `futures-datahub-api`: public HTTPS read API; write/test endpoints remain bearer-token protected.
- `futures-datahub-minio`: S3-compatible canonical object store backed by a persistent disk.

The MinIO image is built from the pinned upstream MinIO source release
`RELEASE.2025-10-15T17-29-55Z` instead of relying on the withdrawn Docker Hub
`minio/minio` image.

## Security

Render generates:

- `MINIO_ROOT_USER`
- `MINIO_ROOT_PASSWORD`
- `DATAHUB_API_TOKEN`

No secret values are committed to Git. GET/HEAD requests are public when
`DATAHUB_PUBLIC_READ=true`; POST/other write operations require the bearer token.

## Storage

The initial MinIO disk is 10 GB. It is intentionally a bootstrap size and can be enlarged without
changing DataHub fingerprints. Do not treat 10 GB as the final full-universe capacity.

## Deploy

Create a Render Blueprint from this repository. Render reads `render.yaml`, builds both services,
creates the persistent disk, generates credentials, and wires the MinIO endpoint/credentials into
the DataHub API automatically.

After deploy:

1. `GET https://<api-host>/health` must return `status=PASS`.
2. `GET https://<api-host>/manifests` must return HTTP 200.
3. The MinIO health endpoint must be green in Render.
4. Populate/publish the first DataHub release before strategy research depends on the service.
