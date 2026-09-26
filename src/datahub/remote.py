"""S3-compatible immutable publication and verified remote materialization."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath

from .core import atomic_json, canonical, fingerprint, safe_name, sha256


def safe_relative(value: str) -> str:
    if not isinstance(value, str) or not value or "\\" in value or "\x00" in value:
        raise ValueError("Unsafe remote path")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError("Unsafe remote path")
    return str(path)


def _missing(exc: Exception) -> bool:
    response = getattr(exc, "response", None) or {}
    error = response.get("Error", {}) if isinstance(response, dict) else {}
    code = str(error.get("Code", ""))
    status = (
        response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if isinstance(response, dict)
        else None
    )
    return code in {"404", "NoSuchKey", "NotFound"} or status == 404


class S3Remote:
    """Minimal S3-compatible object store with immutable-write semantics."""

    def __init__(self, bucket: str, prefix: str = "", client=None, endpoint_url=None, region=None):
        if not bucket:
            raise ValueError("Remote bucket is required")
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        if client is None:
            try:
                import boto3
            except ImportError as exc:  # pragma: no cover - exercised by minimal installs
                raise RuntimeError(
                    "Install futures-research-datahub[online] for S3 support"
                ) from exc
            client = boto3.client("s3", endpoint_url=endpoint_url, region_name=region)
        self.client = client

    @classmethod
    def from_env(cls, client=None):
        bucket = os.getenv("DATAHUB_S3_BUCKET")
        if not bucket:
            raise ValueError("Set DATAHUB_S3_BUCKET")
        return cls(
            bucket,
            prefix=os.getenv("DATAHUB_S3_PREFIX", ""),
            client=client,
            endpoint_url=os.getenv("DATAHUB_S3_ENDPOINT_URL") or None,
            region=os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or None,
        )

    def key(self, relative: str) -> str:
        relative = safe_relative(relative)
        return f"{self.prefix}/{relative}" if self.prefix else relative

    def head(self, relative: str):
        try:
            return self.client.head_object(Bucket=self.bucket, Key=self.key(relative))
        except Exception as exc:
            if _missing(exc):
                return None
            raise

    def get_bytes(self, relative: str) -> bytes:
        response = self.client.get_object(Bucket=self.bucket, Key=self.key(relative))
        body = response["Body"]
        return body.read() if hasattr(body, "read") else bytes(body)

    def put_bytes_immutable(
        self, relative: str, data: bytes, content_type="application/octet-stream"
    ):
        digest = hashlib.sha256(data).hexdigest()
        head = self.head(relative)
        if head is not None:
            remote_digest = (head.get("Metadata") or {}).get("sha256")
            if remote_digest == digest:
                return {"uploaded": False, "sha256": digest, "key": self.key(relative)}
            if (
                remote_digest is None
                and hashlib.sha256(self.get_bytes(relative)).hexdigest() == digest
            ):
                return {"uploaded": False, "sha256": digest, "key": self.key(relative)}
            raise ValueError(f"Immutable remote object differs: {relative}")
        self.client.put_object(
            Bucket=self.bucket,
            Key=self.key(relative),
            Body=data,
            Metadata={"sha256": digest},
            ContentType=content_type,
        )
        return {"uploaded": True, "sha256": digest, "key": self.key(relative)}

    def put_file_immutable(self, relative: str, path, expected_sha256: str):
        path = Path(path)
        digest = sha256(path)
        if digest != expected_sha256:
            raise ValueError(f"Local SHA256 mismatch before upload: {relative}")
        head = self.head(relative)
        if head is not None:
            if (head.get("Metadata") or {}).get("sha256") != expected_sha256:
                raise ValueError(f"Immutable remote object differs: {relative}")
            return {"uploaded": False, "sha256": digest, "key": self.key(relative)}
        with path.open("rb") as stream:
            self.client.put_object(
                Bucket=self.bucket,
                Key=self.key(relative),
                Body=stream,
                Metadata={"sha256": digest},
                ContentType="application/vnd.apache.parquet",
            )
        return {"uploaded": True, "sha256": digest, "key": self.key(relative)}

    def download_file_verified(self, relative: str, destination, expected_sha256: str):
        destination = Path(destination)
        if destination.exists() and sha256(destination) == expected_sha256:
            return destination
        response = self.client.get_object(Bucket=self.bucket, Key=self.key(relative))
        body = response["Body"]
        destination.parent.mkdir(parents=True, exist_ok=True)
        temp = destination.with_name(destination.name + ".partial")
        digest = hashlib.sha256()
        with temp.open("wb") as stream:
            while True:
                chunk = body.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
                stream.write(chunk)
        if digest.hexdigest() != expected_sha256:
            temp.unlink(missing_ok=True)
            raise ValueError(f"Remote SHA256 mismatch: {relative}")
        os.replace(temp, destination)
        return destination

    def list_keys(self, relative_prefix: str):
        prefix = self.key(relative_prefix)
        token = None
        while True:
            params = {"Bucket": self.bucket, "Prefix": prefix}
            if token:
                params["ContinuationToken"] = token
            page = self.client.list_objects_v2(**params)
            for item in page.get("Contents", []):
                key = item["Key"]
                base = f"{self.prefix}/" if self.prefix else ""
                yield key[len(base) :] if base and key.startswith(base) else key
            if not page.get("IsTruncated"):
                return
            token = page.get("NextContinuationToken")
            if not token:
                raise ValueError("Truncated S3 listing omitted continuation token")


def _validate_manifest_document(document, name):
    if document.get("dataset_id") != name:
        raise ValueError("Remote manifest ID mismatch")
    body = {k: v for k, v in document.items() if k != "fingerprint"}
    if fingerprint(body) != document.get("fingerprint"):
        raise ValueError("Remote manifest fingerprint mismatch")
    return document


def materialize_manifest_control(store, remote: S3Remote, name: str):
    safe_name(name)
    path = store.root / f"manifests/{name}.json"
    if path.exists():
        document = json.loads(path.read_text())
        return _validate_manifest_document(document, name)
    document = json.loads(remote.get_bytes(f"manifests/{name}.json"))
    _validate_manifest_document(document, name)
    atomic_json(path, document)
    return document


def materialize_partition(store, remote: S3Remote, part):
    rid = safe_name(part["receipt_id"])
    receipt_path = store.root / f"receipts/{rid}.json"
    if not receipt_path.exists():
        receipt = json.loads(remote.get_bytes(f"receipts/{rid}.json"))
        if fingerprint(receipt) != rid:
            raise ValueError("Remote receipt fingerprint mismatch")
        atomic_json(receipt_path, receipt)
    remote.download_file_verified(part["path"], store.root / part["path"], part["sha256"])
    receipt = store.receipt(rid)
    if dict(receipt_id=rid, **receipt) != part:
        raise ValueError("Remote manifest/receipt mismatch")
    return receipt


def publish_release(store, remote: S3Remote, name: str):
    """Publish one verified immutable release. Existing identical objects are reused."""
    safe_name(name)
    document = store.verify(name)
    uploaded = 0
    reused = 0
    for part in document["partitions"]:
        result = remote.put_file_immutable(part["path"], store.root / part["path"], part["sha256"])
        uploaded += int(result["uploaded"])
        reused += int(not result["uploaded"])
        receipt_path = store.root / f"receipts/{part['receipt_id']}.json"
        result = remote.put_bytes_immutable(
            f"receipts/{part['receipt_id']}.json",
            receipt_path.read_bytes(),
            "application/json",
        )
        uploaded += int(result["uploaded"])
        reused += int(not result["uploaded"])
    result = remote.put_bytes_immutable(
        f"manifests/{name}.json",
        canonical(document),
        "application/json",
    )
    uploaded += int(result["uploaded"])
    reused += int(not result["uploaded"])
    return {
        "status": "PUBLISHED",
        "manifest": name,
        "fingerprint": document["fingerprint"],
        "partitions": len(document["partitions"]),
        "uploaded_objects": uploaded,
        "reused_objects": reused,
        "bucket": remote.bucket,
        "prefix": remote.prefix,
    }


def pull_release(store, remote: S3Remote, name: str):
    """Materialize a full release into a local cache and verify it end to end."""
    document = materialize_manifest_control(store, remote, name)
    for part in document["partitions"]:
        materialize_partition(store, remote, part)
    verified = store.verify(name)
    return {
        "status": "MATERIALIZED",
        "manifest": name,
        "fingerprint": verified["fingerprint"],
        "partitions": len(verified["partitions"]),
    }
