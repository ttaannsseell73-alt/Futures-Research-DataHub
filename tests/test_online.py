import io

from fastapi.testclient import TestClient

from datahub.online import OnlineStore, create_app
from datahub.remote import S3Remote, publish_release
from datahub.research import ResearchJobs, run_one
from datahub.schemas import normalize
from datahub.storage import Store

START = 1_704_067_200_000


class MissingObject(Exception):
    def __init__(self):
        self.response = {
            "Error": {"Code": "NoSuchKey"},
            "ResponseMetadata": {"HTTPStatusCode": 404},
        }


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.buckets = set()

    def head_bucket(self, Bucket):
        if Bucket not in self.buckets:
            raise MissingObject()
        return {}

    def create_bucket(self, Bucket):
        self.buckets.add(Bucket)
        return {}

    def head_object(self, Bucket, Key):
        if Bucket not in self.buckets or Key not in self.objects:
            raise MissingObject()
        item = self.objects[Key]
        return {"Metadata": dict(item["metadata"]), "ContentLength": len(item["data"])}

    def put_object(self, Bucket, Key, Body, Metadata=None, ContentType=None):
        if Bucket not in self.buckets:
            raise MissingObject()
        data = Body.read() if hasattr(Body, "read") else bytes(Body)
        self.objects[Key] = {
            "data": data,
            "metadata": dict(Metadata or {}),
            "content_type": ContentType,
        }
        return {}

    def get_object(self, Bucket, Key):
        if Bucket not in self.buckets or Key not in self.objects:
            raise MissingObject()
        item = self.objects[Key]
        return {"Body": io.BytesIO(item["data"]), "Metadata": dict(item["metadata"])}

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None):
        del ContinuationToken
        if Bucket not in self.buckets:
            raise MissingObject()
        keys = sorted(key for key in self.objects if key.startswith(Prefix))
        return {
            "Contents": [{"Key": key} for key in keys],
            "IsTruncated": False,
        }


def _release(root):
    store = Store(root)
    rows = [
        [START, "10", "12", "9", "11", "2"],
        [START + 60_000, "11", "13", "10", "12", "3"],
    ]
    rid = store.put(
        normalize(rows, "ohlcv"),
        "ohlcv",
        "BTCUSDT",
        "1m",
        START,
        START + 120_000,
        {"source": "fixture"},
    )
    store.manifest("release_v1", [rid])
    return store


def test_remote_publish_reuse_and_on_demand_hydration(tmp_path):
    source = _release(tmp_path / "source")
    fake = FakeS3()
    remote = S3Remote("bucket", "canonical", client=fake)

    first = publish_release(source, remote, "release_v1")
    assert first["status"] == "PUBLISHED"
    assert first["uploaded_objects"] == 3

    second = publish_release(source, remote, "release_v1")
    assert second["uploaded_objects"] == 0
    assert second["reused_objects"] == 3

    cache = Store(tmp_path / "cache")
    view = OnlineStore(cache, remote)
    plan = view.query_plan(
        "release_v1",
        "ohlcv",
        "BTCUSDT",
        "1m",
        START,
        START + 120_000,
    )
    assert plan["manifest_fingerprint"] == source.verify("release_v1")["fingerprint"]
    assert plan["partitions"][0]["remote_key"].startswith("canonical/objects/")
    object_path = cache.root / plan["partitions"][0]["path"]
    assert not object_path.exists()

    table = view.scan_range(
        "release_v1",
        "ohlcv",
        "BTCUSDT",
        "1m",
        START,
        START + 120_000,
    )
    assert table.num_rows == 2
    assert object_path.exists()
    assert cache.verify("release_v1")["fingerprint"] == plan["manifest_fingerprint"]


def test_online_api_auth_query_and_deduplicated_jobs(tmp_path):
    source = _release(tmp_path / "source")
    fake = FakeS3()
    remote = S3Remote("bucket", "canonical", client=fake)
    publish_release(source, remote, "release_v1")

    app = create_app(
        root=tmp_path / "cache",
        remote=remote,
        token="secret",
        job_db=tmp_path / "jobs.sqlite",
    )
    client = TestClient(app)
    assert client.get("/health").status_code == 200
    assert client.get("/manifests").status_code == 401

    headers = {"Authorization": "Bearer secret"}
    assert client.get("/manifests", headers=headers).json()["manifests"] == ["release_v1"]
    bars = client.get(
        "/bars",
        headers=headers,
        params={
            "manifest": "release_v1",
            "dataset": "ohlcv",
            "symbol": "BTCUSDT",
            "timeframe": "1m",
            "start": START,
            "end": START + 120_000,
        },
    )
    assert bars.status_code == 200
    assert bars.json()["count"] == 2

    request = {
        "strategy": "RSI_THRESHOLD_V1",
        "parameters": {"length": 14, "oversold": 30, "overbought": 70},
        "manifest": "release_v1",
        "timeframe": "1m",
        "start": START,
        "end": START + 120_000,
    }
    first = client.post("/tests", headers=headers, json=request)
    second = client.post("/tests", headers=headers, json=request)
    assert first.status_code == 200
    assert first.json()["job_id"] == second.json()["job_id"]
    assert first.json()["status"] == "QUEUED"
    result = client.get(f"/tests/{first.json()['job_id']}/results", headers=headers)
    assert result.status_code == 409


def test_empty_remote_bootstraps_bucket_and_lists_no_manifests(tmp_path):
    fake = FakeS3()
    remote = S3Remote("bucket", "canonical", client=fake)
    view = OnlineStore(Store(tmp_path / "cache"), remote)
    assert view.manifest_names() == []
    assert "bucket" in fake.buckets


def test_public_read_keeps_post_protected(tmp_path, monkeypatch):
    source = _release(tmp_path / "source")
    fake = FakeS3()
    remote = S3Remote("bucket", "canonical", client=fake)
    publish_release(source, remote, "release_v1")
    monkeypatch.setenv("DATAHUB_PUBLIC_READ", "true")

    app = create_app(
        root=tmp_path / "cache",
        remote=remote,
        token="secret",
        job_db=tmp_path / "jobs.sqlite",
    )
    client = TestClient(app)
    assert client.get("/manifests").status_code == 200

    request = {
        "strategy": "FIXTURE",
        "parameters": {},
        "manifest": "release_v1",
        "timeframe": "1m",
        "start": START,
        "end": START + 120_000,
    }
    assert client.post("/tests", json=request).status_code == 401
    assert (
        client.post(
            "/tests",
            json=request,
            headers={"Authorization": "Bearer secret"},
        ).status_code
        == 200
    )


def test_research_worker_persists_result_and_failure(tmp_path):
    jobs = ResearchJobs(tmp_path / "jobs.sqlite")
    base = {
        "strategy": "FIXTURE",
        "parameters": {"x": 1},
        "manifest": "release_v1",
        "timeframe": "5m",
        "start": START,
        "end": START + 300_000,
    }
    first = jobs.submit(base)
    assert jobs.submit(base)["job_id"] == first["job_id"]

    completed = run_one(jobs, lambda request: {"score": request["parameters"]["x"]})
    assert completed["status"] == "COMPLETE"
    assert jobs.result(first["job_id"]) == {"score": 1}

    failed_request = dict(base, strategy="BROKEN", parameters={"x": 2})
    failed = jobs.submit(failed_request)

    def broken(_request):
        raise RuntimeError("fixture failure")

    status = run_one(jobs, broken)
    assert status["job_id"] == failed["job_id"]
    assert status["status"] == "FAILED"
    assert "fixture failure" in status["error"]
