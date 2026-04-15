"""Tests for GCS resumable uploads and object lifecycle rules."""
import json
from datetime import datetime, timedelta, timezone


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rfc3339(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _setup_bucket(client, name="test-bucket"):
    r = client.post("/storage/v1/b", json={"name": name})
    assert r.status_code == 200
    return name


# ---------------------------------------------------------------------------
# Resumable uploads — initiate
# ---------------------------------------------------------------------------


def test_resumable_initiate_returns_location(gcs_client):
    _setup_bucket(gcs_client)
    r = gcs_client.post(
        "/upload/storage/v1/b/test-bucket/o?uploadType=resumable",
        json={"name": "file.txt", "contentType": "text/plain"},
    )
    assert r.status_code == 200
    location = r.headers.get("location", "")
    assert "upload_id=" in location
    assert "uploadType=resumable" in location


def test_resumable_initiate_name_from_query_param(gcs_client):
    _setup_bucket(gcs_client)
    r = gcs_client.post(
        "/upload/storage/v1/b/test-bucket/o?uploadType=resumable&name=query.txt",
        content=b"",
        headers={"content-type": "application/octet-stream"},
    )
    assert r.status_code == 200
    assert "upload_id=" in r.headers.get("location", "")


def test_resumable_initiate_missing_name_returns_400(gcs_client):
    _setup_bucket(gcs_client)
    r = gcs_client.post(
        "/upload/storage/v1/b/test-bucket/o?uploadType=resumable",
        json={},
    )
    assert r.status_code == 400


def test_resumable_initiate_missing_bucket_returns_404(gcs_client):
    r = gcs_client.post(
        "/upload/storage/v1/b/no-such-bucket/o?uploadType=resumable",
        json={"name": "f.txt"},
    )
    assert r.status_code == 404


def test_resumable_initiate_x_upload_headers(gcs_client):
    """X-Upload-Content-Type and X-Upload-Content-Length are respected."""
    _setup_bucket(gcs_client)
    r = gcs_client.post(
        "/upload/storage/v1/b/test-bucket/o?uploadType=resumable",
        json={"name": "typed.bin"},
        headers={
            "x-upload-content-type": "application/octet-stream",
            "x-upload-content-length": "1024",
        },
    )
    assert r.status_code == 200
    assert "upload_id=" in r.headers.get("location", "")


# ---------------------------------------------------------------------------
# Resumable uploads — single-shot PUT (no Content-Range)
# ---------------------------------------------------------------------------


def test_resumable_single_shot_no_content_range(gcs_client):
    """Sending all data in one PUT without Content-Range finalizes immediately."""
    _setup_bucket(gcs_client)
    init_r = gcs_client.post(
        "/upload/storage/v1/b/test-bucket/o?uploadType=resumable",
        json={"name": "single.txt", "contentType": "text/plain"},
    )
    location = init_r.headers["location"]
    upload_id = location.split("upload_id=")[1]

    put_r = gcs_client.put(
        f"/upload/storage/v1/b/test-bucket/o?uploadType=resumable&upload_id={upload_id}",
        content=b"hello world",
    )
    assert put_r.status_code == 200
    assert put_r.json()["name"] == "single.txt"
    assert put_r.json()["size"] == "11"

    # Object is accessible
    dl = gcs_client.get("/download/storage/v1/b/test-bucket/o/single.txt")
    assert dl.content == b"hello world"


# ---------------------------------------------------------------------------
# Resumable uploads — chunked PUT with Content-Range
# ---------------------------------------------------------------------------


def test_resumable_chunked_upload(gcs_client):
    """Upload in two chunks; second chunk finalizes the object."""
    _setup_bucket(gcs_client)
    init_r = gcs_client.post(
        "/upload/storage/v1/b/test-bucket/o?uploadType=resumable",
        json={"name": "chunked.bin", "contentType": "application/octet-stream"},
    )
    upload_id = init_r.headers["location"].split("upload_id=")[1]

    # First chunk (bytes 0-4 of 10)
    r1 = gcs_client.put(
        f"/upload/storage/v1/b/test-bucket/o?uploadType=resumable&upload_id={upload_id}",
        content=b"hello",
        headers={"content-range": "bytes 0-4/10"},
    )
    assert r1.status_code == 308
    assert r1.headers.get("range") == "bytes=0-4"

    # Second (final) chunk (bytes 5-9 of 10)
    r2 = gcs_client.put(
        f"/upload/storage/v1/b/test-bucket/o?uploadType=resumable&upload_id={upload_id}",
        content=b"world",
        headers={"content-range": "bytes 5-9/10"},
    )
    assert r2.status_code == 200
    assert r2.json()["name"] == "chunked.bin"
    assert r2.json()["size"] == "10"

    dl = gcs_client.get("/download/storage/v1/b/test-bucket/o/chunked.bin")
    assert dl.content == b"helloworld"


def test_resumable_status_query(gcs_client):
    """PUT with Content-Range: bytes */N and empty body returns upload progress."""
    _setup_bucket(gcs_client)
    init_r = gcs_client.post(
        "/upload/storage/v1/b/test-bucket/o?uploadType=resumable",
        json={"name": "status.bin"},
    )
    upload_id = init_r.headers["location"].split("upload_id=")[1]

    # Upload first chunk
    gcs_client.put(
        f"/upload/storage/v1/b/test-bucket/o?uploadType=resumable&upload_id={upload_id}",
        content=b"hello",
        headers={"content-range": "bytes 0-4/10"},
    )

    # Status query
    r = gcs_client.put(
        f"/upload/storage/v1/b/test-bucket/o?uploadType=resumable&upload_id={upload_id}",
        content=b"",
        headers={"content-range": "bytes */10"},
    )
    assert r.status_code == 308
    assert r.headers.get("range") == "bytes=0-4"


def test_resumable_status_query_no_bytes_yet(gcs_client):
    """Status query on a fresh session returns 308 with no Range header."""
    _setup_bucket(gcs_client)
    init_r = gcs_client.post(
        "/upload/storage/v1/b/test-bucket/o?uploadType=resumable",
        json={"name": "empty.bin"},
    )
    upload_id = init_r.headers["location"].split("upload_id=")[1]

    r = gcs_client.put(
        f"/upload/storage/v1/b/test-bucket/o?uploadType=resumable&upload_id={upload_id}",
        content=b"",
        headers={"content-range": "bytes */5"},
    )
    assert r.status_code == 308
    assert "range" not in r.headers


def test_resumable_invalid_session_returns_404(gcs_client):
    _setup_bucket(gcs_client)
    r = gcs_client.put(
        "/upload/storage/v1/b/test-bucket/o?uploadType=resumable&upload_id=nonexistent",
        content=b"data",
    )
    assert r.status_code == 404


def test_resumable_full_range_in_single_put(gcs_client):
    """Single PUT with Content-Range bytes 0-N-1/N finalizes immediately."""
    _setup_bucket(gcs_client)
    init_r = gcs_client.post(
        "/upload/storage/v1/b/test-bucket/o?uploadType=resumable",
        json={"name": "full.txt", "contentType": "text/plain"},
    )
    upload_id = init_r.headers["location"].split("upload_id=")[1]

    data = b"complete data"
    r = gcs_client.put(
        f"/upload/storage/v1/b/test-bucket/o?uploadType=resumable&upload_id={upload_id}",
        content=data,
        headers={"content-range": f"bytes 0-{len(data)-1}/{len(data)}"},
    )
    assert r.status_code == 200
    assert r.json()["size"] == str(len(data))


# ---------------------------------------------------------------------------
# Lifecycle rules — bucket PATCH
# ---------------------------------------------------------------------------


def test_patch_bucket_lifecycle(gcs_client):
    """PATCH /storage/v1/b/{bucket} sets lifecycle rules."""
    _setup_bucket(gcs_client)
    r = gcs_client.patch(
        "/storage/v1/b/test-bucket",
        json={"lifecycle": {"rule": [{"action": {"type": "Delete"}, "condition": {"age": 30}}]}},
    )
    assert r.status_code == 200
    assert r.json()["lifecycle"]["rule"][0]["action"]["type"] == "Delete"
    assert r.json()["metageneration"] == "2"


def test_patch_bucket_labels(gcs_client):
    _setup_bucket(gcs_client)
    r = gcs_client.patch("/storage/v1/b/test-bucket", json={"labels": {"env": "test"}})
    assert r.status_code == 200
    assert r.json()["labels"]["env"] == "test"


def test_patch_bucket_missing_returns_404(gcs_client):
    r = gcs_client.patch("/storage/v1/b/no-such-bucket", json={"labels": {}})
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Lifecycle rules — Delete action
# ---------------------------------------------------------------------------


def _upload(client, bucket, name, content=b"data"):
    client.post(
        f"/upload/storage/v1/b/{bucket}/o?name={name}&uploadType=media",
        content=content,
        headers={"content-type": "text/plain"},
    )


def test_lifecycle_delete_old_objects(gcs_client):
    """Objects older than `age` days are deleted when listing."""
    _setup_bucket(gcs_client)
    _upload(gcs_client, "test-bucket", "old.txt")
    _upload(gcs_client, "test-bucket", "new.txt")

    # Manually back-date "old.txt" in the store
    from localgcp.services.gcs.store import get_store
    store = get_store()
    obj = store.get("objects", "test-bucket/old.txt")
    old_time = _rfc3339(datetime.now(timezone.utc) - timedelta(days=40))
    obj["timeCreated"] = old_time
    obj["updated"] = old_time
    store.set("objects", "test-bucket/old.txt", obj)

    # Set lifecycle: delete if age >= 30 days
    gcs_client.patch(
        "/storage/v1/b/test-bucket",
        json={"lifecycle": {"rule": [{"action": {"type": "Delete"}, "condition": {"age": 30}}]}},
    )

    r = gcs_client.get("/storage/v1/b/test-bucket/o")
    names = [o["name"] for o in r.json().get("items", [])]
    assert "old.txt" not in names
    assert "new.txt" in names


def test_lifecycle_delete_by_created_before(gcs_client):
    """Objects created before the given date are deleted."""
    _setup_bucket(gcs_client)
    _upload(gcs_client, "test-bucket", "ancient.txt")

    from localgcp.services.gcs.store import get_store
    store = get_store()
    obj = store.get("objects", "test-bucket/ancient.txt")
    obj["timeCreated"] = _rfc3339(datetime(2020, 1, 1, tzinfo=timezone.utc))
    store.set("objects", "test-bucket/ancient.txt", obj)

    gcs_client.patch(
        "/storage/v1/b/test-bucket",
        json={"lifecycle": {"rule": [{
            "action": {"type": "Delete"},
            "condition": {"createdBefore": "2023-01-01T00:00:00Z"},
        }]}},
    )

    r = gcs_client.get("/storage/v1/b/test-bucket/o")
    names = [o["name"] for o in r.json().get("items", [])]
    assert "ancient.txt" not in names


def test_lifecycle_does_not_delete_new_objects(gcs_client):
    """Objects younger than the age threshold are not deleted."""
    _setup_bucket(gcs_client)
    _upload(gcs_client, "test-bucket", "young.txt")

    gcs_client.patch(
        "/storage/v1/b/test-bucket",
        json={"lifecycle": {"rule": [{"action": {"type": "Delete"}, "condition": {"age": 365}}]}},
    )

    r = gcs_client.get("/storage/v1/b/test-bucket/o")
    names = [o["name"] for o in r.json().get("items", [])]
    assert "young.txt" in names


# ---------------------------------------------------------------------------
# Lifecycle rules — SetStorageClass action
# ---------------------------------------------------------------------------


def test_lifecycle_set_storage_class(gcs_client):
    """Objects matching conditions get their storage class updated."""
    _setup_bucket(gcs_client)
    _upload(gcs_client, "test-bucket", "archive.bin")

    from localgcp.services.gcs.store import get_store
    store = get_store()
    obj = store.get("objects", "test-bucket/archive.bin")
    obj["timeCreated"] = _rfc3339(datetime.now(timezone.utc) - timedelta(days=400))
    store.set("objects", "test-bucket/archive.bin", obj)

    gcs_client.patch(
        "/storage/v1/b/test-bucket",
        json={"lifecycle": {"rule": [{
            "action": {"type": "SetStorageClass", "storageClass": "NEARLINE"},
            "condition": {"age": 365, "matchesStorageClass": ["STANDARD"]},
        }]}},
    )

    gcs_client.get("/storage/v1/b/test-bucket/o")  # trigger lifecycle
    updated = store.get("objects", "test-bucket/archive.bin")
    assert updated["storageClass"] == "NEARLINE"


def test_lifecycle_set_storage_class_no_match(gcs_client):
    """Objects not matching matchesStorageClass are skipped."""
    _setup_bucket(gcs_client)
    _upload(gcs_client, "test-bucket", "already-nearline.bin")

    from localgcp.services.gcs.store import get_store
    store = get_store()
    obj = store.get("objects", "test-bucket/already-nearline.bin")
    obj["storageClass"] = "NEARLINE"
    obj["timeCreated"] = _rfc3339(datetime.now(timezone.utc) - timedelta(days=400))
    store.set("objects", "test-bucket/already-nearline.bin", obj)

    gcs_client.patch(
        "/storage/v1/b/test-bucket",
        json={"lifecycle": {"rule": [{
            "action": {"type": "SetStorageClass", "storageClass": "COLDLINE"},
            "condition": {"age": 365, "matchesStorageClass": ["STANDARD"]},
        }]}},
    )

    gcs_client.get("/storage/v1/b/test-bucket/o")
    updated = store.get("objects", "test-bucket/already-nearline.bin")
    assert updated["storageClass"] == "NEARLINE"  # unchanged


def test_lifecycle_no_rules_leaves_objects_untouched(gcs_client):
    """Bucket with empty lifecycle rules does not affect objects."""
    _setup_bucket(gcs_client)
    _upload(gcs_client, "test-bucket", "safe.txt")

    r = gcs_client.get("/storage/v1/b/test-bucket/o")
    assert len(r.json()["items"]) == 1


def test_lifecycle_object_storageclass_in_metadata(gcs_client):
    """Uploaded objects include a storageClass field."""
    _setup_bucket(gcs_client)
    r = gcs_client.post(
        "/upload/storage/v1/b/test-bucket/o?name=sc.txt&uploadType=media",
        content=b"x",
        headers={"content-type": "text/plain"},
    )
    assert r.status_code == 200
    assert "storageClass" in r.json()
