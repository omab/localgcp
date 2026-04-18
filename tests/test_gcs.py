"""Tests for Cloud Storage emulator."""


def test_create_and_get_bucket(gcs_client):
    r = gcs_client.post("/storage/v1/b", json={"name": "test-bucket"})
    assert r.status_code == 200
    assert r.json()["name"] == "test-bucket"

    r = gcs_client.get("/storage/v1/b/test-bucket")
    assert r.status_code == 200
    assert r.json()["name"] == "test-bucket"


def test_list_buckets(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "bucket-a"})
    gcs_client.post("/storage/v1/b", json={"name": "bucket-b"})
    r = gcs_client.get("/storage/v1/b")
    assert r.status_code == 200
    names = [b["name"] for b in r.json()["items"]]
    assert "bucket-a" in names
    assert "bucket-b" in names


def test_duplicate_bucket_returns_409(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "dup"})
    r = gcs_client.post("/storage/v1/b", json={"name": "dup"})
    assert r.status_code == 409


def test_delete_bucket(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "to-delete"})
    r = gcs_client.delete("/storage/v1/b/to-delete")
    assert r.status_code == 204
    r = gcs_client.get("/storage/v1/b/to-delete")
    assert r.status_code == 404


def test_delete_non_empty_bucket_returns_409(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "nonempty"})
    gcs_client.post(
        "/upload/storage/v1/b/nonempty/o?name=file.txt&uploadType=media",
        content=b"hello",
        headers={"content-type": "text/plain"},
    )
    r = gcs_client.delete("/storage/v1/b/nonempty")
    assert r.status_code == 409


def test_upload_and_download_object(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "bkt"})
    r = gcs_client.post(
        "/upload/storage/v1/b/bkt/o?name=hello.txt&uploadType=media",
        content=b"Hello, world!",
        headers={"content-type": "text/plain"},
    )
    assert r.status_code == 200
    assert r.json()["name"] == "hello.txt"

    r = gcs_client.get("/download/storage/v1/b/bkt/o/hello.txt")
    assert r.status_code == 200
    assert r.content == b"Hello, world!"


def test_list_objects(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "bkt2"})
    for name in ("a.txt", "b.txt", "c.txt"):
        gcs_client.post(
            f"/upload/storage/v1/b/bkt2/o?name={name}&uploadType=media",
            content=b"x",
            headers={"content-type": "text/plain"},
        )
    r = gcs_client.get("/storage/v1/b/bkt2/o")
    assert r.status_code == 200
    names = [o["name"] for o in r.json()["items"]]
    assert set(names) == {"a.txt", "b.txt", "c.txt"}


def test_list_objects_with_prefix(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "pfx"})
    for name in ("dir/a.txt", "dir/b.txt", "other.txt"):
        gcs_client.post(
            f"/upload/storage/v1/b/pfx/o?name={name}&uploadType=media",
            content=b"x",
            headers={"content-type": "text/plain"},
        )
    r = gcs_client.get("/storage/v1/b/pfx/o?prefix=dir/")
    assert r.status_code == 200
    names = [o["name"] for o in r.json()["items"]]
    assert set(names) == {"dir/a.txt", "dir/b.txt"}


def test_delete_object(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "bkt3"})
    gcs_client.post(
        "/upload/storage/v1/b/bkt3/o?name=del.txt&uploadType=media",
        content=b"bye",
        headers={"content-type": "text/plain"},
    )
    r = gcs_client.delete("/storage/v1/b/bkt3/o/del.txt")
    assert r.status_code == 204
    r = gcs_client.get("/storage/v1/b/bkt3/o/del.txt")
    assert r.status_code == 404


def test_get_object_metadata(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "meta-bkt"})
    gcs_client.post(
        "/upload/storage/v1/b/meta-bkt/o?name=file.txt&uploadType=media",
        content=b"data",
        headers={"content-type": "text/plain"},
    )
    r = gcs_client.get("/storage/v1/b/meta-bkt/o/file.txt")
    assert r.status_code == 200
    meta = r.json()
    assert meta["name"] == "file.txt"
    assert meta["size"] == "4"
    assert meta["contentType"] == "text/plain"


def test_copy_object(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "src-bkt"})
    gcs_client.post("/storage/v1/b", json={"name": "dst-bkt"})
    gcs_client.post(
        "/upload/storage/v1/b/src-bkt/o?name=orig.txt&uploadType=media",
        content=b"copy me",
        headers={"content-type": "text/plain"},
    )
    r = gcs_client.post(
        "/storage/v1/b/src-bkt/o/orig.txt/copyTo/b/dst-bkt/o/copy.txt"
    )
    assert r.status_code == 200
    r = gcs_client.get("/download/storage/v1/b/dst-bkt/o/copy.txt")
    assert r.content == b"copy me"


def test_multipart_upload(gcs_client):
    """uploadType=multipart carries name + content-type in the metadata part."""
    import json
    gcs_client.post("/storage/v1/b", json={"name": "mp-bucket"})
    boundary = "foo_boundary"
    metadata = json.dumps({"name": "multi.json", "contentType": "application/json"})
    body_bytes = b'{"key": "value"}'
    payload = (
        f"--{boundary}\r\n"
        "Content-Type: application/json\r\n\r\n"
        f"{metadata}\r\n"
        f"--{boundary}\r\n"
        "Content-Type: application/json\r\n\r\n"
    ).encode() + body_bytes + f"\r\n--{boundary}--".encode()

    r = gcs_client.post(
        "/upload/storage/v1/b/mp-bucket/o?uploadType=multipart",
        content=payload,
        headers={"content-type": f"multipart/related; boundary={boundary}"},
    )
    assert r.status_code == 200
    assert r.json()["name"] == "multi.json"

    r = gcs_client.get("/download/storage/v1/b/mp-bucket/o/multi.json")
    assert r.content == body_bytes


def test_upload_missing_name_returns_400(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "noname-bkt"})
    r = gcs_client.post(
        "/upload/storage/v1/b/noname-bkt/o?uploadType=media",
        content=b"data",
        headers={"content-type": "text/plain"},
    )
    assert r.status_code == 400


def test_upload_missing_bucket_returns_404(gcs_client):
    r = gcs_client.post(
        "/upload/storage/v1/b/ghost-bucket/o?name=f.txt&uploadType=media",
        content=b"data",
        headers={"content-type": "text/plain"},
    )
    assert r.status_code == 404


def test_get_missing_bucket_returns_404(gcs_client):
    r = gcs_client.get("/storage/v1/b/no-such-bucket")
    assert r.status_code == 404


def test_get_missing_object_returns_404(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "empty-bkt"})
    r = gcs_client.get("/storage/v1/b/empty-bkt/o/phantom.txt")
    assert r.status_code == 404


def test_download_alt_media(gcs_client):
    """GET with ?alt=media on the metadata endpoint streams the object body."""
    gcs_client.post("/storage/v1/b", json={"name": "alt-bkt"})
    gcs_client.post(
        "/upload/storage/v1/b/alt-bkt/o?name=payload.bin&uploadType=media",
        content=b"binary content",
        headers={"content-type": "application/octet-stream"},
    )
    r = gcs_client.get("/storage/v1/b/alt-bkt/o/payload.bin?alt=media")
    assert r.status_code == 200
    assert r.content == b"binary content"


def test_metadata_patch(gcs_client):
    """PATCH updates mutable fields without touching the body."""
    gcs_client.post("/storage/v1/b", json={"name": "patch-bkt"})
    gcs_client.post(
        "/upload/storage/v1/b/patch-bkt/o?name=obj.txt&uploadType=media",
        content=b"original",
        headers={"content-type": "text/plain"},
    )
    r = gcs_client.patch(
        "/storage/v1/b/patch-bkt/o/obj.txt",
        json={"contentType": "text/markdown", "metadata": {"author": "alice"}},
    )
    assert r.status_code == 200
    assert r.json()["contentType"] == "text/markdown"
    assert r.json()["metadata"]["author"] == "alice"

    # Body is still intact
    r = gcs_client.get("/download/storage/v1/b/patch-bkt/o/obj.txt")
    assert r.content == b"original"


def test_overwrite_increments_generation(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "gen-bkt"})
    gcs_client.post(
        "/upload/storage/v1/b/gen-bkt/o?name=f.bin&uploadType=media",
        content=b"v1",
        headers={"content-type": "application/octet-stream"},
    )
    r1 = gcs_client.get("/storage/v1/b/gen-bkt/o/f.bin")
    gen1 = int(r1.json()["generation"])

    gcs_client.post(
        "/upload/storage/v1/b/gen-bkt/o?name=f.bin&uploadType=media",
        content=b"v2",
        headers={"content-type": "application/octet-stream"},
    )
    r2 = gcs_client.get("/storage/v1/b/gen-bkt/o/f.bin")
    gen2 = int(r2.json()["generation"])

    assert gen2 > gen1


def test_checksums_present_in_metadata(gcs_client):
    """md5Hash and crc32c are computed and returned on upload."""
    gcs_client.post("/storage/v1/b", json={"name": "chk-bkt"})
    r = gcs_client.post(
        "/upload/storage/v1/b/chk-bkt/o?name=chk.txt&uploadType=media",
        content=b"checksum me",
        headers={"content-type": "text/plain"},
    )
    assert r.status_code == 200
    meta = r.json()
    assert meta["md5Hash"]
    assert meta["crc32c"]
    assert meta["etag"] == meta["md5Hash"]


def test_list_objects_delimiter_virtual_dirs(gcs_client):
    """delimiter collapses common prefixes into the prefixes[] result."""
    gcs_client.post("/storage/v1/b", json={"name": "delim-bkt"})
    for name in ("a/1.txt", "a/2.txt", "b/3.txt", "top.txt"):
        gcs_client.post(
            f"/upload/storage/v1/b/delim-bkt/o?name={name}&uploadType=media",
            content=b"x",
            headers={"content-type": "text/plain"},
        )
    r = gcs_client.get("/storage/v1/b/delim-bkt/o?delimiter=/")
    assert r.status_code == 200
    body = r.json()
    assert set(body["prefixes"]) == {"a/", "b/"}
    assert [o["name"] for o in body["items"]] == ["top.txt"]


def test_delete_missing_object_returns_404(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "del404-bkt"})
    r = gcs_client.delete("/storage/v1/b/del404-bkt/o/ghost.txt")
    assert r.status_code == 404


def test_copy_to_missing_bucket_returns_404(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "copy-src"})
    gcs_client.post(
        "/upload/storage/v1/b/copy-src/o?name=f.txt&uploadType=media",
        content=b"data",
        headers={"content-type": "text/plain"},
    )
    r = gcs_client.post("/storage/v1/b/copy-src/o/f.txt/copyTo/b/no-dst/o/f.txt")
    assert r.status_code == 404


def test_create_bucket_without_name_returns_400(gcs_client):
    r = gcs_client.post("/storage/v1/b", json={})
    assert r.status_code == 400


def test_delete_missing_bucket_returns_404(gcs_client):
    r = gcs_client.delete("/storage/v1/b/no-such-bucket-xyz")
    assert r.status_code == 404


def test_download_missing_object_returns_404(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "dl-bkt"})
    r = gcs_client.get("/download/storage/v1/b/dl-bkt/o/missing.bin")
    assert r.status_code == 404


def test_copy_missing_source_object_returns_404(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "csrc-bkt"})
    gcs_client.post("/storage/v1/b", json={"name": "cdst-bkt"})
    r = gcs_client.post("/storage/v1/b/csrc-bkt/o/ghost.txt/copyTo/b/cdst-bkt/o/copy.txt")
    assert r.status_code == 404


def test_list_objects_missing_bucket_returns_404(gcs_client):
    r = gcs_client.get("/storage/v1/b/no-bkt/o")
    assert r.status_code == 404


def test_notification_crud(gcs_client):
    """Create, get, list, and delete notification configs."""
    gcs_client.post("/storage/v1/b", json={"name": "notif-bkt"})

    # Create
    r = gcs_client.post(
        "/storage/v1/b/notif-bkt/notificationConfigs",
        json={"topic": "projects/p/topics/t", "payload_format": "JSON_API_V1"},
    )
    assert r.status_code == 200
    notif_id = r.json()["id"]

    # Get
    r2 = gcs_client.get(f"/storage/v1/b/notif-bkt/notificationConfigs/{notif_id}")
    assert r2.status_code == 200

    # List
    r3 = gcs_client.get("/storage/v1/b/notif-bkt/notificationConfigs")
    assert r3.status_code == 200

    # Delete
    r4 = gcs_client.delete(f"/storage/v1/b/notif-bkt/notificationConfigs/{notif_id}")
    assert r4.status_code == 204


def test_list_notifications_missing_bucket_returns_404(gcs_client):
    r = gcs_client.get("/storage/v1/b/no-such-bkt/notificationConfigs")
    assert r.status_code == 404


def test_delete_missing_notification_returns_404(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "notif-del-bkt"})
    r = gcs_client.delete("/storage/v1/b/notif-del-bkt/notificationConfigs/999")
    assert r.status_code == 404


def test_create_notification_missing_bucket_returns_404(gcs_client):
    r = gcs_client.post(
        "/storage/v1/b/no-bkt/notificationConfigs",
        json={"topic": "projects/p/topics/t", "payload_format": "JSON_API_V1"},
    )
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Byte-range downloads
# ---------------------------------------------------------------------------

def _upload(gcs_client, bucket, name, content):
    gcs_client.post("/storage/v1/b", json={"name": bucket})
    gcs_client.post(
        f"/upload/storage/v1/b/{bucket}/o?name={name}&uploadType=media",
        content=content,
        headers={"content-type": "application/octet-stream"},
    )


def test_range_full_via_alt_media(gcs_client):
    _upload(gcs_client, "rbkt", "file.bin", b"0123456789")
    r = gcs_client.get(
        "/storage/v1/b/rbkt/o/file.bin?alt=media",
        headers={"range": "bytes=0-9"},
    )
    assert r.status_code == 206
    assert r.content == b"0123456789"
    assert r.headers["content-range"] == "bytes 0-9/10"


def test_range_partial_via_alt_media(gcs_client):
    _upload(gcs_client, "rbkt2", "file.bin", b"abcdefghij")
    r = gcs_client.get(
        "/storage/v1/b/rbkt2/o/file.bin?alt=media",
        headers={"range": "bytes=2-5"},
    )
    assert r.status_code == 206
    assert r.content == b"cdef"
    assert r.headers["content-range"] == "bytes 2-5/10"


def test_range_open_end(gcs_client):
    _upload(gcs_client, "rbkt3", "file.bin", b"abcdefghij")
    r = gcs_client.get(
        "/storage/v1/b/rbkt3/o/file.bin?alt=media",
        headers={"range": "bytes=7-"},
    )
    assert r.status_code == 206
    assert r.content == b"hij"
    assert r.headers["content-range"] == "bytes 7-9/10"


def test_range_suffix(gcs_client):
    _upload(gcs_client, "rbkt4", "file.bin", b"abcdefghij")
    r = gcs_client.get(
        "/storage/v1/b/rbkt4/o/file.bin?alt=media",
        headers={"range": "bytes=-3"},
    )
    assert r.status_code == 206
    assert r.content == b"hij"
    assert r.headers["content-range"] == "bytes 7-9/10"


def test_range_unsatisfiable_returns_416(gcs_client):
    _upload(gcs_client, "rbkt5", "file.bin", b"hello")
    r = gcs_client.get(
        "/storage/v1/b/rbkt5/o/file.bin?alt=media",
        headers={"range": "bytes=100-200"},
    )
    assert r.status_code == 416


def test_no_range_returns_200_with_accept_ranges(gcs_client):
    _upload(gcs_client, "rbkt6", "file.bin", b"hello")
    r = gcs_client.get("/storage/v1/b/rbkt6/o/file.bin?alt=media")
    assert r.status_code == 200
    assert r.headers.get("accept-ranges") == "bytes"
    assert r.content == b"hello"


# ---------------------------------------------------------------------------
# Compose objects
# ---------------------------------------------------------------------------


def test_compose_basic(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "cbkt"})
    for i, chunk in enumerate([b"hello ", b"world", b"!"]):
        gcs_client.post(
            f"/upload/storage/v1/b/cbkt/o?name=part{i}&uploadType=media",
            content=chunk, headers={"content-type": "text/plain"},
        )
    r = gcs_client.post(
        "/storage/v1/b/cbkt/o/composed.txt/compose",
        json={"sourceObjects": [{"name": "part0"}, {"name": "part1"}, {"name": "part2"}],
              "destination": {"contentType": "text/plain"}},
    )
    assert r.status_code == 200
    assert r.json()["name"] == "composed.txt"
    body = gcs_client.get("/storage/v1/b/cbkt/o/composed.txt?alt=media").content
    assert body == b"hello world!"


def test_compose_missing_source_returns_404(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "cbkt2"})
    r = gcs_client.post(
        "/storage/v1/b/cbkt2/o/out/compose",
        json={"sourceObjects": [{"name": "ghost"}]},
    )
    assert r.status_code == 404


def test_compose_too_many_sources_returns_400(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "cbkt3"})
    r = gcs_client.post(
        "/storage/v1/b/cbkt3/o/out/compose",
        json={"sourceObjects": [{"name": f"x{i}"} for i in range(33)]},
    )
    assert r.status_code == 400


def test_compose_generation_match_mismatch(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "cbkt4"})
    gcs_client.post(
        "/upload/storage/v1/b/cbkt4/o?name=src&uploadType=media",
        content=b"data", headers={"content-type": "text/plain"},
    )
    r = gcs_client.post(
        "/storage/v1/b/cbkt4/o/out/compose",
        json={"sourceObjects": [{"name": "src", "objectPreconditions": {"ifGenerationMatch": "999"}}]},
    )
    assert r.status_code == 412


def test_range_via_download_path(gcs_client):
    _upload(gcs_client, "rbkt7", "file.bin", b"0123456789")
    r = gcs_client.get(
        "/download/storage/v1/b/rbkt7/o/file.bin",
        headers={"range": "bytes=3-6"},
    )
    assert r.status_code == 206
    assert r.content == b"3456"
    assert r.headers["content-range"] == "bytes 3-6/10"


# ---------------------------------------------------------------------------
# Conditional requests
# ---------------------------------------------------------------------------


def test_if_match_success(gcs_client):
    _upload(gcs_client, "cond1", "f.bin", b"data")
    meta = gcs_client.get("/storage/v1/b/cond1/o/f.bin").json()
    etag = meta["etag"]
    r = gcs_client.get("/storage/v1/b/cond1/o/f.bin?alt=media", headers={"if-match": etag})
    assert r.status_code == 200


def test_if_match_mismatch_returns_412(gcs_client):
    _upload(gcs_client, "cond2", "f.bin", b"data")
    r = gcs_client.get("/storage/v1/b/cond2/o/f.bin?alt=media", headers={"if-match": "wrong-etag"})
    assert r.status_code == 412


def test_if_none_match_star_returns_412_when_exists(gcs_client):
    _upload(gcs_client, "cond3", "f.bin", b"data")
    r = gcs_client.get("/storage/v1/b/cond3/o/f.bin?alt=media", headers={"if-none-match": "*"})
    assert r.status_code == 412


def test_if_none_match_etag_returns_304_when_matches(gcs_client):
    _upload(gcs_client, "cond4", "f.bin", b"data")
    etag = gcs_client.get("/storage/v1/b/cond4/o/f.bin").json()["etag"]
    r = gcs_client.get("/storage/v1/b/cond4/o/f.bin?alt=media", headers={"if-none-match": etag})
    assert r.status_code == 304


def test_if_generation_match_on_delete(gcs_client):
    _upload(gcs_client, "cond5", "f.bin", b"data")
    gen = gcs_client.get("/storage/v1/b/cond5/o/f.bin").json()["generation"]
    r = gcs_client.delete(f"/storage/v1/b/cond5/o/f.bin?ifGenerationMatch=999")
    assert r.status_code == 412
    r = gcs_client.delete(f"/storage/v1/b/cond5/o/f.bin?ifGenerationMatch={gen}")
    assert r.status_code == 204


def test_if_generation_match_zero_on_upload_prevents_overwrite(gcs_client):
    _upload(gcs_client, "cond6", "f.bin", b"original")
    r = gcs_client.post(
        "/upload/storage/v1/b/cond6/o?name=f.bin&uploadType=media&ifGenerationMatch=0",
        content=b"new", headers={"content-type": "text/plain"},
    )
    assert r.status_code == 412
    assert gcs_client.get("/storage/v1/b/cond6/o/f.bin?alt=media").content == b"original"


def test_if_generation_match_zero_on_upload_allows_new_object(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "cond7"})
    r = gcs_client.post(
        "/upload/storage/v1/b/cond7/o?name=new.bin&uploadType=media&ifGenerationMatch=0",
        content=b"hello", headers={"content-type": "text/plain"},
    )
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# Rewrite object
# ---------------------------------------------------------------------------


def test_rewrite_same_bucket(gcs_client):
    _upload(gcs_client, "rwbkt", "src.txt", b"rewrite me")
    r = gcs_client.post(
        "/storage/v1/b/rwbkt/o/src.txt/rewriteTo/b/rwbkt/o/dst.txt",
        json={},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["done"] is True
    assert body["resource"]["name"] == "dst.txt"
    assert gcs_client.get("/storage/v1/b/rwbkt/o/dst.txt?alt=media").content == b"rewrite me"


def test_rewrite_cross_bucket(gcs_client):
    _upload(gcs_client, "rwsrc", "obj.bin", b"cross")
    gcs_client.post("/storage/v1/b", json={"name": "rwdst"})
    r = gcs_client.post(
        "/storage/v1/b/rwsrc/o/obj.bin/rewriteTo/b/rwdst/o/obj.bin",
        json={},
    )
    assert r.status_code == 200
    assert r.json()["done"] is True
    assert gcs_client.get("/storage/v1/b/rwdst/o/obj.bin?alt=media").content == b"cross"


def test_rewrite_changes_content_type(gcs_client):
    _upload(gcs_client, "rwct", "f.bin", b"data")
    r = gcs_client.post(
        "/storage/v1/b/rwct/o/f.bin/rewriteTo/b/rwct/o/f.txt",
        json={"contentType": "text/plain"},
    )
    assert r.status_code == 200
    assert r.json()["resource"]["contentType"] == "text/plain"


def test_rewrite_missing_source_returns_404(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "rwnone"})
    r = gcs_client.post(
        "/storage/v1/b/rwnone/o/ghost/rewriteTo/b/rwnone/o/dst",
        json={},
    )
    assert r.status_code == 404


def test_rewrite_missing_dst_bucket_returns_404(gcs_client):
    _upload(gcs_client, "rwsrc2", "f.bin", b"x")
    r = gcs_client.post(
        "/storage/v1/b/rwsrc2/o/f.bin/rewriteTo/b/no-such-bucket/o/f.bin",
        json={},
    )
    assert r.status_code == 404


def test_if_metageneration_match_on_patch(gcs_client):
    _upload(gcs_client, "cond8", "f.bin", b"data")
    meta = gcs_client.get("/storage/v1/b/cond8/o/f.bin").json()
    metagen = meta["metageneration"]
    r = gcs_client.patch(
        f"/storage/v1/b/cond8/o/f.bin?ifMetagenerationMatch=999",
        json={"contentType": "text/plain"},
    )
    assert r.status_code == 412
    r = gcs_client.patch(
        f"/storage/v1/b/cond8/o/f.bin?ifMetagenerationMatch={metagen}",
        json={"contentType": "text/plain"},
    )
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# CORS configuration
# ---------------------------------------------------------------------------

CORS_CONFIG = [
    {
        "origin": ["https://example.com", "https://app.example.com"],
        "method": ["GET", "POST", "PUT"],
        "responseHeader": ["Content-Type", "Authorization"],
        "maxAgeSeconds": 3600,
    }
]


def test_cors_set_and_get(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "cors1"})

    r = gcs_client.put("/storage/v1/b/cors1/cors", json={"cors": CORS_CONFIG})
    assert r.status_code == 200
    assert r.json()["cors"] == CORS_CONFIG

    r = gcs_client.get("/storage/v1/b/cors1/cors")
    assert r.status_code == 200
    assert r.json()["cors"] == CORS_CONFIG


def test_cors_via_patch_bucket(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "cors2"})
    r = gcs_client.patch("/storage/v1/b/cors2", json={"cors": CORS_CONFIG})
    assert r.status_code == 200
    assert r.json()["cors"] == CORS_CONFIG


def test_cors_returned_in_bucket_get(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "cors3"})
    gcs_client.put("/storage/v1/b/cors3/cors", json={"cors": CORS_CONFIG})
    r = gcs_client.get("/storage/v1/b/cors3")
    assert r.json()["cors"] == CORS_CONFIG


def test_cors_delete(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "cors4"})
    gcs_client.put("/storage/v1/b/cors4/cors", json={"cors": CORS_CONFIG})
    r = gcs_client.delete("/storage/v1/b/cors4/cors")
    assert r.status_code == 204
    r = gcs_client.get("/storage/v1/b/cors4/cors")
    assert r.json()["cors"] == []


def test_cors_empty_by_default(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "cors5"})
    r = gcs_client.get("/storage/v1/b/cors5/cors")
    assert r.status_code == 200
    assert r.json()["cors"] == []


# ---------------------------------------------------------------------------
# Retention policy
# ---------------------------------------------------------------------------


def test_retention_policy_set_and_get(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "ret1"})
    r = gcs_client.patch("/storage/v1/b/ret1/retentionPolicy", json={
        "retentionPolicy": {"retentionPeriod": "3600"},
    })
    assert r.status_code == 200
    assert r.json()["retentionPolicy"]["retentionPeriod"] == "3600"

    r2 = gcs_client.get("/storage/v1/b/ret1/retentionPolicy")
    assert r2.status_code == 200
    assert r2.json()["retentionPolicy"]["retentionPeriod"] == "3600"


def test_retention_policy_in_bucket_metadata(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "ret2"})
    gcs_client.patch("/storage/v1/b/ret2/retentionPolicy", json={
        "retentionPolicy": {"retentionPeriod": "86400"},
    })
    r = gcs_client.get("/storage/v1/b/ret2")
    assert r.status_code == 200
    assert r.json()["retentionPolicy"]["retentionPeriod"] == "86400"


def test_retention_policy_object_gets_expiry(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "ret3"})
    gcs_client.patch("/storage/v1/b/ret3/retentionPolicy", json={
        "retentionPolicy": {"retentionPeriod": "3600"},
    })
    r = gcs_client.post(
        "/upload/storage/v1/b/ret3/o?name=file.txt&uploadType=media",
        content=b"hello",
        headers={"content-type": "text/plain"},
    )
    assert r.status_code == 200
    meta = r.json()
    assert meta.get("retentionExpirationTime") != ""


def test_retention_policy_blocks_delete(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "ret4"})
    gcs_client.patch("/storage/v1/b/ret4/retentionPolicy", json={
        "retentionPolicy": {"retentionPeriod": "999999"},
    })
    gcs_client.post(
        "/upload/storage/v1/b/ret4/o?name=file.txt&uploadType=media",
        content=b"hello",
        headers={"content-type": "text/plain"},
    )
    r = gcs_client.delete("/storage/v1/b/ret4/o/file.txt")
    assert r.status_code == 403


def test_retention_policy_allows_delete_after_expiry(gcs_client):
    """Objects without a retention policy (period=0) can be deleted freely."""
    gcs_client.post("/storage/v1/b", json={"name": "ret5"})
    gcs_client.post(
        "/upload/storage/v1/b/ret5/o?name=file.txt&uploadType=media",
        content=b"hello",
        headers={"content-type": "text/plain"},
    )
    r = gcs_client.delete("/storage/v1/b/ret5/o/file.txt")
    assert r.status_code == 204


def test_retention_policy_delete_removes_policy(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "ret6"})
    gcs_client.patch("/storage/v1/b/ret6/retentionPolicy", json={
        "retentionPolicy": {"retentionPeriod": "3600"},
    })
    r = gcs_client.delete("/storage/v1/b/ret6/retentionPolicy")
    assert r.status_code == 204
    r2 = gcs_client.get("/storage/v1/b/ret6/retentionPolicy")
    assert r2.json()["retentionPolicy"] == {}


def test_retention_policy_locked_cannot_be_removed(gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "ret7"})
    gcs_client.patch("/storage/v1/b/ret7/retentionPolicy", json={
        "retentionPolicy": {"retentionPeriod": "3600", "isLocked": True},
    })
    r = gcs_client.delete("/storage/v1/b/ret7/retentionPolicy")
    assert r.status_code == 403
