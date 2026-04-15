"""Cloud Storage (GCS) emulator.

Implements the GCS JSON API v1 endpoints used by google-cloud-storage.
"""
from __future__ import annotations

import base64
import hashlib
import uuid
from typing import Annotated

from fastapi import FastAPI, Header, Query, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse

from localgcp.core.errors import GCPError, add_gcp_exception_handler
from localgcp.core.middleware import add_request_logging
from localgcp.services.gcs.models import (
    BucketListResponse,
    BucketModel,
    Lifecycle,
    LifecycleCondition,
    NotificationConfig,
    NotificationListResponse,
    ObjectListResponse,
    ObjectModel,
)
from localgcp.services.gcs.store import get_store

app = FastAPI(title="LocalGCP — Cloud Storage", version="v1")
add_gcp_exception_handler(app)
add_request_logging(app, "gcs")


def _store():
    return get_store()


# ---------------------------------------------------------------------------
# Buckets
# ---------------------------------------------------------------------------


@app.post("/storage/v1/b", status_code=200)
async def create_bucket(request: Request):
    body = await request.json()
    name = body.get("name", "")
    if not name:
        raise GCPError(400, "name is required")
    store = _store()
    if store.exists("buckets", name):
        raise GCPError(409, f"You already own this bucket. Please select another name.")
    bucket = BucketModel(name=name, **{k: v for k, v in body.items() if k != "name"})
    store.set("buckets", name, bucket.model_dump())
    return bucket.model_dump()


@app.get("/storage/v1/b")
async def list_buckets(project: str = Query(default="local-project")):
    store = _store()
    items = [BucketModel(**b) for b in store.list("buckets")]
    return BucketListResponse(items=items).model_dump(exclude_none=True)


@app.get("/storage/v1/b/{bucket}")
async def get_bucket(bucket: str):
    store = _store()
    data = store.get("buckets", bucket)
    if data is None:
        raise GCPError(404, f"The specified bucket does not exist.")
    return data


@app.patch("/storage/v1/b/{bucket}")
async def patch_bucket(bucket: str, request: Request):
    store = _store()
    data = store.get("buckets", bucket)
    if data is None:
        raise GCPError(404, "The specified bucket does not exist.")
    body = await request.json()
    for field in ("lifecycle", "labels", "storageClass", "location"):
        if field in body:
            data[field] = body[field]
    from localgcp.services.gcs.models import _now_rfc3339
    data["updated"] = _now_rfc3339()
    data["metageneration"] = str(int(data.get("metageneration", "1")) + 1)
    store.set("buckets", bucket, data)
    return data


@app.delete("/storage/v1/b/{bucket}", status_code=204)
async def delete_bucket(bucket: str):
    store = _store()
    if not store.exists("buckets", bucket):
        raise GCPError(404, f"The specified bucket does not exist.")
    # Check if bucket is empty
    obj_keys = [k for k in store.keys("objects") if k.startswith(f"{bucket}/")]
    if obj_keys:
        raise GCPError(409, f"The bucket you tried to delete is not empty.")
    store.delete("buckets", bucket)
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Objects — upload
# ---------------------------------------------------------------------------


@app.post("/upload/storage/v1/b/{bucket}/o")
async def upload_object(
    bucket: str,
    request: Request,
    name: str = Query(default=""),
    uploadType: str = Query(default="media"),
    content_type: Annotated[str | None, Header(alias="content-type")] = None,
    x_upload_content_type: Annotated[str | None, Header(alias="x-upload-content-type")] = None,
    x_upload_content_length: Annotated[str | None, Header(alias="x-upload-content-length")] = None,
):
    store = _store()
    if not store.exists("buckets", bucket):
        raise GCPError(404, f"The specified bucket does not exist.")

    ct = content_type or "application/octet-stream"

    if uploadType == "resumable":
        return await _initiate_resumable(
            request, store, bucket, name,
            x_upload_content_type or ct,
            int(x_upload_content_length) if x_upload_content_length else None,
        )

    if uploadType == "multipart":
        # Parse multipart/related: first part is metadata JSON, second is body
        raw = await request.body()
        obj_name, body_bytes, metadata_ct = _parse_multipart(raw, ct)
        if not obj_name and not name:
            raise GCPError(400, "name is required")
        obj_name = obj_name or name
        ct = metadata_ct or ct
    else:
        # Simple media upload
        obj_name = name
        if not obj_name:
            raise GCPError(400, "name is required for uploadType=media")
        body_bytes = await request.body()

    return _store_object(store, bucket, obj_name, body_bytes, ct)


async def _initiate_resumable(
    request: Request,
    store,
    bucket: str,
    name: str,
    content_type: str,
    total_size: int | None,
) -> Response:
    """Initiate a resumable upload session; returns 200 with Location header."""
    try:
        body = await request.json()
    except Exception:
        body = {}

    obj_name = body.get("name", name)
    if not obj_name:
        raise GCPError(400, "name is required")

    ct = body.get("contentType", content_type) or "application/octet-stream"
    upload_id = str(uuid.uuid4())

    session = {
        "bucket": bucket,
        "name": obj_name,
        "content_type": ct,
        "total_size": total_size,
        "received": 0,
    }
    store.set("resumable_sessions", upload_id, session)
    store.set("resumable_bodies", upload_id, b"")

    location = f"/upload/storage/v1/b/{bucket}/o?uploadType=resumable&upload_id={upload_id}"
    return Response(status_code=200, headers={"Location": location})


@app.put("/upload/storage/v1/b/{bucket}/o")
async def upload_resumable_chunk(
    bucket: str,
    request: Request,
    upload_id: str = Query(...),
    content_range: Annotated[str | None, Header(alias="content-range")] = None,
):
    """Upload data (or a chunk) to an active resumable session."""
    store = _store()
    session = store.get("resumable_sessions", upload_id)
    if session is None:
        raise GCPError(404, f"Upload session not found: {upload_id}")

    body = await request.body()

    # Parse Content-Range header (e.g. "bytes 0-999/1000" or "bytes */1000")
    total_size = session.get("total_size")
    is_status_query = False

    if content_range:
        if content_range.startswith("bytes */"):
            is_status_query = True
            try:
                total_size = int(content_range[len("bytes */"):])
            except ValueError:
                pass
        elif content_range.startswith("bytes "):
            rest = content_range[len("bytes "):]
            range_part, total_part = rest.split("/", 1)
            if total_part != "*":
                try:
                    total_size = int(total_part)
                except ValueError:
                    pass
        if total_size is not None:
            session["total_size"] = total_size
            store.set("resumable_sessions", upload_id, session)

    # Status query (empty body, Content-Range: bytes */N)
    if is_status_query or not body:
        received = session["received"]
        headers: dict[str, str] = {}
        if received > 0:
            headers["Range"] = f"bytes=0-{received - 1}"
        return Response(status_code=308, headers=headers)

    # Accumulate chunk
    accumulated = (store.get("resumable_bodies", upload_id) or b"") + body
    session["received"] = len(accumulated)
    store.set("resumable_sessions", upload_id, session)
    store.set("resumable_bodies", upload_id, accumulated)

    # Finalize when all data is received
    if total_size is None or len(accumulated) >= total_size:
        obj = _store_object(
            store, bucket, session["name"], accumulated, session["content_type"]
        )
        store.delete("resumable_sessions", upload_id)
        store.delete("resumable_bodies", upload_id)
        return obj

    # More chunks expected
    return Response(
        status_code=308,
        headers={"Range": f"bytes=0-{len(accumulated) - 1}"},
    )


def _parse_multipart(raw: bytes, content_type: str) -> tuple[str, bytes, str]:
    """Very minimal multipart/related parser for GCS multipart uploads."""
    import email
    import email.policy

    # Extract boundary from content-type header
    boundary = None
    for part in content_type.split(";"):
        p = part.strip()
        if p.startswith("boundary="):
            boundary = p[len("boundary="):].strip('"')
            break

    if not boundary:
        return "", raw, content_type

    # Split on boundary
    delimiter = f"--{boundary}".encode()
    parts = raw.split(delimiter)
    # parts[0] = preamble, parts[1..n-1] = body parts, parts[-1] = epilogue
    segments = [p for p in parts[1:] if p not in (b"", b"--\r\n", b"--")]

    obj_name = ""
    body_bytes = b""
    body_ct = ""

    for i, seg in enumerate(segments):
        if seg.startswith(b"--"):
            break
        # Strip leading \r\n
        seg = seg.lstrip(b"\r\n")
        # Split headers from body
        if b"\r\n\r\n" in seg:
            headers_raw, body = seg.split(b"\r\n\r\n", 1)
        else:
            headers_raw, body = seg, b""

        body = body.rstrip(b"\r\n")
        headers_str = headers_raw.decode("utf-8", errors="replace")

        seg_ct = ""
        for line in headers_str.splitlines():
            if line.lower().startswith("content-type:"):
                seg_ct = line.split(":", 1)[1].strip()

        if i == 0:
            # Metadata part — parse JSON for name
            import json
            try:
                meta = json.loads(body)
                obj_name = meta.get("name", "")
            except Exception:
                pass
        else:
            body_bytes = body
            body_ct = seg_ct

    return obj_name, body_bytes, body_ct


def _store_object(store, bucket: str, name: str, body: bytes, content_type: str) -> dict:
    from localgcp.services.gcs.models import _now_rfc3339

    key = f"{bucket}/{name}"
    md5 = base64.b64encode(hashlib.md5(body).digest()).decode()
    crc32c_val = _crc32c_b64(body)
    size = str(len(body))

    existing = store.get("objects", key)
    generation = str(int(existing["generation"]) + 1) if existing else "1"

    obj = ObjectModel(
        name=name,
        bucket=bucket,
        generation=generation,
        contentType=content_type,
        size=size,
        md5Hash=md5,
        crc32c=crc32c_val,
        etag=md5,
        timeCreated=existing["timeCreated"] if existing else _now_rfc3339(),
        updated=_now_rfc3339(),
    )
    result = obj.model_dump()
    store.set("objects", key, result)
    store.set("bodies", key, body)
    _fire_notifications(store, bucket, "OBJECT_FINALIZE", result)
    return result


def _crc32c_b64(data: bytes) -> str:
    import struct
    crc = 0
    for byte in data:
        crc ^= byte << 24
        for _ in range(8):
            if crc & 0x80000000:
                crc = (crc << 1) ^ 0x1EDC6F41
            else:
                crc <<= 1
        crc &= 0xFFFFFFFF
    return base64.b64encode(struct.pack(">I", crc)).decode()


# ---------------------------------------------------------------------------
# Lifecycle helpers
# ---------------------------------------------------------------------------


def _lifecycle_condition_matches(obj: dict, condition: LifecycleCondition) -> bool:
    """Return True if the object satisfies all conditions in the lifecycle rule."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)

    if condition.age is not None:
        time_created_str = obj.get("timeCreated", "")
        if not time_created_str:
            return False
        try:
            tc = datetime.fromisoformat(time_created_str.replace("Z", "+00:00"))
            if (now - tc).days < condition.age:
                return False
        except ValueError:
            return False

    if condition.createdBefore:
        time_created_str = obj.get("timeCreated", "")
        if not time_created_str:
            return False
        try:
            tc = datetime.fromisoformat(time_created_str.replace("Z", "+00:00"))
            cb = datetime.fromisoformat(condition.createdBefore.replace("Z", "+00:00"))
            if tc >= cb:
                return False
        except ValueError:
            return False

    if condition.matchesStorageClass:
        if obj.get("storageClass", "STANDARD") not in condition.matchesStorageClass:
            return False

    return True


def _apply_lifecycle(store, bucket: str) -> None:
    """Apply the bucket's lifecycle rules to all its objects (lazy enforcement)."""
    bucket_data = store.get("buckets", bucket)
    if not bucket_data:
        return
    lifecycle_raw = bucket_data.get("lifecycle")
    if not lifecycle_raw:
        return
    lifecycle = Lifecycle(**lifecycle_raw)
    if not lifecycle.rule:
        return

    prefix = f"{bucket}/"
    for key in list(store.keys("objects")):
        if not key.startswith(prefix):
            continue
        obj = store.get("objects", key)
        if obj is None:
            continue
        for rule in lifecycle.rule:
            cond = rule.condition
            if not _lifecycle_condition_matches(obj, cond):
                continue
            action_type = rule.action.type
            if action_type == "Delete":
                store.delete("objects", key)
                store.delete("bodies", key)
                _fire_notifications(store, bucket, "OBJECT_DELETE", obj)
                break
            elif action_type == "SetStorageClass":
                new_class = rule.action.storageClass
                if new_class and obj.get("storageClass") != new_class:
                    obj["storageClass"] = new_class
                    store.set("objects", key, obj)
                break


# ---------------------------------------------------------------------------
# Objects — metadata & download
# ---------------------------------------------------------------------------


@app.get("/storage/v1/b/{bucket}/o")
async def list_objects(
    bucket: str,
    prefix: str = Query(default=""),
    delimiter: str = Query(default=""),
    maxResults: int = Query(default=1000),
    pageToken: str = Query(default=""),
):
    store = _store()
    if not store.exists("buckets", bucket):
        raise GCPError(404, f"The specified bucket does not exist.")

    _apply_lifecycle(store, bucket)

    all_keys = store.keys("objects")
    bucket_prefix = f"{bucket}/"
    names = [k[len(bucket_prefix):] for k in all_keys if k.startswith(bucket_prefix)]

    if prefix:
        names = [n for n in names if n.startswith(prefix)]

    prefixes: set[str] = set()
    if delimiter:
        filtered = []
        for n in names:
            after_prefix = n[len(prefix):]
            idx = after_prefix.find(delimiter)
            if idx >= 0:
                prefixes.add(prefix + after_prefix[: idx + len(delimiter)])
            else:
                filtered.append(n)
        names = filtered

    names.sort()

    # Simple page token: offset as int string
    offset = int(pageToken) if pageToken else 0
    page = names[offset: offset + maxResults]
    next_token = str(offset + maxResults) if offset + maxResults < len(names) else None

    items = []
    for n in page:
        data = store.get("objects", f"{bucket}/{n}")
        if data:
            items.append(ObjectModel(**data))

    return ObjectListResponse(
        items=items,
        prefixes=sorted(prefixes),
        nextPageToken=next_token,
    ).model_dump(exclude_none=True)


@app.get("/storage/v1/b/{bucket}/o/{object_name:path}")
async def get_object_metadata(bucket: str, object_name: str, alt: str = Query(default="")):
    store = _store()
    key = f"{bucket}/{object_name}"
    data = store.get("objects", key)
    if data is None:
        raise GCPError(404, f"No such object: {bucket}/{object_name}")

    if alt == "media":
        body = store.get("bodies", key) or b""
        ct = data.get("contentType", "application/octet-stream")
        return Response(content=body, media_type=ct)

    return data


@app.get("/download/storage/v1/b/{bucket}/o/{object_name:path}")
async def download_object(bucket: str, object_name: str):
    store = _store()
    key = f"{bucket}/{object_name}"
    data = store.get("objects", key)
    if data is None:
        raise GCPError(404, f"No such object: {bucket}/{object_name}")
    body = store.get("bodies", key) or b""
    ct = data.get("contentType", "application/octet-stream")
    return Response(content=body, media_type=ct)


@app.patch("/storage/v1/b/{bucket}/o/{object_name:path}")
async def update_object_metadata(bucket: str, object_name: str, request: Request):
    store = _store()
    key = f"{bucket}/{object_name}"
    data = store.get("objects", key)
    if data is None:
        raise GCPError(404, f"No such object: {bucket}/{object_name}")
    body = await request.json()
    # Merge allowed mutable fields
    for field in ("contentType", "metadata", "contentDisposition", "cacheControl", "contentEncoding"):
        if field in body:
            data[field] = body[field]
    from localgcp.services.gcs.models import _now_rfc3339
    data["updated"] = _now_rfc3339()
    data["metageneration"] = str(int(data.get("metageneration", "1")) + 1)
    store.set("objects", key, data)
    _fire_notifications(store, bucket, "OBJECT_METADATA_UPDATE", data)
    return data


@app.delete("/storage/v1/b/{bucket}/o/{object_name:path}", status_code=204)
async def delete_object(bucket: str, object_name: str):
    store = _store()
    key = f"{bucket}/{object_name}"
    obj_data = store.get("objects", key)
    if obj_data is None:
        raise GCPError(404, f"No such object: {bucket}/{object_name}")
    store.delete("objects", key)
    store.delete("bodies", key)
    _fire_notifications(store, bucket, "OBJECT_DELETE", obj_data)
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Copy
# ---------------------------------------------------------------------------


@app.post("/storage/v1/b/{src_bucket}/o/{src_object:path}/copyTo/b/{dst_bucket}/o/{dst_object:path}")
async def copy_object(src_bucket: str, src_object: str, dst_bucket: str, dst_object: str):
    store = _store()
    src_key = f"{src_bucket}/{src_object}"
    data = store.get("objects", src_key)
    if data is None:
        raise GCPError(404, f"No such object: {src_bucket}/{src_object}")
    if not store.exists("buckets", dst_bucket):
        raise GCPError(404, f"The specified bucket does not exist.")
    body = store.get("bodies", src_key) or b""
    return _store_object(store, dst_bucket, dst_object, body, data.get("contentType", "application/octet-stream"))


# ---------------------------------------------------------------------------
# Notification configurations
# ---------------------------------------------------------------------------


def _next_notification_id(store, bucket: str) -> str:
    prefix = f"{bucket}/"
    existing_ids = [
        int(k[len(prefix):])
        for k in store.keys("notifications")
        if k.startswith(prefix) and k[len(prefix):].isdigit()
    ]
    return str(max(existing_ids, default=0) + 1)


@app.post("/storage/v1/b/{bucket}/notificationConfigs", status_code=200)
async def create_notification(bucket: str, request: Request):
    store = _store()
    if not store.exists("buckets", bucket):
        raise GCPError(404, "The specified bucket does not exist.")
    body = await request.json()
    notif_id = _next_notification_id(store, bucket)
    config = NotificationConfig(
        id=notif_id,
        selfLink=f"https://www.googleapis.com/storage/v1/b/{bucket}/notificationConfigs/{notif_id}",
        **{k: v for k, v in body.items() if k not in ("id", "selfLink", "kind", "etag")},
    )
    store.set("notifications", f"{bucket}/{notif_id}", config.model_dump())
    return config.model_dump()


@app.get("/storage/v1/b/{bucket}/notificationConfigs")
async def list_notifications(bucket: str):
    store = _store()
    if not store.exists("buckets", bucket):
        raise GCPError(404, "The specified bucket does not exist.")
    prefix = f"{bucket}/"
    items = [
        NotificationConfig(**v)
        for k, v in [(k, store.get("notifications", k)) for k in store.keys("notifications") if k.startswith(prefix)]
        if v is not None
    ]
    return NotificationListResponse(items=items).model_dump()


@app.get("/storage/v1/b/{bucket}/notificationConfigs/{notif_id}")
async def get_notification(bucket: str, notif_id: str):
    store = _store()
    data = store.get("notifications", f"{bucket}/{notif_id}")
    if data is None:
        raise GCPError(404, f"Notification config {notif_id} not found on bucket {bucket}.")
    return data


@app.delete("/storage/v1/b/{bucket}/notificationConfigs/{notif_id}", status_code=204)
async def delete_notification(bucket: str, notif_id: str):
    store = _store()
    if not store.delete("notifications", f"{bucket}/{notif_id}"):
        raise GCPError(404, f"Notification config {notif_id} not found on bucket {bucket}.")
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Notification dispatch helper
# ---------------------------------------------------------------------------


def _fire_notifications(store, bucket: str, event_type: str, obj_data: dict) -> None:
    """Publish a Pub/Sub notification for bucket/object events.

    Iterates all notification configs on the bucket and enqueues a message
    to the configured Pub/Sub topic for each matching config.
    No-ops silently if the topic or subscription doesn't exist yet.
    """
    import base64
    import json
    import uuid
    from datetime import datetime, timezone

    from localgcp.services.pubsub import store as ps_store

    prefix = f"{bucket}/"
    configs = [
        NotificationConfig(**v)
        for k in store.keys("notifications")
        if k.startswith(prefix)
        for v in [store.get("notifications", k)]
        if v is not None
    ]

    if not configs:
        return

    object_name = obj_data.get("name", "")
    generation = obj_data.get("generation", "1")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    for config in configs:
        if not config.matches(event_type, object_name):
            continue

        topic_name = config.pubsub_topic_name()
        pub_store = ps_store.get_store()
        if not pub_store.exists("topics", topic_name):
            continue

        attributes: dict[str, str] = {
            "notificationConfig": config.selfLink,
            "eventType": event_type,
            "bucketId": bucket,
            "objectId": object_name,
            "objectGeneration": generation,
            "payloadFormat": config.payload_format,
            **config.custom_attributes,
        }

        if config.payload_format == "JSON_API_V1":
            payload_bytes = json.dumps(obj_data).encode("utf-8")
        else:
            payload_bytes = b""

        msg_id = str(uuid.uuid4())
        message = {
            "data": base64.b64encode(payload_bytes).decode("utf-8"),
            "attributes": attributes,
            "messageId": msg_id,
            "publishTime": now,
            "orderingKey": "",
        }

        for sub in pub_store.list("subscriptions"):
            if sub.get("topic") == topic_name:
                sub_name = sub["name"]
                ps_store.ensure_queue(sub_name)
                ps_store.enqueue(sub_name, message)
