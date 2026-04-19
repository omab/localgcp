"""Cloud Storage (GCS) emulator.

Implements the GCS JSON API v1 endpoints used by google-cloud-storage.
"""

from __future__ import annotations

import base64
import hashlib
import uuid
from datetime import UTC
from typing import Annotated

from fastapi import FastAPI, Header, Query, Request, Response

from cloudbox.core.errors import GCPError, add_gcp_exception_handler
from cloudbox.core.middleware import add_request_logging
from cloudbox.services.gcs.models import (
    BucketListResponse,
    BucketModel,
    Lifecycle,
    LifecycleCondition,
    NotificationConfig,
    NotificationListResponse,
    ObjectListResponse,
    ObjectModel,
)
from cloudbox.services.gcs.store import get_store


def _check_preconditions(
    obj: dict | None,
    if_match: str | None = None,
    if_none_match: str | None = None,
    if_generation_match: str | None = None,
    if_metageneration_match: str | None = None,
) -> None:
    """Raise GCPError(412) or GCPError(304) if a precondition is not satisfied.

    if_match / if_none_match compare against the object's etag.
    if_generation_match / if_metageneration_match compare against the
    object's generation / metageneration. A value of "0" for
    if_generation_match means "object must not exist".

    Args:
        obj (dict | None): Current object metadata, or None if the object does not exist.
        if_match (str | None): Required etag value; "*" matches any existing object.
        if_none_match (str | None): Etag that must not match; "*" requires object absence.
        if_generation_match (str | None): Required generation number; "0" requires absence.
        if_metageneration_match (str | None): Required metageneration number.

    Raises:
        GCPError: With status 412 if a precondition fails, or 304 if If-None-Match matches.
    """
    etag = obj.get("etag", "") if obj else ""
    generation = str(obj.get("generation", "")) if obj else ""
    metageneration = str(obj.get("metageneration", "")) if obj else ""

    if if_generation_match is not None:
        expected = str(if_generation_match)
        if expected == "0":
            if obj is not None:
                raise GCPError(412, "Precondition Failed: object already exists")
        else:
            if obj is None or generation != expected:
                raise GCPError(412, "Precondition Failed: generation mismatch")

    if if_metageneration_match is not None:
        if obj is None or metageneration != str(if_metageneration_match):
            raise GCPError(412, "Precondition Failed: metageneration mismatch")

    if if_match is not None:
        if obj is None or (if_match != "*" and etag != if_match):
            raise GCPError(412, "Precondition Failed: If-Match")

    if if_none_match is not None:
        if if_none_match == "*" and obj is not None:
            raise GCPError(412, "Precondition Failed: If-None-Match")
        if obj is not None and etag == if_none_match:
            raise GCPError(304, "Not Modified")


def _parse_range(range_header: str, total: int) -> tuple[int, int] | None:
    """Parse a Range header and return (start, end) byte positions (inclusive).

    Returns None if the header is absent, malformed, or unsatisfiable.
    Supports: bytes=start-end, bytes=start-, bytes=-suffix

    Args:
        range_header (str): Value of the HTTP Range header (e.g. "bytes=0-499").
        total (int): Total number of bytes in the resource.

    Returns:
        tuple[int, int] | None: Inclusive (start, end) byte offsets, or None if unparseable.
    """
    if not range_header or not range_header.startswith("bytes="):
        return None
    spec = range_header[len("bytes=") :]
    if spec.startswith("-"):
        try:
            suffix = int(spec[1:])
        except ValueError:
            return None
        start = max(total - suffix, 0)
        end = total - 1
    elif spec.endswith("-"):
        try:
            start = int(spec[:-1])
        except ValueError:
            return None
        end = total - 1
    else:
        parts = spec.split("-", 1)
        if len(parts) != 2:
            return None
        try:
            start, end = int(parts[0]), int(parts[1])
        except ValueError:
            return None
    if start > end or start >= total:
        return None
    end = min(end, total - 1)
    return start, end


def _range_response(body: bytes, content_type: str, range_header: str | None) -> Response:
    """Return a full 200 or partial 206 response depending on the Range header.

    Args:
        body (bytes): Full object body bytes.
        content_type (str): MIME type to set on the response.
        range_header (str | None): Value of the HTTP Range header, or None for a full response.

    Returns:
        Response: A 200 response with the full body, a 206 partial response, or a 416 error.
    """
    total = len(body)
    headers = {"Accept-Ranges": "bytes"}
    if range_header:
        parsed = _parse_range(range_header, total)
        if parsed is None:
            return Response(
                status_code=416,
                headers={"Content-Range": f"bytes */{total}"},
            )
        start, end = parsed
        chunk = body[start : end + 1]
        headers["Content-Range"] = f"bytes {start}-{end}/{total}"
        headers["Content-Length"] = str(len(chunk))
        return Response(content=chunk, status_code=206, media_type=content_type, headers=headers)
    headers["Content-Length"] = str(total)
    return Response(content=body, status_code=200, media_type=content_type, headers=headers)


app = FastAPI(title="Cloudbox — Cloud Storage", version="v1")
add_gcp_exception_handler(app)
add_request_logging(app, "gcs")


def _store():
    """Return the GCS store instance.

    Returns:
        NamespacedStore: Shared GCS store used by all route handlers.
    """
    return get_store()


# ---------------------------------------------------------------------------
# Buckets
# ---------------------------------------------------------------------------


@app.post("/storage/v1/b", status_code=200)
async def create_bucket(request: Request):
    """Create a new GCS bucket.

    Args:
        request (Request): FastAPI request containing the JSON body with bucket name and options.

    Returns:
        dict: Newly created bucket metadata.

    Raises:
        GCPError: With status 400 if the name is missing, or 409 if the bucket already exists.
    """
    body = await request.json()
    name = body.get("name", "")
    if not name:
        raise GCPError(400, "name is required")
    store = _store()
    if store.exists("buckets", name):
        raise GCPError(409, "You already own this bucket. Please select another name.")
    bucket = BucketModel(name=name, **{k: v for k, v in body.items() if k != "name"})
    store.set("buckets", name, bucket.model_dump())
    return bucket.model_dump()


@app.get("/storage/v1/b")
async def list_buckets(project: str = Query(default="local-project")):
    """List all GCS buckets for a project.

    Args:
        project (str): GCP project ID used as a filter (defaults to "local-project").

    Returns:
        dict: BucketListResponse serialised as a dict.
    """
    store = _store()
    items = [BucketModel(**b) for b in store.list("buckets")]
    return BucketListResponse(items=items).model_dump(exclude_none=True)


@app.get("/storage/v1/b/{bucket}")
async def get_bucket(bucket: str):
    """Get a GCS bucket's metadata.

    Args:
        bucket (str): Name of the GCS bucket.

    Returns:
        dict: Bucket metadata dict.

    Raises:
        GCPError: With status 404 if the bucket does not exist.
    """
    store = _store()
    data = store.get("buckets", bucket)
    if data is None:
        raise GCPError(404, "The specified bucket does not exist.")
    return data


@app.patch("/storage/v1/b/{bucket}")
async def patch_bucket(bucket: str, request: Request):
    """Update mutable fields on a GCS bucket.

    Args:
        bucket (str): Name of the GCS bucket to update.
        request (Request): FastAPI request containing the JSON body with fields to update.

    Returns:
        dict: Updated bucket metadata.

    Raises:
        GCPError: With status 404 if the bucket does not exist.
    """
    store = _store()
    data = store.get("buckets", bucket)
    if data is None:
        raise GCPError(404, "The specified bucket does not exist.")
    body = await request.json()
    for field in ("lifecycle", "labels", "storageClass", "location", "cors", "retentionPolicy"):
        if field in body:
            data[field] = body[field]
    from cloudbox.services.gcs.models import _now_rfc3339

    data["updated"] = _now_rfc3339()
    data["metageneration"] = str(int(data.get("metageneration", "1")) + 1)
    store.set("buckets", bucket, data)
    return data


@app.delete("/storage/v1/b/{bucket}", status_code=204)
async def delete_bucket(bucket: str):
    """Delete a GCS bucket (must be empty).

    Args:
        bucket (str): Name of the GCS bucket to delete.

    Returns:
        Response: Empty 204 response on success.

    Raises:
        GCPError: With status 404 if the bucket does not exist, or 409 if it is not empty.
    """
    store = _store()
    if not store.exists("buckets", bucket):
        raise GCPError(404, "The specified bucket does not exist.")
    # Check if bucket is empty
    obj_keys = [k for k in store.keys("objects") if k.startswith(f"{bucket}/")]
    if obj_keys:
        raise GCPError(409, "The bucket you tried to delete is not empty.")
    store.delete("buckets", bucket)
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Bucket CORS configuration
# ---------------------------------------------------------------------------


@app.get("/storage/v1/b/{bucket}/cors")
async def get_bucket_cors(bucket: str):
    """Get the CORS configuration for a bucket.

    Args:
        bucket (str): Name of the GCS bucket.

    Returns:
        dict: Mapping with "cors", "kind", and "id" keys.

    Raises:
        GCPError: With status 404 if the bucket does not exist.
    """
    store = _store()
    data = store.get("buckets", bucket)
    if data is None:
        raise GCPError(404, "The specified bucket does not exist.")
    return {"cors": data.get("cors", []), "kind": "storage#bucket", "id": bucket}


@app.put("/storage/v1/b/{bucket}/cors")
async def set_bucket_cors(bucket: str, request: Request):
    """Replace the CORS configuration for a bucket.

    Args:
        bucket (str): Name of the GCS bucket.
        request (Request): FastAPI request containing the JSON body with the new CORS rules.

    Returns:
        dict: Mapping with updated "cors", "kind", and "id" keys.

    Raises:
        GCPError: With status 404 if the bucket does not exist.
    """
    store = _store()
    data = store.get("buckets", bucket)
    if data is None:
        raise GCPError(404, "The specified bucket does not exist.")
    body = await request.json()
    data["cors"] = body.get("cors", [])
    from cloudbox.services.gcs.models import _now_rfc3339

    data["updated"] = _now_rfc3339()
    data["metageneration"] = str(int(data.get("metageneration", "1")) + 1)
    store.set("buckets", bucket, data)
    return {"cors": data["cors"], "kind": "storage#bucket", "id": bucket}


@app.delete("/storage/v1/b/{bucket}/cors", status_code=204)
async def delete_bucket_cors(bucket: str):
    """Clear the CORS configuration for a bucket.

    Args:
        bucket (str): Name of the GCS bucket.

    Returns:
        Response: Empty 204 response on success.

    Raises:
        GCPError: With status 404 if the bucket does not exist.
    """
    store = _store()
    data = store.get("buckets", bucket)
    if data is None:
        raise GCPError(404, "The specified bucket does not exist.")
    data["cors"] = []
    from cloudbox.services.gcs.models import _now_rfc3339

    data["updated"] = _now_rfc3339()
    data["metageneration"] = str(int(data.get("metageneration", "1")) + 1)
    store.set("buckets", bucket, data)
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Bucket retention policy
# ---------------------------------------------------------------------------


@app.get("/storage/v1/b/{bucket}/retentionPolicy")
async def get_bucket_retention(bucket: str):
    """Get the retention policy for a bucket.

    Args:
        bucket (str): Name of the GCS bucket.

    Returns:
        dict: Mapping with "retentionPolicy", "kind", and "id" keys.

    Raises:
        GCPError: With status 404 if the bucket does not exist.
    """
    store = _store()
    data = store.get("buckets", bucket)
    if data is None:
        raise GCPError(404, "The specified bucket does not exist.")
    policy = data.get("retentionPolicy") or {}
    return {"retentionPolicy": policy, "kind": "storage#bucket", "id": bucket}


@app.patch("/storage/v1/b/{bucket}/retentionPolicy")
async def set_bucket_retention(bucket: str, request: Request):
    """Set or update the retention policy for a bucket.

    Args:
        bucket (str): Name of the GCS bucket.
        request (Request): FastAPI request containing the JSON body with the retention policy.

    Returns:
        dict: Mapping with updated "retentionPolicy", "kind", and "id" keys.

    Raises:
        GCPError: With status 404 if the bucket does not exist.
    """
    from cloudbox.services.gcs.models import RetentionPolicy, _now_rfc3339

    store = _store()
    data = store.get("buckets", bucket)
    if data is None:
        raise GCPError(404, "The specified bucket does not exist.")
    body = await request.json()
    policy_in = body.get("retentionPolicy", body)
    period_s = str(policy_in.get("retentionPeriod", "0"))
    policy = RetentionPolicy(
        retentionPeriod=period_s,
        effectiveTime=_now_rfc3339(),
        isLocked=bool(policy_in.get("isLocked", False)),
    )
    data["retentionPolicy"] = policy.model_dump()
    data["updated"] = _now_rfc3339()
    data["metageneration"] = str(int(data.get("metageneration", "1")) + 1)
    store.set("buckets", bucket, data)
    return {"retentionPolicy": data["retentionPolicy"], "kind": "storage#bucket", "id": bucket}


@app.delete("/storage/v1/b/{bucket}/retentionPolicy", status_code=204)
async def delete_bucket_retention(bucket: str):
    """Remove the retention policy from a bucket (fails if the policy is locked).

    Args:
        bucket (str): Name of the GCS bucket.

    Returns:
        Response: Empty 204 response on success.

    Raises:
        GCPError: With status 404 if the bucket does not exist, or 403 if the policy is locked.
    """
    from cloudbox.services.gcs.models import _now_rfc3339

    store = _store()
    data = store.get("buckets", bucket)
    if data is None:
        raise GCPError(404, "The specified bucket does not exist.")
    if data.get("retentionPolicy", {}).get("isLocked"):
        raise GCPError(403, "Retention policy is locked and cannot be removed.")
    data["retentionPolicy"] = None
    data["updated"] = _now_rfc3339()
    data["metageneration"] = str(int(data.get("metageneration", "1")) + 1)
    store.set("buckets", bucket, data)
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
    ifGenerationMatch: str = Query(default=""),
    content_type: Annotated[str | None, Header(alias="content-type")] = None,
    x_upload_content_type: Annotated[str | None, Header(alias="x-upload-content-type")] = None,
    x_upload_content_length: Annotated[str | None, Header(alias="x-upload-content-length")] = None,
):
    """Upload a new object (media, multipart, or resumable initiation).

    Args:
        bucket (str): Name of the destination GCS bucket.
        request (Request): FastAPI request containing the object body or multipart data.
        name (str): Object name; required for media uploads.
        uploadType (str): One of "media", "multipart", or "resumable".
        ifGenerationMatch (str): Precondition: required generation number; "0" means must not exist.
        content_type (str | None): MIME type from the Content-Type header.
        x_upload_content_type (str | None): MIME type hint for resumable uploads.
        x_upload_content_length (str | None): Total byte length hint for resumable uploads.

    Returns:
        dict | Response: Object metadata dict for media/multipart, or a 200 Response with a
            Location header for resumable initiation.

    Raises:
        GCPError: With status 400 if required parameters are missing, or 404 if the bucket
            does not exist.
    """
    store = _store()
    if not store.exists("buckets", bucket):
        raise GCPError(404, "The specified bucket does not exist.")

    ct = content_type or "application/octet-stream"

    if uploadType == "resumable":
        return await _initiate_resumable(
            request,
            store,
            bucket,
            name,
            x_upload_content_type or ct,
            int(x_upload_content_length) if x_upload_content_length else None,
            if_generation_match=ifGenerationMatch or None,
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

    if ifGenerationMatch:
        _check_preconditions(
            store.get("objects", f"{bucket}/{obj_name}"),
            if_generation_match=ifGenerationMatch,
        )
    return _store_object(store, bucket, obj_name, body_bytes, ct)


async def _initiate_resumable(
    request: Request,
    store,
    bucket: str,
    name: str,
    content_type: str,
    total_size: int | None,
    if_generation_match: str | None = None,
) -> Response:
    """Initiate a resumable upload session; returns 200 with Location header.

    Args:
        request (Request): FastAPI request containing the JSON body with optional object metadata.
        store (NamespacedStore): GCS store instance used to persist session state.
        bucket (str): Name of the destination GCS bucket.
        name (str): Object name; may be overridden by the JSON body.
        content_type (str): MIME type for the object.
        total_size (int | None): Total expected upload size in bytes, or None if unknown.
        if_generation_match (str | None): Precondition generation value to enforce on finalise.

    Returns:
        Response: 200 response with a Location header pointing to the resumable session URL.

    Raises:
        GCPError: With status 400 if the object name cannot be determined.
    """
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
        "if_generation_match": if_generation_match,
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
    """Upload data (or a chunk) to an active resumable session.

    Args:
        bucket (str): Name of the destination GCS bucket.
        request (Request): FastAPI request containing the chunk body bytes.
        upload_id (str): Resumable session identifier returned by the initiation request.
        content_range (str | None): HTTP Content-Range header describing this chunk's range.

    Returns:
        dict | Response: Object metadata dict when the upload is complete, or a 308 Response
            with a Range header indicating how many bytes have been received so far.

    Raises:
        GCPError: With status 404 if the upload session is not found.
    """
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
                total_size = int(content_range[len("bytes */") :])
            except ValueError:
                pass
        elif content_range.startswith("bytes "):
            rest = content_range[len("bytes ") :]
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
        gen_match = session.get("if_generation_match")
        if gen_match:
            _check_preconditions(
                store.get("objects", f"{bucket}/{session['name']}"),
                if_generation_match=gen_match,
            )
        obj = _store_object(store, bucket, session["name"], accumulated, session["content_type"])
        store.delete("resumable_sessions", upload_id)
        store.delete("resumable_bodies", upload_id)
        return obj

    # More chunks expected
    return Response(
        status_code=308,
        headers={"Range": f"bytes=0-{len(accumulated) - 1}"},
    )


def _parse_multipart(raw: bytes, content_type: str) -> tuple[str, bytes, str]:
    """Very minimal multipart/related parser for GCS multipart uploads.

    Args:
        raw (bytes): Raw request body bytes containing the multipart/related payload.
        content_type (str): Value of the Content-Type header, used to extract the boundary.

    Returns:
        tuple[str, bytes, str]: A 3-tuple of (object_name, body_bytes, body_content_type).
            object_name is empty if not found in the metadata part.
    """
    # Extract boundary from content-type header
    boundary = None
    for part in content_type.split(";"):
        p = part.strip()
        if p.startswith("boundary="):
            boundary = p[len("boundary=") :].strip('"')
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


def _retention_expiry(bucket_data: dict, time_created: str) -> str:
    """Compute retentionExpirationTime for an object given the bucket's policy.

    Args:
        bucket_data (dict): Bucket metadata dict, which may contain a "retentionPolicy" key.
        time_created (str): RFC 3339 timestamp of when the object was created.

    Returns:
        str: RFC 3339 expiry timestamp, or an empty string if no retention policy is set.
    """
    from datetime import datetime, timedelta

    policy = bucket_data.get("retentionPolicy") if bucket_data else None
    if not policy:
        return ""
    try:
        period_s = int(policy.get("retentionPeriod", 0))
    except (TypeError, ValueError):
        return ""
    if period_s <= 0:
        return ""
    created = datetime.fromisoformat(time_created.replace("Z", "+00:00"))
    expiry = created + timedelta(seconds=period_s)
    return expiry.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _store_object(store, bucket: str, name: str, body: bytes, content_type: str) -> dict:
    """Persist an object's body and metadata, then fire OBJECT_FINALIZE notifications.

    Args:
        store (NamespacedStore): GCS store instance.
        bucket (str): Name of the destination GCS bucket.
        name (str): Object name within the bucket.
        body (bytes): Raw object body bytes to store.
        content_type (str): MIME type of the object.

    Returns:
        dict: ObjectModel metadata dict for the newly stored object.
    """
    from cloudbox.services.gcs.models import _now_rfc3339

    key = f"{bucket}/{name}"
    md5 = base64.b64encode(hashlib.md5(body).digest()).decode()
    crc32c_val = _crc32c_b64(body)
    size = str(len(body))

    existing = store.get("objects", key)
    generation = str(int(existing["generation"]) + 1) if existing else "1"
    time_created = existing["timeCreated"] if existing else _now_rfc3339()

    bucket_data = store.get("buckets", bucket)
    retention_expiry = _retention_expiry(bucket_data, time_created)

    obj = ObjectModel(
        name=name,
        bucket=bucket,
        generation=generation,
        contentType=content_type,
        size=size,
        md5Hash=md5,
        crc32c=crc32c_val,
        etag=md5,
        timeCreated=time_created,
        updated=_now_rfc3339(),
        retentionExpirationTime=retention_expiry,
    )
    result = obj.model_dump()
    store.set("objects", key, result)
    store.set("bodies", key, body)
    _fire_notifications(store, bucket, "OBJECT_FINALIZE", result)
    return result


def _crc32c_b64(data: bytes) -> str:
    """Compute the CRC32c checksum of data and return it base64-encoded.

    Args:
        data (bytes): Input bytes to checksum.

    Returns:
        str: Base64-encoded 4-byte CRC32c checksum of the input.
    """
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
    """Return True if the object satisfies all conditions in the lifecycle rule.

    Args:
        obj (dict): Object metadata dict to evaluate against the condition.
        condition (LifecycleCondition): Lifecycle condition containing age, date, and class filters.

    Returns:
        bool: True if all non-None condition fields are satisfied by the object.
    """
    from datetime import datetime

    now = datetime.now(UTC)

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
    """Apply the bucket's lifecycle rules to all its objects (lazy enforcement).

    Args:
        store (NamespacedStore): GCS store instance.
        bucket (str): Name of the GCS bucket whose lifecycle rules should be evaluated.
    """
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
    """List objects in a bucket with optional prefix and delimiter filtering.

    Args:
        bucket (str): Name of the GCS bucket to list.
        prefix (str): Only return objects whose names begin with this prefix.
        delimiter (str): Collapse object names at this delimiter into "prefixes" entries.
        maxResults (int): Maximum number of objects to return per page.
        pageToken (str): Opaque token from a previous response to retrieve the next page.

    Returns:
        dict: ObjectListResponse serialised as a dict, including items and prefixes.

    Raises:
        GCPError: With status 404 if the bucket does not exist.
    """
    store = _store()
    if not store.exists("buckets", bucket):
        raise GCPError(404, "The specified bucket does not exist.")

    _apply_lifecycle(store, bucket)

    all_keys = store.keys("objects")
    bucket_prefix = f"{bucket}/"
    names = [k[len(bucket_prefix) :] for k in all_keys if k.startswith(bucket_prefix)]

    if prefix:
        names = [n for n in names if n.startswith(prefix)]

    prefixes: set[str] = set()
    if delimiter:
        filtered = []
        for n in names:
            after_prefix = n[len(prefix) :]
            idx = after_prefix.find(delimiter)
            if idx >= 0:
                prefixes.add(prefix + after_prefix[: idx + len(delimiter)])
            else:
                filtered.append(n)
        names = filtered

    names.sort()

    # Simple page token: offset as int string
    offset = int(pageToken) if pageToken else 0
    page = names[offset : offset + maxResults]
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
async def get_object_metadata(
    bucket: str,
    object_name: str,
    alt: str = Query(default=""),
    ifGenerationMatch: str = Query(default=""),
    ifMetagenerationMatch: str = Query(default=""),
    range: Annotated[str | None, Header(alias="range")] = None,
    if_match: Annotated[str | None, Header(alias="if-match")] = None,
    if_none_match: Annotated[str | None, Header(alias="if-none-match")] = None,
):
    """Get object metadata or download object body (when alt=media).

    Args:
        bucket (str): Name of the GCS bucket.
        object_name (str): Name of the GCS object.
        alt (str): When set to "media", streams the object body instead of returning metadata.
        ifGenerationMatch (str): Precondition: required generation number.
        ifMetagenerationMatch (str): Precondition: required metageneration number.
        range (str | None): HTTP Range header for partial content requests.
        if_match (str | None): HTTP If-Match header for etag preconditions.
        if_none_match (str | None): HTTP If-None-Match header for etag preconditions.

    Returns:
        dict | Response: Object metadata dict, or a 200/206 Response with the object body.

    Raises:
        GCPError: With status 404 if the object does not exist, or 412/304 on precondition failure.
    """
    store = _store()
    key = f"{bucket}/{object_name}"
    data = store.get("objects", key)
    if data is None:
        raise GCPError(404, f"No such object: {bucket}/{object_name}")

    _check_preconditions(
        data,
        if_match=if_match,
        if_none_match=if_none_match,
        if_generation_match=ifGenerationMatch or None,
        if_metageneration_match=ifMetagenerationMatch or None,
    )

    if alt == "media":
        body = store.get("bodies", key) or b""
        ct = data.get("contentType", "application/octet-stream")
        return _range_response(body, ct, range)

    return data


@app.get("/download/storage/v1/b/{bucket}/o/{object_name:path}")
async def download_object(
    bucket: str,
    object_name: str,
    range: Annotated[str | None, Header(alias="range")] = None,
):
    """Download an object's body via the /download/storage path.

    Args:
        bucket (str): Name of the GCS bucket.
        object_name (str): Name of the GCS object to download.
        range (str | None): HTTP Range header for partial content requests.

    Returns:
        Response: A 200 or 206 Response containing the object body bytes.

    Raises:
        GCPError: With status 404 if the object does not exist.
    """
    store = _store()
    key = f"{bucket}/{object_name}"
    data = store.get("objects", key)
    if data is None:
        raise GCPError(404, f"No such object: {bucket}/{object_name}")
    body = store.get("bodies", key) or b""
    ct = data.get("contentType", "application/octet-stream")
    return _range_response(body, ct, range)


@app.patch("/storage/v1/b/{bucket}/o/{object_name:path}")
async def update_object_metadata(
    bucket: str,
    object_name: str,
    request: Request,
    ifGenerationMatch: str = Query(default=""),
    ifMetagenerationMatch: str = Query(default=""),
    if_match: Annotated[str | None, Header(alias="if-match")] = None,
    if_none_match: Annotated[str | None, Header(alias="if-none-match")] = None,
):
    """Update mutable metadata fields on an existing GCS object.

    Args:
        bucket (str): Name of the GCS bucket.
        object_name (str): Name of the GCS object to update.
        request (Request): FastAPI request containing the JSON body with fields to patch.
        ifGenerationMatch (str): Precondition: required generation number.
        ifMetagenerationMatch (str): Precondition: required metageneration number.
        if_match (str | None): HTTP If-Match header for etag preconditions.
        if_none_match (str | None): HTTP If-None-Match header for etag preconditions.

    Returns:
        dict: Updated object metadata dict.

    Raises:
        GCPError: With status 404 if the object does not exist, or 412/304 on precondition failure.
    """
    store = _store()
    key = f"{bucket}/{object_name}"
    data = store.get("objects", key)
    if data is None:
        raise GCPError(404, f"No such object: {bucket}/{object_name}")
    _check_preconditions(
        data,
        if_match=if_match,
        if_none_match=if_none_match,
        if_generation_match=ifGenerationMatch or None,
        if_metageneration_match=ifMetagenerationMatch or None,
    )
    body = await request.json()
    # Merge allowed mutable fields
    for field in (
        "contentType",
        "metadata",
        "contentDisposition",
        "cacheControl",
        "contentEncoding",
    ):
        if field in body:
            data[field] = body[field]
    from cloudbox.services.gcs.models import _now_rfc3339

    data["updated"] = _now_rfc3339()
    data["metageneration"] = str(int(data.get("metageneration", "1")) + 1)
    store.set("objects", key, data)
    _fire_notifications(store, bucket, "OBJECT_METADATA_UPDATE", data)
    return data


@app.delete("/storage/v1/b/{bucket}/o/{object_name:path}", status_code=204)
async def delete_object(
    bucket: str,
    object_name: str,
    ifGenerationMatch: str = Query(default=""),
    ifMetagenerationMatch: str = Query(default=""),
    if_match: Annotated[str | None, Header(alias="if-match")] = None,
    if_none_match: Annotated[str | None, Header(alias="if-none-match")] = None,
):
    """Delete a GCS object (blocked if within a retention period).

    Args:
        bucket (str): Name of the GCS bucket.
        object_name (str): Name of the GCS object to delete.
        ifGenerationMatch (str): Precondition: required generation number.
        ifMetagenerationMatch (str): Precondition: required metageneration number.
        if_match (str | None): HTTP If-Match header for etag preconditions.
        if_none_match (str | None): HTTP If-None-Match header for etag preconditions.

    Returns:
        Response: Empty 204 response on success.

    Raises:
        GCPError: With status 404 if the object does not exist, 412/304 on precondition failure,
            or 403 if the object is within a retention period.
    """
    store = _store()
    key = f"{bucket}/{object_name}"
    obj_data = store.get("objects", key)
    if obj_data is None:
        raise GCPError(404, f"No such object: {bucket}/{object_name}")
    _check_preconditions(
        obj_data,
        if_match=if_match,
        if_none_match=if_none_match,
        if_generation_match=ifGenerationMatch or None,
        if_metageneration_match=ifMetagenerationMatch or None,
    )
    expiry = obj_data.get("retentionExpirationTime", "")
    if expiry:
        from datetime import datetime

        exp_dt = datetime.fromisoformat(expiry.replace("Z", "+00:00"))
        if datetime.now(UTC) < exp_dt:
            raise GCPError(
                403,
                f"Object '{object_name}' is subject to a retention policy"
                f" and cannot be deleted until {expiry}.",
            )
    store.delete("objects", key)
    store.delete("bodies", key)
    _fire_notifications(store, bucket, "OBJECT_DELETE", obj_data)
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Copy
# ---------------------------------------------------------------------------


@app.post(
    "/storage/v1/b/{src_bucket}/o/{src_object:path}/copyTo/b/{dst_bucket}/o/{dst_object:path}"
)
async def copy_object(src_bucket: str, src_object: str, dst_bucket: str, dst_object: str):
    """Copy a GCS object to a new location (server-side copy).

    Args:
        src_bucket (str): Source bucket name.
        src_object (str): Source object name.
        dst_bucket (str): Destination bucket name.
        dst_object (str): Destination object name.

    Returns:
        dict: Metadata dict for the newly created destination object.

    Raises:
        GCPError: With status 404 if the source object or destination bucket does not exist.
    """
    store = _store()
    src_key = f"{src_bucket}/{src_object}"
    data = store.get("objects", src_key)
    if data is None:
        raise GCPError(404, f"No such object: {src_bucket}/{src_object}")
    if not store.exists("buckets", dst_bucket):
        raise GCPError(404, "The specified bucket does not exist.")
    body = store.get("bodies", src_key) or b""
    return _store_object(
        store, dst_bucket, dst_object, body, data.get("contentType", "application/octet-stream")
    )


# ---------------------------------------------------------------------------
# Compose
# ---------------------------------------------------------------------------


@app.post("/storage/v1/b/{bucket}/o/{object_name:path}/compose")
async def compose_object(bucket: str, object_name: str, request: Request):
    """Concatenate up to 32 source objects into a single destination object.

    Args:
        bucket (str): Name of the GCS bucket that contains both sources and the destination.
        object_name (str): Name of the composed destination object.
        request (Request): FastAPI request containing the JSON body with sourceObjects list.

    Returns:
        dict: Metadata dict for the newly composed destination object.

    Raises:
        GCPError: With status 400 if sourceObjects is empty or exceeds 32 entries, or 404/412
            if any source object is missing or fails a generation precondition.
    """
    store = _store()
    if not store.exists("buckets", bucket):
        raise GCPError(404, "The specified bucket does not exist.")

    body = await request.json()
    source_objects = body.get("sourceObjects", [])
    if not source_objects:
        raise GCPError(400, "sourceObjects must contain at least one entry")
    if len(source_objects) > 32:
        raise GCPError(400, "sourceObjects may contain at most 32 entries")

    destination_meta = body.get("destination", {})
    content_type = destination_meta.get("contentType", "application/octet-stream")

    composed = b""
    for src in source_objects:
        src_name = src.get("name", "")
        if not src_name:
            raise GCPError(400, "Each sourceObject must have a name")
        src_key = f"{bucket}/{src_name}"
        src_data = store.get("objects", src_key)
        if src_data is None:
            raise GCPError(404, f"No such object: {bucket}/{src_name}")
        # Honour optional generationMatch
        gen_match = src.get("objectPreconditions", {}).get("ifGenerationMatch")
        if gen_match is not None and str(src_data.get("generation")) != str(gen_match):
            raise GCPError(412, f"Precondition failed for source object {src_name}")
        composed += store.get("bodies", src_key) or b""

    if not content_type or content_type == "application/octet-stream":
        first_key = f"{bucket}/{source_objects[0]['name']}"
        first_meta = store.get("objects", first_key)
        if first_meta:
            content_type = first_meta.get("contentType", content_type)

    return _store_object(store, bucket, object_name, composed, content_type)


# ---------------------------------------------------------------------------
# Rewrite
# ---------------------------------------------------------------------------


@app.post(
    "/storage/v1/b/{src_bucket}/o/{src_object:path}/rewriteTo/b/{dst_bucket}/o/{dst_object:path}"
)
async def rewrite_object(
    src_bucket: str,
    src_object: str,
    dst_bucket: str,
    dst_object: str,
    request: Request,
):
    """Rewrite (copy + optional metadata update) an object.

    Completes in a single shot — rewriteToken is not used for resumption.
    The response mirrors the real GCS rewrite response so the SDK's
    polling loop terminates immediately on the first call.

    Args:
        src_bucket (str): Source bucket name.
        src_object (str): Source object name.
        dst_bucket (str): Destination bucket name.
        dst_object (str): Destination object name.
        request (Request): FastAPI request containing the JSON body with optional metadata overrides.

    Returns:
        dict: GCS rewrite response with "done", "resource", and byte count fields.

    Raises:
        GCPError: With status 404 if the source object or destination bucket does not exist.
    """
    store = _store()
    src_key = f"{src_bucket}/{src_object}"
    src_data = store.get("objects", src_key)
    if src_data is None:
        raise GCPError(404, f"No such object: {src_bucket}/{src_object}")
    if not store.exists("buckets", dst_bucket):
        raise GCPError(404, "The destination bucket does not exist.")

    body = {}
    try:
        body = await request.json()
    except Exception:
        pass

    body_bytes = store.get("bodies", src_key) or b""
    content_type = body.get("contentType") or src_data.get(
        "contentType", "application/octet-stream"
    )
    storage_class = body.get("storageClass") or src_data.get("storageClass", "STANDARD")

    dst = _store_object(store, dst_bucket, dst_object, body_bytes, content_type)
    if storage_class != "STANDARD":
        dst["storageClass"] = storage_class
        store.set("objects", f"{dst_bucket}/{dst_object}", dst)

    return {
        "kind": "storage#rewriteResponse",
        "totalBytesRewritten": str(len(body_bytes)),
        "objectSize": str(len(body_bytes)),
        "done": True,
        "resource": dst,
    }


# ---------------------------------------------------------------------------
# Notification configurations
# ---------------------------------------------------------------------------


def _next_notification_id(store, bucket: str) -> str:
    """Return the next sequential notification config ID for a bucket.

    Args:
        store (NamespacedStore): GCS store instance.
        bucket (str): Name of the GCS bucket.

    Returns:
        str: Next integer ID as a string (e.g. "1", "2", ...).
    """
    prefix = f"{bucket}/"
    existing_ids = [
        int(k[len(prefix) :])
        for k in store.keys("notifications")
        if k.startswith(prefix) and k[len(prefix) :].isdigit()
    ]
    return str(max(existing_ids, default=0) + 1)


@app.post("/storage/v1/b/{bucket}/notificationConfigs", status_code=200)
async def create_notification(bucket: str, request: Request):
    """Create a bucket notification configuration.

    Args:
        bucket (str): Name of the GCS bucket to attach the notification to.
        request (Request): FastAPI request containing the JSON body with notification config fields.

    Returns:
        dict: Newly created NotificationConfig serialised as a dict.

    Raises:
        GCPError: With status 404 if the bucket does not exist.
    """
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
    """List all notification configurations for a bucket.

    Args:
        bucket (str): Name of the GCS bucket.

    Returns:
        dict: NotificationListResponse serialised as a dict.

    Raises:
        GCPError: With status 404 if the bucket does not exist.
    """
    store = _store()
    if not store.exists("buckets", bucket):
        raise GCPError(404, "The specified bucket does not exist.")
    prefix = f"{bucket}/"
    items = [
        NotificationConfig(**v)
        for k, v in [
            (k, store.get("notifications", k))
            for k in store.keys("notifications")
            if k.startswith(prefix)
        ]
        if v is not None
    ]
    return NotificationListResponse(items=items).model_dump()


@app.get("/storage/v1/b/{bucket}/notificationConfigs/{notif_id}")
async def get_notification(bucket: str, notif_id: str):
    """Get a bucket notification configuration by ID.

    Args:
        bucket (str): Name of the GCS bucket.
        notif_id (str): Notification configuration ID.

    Returns:
        dict: NotificationConfig metadata dict.

    Raises:
        GCPError: With status 404 if the notification config does not exist.
    """
    store = _store()
    data = store.get("notifications", f"{bucket}/{notif_id}")
    if data is None:
        raise GCPError(404, f"Notification config {notif_id} not found on bucket {bucket}.")
    return data


@app.delete("/storage/v1/b/{bucket}/notificationConfigs/{notif_id}", status_code=204)
async def delete_notification(bucket: str, notif_id: str):
    """Delete a bucket notification configuration.

    Args:
        bucket (str): Name of the GCS bucket.
        notif_id (str): Notification configuration ID to delete.

    Returns:
        Response: Empty 204 response on success.

    Raises:
        GCPError: With status 404 if the notification config does not exist.
    """
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
    No-ops silently if the topic or subscription does not exist yet.

    Args:
        store (NamespacedStore): GCS store instance used to look up notification configs.
        bucket (str): Name of the GCS bucket that originated the event.
        event_type (str): GCS event type string (e.g. "OBJECT_FINALIZE", "OBJECT_DELETE").
        obj_data (dict): Object metadata dict describing the object involved in the event.
    """
    import base64
    import json
    import uuid
    from datetime import datetime

    from cloudbox.services.pubsub import store as ps_store

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
    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

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
