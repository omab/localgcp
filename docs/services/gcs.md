# Cloud Storage (GCS)

Cloudbox emulates the GCS JSON API v1. The `google-cloud-storage` Python SDK works against it
without modification — point it at `http://localhost:4443` and supply any credentials.

## Connection

**Port:** `4443` (override with `CLOUDBOX_GCS_PORT`)

```python
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage

client = storage.Client(
    project="local-project",
    credentials=AnonymousCredentials(),
    client_options={"api_endpoint": "http://localhost:4443"},
)
```

Or with the `gsutillocal` CLI shim (installed after `uv sync`):

```bash
gsutillocal ls gs://my-bucket
gsutillocal cp local.txt gs://my-bucket/path/to/object.txt
```

---

## Buckets

### Create bucket

```
POST /storage/v1/b
```

```json
{ "name": "my-bucket" }
```

Returns the bucket metadata. Fails with `409` if the bucket already exists.

Optional fields accepted in the body: `storageClass`, `location`, `labels`, `cors`,
`lifecycle`, `retentionPolicy`.

### Get bucket

```
GET /storage/v1/b/{bucket}
```

Returns bucket metadata. `404` if not found.

### List buckets

```
GET /storage/v1/b?project={project}
```

Returns `{ "kind": "storage#buckets", "items": [...] }`.

### Patch bucket

```
PATCH /storage/v1/b/{bucket}
```

Updates mutable fields: `lifecycle`, `labels`, `storageClass`, `location`, `cors`,
`retentionPolicy`. Increments `metageneration`.

### Delete bucket

```
DELETE /storage/v1/b/{bucket}
```

`204` on success. `409` if the bucket is not empty. `404` if not found.

---

## Objects — uploads

### Simple (media) upload

```
POST /upload/storage/v1/b/{bucket}/o?uploadType=media&name={object}
Content-Type: image/png

<raw bytes>
```

Returns object metadata. Object name is required as a query parameter.

### Multipart upload

```
POST /upload/storage/v1/b/{bucket}/o?uploadType=multipart
Content-Type: multipart/related; boundary=foo

--foo
Content-Type: application/json

{"name": "my-object", "contentType": "image/png"}
--foo
Content-Type: image/png

<raw bytes>
--foo--
```

The metadata part (first section) may contain `name` and `contentType`. The object name
may also be supplied as a `name` query parameter.

### Resumable upload

**Initiate:**

```
POST /upload/storage/v1/b/{bucket}/o?uploadType=resumable&name={object}
X-Upload-Content-Type: video/mp4
X-Upload-Content-Length: 10485760
```

Returns `200` with a `Location` header pointing to the upload session URL
(`?uploadType=resumable&upload_id=<id>`).

**Upload chunk (PUT to session URL):**

```
PUT /upload/storage/v1/b/{bucket}/o?uploadType=resumable&upload_id={id}
Content-Range: bytes 0-999999/10485760

<chunk bytes>
```

Returns `308 Resume Incomplete` with a `Range` header while more data is expected,
or object metadata when the upload is complete.

**Query status:**

Send a PUT with an empty body and `Content-Range: bytes */{total}` to get a `308`
response that includes the `Range` header showing how many bytes have been received.

### Upload preconditions

All upload types accept `ifGenerationMatch` as a query parameter. Pass `"0"` to
require that the object does not already exist.

---

## Objects — download

### Download via metadata endpoint

```
GET /storage/v1/b/{bucket}/o/{object}?alt=media
```

Returns the object body with its stored `Content-Type`.

### Download via dedicated path

```
GET /download/storage/v1/b/{bucket}/o/{object}
```

Equivalent to `alt=media` above. Both paths support byte-range requests.

### Byte-range download

Include an HTTP `Range` header:

```
Range: bytes=0-499      # first 500 bytes
Range: bytes=500-       # from byte 500 to end
Range: bytes=-100       # last 100 bytes
```

Returns `206 Partial Content` with a `Content-Range` header. Returns `416` if the range
is unsatisfiable.

---

## Objects — metadata

### Get object metadata

```
GET /storage/v1/b/{bucket}/o/{object}
```

Returns the full object metadata JSON (without `alt=media`).

### Patch object metadata

```
PATCH /storage/v1/b/{bucket}/o/{object}
```

Updates mutable fields: `contentType`, `metadata`, `contentDisposition`, `cacheControl`,
`contentEncoding`, `temporaryHold`, `eventBasedHold`. Increments `metageneration`. Fires
`OBJECT_METADATA_UPDATE` notification.

### List objects

```
GET /storage/v1/b/{bucket}/o
```

Query parameters:

| Parameter | Default | Description |
|---|---|---|
| `prefix` | `""` | Filter to objects whose name begins with this value |
| `delimiter` | `""` | Collapse names at this delimiter into `prefixes` entries |
| `maxResults` | `1000` | Maximum objects per page |
| `pageToken` | `""` | Opaque token from a previous response for pagination |

Returns:

```json
{
  "kind": "storage#objects",
  "items": [...],
  "prefixes": ["folder/"],
  "nextPageToken": "1000"
}
```

### Delete object

```
DELETE /storage/v1/b/{bucket}/o/{object}
```

`204` on success. `403` if the object is within a retention period. Fires `OBJECT_DELETE`
notification.

---

## Object operations

### Copy object

```
POST /storage/v1/b/{src_bucket}/o/{src_object}/copyTo/b/{dst_bucket}/o/{dst_object}
```

Server-side copy. Returns the destination object metadata. Both the source object and
destination bucket must exist.

### Compose objects

```
POST /storage/v1/b/{bucket}/o/{destination}/compose
```

```json
{
  "sourceObjects": [
    { "name": "part-1" },
    { "name": "part-2", "objectPreconditions": { "ifGenerationMatch": "3" } }
  ],
  "destination": { "contentType": "application/octet-stream" }
}
```

Concatenates up to 32 source objects in order. Optional `ifGenerationMatch` per source
enforces a generation precondition. Returns destination object metadata.

### Rewrite object

```
POST /storage/v1/b/{src_bucket}/o/{src_object}/rewriteTo/b/{dst_bucket}/o/{dst_object}
```

Copies an object with optional metadata overrides (`contentType`, `storageClass`).
Completes in a single request — no `rewriteToken` polling required.

Response shape:

```json
{
  "kind": "storage#rewriteResponse",
  "done": true,
  "totalBytesRewritten": "1024",
  "objectSize": "1024",
  "resource": { ... }
}
```

---

## Preconditions

All object read, write, and delete operations accept conditional request headers and
query parameters:

| Mechanism | Header / Parameter | Description |
|---|---|---|
| ETag match | `If-Match: <etag>` | Fail unless etag matches |
| ETag non-match | `If-None-Match: <etag>` | Fail (304) if etag matches |
| Generation | `ifGenerationMatch=<n>` | Fail unless generation equals `n`; `"0"` requires absence |
| Metageneration | `ifMetagenerationMatch=<n>` | Fail unless metageneration equals `n` |

Precondition failures return `412 Precondition Failed` (or `304 Not Modified` for
`If-None-Match`).

---

## Checksums

Every stored object receives automatically computed checksums:

- **`md5Hash`** — MD5 digest of the object body, base64-encoded.
- **`crc32c`** — CRC32c checksum, base64-encoded.
- **`etag`** — Same value as `md5Hash`.

These are returned in object metadata and can be used for integrity verification.

---

## CORS configuration

### Get CORS

```
GET /storage/v1/b/{bucket}/cors
```

### Set CORS

```
PUT /storage/v1/b/{bucket}/cors
```

```json
{
  "cors": [
    {
      "origin": ["https://example.com"],
      "method": ["GET", "PUT"],
      "responseHeader": ["Content-Type"],
      "maxAgeSeconds": 3600
    }
  ]
}
```

Replaces the entire CORS configuration for the bucket.

### Delete CORS

```
DELETE /storage/v1/b/{bucket}/cors
```

Clears all CORS rules. Returns `204`.

---

## Retention policies

### Get retention policy

```
GET /storage/v1/b/{bucket}/retentionPolicy
```

### Set / update retention policy

```
PATCH /storage/v1/b/{bucket}/retentionPolicy
```

```json
{ "retentionPolicy": { "retentionPeriod": "86400" } }
```

`retentionPeriod` is in seconds. Once set, objects in the bucket cannot be deleted until
their `retentionExpirationTime` has passed. The expiry is computed at upload time and stored
on the object.

### Delete retention policy

```
DELETE /storage/v1/b/{bucket}/retentionPolicy
```

Fails with `403` if the policy has been locked (`isLocked: true`).

---

## Lifecycle rules

Set lifecycle rules when creating or patching a bucket:

```json
{
  "lifecycle": {
    "rule": [
      {
        "action": { "type": "Delete" },
        "condition": { "age": 30 }
      },
      {
        "action": { "type": "SetStorageClass", "storageClass": "COLDLINE" },
        "condition": { "createdBefore": "2024-01-01", "matchesStorageClass": ["STANDARD"] }
      }
    ]
  }
}
```

Lifecycle rules are evaluated lazily on each `list` request. Supported conditions:

| Condition | Description |
|---|---|
| `age` | Object age in days since `timeCreated` |
| `createdBefore` | RFC 3339 date; objects created before this date match |
| `matchesStorageClass` | List of storage class names to match |

Supported actions: `Delete`, `SetStorageClass`.

---

## Pub/Sub notifications

Buckets can be configured to publish messages to a Pub/Sub topic when object events occur.
The Pub/Sub topic must exist in the Cloudbox Pub/Sub emulator.

### Create notification config

```
POST /storage/v1/b/{bucket}/notificationConfigs
```

```json
{
  "topic": "projects/local-project/topics/my-topic",
  "payload_format": "JSON_API_V1",
  "event_types": ["OBJECT_FINALIZE", "OBJECT_DELETE"],
  "object_name_prefix": "uploads/",
  "custom_attributes": { "env": "local" }
}
```

| Field | Description |
|---|---|
| `topic` | Full Pub/Sub topic resource name |
| `payload_format` | `"JSON_API_V1"` (full object metadata) or `"NONE"` (empty payload) |
| `event_types` | List of event types to fire on; omit to fire on all events |
| `object_name_prefix` | Only fire for objects with this name prefix |
| `custom_attributes` | Key-value pairs added to message attributes |

Event types: `OBJECT_FINALIZE`, `OBJECT_DELETE`, `OBJECT_METADATA_UPDATE`, `OBJECT_ARCHIVE`.

### List notification configs

```
GET /storage/v1/b/{bucket}/notificationConfigs
```

### Get notification config

```
GET /storage/v1/b/{bucket}/notificationConfigs/{id}
```

### Delete notification config

```
DELETE /storage/v1/b/{bucket}/notificationConfigs/{id}
```

---

## Object metadata fields

| Field | Type | Description |
|---|---|---|
| `name` | string | Object name within the bucket |
| `bucket` | string | Bucket name |
| `generation` | string | Integer generation number (increments on each overwrite) |
| `metageneration` | string | Integer metadata generation (increments on metadata update) |
| `contentType` | string | MIME type |
| `size` | string | Object size in bytes |
| `md5Hash` | string | Base64-encoded MD5 of the body |
| `crc32c` | string | Base64-encoded CRC32c of the body |
| `etag` | string | Same as `md5Hash` |
| `timeCreated` | string | RFC 3339 creation timestamp (preserved across overwrites) |
| `updated` | string | RFC 3339 last-modified timestamp |
| `storageClass` | string | Storage class (default: `STANDARD`) |
| `retentionExpirationTime` | string | RFC 3339 expiry from bucket retention policy, if set |
| `metadata` | object | Arbitrary user-defined key-value pairs |
| `contentDisposition` | string | `Content-Disposition` header value |
| `cacheControl` | string | `Cache-Control` header value |
| `contentEncoding` | string | `Content-Encoding` header value |
| `temporaryHold` | bool | When `true`, blocks deletion until cleared |
| `eventBasedHold` | bool | When `true`, blocks deletion until cleared |

---

## Known limitations

The following GCS features are not emulated:

| Feature | Notes |
|---|---|
| Object versioning | Only the latest generation of each object is retained |
| ACLs (`objectAccessControls`, `bucketAccessControls`) | No access control enforcement |
| IAM (`getIamPolicy` / `setIamPolicy`) | Endpoints not implemented |
| Signed URLs (V2 / V4) | No signed URL generation or validation |
| XML API (S3-compatible) | Only the JSON API is supported |
| Customer-managed encryption keys (CMEK) | Objects are stored in plaintext |
| Object holds — release-on-event semantics | Holds block deletion; the emulator does not model "event" release triggers — clear holds explicitly via PATCH |
| Uniform bucket-level access | Not enforced |

---

## Examples

```bash
# Run all GCS examples (requires Cloudbox running on port 4443)
uv run python examples/gcs/upload_download.py
uv run python examples/gcs/compose.py
uv run python examples/gcs/byte_range.py
uv run python examples/gcs/cors.py
uv run python examples/gcs/retention.py
```

| Example | What it demonstrates |
|---|---|
| `upload_download.py` | Media, multipart, and resumable uploads; download; metadata patch; delete |
| `compose.py` | Composing multiple objects into one; generation preconditions |
| `byte_range.py` | Partial content requests with `Range` header; 416 on bad ranges |
| `cors.py` | Setting, reading, updating, and clearing CORS rules |
| `retention.py` | Retention policies; locked policies; delete blocked within retention window |
