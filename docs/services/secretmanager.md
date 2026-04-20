# Secret Manager

Cloudbox emulates the Secret Manager REST API (v1). The `google-cloud-secret-manager`
Python SDK works against it without modification.

## Connection

**Port:** `8090` (override with `CLOUDBOX_SECRET_MANAGER_PORT`)

```python
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import secretmanager

client = secretmanager.SecretManagerServiceClient(
    credentials=AnonymousCredentials(),
    client_options=ClientOptions(api_endpoint="http://localhost:8090"),
)
```

---

## Secrets

### Create secret

```
POST /v1/projects/{project}/secrets?secretId={id}
```

```json
{
  "replication": { "automatic": {} },
  "labels": { "env": "local" }
}
```

The `secretId` must be provided as a query parameter or in the body. Returns the secret
resource. `409` if the secret already exists.

### Get secret

```
GET /v1/projects/{project}/secrets/{secret_id}
```

Returns the secret metadata. `404` if not found.

### List secrets

```
GET /v1/projects/{project}/secrets?pageSize=25&pageToken=
```

Returns `{ "secrets": [...], "nextPageToken": "...", "totalSize": N }`.

### Update secret (patch labels)

```
PATCH /v1/projects/{project}/secrets/{secret_id}
```

```json
{ "labels": { "updated": "true" } }
```

Currently only `labels` can be updated. Returns the updated secret resource.

### Delete secret

```
DELETE /v1/projects/{project}/secrets/{secret_id}
```

Deletes the secret and all its versions. Returns `{}`.

---

## Secret versions

Secret values are stored as versions. Each `addVersion` call creates a new numbered version.
The latest version is always accessible via the special alias `"latest"`.

### Add version

```
POST /v1/projects/{project}/secrets/{secret_id}:addVersion
```

```json
{
  "payload": {
    "data": "bXktc2VjcmV0LXZhbHVl"
  }
}
```

`payload.data` must be base64-encoded. Returns the newly created version resource with
`state: ENABLED`.

Version numbers are sequential integers starting at `1`.

### Access (read) version payload

```
POST /v1/projects/{project}/secrets/{secret_id}/versions/{version_id}:access
```

`version_id` may be a version number (`"1"`, `"2"`) or the alias `"latest"`.

Returns:

```json
{
  "name": "projects/local-project/secrets/my-secret/versions/1",
  "payload": { "data": "bXktc2VjcmV0LXZhbHVl" }
}
```

`403` if the version is disabled or destroyed.

### Get version metadata

```
GET /v1/projects/{project}/secrets/{secret_id}/versions/{version_id}
```

Returns version metadata without the payload. Supports `"latest"` alias.

### List versions

```
GET /v1/projects/{project}/secrets/{secret_id}/versions?pageSize=25&pageToken=&filter=
```

Optional `filter` parameter accepts `state=ENABLED`, `state=DISABLED`, or
`state=DESTROYED` to narrow results.

Returns `{ "versions": [...], "nextPageToken": "...", "totalSize": N }`.

### Disable version

```
POST /v1/projects/{project}/secrets/{secret_id}/versions/{version_id}:disable
```

Sets the version state to `DISABLED`. Disabled versions cannot be accessed.
Returns the updated version resource.

### Enable version

```
POST /v1/projects/{project}/secrets/{secret_id}/versions/{version_id}:enable
```

Sets the version state back to `ENABLED`. Returns the updated version resource.

### Destroy version

```
POST /v1/projects/{project}/secrets/{secret_id}/versions/{version_id}:destroy
```

Sets the version state to `DESTROYED` and wipes the payload permanently.
Returns the updated version resource.

---

## Version states

| State | Description |
|---|---|
| `ENABLED` | Active — payload can be accessed |
| `DISABLED` | Suspended — access returns 403 |
| `DESTROYED` | Permanently deleted — payload is gone |

---

## Secret resource fields

| Field | Type | Description |
|---|---|---|
| `name` | string | Full resource name: `projects/{project}/secrets/{id}` |
| `replication` | object | Replication policy (stored but not enforced) |
| `labels` | object | User-defined key-value labels |
| `createTime` | string | RFC 3339 creation timestamp |

## Version resource fields

| Field | Type | Description |
|---|---|---|
| `name` | string | Full resource name including version number |
| `createTime` | string | RFC 3339 creation timestamp |
| `state` | string | `ENABLED`, `DISABLED`, or `DESTROYED` |

---

## Known limitations

| Feature | Notes |
|---|---|
| CMEK (customer-managed encryption) | Payloads are stored as plaintext; `kmsKeyName` field accepted but not enforced |
| Rotation notifications | Pub/Sub rotation notifications not published on `addVersion` |
| IAM (`getIamPolicy` / `setIamPolicy`) | Not implemented |
| Replication policies | `replication` field is accepted and stored but not enforced |

---

## Examples

```bash
uv run python examples/secretmanager/basic.py
```
