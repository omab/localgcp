# Cloud Firestore

Cloudbox emulates the Firestore REST API (v1). The `google-cloud-firestore` Python SDK works
against it without modification.

## Connection

**Port:** `8080` (override with `CLOUDBOX_FIRESTORE_PORT`)

```python
import os
os.environ["FIRESTORE_EMULATOR_HOST"] = "localhost:8080"

from google.cloud import firestore

db = firestore.Client(project="local-project")
```

The `FIRESTORE_EMULATOR_HOST` environment variable is the standard way to point the SDK at
any Firestore emulator. No credential setup is needed.

---

## Documents

All document operations use the path pattern:

```
projects/{project}/databases/{database}/documents/{collection}/{document}
```

The default database is `(default)`.

### Create document (auto-ID)

```
POST /v1/projects/{project}/databases/{database}/documents/{collection}
```

```json
{
  "fields": {
    "name": { "stringValue": "Alice" },
    "age":  { "integerValue": "30" }
  }
}
```

Generates a random document ID. Returns the created document.

### Create or overwrite document (known ID)

```
POST /v1/projects/{project}/databases/{database}/documents/{collection}/{doc_id}
```

Same body as above. If the document already exists it is overwritten.

### Get document

```
GET /v1/projects/{project}/databases/{database}/documents/{collection}/{doc_id}
```

Returns the document resource. `404` if not found.

### Update document (patch)

```
PATCH /v1/projects/{project}/databases/{database}/documents/{collection}/{doc_id}
?updateMask.fieldPaths=name&updateMask.fieldPaths=age
```

Merges the supplied fields into the document. Without `updateMask`, replaces all fields.
Supports `currentDocument.exists` and `currentDocument.updateTime` preconditions via query
parameters.

### Delete document

```
DELETE /v1/projects/{project}/databases/{database}/documents/{collection}/{doc_id}
```

`200` on success. Supports `currentDocument.exists` precondition.

---

## Batch operations

### Batch get

```
POST /v1/projects/{project}/databases/{database}/documents:batchGet
```

```json
{
  "documents": [
    "projects/local-project/databases/(default)/documents/users/alice",
    "projects/local-project/databases/(default)/documents/users/bob"
  ]
}
```

Returns one result per requested document:

```json
[
  { "found": { "name": "...", "fields": {...}, "readTime": "..." }, "readTime": "..." },
  { "missing": "projects/.../users/bob",                            "readTime": "..." }
]
```

### Batch write

```
POST /v1/projects/{project}/databases/{database}:batchWrite
```

```json
{
  "writes": [
    { "update": { "name": "projects/.../users/alice", "fields": { "score": { "integerValue": "99" } } } },
    { "delete": "projects/.../users/bob" }
  ]
}
```

Applies a list of mutations atomically. Supported write types: `update`, `delete`,
`transform` (field transforms). Returns `writeResults` and `status` for each write.

---

## Queries

### Run structured query

```
POST /v1/projects/{project}/databases/{database}/documents:runQuery
```

Or against a subcollection parent:

```
POST /v1/{parent_path}/documents:runQuery
```

```json
{
  "structuredQuery": {
    "from": [{ "collectionId": "users" }],
    "where": {
      "fieldFilter": {
        "field": { "fieldPath": "age" },
        "op": "GREATER_THAN_OR_EQUAL",
        "value": { "integerValue": "18" }
      }
    },
    "orderBy": [{ "field": { "fieldPath": "age" }, "direction": "ASCENDING" }],
    "limit": 10,
    "offset": 0
  }
}
```

Returns a list of `{"document": {...}, "readTime": "..."}` objects.

Supported filter operators: `EQUAL`, `NOT_EQUAL`, `LESS_THAN`, `LESS_THAN_OR_EQUAL`,
`GREATER_THAN`, `GREATER_THAN_OR_EQUAL`, `ARRAY_CONTAINS`, `IN`, `ARRAY_CONTAINS_ANY`,
`NOT_IN`.

Composite filters (`AND`, `OR`) are supported via `compositeFilter`.

`allDescendants: true` in the `from` clause enables collection group queries.

### Aggregation query

```
POST /v1/projects/{project}/databases/{database}/documents:runAggregationQuery
```

```json
{
  "structuredAggregationQuery": {
    "structuredQuery": { "from": [{ "collectionId": "orders" }] },
    "aggregations": [
      { "alias": "total_count", "count": {} },
      { "alias": "total_revenue", "sum": { "field": { "fieldPath": "amount" } } },
      { "alias": "avg_amount", "avg": { "field": { "fieldPath": "amount" } } }
    ]
  }
}
```

Supported aggregations: `count` (with optional `upTo`), `sum`, `avg`. Returns a list with
one `{"result": {"aggregateFields": {...}}, "readTime": "..."}` object.

---

## Transactions

### Begin transaction

```
POST /v1/projects/{project}/databases/{database}:beginTransaction
```

Returns `{ "transaction": "<base64-id>" }`. The transaction ID can be passed with read
operations (reads inside a transaction are not yet isolated, but the ID is accepted).

### Commit

```
POST /v1/projects/{project}/databases/{database}:commit
```

```json
{
  "transaction": "<base64-id>",
  "writes": [
    { "update": { "name": "...", "fields": {...} } }
  ]
}
```

Applies the writes and returns `writeResults` and `commitTime`.

### Rollback

```
POST /v1/projects/{project}/databases/{database}:rollback
```

```json
{ "transaction": "<base64-id>" }
```

No-op — returns `{}`. Transactions are not currently tracked between begin and rollback.

---

## Firestore value types

Fields use Firestore's typed value format:

| Type | JSON key | Example |
|---|---|---|
| String | `stringValue` | `{"stringValue": "hello"}` |
| Integer | `integerValue` | `{"integerValue": "42"}` |
| Double | `doubleValue` | `{"doubleValue": 3.14}` |
| Boolean | `booleanValue` | `{"booleanValue": true}` |
| Null | `nullValue` | `{"nullValue": "NULL_VALUE"}` |
| Bytes | `bytesValue` | `{"bytesValue": "<base64>"}` |
| Timestamp | `timestampValue` | `{"timestampValue": "2024-01-01T00:00:00Z"}` |
| Reference | `referenceValue` | `{"referenceValue": "projects/p/databases/d/documents/..."}` |
| GeoPoint | `geoPointValue` | `{"geoPointValue": {"latitude": 37.4, "longitude": -122.0}}` |
| Array | `arrayValue` | `{"arrayValue": {"values": [...]}}` |
| Map | `mapValue` | `{"mapValue": {"fields": {...}}}` |

---

## Known limitations

| Feature | Notes |
|---|---|
| Real-time listeners (`on_snapshot`) | Not implemented — long-lived SSE/WebSocket not supported |
| Transaction isolation | Reads inside a transaction are not isolated from concurrent writes |
| Composite index enforcement | Queries that would require a composite index in production succeed locally |
| Security rules | Not evaluated — all operations are permitted |
| Database management (create/delete databases) | Always uses `(default)` database |

---

## Examples

```bash
uv run python examples/firestore/crud.py
uv run python examples/firestore/queries.py
```
