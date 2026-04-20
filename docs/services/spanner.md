# Cloud Spanner

Cloudbox emulates the Cloud Spanner REST API (v1) using DuckDB as the backend. The
`google-cloud-spanner` Python SDK works against it with minimal configuration changes.

## Connection

**Port:** `9010` (override with `CLOUDBOX_SPANNER_PORT`)

```python
from google.auth.credentials import AnonymousCredentials
from google.cloud import spanner

client = spanner.Client(
    project="local-project",
    credentials=AnonymousCredentials(),
    client_options={"api_endpoint": "http://localhost:9010"},
)

instance = client.instance("my-instance")
database = instance.database("my-database")
```

---

## Instances

### Instance configs

```
GET /v1/projects/{project}/instanceConfigs
GET /v1/projects/{project}/instanceConfigs/{config}
```

Returns a list of available instance configurations. The emulator advertises a single
config: `regional-us-central1`. This is a no-op — all instances share the same DuckDB
backend.

### Create instance

```
POST /v1/projects/{project}/instances
```

```json
{
  "instanceId": "my-instance",
  "instance": {
    "config": "projects/local-project/instanceConfigs/regional-us-central1",
    "displayName": "My Instance",
    "nodeCount": 1,
    "labels": {}
  }
}
```

Returns a Long-Running Operation (LRO) that is immediately marked as `done: true`.

### Get instance

```
GET /v1/projects/{project}/instances/{instance_id}
```

### List instances

```
GET /v1/projects/{project}/instances
```

### Update instance

```
PATCH /v1/projects/{project}/instances/{instance_id}
```

Updates `displayName`, `nodeCount`, or `labels`. Returns an LRO.

### Delete instance

```
DELETE /v1/projects/{project}/instances/{instance_id}
```

Drops all databases in the instance.

---

## Databases

### Create database

```
POST /v1/projects/{project}/instances/{instance_id}/databases
```

```json
{
  "createStatement": "CREATE DATABASE `my-database`",
  "extraStatements": [
    "CREATE TABLE users (id INT64 NOT NULL, name STRING(MAX)) PRIMARY KEY (id)"
  ]
}
```

`extraStatements` are DDL statements executed after creating the database. Each statement
creates or alters a DuckDB table. Returns an LRO.

### Get database

```
GET /v1/projects/{project}/instances/{instance_id}/databases/{database_id}
```

### List databases

```
GET /v1/projects/{project}/instances/{instance_id}/databases
```

### Update database DDL

```
PATCH /v1/projects/{project}/instances/{instance_id}/databases/{database_id}/ddl
```

```json
{
  "statements": [
    "ALTER TABLE users ADD COLUMN email STRING(MAX)",
    "CREATE TABLE orders (id INT64, user_id INT64) PRIMARY KEY (id)"
  ]
}
```

Applies DDL statements to the DuckDB database. Returns an LRO.

### Get database DDL

```
GET /v1/projects/{project}/instances/{instance_id}/databases/{database_id}/ddl
```

Returns the DDL statements used to create all tables in the database.

### Delete database

```
DELETE /v1/projects/{project}/instances/{instance_id}/databases/{database_id}
```

---

## Sessions

All data operations require a session. Sessions are lightweight — they track which database
is being used but do not hold connection state between requests.

### Create session

```
POST /v1/projects/{project}/instances/{instance_id}/databases/{database_id}/sessions
```

Returns `{ "name": "...databases/{db}/sessions/{uuid}" }`.

### Batch create sessions

```
POST /v1/projects/{project}/instances/{instance_id}/databases/{database_id}/sessions:batchCreate
```

```json
{ "sessionCount": 5 }
```

Returns `{ "session": [...] }` with `sessionCount` sessions.

### List sessions

```
GET /v1/projects/{project}/instances/{instance_id}/databases/{database_id}/sessions
```

### Get session

```
GET /v1/projects/{project}/instances/{instance_id}/databases/{database_id}/sessions/{session_id}
```

### Delete session

```
DELETE /v1/projects/{project}/instances/{instance_id}/databases/{database_id}/sessions/{session_id}
```

---

## Reading data

### Key-set read

```
POST /v1/.../{database_id}/sessions/{session_id}:read
```

```json
{
  "table": "users",
  "columns": ["id", "name", "email"],
  "keySet": {
    "keys": [["1"], ["2"]],
    "ranges": [{ "startClosed": ["10"], "endOpen": ["20"] }],
    "all": false
  },
  "limit": 100,
  "index": ""
}
```

`keySet.all: true` reads all rows. `keySet.keys` reads specific primary key values.
`keySet.ranges` reads key ranges (inclusive/exclusive `startClosed`/`startOpen`/
`endClosed`/`endOpen`).

Returns:

```json
{
  "metadata": { "rowType": { "fields": [{ "name": "id", "type": { "code": "INT64" } }, ...] } },
  "rows": [["1", "Alice", "alice@example.com"]]
}
```

### Streaming read

```
POST /v1/.../{database_id}/sessions/{session_id}:streamingRead
```

Same request body as `:read`. Returns a newline-delimited JSON stream of
`PartialResultSet` objects (compatible with the SDK's streaming path).

---

## SQL queries and DML

### Execute SQL

```
POST /v1/.../{database_id}/sessions/{session_id}:executeSql
```

```json
{
  "sql": "SELECT id, name FROM users WHERE id = @user_id",
  "params": { "user_id": "1" },
  "paramTypes": { "user_id": { "code": "INT64" } }
}
```

For SELECT queries, returns a ResultSet with `metadata` and `rows`. For DML
(`INSERT`, `UPDATE`, `DELETE`), returns `{ "stats": { "rowCountExact": "N" } }`.

### Execute streaming SQL

```
POST /v1/.../{database_id}/sessions/{session_id}:executeStreamingSql
```

Same as `:executeSql` but streams results as newline-delimited `PartialResultSet` JSON.

### Batch DML

```
POST /v1/.../{database_id}/sessions/{session_id}:executeBatchDml
```

```json
{
  "statements": [
    {
      "sql": "INSERT INTO users (id, name) VALUES (@id, @name)",
      "params": { "id": "1", "name": "Alice" },
      "paramTypes": { "id": { "code": "INT64" }, "name": { "code": "STRING" } }
    },
    { "sql": "UPDATE users SET name = 'Bob' WHERE id = 2", "params": {}, "paramTypes": {} }
  ]
}
```

Executes multiple DML statements in sequence. Returns:

```json
{
  "resultSets": [
    { "stats": { "rowCountExact": "1" } },
    { "stats": { "rowCountExact": "1" } }
  ],
  "status": {}
}
```

---

## Mutations and transactions

### Begin transaction

```
POST /v1/.../{database_id}/sessions/{session_id}:beginTransaction
```

```json
{
  "options": {
    "readWrite": {},
    "readOnly": { "strong": true }
  }
}
```

Returns `{ "id": "<base64-transaction-id>" }`.

### Commit

```
POST /v1/.../{database_id}/sessions/{session_id}:commit
```

```json
{
  "transactionId": "<base64-id>",
  "mutations": [
    {
      "insert": {
        "table": "users",
        "columns": ["id", "name"],
        "values": [["3", "Carol"]]
      }
    },
    {
      "update": {
        "table": "users",
        "columns": ["id", "name"],
        "values": [["1", "Alice Updated"]]
      }
    },
    {
      "delete": {
        "table": "users",
        "keySet": { "keys": [["2"]] }
      }
    }
  ]
}
```

Supported mutation types: `insert`, `update`, `insertOrUpdate`, `replace`, `delete`.

Returns `{ "commitTimestamp": "..." }`.

### Rollback

```
POST /v1/.../{database_id}/sessions/{session_id}:rollback
```

```json
{ "transactionId": "<base64-id>" }
```

No-op — returns `{}`.

---

## Long-Running Operations

Instance and database creation/modification operations return LRO resources:

```json
{
  "name": "projects/.../instances/my-instance/operations/op-uuid",
  "done": true,
  "response": { ... }
}
```

All operations complete immediately (`done: true`). Operation status can be polled:

```
GET /v1/projects/{project}/instances/{instance_id}/operations/{op_id}
GET /v1/projects/{project}/instances/{instance_id}/databases/{database_id}/operations/{op_id}
```

---

## Spanner type mapping

| Spanner type | DuckDB type |
|---|---|
| `BOOL` | `BOOLEAN` |
| `INT64` | `BIGINT` |
| `FLOAT64` | `DOUBLE` |
| `STRING(N)` / `STRING(MAX)` | `VARCHAR` |
| `BYTES(N)` / `BYTES(MAX)` | `BLOB` |
| `DATE` | `DATE` |
| `TIMESTAMP` | `TIMESTAMPTZ` |
| `NUMERIC` | `DECIMAL(38, 9)` |
| `JSON` | `JSON` |
| `ARRAY<T>` | DuckDB array type |

---

## Known limitations

| Feature | Notes |
|---|---|
| Partitioned reads and DML | `partitionRead` / `partitionQuery` not implemented |
| Stale reads | `readTimestamp` / `exactStaleness` accepted but returns current data |
| Read isolation | Transactions do not provide MVCC — concurrent writes are visible mid-transaction |
| Change streams | Not implemented |
| Full-text search indexes | Not implemented |
| IAM | Not enforced |

---

## Examples

```bash
uv run python examples/spanner/basic.py
```
