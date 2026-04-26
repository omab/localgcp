# BigQuery

Cloudbox emulates the BigQuery REST API (v2) using DuckDB as the backend. Standard SQL
queries, streaming inserts, and dataset/table management all work through the `google-cloud-bigquery`
Python SDK without modification.

## Connection

**Port:** `9050` (override with `CLOUDBOX_BIGQUERY_PORT`)

```python
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

client = bigquery.Client(
    project="local-project",
    credentials=AnonymousCredentials(),
    client_options={"api_endpoint": "http://localhost:9050"},
)
```

---

## Datasets

### Create dataset

```
POST /bigquery/v2/projects/{project}/datasets
```

```json
{
  "datasetReference": { "datasetId": "my_dataset" },
  "location": "US",
  "labels": { "env": "local" }
}
```

Creates the dataset and the corresponding DuckDB schema. Returns the dataset resource.
`409` if it already exists.

### Get dataset

```
GET /bigquery/v2/projects/{project}/datasets/{dataset_id}
```

Returns the dataset resource. `404` if not found.

### List datasets

```
GET /bigquery/v2/projects/{project}/datasets
```

Returns `{ "kind": "bigquery#datasetList", "datasets": [...] }`.

### Delete dataset

```
DELETE /bigquery/v2/projects/{project}/datasets/{dataset_id}
```

Drops the DuckDB schema and all its tables. `204` on success.

---

## Tables

### Create table

```
POST /bigquery/v2/projects/{project}/datasets/{dataset_id}/tables
```

```json
{
  "tableReference": { "tableId": "my_table" },
  "schema": {
    "fields": [
      { "name": "id",    "type": "INTEGER", "mode": "REQUIRED" },
      { "name": "name",  "type": "STRING",  "mode": "NULLABLE" },
      { "name": "score", "type": "FLOAT",   "mode": "NULLABLE" }
    ]
  }
}
```

Creates the DuckDB table. Supported field types: `STRING`, `INTEGER`, `INT64`, `FLOAT`,
`FLOAT64`, `BOOLEAN`, `BOOL`, `BYTES`, `DATE`, `TIME`, `DATETIME`, `TIMESTAMP`, `NUMERIC`,
`BIGNUMERIC`, `JSON`, `RECORD` (nested). Returns the table resource.

### Get table

```
GET /bigquery/v2/projects/{project}/datasets/{dataset_id}/tables/{table_id}
```

Returns the table resource including schema. `404` if not found.

### List tables

```
GET /bigquery/v2/projects/{project}/datasets/{dataset_id}/tables
```

Returns `{ "kind": "bigquery#tableList", "tables": [...] }`.

### Update / patch table

```
PATCH /bigquery/v2/projects/{project}/datasets/{dataset_id}/tables/{table_id}
PUT   /bigquery/v2/projects/{project}/datasets/{dataset_id}/tables/{table_id}
```

Updates table metadata (description, labels). Schema updates (adding/removing columns)
are also accepted and applied to the underlying DuckDB table.

### Delete table

```
DELETE /bigquery/v2/projects/{project}/datasets/{dataset_id}/tables/{table_id}
```

Drops the DuckDB table. `204` on success.

---

## Running queries

### Asynchronous job

```
POST /bigquery/v2/projects/{project}/jobs
```

```json
{
  "configuration": {
    "query": {
      "query": "SELECT id, name FROM `local-project.my_dataset.my_table` WHERE score > 0.5",
      "useLegacySql": false,
      "queryParameters": [],
      "parameterMode": "NONE"
    }
  },
  "jobReference": { "jobId": "job-abc123" }
}
```

All jobs complete synchronously — there is no async execution. The response immediately
contains the results. `jobId` is optional (auto-generated if omitted).

### Get job results

```
GET /bigquery/v2/projects/{project}/queries/{job_id}?maxResults=1000&pageToken=
```

Returns the query results for a previously submitted job:

```json
{
  "kind": "bigquery#queryResponse",
  "jobComplete": true,
  "schema": { "fields": [...] },
  "rows": [{ "f": [{ "v": "1" }, { "v": "Alice" }] }],
  "totalRows": "1"
}
```

### Synchronous query

```
POST /bigquery/v2/projects/{project}/queries
```

```json
{
  "query": "SELECT COUNT(*) FROM `local-project.my_dataset.my_table`",
  "useLegacySql": false
}
```

Executes the query and returns results in a single response — no separate job polling needed.

### Get job status

```
GET /bigquery/v2/projects/{project}/jobs/{job_id}
```

Returns the job resource. Since all jobs are synchronous, `status.state` is always
`DONE`.

### Cancel job

```
POST /bigquery/v2/projects/{project}/jobs/{job_id}/cancel
```

No-op — returns the job resource unchanged (jobs are already complete).

---

## Streaming inserts

```
POST /bigquery/v2/projects/{project}/datasets/{dataset_id}/tables/{table_id}/insertAll
```

```json
{
  "rows": [
    { "insertId": "row-1", "json": { "id": 1, "name": "Alice", "score": 0.9 } },
    { "insertId": "row-2", "json": { "id": 2, "name": "Bob",   "score": 0.7 } }
  ]
}
```

Inserts rows directly into the DuckDB table. `insertId` is optional. Returns:

```json
{ "kind": "bigquery#tableDataInsertAllResponse", "insertErrors": [] }
```

---

## Reading table rows

```
GET /bigquery/v2/projects/{project}/datasets/{dataset_id}/tables/{table_id}/data
    ?maxResults=1000&pageToken=
```

Lists rows from a table without running a query job. Returns:

```json
{
  "kind": "bigquery#tableDataList",
  "totalRows": "100",
  "schema": { "fields": [...] },
  "rows": [{ "f": [...] }],
  "pageToken": "1000"
}
```

---

## SQL dialect and identifier syntax

The emulator uses DuckDB's SQL dialect. BigQuery identifier syntax is rewritten
automatically at query time:

| BigQuery syntax | DuckDB equivalent |
|---|---|
| `` `project.dataset.table` `` | `"project"."dataset"."table"` |
| `` `dataset.table` `` | `"dataset"."table"` |
| `` `table` `` | `"table"` |

Legacy SQL (`useLegacySql: true`) is accepted but falls through to DuckDB standard SQL
without transformation.

### Parameterized queries

Named and positional parameters are supported:

```json
{
  "query": "SELECT * FROM `my_dataset.my_table` WHERE name = @username",
  "parameterMode": "NAMED",
  "queryParameters": [
    {
      "name": "username",
      "parameterType": { "type": "STRING" },
      "parameterValue": { "value": "Alice" }
    }
  ]
}
```

Supported parameter types: `STRING`, `INT64`, `FLOAT64`, `BOOL`, `DATE`, `DATETIME`,
`TIMESTAMP`.

---

## DML statements

`INSERT`, `UPDATE`, `DELETE`, and `MERGE`-style operations are supported via the standard
query endpoints. After execution, `numDmlAffectedRows` is reported in the job status:

```json
{
  "statistics": {
    "query": { "numDmlAffectedRows": "5" }
  }
}
```

---

## Multi-statement scripts

Multiple SQL statements separated by `;` are supported. Statements are executed
sequentially in a single DuckDB connection. The job result contains:

- Rows and schema from the **last SELECT** statement (if any).
- The **sum of `numDmlAffectedRows`** across all DML statements when no SELECT is present.

```sql
CREATE TABLE my_dataset.events (id INTEGER, name STRING);
INSERT INTO my_dataset.events VALUES (1, 'click'), (2, 'view');
SELECT id, name FROM my_dataset.events ORDER BY id
```

Limitations: `DECLARE`/`SET` variable statements and `BEGIN…END` blocks are not supported.

---

## Known limitations

| Feature | Notes |
|---|---|
| Partitioned tables | Partition metadata accepted but query pruning not implemented |
| Clustered tables | Clustering config accepted but not enforced |
| External tables | Not supported |
| BigQuery ML (`CREATE MODEL`, `ML.PREDICT`) | Not implemented |
| Materialized views | Not implemented |
| Jobs.insert with load/extract config | Only `query` configuration is supported |
| Dataset / table IAM | Not enforced |

---

## Examples

```bash
uv run python examples/bigquery/query.py
uv run python examples/bigquery/streaming_insert.py
```
