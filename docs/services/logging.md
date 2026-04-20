# Cloud Logging and Cloud Monitoring

Cloudbox emulates the Cloud Logging API (v2) and the Cloud Monitoring API (v3) on a single
port. Log entries, sinks, metrics, and exclusions are stored in memory (or on disk with
`CLOUDBOX_DATA_DIR`).

## Connection

**Port:** `9020` (override with `CLOUDBOX_LOGGING_PORT`)

```python
from google.auth.credentials import AnonymousCredentials
from google.cloud import logging as cloud_logging

client = cloud_logging.Client(
    project="local-project",
    credentials=AnonymousCredentials(),
    client_options={"api_endpoint": "http://localhost:9020"},
)
```

For Cloud Monitoring:

```python
from google.auth.credentials import AnonymousCredentials
from google.cloud import monitoring_v3

client = monitoring_v3.MetricServiceClient(
    credentials=AnonymousCredentials(),
    client_options={"api_endpoint": "http://localhost:9020"},
)
```

---

## Log entries

### Write entries

```
POST /v2/entries:write
```

```json
{
  "logName": "projects/local-project/logs/my-app",
  "resource": { "type": "global", "labels": {} },
  "labels": { "env": "local" },
  "entries": [
    {
      "severity": "INFO",
      "textPayload": "Server started",
      "timestamp": "2024-01-01T00:00:00Z",
      "insertId": "entry-1"
    },
    {
      "severity": "ERROR",
      "jsonPayload": { "message": "Something failed", "code": 500 },
      "httpRequest": { "requestMethod": "GET", "requestUrl": "/api/data", "status": 500 }
    }
  ]
}
```

Top-level `logName`, `resource`, and `labels` serve as defaults; per-entry values
override them. Entries matching an active exclusion filter are silently dropped.

Supported payload types: `textPayload`, `jsonPayload`, `protoPayload`.

Returns `{}`.

### List entries

```
POST /v2/entries:list
```

```json
{
  "resourceNames": ["projects/local-project"],
  "filter": "severity >= ERROR",
  "orderBy": "timestamp desc",
  "pageSize": 50,
  "pageToken": ""
}
```

Returns `{ "entries": [...], "nextPageToken": "..." }`.

Supported filter operators: `=`, `!=`, `>`, `>=`, `<`, `<=`, `AND`, `OR`, `NOT`.
Supported filter fields: `logName`, `severity`, `resource.type`, `labels.*`,
`textPayload`, `jsonPayload.*`, `timestamp`.

---

## Logs

### List logs

```
GET /v2/projects/{project}/logs
```

Returns `{ "logNames": ["projects/.../logs/my-app", ...] }` — the set of distinct log
names that have entries in the store.

### Delete log

```
DELETE /v2/projects/{project}/logs/{log_id}
```

Deletes all entries whose `logName` matches. Returns `{}`.

---

## Sinks

Sinks define routing rules for log entries. Cloudbox stores sink configuration and returns
it correctly but does not actually route entries to external destinations.

### Create sink

```
POST /v2/projects/{project}/sinks
```

```json
{
  "name": "my-sink",
  "destination": "bigquery.googleapis.com/projects/local-project/datasets/logs",
  "filter": "severity >= WARNING",
  "description": "Route warnings to BigQuery"
}
```

Returns the sink resource including a `writerIdentity` service account.

### Get sink

```
GET /v2/projects/{project}/sinks/{sink_id}
```

### List sinks

```
GET /v2/projects/{project}/sinks
```

### Update sink

```
PATCH /v2/projects/{project}/sinks/{sink_id}
```

Updates `destination`, `filter`, and `description`.

### Delete sink

```
DELETE /v2/projects/{project}/sinks/{sink_id}
```

---

## Log-based metrics

### Create metric

```
POST /v2/projects/{project}/metrics
```

```json
{
  "name": "error-count",
  "filter": "severity = ERROR",
  "description": "Count of error log entries",
  "metricDescriptor": { "metricKind": "DELTA", "valueType": "INT64" }
}
```

### Get metric

```
GET /v2/projects/{project}/metrics/{metric_id}
```

### List metrics

```
GET /v2/projects/{project}/metrics
```

### Update metric

```
PATCH /v2/projects/{project}/metrics/{metric_id}
```

### Delete metric

```
DELETE /v2/projects/{project}/metrics/{metric_id}
```

---

## Exclusions

Exclusions filter out log entries matching a given filter before they are stored.

### Create exclusion

```
POST /v2/projects/{project}/exclusions
```

```json
{
  "name": "debug-filter",
  "filter": "severity = DEBUG",
  "disabled": false,
  "description": "Drop debug entries"
}
```

### Get exclusion

```
GET /v2/projects/{project}/exclusions/{exclusion_id}
```

### List exclusions

```
GET /v2/projects/{project}/exclusions
```

### Update exclusion

```
PATCH /v2/projects/{project}/exclusions/{exclusion_id}
```

Updates `filter`, `disabled`, and `description`.

### Delete exclusion

```
DELETE /v2/projects/{project}/exclusions/{exclusion_id}
```

---

## Cloud Monitoring (v3)

### Create time series

```
POST /v3/projects/{project}/timeSeries
```

```json
{
  "timeSeries": [
    {
      "metric": {
        "type": "custom.googleapis.com/request_count",
        "labels": { "method": "GET" }
      },
      "resource": { "type": "global", "labels": { "project_id": "local-project" } },
      "points": [
        {
          "interval": { "endTime": "2024-01-01T00:01:00Z" },
          "value": { "int64Value": "42" }
        }
      ]
    }
  ]
}
```

Time series are stored in memory. Returns `{}`.

### Query time series

```
POST /v3/projects/{project}/timeSeries:query
```

```json
{ "query": "fetch global::custom.googleapis.com/request_count | within 1h" }
```

Accepted and stored — the emulator does not execute MQL query expressions. Returns `{}`.

### List metric descriptors

```
GET /v3/projects/{project}/metricDescriptors
```

Returns descriptors for all custom metrics that have time-series data. The response
includes auto-derived descriptors for any metric type seen in `createTimeSeries` calls.

### List monitored resource descriptors

```
GET /v3/projects/{project}/monitoredResourceDescriptors
```

Returns a static list of common GCP monitored resource types (e.g. `global`,
`gce_instance`, `k8s_container`).

---

## Log entry fields

| Field | Type | Description |
|---|---|---|
| `logName` | string | Log resource name: `projects/{project}/logs/{log_id}` |
| `resource` | object | Monitored resource: `{ "type": "...", "labels": {...} }` |
| `severity` | string | `DEFAULT`, `DEBUG`, `INFO`, `NOTICE`, `WARNING`, `ERROR`, `CRITICAL`, `ALERT`, `EMERGENCY` |
| `timestamp` | string | RFC 3339 entry timestamp |
| `insertId` | string | Deduplication ID (auto-generated if absent) |
| `labels` | object | Key-value metadata |
| `textPayload` | string | Plain-text log message |
| `jsonPayload` | object | Structured JSON log message |
| `protoPayload` | object | Protobuf payload serialized as JSON |
| `httpRequest` | object | HTTP request metadata |
| `operation` | object | Long-running operation metadata |

---

## Known limitations

| Feature | Notes |
|---|---|
| Complex filter expressions | `AND`/`OR`/`NOT` and nested field filters have partial support; simple equality and comparison work reliably |
| Log buckets and views | All entries are stored in a single namespace — no bucket/view scoping |
| Sink routing | Entries are not actually forwarded to BigQuery, GCS, or Pub/Sub destinations |
| Time series query execution | MQL queries accepted but not evaluated |
| Alerting policies | Not implemented |

---

## Examples

```bash
uv run python examples/logging/write_read.py
```
