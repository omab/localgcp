# Cloudbox

[![Tests](https://github.com/omab/cloudbox/actions/workflows/tests.yml/badge.svg)](https://github.com/omab/cloudbox/actions/workflows/tests.yml)
[![Coverage](https://raw.githubusercontent.com/omab/cloudbox/main/coverage.svg)](https://github.com/omab/cloudbox/actions/workflows/tests.yml)

A local emulator for Google Cloud Platform services — like LocalStack, but for GCP.

Run Cloud Storage, Pub/Sub, Firestore, Secret Manager, Cloud Tasks, BigQuery, Cloud Spanner, Cloud Logging, and Cloud Scheduler entirely on your machine, with no real GCP credentials or network access required.

## Table of Contents

- [About this project](#about-this-project)
- [Services](#services)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Using the GCP SDK](#using-the-gcp-sdk)
- [Examples](#examples)
- [`gsutillocal` CLI](#gsutillocal-cli)
- [`gcloudlocal` CLI](#gcloudlocal-cli)
- [BigQuery](#bigquery)
- [Cloud Scheduler](#cloud-scheduler)
- [Development](#development)
- [Architecture](#architecture)
- [Feature Matrix](#feature-matrix)
  - [Cloud Storage](#cloud-storage)
  - [Pub/Sub](#pubsub)
  - [Firestore](#firestore)
  - [Secret Manager](#secret-manager)
  - [Cloud Tasks](#cloud-tasks)
  - [BigQuery](#bigquery-1)
  - [Cloud Spanner](#cloud-spanner-1)
  - [Cloud Logging](#cloud-logging-1)
  - [Cloud Scheduler](#cloud-scheduler-1)
- [Roadmap](#roadmap)

## About this project

Cloudbox is an experiment in LLM-driven development. The entire codebase — services, tests, CLI tools, admin UI, and documentation — was written through an iterative conversation with [Claude Code](https://claude.ai/code), Anthropic's AI coding assistant, with no manual code authoring by the human developer.

The goal is twofold: to explore how far AI-assisted development can go on a non-trivial engineering project, and to produce something genuinely useful for engineers who want to run GCP-dependent services locally without real credentials or network access.

The feature matrix, test coverage, and architecture reflect real engineering tradeoffs, not generated boilerplate. Feedback, issues, and contributions are welcome.

## Services

| Service            | Default Port | Protocol        |
|--------------------|-------------|-----------------|
| Cloud Storage      | 4443        | REST            |
| Cloud Pub/Sub      | 8085        | gRPC            |
| Cloud Pub/Sub REST | 8086        | REST (HTTP/1.1) |
| Cloud Firestore    | 8080        | REST            |
| Secret Manager     | 8090        | REST            |
| Cloud Tasks        | 8123        | REST            |
| BigQuery           | 9050        | REST            |
| Cloud Spanner      | 9010        | REST            |
| Cloud Logging      | 9020        | REST            |
| Cloud Scheduler    | 8091        | REST            |
| Admin UI           | 8888        | HTTP            |

## Quick Start

### Docker (recommended)

```bash
docker compose up
```

Or pull and run directly from Docker Hub:

```bash
docker run --rm -p 4443:4443 -p 8080:8080 -p 8085:8085 -p 8086:8086 \
  -p 8090:8090 -p 8091:8091 -p 8123:8123 -p 8888:8888 \
  -p 9010:9010 -p 9020:9020 -p 9050:9050 \
  omab/cloudbox:latest
```

To persist data across restarts, mount a host directory:

```bash
docker run --rm -p 4443:4443 -p 8080:8080 -p 8085:8085 -p 8086:8086 \
  -p 8090:8090 -p 8091:8091 -p 8123:8123 -p 8888:8888 \
  -p 9010:9010 -p 9020:9020 -p 9050:9050 \
  -v "$(pwd)/cloudbox-data:/data" -e CLOUDBOX_DATA_DIR=/data \
  omab/cloudbox:latest
```

The container starts all services and exposes their ports automatically. The Admin UI is available at http://localhost:8888.

### Local (Python)

Requires Python 3.12+ and [uv](https://github.com/astral-sh/uv).

```bash
uv sync
uv run python -m cloudbox.main
# or
uv run cloudbox
```

## Configuration

All settings are controlled via environment variables:

| Variable                      | Default        | Description                                         |
|-------------------------------|----------------|-----------------------------------------------------|
| `CLOUDBOX_PROJECT`            | `local-project`| Default GCP project ID                              |
| `CLOUDBOX_LOCATION`           | `us-central1`  | Default GCP region                                  |
| `CLOUDBOX_DATA_DIR`           | *(unset)*      | Directory for JSON persistence; in-memory if unset  |
| `CLOUDBOX_HOST`               | `0.0.0.0`      | Bind address                                        |
| `CLOUDBOX_LOG_LEVEL`          | `info`         | Log level (`debug`, `info`, `warning`, …)           |
| `CLOUDBOX_GCS_PORT`           | `4443`         | Cloud Storage port                                  |
| `CLOUDBOX_PUBSUB_PORT`        | `8085`         | Pub/Sub gRPC port                                   |
| `CLOUDBOX_PUBSUB_REST_PORT`   | `8086`         | Pub/Sub REST port                                   |
| `CLOUDBOX_FIRESTORE_PORT`     | `8080`         | Firestore port                                      |
| `CLOUDBOX_SECRETMANAGER_PORT` | `8090`         | Secret Manager port                                 |
| `CLOUDBOX_TASKS_PORT`         | `8123`         | Cloud Tasks port                                    |
| `CLOUDBOX_BIGQUERY_PORT`      | `9050`         | BigQuery port                                       |
| `CLOUDBOX_SCHEDULER_PORT`     | `8091`         | Cloud Scheduler port                                |
| `CLOUDBOX_ADMIN_PORT`         | `8888`         | Admin UI port                                       |

To enable data persistence across restarts, set `CLOUDBOX_DATA_DIR` to a writable path (and mount it as a Docker volume if using containers).

## Using the GCP SDK

Point the official `google-cloud-*` SDK clients at Cloudbox using the helpers in `sdk_compat/clients.py`:

```python
from sdk_compat.clients import (
    storage_client,
    pubsub_publisher,
    pubsub_subscriber,
    firestore_client,
    secret_manager_client,
    tasks_client,
    bigquery_client,
    scheduler_client,
)

# Cloud Storage
client = storage_client()
bucket = client.bucket("my-bucket")

# Pub/Sub (gRPC by default, or pass transport="rest")
publisher = pubsub_publisher()
subscriber = pubsub_subscriber()

# Firestore
db = firestore_client()

# Secret Manager
sm = secret_manager_client()

# Cloud Tasks
tasks = tasks_client()

# BigQuery
bq = bigquery_client()
result = bq.query("SELECT 1 AS n").result()

# Cloud Scheduler
sched = scheduler_client()
```

### Pub/Sub — connecting directly

For gRPC transport (the standard emulator pattern), set the environment variable before importing the SDK:

```bash
export PUBSUB_EMULATOR_HOST=localhost:8085
```

For REST transport, use `api_endpoint="http://localhost:8086"` in `ClientOptions`.

## Examples

The `examples/` directory contains runnable scripts for every service. Each
script is self-contained and targets a live Cloudbox instance via the REST APIs.

```
examples/
  shared.py                    Base URLs and httpx client helpers
  gcs/
    upload_download.py         Upload, list, download, delete objects
    compose.py                 Compose multiple objects into one
    byte_range.py              Partial downloads via Range header
    cors.py                    Set, inspect, and clear bucket CORS rules
    retention.py               Set retention policy, upload object, verify enforcement
  pubsub/
    publish_subscribe.py       Create topic/subscription, publish, pull, ack
    batch_publish.py           Batch publish with message attributes
  firestore/
    crud.py                    Create, get, field-mask update, delete
    queries.py                 Filters, ordering, cursor pagination, field projection, aggregation
    transactions.py            Atomic commit with field transforms
    batch_write.py             batchWrite with per-write error handling
  bigquery/
    tables.py                  Create dataset/table, insert rows, query, schema evolution
    views.py                   Create, query, update, and delete views
    parameterized_query.py     Named (@param) and positional (?) parameters
    information_schema.py      Query INFORMATION_SCHEMA for tables, columns, schemata
  secretmanager/
    secrets.py                 Create secret, add versions, access, disable
  tasks/
    tasks.py                   Create queue, enqueue tasks, list, delete
  scheduler/
    jobs.py                    Create, pause, resume, delete cron jobs
  logging/
    exclusions.py              Create exclusions, verify filtering at write time
```

Start Cloudbox, then run any example from the repo root:

```bash
uv run cloudbox &
uv run python examples/gcs/upload_download.py
uv run python examples/firestore/queries.py
```

## `gsutillocal` CLI

`gsutillocal` mirrors the `gsutil` command syntax for Cloud Storage. After `uv sync`:

```bash
gsutillocal ls                              # list all buckets
gsutillocal ls gs://my-bucket              # list objects
gsutillocal ls -l gs://my-bucket           # long listing (size + timestamp)
gsutillocal ls -r gs://my-bucket           # recursive
gsutillocal mb gs://my-bucket              # create bucket
gsutillocal mb -l US-EAST1 gs://my-bucket  # with location
gsutillocal rb gs://my-bucket              # delete bucket
gsutillocal cp ./file.txt gs://bucket/     # upload
gsutillocal cp -r ./dir/ gs://bucket/dir/  # recursive upload
gsutillocal cp gs://bucket/file.txt ./     # download
gsutillocal cp gs://b1/obj gs://b2/obj     # GCS-to-GCS copy
gsutillocal mv gs://b/old gs://b/new       # move / rename
gsutillocal rm gs://bucket/file.txt        # delete object
gsutillocal rm gs://bucket/logs/*          # wildcard delete
gsutillocal rm -r gs://bucket              # delete all objects + bucket
gsutillocal cat gs://bucket/file.txt       # print to stdout
gsutillocal stat gs://bucket/file.txt      # object metadata
gsutillocal du gs://bucket                 # disk usage
```

Global flags `-m` (parallel) and `-o` (boto options) are accepted and silently ignored for drop-in compatibility with existing scripts.

## `gcloudlocal` CLI

`gcloudlocal` is a `gcloud`-compatible CLI installed as an entry point that targets the Cloudbox emulator. After `uv sync` it is available as:

```bash
uv run gcloudlocal [--project PROJECT] [--location LOCATION] [--format json] \
    SERVICE RESOURCE VERB [ARGS] [FLAGS]
```

Or use the root wrapper script:

```bash
./gcloud [--project PROJECT] [--location LOCATION] [--format json] \
    SERVICE RESOURCE VERB [ARGS] [FLAGS]
```

### Examples

```bash
# Cloud Storage
gcloudlocal storage buckets list
gcloudlocal storage buckets create my-bucket
gcloudlocal storage cp ./file.txt gs://my-bucket/file.txt

# Pub/Sub
gcloudlocal pubsub topics create my-topic
gcloudlocal pubsub subscriptions create my-sub --topic my-topic
gcloudlocal pubsub topics publish my-topic --message "hello"

# Secrets
gcloudlocal secrets create my-secret
gcloudlocal secrets versions add my-secret --data "s3cr3t"

# Firestore
gcloudlocal firestore documents list my-collection

# Cloud Tasks
gcloudlocal tasks queues create my-queue
gcloudlocal tasks tasks create my-queue --url http://localhost:8000/task

# Cloud Scheduler
gcloudlocal scheduler jobs list
gcloudlocal scheduler jobs create my-job --schedule "* * * * *" --uri http://localhost:8000/handler
```

## BigQuery

BigQuery is backed by [DuckDB](https://duckdb.org/). Datasets map to DuckDB schemas; tables map to DuckDB tables. SQL is rewritten at query time to convert backtick identifiers (`project.dataset.table`) to double-quoted DuckDB form.

Supported operations:
- Dataset and table CRUD
- `SELECT`, `WITH`, `VALUES` queries
- DML: `INSERT`, `UPDATE`, `DELETE` — returns `numDmlAffectedRows`
- `CREATE TABLE AS SELECT` (CTAS)
- `SHOW`, `DESCRIBE`, `EXPLAIN`, `PRAGMA`

## Cloud Scheduler

Jobs are stored in memory (or persisted to `CLOUDBOX_DATA_DIR`). A background worker polls every 30 seconds and fires HTTP requests via `httpTarget`. Job state transitions (enable/pause/resume) and force-run are supported.

```python
from sdk_compat.clients import scheduler_client
from google.cloud import scheduler_v1

client = scheduler_client()
parent = f"projects/local-project/locations/us-central1"

job = scheduler_v1.Job(
    name=f"{parent}/jobs/my-job",
    schedule="* * * * *",
    time_zone="UTC",
    http_target=scheduler_v1.HttpTarget(
        uri="http://localhost:8000/handler",
        http_method=scheduler_v1.HttpMethod.POST,
    ),
)
client.create_job(request={"parent": parent, "job": job})
```

## Development

### Running tests

```bash
uv run pytest tests/
```

Tests use `pytest-asyncio` with `asyncio_mode = "auto"`. Each test file covers one service.

### Live SDK smoke tests

```bash
uv run python sdk_compat/test_with_sdk.py
```

This requires Cloudbox to be running and the dev dependencies installed (`uv sync`).

### Helper scripts

The `bin/` directory contains shell scripts for common setup tasks:

- `bin/setup_gcs_pubsub.sh` — create a GCS bucket with a Pub/Sub notification
- `bin/create_pubsub_topic_subscription.sh` — create a topic and subscription
- `bin/print_pubsub_messages.sh` — pull and print messages from a subscription
- `bin/upload_to_gcs.sh` — upload a file to a GCS bucket

## Architecture

All state is stored in memory by default, using a `NamespacedStore` (`cloudbox/core/store.py`). Each service is a standalone FastAPI application started concurrently by `cloudbox/main.py` via uvicorn. Pub/Sub additionally runs a gRPC server on port 8085 (compatible with `PUBSUB_EMULATOR_HOST`). Cloud Scheduler runs a background asyncio worker that polls every 30 seconds.

Every service module follows the same layout:

```
cloudbox/services/<service>/
    app.py      — FastAPI routes
    models.py   — Pydantic v2 models
    store.py    — NamespacedStore wrapper
    worker.py   — (scheduler only) background dispatch loop
```

BigQuery uses an additional `engine.py` module wrapping DuckDB instead of a `NamespacedStore`.

## Feature Matrix

Legend: ✅ Supported · 🟡 Partial · ❌ Not supported

### Cloud Storage

| Feature | Status | Notes |
|---------|:------:|-------|
| Create / get / list / delete bucket | ✅ | |
| Upload — simple media (`uploadType=media`) | ✅ | |
| Upload — multipart (`uploadType=multipart`) | ✅ | Metadata + body in one request |
| Upload — resumable (`uploadType=resumable`) | ✅ | Single-shot and chunked; `308 Resume Incomplete` with `Range` header |
| Download object | ✅ | Both `/o/{name}?alt=media` and `/download/` paths |
| Get / update object metadata | ✅ | PATCH merges `contentType`, `metadata`, `cacheControl`, `contentEncoding`, `contentDisposition` |
| Delete object | ✅ | |
| List objects (prefix, delimiter, pagination) | ✅ | Virtual directory simulation via delimiter |
| Copy object (within or across buckets) | ✅ | |
| Compose objects | ✅ | Up to 32 source objects; optional `ifGenerationMatch` per source |
| Rewrite object | ✅ | Single-shot (no resumption); supports `contentType` and `storageClass` override in request body |
| Object versioning | ❌ | Generation number increments on overwrite but old versions are not retained |
| MD5 hash + CRC32c checksum | ✅ | Computed and returned on upload |
| ETag | ✅ | MD5-based |
| Byte-range downloads (`Range` header) | ✅ | `bytes=start-end`, `bytes=start-`, `bytes=-suffix`; returns 206 with `Content-Range`; 416 on unsatisfiable range |
| Conditional requests (`If-Match`, `If-None-Match`, `ifGenerationMatch`, `ifMetagenerationMatch`) | ✅ | Checked on GET, PATCH, DELETE, and upload; `ifGenerationMatch=0` for create-only writes |
| Pub/Sub notifications (bucket events) | ✅ | `OBJECT_FINALIZE`, `OBJECT_DELETE`, `OBJECT_METADATA_UPDATE` |
| Notification config CRUD | ✅ | |
| Object lifecycle rules | ✅ | `Delete` and `SetStorageClass` actions; `age`, `createdBefore`, `matchesStorageClass` conditions; enforced on list |
| Bucket / object ACLs | ❌ | All requests succeed regardless of caller identity |
| IAM policies (`getIamPolicy` / `setIamPolicy`) | ❌ | |
| Signed URLs | ❌ | |
| CORS configuration | ✅ | Per-bucket `cors` list; GET/PUT `/b/{bucket}/cors`; also settable via `PATCH /b/{bucket}` |
| Bucket retention policies | ✅ | `retentionPeriod` (seconds) stored on bucket; objects get `retentionExpirationTime`; deletes blocked until expiry; policy removable unless `isLocked=true` |

### Pub/Sub

| Feature | Status | Notes |
|---------|:------:|-------|
| Create / get / list / delete topic | ✅ | |
| Publish messages (single or batch) | ✅ | |
| Create / get / list / delete subscription | ✅ | |
| Pull messages | ✅ | `maxMessages`, returns `ackId` + `deliveryAttempt` |
| Acknowledge messages | ✅ | |
| Modify ack deadline | ✅ | Including deadline=0 for immediate nack |
| Ack deadline expiry + redelivery | ✅ | Expired messages re-queued on next pull |
| Push subscriptions | ✅ | POSTs to `pushEndpoint`; 2xx → ack, non-2xx/error → nack + requeue |
| Dead-letter policy | ✅ | Routes to DLQ topic after `maxDeliveryAttempts` |
| Retry policy (exponential backoff) | ✅ | `minimumBackoff` / `maximumBackoff` per GCP duration string |
| Message filtering | ✅ | `attributes.KEY = "VAL"`, `hasPrefix(...)`, `NOT` / `AND` / `OR` |
| Message ordering | ✅ | One in-flight message per `orderingKey` at a time |
| gRPC server (port 8085) | ✅ | Compatible with `PUBSUB_EMULATOR_HOST` |
| REST server (port 8086) | ✅ | `transport="rest"` SDK clients |
| Streaming Pull (gRPC) | ✅ | Bidirectional stream; delivers messages, processes ack/nack/modifyDeadline |
| Snapshots / seek | ✅ | Create/delete/list snapshots; seek to snapshot or RFC3339 timestamp |
| Schema validation | ✅ | AVRO and PROTOCOL_BUFFER schemas; CRUD + `ValidateSchema` / `ValidateMessage`; topic `schemaSettings` enforced on publish |
| BigQuery subscriptions | ✅ | `bigqueryConfig.table`; `writeMetadata`, `useTopicSchema`, `dropUnknownFields` supported |
| Cloud Storage subscriptions | ✅ | `cloudStorageConfig.bucket`; text (raw bytes) and Avro-JSON formats; `filenamePrefix`/`filenameSuffix`; `avroConfig.writeMetadata` |
| Topic message retention | ✅ | `messageRetentionDuration` honoured; messages retained in topic log |

### Firestore

| Feature | Status | Notes |
|---------|:------:|-------|
| Create / get / update / delete document | ✅ | |
| Create with auto-generated ID | ✅ | |
| List documents in collection | ✅ | Pagination via `pageSize` / `pageToken` |
| Batch get | ✅ | |
| Transactions (`beginTransaction` / `commit` / `rollback`) | ✅ | Serialized; no true isolation (optimistic concurrency not enforced) |
| Field mask updates (`updateMask`) | ✅ | |
| `runQuery` — `WHERE` filters | ✅ | `EQUAL`, `NOT_EQUAL`, `<`, `<=`, `>`, `>=`, `IN`, `NOT_IN`, `ARRAY_CONTAINS`, `ARRAY_CONTAINS_ANY`, `IS_NULL`, `IS_NAN` and negations |
| `runQuery` — composite filters (`AND` / `OR`) | ✅ | |
| `runQuery` — `ORDER BY` (multi-field) | ✅ | `ASCENDING` / `DESCENDING` |
| `runQuery` — `LIMIT` / `OFFSET` | ✅ | |
| `runQuery` — collection group queries | ✅ | `allDescendants: true` |
| `runQuery` — cursor pagination (`startAt` / `endBefore`) | ✅ | All four variants: `startAt` (inclusive), `startAfter` (exclusive), `endBefore` (exclusive), `endAt` (inclusive); respects `orderBy` direction; composes with `limit` |
| `runQuery` — field projection (`SELECT`) | ✅ | `select.fields` list of field paths; applied after filtering/ordering; dotted nested paths supported |
| Batch write (`batchWrite`) | ✅ | Independent per-write success/failure; same semantics as `commit`; `currentDocument` preconditions supported |
| Field transforms (`increment`, `arrayUnion`, `arrayRemove`, `serverTimestamp`) | ✅ | `updateTransforms` in commit writes; dotted field paths supported |
| Aggregation queries (`COUNT`, `SUM`, `AVG`) | ✅ | `runAggregationQuery`; `count` (with optional `upTo`), `sum`, `avg`; multiple aggregations per request; respects `where` / `limit` from nested `structuredQuery` |
| Real-time listeners (`listen` endpoint) | ❌ | `on_snapshot()` / `DocumentReference.listen()` not supported |
| Document preconditions (`exists`, `updateTime`) | ✅ | `currentDocument.exists` and `currentDocument.updateTime` checked in `commit` and `batchWrite` |
| IAM / security rules | ❌ | All requests succeed |

### Secret Manager

| Feature | Status | Notes |
|---------|:------:|-------|
| Create / get / list / update / delete secret | ✅ | |
| Add secret version | ✅ | |
| Get / list versions | ✅ | `filter` by state supported |
| Access secret version (read payload) | ✅ | Respects `ENABLED` state check |
| Enable / disable / destroy version | ✅ | Destroy wipes payload |
| Resolve `latest` version | ✅ | Returns highest-numbered `ENABLED` version |
| Secret labels | ✅ | |
| IAM policies | ❌ | |
| Rotation notifications (Pub/Sub) | ❌ | |
| Replication configuration | ❌ | Single-region assumed |
| Secret annotations / etag conditions | ❌ | |
| CMEK | ❌ | Not applicable for local dev |

### Cloud Tasks

| Feature | Status | Notes |
|---------|:------:|-------|
| Create / get / list / delete queue | ✅ | |
| Update queue (`rateLimits`, `retryConfig`) | ✅ | Stored but rate limits not enforced |
| Pause / resume / purge queue | ✅ | |
| Create task (HTTP target) | ✅ | `url`, `httpMethod`, `headers`, `body` (base64) |
| List / get / delete task | ✅ | |
| Force-run task | ✅ | Ignores `scheduleTime` |
| Deferred execution (`scheduleTime`) | ✅ | Worker dispatches when time arrives |
| Retry on failure (`maxAttempts`) | ✅ | Task dropped after `maxAttempts` |
| Automatic dispatch worker | ✅ | Polls every 1 second |
| `dispatchCount` / `firstAttempt` / `lastAttempt` tracking | ✅ | |
| App Engine HTTP tasks | ❌ | |
| OIDC / OAuth tokens in task dispatch | ❌ | |
| Rate limiting enforcement (`maxDispatchesPerSecond`, `maxConcurrentDispatches`) | ❌ | Config stored, not enforced |
| Retry backoff (`minBackoff`, `maxBackoff`, `maxDoublings`) | ✅ | Exponential backoff: `minBackoff * 2^min(attempt-1, maxDoublings)`, capped at `maxBackoff`; next `scheduleTime` set accordingly |
| Task deduplication (content-based) | ❌ | Duplicate rejected only on exact name collision |

### BigQuery

| Feature | Status | Notes |
|---------|:------:|-------|
| Create / get / list / delete dataset | ✅ | |
| Create / get / list / delete table (with schema) | ✅ | |
| `SELECT` / `WITH` / `VALUES` queries | ✅ | Executed via DuckDB |
| DML: `INSERT` / `UPDATE` / `DELETE` | ✅ | Returns `numDmlAffectedRows` |
| `CREATE TABLE AS SELECT` (CTAS) | ✅ | |
| `SHOW` / `DESCRIBE` / `EXPLAIN` / `PRAGMA` | ✅ | DuckDB native |
| Backtick identifier rewriting (`project.dataset.table`) | ✅ | Converted to DuckDB double-quoted form |
| Streaming inserts (`insertAll`) | ✅ | Rows appended via DuckDB `INSERT` |
| Read rows (`tabledata.list`) | ✅ | |
| Async job insert + poll (`jobs.insert` / `jobs.get`) | ✅ | Jobs complete synchronously; polling always returns done |
| Sync query shortcut (`queries` endpoint) | ✅ | |
| `getQueryResults` | ✅ | |
| Parameterized queries (`queryParameters`) | ✅ | Named (`@param`) and positional (`?`) modes; INT64, FLOAT64, BOOL, STRING, BYTES, ARRAY |
| Partitioned / clustered tables | ❌ | Schema ignored; data stored flat in DuckDB |
| Table update / schema evolution | ✅ | `PATCH`/`PUT` adds new columns via `ALTER TABLE ADD COLUMN`; updates description and labels; existing columns are preserved |
| Views | ✅ | `CREATE VIEW` backed by DuckDB; create, query, update (`PATCH`/`PUT`), delete; listed with `type: VIEW` |
| Authorized views | ❌ | |
| External tables | ❌ | |
| Scripting / multi-statement queries | 🟡 | Single-statement only; DuckDB may handle simple cases |
| `INFORMATION_SCHEMA` queries | ✅ | `TABLES`, `COLUMNS`, `SCHEMATA`, `VIEWS` rewritten to DuckDB's `information_schema`; project/dataset-qualified and unqualified forms both supported |
| Geography / spatial functions | 🟡 | Only what DuckDB supports natively |
| Array / struct / JSON functions | 🟡 | DuckDB syntax, not always identical to BigQuery |
| IAM / row-level security | ❌ | |
| BI Engine / reservations | ❌ | Not applicable for local dev |

### Cloud Scheduler

| Feature | Status | Notes |
|---------|:------:|-------|
| Create / get / list / update / delete job | ✅ | |
| Pause / resume job | ✅ | |
| Force-run job | ✅ | Dispatches immediately regardless of schedule |
| HTTP target (`httpTarget`) | ✅ | All HTTP methods; custom headers supported |
| Cron schedule (all standard expressions) | ✅ | Evaluated via `croniter` |
| Background dispatch worker | ✅ | Polls every 30 seconds |
| Job state tracking (`lastAttemptTime`, `status`) | ✅ | |
| Pub/Sub target | ❌ | |
| App Engine target | ❌ | |
| OIDC / OAuth auth for HTTP target | ❌ | Requests sent without auth headers |
| Retry configuration | ✅ | Exponential backoff (`minBackoffDuration * 2^min(attempt-1, maxDoublings)`, capped at `maxBackoffDuration`); bounded by `retryCount` and `maxRetryDuration` |
| Timezone support | 🟡 | Parsed by `croniter`; UTC works reliably, some IANA zones may differ from GCP |

### Cloud Spanner

| Feature | Status | Notes |
|---------|:------:|-------|
| Create / get / list / update / delete instance | ✅ | |
| Create / get / list / delete database | ✅ | Returns LRO (immediately done) |
| DDL execution (`updateDdl`) | ✅ | CREATE TABLE, DROP TABLE, ALTER TABLE, CREATE INDEX |
| Get DDL (`getDatabaseDdl`) | ✅ | Returns stored DDL statements |
| Spanner → DuckDB type mapping | ✅ | STRING, INT64, FLOAT64, BOOL, BYTES, DATE, TIMESTAMP, NUMERIC, JSON, ARRAY |
| `INTERLEAVE IN PARENT` clause | ✅ | Silently stripped — no parent-child enforcement |
| Sessions: create, get, delete, batchCreate | ✅ | |
| Transactions: beginTransaction, rollback | ✅ | No real isolation — local dev only |
| Mutations: insert, update, insertOrUpdate, replace, delete | ✅ | All mutation types supported |
| `commit` with `singleUseTransaction` | ✅ | |
| `executeSql` | ✅ | SELECT and DML; `@param_name` parameters |
| `executeStreamingSql` | ✅ | Returns newline-delimited JSON PartialResultSet |
| `executeBatchDml` | ✅ | Per-statement row counts returned |
| `read` (key-based reads) | ✅ | `keys`, `all: true`, key ranges |
| `streamingRead` | ✅ | Same as read but streaming response |
| Operations (`GET .../operations/{id}`) | ✅ | Always returns `done: true` |
| Instance configs (stub) | ✅ | Returns one regional config |
| True MVCC / read isolation | ❌ | All reads see latest committed state |
| Partitioned reads / DML | ❌ | |
| Mutation groups (atomic transactions across mutations) | 🟡 | Applied sequentially, no atomicity guarantee |

### Cloud Logging

| Feature | Status | Notes |
|---------|:------:|-------|
| Write log entries (`entries:write`) | ✅ | Batch writes; default logName/resource/labels applied |
| List log entries (`entries:list`) | ✅ | Pagination via `pageToken` |
| Filter: `logName=`, `severity>=`, `timestamp>=`, `resource.type=` | ✅ | |
| List logs (`GET /v2/projects/{project}/logs`) | ✅ | Returns distinct log names |
| Delete log | ✅ | Deletes all entries with matching logName |
| Sinks: create, get, list, update, delete | ✅ | `writerIdentity` auto-set |
| Log-based metrics: create, get, list, update, delete | ✅ | |
| Auto-fill `timestamp` and `insertId` on write | ✅ | |
| Cloud Monitoring: write time series | ✅ | Stores points; no aggregation |
| Cloud Monitoring: metric descriptors (stub) | ✅ | |
| Complex filter expressions (`AND`, `OR`, `NOT`) | ❌ | Simple equality / comparison only |
| Log exclusions | ✅ | Create/get/list/update/delete; active exclusions silently drop matching entries at write time; `disabled=true` bypasses filtering |
| Log views | ❌ | |
| Log buckets | ❌ | |
| Monitored resource metadata | ❌ | Stored as-is; not validated |

---

## Roadmap

- Vertex AI
