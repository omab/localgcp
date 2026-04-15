# LocalGCP

A local emulator for Google Cloud Platform services — like LocalStack, but for GCP.

Run Cloud Storage, Pub/Sub, Firestore, Secret Manager, Cloud Tasks, BigQuery, and Cloud Scheduler entirely on your machine, with no real GCP credentials or network access required.

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
| Cloud Scheduler    | 8091        | REST            |
| Admin UI           | 8888        | HTTP            |

## Quick Start

### Docker (recommended)

```bash
docker compose up
```

The container starts all services and exposes their ports automatically. The Admin UI is available at http://localhost:8888.

### Local (Python)

Requires Python 3.12+ and [uv](https://github.com/astral-sh/uv).

```bash
uv sync
uv run python -m localgcp.main
# or
uv run localgcp
```

## Configuration

All settings are controlled via environment variables:

| Variable                      | Default        | Description                                         |
|-------------------------------|----------------|-----------------------------------------------------|
| `LOCALGCP_PROJECT`            | `local-project`| Default GCP project ID                              |
| `LOCALGCP_LOCATION`           | `us-central1`  | Default GCP region                                  |
| `LOCALGCP_DATA_DIR`           | *(unset)*      | Directory for JSON persistence; in-memory if unset  |
| `LOCALGCP_HOST`               | `0.0.0.0`      | Bind address                                        |
| `LOCALGCP_LOG_LEVEL`          | `info`         | Log level (`debug`, `info`, `warning`, …)           |
| `LOCALGCP_GCS_PORT`           | `4443`         | Cloud Storage port                                  |
| `LOCALGCP_PUBSUB_PORT`        | `8085`         | Pub/Sub gRPC port                                   |
| `LOCALGCP_PUBSUB_REST_PORT`   | `8086`         | Pub/Sub REST port                                   |
| `LOCALGCP_FIRESTORE_PORT`     | `8080`         | Firestore port                                      |
| `LOCALGCP_SECRETMANAGER_PORT` | `8090`         | Secret Manager port                                 |
| `LOCALGCP_TASKS_PORT`         | `8123`         | Cloud Tasks port                                    |
| `LOCALGCP_BIGQUERY_PORT`      | `9050`         | BigQuery port                                       |
| `LOCALGCP_SCHEDULER_PORT`     | `8091`         | Cloud Scheduler port                                |
| `LOCALGCP_ADMIN_PORT`         | `8888`         | Admin UI port                                       |

To enable data persistence across restarts, set `LOCALGCP_DATA_DIR` to a writable path (and mount it as a Docker volume if using containers).

## Using the GCP SDK

Point the official `google-cloud-*` SDK clients at LocalGCP using the helpers in `sdk_compat/clients.py`:

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

## `gcloudlocal` CLI

`gcloudlocal` is a `gcloud`-compatible CLI installed as an entry point that targets the LocalGCP emulator. After `uv sync` it is available as:

```bash
uv run gcloudlocal [--project PROJECT] [--location LOCATION] [--format json] \
    SERVICE RESOURCE VERB [ARGS] [FLAGS]
```

Or run it directly via the shim:

```bash
python bin/gcloudlocal.py ...
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

Jobs are stored in memory (or persisted to `LOCALGCP_DATA_DIR`). A background worker polls every 30 seconds and fires HTTP requests via `httpTarget`. Job state transitions (enable/pause/resume) and force-run are supported.

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

This requires LocalGCP to be running and the dev dependencies installed (`uv sync`).

### Helper scripts

The `bin/` directory contains shell scripts for common setup tasks:

- `bin/setup_gcs_pubsub.sh` — create a GCS bucket with a Pub/Sub notification
- `bin/create_pubsub_topic_subscription.sh` — create a topic and subscription
- `bin/print_pubsub_messages.sh` — pull and print messages from a subscription
- `bin/upload_to_gcs.sh` — upload a file to a GCS bucket

## Architecture

All state is stored in memory by default, using a `NamespacedStore` (`localgcp/core/store.py`). Each service is a standalone FastAPI application started concurrently by `localgcp/main.py` via uvicorn. Pub/Sub additionally runs a gRPC server on port 8085 (compatible with `PUBSUB_EMULATOR_HOST`). Cloud Scheduler runs a background asyncio worker that polls every 30 seconds.

Every service module follows the same layout:

```
localgcp/services/<service>/
    app.py      — FastAPI routes
    models.py   — Pydantic v2 models
    store.py    — NamespacedStore wrapper
    worker.py   — (scheduler only) background dispatch loop
```

BigQuery uses an additional `engine.py` module wrapping DuckDB instead of a `NamespacedStore`.

## Roadmap

- Spanner
- Cloud Logging / Monitoring
- Vertex AI
