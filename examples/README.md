# Cloudbox Examples

Runnable examples for each Cloudbox service. Each script is self-contained
and targets a locally running Cloudbox instance.

## Prerequisites

Start Cloudbox first:

```bash
uv run cloudbox
# or: docker compose up
```

Then run any example from the repo root:

```bash
uv run python examples/gcs/upload_download.py
```

## Structure

```
examples/
  shared.py                    Base URLs, httpx client, helpers
  gcs/
    upload_download.py         Upload, list, download, delete objects
    compose.py                 Compose multiple objects into one
    byte_range.py              Partial downloads via Range header
    cors.py                    Set, inspect, and clear bucket CORS rules
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
  secretmanager/
    secrets.py                 Create secret, add versions, access, disable
  tasks/
    tasks.py                   Create queue, enqueue tasks, list, delete
  scheduler/
    jobs.py                    Create, pause, resume, delete cron jobs
```

## Configuration

All examples read the same environment variables as Cloudbox itself:

| Variable | Default |
|---|---|
| `CLOUDBOX_HOST` | `localhost` |
| `CLOUDBOX_PROJECT` | `local-project` |
| `CLOUDBOX_LOCATION` | `us-central1` |
| `CLOUDBOX_GCS_PORT` | `4443` |
| `CLOUDBOX_PUBSUB_REST_PORT` | `8086` |
| `CLOUDBOX_FIRESTORE_PORT` | `8080` |
| `CLOUDBOX_SECRETMANAGER_PORT` | `8090` |
| `CLOUDBOX_TASKS_PORT` | `8123` |
| `CLOUDBOX_BIGQUERY_PORT` | `9050` |
| `CLOUDBOX_SCHEDULER_PORT` | `8091` |
