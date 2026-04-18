# Cloudbox — Claude Code Guide

## Project overview

Cloudbox is a local emulator for GCP services (Cloud Storage, Pub/Sub, Firestore, Secret Manager, Cloud Tasks, BigQuery, Cloud Spanner, Cloud Logging, Cloud Scheduler). It is written in Python using FastAPI and runs every service as a separate uvicorn server, all started concurrently from a single entry point.

**Stack:** Python 3.12+, FastAPI, uvicorn, Pydantic v2, grpcio, DuckDB, croniter, uv (package manager)

## Running locally

```bash
uv sync           # install all deps (including dev)
uv run cloudbox   # start all services
```

Or via Docker:

```bash
docker compose up
```

## Running tests

```bash
uv run pytest tests/
```

All 617 tests should pass. Tests use `pytest-asyncio` with `asyncio_mode = "auto"` (set in `pyproject.toml`). No external services required — each test file creates its own in-process test client.

## Project layout

```
cloudbox/
  main.py                   Entry point — spawns all service servers concurrently
  config.py                 Settings dataclass, reads env vars
  gcloudlocal.py            gcloud-compatible CLI (installed as 'gcloudlocal' entry point)
  gsutillocal.py            gsutil-compatible CLI for Cloud Storage (installed as 'gsutillocal')
  core/
    store.py                NamespacedStore — shared in-memory (or file-backed) state
    auth.py                 Auth middleware (no-op for local dev)
    errors.py               Common HTTP error helpers
    middleware.py           Request middleware
  services/
    gcs/                    Cloud Storage (port 4443)
    pubsub/                 Pub/Sub gRPC (8085) + REST (8086)
    firestore/              Firestore (port 8080)
    secretmanager/          Secret Manager (port 8090)
    tasks/                  Cloud Tasks (port 8123)
    bigquery/               BigQuery via DuckDB (port 9050)
      app.py                FastAPI routes
      models.py             Pydantic models
      engine.py             BigQueryEngine wrapping DuckDB (no NamespacedStore)
    spanner/                Cloud Spanner via DuckDB (port 9010)
      app.py                FastAPI routes
      engine.py             SpannerEngine wrapping DuckDB (no NamespacedStore)
    logging/                Cloud Logging + Cloud Monitoring (port 9020)
      app.py                FastAPI routes (v2 Logging + v3 Monitoring)
      store.py              NamespacedStore wrapper
    scheduler/              Cloud Scheduler (port 8091)
      app.py                FastAPI routes
      models.py             Pydantic models
      store.py              NamespacedStore wrapper
      worker.py             Background asyncio dispatch loop
  admin/                    Admin UI (port 8888)

tests/                      One file per service (test_gcs.py, test_pubsub.py, …)
sdk_compat/
  clients.py                Pre-configured GCP SDK client factories
  test_with_sdk.py          Live smoke tests (requires a running instance)
bin/
  gcloudlocal.py            Legacy shim (unused; kept for backwards compat)
  *.sh                      Shell helper scripts
gcloud                      Wrapper script: uv run python cloudbox/gcloudlocal.py "$@"
gsutil                      Wrapper script: uv run python cloudbox/gsutillocal.py "$@"
```

## Service pattern

Every service (except BigQuery) follows the same three-file layout:

```
cloudbox/services/<name>/
    app.py      FastAPI application with all routes
    models.py   Pydantic v2 request/response models
    store.py    Thin wrapper around NamespacedStore
```

BigQuery and Cloud Spanner use an `engine.py` module wrapping DuckDB instead of a `NamespacedStore`. Cloud Scheduler adds a `worker.py` for the background dispatch loop. Cloud Logging uses a standard `NamespacedStore` (entries are append-only structured records).

When adding a new service:
1. Create the directory and three files above.
2. Register the service in `cloudbox/main.py` by adding it to `_SERVICES` and `apps` in `_build_configs()`.
3. Add a port setting to `cloudbox/config.py` and `docker-compose.yml`.
4. Add a test file under `tests/`.
5. Add an admin UI tab in `cloudbox/admin/app.py` (stats, panel HTML, JS loader, API endpoints, reset wiring).

## Key environment variables

| Variable | Default | Purpose |
|---|---|---|
| `CLOUDBOX_PROJECT` | `local-project` | Default project ID |
| `CLOUDBOX_LOCATION` | `us-central1` | Default region |
| `CLOUDBOX_DATA_DIR` | *(unset)* | Enables file-backed persistence |
| `CLOUDBOX_LOG_LEVEL` | `info` | Log verbosity |
| Port variables (`CLOUDBOX_*_PORT`) | see config.py | Per-service port overrides |

## Pub/Sub transport notes

Pub/Sub is the only service with two endpoints:
- **Port 8085** — gRPC server, compatible with `PUBSUB_EMULATOR_HOST=localhost:8085`
- **Port 8086** — HTTP/1.1 REST server, for `transport="rest"` SDK clients

Use `sdk_compat/clients.py` helpers to get correctly-configured SDK clients without manual setup.

## BigQuery / DuckDB notes

BigQuery is backed by DuckDB. Datasets map to DuckDB schemas; tables map to DuckDB tables.

SQL is rewritten at query time by `_rewrite_sql()` in `engine.py`:
- Backtick identifiers (`project.dataset.table`) → double-quoted DuckDB form
- 3-part, 2-part, and single identifiers are handled

DML result detection: after INSERT/UPDATE/DELETE, DuckDB returns a single-column `Count` result. `_is_select()` distinguishes SELECT-like queries from DML; DML results are reported as `numDmlAffectedRows`.

## Cloud Scheduler notes

The background worker (`cloudbox/services/scheduler/worker.py`) polls every 30 seconds. `_is_due()` uses `croniter` to check whether the next cron occurrence after `lastAttemptTime` is ≤ now. Jobs that have never run are always due on the first poll.

When testing scheduler routes that call `_dispatch`, patch `cloudbox.services.scheduler.worker._dispatch` (not `app.httpx`) because httpx is imported locally inside `run_job` in `app.py`:

```python
with patch("cloudbox.services.scheduler.worker._dispatch", new_callable=AsyncMock):
    r = scheduler_client.post(f"{BASE}/my-job:run")
```

## Entry points

Three CLI entry points are installed after `uv sync`:

- `cloudbox` → `cloudbox.main:main` — starts all emulator services
- `gcloudlocal` → `cloudbox.gcloudlocal:main` — gcloud-compatible CLI
- `gsutillocal` → `cloudbox.gsutillocal:main` — gsutil-compatible CLI for Cloud Storage

Packaging is configured in `pyproject.toml` with `[tool.uv] package = true` and hatchling as the build backend.

## Persistence

By default all state is in-memory and lost on restart. Set `CLOUDBOX_DATA_DIR` to a directory path to enable JSON file persistence. The `NamespacedStore` in `cloudbox/core/store.py` handles both modes transparently. Writes are atomic (write to `.tmp`, then `Path.replace()`).

## Committing changes

After each meaningful change (bug fix, feature, docs update, refactor), generate a git commit. Do not batch unrelated changes into one commit.
