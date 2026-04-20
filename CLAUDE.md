# Cloudbox — Claude Code Guide

## Project overview

Cloudbox is a local emulator for GCP services (Cloud Storage, Pub/Sub, Firestore, Secret Manager, Cloud Tasks, BigQuery, Cloud Spanner, Cloud Logging, Cloud Scheduler, Cloud KMS). It is written in Python using FastAPI and runs every service as a separate uvicorn server, all started concurrently from a single entry point.

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

All 641 tests should pass. Tests use `pytest-asyncio` with `asyncio_mode = "auto"` (set in `pyproject.toml`). No external services required — each test file creates its own in-process test client.

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
    kms/                    Cloud KMS (port 8092)
      app.py                FastAPI routes
      models.py             Pydantic models
      store.py              NamespacedStore wrapper
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
6. Create `docs/services/{name}.md` — see the **Service documentation** section below.

## Key environment variables

| Variable | Default | Purpose |
|---|---|---|
| `CLOUDBOX_PROJECT` | `local-project` | Default project ID |
| `CLOUDBOX_LOCATION` | `us-central1` | Default region |
| `CLOUDBOX_DATA_DIR` | *(unset)* | Enables file-backed persistence |
| `CLOUDBOX_LOG_LEVEL` | `info` | Log verbosity |
| `CLOUDBOX_KMS_PORT` | `8092` | Cloud KMS port |
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

## Code standards

All Python code in this project must follow these standards. Apply them to every file you create or modify — do not leave new code that violates them.

### Linting and formatting

Run ruff after every change:

```bash
uv run ruff check --fix cloudbox/ tests/   # lint + auto-fix
uv run ruff format cloudbox/ tests/        # format
```

The ruff config is in `pyproject.toml`. Key rules enforced:

| Rule set | What it checks |
|---|---|
| `E` / `W` | PEP 8 style (pycodestyle) |
| `F` | Undefined names, unused imports (pyflakes) |
| `I` | Import sort order (isort) |
| `UP` | Modern Python syntax (pyupgrade) |
| `B` | Common bugs and anti-patterns (flake8-bugbear) |
| `C4` | Idiomatic comprehensions |
| `D` | Docstrings — Google style (pydocstyle) |

Line length is 100. Quote style is double. Never suppress a ruff error without a comment explaining why.

### Type annotations

All function signatures must carry full type annotations — parameters and return types. Use the modern union syntax (`X | Y` not `Union[X, Y]`) and built-in generics (`list[str]` not `List[str]`) as enforced by `pyupgrade`. Always include `from __future__ import annotations` at the top of every module.

```python
# correct
def get_version(secret_name: str, version_id: str) -> str | None:
    ...

# wrong — missing return type, old-style union
def get_version(secret_name, version_id) -> Optional[str]:
    ...
```

### Docstrings

Every public function, method, and class must have a Google-style docstring. Private helpers (`_name`) are optional but encouraged when the logic is non-obvious.

Format:

```python
def create_key_ring(project: str, location: str, key_ring_id: str) -> KeyRingModel:
    """Create a new key ring in the given project and location.

    Args:
        project: GCP project ID.
        location: GCP region (e.g. ``us-central1``).
        key_ring_id: User-supplied key ring identifier.

    Returns:
        The newly created KeyRingModel.

    Raises:
        GCPError: If a key ring with the same name already exists (409).
    """
```

Rules:
- One-line summary on the opening line of the docstring, not on a blank line.
- `Args:`, `Returns:`, and `Raises:` sections use 4-space-indented descriptions.
- Each `Args` entry: `name: Description.` (no type — it's already in the signature).
- `Returns:` describes the return value; omit for `-> None` functions.
- `Raises:` lists only exceptions the function itself raises intentionally.

## Service documentation

Every service has a reference document at `docs/services/{name}.md`. Keep these in sync
whenever you add or change service behaviour.

### When to update docs

- **Adding a new service** — create `docs/services/{name}.md` as step 6 of the service
  checklist above.
- **Adding an endpoint** — add it to the relevant section in the service doc. Include the
  HTTP method + path, the request body shape, and the response shape.
- **Changing behaviour** — update the description, error codes, or field tables that are
  affected. Pay particular attention to the **Known limitations** section: remove an entry
  when a limitation is fixed, add one when you intentionally leave something unimplemented.
- **Changing a port or env var** — update the **Connection** section and the port/env var
  table in the doc.

### Document structure

Each service doc follows this layout (omit sections that don't apply):

```
# {Service Name}

One-sentence description + SDK compatibility note.

## Connection
Port, env var override, SDK client setup snippet.

## {Resource type(s)}
CRUD endpoints: method + path, minimal request/response JSON, error codes.

## {Operation groups}
Non-CRUD operations grouped by purpose.

## {Resource} fields
Table of field name / type / description for the primary resource.

## Known limitations
Table of unimplemented features with a short reason or workaround.

## Examples
Commands to run the examples under examples/{name}/.
```

The docs live in `docs/services/` and are linked from `README.md`. Do not add prose that
duplicates what the code already expresses — focus on the _contract_ (inputs, outputs,
error codes) and _limitations_ (what the emulator does not do that production does).

## Committing changes

After each meaningful change (bug fix, feature, docs update, refactor), generate a git commit. Do not batch unrelated changes into one commit.
