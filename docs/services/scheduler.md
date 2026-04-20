# Cloud Scheduler

Cloudbox emulates the Cloud Scheduler REST API (v1). The `google-cloud-scheduler` Python
SDK works against it without modification.

## Connection

**Port:** `8091` (override with `CLOUDBOX_SCHEDULER_PORT`)

```python
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import scheduler_v1

client = scheduler_v1.CloudSchedulerClient(
    credentials=AnonymousCredentials(),
    client_options=ClientOptions(api_endpoint="http://localhost:8091"),
)
```

---

## Jobs

### Create job

```
POST /v1/projects/{project}/locations/{location}/jobs
```

```json
{
  "name": "projects/local-project/locations/us-central1/jobs/my-job",
  "description": "Runs every 5 minutes",
  "schedule": "*/5 * * * *",
  "timeZone": "UTC",
  "httpTarget": {
    "uri": "http://localhost:8080/cron/task",
    "httpMethod": "POST",
    "headers": { "Content-Type": "application/json" },
    "body": "e30="
  },
  "retryConfig": {
    "retryCount": 3,
    "maxRetryDuration": "3600s",
    "minBackoffDuration": "5s",
    "maxBackoffDuration": "3600s",
    "maxDoublings": 5
  }
}
```

`name` is required. `schedule` is a standard Unix cron expression (5 or 6 fields, via
`croniter`). `httpTarget.body` must be base64-encoded. Returns the job resource with
`state: ENABLED` and a computed `scheduleTime`.

`409` if the job already exists.

### Get job

```
GET /v1/projects/{project}/locations/{location}/jobs/{job_id}
```

Returns the job resource. `404` if not found.

### List jobs

```
GET /v1/projects/{project}/locations/{location}/jobs?pageSize=100&pageToken=
```

Returns `{ "jobs": [...], "nextPageToken": "..." }`.

### Update job

```
PATCH /v1/projects/{project}/locations/{location}/jobs/{job_id}
```

Updates mutable fields: `description`, `schedule`, `timeZone`, `httpTarget`,
`retryConfig`. Updating `schedule` recomputes `scheduleTime`. Returns the updated resource.

### Delete job

```
DELETE /v1/projects/{project}/locations/{location}/jobs/{job_id}
```

`204` on success. `404` if not found.

---

## Job actions

### Force-run

```
POST /v1/projects/{project}/locations/{location}/jobs/{job_id}:run
```

Dispatches the job immediately, regardless of its cron schedule. Makes an HTTP POST to
`httpTarget.uri` synchronously within the request. Updates `lastAttemptTime` and `status`.

Returns the updated job resource. If the HTTP call fails, `status.code` is set to `2` and
`status.message` contains the error.

### Pause

```
POST /v1/projects/{project}/locations/{location}/jobs/{job_id}:pause
```

Sets `state: PAUSED` — the background worker stops dispatching this job. Returns the
updated job resource.

### Resume

```
POST /v1/projects/{project}/locations/{location}/jobs/{job_id}:resume
```

Sets `state: ENABLED` — the background worker resumes dispatching. Returns the updated
job resource.

---

## Background scheduler

Cloudbox runs a background poll loop that dispatches due jobs:

- **Poll interval:** 30 seconds.
- **Due check:** a job is due if `croniter` computes its next fire time (after
  `lastAttemptTime`) as ≤ now. Jobs that have never run are always due on the first poll.
- **Dispatch:** an HTTP POST is sent to `httpTarget.uri` with the configured headers and
  body. A `2xx` response marks success; any other status or a network error is recorded
  in `status`.

Only jobs with `state: ENABLED` and a non-empty `httpTarget` are dispatched.

---

## Job resource fields

| Field | Type | Description |
|---|---|---|
| `name` | string | Full resource name |
| `description` | string | Human-readable description |
| `schedule` | string | Cron expression (`"*/5 * * * *"`, `"0 9 * * 1-5"`, etc.) |
| `timeZone` | string | IANA timezone name (default: `"UTC"`) |
| `state` | string | `ENABLED`, `PAUSED`, or `DISABLED` |
| `httpTarget` | object | HTTP dispatch configuration |
| `retryConfig` | object | Retry policy for failed dispatches |
| `scheduleTime` | string | RFC 3339 time of the next scheduled execution |
| `lastAttemptTime` | string | RFC 3339 time of the most recent dispatch attempt |
| `userUpdateTime` | string | RFC 3339 time of the last user update |
| `status` | object | Status of the last dispatch attempt |

### HTTP target fields

| Field | Type | Description |
|---|---|---|
| `uri` | string | Target URL |
| `httpMethod` | string | `GET`, `POST`, `PUT`, `DELETE`, etc. (default: `POST`) |
| `headers` | object | HTTP headers |
| `body` | string | Base64-encoded request body |
| `oidcToken` | object | OIDC token config (stored, not enforced) |
| `oauthToken` | object | OAuth token config (stored, not enforced) |

---

## Cron expression reference

| Expression | Meaning |
|---|---|
| `* * * * *` | Every minute |
| `*/5 * * * *` | Every 5 minutes |
| `0 * * * *` | Every hour |
| `0 9 * * 1-5` | Weekdays at 09:00 |
| `0 0 1 * *` | First day of every month |
| `0 0 * * 0` | Every Sunday at midnight |

---

## Testing scheduler jobs

In tests, patch the internal `_dispatch` function to avoid real HTTP calls:

```python
from unittest.mock import AsyncMock, patch

with patch("cloudbox.services.scheduler.worker._dispatch", new_callable=AsyncMock):
    response = client.post(".../my-job:run")
```

This is necessary because `httpx` is imported inside the route handler, so patching
`httpx.AsyncClient` directly does not intercept the call.

---

## Known limitations

| Feature | Notes |
|---|---|
| Pub/Sub target | Only `httpTarget` is supported; `pubsubTarget` is not dispatched |
| App Engine target | `appEngineHttpTarget` is stored but not dispatched |
| OIDC / OAuth token injection | Auth tokens accepted and stored but not attached to dispatch requests |
| Sub-minute scheduling | The 30-second poll interval limits effective resolution to ~1 minute |

---

## Examples

```bash
uv run python examples/scheduler/basic.py
```
