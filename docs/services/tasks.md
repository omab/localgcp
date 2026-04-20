# Cloud Tasks

Cloudbox emulates the Cloud Tasks REST API (v2). The `google-cloud-tasks` Python SDK works
against it without modification.

## Connection

**Port:** `8123` (override with `CLOUDBOX_TASKS_PORT`)

```python
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import tasks_v2

client = tasks_v2.CloudTasksClient(
    credentials=AnonymousCredentials(),
    client_options=ClientOptions(api_endpoint="http://localhost:8123"),
)
```

---

## Queues

### Create queue

```
POST /v2/projects/{project}/locations/{location}/queues
```

```json
{
  "name": "projects/local-project/locations/us-central1/queues/my-queue",
  "rateLimits": {
    "maxDispatchesPerSecond": 500,
    "maxConcurrentDispatches": 1000,
    "maxBurstSize": 100
  },
  "retryConfig": {
    "maxAttempts": 3,
    "maxRetryDuration": "3600s",
    "minBackoff": "0.1s",
    "maxBackoff": "3600s",
    "maxDoublings": 16
  }
}
```

`name` may be omitted — a random queue ID is generated. Returns the queue resource.
`409` if the queue already exists.

### Get queue

```
GET /v2/projects/{project}/locations/{location}/queues/{queue_id}
```

Returns the queue resource. `404` if not found.

### List queues

```
GET /v2/projects/{project}/locations/{location}/queues?pageSize=100&pageToken=
```

Returns `{ "queues": [...], "nextPageToken": "..." }`.

### Update queue

```
PATCH /v2/projects/{project}/locations/{location}/queues/{queue_id}
```

```json
{
  "rateLimits": { "maxDispatchesPerSecond": 100 },
  "retryConfig": { "maxAttempts": 5 }
}
```

Merges `rateLimits` and `retryConfig` fields. Returns the updated queue resource.

### Delete queue

```
DELETE /v2/projects/{project}/locations/{location}/queues/{queue_id}
```

Deletes the queue and all its tasks. Returns `{}`.

### Pause queue

```
POST /v2/projects/{project}/locations/{location}/queues/{queue_id}:pause
```

Sets queue state to `PAUSED` — new task dispatches are suppressed. Returns the updated
queue resource.

### Resume queue

```
POST /v2/projects/{project}/locations/{location}/queues/{queue_id}:resume
```

Sets queue state back to `RUNNING`. Returns the updated queue resource.

### Purge queue

```
POST /v2/projects/{project}/locations/{location}/queues/{queue_id}:purge
```

Deletes all tasks in the queue without deleting the queue itself. Returns the queue
resource (now empty).

---

## Tasks

### Create task

```
POST /v2/projects/{project}/locations/{location}/queues/{queue_id}/tasks
```

```json
{
  "task": {
    "name": "projects/local-project/locations/us-central1/queues/my-queue/tasks/my-task",
    "httpRequest": {
      "url": "http://localhost:8080/worker",
      "httpMethod": "POST",
      "headers": { "Content-Type": "application/json" },
      "body": "eyJrZXkiOiAidmFsdWUifQ=="
    },
    "scheduleTime": "2024-01-01T00:05:00Z"
  }
}
```

`task.name` is optional — a random task ID is generated if omitted. `httpRequest.body`
must be base64-encoded. Returns the created task resource. `409` if a task with the same
name already exists.

### Get task

```
GET /v2/projects/{project}/locations/{location}/queues/{queue_id}/tasks/{task_id}
```

Returns the task resource. `404` if not found.

### List tasks

```
GET /v2/projects/{project}/locations/{location}/queues/{queue_id}/tasks
    ?pageSize=1000&pageToken=&responseView=BASIC
```

`responseView` can be `BASIC` (default) or `FULL`. Tasks are ordered by `scheduleTime`.
Returns `{ "tasks": [...], "nextPageToken": "..." }`.

### Delete task

```
DELETE /v2/projects/{project}/locations/{location}/queues/{queue_id}/tasks/{task_id}
```

Returns `{}`. `404` if not found.

### Force-run task

```
POST /v2/projects/{project}/locations/{location}/queues/{queue_id}/tasks/{task_id}:run
```

Resets the task's `scheduleTime` to now, making it immediately eligible for dispatch.
Returns the updated task resource.

> **Note:** Cloudbox does not include a background dispatcher. Tasks are stored but not
> automatically dispatched to their `httpRequest.url`. Use `:run` to trigger dispatch in
> tests, or read the task from the queue and dispatch manually.

---

## Task resource fields

| Field | Type | Description |
|---|---|---|
| `name` | string | Full resource name |
| `httpRequest` | object | HTTP target configuration |
| `scheduleTime` | string | RFC 3339 time at which the task should be dispatched |
| `createTime` | string | RFC 3339 creation timestamp |
| `dispatchCount` | int | Number of dispatch attempts |
| `responseCount` | int | Number of responses received |
| `firstAttempt` | object | Metadata from the first dispatch attempt |
| `lastAttempt` | object | Metadata from the most recent dispatch attempt |

### HTTP request fields

| Field | Type | Description |
|---|---|---|
| `url` | string | Target URL |
| `httpMethod` | string | `POST`, `GET`, `PUT`, `DELETE`, etc. (default: `POST`) |
| `headers` | object | HTTP headers to include |
| `body` | string | Base64-encoded request body |
| `oidcToken` | object | OIDC token configuration (stored, not enforced) |
| `oauthToken` | object | OAuth token configuration (stored, not enforced) |

---

## Queue resource fields

| Field | Type | Description |
|---|---|---|
| `name` | string | Full resource name |
| `state` | string | `RUNNING`, `PAUSED`, or `DISABLED` |
| `rateLimits` | object | `maxDispatchesPerSecond`, `maxConcurrentDispatches`, `maxBurstSize` |
| `retryConfig` | object | `maxAttempts`, `maxRetryDuration`, `minBackoff`, `maxBackoff`, `maxDoublings` |

---

## Known limitations

| Feature | Notes |
|---|---|
| Automatic task dispatch | Tasks are not dispatched automatically; use `:run` or pull manually |
| OIDC / OAuth token injection | Auth tokens accepted and stored but not attached to dispatch requests |
| App Engine HTTP tasks | `appEngineHttpRequest` accepted but not dispatched |
| Content-based deduplication | Only exact name collisions are rejected — body-hash deduplication is not implemented |
| Rate limiting | `rateLimits` stored and returned but not enforced |

---

## Examples

```bash
uv run python examples/tasks/enqueue.py
```
