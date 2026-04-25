"""Cloud Tasks emulator.

Implements the Cloud Tasks REST API v2 used by google-cloud-tasks.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Query, Request

from cloudbox.core.errors import GCPError, add_gcp_exception_handler
from cloudbox.core.middleware import add_request_logging
from cloudbox.services.tasks.models import (
    CreateTaskRequest,
    HttpRequest,
    ListQueuesResponse,
    ListTasksResponse,
    QueueModel,
    QueueState,
    TaskModel,
    _now,
)
from cloudbox.services.tasks.store import get_store
from cloudbox.services.tasks.worker import dispatch_loop


@asynccontextmanager
async def _lifespan(app: FastAPI):
    """Start the background dispatch loop on startup and cancel it on shutdown.

    Args:
        app (FastAPI): The FastAPI application instance.
    """
    task = asyncio.create_task(dispatch_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="Cloudbox — Cloud Tasks", version="v2", lifespan=_lifespan)
add_gcp_exception_handler(app)
add_request_logging(app, "tasks")


_DEDUP_WINDOW_S = 3600  # 1 hour, matching Cloud Tasks' named-task dedup window


def _store():
    """Return the Cloud Tasks store instance.

    Returns:
        NamespacedStore: The shared Cloud Tasks store.
    """
    return get_store()


def _task_body_hash(task_data: dict) -> str:
    """Compute a stable SHA-256 hash of a task's HTTP request for content-based dedup.

    Args:
        task_data (dict): Raw task dict from the create request body.

    Returns:
        str: Hex SHA-256 digest of the task's url, method, and body fields.
    """
    http_req = task_data.get("httpRequest", {})
    key = json.dumps(
        {
            "url": http_req.get("url", ""),
            "httpMethod": http_req.get("httpMethod", "POST"),
            "body": http_req.get("body", ""),
        },
        sort_keys=True,
    )
    return hashlib.sha256(key.encode()).hexdigest()


def _check_dedup(store, dedup_key: str) -> None:
    """Raise GCPError(409) if a live dedup entry exists for dedup_key.

    Args:
        store: The Cloud Tasks NamespacedStore.
        dedup_key (str): Key to look up in the "dedup" namespace.

    Raises:
        GCPError: 409 if the dedup window is still active for this key.
    """
    entry = store.get("dedup", dedup_key)
    if entry and entry.get("expiresAt", 0) > time.time():
        raise GCPError(409, f"Duplicate task rejected (dedup window active): {dedup_key}")


def _set_dedup(store, dedup_key: str) -> None:
    """Record a dedup entry that expires after _DEDUP_WINDOW_S seconds.

    Args:
        store: The Cloud Tasks NamespacedStore.
        dedup_key (str): Key to store in the "dedup" namespace.
    """
    store.set("dedup", dedup_key, {"expiresAt": time.time() + _DEDUP_WINDOW_S})


# ---------------------------------------------------------------------------
# Queues
# ---------------------------------------------------------------------------


@app.post("/v2/projects/{project}/locations/{location}/queues")
async def create_queue(project: str, location: str, request: Request):
    """Create a new Cloud Tasks queue.

    Args:
        project (str): GCP project ID.
        location (str): GCP region or zone.
        request (Request): HTTP request body with optional name, rateLimits, retryConfig.

    Returns:
        dict: The newly created QueueModel dict.

    Raises:
        GCPError: If the queue already exists (409).
    """
    body = await request.json()
    name = (
        body.get("name") or f"projects/{project}/locations/{location}/queues/{uuid.uuid4().hex[:8]}"
    )
    store = _store()
    if store.exists("queues", name):
        raise GCPError(409, f"Queue {name} already exists.")
    queue = QueueModel(name=name, **{k: v for k, v in body.items() if k != "name"})
    store.set("queues", name, queue.model_dump())
    return queue.model_dump()


@app.get("/v2/projects/{project}/locations/{location}/queues")
async def list_queues(
    project: str,
    location: str,
    pageSize: int = Query(default=100),
    pageToken: str = Query(default=""),
):
    """List Cloud Tasks queues for a project and location.

    Args:
        project (str): GCP project ID.
        location (str): GCP region or zone.
        pageSize (int): Maximum number of queues to return per page.
        pageToken (str): Pagination token from a previous response.

    Returns:
        dict: ListQueuesResponse with queues and optional nextPageToken.
    """
    store = _store()
    prefix = f"projects/{project}/locations/{location}/queues/"
    all_queues = [QueueModel(**v) for v in store.list("queues") if v["name"].startswith(prefix)]
    all_queues.sort(key=lambda q: q.name)
    offset = int(pageToken) if pageToken else 0
    page = all_queues[offset : offset + pageSize]
    next_token = str(offset + pageSize) if offset + pageSize < len(all_queues) else None
    return ListQueuesResponse(queues=page, nextPageToken=next_token).model_dump(exclude_none=True)


@app.get("/v2/projects/{project}/locations/{location}/queues/{queue_id}")
async def get_queue(project: str, location: str, queue_id: str):
    """Get a Cloud Tasks queue by ID.

    Args:
        project (str): GCP project ID.
        location (str): GCP region or zone.
        queue_id (str): Queue resource ID.

    Returns:
        dict: The QueueModel dict for the requested queue.

    Raises:
        GCPError: If the queue does not exist (404).
    """
    name = f"projects/{project}/locations/{location}/queues/{queue_id}"
    store = _store()
    data = store.get("queues", name)
    if data is None:
        raise GCPError(404, f"Queue {name} not found.")
    return data


@app.patch("/v2/projects/{project}/locations/{location}/queues/{queue_id}")
async def update_queue(project: str, location: str, queue_id: str, request: Request):
    """Update rate limits or retry config of a Cloud Tasks queue.

    Args:
        project (str): GCP project ID.
        location (str): GCP region or zone.
        queue_id (str): Queue resource ID.
        request (Request): HTTP request body with rateLimits and/or retryConfig fields.

    Returns:
        dict: The updated QueueModel dict.

    Raises:
        GCPError: If the queue does not exist (404).
    """
    name = f"projects/{project}/locations/{location}/queues/{queue_id}"
    store = _store()
    data = store.get("queues", name)
    if data is None:
        raise GCPError(404, f"Queue {name} not found.")
    body = await request.json()
    for field in ("rateLimits", "retryConfig"):
        if field in body:
            data[field] = {**data.get(field, {}), **body[field]}
    store.set("queues", name, data)
    return data


@app.delete("/v2/projects/{project}/locations/{location}/queues/{queue_id}", status_code=200)
async def delete_queue(project: str, location: str, queue_id: str):
    """Delete a Cloud Tasks queue and all its tasks.

    Args:
        project (str): GCP project ID.
        location (str): GCP region or zone.
        queue_id (str): Queue resource ID.

    Returns:
        dict: Empty dict on success.

    Raises:
        GCPError: If the queue does not exist (404).
    """
    name = f"projects/{project}/locations/{location}/queues/{queue_id}"
    store = _store()
    if not store.exists("queues", name):
        raise GCPError(404, f"Queue {name} not found.")
    store.delete("queues", name)
    # Delete all tasks in the queue
    for k in list(store.keys("tasks")):
        if k.startswith(f"{name}/tasks/"):
            store.delete("tasks", k)
    return {}


@app.post("/v2/projects/{project}/locations/{location}/queues/{queue_id}:pause")
async def pause_queue(project: str, location: str, queue_id: str):
    """Pause a queue, preventing new task dispatches.

    Args:
        project (str): GCP project ID.
        location (str): GCP region or zone.
        queue_id (str): Queue resource ID.

    Returns:
        dict: The updated QueueModel dict with state PAUSED.

    Raises:
        GCPError: If the queue does not exist (404).
    """
    return _set_queue_state(project, location, queue_id, QueueState.PAUSED)


@app.post("/v2/projects/{project}/locations/{location}/queues/{queue_id}:resume")
async def resume_queue(project: str, location: str, queue_id: str):
    """Resume a paused queue.

    Args:
        project (str): GCP project ID.
        location (str): GCP region or zone.
        queue_id (str): Queue resource ID.

    Returns:
        dict: The updated QueueModel dict with state RUNNING.

    Raises:
        GCPError: If the queue does not exist (404).
    """
    return _set_queue_state(project, location, queue_id, QueueState.RUNNING)


@app.post("/v2/projects/{project}/locations/{location}/queues/{queue_id}:purge")
async def purge_queue(project: str, location: str, queue_id: str):
    """Delete all tasks in a queue without deleting the queue itself.

    Args:
        project (str): GCP project ID.
        location (str): GCP region or zone.
        queue_id (str): Queue resource ID.

    Returns:
        dict: The QueueModel dict for the purged queue.

    Raises:
        GCPError: If the queue does not exist (404).
    """
    name = f"projects/{project}/locations/{location}/queues/{queue_id}"
    store = _store()
    if not store.exists("queues", name):
        raise GCPError(404, f"Queue {name} not found.")
    for k in list(store.keys("tasks")):
        if k.startswith(f"{name}/tasks/"):
            store.delete("tasks", k)
    return store.get("queues", name)


def _set_queue_state(project: str, location: str, queue_id: str, state: str):
    """Set the state of a queue and persist it.

    Args:
        project (str): GCP project ID.
        location (str): GCP region or zone.
        queue_id (str): Queue resource ID.
        state (str): Target state from QueueState constants.

    Returns:
        dict: The updated QueueModel dict.

    Raises:
        GCPError: If the queue does not exist (404).
    """
    name = f"projects/{project}/locations/{location}/queues/{queue_id}"
    store = _store()
    data = store.get("queues", name)
    if data is None:
        raise GCPError(404, f"Queue {name} not found.")
    data["state"] = state
    store.set("queues", name, data)
    return data


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@app.post("/v2/projects/{project}/locations/{location}/queues/{queue_id}/tasks")
async def create_task(project: str, location: str, queue_id: str, body: CreateTaskRequest):
    """Create a new task in a queue.

    Args:
        project (str): GCP project ID.
        location (str): GCP region or zone.
        queue_id (str): Queue resource ID.
        body (CreateTaskRequest): Request body with task definition and optional responseView.

    Returns:
        dict: The newly created TaskModel dict.

    Raises:
        GCPError: If the queue does not exist (404) or the task already exists (409).
    """
    queue_name = f"projects/{project}/locations/{location}/queues/{queue_id}"
    store = _store()
    if not store.exists("queues", queue_name):
        raise GCPError(404, f"Queue {queue_name} not found.")

    task_data = body.task
    explicit_name = task_data.get("name", "").split("/tasks/")[-1]
    is_named = bool(explicit_name)
    task_id = explicit_name or uuid.uuid4().hex
    task_name = f"{queue_name}/tasks/{task_id}"

    if store.exists("tasks", task_name):
        raise GCPError(409, f"Task {task_name} already exists.")

    # Dedup: named tasks use a tombstone key; un-named tasks use a body hash.
    dedup_key = task_name if is_named else f"body:{_task_body_hash(task_data)}"
    _check_dedup(store, dedup_key)

    http_req = task_data.get("httpRequest")
    task = TaskModel(
        name=task_name,
        httpRequest=HttpRequest(**http_req) if http_req else None,
        scheduleTime=task_data.get("scheduleTime", _now()),
    )
    store.set("tasks", task_name, task.model_dump())
    _set_dedup(store, dedup_key)
    return task.model_dump()


@app.get("/v2/projects/{project}/locations/{location}/queues/{queue_id}/tasks")
async def list_tasks(
    project: str,
    location: str,
    queue_id: str,
    pageSize: int = Query(default=1000),
    pageToken: str = Query(default=""),
    responseView: str = Query(default="BASIC"),
):
    """List tasks in a queue.

    Args:
        project (str): GCP project ID.
        location (str): GCP region or zone.
        queue_id (str): Queue resource ID.
        pageSize (int): Maximum number of tasks to return per page.
        pageToken (str): Pagination token from a previous response.
        responseView (str): Level of detail to include ('BASIC' or 'FULL').

    Returns:
        dict: ListTasksResponse with tasks and optional nextPageToken.

    Raises:
        GCPError: If the queue does not exist (404).
    """
    queue_name = f"projects/{project}/locations/{location}/queues/{queue_id}"
    store = _store()
    if not store.exists("queues", queue_name):
        raise GCPError(404, f"Queue {queue_name} not found.")

    prefix = f"{queue_name}/tasks/"
    all_tasks = [TaskModel(**v) for v in store.list("tasks") if v["name"].startswith(prefix)]
    all_tasks.sort(key=lambda t: t.scheduleTime)

    offset = int(pageToken) if pageToken else 0
    page = all_tasks[offset : offset + pageSize]
    next_token = str(offset + pageSize) if offset + pageSize < len(all_tasks) else None
    return ListTasksResponse(tasks=page, nextPageToken=next_token).model_dump(exclude_none=True)


@app.get("/v2/projects/{project}/locations/{location}/queues/{queue_id}/tasks/{task_id}")
async def get_task(project: str, location: str, queue_id: str, task_id: str):
    """Get a task by ID.

    Args:
        project (str): GCP project ID.
        location (str): GCP region or zone.
        queue_id (str): Queue resource ID.
        task_id (str): Task resource ID.

    Returns:
        dict: The TaskModel dict for the requested task.

    Raises:
        GCPError: If the task does not exist (404).
    """
    task_name = f"projects/{project}/locations/{location}/queues/{queue_id}/tasks/{task_id}"
    store = _store()
    data = store.get("tasks", task_name)
    if data is None:
        raise GCPError(404, f"Task {task_name} not found.")
    return data


@app.delete(
    "/v2/projects/{project}/locations/{location}/queues/{queue_id}/tasks/{task_id}", status_code=200
)
async def delete_task(project: str, location: str, queue_id: str, task_id: str):
    """Delete a task from a queue.

    Args:
        project (str): GCP project ID.
        location (str): GCP region or zone.
        queue_id (str): Queue resource ID.
        task_id (str): Task resource ID.

    Returns:
        dict: Empty dict on success.

    Raises:
        GCPError: If the task does not exist (404).
    """
    task_name = f"projects/{project}/locations/{location}/queues/{queue_id}/tasks/{task_id}"
    store = _store()
    if not store.exists("tasks", task_name):
        raise GCPError(404, f"Task {task_name} not found.")
    store.delete("tasks", task_name)
    # Keep a tombstone so the same name cannot be reused within the dedup window.
    _set_dedup(store, task_name)
    return {}


@app.post("/v2/projects/{project}/locations/{location}/queues/{queue_id}/tasks/{task_id}:run")
async def run_task(project: str, location: str, queue_id: str, task_id: str):
    """Force-dispatch a task immediately, ignoring its scheduleTime.

    Args:
        project (str): GCP project ID.
        location (str): GCP region or zone.
        queue_id (str): Queue resource ID.
        task_id (str): Task resource ID.

    Returns:
        dict: The updated TaskModel dict with scheduleTime reset to now.

    Raises:
        GCPError: If the task does not exist (404).
    """
    task_name = f"projects/{project}/locations/{location}/queues/{queue_id}/tasks/{task_id}"
    store = _store()
    data = store.get("tasks", task_name)
    if data is None:
        raise GCPError(404, f"Task {task_name} not found.")
    # Reset scheduleTime to now so worker picks it up immediately
    data["scheduleTime"] = _now()
    store.set("tasks", task_name, data)
    return data
