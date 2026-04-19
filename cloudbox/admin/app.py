"""Admin UI for Cloudbox.

Provides a web dashboard showing the state of all emulated services,
allowing data to be browsed, interacted with, and reset.
"""

from __future__ import annotations

import base64
from datetime import UTC
from pathlib import Path

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from cloudbox.config import settings
from cloudbox.core.middleware import add_request_logging

app = FastAPI(title="Cloudbox Admin", version="v1")
add_request_logging(app, "admin")

_STATIC = Path(__file__).parent / "static"
if _STATIC.exists():
    app.mount("/static", StaticFiles(directory=str(_STATIC)), name="static")

_TEMPLATES = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


# ---------------------------------------------------------------------------
# Stats helper (used by overview + /api/stats)
# ---------------------------------------------------------------------------


def _get_services() -> dict:
    """Collect stats and metadata for every registered service.

    Returns:
        dict: Mapping of service name to port, stats, and docs URL.
    """
    from cloudbox.services.bigquery.engine import get_engine as bq_get_engine
    from cloudbox.services.firestore.store import get_store as firestore_store
    from cloudbox.services.gcs.store import get_store as gcs_store
    from cloudbox.services.pubsub.store import get_store as pubsub_store
    from cloudbox.services.secretmanager.store import get_store as sm_store
    from cloudbox.services.tasks.store import get_store as tasks_store

    bq = bq_get_engine()
    bq_datasets = bq.list_datasets(settings.default_project)
    bq_table_count = sum(
        len(bq.list_tables(settings.default_project, d["datasetReference"]["datasetId"]))
        for d in bq_datasets
    )

    from cloudbox.services.logging.store import get_store as logging_store

    ls = logging_store()
    log_entry_count = len(ls.list("entries"))
    log_sink_count = len(ls.list("sinks"))

    from cloudbox.services.spanner.engine import get_engine as spanner_get_engine

    sp = spanner_get_engine()
    sp_instances = sp.list_instances(settings.default_project)
    sp_db_count = sum(
        len(sp.list_databases(settings.default_project, inst["name"].rsplit("/", 1)[-1]))
        for inst in sp_instances
    )

    from cloudbox.services.scheduler.store import get_store as scheduler_store

    sched = scheduler_store()
    sched_jobs = sched.list("jobs")
    sched_enabled = sum(1 for j in sched_jobs if j.get("state") == "ENABLED")
    sched_paused = sum(1 for j in sched_jobs if j.get("state") == "PAUSED")

    return {
        "gcs": {
            "port": settings.gcs_port,
            "stats": gcs_store().stats(),
            "docs_url": f"http://localhost:{settings.gcs_port}/docs",
        },
        "pubsub": {
            "port": settings.pubsub_rest_port,
            "stats": pubsub_store().stats(),
            "docs_url": f"http://localhost:{settings.pubsub_rest_port}/docs",
            "grpc_port": settings.pubsub_port,
        },
        "firestore": {
            "port": settings.firestore_port,
            "stats": firestore_store().stats(),
            "docs_url": f"http://localhost:{settings.firestore_port}/docs",
        },
        "secretmanager": {
            "port": settings.secretmanager_port,
            "stats": sm_store().stats(),
            "docs_url": f"http://localhost:{settings.secretmanager_port}/docs",
        },
        "tasks": {
            "port": settings.tasks_port,
            "stats": tasks_store().stats(),
            "docs_url": f"http://localhost:{settings.tasks_port}/docs",
        },
        "bigquery": {
            "port": settings.bigquery_port,
            "stats": {"datasets": len(bq_datasets), "tables": bq_table_count},
            "docs_url": f"http://localhost:{settings.bigquery_port}/docs",
        },
        "logging": {
            "port": settings.logging_port,
            "stats": {"entries": log_entry_count, "sinks": log_sink_count},
            "docs_url": f"http://localhost:{settings.logging_port}/docs",
        },
        "spanner": {
            "port": settings.spanner_port,
            "stats": {"instances": len(sp_instances), "databases": sp_db_count},
            "docs_url": f"http://localhost:{settings.spanner_port}/docs",
        },
        "scheduler": {
            "port": settings.scheduler_port,
            "stats": {"jobs": len(sched_jobs), "enabled": sched_enabled, "paused": sched_paused},
            "docs_url": f"http://localhost:{settings.scheduler_port}/docs",
        },
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/")
async def dashboard(request: Request):
    """Render the admin dashboard HTML page.

    Args:
        request (Request): The incoming HTTP request.

    Returns:
        TemplateResponse: Rendered index.html template.
    """
    return _TEMPLATES.TemplateResponse(request, "index.html")


@app.get("/api/stats")
async def api_stats():
    """Return project name and per-service status summary.

    Returns:
        JSONResponse: Project name and per-service stats keyed by service name.
    """
    return JSONResponse(
        content={
            "project": settings.default_project,
            "services": _get_services(),
        }
    )


# ── GCS ──────────────────────────────────────────────────────────────────────


@app.get("/api/gcs/buckets")
async def api_gcs_buckets():
    """List all GCS buckets with object and notification counts.

    Returns:
        list[dict]: Sorted list of bucket metadata including objectCount and notificationCount.
    """
    from cloudbox.services.gcs.store import get_store

    store = get_store()
    buckets = store.list("buckets")
    result = []
    for b in sorted(buckets, key=lambda x: x["name"]):
        count = sum(1 for k in store.keys("objects") if k.startswith(f"{b['name']}/"))
        notif_count = sum(1 for k in store.keys("notifications") if k.startswith(f"{b['name']}/"))
        result.append({**b, "objectCount": count, "notificationCount": notif_count})
    return result


@app.get("/api/gcs/notifications")
async def api_gcs_notifications(bucket: str = Query(...)):
    """List notification configs for a GCS bucket.

    Args:
        bucket (str): Name of the GCS bucket.

    Returns:
        list[dict]: Notification configuration objects for the bucket.
    """
    from cloudbox.services.gcs.store import get_store

    store = get_store()
    prefix = f"{bucket}/"
    result = []
    for k in sorted(k for k in store.keys("notifications") if k.startswith(prefix)):
        data = store.get("notifications", k)
        if data:
            result.append(data)
    return result


@app.get("/api/gcs/download")
async def api_gcs_download(bucket: str = Query(...), name: str = Query(...)):
    """Download a GCS object as an attachment.

    Args:
        bucket (str): Name of the GCS bucket.
        name (str): Object name within the bucket.

    Returns:
        Response: Raw object bytes with content-disposition attachment header, or 404 JSON error.
    """
    from cloudbox.services.gcs.store import get_store

    store = get_store()
    key = f"{bucket}/{name}"
    data = store.get("objects", key)
    if data is None:
        return JSONResponse(status_code=404, content={"error": "Object not found"})
    body = store.get("bodies", key) or b""
    content_type = data.get("contentType", "application/octet-stream")
    filename = name.rsplit("/", 1)[-1]
    return Response(
        content=body,
        media_type=content_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/gcs/objects")
async def api_gcs_objects(bucket: str = Query(...)):
    """List all objects in a GCS bucket.

    Args:
        bucket (str): Name of the GCS bucket.

    Returns:
        list[dict]: Sorted list of object metadata for the given bucket.
    """
    from cloudbox.services.gcs.store import get_store

    store = get_store()
    prefix = f"{bucket}/"
    items = []
    for k in sorted(k for k in store.keys("objects") if k.startswith(prefix)):
        data = store.get("objects", k)
        if data:
            items.append(data)
    return items


@app.delete("/api/gcs/buckets")
async def api_gcs_delete_bucket(bucket: str = Query(...)):
    """Delete a GCS bucket and all its objects.

    Args:
        bucket (str): Name of the GCS bucket to delete.

    Returns:
        dict: Confirmation with the deleted bucket name.
    """
    from cloudbox.services.gcs.store import get_store

    store = get_store()
    for k in list(store.keys("objects")):
        if k.startswith(f"{bucket}/"):
            store.delete("objects", k)
            store.delete("bodies", k)
    store.delete("buckets", bucket)
    return {"deleted": bucket}


@app.delete("/api/gcs/objects")
async def api_gcs_delete_object(bucket: str = Query(...), name: str = Query(...)):
    """Delete a single GCS object.

    Args:
        bucket (str): Name of the GCS bucket containing the object.
        name (str): Object name within the bucket.

    Returns:
        dict: Confirmation with the deleted object key.
    """
    from cloudbox.services.gcs.store import get_store

    store = get_store()
    key = f"{bucket}/{name}"
    store.delete("objects", key)
    store.delete("bodies", key)
    return {"deleted": key}


# ── Pub/Sub ───────────────────────────────────────────────────────────────────


@app.get("/api/pubsub/topics")
async def api_pubsub_topics():
    """List all Pub/Sub topics with subscription and retained message counts.

    Returns:
        list[dict]: Sorted list of topic metadata including subscriptionCount and retainedCount.
    """
    from cloudbox.services.pubsub.store import get_store, retained_count

    store = get_store()
    topics = store.list("topics")
    subs = store.list("subscriptions")
    result = []
    for t in sorted(topics, key=lambda x: x["name"]):
        count = sum(1 for s in subs if s["topic"] == t["name"])
        result.append({**t, "subscriptionCount": count, "retainedCount": retained_count(t["name"])})
    return result


@app.get("/api/pubsub/subscriptions")
async def api_pubsub_subscriptions():
    """List all Pub/Sub subscriptions with queue depth and unacked message counts.

    Returns:
        list[dict]: Sorted list of subscription metadata including queueDepth and unackedCount.
    """
    from cloudbox.services.pubsub.store import get_store, queue_depth, unacked_count

    store = get_store()
    subs = store.list("subscriptions")
    result = []
    for s in sorted(subs, key=lambda x: x["name"]):
        result.append(
            {**s, "queueDepth": queue_depth(s["name"]), "unackedCount": unacked_count(s["name"])}
        )
    return result


@app.post("/api/pubsub/publish")
async def api_pubsub_publish(request: Request):
    """Publish a message to a topic via the admin UI.

    Args:
        request (Request): JSON body with keys topic, data (str), and optional attributes (dict).

    Returns:
        dict: Published messageId and deliveredToSubscriptions count, or 404 JSON error.
    """
    import uuid
    from datetime import datetime

    from cloudbox.services.pubsub.store import enqueue, ensure_queue, get_store

    body = await request.json()
    topic = body.get("topic", "")
    data = body.get("data", "")
    attributes = body.get("attributes", {})

    store = get_store()
    if not store.exists("topics", topic):
        return JSONResponse(status_code=404, content={"error": "Topic not found"})

    encoded = base64.b64encode(data.encode()).decode() if data else ""
    msg_id = str(uuid.uuid4())
    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    message = {
        "data": encoded,
        "attributes": attributes,
        "messageId": msg_id,
        "publishTime": now,
        "orderingKey": "",
    }

    count = 0
    for s in store.list("subscriptions"):
        if s["topic"] == topic:
            ensure_queue(s["name"])
            enqueue(s["name"], message)
            count += 1

    return {"messageId": msg_id, "deliveredToSubscriptions": count}


@app.delete("/api/pubsub/topics")
async def api_pubsub_delete_topic(topic: str = Query(...)):
    """Delete a Pub/Sub topic and all associated subscriptions.

    Args:
        topic (str): Full resource name of the topic to delete.

    Returns:
        dict: Confirmation with the deleted topic name.
    """
    from cloudbox.services.pubsub.store import get_store, remove_queue

    store = get_store()
    store.delete("topics", topic)
    for s in store.list("subscriptions"):
        if s["topic"] == topic:
            store.delete("subscriptions", s["name"])
            remove_queue(s["name"])
    return {"deleted": topic}


@app.delete("/api/pubsub/subscriptions")
async def api_pubsub_delete_subscription(subscription: str = Query(...)):
    """Delete a Pub/Sub subscription.

    Args:
        subscription (str): Full resource name of the subscription to delete.

    Returns:
        dict: Confirmation with the deleted subscription name.
    """
    from cloudbox.services.pubsub.store import get_store, remove_queue

    store = get_store()
    store.delete("subscriptions", subscription)
    remove_queue(subscription)
    return {"deleted": subscription}


# ── Firestore ─────────────────────────────────────────────────────────────────


@app.get("/api/firestore/collections")
async def api_firestore_collections():
    """List all Firestore collections with document counts.

    Returns:
        list[dict]: Sorted list of collection names and their documentCount.
    """
    from cloudbox.services.firestore.store import get_store

    store = get_store()
    counts: dict[str, int] = {}
    for key in store.keys("documents"):
        parts = key.split("/documents/", 1)
        if len(parts) == 2:
            segments = parts[1].split("/")
            if len(segments) >= 2:
                counts[segments[0]] = counts.get(segments[0], 0) + 1
    return [{"name": c, "documentCount": n} for c, n in sorted(counts.items())]


@app.get("/api/firestore/documents")
async def api_firestore_documents(collection: str = Query(...)):
    """List all documents in a Firestore collection.

    Args:
        collection (str): Collection name to query.

    Returns:
        list[dict]: Documents in the collection, sorted by resource name.
    """
    from cloudbox.services.firestore.store import get_store

    store = get_store()
    docs = []
    for key in store.keys("documents"):
        parts = key.split("/documents/", 1)
        if len(parts) == 2:
            segments = parts[1].split("/")
            if len(segments) == 2 and segments[0] == collection:
                data = store.get("documents", key)
                if data:
                    docs.append(data)
    return sorted(docs, key=lambda x: x.get("name", ""))


@app.delete("/api/firestore/documents")
async def api_firestore_delete_document(path: str = Query(...)):
    """Delete a Firestore document by its resource path.

    Args:
        path (str): Full store key (resource path) of the document to delete.

    Returns:
        dict: Confirmation with the deleted path.
    """
    from cloudbox.services.firestore.store import get_store

    get_store().delete("documents", path)
    return {"deleted": path}


# ── Secret Manager ────────────────────────────────────────────────────────────


@app.get("/api/secretmanager/secrets")
async def api_sm_secrets():
    """List all Secret Manager secrets with version counts.

    Returns:
        list[dict]: Sorted list of secret metadata including versionCount.
    """
    from cloudbox.services.secretmanager.store import get_store

    store = get_store()
    secrets = store.list("secrets")
    result = []
    for s in sorted(secrets, key=lambda x: x["name"]):
        count = sum(1 for k in store.keys("versions") if k.startswith(f"{s['name']}/versions/"))
        result.append({**s, "versionCount": count})
    return result


@app.get("/api/secretmanager/versions")
async def api_sm_versions(secret: str = Query(...)):
    """List versions for a secret identified by its short name.

    Args:
        secret (str): Short secret name (e.g. "my-secret"), not the full resource path.

    Returns:
        list[dict]: Version metadata objects sorted by version key, or 404 JSON error.
    """
    from cloudbox.services.secretmanager.store import get_store

    store = get_store()
    full_name = next((k for k in store.keys("secrets") if k.endswith(f"/secrets/{secret}")), None)
    if full_name is None:
        return JSONResponse(status_code=404, content={"error": "Secret not found"})
    prefix = f"{full_name}/versions/"
    versions = [
        store.get("versions", k)
        for k in sorted(k for k in store.keys("versions") if k.startswith(prefix))
    ]
    return [v for v in versions if v]


@app.get("/api/secretmanager/value")
async def api_sm_value(secret: str = Query(...), version: str = Query(...)):
    """Return the decoded payload for a secret version.

    Args:
        secret (str): Short secret name (e.g. "my-secret").
        version (str): Version identifier (e.g. "1" or "latest").

    Returns:
        dict: Decoded plaintext value under key "value", or 404 JSON error.
    """
    from cloudbox.services.secretmanager.store import get_store

    store = get_store()
    full_name = next((k for k in store.keys("secrets") if k.endswith(f"/secrets/{secret}")), None)
    if full_name is None:
        return JSONResponse(status_code=404, content={"error": "Secret not found"})
    version_key = f"{full_name}/versions/{version}"
    payload = store.get("payloads", version_key)
    if payload is None:
        return JSONResponse(status_code=404, content={"error": "Version not found"})
    try:
        decoded = base64.b64decode(payload).decode("utf-8")
    except Exception:
        decoded = payload
    return {"value": decoded}


@app.delete("/api/secretmanager/secrets")
async def api_sm_delete_secret(secret: str = Query(...)):
    """Delete a secret and all its versions.

    Args:
        secret (str): Short secret name (e.g. "my-secret").

    Returns:
        dict: Confirmation with the deleted secret name, or 404 JSON error.
    """
    from cloudbox.services.secretmanager.store import get_store

    store = get_store()
    full_name = next((k for k in store.keys("secrets") if k.endswith(f"/secrets/{secret}")), None)
    if full_name is None:
        return JSONResponse(status_code=404, content={"error": "Secret not found"})
    store.delete("secrets", full_name)
    for k in list(store.keys("versions")):
        if k.startswith(f"{full_name}/versions/"):
            store.delete("versions", k)
            store.delete("payloads", k)
    return {"deleted": secret}


# ── Cloud Tasks ───────────────────────────────────────────────────────────────


@app.get("/api/tasks/queues")
async def api_tasks_queues():
    """List all Cloud Tasks queues with task counts.

    Returns:
        list[dict]: Sorted list of queue metadata including taskCount.
    """
    from cloudbox.services.tasks.store import get_store

    store = get_store()
    queues = store.list("queues")
    result = []
    for q in sorted(queues, key=lambda x: x["name"]):
        count = sum(1 for k in store.keys("tasks") if k.startswith(f"{q['name']}/tasks/"))
        result.append({**q, "taskCount": count})
    return result


@app.get("/api/tasks/tasks")
async def api_tasks_list(queue: str = Query(...)):
    """List all tasks in a Cloud Tasks queue.

    Args:
        queue (str): Full resource name of the queue.

    Returns:
        list[dict]: Task metadata objects sorted by task key.
    """
    from cloudbox.services.tasks.store import get_store

    store = get_store()
    prefix = f"{queue}/tasks/"
    tasks = [
        store.get("tasks", k)
        for k in sorted(k for k in store.keys("tasks") if k.startswith(prefix))
    ]
    return [t for t in tasks if t]


@app.delete("/api/tasks/task")
async def api_tasks_delete_task(task: str = Query(...)):
    """Delete a single Cloud Task by its full resource name.

    Args:
        task (str): Full resource name of the task to delete.

    Returns:
        dict: Confirmation with the deleted task name.
    """
    from cloudbox.services.tasks.store import get_store

    get_store().delete("tasks", task)
    return {"deleted": task}


# ── BigQuery ──────────────────────────────────────────────────────────────────


@app.get("/api/bigquery/datasets")
async def api_bq_datasets():
    """List all BigQuery datasets with table counts.

    Returns:
        list[dict]: Sorted list of dataset info including datasetId, location, and tableCount.
    """
    from cloudbox.services.bigquery.engine import get_engine

    engine = get_engine()
    project = settings.default_project
    datasets = engine.list_datasets(project)
    result = []
    for d in sorted(datasets, key=lambda x: x["datasetReference"]["datasetId"]):
        ds_id = d["datasetReference"]["datasetId"]
        tables = engine.list_tables(project, ds_id)
        result.append(
            {
                "datasetId": ds_id,
                "location": d.get("location", "US"),
                "tableCount": len(tables),
            }
        )
    return result


@app.get("/api/bigquery/tables")
async def api_bq_tables(dataset: str = Query(...)):
    """List all tables in a BigQuery dataset.

    Args:
        dataset (str): BigQuery dataset ID.

    Returns:
        list[dict]: Sorted list of table info including tableId, schema, and numRows.
    """
    from cloudbox.services.bigquery.engine import get_engine

    engine = get_engine()
    tables = engine.list_tables(settings.default_project, dataset)
    return [
        {
            "tableId": t["tableReference"]["tableId"],
            "schema": t.get("schema"),
            "numRows": t.get("numRows", "0"),
        }
        for t in sorted(tables, key=lambda x: x["tableReference"]["tableId"])
    ]


@app.get("/api/bigquery/preview")
async def api_bq_preview(
    dataset: str = Query(...),
    table: str = Query(...),
    maxResults: int = Query(default=50),
):
    """Preview rows from a BigQuery table.

    Args:
        dataset (str): BigQuery dataset ID.
        table (str): BigQuery table ID.
        maxResults (int): Maximum number of rows to return (default 50).

    Returns:
        dict: Row data from the engine, or 404 JSON error if the table does not exist.
    """
    from cloudbox.services.bigquery.engine import get_engine

    engine = get_engine()
    try:
        return engine.list_rows(settings.default_project, dataset, table, max_results=maxResults)
    except ValueError as e:
        return JSONResponse(status_code=404, content={"error": str(e)})


@app.delete("/api/bigquery/dataset")
async def api_bq_delete_dataset(dataset: str = Query(...)):
    """Delete a BigQuery dataset and all its tables.

    Args:
        dataset (str): BigQuery dataset ID to delete.

    Returns:
        dict: Confirmation with the deleted dataset ID, or 404 JSON error.
    """
    from cloudbox.services.bigquery.engine import get_engine

    engine = get_engine()
    try:
        engine.delete_dataset(settings.default_project, dataset, delete_contents=True)
    except ValueError as e:
        return JSONResponse(status_code=404, content={"error": str(e)})
    return {"deleted": dataset}


@app.delete("/api/bigquery/table")
async def api_bq_delete_table(dataset: str = Query(...), table: str = Query(...)):
    """Delete a single BigQuery table.

    Args:
        dataset (str): BigQuery dataset ID containing the table.
        table (str): BigQuery table ID to delete.

    Returns:
        dict: Confirmation with the deleted "dataset.table" identifier, or 404 JSON error.
    """
    from cloudbox.services.bigquery.engine import get_engine

    engine = get_engine()
    found = engine.delete_table(settings.default_project, dataset, table)
    if not found:
        return JSONResponse(status_code=404, content={"error": "Table not found"})
    return {"deleted": f"{dataset}.{table}"}


# ── Cloud Logging ─────────────────────────────────────────────────────────────


@app.get("/api/logging/entries")
async def api_logging_entries(
    log: str = Query(default=""),
    severity: str = Query(default=""),
    limit: int = Query(default=100),
):
    """Return log entries filtered by log name and minimum severity.

    Args:
        log (str): Short log name suffix to filter by; empty string means no filter.
        severity (str): Minimum severity level (e.g. "WARNING"); empty string means no filter.
        limit (int): Maximum number of entries to return (default 100).

    Returns:
        list[dict]: Log entries sorted by timestamp descending, truncated to limit.
    """
    from cloudbox.services.logging.store import get_store

    store = get_store()
    entries = store.list("entries")
    if log:
        entries = [e for e in entries if e.get("logName", "").endswith(f"/logs/{log}")]
    if severity:
        from cloudbox.services.logging.app import _SEVERITY_ORDER

        min_level = _SEVERITY_ORDER.get(severity.upper(), 0)
        entries = [
            e
            for e in entries
            if _SEVERITY_ORDER.get(e.get("severity", "DEFAULT").upper(), 0) >= min_level
        ]
    entries.sort(key=lambda e: e.get("timestamp", ""), reverse=True)
    return entries[:limit]


@app.get("/api/logging/logs")
async def api_logging_logs():
    """Return a deduplicated list of log names seen in stored entries.

    Returns:
        list[dict]: Sorted list of objects with logName and shortName fields.
    """
    from cloudbox.services.logging.store import get_store

    store = get_store()
    entries = store.list("entries")
    log_names = sorted({e.get("logName", "") for e in entries if e.get("logName")})
    return [{"logName": ln, "shortName": ln.rsplit("/logs/", 1)[-1]} for ln in log_names]


@app.delete("/api/logging/entries")
async def api_logging_clear(log: str = Query(default="")):
    """Clear log entries, either all entries or only those matching a specific log name.

    Args:
        log (str): Short log name suffix to match; empty string clears all entries.

    Returns:
        dict: Confirmation with the cleared log name or "all".
    """
    from cloudbox.services.logging.store import get_store

    store = get_store()
    if log:
        all_keys = store.keys("entries")
        for k in all_keys:
            v = store.get("entries", k)
            if v and v.get("logName", "").endswith(f"/logs/{log}"):
                store.delete("entries", k)
    else:
        store.clear_namespace("entries")
    return {"cleared": log or "all"}


# ── Cloud Spanner ─────────────────────────────────────────────────────────────


@app.get("/api/spanner/instances")
async def api_spanner_instances():
    """List all Cloud Spanner instances with database counts.

    Returns:
        list[dict]: Sorted list of instance info including instanceId, displayName, state, and databaseCount.
    """
    from cloudbox.services.spanner.engine import get_engine

    engine = get_engine()
    instances = engine.list_instances(settings.default_project)
    result = []
    for inst in sorted(instances, key=lambda x: x["name"]):
        instance_id = inst["name"].rsplit("/", 1)[-1]
        dbs = engine.list_databases(settings.default_project, instance_id)
        result.append(
            {
                "instanceId": instance_id,
                "displayName": inst.get("displayName", instance_id),
                "state": inst.get("state", "READY"),
                "databaseCount": len(dbs),
            }
        )
    return result


@app.get("/api/spanner/databases")
async def api_spanner_databases(instance: str = Query(...)):
    """List all databases in a Cloud Spanner instance with table counts.

    Args:
        instance (str): Cloud Spanner instance ID.

    Returns:
        list[dict]: Sorted list of database info including databaseId, state, and tableCount.
    """
    from cloudbox.services.spanner.engine import get_engine

    engine = get_engine()
    dbs = engine.list_databases(settings.default_project, instance)
    result = []
    for db in sorted(dbs, key=lambda x: x["name"]):
        database_id = db["name"].rsplit("/", 1)[-1]
        tables = engine.list_tables(settings.default_project, instance, database_id)
        result.append(
            {
                "databaseId": database_id,
                "state": db.get("state", "READY"),
                "tableCount": len(tables),
            }
        )
    return result


@app.get("/api/spanner/tables")
async def api_spanner_tables(instance: str = Query(...), database: str = Query(...)):
    """List all tables in a Cloud Spanner database.

    Args:
        instance (str): Cloud Spanner instance ID.
        database (str): Cloud Spanner database ID.

    Returns:
        list[dict]: Sorted list of objects with tableName field.
    """
    from cloudbox.services.spanner.engine import get_engine

    engine = get_engine()
    tables = engine.list_tables(settings.default_project, instance, database)
    return [{"tableName": t} for t in sorted(tables)]


@app.delete("/api/spanner/databases")
async def api_spanner_delete_database(instance: str = Query(...), database: str = Query(...)):
    """Delete a Cloud Spanner database.

    Args:
        instance (str): Cloud Spanner instance ID.
        database (str): Cloud Spanner database ID to delete.

    Returns:
        dict: Confirmation with the deleted database ID, or 404 JSON error.
    """
    from cloudbox.services.spanner.engine import get_engine

    engine = get_engine()
    found = engine.delete_database(settings.default_project, instance, database)
    if not found:
        return JSONResponse(status_code=404, content={"error": "Database not found"})
    return {"deleted": database}


# ── Scheduler ─────────────────────────────────────────────────────────────────


@app.get("/api/scheduler/jobs")
async def api_sched_jobs():
    """List all Cloud Scheduler jobs.

    Returns:
        list[dict]: All jobs sorted by resource name.
    """
    from cloudbox.services.scheduler.store import get_store

    store = get_store()
    jobs = store.list("jobs")
    return sorted(jobs, key=lambda x: x.get("name", ""))


@app.post("/api/scheduler/jobs/{job_id}:run")
async def api_sched_run(job_id: str):
    """Manually trigger a Cloud Scheduler job immediately.

    Args:
        job_id (str): Short job ID (last path segment of the resource name).

    Returns:
        dict: Updated job state after dispatch, or 404 JSON error if not found.
    """
    from cloudbox.services.scheduler.store import get_store
    from cloudbox.services.scheduler.worker import _dispatch

    store = get_store()
    job = next((j for j in store.list("jobs") if j.get("name", "").endswith(f"/{job_id}")), None)
    if job is None:
        return JSONResponse(status_code=404, content={"error": "Job not found"})
    from datetime import datetime

    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        await _dispatch(job)
        job["lastAttemptTime"] = now
        job.pop("status", None)
    except Exception as exc:
        job["lastAttemptTime"] = now
        job["status"] = {"code": 2, "message": str(exc)}
    store.set("jobs", job["name"], job)
    return job


@app.post("/api/scheduler/jobs/{job_id}:pause")
async def api_sched_pause(job_id: str):
    """Pause a Cloud Scheduler job.

    Args:
        job_id (str): Short job ID (last path segment of the resource name).

    Returns:
        dict: Updated job state with state set to PAUSED, or 404 JSON error.
    """
    from cloudbox.services.scheduler.store import get_store

    store = get_store()
    job = next((j for j in store.list("jobs") if j.get("name", "").endswith(f"/{job_id}")), None)
    if job is None:
        return JSONResponse(status_code=404, content={"error": "Job not found"})
    job["state"] = "PAUSED"
    store.set("jobs", job["name"], job)
    return job


@app.post("/api/scheduler/jobs/{job_id}:resume")
async def api_sched_resume(job_id: str):
    """Resume a paused Cloud Scheduler job.

    Args:
        job_id (str): Short job ID (last path segment of the resource name).

    Returns:
        dict: Updated job state with state set to ENABLED, or 404 JSON error.
    """
    from cloudbox.services.scheduler.store import get_store

    store = get_store()
    job = next((j for j in store.list("jobs") if j.get("name", "").endswith(f"/{job_id}")), None)
    if job is None:
        return JSONResponse(status_code=404, content={"error": "Job not found"})
    job["state"] = "ENABLED"
    store.set("jobs", job["name"], job)
    return job


@app.delete("/api/scheduler/jobs/{job_id}")
async def api_sched_delete(job_id: str):
    """Delete a Cloud Scheduler job.

    Args:
        job_id (str): Short job ID (last path segment of the resource name).

    Returns:
        Response: Empty 204 response on success, or 404 JSON error if not found.
    """
    from cloudbox.services.scheduler.store import get_store

    store = get_store()
    job = next((j for j in store.list("jobs") if j.get("name", "").endswith(f"/{job_id}")), None)
    if job is None:
        return JSONResponse(status_code=404, content={"error": "Job not found"})
    store.delete("jobs", job["name"])
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Reset endpoints
# ---------------------------------------------------------------------------


@app.post("/reset/{service}")
async def reset_service(service: str):
    """Reset a single service's in-memory state.

    Args:
        service (str): Service name (e.g. "gcs", "pubsub", "firestore").

    Returns:
        dict: Confirmation with the reset service name.
    """
    _reset_one(service)
    return {"reset": service}


@app.post("/reset")
async def reset_all():
    """Reset all services' in-memory state.

    Returns:
        dict: Confirmation with value "all".
    """
    for svc in (
        "gcs",
        "pubsub",
        "firestore",
        "secretmanager",
        "tasks",
        "bigquery",
        "spanner",
        "logging",
        "scheduler",
    ):
        _reset_one(svc)
    return {"reset": "all"}


def _reset_one(service: str) -> None:
    """Reset the in-memory state for a single named service.

    Args:
        service (str): Service name key (e.g. "gcs", "pubsub", "bigquery").
    """
    if service == "gcs":
        from cloudbox.services.gcs.store import get_store

        get_store().reset()
    elif service == "pubsub":
        from cloudbox.services.pubsub.store import _queues, _unacked, get_store

        get_store().reset()
        _queues.clear()
        _unacked.clear()
    elif service == "firestore":
        from cloudbox.services.firestore.store import get_store

        get_store().reset()
    elif service == "secretmanager":
        from cloudbox.services.secretmanager.store import get_store

        get_store().reset()
    elif service == "tasks":
        from cloudbox.services.tasks.store import get_store

        get_store().reset()
    elif service == "bigquery":
        from cloudbox.services.bigquery.engine import get_engine

        get_engine().reset()
    elif service == "spanner":
        from cloudbox.services.spanner.engine import get_engine

        get_engine().reset()
    elif service == "logging":
        from cloudbox.services.logging.store import get_store

        get_store().reset()
    elif service == "scheduler":
        from cloudbox.services.scheduler.store import get_store

        get_store().reset()


@app.get("/health")
async def health():
    """Return a simple health-check response."""
    return {"status": "ok", "project": settings.default_project}
