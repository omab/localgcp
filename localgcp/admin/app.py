"""Admin UI for LocalGCP.

Provides a web dashboard showing the state of all emulated services,
allowing data to be browsed, interacted with, and reset.
"""
from __future__ import annotations

import base64
from pathlib import Path

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from localgcp.config import settings
from localgcp.core.middleware import add_request_logging

app = FastAPI(title="LocalGCP Admin", version="v1")
add_request_logging(app, "admin")

_STATIC = Path(__file__).parent / "static"
if _STATIC.exists():
    app.mount("/static", StaticFiles(directory=str(_STATIC)), name="static")

_TEMPLATES = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))


# ---------------------------------------------------------------------------
# Stats helper (used by overview + /api/stats)
# ---------------------------------------------------------------------------


def _get_services() -> dict:
    from localgcp.services.gcs.store import get_store as gcs_store
    from localgcp.services.pubsub.store import get_store as pubsub_store
    from localgcp.services.firestore.store import get_store as firestore_store
    from localgcp.services.secretmanager.store import get_store as sm_store
    from localgcp.services.tasks.store import get_store as tasks_store

    from localgcp.services.bigquery.engine import get_engine as bq_get_engine
    bq = bq_get_engine()
    bq_datasets = bq.list_datasets(settings.default_project)
    bq_table_count = sum(
        len(bq.list_tables(settings.default_project, d["datasetReference"]["datasetId"]))
        for d in bq_datasets
    )

    from localgcp.services.scheduler.store import get_store as scheduler_store
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
    return _TEMPLATES.TemplateResponse(request, "index.html")


@app.get("/api/stats")
async def api_stats():
    return JSONResponse(content={
        "project": settings.default_project,
        "services": _get_services(),
    })


# ── GCS ──────────────────────────────────────────────────────────────────────

@app.get("/api/gcs/buckets")
async def api_gcs_buckets():
    from localgcp.services.gcs.store import get_store
    store = get_store()
    buckets = store.list("buckets")
    result = []
    for b in sorted(buckets, key=lambda x: x["name"]):
        count = sum(1 for k in store.keys("objects") if k.startswith(f"{b['name']}/"))
        result.append({**b, "objectCount": count})
    return result


@app.get("/api/gcs/objects")
async def api_gcs_objects(bucket: str = Query(...)):
    from localgcp.services.gcs.store import get_store
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
    from localgcp.services.gcs.store import get_store
    store = get_store()
    for k in list(store.keys("objects")):
        if k.startswith(f"{bucket}/"):
            store.delete("objects", k)
            store.delete("bodies", k)
    store.delete("buckets", bucket)
    return {"deleted": bucket}


@app.delete("/api/gcs/objects")
async def api_gcs_delete_object(bucket: str = Query(...), name: str = Query(...)):
    from localgcp.services.gcs.store import get_store
    store = get_store()
    key = f"{bucket}/{name}"
    store.delete("objects", key)
    store.delete("bodies", key)
    return {"deleted": key}


# ── Pub/Sub ───────────────────────────────────────────────────────────────────

@app.get("/api/pubsub/topics")
async def api_pubsub_topics():
    from localgcp.services.pubsub.store import get_store
    store = get_store()
    topics = store.list("topics")
    subs = store.list("subscriptions")
    result = []
    for t in sorted(topics, key=lambda x: x["name"]):
        count = sum(1 for s in subs if s["topic"] == t["name"])
        result.append({**t, "subscriptionCount": count})
    return result


@app.get("/api/pubsub/subscriptions")
async def api_pubsub_subscriptions():
    from localgcp.services.pubsub.store import get_store, queue_depth
    store = get_store()
    subs = store.list("subscriptions")
    result = []
    for s in sorted(subs, key=lambda x: x["name"]):
        result.append({**s, "queueDepth": queue_depth(s["name"])})
    return result


@app.post("/api/pubsub/publish")
async def api_pubsub_publish(request: Request):
    import uuid
    from datetime import datetime, timezone
    from localgcp.services.pubsub.store import get_store, ensure_queue, enqueue

    body = await request.json()
    topic = body.get("topic", "")
    data = body.get("data", "")
    attributes = body.get("attributes", {})

    store = get_store()
    if not store.exists("topics", topic):
        return JSONResponse(status_code=404, content={"error": "Topic not found"})

    encoded = base64.b64encode(data.encode()).decode() if data else ""
    msg_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
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
    from localgcp.services.pubsub.store import get_store, remove_queue
    store = get_store()
    store.delete("topics", topic)
    for s in store.list("subscriptions"):
        if s["topic"] == topic:
            store.delete("subscriptions", s["name"])
            remove_queue(s["name"])
    return {"deleted": topic}


@app.delete("/api/pubsub/subscriptions")
async def api_pubsub_delete_subscription(subscription: str = Query(...)):
    from localgcp.services.pubsub.store import get_store, remove_queue
    store = get_store()
    store.delete("subscriptions", subscription)
    remove_queue(subscription)
    return {"deleted": subscription}


# ── Firestore ─────────────────────────────────────────────────────────────────

@app.get("/api/firestore/collections")
async def api_firestore_collections():
    from localgcp.services.firestore.store import get_store
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
    from localgcp.services.firestore.store import get_store
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
    from localgcp.services.firestore.store import get_store
    get_store().delete("documents", path)
    return {"deleted": path}


# ── Secret Manager ────────────────────────────────────────────────────────────

@app.get("/api/secretmanager/secrets")
async def api_sm_secrets():
    from localgcp.services.secretmanager.store import get_store
    store = get_store()
    secrets = store.list("secrets")
    result = []
    for s in sorted(secrets, key=lambda x: x["name"]):
        count = sum(1 for k in store.keys("versions") if k.startswith(f"{s['name']}/versions/"))
        result.append({**s, "versionCount": count})
    return result


@app.get("/api/secretmanager/versions")
async def api_sm_versions(secret: str = Query(...)):
    """List versions for a secret identified by its short name (e.g. 'my-secret')."""
    from localgcp.services.secretmanager.store import get_store
    store = get_store()
    full_name = next(
        (k for k in store.keys("secrets") if k.endswith(f"/secrets/{secret}")), None
    )
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
    """Return the decoded payload for a secret version."""
    from localgcp.services.secretmanager.store import get_store
    store = get_store()
    full_name = next(
        (k for k in store.keys("secrets") if k.endswith(f"/secrets/{secret}")), None
    )
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
    from localgcp.services.secretmanager.store import get_store
    store = get_store()
    full_name = next(
        (k for k in store.keys("secrets") if k.endswith(f"/secrets/{secret}")), None
    )
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
    from localgcp.services.tasks.store import get_store
    store = get_store()
    queues = store.list("queues")
    result = []
    for q in sorted(queues, key=lambda x: x["name"]):
        count = sum(1 for k in store.keys("tasks") if k.startswith(f"{q['name']}/tasks/"))
        result.append({**q, "taskCount": count})
    return result


@app.get("/api/tasks/tasks")
async def api_tasks_list(queue: str = Query(...)):
    from localgcp.services.tasks.store import get_store
    store = get_store()
    prefix = f"{queue}/tasks/"
    tasks = [
        store.get("tasks", k)
        for k in sorted(k for k in store.keys("tasks") if k.startswith(prefix))
    ]
    return [t for t in tasks if t]


@app.delete("/api/tasks/task")
async def api_tasks_delete_task(task: str = Query(...)):
    from localgcp.services.tasks.store import get_store
    get_store().delete("tasks", task)
    return {"deleted": task}


# ── BigQuery ──────────────────────────────────────────────────────────────────

@app.get("/api/bigquery/datasets")
async def api_bq_datasets():
    from localgcp.services.bigquery.engine import get_engine
    engine = get_engine()
    project = settings.default_project
    datasets = engine.list_datasets(project)
    result = []
    for d in sorted(datasets, key=lambda x: x["datasetReference"]["datasetId"]):
        ds_id = d["datasetReference"]["datasetId"]
        tables = engine.list_tables(project, ds_id)
        result.append({
            "datasetId": ds_id,
            "location": d.get("location", "US"),
            "tableCount": len(tables),
        })
    return result


@app.get("/api/bigquery/tables")
async def api_bq_tables(dataset: str = Query(...)):
    from localgcp.services.bigquery.engine import get_engine
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
    from localgcp.services.bigquery.engine import get_engine
    engine = get_engine()
    try:
        return engine.list_rows(settings.default_project, dataset, table, max_results=maxResults)
    except ValueError as e:
        return JSONResponse(status_code=404, content={"error": str(e)})


@app.delete("/api/bigquery/dataset")
async def api_bq_delete_dataset(dataset: str = Query(...)):
    from localgcp.services.bigquery.engine import get_engine
    engine = get_engine()
    try:
        engine.delete_dataset(settings.default_project, dataset, delete_contents=True)
    except ValueError as e:
        return JSONResponse(status_code=404, content={"error": str(e)})
    return {"deleted": dataset}


@app.delete("/api/bigquery/table")
async def api_bq_delete_table(dataset: str = Query(...), table: str = Query(...)):
    from localgcp.services.bigquery.engine import get_engine
    engine = get_engine()
    found = engine.delete_table(settings.default_project, dataset, table)
    if not found:
        return JSONResponse(status_code=404, content={"error": "Table not found"})
    return {"deleted": f"{dataset}.{table}"}


# ── Scheduler ─────────────────────────────────────────────────────────────────

@app.get("/api/scheduler/jobs")
async def api_sched_jobs():
    from localgcp.services.scheduler.store import get_store
    store = get_store()
    jobs = store.list("jobs")
    return sorted(jobs, key=lambda x: x.get("name", ""))


@app.post("/api/scheduler/jobs/{job_id}:run")
async def api_sched_run(job_id: str):
    import httpx
    from localgcp.services.scheduler.store import get_store
    from localgcp.services.scheduler.worker import _dispatch
    store = get_store()
    job = next(
        (j for j in store.list("jobs") if j.get("name", "").endswith(f"/{job_id}")), None
    )
    if job is None:
        return JSONResponse(status_code=404, content={"error": "Job not found"})
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
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
    from localgcp.services.scheduler.store import get_store
    store = get_store()
    job = next(
        (j for j in store.list("jobs") if j.get("name", "").endswith(f"/{job_id}")), None
    )
    if job is None:
        return JSONResponse(status_code=404, content={"error": "Job not found"})
    job["state"] = "PAUSED"
    store.set("jobs", job["name"], job)
    return job


@app.post("/api/scheduler/jobs/{job_id}:resume")
async def api_sched_resume(job_id: str):
    from localgcp.services.scheduler.store import get_store
    store = get_store()
    job = next(
        (j for j in store.list("jobs") if j.get("name", "").endswith(f"/{job_id}")), None
    )
    if job is None:
        return JSONResponse(status_code=404, content={"error": "Job not found"})
    job["state"] = "ENABLED"
    store.set("jobs", job["name"], job)
    return job


@app.delete("/api/scheduler/jobs/{job_id}")
async def api_sched_delete(job_id: str):
    from localgcp.services.scheduler.store import get_store
    store = get_store()
    job = next(
        (j for j in store.list("jobs") if j.get("name", "").endswith(f"/{job_id}")), None
    )
    if job is None:
        return JSONResponse(status_code=404, content={"error": "Job not found"})
    store.delete("jobs", job["name"])
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Reset endpoints
# ---------------------------------------------------------------------------


@app.post("/reset/{service}")
async def reset_service(service: str):
    _reset_one(service)
    return {"reset": service}


@app.post("/reset")
async def reset_all():
    for svc in ("gcs", "pubsub", "firestore", "secretmanager", "tasks", "bigquery", "scheduler"):
        _reset_one(svc)
    return {"reset": "all"}


def _reset_one(service: str) -> None:
    if service == "gcs":
        from localgcp.services.gcs.store import get_store
        get_store().reset()
    elif service == "pubsub":
        from localgcp.services.pubsub.store import get_store, _queues, _unacked
        get_store().reset()
        _queues.clear()
        _unacked.clear()
    elif service == "firestore":
        from localgcp.services.firestore.store import get_store
        get_store().reset()
    elif service == "secretmanager":
        from localgcp.services.secretmanager.store import get_store
        get_store().reset()
    elif service == "tasks":
        from localgcp.services.tasks.store import get_store
        get_store().reset()
    elif service == "bigquery":
        from localgcp.services.bigquery.engine import get_engine
        get_engine().reset()
    elif service == "scheduler":
        from localgcp.services.scheduler.store import get_store
        get_store().reset()


@app.get("/health")
async def health():
    return {"status": "ok", "project": settings.default_project}
