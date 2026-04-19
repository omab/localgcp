"""Cloud Scheduler emulator.

Implements the Cloud Scheduler REST API v1 used by google-cloud-scheduler.

Routes are prefixed with /v1 — the SDK appends this to api_endpoint.
"""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import FastAPI, Request, Response

from cloudbox.core.errors import GCPError, add_gcp_exception_handler
from cloudbox.core.middleware import add_request_logging
from cloudbox.services.scheduler import store as sched_store
from cloudbox.services.scheduler.models import JobListResponse, JobModel
from cloudbox.services.scheduler.worker import _next_run_time

app = FastAPI(title="Cloudbox — Cloud Scheduler", version="v1")
add_gcp_exception_handler(app)
add_request_logging(app, "scheduler")


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _job_id(full_name: str) -> str:
    return full_name.rsplit("/", 1)[-1]


# ---------------------------------------------------------------------------
# Jobs CRUD
# ---------------------------------------------------------------------------


@app.post("/v1/projects/{project}/locations/{location}/jobs", status_code=200)
async def create_job(project: str, location: str, request: Request):
    body = await request.json()
    name_field = body.get("name", "")
    # Derive job ID from name or generate from body
    if name_field:
        job_id = _job_id(name_field)
    else:
        raise GCPError(400, "name is required")

    full_name = f"projects/{project}/locations/{location}/jobs/{job_id}"
    store = sched_store.get_store()
    if store.exists("jobs", full_name):
        raise GCPError(409, f"Job already exists: {full_name}")

    now = _now()
    schedule = body.get("schedule", "")
    job = JobModel(
        name=full_name,
        description=body.get("description", ""),
        schedule=schedule,
        timeZone=body.get("timeZone", "UTC"),
        state="ENABLED",
        httpTarget=body.get("httpTarget"),
        retryConfig=body.get("retryConfig", {}),
        userUpdateTime=now,
        scheduleTime=_next_run_time(schedule, datetime.now(UTC)) if schedule else "",
    )
    store.set("jobs", full_name, job.model_dump())
    return job.model_dump(exclude_none=True)


@app.get("/v1/projects/{project}/locations/{location}/jobs/{job_id}")
async def get_job(project: str, location: str, job_id: str):
    full_name = f"projects/{project}/locations/{location}/jobs/{job_id}"
    store = sched_store.get_store()
    data = store.get("jobs", full_name)
    if data is None:
        raise GCPError(404, f"Job not found: {full_name}")
    return data


@app.get("/v1/projects/{project}/locations/{location}/jobs")
async def list_jobs(project: str, location: str, pageSize: int = 100, pageToken: str = ""):
    store = sched_store.get_store()
    prefix = f"projects/{project}/locations/{location}/jobs/"
    items = [JobModel(**v) for v in store.list("jobs") if v["name"].startswith(prefix)]
    offset = int(pageToken) if pageToken else 0
    page = items[offset : offset + pageSize]
    next_token = str(offset + pageSize) if offset + pageSize < len(items) else None
    return JobListResponse(jobs=page, nextPageToken=next_token).model_dump(exclude_none=True)


@app.patch("/v1/projects/{project}/locations/{location}/jobs/{job_id}")
async def update_job(project: str, location: str, job_id: str, request: Request):
    full_name = f"projects/{project}/locations/{location}/jobs/{job_id}"
    store = sched_store.get_store()
    existing = store.get("jobs", full_name)
    if existing is None:
        raise GCPError(404, f"Job not found: {full_name}")
    body = await request.json()
    # Merge fields
    for key in ("description", "schedule", "timeZone", "httpTarget", "retryConfig"):
        if key in body:
            existing[key] = body[key]
    existing["userUpdateTime"] = _now()
    if "schedule" in body and body["schedule"]:
        existing["scheduleTime"] = _next_run_time(body["schedule"], datetime.now(UTC))
    store.set("jobs", full_name, existing)
    return existing


@app.delete("/v1/projects/{project}/locations/{location}/jobs/{job_id}", status_code=204)
async def delete_job(project: str, location: str, job_id: str):
    full_name = f"projects/{project}/locations/{location}/jobs/{job_id}"
    store = sched_store.get_store()
    if not store.delete("jobs", full_name):
        raise GCPError(404, f"Job not found: {full_name}")
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Job actions
# ---------------------------------------------------------------------------


@app.post("/v1/projects/{project}/locations/{location}/jobs/{job_id}:run")
async def run_job(project: str, location: str, job_id: str):
    """Force-run a job immediately, regardless of schedule."""
    from cloudbox.services.scheduler.worker import _dispatch

    full_name = f"projects/{project}/locations/{location}/jobs/{job_id}"
    store = sched_store.get_store()
    data = store.get("jobs", full_name)
    if data is None:
        raise GCPError(404, f"Job not found: {full_name}")

    http_target = data.get("httpTarget")
    if http_target:
        import httpx

        now = _now()
        data["lastAttemptTime"] = now
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                await _dispatch(client, http_target)
            data["status"] = {}
        except Exception as exc:
            data["status"] = {"code": 2, "message": str(exc)}
        store.set("jobs", full_name, data)

    return data


@app.post("/v1/projects/{project}/locations/{location}/jobs/{job_id}:pause")
async def pause_job(project: str, location: str, job_id: str):
    full_name = f"projects/{project}/locations/{location}/jobs/{job_id}"
    store = sched_store.get_store()
    data = store.get("jobs", full_name)
    if data is None:
        raise GCPError(404, f"Job not found: {full_name}")
    data["state"] = "PAUSED"
    store.set("jobs", full_name, data)
    return data


@app.post("/v1/projects/{project}/locations/{location}/jobs/{job_id}:resume")
async def resume_job(project: str, location: str, job_id: str):
    full_name = f"projects/{project}/locations/{location}/jobs/{job_id}"
    store = sched_store.get_store()
    data = store.get("jobs", full_name)
    if data is None:
        raise GCPError(404, f"Job not found: {full_name}")
    data["state"] = "ENABLED"
    store.set("jobs", full_name, data)
    return data
