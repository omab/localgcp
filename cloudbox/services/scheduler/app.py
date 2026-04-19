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
    """Return the current UTC timestamp in ISO 8601 format with second precision.

    Returns:
        str: Current UTC time formatted as 'YYYY-MM-DDTHH:MM:SSZ'.
    """
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _job_id(full_name: str) -> str:
    """Extract the job ID from a full resource name.

    Args:
        full_name (str): Full job resource name such as 'projects/p/locations/l/jobs/my-job'.

    Returns:
        str: The last path component, which is the job ID.
    """
    return full_name.rsplit("/", 1)[-1]


# ---------------------------------------------------------------------------
# Jobs CRUD
# ---------------------------------------------------------------------------


@app.post("/v1/projects/{project}/locations/{location}/jobs", status_code=200)
async def create_job(project: str, location: str, request: Request):
    """Create a new Cloud Scheduler job.

    Args:
        project (str): GCP project ID.
        location (str): GCP region.
        request (Request): HTTP request body with name, schedule, timeZone, httpTarget, etc.

    Returns:
        dict: The newly created JobModel dict.

    Raises:
        GCPError: If name is missing (400) or the job already exists (409).
    """
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
    """Get a Cloud Scheduler job by ID.

    Args:
        project (str): GCP project ID.
        location (str): GCP region.
        job_id (str): Job resource ID.

    Returns:
        dict: The JobModel dict for the requested job.

    Raises:
        GCPError: If the job does not exist (404).
    """
    full_name = f"projects/{project}/locations/{location}/jobs/{job_id}"
    store = sched_store.get_store()
    data = store.get("jobs", full_name)
    if data is None:
        raise GCPError(404, f"Job not found: {full_name}")
    return data


@app.get("/v1/projects/{project}/locations/{location}/jobs")
async def list_jobs(project: str, location: str, pageSize: int = 100, pageToken: str = ""):
    """List Cloud Scheduler jobs for a project and location.

    Args:
        project (str): GCP project ID.
        location (str): GCP region.
        pageSize (int): Maximum number of jobs to return per page.
        pageToken (str): Pagination token from a previous response.

    Returns:
        dict: JobListResponse with jobs and optional nextPageToken.
    """
    store = sched_store.get_store()
    prefix = f"projects/{project}/locations/{location}/jobs/"
    items = [JobModel(**v) for v in store.list("jobs") if v["name"].startswith(prefix)]
    offset = int(pageToken) if pageToken else 0
    page = items[offset : offset + pageSize]
    next_token = str(offset + pageSize) if offset + pageSize < len(items) else None
    return JobListResponse(jobs=page, nextPageToken=next_token).model_dump(exclude_none=True)


@app.patch("/v1/projects/{project}/locations/{location}/jobs/{job_id}")
async def update_job(project: str, location: str, job_id: str, request: Request):
    """Update fields of an existing Cloud Scheduler job.

    Args:
        project (str): GCP project ID.
        location (str): GCP region.
        job_id (str): Job resource ID.
        request (Request): HTTP request body with fields to update.

    Returns:
        dict: The updated JobModel dict.

    Raises:
        GCPError: If the job does not exist (404).
    """
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
    """Delete a Cloud Scheduler job.

    Args:
        project (str): GCP project ID.
        location (str): GCP region.
        job_id (str): Job resource ID.

    Returns:
        Response: HTTP 204 No Content response.

    Raises:
        GCPError: If the job does not exist (404).
    """
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
    """Force-run a job immediately, regardless of its schedule.

    Args:
        project (str): GCP project ID.
        location (str): GCP region.
        job_id (str): Job resource ID.

    Returns:
        dict: The updated JobModel dict with lastAttemptTime and status populated.

    Raises:
        GCPError: If the job does not exist (404).
    """
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
    """Pause a Cloud Scheduler job, preventing future dispatches.

    Args:
        project (str): GCP project ID.
        location (str): GCP region.
        job_id (str): Job resource ID.

    Returns:
        dict: The updated JobModel dict with state PAUSED.

    Raises:
        GCPError: If the job does not exist (404).
    """
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
    """Resume a paused Cloud Scheduler job.

    Args:
        project (str): GCP project ID.
        location (str): GCP region.
        job_id (str): Job resource ID.

    Returns:
        dict: The updated JobModel dict with state ENABLED.

    Raises:
        GCPError: If the job does not exist (404).
    """
    full_name = f"projects/{project}/locations/{location}/jobs/{job_id}"
    store = sched_store.get_store()
    data = store.get("jobs", full_name)
    if data is None:
        raise GCPError(404, f"Job not found: {full_name}")
    data["state"] = "ENABLED"
    store.set("jobs", full_name, data)
    return data
