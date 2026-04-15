"""Tests for Cloud Scheduler emulator."""
from unittest.mock import AsyncMock, MagicMock, patch

PROJECT = "local-project"
LOCATION = "us-central1"
BASE = f"/v1/projects/{PROJECT}/locations/{LOCATION}/jobs"

JOB_BODY = {
    "name": f"projects/{PROJECT}/locations/{LOCATION}/jobs/my-job",
    "schedule": "* * * * *",
    "timeZone": "UTC",
    "httpTarget": {"uri": "http://localhost:9999/handler", "httpMethod": "POST"},
}


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------


def test_create_and_get_job(scheduler_client):
    r = scheduler_client.post(BASE, json=JOB_BODY)
    assert r.status_code == 200
    body = r.json()
    assert body["name"].endswith("/my-job")
    assert body["state"] == "ENABLED"
    assert body["schedule"] == "* * * * *"

    r2 = scheduler_client.get(f"{BASE}/my-job")
    assert r2.status_code == 200
    assert r2.json()["name"] == body["name"]


def test_list_jobs(scheduler_client):
    for i in range(3):
        scheduler_client.post(BASE, json={
            **JOB_BODY,
            "name": f"projects/{PROJECT}/locations/{LOCATION}/jobs/job-{i}",
        })
    r = scheduler_client.get(BASE)
    assert r.status_code == 200
    assert len(r.json()["jobs"]) == 3


def test_duplicate_job_returns_409(scheduler_client):
    scheduler_client.post(BASE, json=JOB_BODY)
    r = scheduler_client.post(BASE, json=JOB_BODY)
    assert r.status_code == 409


def test_delete_job(scheduler_client):
    scheduler_client.post(BASE, json=JOB_BODY)
    r = scheduler_client.delete(f"{BASE}/my-job")
    assert r.status_code == 204
    r2 = scheduler_client.get(f"{BASE}/my-job")
    assert r2.status_code == 404


def test_update_job(scheduler_client):
    scheduler_client.post(BASE, json=JOB_BODY)
    r = scheduler_client.patch(f"{BASE}/my-job", json={"schedule": "0 * * * *", "description": "hourly"})
    assert r.status_code == 200
    assert r.json()["schedule"] == "0 * * * *"
    assert r.json()["description"] == "hourly"


# ---------------------------------------------------------------------------
# State transitions
# ---------------------------------------------------------------------------


def test_pause_and_resume(scheduler_client):
    scheduler_client.post(BASE, json=JOB_BODY)

    r = scheduler_client.post(f"{BASE}/my-job:pause")
    assert r.status_code == 200
    assert r.json()["state"] == "PAUSED"

    r2 = scheduler_client.post(f"{BASE}/my-job:resume")
    assert r2.status_code == 200
    assert r2.json()["state"] == "ENABLED"


# ---------------------------------------------------------------------------
# Force run
# ---------------------------------------------------------------------------


def test_run_job_dispatches_http(scheduler_client):
    """Force-running a job POSTs to its httpTarget URI."""
    scheduler_client.post(BASE, json=JOB_BODY)

    with patch("localgcp.services.scheduler.worker._dispatch", new_callable=AsyncMock):
        r = scheduler_client.post(f"{BASE}/my-job:run")

    assert r.status_code == 200
    assert r.json()["lastAttemptTime"] != ""


def test_run_job_records_error_on_failure(scheduler_client):
    """A failed HTTP dispatch is recorded in the job's status."""
    scheduler_client.post(BASE, json=JOB_BODY)

    with patch(
        "localgcp.services.scheduler.worker._dispatch",
        new_callable=AsyncMock,
        side_effect=Exception("connection refused"),
    ):
        r = scheduler_client.post(f"{BASE}/my-job:run")

    assert r.status_code == 200
    assert r.json()["status"].get("message") == "connection refused"


# ---------------------------------------------------------------------------
# Worker logic
# ---------------------------------------------------------------------------


def test_worker_is_due_never_run():
    from localgcp.services.scheduler.worker import _is_due
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    # Job that has never run is always due
    assert _is_due("* * * * *", "", now) is True


def test_worker_is_due_after_interval():
    from localgcp.services.scheduler.worker import _is_due
    from datetime import datetime, timezone, timedelta
    # Last run 2 minutes ago, schedule is every minute → due
    last = (datetime.now(timezone.utc) - timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    now = datetime.now(timezone.utc)
    assert _is_due("* * * * *", last, now) is True


def test_worker_not_due_just_ran():
    from localgcp.services.scheduler.worker import _is_due
    from datetime import datetime, timezone, timedelta
    # Last run 5 seconds ago, schedule is every hour → not due
    last = (datetime.now(timezone.utc) - timedelta(seconds=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
    now = datetime.now(timezone.utc)
    assert _is_due("0 * * * *", last, now) is False
