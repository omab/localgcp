"""Tests for Cloud Scheduler emulator."""

from datetime import UTC
from unittest.mock import AsyncMock, patch

import pytest

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
        scheduler_client.post(
            BASE,
            json={
                **JOB_BODY,
                "name": f"projects/{PROJECT}/locations/{LOCATION}/jobs/job-{i}",
            },
        )
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
    r = scheduler_client.patch(
        f"{BASE}/my-job", json={"schedule": "0 * * * *", "description": "hourly"}
    )
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

    with patch("cloudbox.services.scheduler.worker._dispatch", new_callable=AsyncMock):
        r = scheduler_client.post(f"{BASE}/my-job:run")

    assert r.status_code == 200
    assert r.json()["lastAttemptTime"] != ""


def test_run_job_records_error_on_failure(scheduler_client):
    """A failed HTTP dispatch is recorded in the job's status."""
    scheduler_client.post(BASE, json=JOB_BODY)

    with patch(
        "cloudbox.services.scheduler.worker._dispatch",
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
    from datetime import datetime

    from cloudbox.services.scheduler.worker import _is_due

    now = datetime.now(UTC)
    # Job that has never run is always due
    assert _is_due("* * * * *", "", now) is True


def test_worker_is_due_after_interval():
    from datetime import datetime, timedelta

    from cloudbox.services.scheduler.worker import _is_due

    # Last run 2 minutes ago, schedule is every minute → due
    last = (datetime.now(UTC) - timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    now = datetime.now(UTC)
    assert _is_due("* * * * *", last, now) is True


def test_worker_not_due_just_ran():
    from datetime import datetime, timedelta

    from cloudbox.services.scheduler.worker import _is_due

    # Last run 5 seconds ago, schedule is every hour → not due
    last = (datetime.now(UTC) - timedelta(seconds=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
    now = datetime.now(UTC)
    assert _is_due("0 * * * *", last, now) is False


# ---------------------------------------------------------------------------
# Retry backoff helpers
# ---------------------------------------------------------------------------


def test_parse_duration_s():
    from cloudbox.services.scheduler.worker import _parse_duration_s

    assert _parse_duration_s("0s") == 0.0
    assert _parse_duration_s("5s") == 5.0
    assert _parse_duration_s("30m") == 1800.0
    assert _parse_duration_s("1h") == 3600.0
    assert _parse_duration_s("1h30m") == 5400.0


def test_retry_backoff():
    from cloudbox.services.scheduler.worker import _retry_backoff

    cfg = {"minBackoffDuration": "5s", "maxBackoffDuration": "300s", "maxDoublings": 3}
    assert _retry_backoff(cfg, 1) == pytest.approx(5.0)  # 5 * 2^0
    assert _retry_backoff(cfg, 2) == pytest.approx(10.0)  # 5 * 2^1
    assert _retry_backoff(cfg, 3) == pytest.approx(20.0)  # 5 * 2^2
    assert _retry_backoff(cfg, 4) == pytest.approx(40.0)  # 5 * 2^3 (capped at maxDoublings)
    assert _retry_backoff(cfg, 5) == pytest.approx(40.0)  # still capped at maxDoublings


def test_retry_backoff_capped_at_max():
    from cloudbox.services.scheduler.worker import _retry_backoff

    cfg = {"minBackoffDuration": "60s", "maxBackoffDuration": "100s", "maxDoublings": 10}
    assert _retry_backoff(cfg, 3) == pytest.approx(100.0)  # 60*4=240 capped to 100


def test_schedule_retry_sets_state():
    from datetime import datetime

    from cloudbox.services.scheduler.worker import _schedule_retry

    now = datetime.now(UTC)
    job = {
        "name": "projects/p/locations/l/jobs/j",
        "retryConfig": {
            "retryCount": 3,
            "minBackoffDuration": "1s",
            "maxBackoffDuration": "60s",
            "maxDoublings": 5,
        },
        "_retryStartTime": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    _schedule_retry(job, 0, now)
    assert job["_retryAttempt"] == 1
    assert "_nextRetryTime" in job


def test_schedule_retry_exhausted():
    from datetime import datetime

    from cloudbox.services.scheduler.worker import _schedule_retry

    now = datetime.now(UTC)
    job = {
        "name": "projects/p/locations/l/jobs/j",
        "retryConfig": {"retryCount": 2},
        "_retryStartTime": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "_retryAttempt": 2,
    }
    _schedule_retry(job, 2, now)  # attempt 2 of 2 — exhausted
    assert "_retryAttempt" not in job


def test_schedule_retry_max_duration_exceeded():
    from datetime import datetime, timedelta

    from cloudbox.services.scheduler.worker import _schedule_retry

    now = datetime.now(UTC)
    start = now - timedelta(seconds=120)  # started 2 minutes ago
    job = {
        "name": "projects/p/locations/l/jobs/j",
        "retryConfig": {"retryCount": 10, "maxRetryDuration": "60s"},
        "_retryStartTime": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    _schedule_retry(job, 0, now)
    assert "_retryAttempt" not in job  # duration exceeded → no retry


def test_schedule_retry_no_retry_count():
    from datetime import datetime

    from cloudbox.services.scheduler.worker import _schedule_retry

    now = datetime.now(UTC)
    job = {
        "name": "projects/p/locations/l/jobs/j",
        "retryConfig": {"retryCount": 0},
    }
    _schedule_retry(job, 0, now)
    assert "_retryAttempt" not in job


def test_update_job_retry_config(scheduler_client):
    """Verify retryConfig is stored and returned by the API."""
    scheduler_client.post(
        BASE,
        json={
            "name": f"{BASE.replace('/jobs', '')}/jobs/retry-job",
            "schedule": "* * * * *",
            "timeZone": "UTC",
            "httpTarget": {"uri": "http://localhost:9999/noop"},
            "retryConfig": {
                "retryCount": 5,
                "minBackoffDuration": "2s",
                "maxBackoffDuration": "120s",
                "maxDoublings": 4,
                "maxRetryDuration": "10m",
            },
        },
    )
    r = scheduler_client.get(f"{BASE}/retry-job")
    assert r.status_code == 200
    rc = r.json()["retryConfig"]
    assert rc["retryCount"] == 5
    assert rc["minBackoffDuration"] == "2s"
    assert rc["maxBackoffDuration"] == "120s"
    assert rc["maxDoublings"] == 4
    assert rc["maxRetryDuration"] == "10m"


# ---------------------------------------------------------------------------
# Missing-resource 404 / 400 paths
# ---------------------------------------------------------------------------


def test_create_job_without_name_returns_400(scheduler_client):
    r = scheduler_client.post(BASE, json={"schedule": "* * * * *", "timeZone": "UTC"})
    assert r.status_code == 400


def test_get_missing_job_returns_404(scheduler_client):
    r = scheduler_client.get(f"{BASE}/no-such-job")
    assert r.status_code == 404


def test_update_missing_job_returns_404(scheduler_client):
    r = scheduler_client.patch(f"{BASE}/no-such-job", json={"description": "x"})
    assert r.status_code == 404


def test_delete_missing_job_returns_404(scheduler_client):
    r = scheduler_client.delete(f"{BASE}/no-such-job")
    assert r.status_code == 404


def test_run_missing_job_returns_404(scheduler_client):
    r = scheduler_client.post(f"{BASE}/no-such-job:run")
    assert r.status_code == 404


def test_pause_missing_job_returns_404(scheduler_client):
    r = scheduler_client.post(f"{BASE}/no-such-job:pause")
    assert r.status_code == 404


def test_resume_missing_job_returns_404(scheduler_client):
    r = scheduler_client.post(f"{BASE}/no-such-job:resume")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Pub/Sub target
# ---------------------------------------------------------------------------


def test_create_pubsub_target_job(scheduler_client, pubsub_client):
    topic = f"projects/{PROJECT}/topics/sched-topic"
    pubsub_client.put(f"/v1/{topic}")
    sub = f"projects/{PROJECT}/subscriptions/sched-sub"
    pubsub_client.put(f"/v1/{sub}", json={"name": sub, "topic": topic})

    r = scheduler_client.post(
        BASE,
        json={
            "name": f"{BASE.replace('/jobs', '')}/jobs/ps-job",
            "schedule": "* * * * *",
            "timeZone": "UTC",
            "pubsubTarget": {
                "topicName": topic,
                "data": "aGVsbG8=",  # base64("hello")
                "attributes": {"env": "test"},
            },
        },
    )
    assert r.status_code == 200
    body = r.json()
    assert body["pubsubTarget"]["topicName"] == topic


def test_run_pubsub_target_job_delivers_message(scheduler_client, pubsub_client):
    topic = f"projects/{PROJECT}/topics/run-ps-topic"
    pubsub_client.put(f"/v1/{topic}")
    sub = f"projects/{PROJECT}/subscriptions/run-ps-sub"
    pubsub_client.put(f"/v1/{sub}", json={"name": sub, "topic": topic})

    scheduler_client.post(
        BASE,
        json={
            "name": f"{BASE.replace('/jobs', '')}/jobs/run-ps-job",
            "schedule": "* * * * *",
            "timeZone": "UTC",
            "pubsubTarget": {
                "topicName": topic,
                "data": "aGVsbG8=",
            },
        },
    )

    r = scheduler_client.post(f"{BASE}/run-ps-job:run")
    assert r.status_code == 200
    assert r.json()["lastAttemptTime"] != ""

    r2 = pubsub_client.post(f"/v1/{sub}:pull", json={"maxMessages": 1})
    msgs = r2.json()["receivedMessages"]
    assert len(msgs) == 1
    assert msgs[0]["message"]["data"] == "aGVsbG8="


def test_run_pubsub_job_missing_topic_records_error(scheduler_client):
    scheduler_client.post(
        BASE,
        json={
            "name": f"{BASE.replace('/jobs', '')}/jobs/bad-ps-job",
            "schedule": "* * * * *",
            "timeZone": "UTC",
            "pubsubTarget": {"topicName": "projects/x/topics/ghost"},
        },
    )

    r = scheduler_client.post(f"{BASE}/bad-ps-job:run")
    assert r.status_code == 200
    assert "message" in r.json()["status"]
