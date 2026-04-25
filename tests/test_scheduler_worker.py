"""Unit tests for the Cloud Scheduler background worker."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

PROJECT = "local-project"
LOCATION = "us-central1"


def _job(name="test-job", state="ENABLED", last_attempt="", schedule="* * * * *"):
    return {
        "name": f"projects/{PROJECT}/locations/{LOCATION}/jobs/{name}",
        "state": state,
        "schedule": schedule,
        "timeZone": "UTC",
        "httpTarget": {"uri": "http://localhost:9999/handler", "httpMethod": "POST"},
        "lastAttemptTime": last_attempt,
        "scheduleTime": "",
        "status": {},
    }


# ---------------------------------------------------------------------------
# _parse_dt
# ---------------------------------------------------------------------------


def test_parse_dt_valid():
    from cloudbox.services.scheduler.worker import _parse_dt

    dt = _parse_dt("2024-01-15T12:00:00Z")
    assert dt is not None
    assert dt.year == 2024


def test_parse_dt_empty_returns_none():
    from cloudbox.services.scheduler.worker import _parse_dt

    assert _parse_dt("") is None


def test_parse_dt_invalid_returns_none():
    from cloudbox.services.scheduler.worker import _parse_dt

    assert _parse_dt("not-a-date") is None


# ---------------------------------------------------------------------------
# _next_run_time
# ---------------------------------------------------------------------------


def test_next_run_time_returns_future():
    from cloudbox.services.scheduler.worker import _next_run_time

    base = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    result = _next_run_time("* * * * *", base)
    assert result > base.strftime("%Y-%m-%dT%H:%M:%SZ")


def test_next_run_time_invalid_schedule_returns_empty():
    from cloudbox.services.scheduler.worker import _next_run_time

    base = datetime.now(UTC)
    result = _next_run_time("not a cron", base)
    assert result == ""


# ---------------------------------------------------------------------------
# _dispatch_http (the low-level HTTP sender)
# ---------------------------------------------------------------------------


async def test_dispatch_http_sends_request():
    from cloudbox.services.scheduler.worker import _dispatch_http

    mock_resp = MagicMock()
    mock_resp.status_code = 200

    mock_client = AsyncMock()
    mock_client.request = AsyncMock(return_value=mock_resp)

    await _dispatch_http(mock_client, {"uri": "http://example.com/cb", "httpMethod": "POST"})

    mock_client.request.assert_called_once_with(
        "POST", "http://example.com/cb", headers={}, content=b""
    )


async def test_dispatch_http_raises_on_4xx():
    import pytest

    from cloudbox.services.scheduler.worker import _dispatch_http

    mock_resp = MagicMock()
    mock_resp.status_code = 500

    mock_client = AsyncMock()
    mock_client.request = AsyncMock(return_value=mock_resp)

    with pytest.raises(RuntimeError, match="HTTP 500"):
        await _dispatch_http(mock_client, {"uri": "http://example.com/cb"})


async def test_dispatch_http_sends_body_and_headers():
    import base64

    from cloudbox.services.scheduler.worker import _dispatch_http

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_client = AsyncMock()
    mock_client.request = AsyncMock(return_value=mock_resp)

    body_b64 = base64.b64encode(b"hello").decode()
    await _dispatch_http(
        mock_client,
        {
            "uri": "http://example.com/",
            "httpMethod": "PUT",
            "headers": {"X-Key": "val"},
            "body": body_b64,
        },
    )

    _, kwargs = mock_client.request.call_args
    assert kwargs["content"] == b"hello"
    assert kwargs["headers"] == {"X-Key": "val"}


# ---------------------------------------------------------------------------
# _tick
# ---------------------------------------------------------------------------


async def test_tick_dispatches_due_job(reset_stores):
    from cloudbox.services.scheduler import store as sched_store
    from cloudbox.services.scheduler.worker import _tick

    store = sched_store.get_store()
    job = _job(last_attempt="")  # never run → always due
    store.set("jobs", job["name"], job)

    with patch("cloudbox.services.scheduler.worker._dispatch", new_callable=AsyncMock) as mock_d:
        await _tick()

    mock_d.assert_called_once()
    updated = store.get("jobs", job["name"])
    assert updated["lastAttemptTime"] != ""
    assert updated["status"] == {}


async def test_tick_skips_paused_job(reset_stores):
    from cloudbox.services.scheduler import store as sched_store
    from cloudbox.services.scheduler.worker import _tick

    store = sched_store.get_store()
    job = _job(state="PAUSED")
    store.set("jobs", job["name"], job)

    with patch("cloudbox.services.scheduler.worker._dispatch", new_callable=AsyncMock) as mock_d:
        await _tick()

    mock_d.assert_not_called()


async def test_tick_records_error_on_failed_dispatch(reset_stores):
    from cloudbox.services.scheduler import store as sched_store
    from cloudbox.services.scheduler.worker import _tick

    store = sched_store.get_store()
    job = _job(last_attempt="")
    store.set("jobs", job["name"], job)

    with patch(
        "cloudbox.services.scheduler.worker._dispatch",
        new_callable=AsyncMock,
        side_effect=Exception("timeout"),
    ):
        await _tick()

    updated = store.get("jobs", job["name"])
    assert updated["status"]["message"] == "timeout"


async def test_tick_skips_not_yet_due_job(reset_stores):
    from cloudbox.services.scheduler import store as sched_store
    from cloudbox.services.scheduler.worker import _tick

    store = sched_store.get_store()
    # Last attempt was 5 seconds ago, schedule is hourly → not due
    last = (datetime.now(UTC) - timedelta(seconds=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
    job = _job(last_attempt=last, schedule="0 * * * *")
    store.set("jobs", job["name"], job)

    with patch("cloudbox.services.scheduler.worker._dispatch", new_callable=AsyncMock) as mock_d:
        await _tick()

    mock_d.assert_not_called()


async def test_tick_skips_job_without_any_target(reset_stores):
    from cloudbox.services.scheduler import store as sched_store
    from cloudbox.services.scheduler.worker import _tick

    store = sched_store.get_store()
    job = _job()
    job.pop("httpTarget")
    store.set("jobs", job["name"], job)

    with patch("cloudbox.services.scheduler.worker._dispatch", new_callable=AsyncMock) as mock_d:
        await _tick()

    mock_d.assert_not_called()
