"""Background asyncio worker that fires scheduled jobs.

Polls every 30 seconds, checks all ENABLED jobs against their cron schedule,
and dispatches HTTP requests for any that are due.

Retry logic:
  When a dispatch fails, the job is retried up to retryConfig.retryCount times
  using exponential backoff (minBackoffDuration * 2^min(attempt-1, maxDoublings),
  capped at maxBackoffDuration). Retries are also bounded by maxRetryDuration
  (total wall-clock time from the first failure). Retry state is stored as
  ephemeral _retry* fields on the job dict.
"""

from __future__ import annotations

import asyncio
import base64
import logging
from datetime import UTC, datetime, timedelta

import httpx
from croniter import croniter

from cloudbox.services.scheduler.store import get_store

logger = logging.getLogger("cloudbox.scheduler.worker")

_POLL_INTERVAL = 30  # seconds


def _now_utc() -> datetime:
    """Return the current UTC datetime.

    Returns:
        datetime: Current time as a timezone-aware UTC datetime.
    """
    return datetime.now(UTC)


def _parse_dt(s: str) -> datetime | None:
    """Parse an ISO 8601 string to a timezone-aware UTC datetime.

    Args:
        s (str): ISO 8601 datetime string, optionally ending with 'Z'.

    Returns:
        datetime | None: Parsed UTC datetime, or None if the string is empty or invalid.
    """
    if not s:
        return None
    try:
        s = s.rstrip("Z")
        dt = datetime.fromisoformat(s)
        return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt
    except Exception:
        return None


def _parse_duration_s(s: str) -> float:
    """Parse a duration string like '5s', '30m', '1h', or '1h30m' to seconds.

    Args:
        s (str): Duration string using h/m/s unit suffixes.

    Returns:
        float: Total duration in seconds, or 0.0 if the string is empty or '0s'.
    """
    s = s.strip()
    if not s or s == "0s":
        return 0.0
    total = 0.0
    import re

    for value, unit in re.findall(r"(\d+(?:\.\d+)?)([smh])", s):
        v = float(value)
        if unit == "s":
            total += v
        elif unit == "m":
            total += v * 60
        elif unit == "h":
            total += v * 3600
    return total


def _retry_backoff(retry_config: dict, attempt: int) -> float:
    """Return seconds to wait before retry attempt N (1-based).

    Args:
        retry_config (dict): Job retryConfig dict with minBackoffDuration, maxBackoffDuration,
            and maxDoublings.
        attempt (int): 1-based retry attempt number.

    Returns:
        float: Seconds to wait before the retry, capped at maxBackoffDuration.
    """
    min_b = _parse_duration_s(retry_config.get("minBackoffDuration", "5s"))
    max_b = _parse_duration_s(retry_config.get("maxBackoffDuration", "3600s"))
    doublings = int(retry_config.get("maxDoublings", 5))
    exponent = min(attempt - 1, doublings)
    return min(min_b * (2**exponent), max_b)


def _is_due(schedule: str, last_attempt: str, now: datetime) -> bool:
    """Return True if the job's schedule has fired since the last attempt.

    Args:
        schedule (str): Cron expression for the job schedule.
        last_attempt (str): ISO 8601 timestamp of the last attempt, or empty string.
        now (datetime): Current UTC datetime to compare against.

    Returns:
        bool: True if the next scheduled occurrence after last_attempt is at or before now.
    """
    try:
        last = _parse_dt(last_attempt)
        if last is None:
            return True  # Never run — fire immediately on first poll
        cron = croniter(schedule, last)
        next_run = cron.get_next(datetime).replace(tzinfo=UTC)
        return next_run <= now
    except Exception:
        logger.warning("Invalid cron schedule '%s'", schedule)
        return False


def _next_run_time(schedule: str, base: datetime) -> str:
    """Return ISO string of the next scheduled time after base.

    Args:
        schedule (str): Cron expression for the job schedule.
        base (datetime): Reference datetime to compute the next occurrence from.

    Returns:
        str: ISO 8601 UTC timestamp of the next run, or empty string if schedule is invalid.
    """
    try:
        cron = croniter(schedule, base)
        nxt = cron.get_next(datetime).replace(tzinfo=UTC)
        return nxt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return ""


async def dispatch_loop() -> None:
    """Run forever, dispatching jobs that are due.

    Starts an httpx AsyncClient and calls _tick every _POLL_INTERVAL seconds until cancelled.
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
            try:
                await _tick(client)
            except Exception:
                logger.exception("Scheduler worker tick error")
            await asyncio.sleep(_POLL_INTERVAL)


async def _tick(client: httpx.AsyncClient) -> None:
    """Process one scheduler poll: dispatch all ENABLED jobs that are due.

    Args:
        client (httpx.AsyncClient): Shared HTTP client used for job dispatch.
    """
    store = get_store()
    now = _now_utc()
    now_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    for job in store.list("jobs"):
        if job.get("state") != "ENABLED":
            continue

        schedule = job.get("schedule", "")
        retry_attempt = job.get("_retryAttempt", 0)

        if retry_attempt > 0:
            # Job is in retry state — check if the retry delay has elapsed
            next_retry = _parse_dt(job.get("_nextRetryTime", ""))
            if next_retry and next_retry > now:
                continue
        else:
            # Normal schedule check
            if not schedule:
                continue
            if not _is_due(schedule, job.get("lastAttemptTime", ""), now):
                continue

        http_target = job.get("httpTarget")
        if not http_target:
            continue

        job_name = job["name"]
        job["lastAttemptTime"] = now_str

        if retry_attempt == 0:
            # First attempt: advance the schedule and record retry start time
            job["scheduleTime"] = _next_run_time(schedule, now)
            job["_retryStartTime"] = now_str
            logger.info("Firing scheduled job %s", job_name)
        else:
            logger.info("Retrying job %s (attempt %d)", job_name, retry_attempt + 1)

        try:
            await _dispatch(client, http_target)
            job["status"] = {}
            _clear_retry_state(job)
        except Exception as exc:
            logger.warning(
                "Job %s dispatch failed (attempt %d): %s", job_name, retry_attempt + 1, exc
            )
            job["status"] = {"code": 2, "message": str(exc)}
            _schedule_retry(job, retry_attempt, now)

        store.set("jobs", job_name, job)


def _schedule_retry(job: dict, current_attempt: int, now: datetime) -> None:
    """Set retry state on the job dict, or clear it if retries are exhausted.

    Args:
        job (dict): Job data dict, mutated in place with retry state fields.
        current_attempt (int): Zero-based index of the attempt that just failed.
        now (datetime): Current UTC datetime used to compute the next retry time.
    """
    retry_config = job.get("retryConfig", {})
    max_retries = int(retry_config.get("retryCount", 0))

    if max_retries <= 0:
        _clear_retry_state(job)
        return

    next_attempt = current_attempt + 1
    if next_attempt > max_retries:
        logger.warning("Job %s exhausted %d retries", job["name"], max_retries)
        _clear_retry_state(job)
        return

    # Check maxRetryDuration
    max_dur_s = _parse_duration_s(retry_config.get("maxRetryDuration", "0s"))
    if max_dur_s > 0:
        retry_start = _parse_dt(job.get("_retryStartTime", "")) or now
        elapsed = (now - retry_start).total_seconds()
        if elapsed >= max_dur_s:
            logger.warning("Job %s exceeded maxRetryDuration (%.0fs)", job["name"], max_dur_s)
            _clear_retry_state(job)
            return

    delay = _retry_backoff(retry_config, next_attempt)
    next_retry_dt = now + timedelta(seconds=delay)
    job["_retryAttempt"] = next_attempt
    job["_nextRetryTime"] = next_retry_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    logger.debug("Job %s scheduled for retry %d in %.1fs", job["name"], next_attempt, delay)


def _clear_retry_state(job: dict) -> None:
    """Remove all ephemeral retry state fields from a job dict.

    Args:
        job (dict): Job data dict, mutated in place to remove _retryAttempt,
            _nextRetryTime, and _retryStartTime keys.
    """
    for key in ("_retryAttempt", "_nextRetryTime", "_retryStartTime"):
        job.pop(key, None)


async def _dispatch(client: httpx.AsyncClient, http_target: dict) -> None:
    """Send an HTTP request for a scheduled job's httpTarget.

    Args:
        client (httpx.AsyncClient): Shared HTTP client used for the request.
        http_target (dict): Job httpTarget dict containing uri, httpMethod, headers, and body.

    Raises:
        RuntimeError: If the response status code is 400 or higher.
    """
    uri = http_target.get("uri", "")
    method = http_target.get("httpMethod", "POST")
    headers = dict(http_target.get("headers", {}))
    body_b64 = http_target.get("body", "")
    body = base64.b64decode(body_b64) if body_b64 else b""

    resp = await client.request(method, uri, headers=headers, content=body)
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code}")
