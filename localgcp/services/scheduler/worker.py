"""Background asyncio worker that fires scheduled jobs.

Polls every 30 seconds, checks all ENABLED jobs against their cron schedule,
and dispatches HTTP requests for any that are due.
"""
from __future__ import annotations

import asyncio
import base64
import logging
from datetime import datetime, timezone

import httpx
from croniter import croniter

from localgcp.services.scheduler.store import get_store

logger = logging.getLogger("localgcp.scheduler.worker")

_POLL_INTERVAL = 30  # seconds


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_dt(s: str) -> datetime | None:
    if not s:
        return None
    try:
        s = s.rstrip("Z")
        dt = datetime.fromisoformat(s)
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
    except Exception:
        return None


def _is_due(schedule: str, last_attempt: str, now: datetime) -> bool:
    """Return True if the job's schedule has fired since the last attempt."""
    try:
        last = _parse_dt(last_attempt)
        # base: the point from which we look for the next occurrence
        if last is None:
            # Never run — fire immediately on first poll
            return True
        cron = croniter(schedule, last)
        next_run = cron.get_next(datetime).replace(tzinfo=timezone.utc)
        return next_run <= now
    except Exception:
        logger.warning("Invalid cron schedule '%s'", schedule)
        return False


def _next_run_time(schedule: str, base: datetime) -> str:
    """Return ISO string of the next scheduled time after base."""
    try:
        cron = croniter(schedule, base)
        nxt = cron.get_next(datetime).replace(tzinfo=timezone.utc)
        return nxt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return ""


async def dispatch_loop() -> None:
    """Run forever, dispatching jobs that are due."""
    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
            try:
                await _tick(client)
            except Exception:
                logger.exception("Scheduler worker tick error")
            await asyncio.sleep(_POLL_INTERVAL)


async def _tick(client: httpx.AsyncClient) -> None:
    store = get_store()
    now = _now_utc()
    now_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    for job in store.list("jobs"):
        if job.get("state") != "ENABLED":
            continue
        schedule = job.get("schedule", "")
        if not schedule:
            continue
        if not _is_due(schedule, job.get("lastAttemptTime", ""), now):
            continue

        http_target = job.get("httpTarget")
        if not http_target:
            continue

        job_name = job["name"]
        logger.info("Firing scheduled job %s", job_name)
        job["lastAttemptTime"] = now_str
        job["scheduleTime"] = _next_run_time(schedule, now)

        try:
            await _dispatch(client, http_target)
            job["status"] = {}
        except Exception as exc:
            logger.warning("Job %s dispatch failed: %s", job_name, exc)
            job["status"] = {"code": 2, "message": str(exc)}

        store.set("jobs", job_name, job)


async def _dispatch(client: httpx.AsyncClient, http_target: dict) -> None:
    uri = http_target.get("uri", "")
    method = http_target.get("httpMethod", "POST")
    headers = dict(http_target.get("headers", {}))
    body_b64 = http_target.get("body", "")
    body = base64.b64decode(body_b64) if body_b64 else b""

    resp = await client.request(method, uri, headers=headers, content=body)
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code}")
