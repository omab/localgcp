"""Background asyncio worker that dispatches HTTP tasks.

The worker loop runs as a FastAPI lifespan task. It scans all RUNNING
queues every second, picks up tasks whose scheduleTime has passed, and
dispatches them via HTTP, respecting per-queue rate limits.
"""

from __future__ import annotations

import asyncio
import base64
import logging
from datetime import UTC, datetime, timedelta

import httpx

from cloudbox.services.tasks.store import get_store

logger = logging.getLogger("cloudbox.tasks.worker")

# Per-queue semaphores enforcing maxConcurrentDispatches.
# Recreated whenever max_concurrent changes.
_semaphores: dict[str, tuple[int, asyncio.Semaphore]] = {}  # queue_name → (limit, sem)


def _get_semaphore(queue_name: str, max_concurrent: int) -> asyncio.Semaphore:
    """Return (or create) a per-queue semaphore enforcing maxConcurrentDispatches.

    Args:
        queue_name (str): Full resource name of the queue.
        max_concurrent (int): Maximum number of concurrent dispatches allowed.

    Returns:
        asyncio.Semaphore: A semaphore sized to max_concurrent for this queue.
    """
    entry = _semaphores.get(queue_name)
    if entry is None or entry[0] != max_concurrent:
        sem = asyncio.Semaphore(max_concurrent)
        _semaphores[queue_name] = (max_concurrent, sem)
        return sem
    return entry[1]


def _now() -> str:
    """Return the current UTC timestamp in ISO 8601 format with millisecond precision.

    Returns:
        str: Current UTC time formatted as 'YYYY-MM-DDTHH:MM:SS.mmmZ'.
    """
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _parse_dt(s: str) -> datetime:
    """Parse an ISO 8601 UTC datetime string to a timezone-aware datetime.

    Args:
        s (str): ISO 8601 string, with or without trailing 'Z' or fractional seconds.

    Returns:
        datetime: Parsed datetime with UTC timezone.
    """
    s = s.rstrip("Z")
    if "." in s:
        return datetime.fromisoformat(s).replace(tzinfo=UTC)
    return datetime.fromisoformat(s).replace(tzinfo=UTC)


def _parse_duration_s(s: str) -> float:
    """Parse a Cloud Tasks duration string like '10s' or '0.5s' to seconds.

    Args:
        s (str): Duration string ending in 's', such as '10s' or '0.5s'.

    Returns:
        float: Duration in seconds, or 0.1 if the string cannot be parsed.
    """
    s = s.strip()
    if s.endswith("s"):
        try:
            return float(s[:-1])
        except ValueError:
            pass
    return 0.1


def _retry_delay(retry_config: dict, attempt: int) -> float:
    """Return the delay in seconds before the next retry attempt.

    Uses exponential backoff: min_backoff * 2^min(attempt-1, max_doublings),
    capped at max_backoff.

    Args:
        retry_config (dict): Queue retryConfig dict with minBackoff, maxBackoff, maxDoublings.
        attempt (int): Current dispatch attempt count (1-based).

    Returns:
        float: Seconds to wait before the next retry.
    """
    min_b = _parse_duration_s(retry_config.get("minBackoff", "0.1s"))
    max_b = _parse_duration_s(retry_config.get("maxBackoff", "3600s"))
    doublings = int(retry_config.get("maxDoublings", 16))
    exponent = min(attempt - 1, doublings)
    delay = min_b * (2**exponent)
    return min(delay, max_b)


async def dispatch_loop() -> None:
    """Run forever, dispatching tasks that are ready.

    Starts an httpx AsyncClient and calls _tick every second until cancelled.
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
            try:
                await _tick(client)
            except Exception:
                logger.exception("Worker tick error")
            await asyncio.sleep(1.0)


async def _tick(client: httpx.AsyncClient) -> None:
    """Process one dispatch tick: find and launch all ready tasks in every RUNNING queue.

    Args:
        client (httpx.AsyncClient): Shared HTTP client used for task dispatch.
    """
    store = get_store()
    now = datetime.now(UTC)

    queues = store.list("queues")
    for queue in queues:
        if queue.get("state") != "RUNNING":
            continue

        queue_name = queue["name"]
        rate_limits = queue.get("rateLimits", {})
        max_dps = float(rate_limits.get("maxDispatchesPerSecond", 500.0))
        max_concurrent = int(rate_limits.get("maxConcurrentDispatches", 1000))

        prefix = f"{queue_name}/tasks/"
        task_keys = [k for k in store.keys("tasks") if k.startswith(prefix)]

        # Collect tasks that are ready to dispatch
        ready: list[tuple[str, dict]] = []
        for task_key in task_keys:
            task = store.get("tasks", task_key)
            if task is None:
                continue
            try:
                sched = _parse_dt(task["scheduleTime"])
            except Exception:
                sched = now
            if sched > now:
                continue

            http_req = task.get("httpRequest")
            if not http_req:
                store.delete("tasks", task_key)
                continue

            ready.append((task_key, task))

        # Enforce maxDispatchesPerSecond — cap the batch started in this tick
        if max_dps > 0 and len(ready) > max_dps:
            ready = ready[: int(max_dps)]

        if not ready:
            continue

        # Dispatch concurrently, bounded by maxConcurrentDispatches
        sem = _get_semaphore(queue_name, max_concurrent)
        await asyncio.gather(
            *(
                _dispatch_with_sem(client, store, task_key, task, http_req, sem)
                for task_key, task in ready
                for http_req in [task["httpRequest"]]
            )
        )


async def _dispatch_with_sem(
    client: httpx.AsyncClient,
    store,
    task_key: str,
    task: dict,
    http_req: dict,
    sem: asyncio.Semaphore,
) -> None:
    """Acquire the semaphore then dispatch a task.

    Args:
        client (httpx.AsyncClient): Shared HTTP client used for task dispatch.
        store (NamespacedStore): The Cloud Tasks store instance.
        task_key (str): Store key for the task.
        task (dict): Task data dict.
        http_req (dict): HTTP request configuration from the task.
        sem (asyncio.Semaphore): Semaphore limiting concurrent dispatches for the queue.
    """
    async with sem:
        await _dispatch(client, store, task_key, task, http_req)


async def _dispatch(client, store, task_key: str, task: dict, http_req: dict) -> None:
    """Execute the HTTP request for a task, update attempt metadata, and handle retries.

    Args:
        client (httpx.AsyncClient): Shared HTTP client used for the request.
        store (NamespacedStore): The Cloud Tasks store instance.
        task_key (str): Store key for the task.
        task (dict): Task data dict, mutated in place with attempt metadata.
        http_req (dict): HTTP request configuration containing url, httpMethod, headers, body.
    """
    url = http_req.get("url", "")
    method = http_req.get("httpMethod", "POST")
    headers = dict(http_req.get("headers", {}))
    body_b64 = http_req.get("body", "")
    body = base64.b64decode(body_b64) if body_b64 else b""

    now = _now()
    task["dispatchCount"] = task.get("dispatchCount", 0) + 1
    attempt = {
        "scheduleTime": task["scheduleTime"],
        "dispatchTime": now,
    }
    if not task.get("firstAttempt"):
        task["firstAttempt"] = attempt
    task["lastAttempt"] = attempt

    try:
        response = await client.request(method, url, headers=headers, content=body)
        task["responseCount"] = task.get("responseCount", 0) + 1
        task["lastAttempt"]["responseTime"] = _now()
        task["lastAttempt"]["responseStatus"] = {"code": response.status_code}

        if 200 <= response.status_code < 300:
            logger.info("Task %s dispatched successfully (%d)", task_key, response.status_code)
            store.delete("tasks", task_key)
            return

        logger.warning("Task %s returned %d", task_key, response.status_code)
    except Exception as exc:
        logger.warning("Task %s dispatch error: %s", task_key, exc)

    # Retry logic
    queue_name = task_key.rsplit("/tasks/", 1)[0]
    queue = store.get("queues", queue_name)
    retry_config = (queue or {}).get("retryConfig", {})
    max_attempts = int(retry_config.get("maxAttempts", 100))

    if task["dispatchCount"] >= max_attempts:
        logger.warning("Task %s exceeded maxAttempts (%d), dropping", task_key, max_attempts)
        store.delete("tasks", task_key)
        return

    delay = _retry_delay(retry_config, task["dispatchCount"])
    next_time = datetime.now(UTC) + timedelta(seconds=delay)
    task["scheduleTime"] = next_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    logger.debug("Task %s retry %d in %.2fs", task_key, task["dispatchCount"], delay)
    store.set("tasks", task_key, task)
