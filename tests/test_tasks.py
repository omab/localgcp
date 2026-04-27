"""Tests for Cloud Tasks emulator."""

from datetime import UTC

import pytest

PROJECT = "local-project"
LOCATION = "us-central1"
BASE = f"/v2/projects/{PROJECT}/locations/{LOCATION}"


def test_create_and_get_queue(tasks_client):
    r = tasks_client.post(
        f"{BASE}/queues",
        json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/my-queue"},
    )
    assert r.status_code == 200
    assert r.json()["name"].endswith("/queues/my-queue")

    r = tasks_client.get(f"{BASE}/queues/my-queue")
    assert r.status_code == 200


def test_list_queues(tasks_client):
    for qid in ("q1", "q2"):
        tasks_client.post(
            f"{BASE}/queues",
            json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/{qid}"},
        )
    r = tasks_client.get(f"{BASE}/queues")
    assert r.status_code == 200
    names = [q["name"].split("/")[-1] for q in r.json()["queues"]]
    assert {"q1", "q2"}.issubset(set(names))


def test_duplicate_queue_returns_409(tasks_client):
    tasks_client.post(
        f"{BASE}/queues",
        json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/dup-q"},
    )
    r = tasks_client.post(
        f"{BASE}/queues",
        json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/dup-q"},
    )
    assert r.status_code == 409


def test_create_and_list_tasks(tasks_client):
    tasks_client.post(
        f"{BASE}/queues",
        json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/work"},
    )
    r = tasks_client.post(
        f"{BASE}/queues/work/tasks",
        json={
            "task": {
                "httpRequest": {"url": "http://example.com/task", "httpMethod": "POST"},
            }
        },
    )
    assert r.status_code == 200
    task_name = r.json()["name"]

    r = tasks_client.get(f"{BASE}/queues/work/tasks")
    assert r.status_code == 200
    names = [t["name"] for t in r.json()["tasks"]]
    assert task_name in names


def test_delete_task(tasks_client):
    tasks_client.post(
        f"{BASE}/queues",
        json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/del-q"},
    )
    r = tasks_client.post(
        f"{BASE}/queues/del-q/tasks",
        json={"task": {"httpRequest": {"url": "http://example.com"}}},
    )
    task_id = r.json()["name"].split("/tasks/")[1]

    r = tasks_client.delete(f"{BASE}/queues/del-q/tasks/{task_id}")
    assert r.status_code == 200

    r = tasks_client.get(f"{BASE}/queues/del-q/tasks/{task_id}")
    assert r.status_code == 404


def test_pause_and_resume_queue(tasks_client):
    tasks_client.post(
        f"{BASE}/queues",
        json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/pausable"},
    )
    r = tasks_client.post(f"{BASE}/queues/pausable:pause")
    assert r.status_code == 200
    assert r.json()["state"] == "PAUSED"

    r = tasks_client.post(f"{BASE}/queues/pausable:resume")
    assert r.status_code == 200
    assert r.json()["state"] == "RUNNING"


def test_purge_queue(tasks_client):
    tasks_client.post(
        f"{BASE}/queues",
        json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/purgeable"},
    )
    for _ in range(3):
        tasks_client.post(
            f"{BASE}/queues/purgeable/tasks",
            json={"task": {"httpRequest": {"url": "http://example.com"}}},
        )
    tasks_client.post(f"{BASE}/queues/purgeable:purge")
    r = tasks_client.get(f"{BASE}/queues/purgeable/tasks")
    assert r.json()["tasks"] == []


def test_delete_queue_removes_tasks(tasks_client):
    tasks_client.post(
        f"{BASE}/queues",
        json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/cascade-q"},
    )
    tasks_client.post(
        f"{BASE}/queues/cascade-q/tasks",
        json={"task": {"httpRequest": {"url": "http://example.com"}}},
    )
    tasks_client.delete(f"{BASE}/queues/cascade-q")
    r = tasks_client.get(f"{BASE}/queues/cascade-q")
    assert r.status_code == 404


def test_get_task(tasks_client):
    tasks_client.post(
        f"{BASE}/queues",
        json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/get-q"},
    )
    r = tasks_client.post(
        f"{BASE}/queues/get-q/tasks",
        json={"task": {"httpRequest": {"url": "http://example.com/work"}}},
    )
    task_id = r.json()["name"].split("/tasks/")[1]

    r2 = tasks_client.get(f"{BASE}/queues/get-q/tasks/{task_id}")
    assert r2.status_code == 200
    assert r2.json()["httpRequest"]["url"] == "http://example.com/work"


def test_task_with_headers_and_body(tasks_client):
    import base64

    tasks_client.post(
        f"{BASE}/queues",
        json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/hdr-q"},
    )
    body_b64 = base64.b64encode(b'{"key":"value"}').decode()
    r = tasks_client.post(
        f"{BASE}/queues/hdr-q/tasks",
        json={
            "task": {
                "httpRequest": {
                    "url": "http://example.com/endpoint",
                    "httpMethod": "POST",
                    "headers": {"Content-Type": "application/json", "X-Custom": "header"},
                    "body": body_b64,
                }
            }
        },
    )
    assert r.status_code == 200
    task = r.json()
    assert task["httpRequest"]["headers"]["X-Custom"] == "header"
    assert task["httpRequest"]["body"] == body_b64


def test_duplicate_task_name_returns_409(tasks_client):
    tasks_client.post(
        f"{BASE}/queues",
        json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/dedup-q"},
    )
    task_body = {
        "task": {
            "name": f"projects/{PROJECT}/locations/{LOCATION}/queues/dedup-q/tasks/task-1",
            "httpRequest": {"url": "http://example.com"},
        }
    }
    tasks_client.post(f"{BASE}/queues/dedup-q/tasks", json=task_body)
    r = tasks_client.post(f"{BASE}/queues/dedup-q/tasks", json=task_body)
    assert r.status_code == 409


def test_update_queue_retry_config(tasks_client):
    tasks_client.post(
        f"{BASE}/queues",
        json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/update-q"},
    )
    r = tasks_client.patch(
        f"{BASE}/queues/update-q",
        json={"retryConfig": {"maxAttempts": 5}},
    )
    assert r.status_code == 200
    assert r.json()["retryConfig"]["maxAttempts"] == 5


def test_create_task_on_missing_queue_returns_404(tasks_client):
    r = tasks_client.post(
        f"{BASE}/queues/ghost-q/tasks",
        json={"task": {"httpRequest": {"url": "http://example.com"}}},
    )
    assert r.status_code == 404


def test_force_run_task(tasks_client):
    """tasks/{id}:run resets scheduleTime so the worker dispatches it immediately."""
    from datetime import datetime, timedelta

    tasks_client.post(
        f"{BASE}/queues",
        json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/run-q"},
    )
    future = (datetime.now(UTC) + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    r = tasks_client.post(
        f"{BASE}/queues/run-q/tasks",
        json={
            "task": {
                "httpRequest": {"url": "http://example.com"},
                "scheduleTime": future,
            }
        },
    )
    task_id = r.json()["name"].split("/tasks/")[1]

    r2 = tasks_client.post(f"{BASE}/queues/run-q/tasks/{task_id}:run")
    assert r2.status_code == 200
    # scheduleTime should have been reset to approximately now (not the future)
    from datetime import datetime

    sched = datetime.fromisoformat(r2.json()["scheduleTime"].rstrip("Z")).replace(tzinfo=UTC)
    assert sched <= datetime.now(UTC) + timedelta(seconds=5)


def test_update_missing_queue_returns_404(tasks_client):
    r = tasks_client.patch(
        f"{BASE}/queues/no-such-queue",
        json={"rateLimits": {"maxDispatchesPerSecond": 5}},
    )
    assert r.status_code == 404


def test_delete_missing_queue_returns_404(tasks_client):
    r = tasks_client.delete(f"{BASE}/queues/no-such-queue")
    assert r.status_code == 404


def test_purge_missing_queue_returns_404(tasks_client):
    r = tasks_client.post(f"{BASE}/queues/no-such-queue:purge")
    assert r.status_code == 404


def test_pause_missing_queue_returns_404(tasks_client):
    r = tasks_client.post(f"{BASE}/queues/no-such-queue:pause")
    assert r.status_code == 404


def test_resume_missing_queue_returns_404(tasks_client):
    r = tasks_client.post(f"{BASE}/queues/no-such-queue:resume")
    assert r.status_code == 404


def test_get_missing_task_returns_404(tasks_client):
    tasks_client.post(
        f"{BASE}/queues",
        json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/tsk-q"},
    )
    r = tasks_client.get(f"{BASE}/queues/tsk-q/tasks/no-such-task")
    assert r.status_code == 404


def test_delete_missing_task_returns_404(tasks_client):
    tasks_client.post(
        f"{BASE}/queues",
        json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/del-tsk-q"},
    )
    r = tasks_client.delete(f"{BASE}/queues/del-tsk-q/tasks/no-such-task")
    assert r.status_code == 404


def test_list_tasks_missing_queue_returns_404(tasks_client):
    r = tasks_client.get(f"{BASE}/queues/no-such-queue/tasks")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Retry backoff
# ---------------------------------------------------------------------------


def test_retry_delay_calculation():
    """_retry_delay returns correct exponential backoff values."""
    from cloudbox.services.tasks.worker import _retry_delay

    config = {"minBackoff": "1s", "maxBackoff": "300s", "maxDoublings": 3}

    # attempt 1: 1 * 2^0 = 1s
    assert _retry_delay(config, 1) == pytest.approx(1.0)
    # attempt 2: 1 * 2^1 = 2s
    assert _retry_delay(config, 2) == pytest.approx(2.0)
    # attempt 3: 1 * 2^2 = 4s
    assert _retry_delay(config, 3) == pytest.approx(4.0)
    # attempt 4: maxDoublings=3, so exponent capped at 3 → 1 * 2^3 = 8s
    assert _retry_delay(config, 4) == pytest.approx(8.0)
    # attempt 5: still capped at doublings → 8s (not 16)
    assert _retry_delay(config, 5) == pytest.approx(8.0)


def test_retry_delay_respects_max_backoff():
    from cloudbox.services.tasks.worker import _retry_delay

    config = {"minBackoff": "10s", "maxBackoff": "30s", "maxDoublings": 16}
    # 10 * 2^2 = 40s, capped to 30s
    assert _retry_delay(config, 3) == pytest.approx(30.0)


def test_retry_delay_defaults():
    """Default retry config matches Cloud Tasks defaults."""
    from cloudbox.services.tasks.worker import _retry_delay

    config = {}
    # defaults: minBackoff=0.1s, maxDoublings=16, maxBackoff=3600s
    assert _retry_delay(config, 1) == pytest.approx(0.1)
    assert _retry_delay(config, 2) == pytest.approx(0.2)


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------


def test_rate_limits_stored_and_returned(tasks_client):
    """RateLimits set on create are stored and returned on GET."""
    r = tasks_client.post(
        f"{BASE}/queues",
        json={
            "name": f"projects/{PROJECT}/locations/{LOCATION}/queues/rl-queue",
            "rateLimits": {
                "maxDispatchesPerSecond": 5.0,
                "maxConcurrentDispatches": 2,
            },
        },
    )
    assert r.status_code == 200
    rl = r.json()["rateLimits"]
    assert rl["maxDispatchesPerSecond"] == pytest.approx(5.0)
    assert rl["maxConcurrentDispatches"] == 2


def test_rate_limits_updated_via_patch(tasks_client):
    """PATCH can change rateLimits on an existing queue."""
    tasks_client.post(
        f"{BASE}/queues",
        json={
            "name": f"projects/{PROJECT}/locations/{LOCATION}/queues/rl-patch-queue",
        },
    )
    r = tasks_client.patch(
        f"{BASE}/queues/rl-patch-queue",
        json={
            "rateLimits": {
                "maxDispatchesPerSecond": 10.0,
                "maxConcurrentDispatches": 3,
            },
        },
    )
    assert r.status_code == 200
    rl = r.json()["rateLimits"]
    assert rl["maxDispatchesPerSecond"] == pytest.approx(10.0)
    assert rl["maxConcurrentDispatches"] == 3


def test_max_dispatches_per_second_caps_tick():
    """_tick dispatches at most maxDispatchesPerSecond tasks per second."""
    import asyncio
    from datetime import datetime, timedelta
    from unittest.mock import MagicMock, patch

    from cloudbox.services.tasks.store import get_store
    from cloudbox.services.tasks.worker import _tick

    store = get_store()
    store.reset()

    queue_name = "projects/p/locations/l/queues/rate-q"
    store.set(
        "queues",
        queue_name,
        {
            "name": queue_name,
            "state": "RUNNING",
            "rateLimits": {"maxDispatchesPerSecond": 2.0, "maxConcurrentDispatches": 10},
            "retryConfig": {"maxAttempts": 1},
        },
    )

    past = (datetime.now(UTC) - timedelta(seconds=5)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    for i in range(5):
        task_key = f"{queue_name}/tasks/t{i}"
        store.set(
            "tasks",
            task_key,
            {
                "name": task_key,
                "scheduleTime": past,
                "httpRequest": {"url": "http://localhost:9999/noop", "httpMethod": "POST"},
            },
        )

    dispatch_calls = []

    async def fake_dispatch(client, store, task_key, task, http_req, sem):
        dispatch_calls.append(task_key)

    with patch("cloudbox.services.tasks.worker._dispatch_with_sem", side_effect=fake_dispatch):
        asyncio.run(_tick(MagicMock()))

    # Only 2 of the 5 tasks should have been dispatched
    assert len(dispatch_calls) == 2


def test_max_concurrent_dispatches_limits_inflight():
    """maxConcurrentDispatches=1 serialises dispatches via the semaphore."""
    import asyncio
    from datetime import datetime, timedelta
    from unittest.mock import MagicMock, patch

    from cloudbox.services.tasks.store import get_store
    from cloudbox.services.tasks.worker import _get_semaphore, _tick

    store = get_store()
    store.reset()

    queue_name = "projects/p/locations/l/queues/conc-q"
    store.set(
        "queues",
        queue_name,
        {
            "name": queue_name,
            "state": "RUNNING",
            "rateLimits": {"maxDispatchesPerSecond": 100.0, "maxConcurrentDispatches": 1},
            "retryConfig": {"maxAttempts": 1},
        },
    )

    past = (datetime.now(UTC) - timedelta(seconds=5)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    for i in range(3):
        task_key = f"{queue_name}/tasks/c{i}"
        store.set(
            "tasks",
            task_key,
            {
                "name": task_key,
                "scheduleTime": past,
                "httpRequest": {"url": "http://localhost:9999/noop", "httpMethod": "POST"},
            },
        )

    # Force semaphore recreation with limit=1
    _get_semaphore(queue_name, 1)

    max_inflight = [0]
    current_inflight = [0]

    async def fake_dispatch(client, store, task_key, task, http_req, sem):
        async with sem:
            current_inflight[0] += 1
            max_inflight[0] = max(max_inflight[0], current_inflight[0])
            await asyncio.sleep(0)  # yield so other coroutines can try
            current_inflight[0] -= 1

    with patch("cloudbox.services.tasks.worker._dispatch_with_sem", side_effect=fake_dispatch):
        asyncio.run(_tick(MagicMock()))

    assert max_inflight[0] <= 1


def test_get_missing_queue_returns_404(tasks_client):
    r = tasks_client.get(f"{BASE}/queues/no-such-queue")
    assert r.status_code == 404


def test_run_missing_task_returns_404(tasks_client):
    tasks_client.post(
        f"{BASE}/queues",
        json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/run-q2"},
    )
    r = tasks_client.post(f"{BASE}/queues/run-q2/tasks/nonexistent-task:run")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------


def test_named_task_tombstone_prevents_recreate(tasks_client):
    tasks_client.post(
        f"{BASE}/queues",
        json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/dedup-q"},
    )
    tasks_client.post(
        f"{BASE}/queues/dedup-q/tasks",
        json={
            "task": {
                "name": f"projects/{PROJECT}/locations/{LOCATION}/queues/dedup-q/tasks/named-t",
                "httpRequest": {"url": "http://localhost/noop", "httpMethod": "POST"},
            }
        },
    )
    tasks_client.delete(f"{BASE}/queues/dedup-q/tasks/named-t")

    # Recreating within the dedup window should fail
    r = tasks_client.post(
        f"{BASE}/queues/dedup-q/tasks",
        json={
            "task": {
                "name": f"projects/{PROJECT}/locations/{LOCATION}/queues/dedup-q/tasks/named-t",
                "httpRequest": {"url": "http://localhost/noop", "httpMethod": "POST"},
            }
        },
    )
    assert r.status_code == 409


def test_content_dedup_rejects_identical_body(tasks_client):
    tasks_client.post(
        f"{BASE}/queues",
        json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/content-q"},
    )
    task_body = {"task": {"httpRequest": {"url": "http://localhost/dedup", "httpMethod": "POST"}}}

    r1 = tasks_client.post(f"{BASE}/queues/content-q/tasks", json=task_body)
    assert r1.status_code == 200

    r2 = tasks_client.post(f"{BASE}/queues/content-q/tasks", json=task_body)
    assert r2.status_code == 409


def test_content_dedup_allows_different_bodies(tasks_client):
    tasks_client.post(
        f"{BASE}/queues",
        json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/diff-q"},
    )

    r1 = tasks_client.post(
        f"{BASE}/queues/diff-q/tasks",
        json={"task": {"httpRequest": {"url": "http://localhost/a", "httpMethod": "POST"}}},
    )
    assert r1.status_code == 200

    r2 = tasks_client.post(
        f"{BASE}/queues/diff-q/tasks",
        json={"task": {"httpRequest": {"url": "http://localhost/b", "httpMethod": "POST"}}},
    )
    assert r2.status_code == 200


# ---------------------------------------------------------------------------
# Pub/Sub target
# ---------------------------------------------------------------------------


def test_create_task_with_pubsub_target(tasks_client):
    """Tasks with pubsubTarget are accepted and stored."""
    tasks_client.post(
        f"{BASE}/queues",
        json={"name": f"projects/{PROJECT}/locations/{LOCATION}/queues/ps-q"},
    )
    r = tasks_client.post(
        f"{BASE}/queues/ps-q/tasks",
        json={
            "task": {
                "pubsubTarget": {
                    "topicName": "projects/local-project/topics/my-topic",
                    "data": "aGVsbG8=",
                    "attributes": {"key": "val"},
                }
            }
        },
    )
    assert r.status_code == 200
    task = r.json()
    assert task["pubsubTarget"]["topicName"] == "projects/local-project/topics/my-topic"


def test_dispatch_pubsub_target_enqueues_message():
    """_dispatch_pubsub publishes the message to the Pub/Sub store and deletes the task."""
    import asyncio

    from cloudbox.services.pubsub.store import _queues, ensure_queue
    from cloudbox.services.pubsub.store import get_store as pubsub_store
    from cloudbox.services.tasks.store import get_store as tasks_store
    from cloudbox.services.tasks.worker import _dispatch_pubsub

    t_store = tasks_store()
    t_store.reset()
    ps_store = pubsub_store()
    ps_store.reset()
    _queues.clear()

    topic = f"projects/{PROJECT}/topics/dispatch-topic"
    sub_name = f"projects/{PROJECT}/subscriptions/dispatch-sub"
    ps_store.set("topics", topic, {"name": topic})
    ps_store.set("subscriptions", sub_name, {"name": sub_name, "topic": topic})
    ensure_queue(sub_name)

    queue_name = f"projects/{PROJECT}/locations/us-central1/queues/ps-dispatch-q"
    task_key = f"{queue_name}/tasks/t1"
    task = {
        "name": task_key,
        "scheduleTime": "2020-01-01T00:00:00.000Z",
        "pubsubTarget": {
            "topicName": topic,
            "data": "dGVzdA==",
            "attributes": {"env": "test"},
        },
    }
    t_store.set("tasks", task_key, task)

    asyncio.run(_dispatch_pubsub(t_store, task_key, task, task["pubsubTarget"]))

    # Task should be deleted after dispatch
    assert t_store.get("tasks", task_key) is None

    # Message should be in the subscription queue
    from cloudbox.services.pubsub.store import _queues as queues_after

    envelopes = list(queues_after.get(sub_name, []))
    assert len(envelopes) == 1
    assert envelopes[0].message["data"] == "dGVzdA=="
    assert envelopes[0].message["attributes"]["env"] == "test"


def test_dispatch_pubsub_target_drops_task_when_topic_missing():
    """_dispatch_pubsub silently drops the task if the topic does not exist."""
    import asyncio

    from cloudbox.services.pubsub.store import _queues
    from cloudbox.services.pubsub.store import get_store as pubsub_store
    from cloudbox.services.tasks.store import get_store as tasks_store
    from cloudbox.services.tasks.worker import _dispatch_pubsub

    t_store = tasks_store()
    t_store.reset()
    pubsub_store().reset()
    _queues.clear()

    queue_name = f"projects/{PROJECT}/locations/us-central1/queues/ps-miss-q"
    task_key = f"{queue_name}/tasks/t1"
    task = {
        "name": task_key,
        "scheduleTime": "2020-01-01T00:00:00.000Z",
        "pubsubTarget": {
            "topicName": "projects/x/topics/ghost",
            "data": "",
            "attributes": {},
        },
    }
    t_store.set("tasks", task_key, task)
    asyncio.run(_dispatch_pubsub(t_store, task_key, task, task["pubsubTarget"]))
    assert t_store.get("tasks", task_key) is None
