"""Cloud Tasks — queue rate limiting: maxDispatchesPerSecond and maxConcurrentDispatches.

    uv run python examples/tasks/rate_limits.py
"""
import sys
import os
import base64
import json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from examples.shared import TASKS_BASE, PROJECT, LOCATION, client, ok

QUEUE_ID = "rate-limited-queue"


def main():
    http = client()

    parent = f"projects/{PROJECT}/locations/{LOCATION}"
    queue_name = f"{parent}/queues/{QUEUE_ID}"

    # Create a queue with tight rate limits
    r = ok(http.post(
        f"{TASKS_BASE}/v2/{parent}/queues",
        json={
            "name": queue_name,
            "rateLimits": {
                "maxDispatchesPerSecond": 5.0,    # max 5 dispatches per second
                "maxConcurrentDispatches": 2,     # at most 2 in-flight simultaneously
                "maxBurstSize": 5,
            },
            "retryConfig": {
                "maxAttempts": 3,
                "minBackoff": "1s",
                "maxBackoff": "10s",
                "maxDoublings": 3,
            },
        },
    ))
    rl = r.json()["rateLimits"]
    print(f"Created queue: {QUEUE_ID}")
    print(f"  maxDispatchesPerSecond: {rl['maxDispatchesPerSecond']}")
    print(f"  maxConcurrentDispatches: {rl['maxConcurrentDispatches']}")

    # Enqueue several tasks
    for i in range(6):
        payload = base64.b64encode(json.dumps({"index": i}).encode()).decode()
        ok(http.post(
            f"{TASKS_BASE}/v2/{queue_name}/tasks",
            json={"task": {"httpRequest": {
                "url": "http://localhost:8080/worker",
                "httpMethod": "POST",
                "body": payload,
            }}},
        ))
    r = ok(http.get(f"{TASKS_BASE}/v2/{queue_name}/tasks"))
    print(f"\nEnqueued 6 tasks. Queue depth: {len(r.json().get('tasks', []))}")

    # Update rate limits on an existing queue via PATCH
    r = ok(http.patch(
        f"{TASKS_BASE}/v2/{queue_name}",
        json={
            "rateLimits": {
                "maxDispatchesPerSecond": 10.0,
                "maxConcurrentDispatches": 4,
            },
        },
    ))
    rl2 = r.json()["rateLimits"]
    print(f"\nUpdated rate limits via PATCH:")
    print(f"  maxDispatchesPerSecond: {rl2['maxDispatchesPerSecond']}")
    print(f"  maxConcurrentDispatches: {rl2['maxConcurrentDispatches']}")

    # Inspect queue config
    r = ok(http.get(f"{TASKS_BASE}/v2/{queue_name}"))
    q = r.json()
    print(f"\nQueue state: {q['state']}")
    print(f"retryConfig: maxAttempts={q['retryConfig']['maxAttempts']}, "
          f"minBackoff={q['retryConfig']['minBackoff']}")

    # Cleanup
    http.delete(f"{TASKS_BASE}/v2/{queue_name}")
    print("\nDeleted queue. Done.")


if __name__ == "__main__":
    main()
