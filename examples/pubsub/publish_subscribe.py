"""Pub/Sub — create topic and subscription, publish messages, pull and acknowledge.

    uv run python examples/pubsub/publish_subscribe.py
"""
import sys
import os
import base64
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from examples.shared import PUBSUB_BASE, PROJECT, client, ok

TOPIC = "example-topic"
SUBSCRIPTION = "example-sub"


def main():
    http = client()

    topic_path = f"projects/{PROJECT}/topics/{TOPIC}"
    sub_path = f"projects/{PROJECT}/subscriptions/{SUBSCRIPTION}"

    # Create topic
    ok(http.put(f"{PUBSUB_BASE}/v1/{topic_path}"))
    print(f"Created topic: {topic_path}")

    # Create subscription
    ok(http.put(f"{PUBSUB_BASE}/v1/{sub_path}", json={"name": sub_path, "topic": topic_path}))
    print(f"Created subscription: {sub_path}")

    # Publish messages
    messages = ["Hello", "from", "Cloudbox"]
    ok(http.post(
        f"{PUBSUB_BASE}/v1/{topic_path}:publish",
        json={"messages": [{"data": base64.b64encode(m.encode()).decode()} for m in messages]},
    ))
    print(f"Published {len(messages)} messages")

    # Pull messages
    r = ok(http.post(f"{PUBSUB_BASE}/v1/{sub_path}:pull", json={"maxMessages": 10}))
    received = r.json().get("receivedMessages", [])
    ack_ids = []
    for msg in received:
        data = base64.b64decode(msg["message"]["data"]).decode()
        print(f"  Received: {data!r}")
        ack_ids.append(msg["ackId"])

    # Acknowledge
    ok(http.post(f"{PUBSUB_BASE}/v1/{sub_path}:acknowledge", json={"ackIds": ack_ids}))
    print(f"Acknowledged {len(ack_ids)} messages")

    # Cleanup
    http.delete(f"{PUBSUB_BASE}/v1/{sub_path}")
    http.delete(f"{PUBSUB_BASE}/v1/{topic_path}")
    print("Cleaned up")


if __name__ == "__main__":
    main()
