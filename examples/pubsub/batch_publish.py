"""Pub/Sub — publish a batch of messages with attributes, then pull them.

    uv run python examples/pubsub/batch_publish.py
"""
import sys
import os
import base64
import json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from examples.shared import PUBSUB_BASE, PROJECT, client, ok

TOPIC = "batch-topic"
SUBSCRIPTION = "batch-sub"


def main():
    http = client()

    topic_path = f"projects/{PROJECT}/topics/{TOPIC}"
    sub_path = f"projects/{PROJECT}/subscriptions/{SUBSCRIPTION}"

    ok(http.put(f"{PUBSUB_BASE}/v1/{topic_path}"))
    ok(http.put(f"{PUBSUB_BASE}/v1/{sub_path}", json={"name": sub_path, "topic": topic_path}))

    # Publish a batch with message attributes
    events = [
        {"type": "order.created", "order_id": "A001", "amount": 49.99},
        {"type": "order.shipped", "order_id": "A001", "carrier": "UPS"},
        {"type": "order.delivered", "order_id": "A001"},
    ]
    messages = [
        {
            "data": base64.b64encode(json.dumps(e).encode()).decode(),
            "attributes": {"event_type": e["type"]},
        }
        for e in events
    ]
    r = ok(http.post(f"{PUBSUB_BASE}/v1/{topic_path}:publish", json={"messages": messages}))
    print(f"Published {len(messages)} messages, IDs: {r.json().get('messageIds', [])}")

    # Pull and inspect
    r = ok(http.post(f"{PUBSUB_BASE}/v1/{sub_path}:pull", json={"maxMessages": 10}))
    received = r.json().get("receivedMessages", [])
    ack_ids = []
    for msg in received:
        payload = json.loads(base64.b64decode(msg["message"]["data"]))
        attrs = msg["message"].get("attributes", {})
        print(f"  [{attrs.get('event_type')}] {payload}")
        ack_ids.append(msg["ackId"])

    ok(http.post(f"{PUBSUB_BASE}/v1/{sub_path}:acknowledge", json={"ackIds": ack_ids}))

    # Cleanup
    http.delete(f"{PUBSUB_BASE}/v1/{sub_path}")
    http.delete(f"{PUBSUB_BASE}/v1/{topic_path}")
    print("Done")


if __name__ == "__main__":
    main()
