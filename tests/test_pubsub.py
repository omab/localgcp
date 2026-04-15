"""Tests for Cloud Pub/Sub emulator."""
import asyncio
import base64
from unittest.mock import AsyncMock, MagicMock, patch


PROJECT = "projects/local-project"


def test_create_and_get_topic(pubsub_client):
    r = pubsub_client.put(f"/v1/{PROJECT}/topics/my-topic")
    assert r.status_code == 200
    assert r.json()["name"] == f"{PROJECT}/topics/my-topic"

    r = pubsub_client.get(f"/v1/{PROJECT}/topics/my-topic")
    assert r.status_code == 200


def test_list_topics(pubsub_client):
    pubsub_client.put(f"/v1/{PROJECT}/topics/t1")
    pubsub_client.put(f"/v1/{PROJECT}/topics/t2")
    r = pubsub_client.get(f"/v1/{PROJECT}/topics")
    assert r.status_code == 200
    names = [t["name"] for t in r.json()["topics"]]
    assert f"{PROJECT}/topics/t1" in names
    assert f"{PROJECT}/topics/t2" in names


def test_publish_and_pull(pubsub_client):
    topic = f"{PROJECT}/topics/events"
    sub = f"{PROJECT}/subscriptions/events-sub"

    pubsub_client.put(f"/v1/{topic}")
    pubsub_client.put(f"/v1/{sub}", json={"name": sub, "topic": topic})

    data = base64.b64encode(b"hello pubsub").decode()
    r = pubsub_client.post(f"/v1/{topic}:publish", json={"messages": [{"data": data}]})
    assert r.status_code == 200
    assert len(r.json()["messageIds"]) == 1

    r = pubsub_client.post(f"/v1/{sub}:pull", json={"maxMessages": 10})
    assert r.status_code == 200
    msgs = r.json()["receivedMessages"]
    assert len(msgs) == 1
    assert msgs[0]["message"]["data"] == data


def test_acknowledge(pubsub_client):
    topic = f"{PROJECT}/topics/ack-topic"
    sub = f"{PROJECT}/subscriptions/ack-sub"

    pubsub_client.put(f"/v1/{topic}")
    pubsub_client.put(f"/v1/{sub}", json={"name": sub, "topic": topic})
    pubsub_client.post(f"/v1/{topic}:publish", json={"messages": [{"data": "dGVzdA=="}]})

    r = pubsub_client.post(f"/v1/{sub}:pull", json={"maxMessages": 1})
    ack_id = r.json()["receivedMessages"][0]["ackId"]

    r = pubsub_client.post(f"/v1/{sub}:acknowledge", json={"ackIds": [ack_id]})
    assert r.status_code == 200

    # After ack, queue should be empty
    r = pubsub_client.post(f"/v1/{sub}:pull", json={"maxMessages": 1})
    assert r.json()["receivedMessages"] == []


def test_publish_fanout(pubsub_client):
    topic = f"{PROJECT}/topics/fanout"
    sub1 = f"{PROJECT}/subscriptions/fan-sub1"
    sub2 = f"{PROJECT}/subscriptions/fan-sub2"

    pubsub_client.put(f"/v1/{topic}")
    pubsub_client.put(f"/v1/{sub1}", json={"name": sub1, "topic": topic})
    pubsub_client.put(f"/v1/{sub2}", json={"name": sub2, "topic": topic})

    pubsub_client.post(f"/v1/{topic}:publish", json={"messages": [{"data": "dA=="}]})

    r1 = pubsub_client.post(f"/v1/{sub1}:pull", json={"maxMessages": 1})
    r2 = pubsub_client.post(f"/v1/{sub2}:pull", json={"maxMessages": 1})
    assert len(r1.json()["receivedMessages"]) == 1
    assert len(r2.json()["receivedMessages"]) == 1


def test_push_subscription_dispatches_to_endpoint(pubsub_client):
    """Publishing to a push subscription POSTs to its configured endpoint."""
    topic = f"{PROJECT}/topics/push-topic"
    sub = f"{PROJECT}/subscriptions/push-sub"
    push_url = "http://example.com/push"

    pubsub_client.put(f"/v1/{topic}")
    pubsub_client.put(f"/v1/{sub}", json={
        "name": sub, "topic": topic,
        "pushConfig": {"pushEndpoint": push_url},
    })

    data = base64.b64encode(b"push me").decode()

    with patch("localgcp.services.pubsub.app._dispatch_push", new_callable=AsyncMock) as mock_push:
        r = pubsub_client.post(f"/v1/{topic}:publish", json={"messages": [{"data": data}]})
        assert r.status_code == 200

    mock_push.assert_called_once()
    endpoint, sub_name, ack_id, msg = mock_push.call_args.args
    assert endpoint == push_url
    assert sub_name == sub
    assert isinstance(ack_id, str)
    assert msg["data"] == data


def test_pull_on_push_subscription_returns_400(pubsub_client):
    """Pull is not allowed on a push subscription."""
    topic = f"{PROJECT}/topics/push-nopull-topic"
    sub = f"{PROJECT}/subscriptions/push-nopull-sub"

    pubsub_client.put(f"/v1/{topic}")
    pubsub_client.put(f"/v1/{sub}", json={
        "name": sub, "topic": topic,
        "pushConfig": {"pushEndpoint": "http://example.com/push"},
    })

    r = pubsub_client.post(f"/v1/{sub}:pull", json={"maxMessages": 1})
    assert r.status_code == 400


def test_push_subscription_not_pullable(pubsub_client):
    """Push subscriptions enqueue + immediately dispatch; pull subscriptions on the same topic still work."""
    topic = f"{PROJECT}/topics/mixed-topic"
    pull_sub = f"{PROJECT}/subscriptions/mixed-pull-sub"
    push_sub = f"{PROJECT}/subscriptions/mixed-push-sub"

    pubsub_client.put(f"/v1/{topic}")
    pubsub_client.put(f"/v1/{pull_sub}", json={"name": pull_sub, "topic": topic})
    pubsub_client.put(f"/v1/{push_sub}", json={
        "name": push_sub, "topic": topic,
        "pushConfig": {"pushEndpoint": "http://example.com/push"},
    })

    data = base64.b64encode(b"fanout").decode()

    with patch("localgcp.services.pubsub.app._dispatch_push", new_callable=AsyncMock):
        pubsub_client.post(f"/v1/{topic}:publish", json={"messages": [{"data": data}]})

    # Pull sub receives the message
    r = pubsub_client.post(f"/v1/{pull_sub}:pull", json={"maxMessages": 1})
    assert len(r.json()["receivedMessages"]) == 1

    # Push sub has nothing pending in its queue (message moved to unacked for dispatch)
    from localgcp.services.pubsub.store import queue_depth
    assert queue_depth(push_sub) == 0


async def test_push_dispatch_acks_message_on_success():
    """_dispatch_push acks the message when the endpoint returns 2xx."""
    from localgcp.services.pubsub import store as ps_store
    from localgcp.services.pubsub.app import _dispatch_push

    sub_name = f"{PROJECT}/subscriptions/ack-push-sub"
    ps_store.ensure_queue(sub_name)
    msg = {"data": "dA==", "messageId": "1", "publishTime": "2024-01-01T00:00:00Z", "attributes": {}, "orderingKey": ""}
    ps_store.enqueue(sub_name, msg)
    [(ack_id, pulled_msg, _)] = ps_store.pull(sub_name, 1)

    mock_resp = MagicMock()
    mock_resp.status_code = 200

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.post = AsyncMock(return_value=mock_resp)

    with patch("localgcp.services.pubsub.app.httpx.AsyncClient", return_value=mock_client):
        await _dispatch_push("http://example.com/push", sub_name, ack_id, pulled_msg)

    # Message should be acked — no longer in unacked
    from localgcp.services.pubsub.store import _unacked
    assert ack_id not in _unacked.get(sub_name, {})


async def test_push_dispatch_requeues_message_on_failure():
    """_dispatch_push nacks (requeues) the message when the endpoint returns non-2xx."""
    from localgcp.services.pubsub import store as ps_store
    from localgcp.services.pubsub.app import _dispatch_push

    sub_name = f"{PROJECT}/subscriptions/nack-push-sub"
    ps_store.ensure_queue(sub_name)
    msg = {"data": "dA==", "messageId": "2", "publishTime": "2024-01-01T00:00:00Z", "attributes": {}, "orderingKey": ""}
    ps_store.enqueue(sub_name, msg)
    [(ack_id, pulled_msg, _)] = ps_store.pull(sub_name, 1)

    mock_resp = MagicMock()
    mock_resp.status_code = 500

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.post = AsyncMock(return_value=mock_resp)

    with patch("localgcp.services.pubsub.app.httpx.AsyncClient", return_value=mock_client):
        await _dispatch_push("http://example.com/push", sub_name, ack_id, pulled_msg)

    # Deadline set to 0 → message immediately re-eligible; a subsequent pull should return it
    results = ps_store.pull(sub_name, 1)
    assert len(results) == 1
    assert results[0][1]["messageId"] == "2"
    assert results[0][2] == 2  # delivery_attempt incremented


def test_dead_letter_policy_routes_after_max_attempts(pubsub_client):
    """Messages exceeding maxDeliveryAttempts are routed to the dead-letter topic."""
    topic = f"{PROJECT}/topics/main-topic"
    dlq_topic = f"{PROJECT}/topics/dlq-topic"
    sub = f"{PROJECT}/subscriptions/main-sub"
    dlq_sub = f"{PROJECT}/subscriptions/dlq-sub"

    pubsub_client.put(f"/v1/{topic}")
    pubsub_client.put(f"/v1/{dlq_topic}")
    pubsub_client.put(f"/v1/{dlq_sub}", json={"name": dlq_sub, "topic": dlq_topic})
    pubsub_client.put(f"/v1/{sub}", json={
        "name": sub, "topic": topic,
        "deadLetterPolicy": {"deadLetterTopic": dlq_topic, "maxDeliveryAttempts": 2},
    })

    data = base64.b64encode(b"die hard").decode()
    pubsub_client.post(f"/v1/{topic}:publish", json={"messages": [{"data": data}]})

    from localgcp.services.pubsub import store as ps_store
    import time

    # Simulate exceeding maxDeliveryAttempts by force-expiring ack deadlines
    for attempt in range(1, 3):  # attempts 1, 2 → on attempt 3 it should DLQ
        r = pubsub_client.post(f"/v1/{sub}:pull", json={"maxMessages": 1})
        msgs = r.json()["receivedMessages"]
        assert len(msgs) == 1, f"Expected message on attempt {attempt}"
        ack_id = msgs[0]["ackId"]
        # Expire the ack deadline immediately
        ps_store.modify_ack_deadline(sub, [ack_id], 0)

    # Force the expired message through the re-enqueue + DLQ check by pulling again
    # At delivery_attempt=3 > maxDeliveryAttempts=2, should route to DLQ
    r = pubsub_client.post(f"/v1/{sub}:pull", json={"maxMessages": 1})
    assert r.json()["receivedMessages"] == []  # no longer in main sub

    # DLQ sub should have the message
    r = pubsub_client.post(f"/v1/{dlq_sub}:pull", json={"maxMessages": 1})
    dlq_msgs = r.json()["receivedMessages"]
    assert len(dlq_msgs) == 1
    assert dlq_msgs[0]["message"]["data"] == data


def test_retry_policy_delays_redelivery(pubsub_client):
    """Messages are not immediately redelivered when a retry policy is configured."""
    topic = f"{PROJECT}/topics/retry-topic"
    sub = f"{PROJECT}/subscriptions/retry-sub"

    pubsub_client.put(f"/v1/{topic}")
    pubsub_client.put(f"/v1/{sub}", json={
        "name": sub, "topic": topic,
        "retryPolicy": {"minimumBackoff": "30s", "maximumBackoff": "300s"},
    })

    data = base64.b64encode(b"retry me").decode()
    pubsub_client.post(f"/v1/{topic}:publish", json={"messages": [{"data": data}]})

    from localgcp.services.pubsub import store as ps_store

    # First pull succeeds
    r = pubsub_client.post(f"/v1/{sub}:pull", json={"maxMessages": 1})
    msgs = r.json()["receivedMessages"]
    assert len(msgs) == 1
    ack_id = msgs[0]["ackId"]

    # Expire deadline immediately (nack)
    ps_store.modify_ack_deadline(sub, [ack_id], 0)

    # Immediate second pull should return nothing (backoff not elapsed)
    r = pubsub_client.post(f"/v1/{sub}:pull", json={"maxMessages": 1})
    assert r.json()["receivedMessages"] == []


def test_retry_policy_delivers_after_backoff(pubsub_client):
    """After backoff elapses, message becomes available again."""
    topic = f"{PROJECT}/topics/retry2-topic"
    sub = f"{PROJECT}/subscriptions/retry2-sub"

    pubsub_client.put(f"/v1/{topic}")
    # Use 0s minimum backoff so backoff expires immediately
    pubsub_client.put(f"/v1/{sub}", json={
        "name": sub, "topic": topic,
        "retryPolicy": {"minimumBackoff": "0s", "maximumBackoff": "0s"},
    })

    data = base64.b64encode(b"no wait").decode()
    pubsub_client.post(f"/v1/{topic}:publish", json={"messages": [{"data": data}]})

    from localgcp.services.pubsub import store as ps_store

    r = pubsub_client.post(f"/v1/{sub}:pull", json={"maxMessages": 1})
    ack_id = r.json()["receivedMessages"][0]["ackId"]
    ps_store.modify_ack_deadline(sub, [ack_id], 0)

    # With 0s backoff, redelivery is immediate
    r = pubsub_client.post(f"/v1/{sub}:pull", json={"maxMessages": 1})
    msgs = r.json()["receivedMessages"]
    assert len(msgs) == 1
    assert msgs[0]["deliveryAttempt"] == 2


def test_filter_attribute_equality(pubsub_client):
    """Subscription filter on attribute equality drops non-matching messages."""
    topic = f"{PROJECT}/topics/filter-topic"
    sub_red = f"{PROJECT}/subscriptions/filter-red"
    sub_all = f"{PROJECT}/subscriptions/filter-all"

    pubsub_client.put(f"/v1/{topic}")
    pubsub_client.put(f"/v1/{sub_red}", json={
        "name": sub_red, "topic": topic,
        "filter": 'attributes.color = "red"',
    })
    pubsub_client.put(f"/v1/{sub_all}", json={"name": sub_all, "topic": topic})

    def _publish(color: str) -> None:
        data = base64.b64encode(color.encode()).decode()
        pubsub_client.post(f"/v1/{topic}:publish", json={
            "messages": [{"data": data, "attributes": {"color": color}}]
        })

    _publish("red")
    _publish("blue")

    # sub_red should only have 1 message (red)
    r = pubsub_client.post(f"/v1/{sub_red}:pull", json={"maxMessages": 10})
    msgs = r.json()["receivedMessages"]
    assert len(msgs) == 1
    assert base64.b64decode(msgs[0]["message"]["data"]) == b"red"

    # sub_all should have both
    r = pubsub_client.post(f"/v1/{sub_all}:pull", json={"maxMessages": 10})
    assert len(r.json()["receivedMessages"]) == 2


def test_filter_has_prefix(pubsub_client):
    """hasPrefix filter passes messages whose attribute starts with the given prefix."""
    topic = f"{PROJECT}/topics/prefix-topic"
    sub = f"{PROJECT}/subscriptions/prefix-sub"

    pubsub_client.put(f"/v1/{topic}")
    pubsub_client.put(f"/v1/{sub}", json={
        "name": sub, "topic": topic,
        "filter": 'hasPrefix(attributes.env, "prod")',
    })

    def _publish(env: str) -> None:
        pubsub_client.post(f"/v1/{topic}:publish", json={
            "messages": [{"data": "dA==", "attributes": {"env": env}}]
        })

    _publish("prod-us")
    _publish("prod-eu")
    _publish("staging")

    r = pubsub_client.post(f"/v1/{sub}:pull", json={"maxMessages": 10})
    assert len(r.json()["receivedMessages"]) == 2


def test_filter_not_and_or():
    """Unit test the filter evaluator for NOT, AND, OR."""
    from localgcp.services.pubsub.filter import matches

    msg_red_large = {"attributes": {"color": "red", "size": "large"}}
    msg_blue_large = {"attributes": {"color": "blue", "size": "large"}}
    msg_red_small = {"attributes": {"color": "red", "size": "small"}}

    assert matches('NOT attributes.color = "red"', msg_red_large) is False
    assert matches('NOT attributes.color = "red"', msg_blue_large) is True
    assert matches('attributes.color = "red" AND attributes.size = "large"', msg_red_large) is True
    assert matches('attributes.color = "red" AND attributes.size = "large"', msg_red_small) is False
    assert matches('attributes.color = "red" OR attributes.color = "blue"', msg_red_large) is True
    assert matches('attributes.color = "red" OR attributes.color = "blue"', msg_blue_large) is True
    assert matches('attributes.color = "green" OR attributes.color = "blue"', msg_red_large) is False


def test_ordering_enforces_sequential_delivery(pubsub_client):
    """With enableMessageOrdering, only one message per orderingKey is in-flight at a time."""
    topic = f"{PROJECT}/topics/ordered-topic"
    sub = f"{PROJECT}/subscriptions/ordered-sub"

    pubsub_client.put(f"/v1/{topic}")
    pubsub_client.put(f"/v1/{sub}", json={
        "name": sub, "topic": topic, "enableMessageOrdering": True,
    })

    # Publish 3 messages with the same ordering key
    for i in range(1, 4):
        data = base64.b64encode(str(i).encode()).decode()
        pubsub_client.post(f"/v1/{topic}:publish", json={
            "messages": [{"data": data, "orderingKey": "key-A"}]
        })

    # First pull: should get message 1 only (key-A now in-flight)
    r = pubsub_client.post(f"/v1/{sub}:pull", json={"maxMessages": 10})
    msgs = r.json()["receivedMessages"]
    assert len(msgs) == 1
    assert base64.b64decode(msgs[0]["message"]["data"]) == b"1"
    ack_id = msgs[0]["ackId"]

    # Second pull without ack: key-A is still in-flight → no messages
    r = pubsub_client.post(f"/v1/{sub}:pull", json={"maxMessages": 10})
    assert r.json()["receivedMessages"] == []

    # Ack message 1 → key-A released
    pubsub_client.post(f"/v1/{sub}:acknowledge", json={"ackIds": [ack_id]})

    # Third pull: should get message 2
    r = pubsub_client.post(f"/v1/{sub}:pull", json={"maxMessages": 10})
    msgs = r.json()["receivedMessages"]
    assert len(msgs) == 1
    assert base64.b64decode(msgs[0]["message"]["data"]) == b"2"


def test_ordering_different_keys_concurrent(pubsub_client):
    """Messages with different ordering keys are delivered concurrently."""
    topic = f"{PROJECT}/topics/multikey-topic"
    sub = f"{PROJECT}/subscriptions/multikey-sub"

    pubsub_client.put(f"/v1/{topic}")
    pubsub_client.put(f"/v1/{sub}", json={
        "name": sub, "topic": topic, "enableMessageOrdering": True,
    })

    # Publish one message for key-A and one for key-B
    for key in ("key-A", "key-B"):
        data = base64.b64encode(key.encode()).decode()
        pubsub_client.post(f"/v1/{topic}:publish", json={
            "messages": [{"data": data, "orderingKey": key}]
        })

    # Both messages should be deliverable in one pull since they have different keys
    r = pubsub_client.post(f"/v1/{sub}:pull", json={"maxMessages": 10})
    msgs = r.json()["receivedMessages"]
    assert len(msgs) == 2


def test_delete_topic_removes_subscriptions(pubsub_client):
    topic = f"{PROJECT}/topics/del-topic"
    sub = f"{PROJECT}/subscriptions/del-sub"
    pubsub_client.put(f"/v1/{topic}")
    pubsub_client.put(f"/v1/{sub}", json={"name": sub, "topic": topic})
    pubsub_client.delete(f"/v1/{topic}")

    r = pubsub_client.get(f"/v1/{sub}")
    assert r.status_code == 404
