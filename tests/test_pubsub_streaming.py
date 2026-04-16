"""Integration tests for Pub/Sub gRPC StreamingPull."""
import asyncio
import base64

import pytest


PROJECT = "local-project"
TOPIC = f"projects/{PROJECT}/topics/stream-topic"
SUB = f"projects/{PROJECT}/subscriptions/stream-sub"


# ---------------------------------------------------------------------------
# Fixtures — spin up a real grpc.aio server on a random port
# ---------------------------------------------------------------------------


@pytest.fixture
async def grpc_port(reset_stores):
    """Start a live gRPC server, yield its port, shut it down after the test."""
    from localgcp.services.pubsub.grpc_server import create_server

    server = await create_server("127.0.0.1", 0)
    await server.start()
    port = None
    for sock in server._state.generic_handlers[0]._state.port_numbers:
        port = sock
        break
    # Grab the port from the first bound address
    # grpc.aio doesn't expose the bound port directly when port=0, so we
    # add the port explicitly and read it back.
    # Instead: re-bind with a known free port via the server internals.
    # Simpler: use add_insecure_port before start.
    await server.stop(grace=0)

    # Re-create with explicit random port selection via OS
    import socket
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    free_port = s.getsockname()[1]
    s.close()

    server = await create_server("127.0.0.1", free_port)
    await server.start()
    yield free_port
    await server.stop(grace=0)


# ---------------------------------------------------------------------------
# Helper: create topic + subscription via the gRPC channel
# ---------------------------------------------------------------------------


async def _setup_topic_and_sub(channel):
    from google.pubsub_v1.services.publisher import PublisherAsyncClient
    from google.pubsub_v1.services.subscriber import SubscriberAsyncClient
    from google.pubsub_v1.types import pubsub as t
    from google.api_core.client_options import ClientOptions
    import grpc.aio

    pub = PublisherAsyncClient(
        transport="grpc_asyncio",
        channel=channel,
    )
    sub_client = SubscriberAsyncClient(
        transport="grpc_asyncio",
        channel=channel,
    )
    await pub.create_topic(request=t.Topic(name=TOPIC))
    await sub_client.create_subscription(request=t.Subscription(name=SUB, topic=TOPIC))


# ---------------------------------------------------------------------------
# Unit tests — test _streaming_pull directly without a real server
# ---------------------------------------------------------------------------


async def test_streaming_pull_delivers_messages(reset_stores):
    """_streaming_pull sends queued messages to the client."""
    from localgcp.services.pubsub.grpc_server import _streaming_pull
    from localgcp.services.pubsub import store as ps_store
    from localgcp.services.pubsub.store import get_store

    store = get_store()
    store.set("subscriptions", SUB, {
        "name": SUB,
        "topic": TOPIC,
        "ackDeadlineSeconds": 10,
        "retainAckedMessages": False,
        "enableMessageOrdering": False,
        "labels": {},
        "pushConfig": {"pushEndpoint": "", "attributes": {}},
    })
    ps_store.ensure_queue(SUB)

    msg = {
        "data": base64.b64encode(b"hello streaming").decode(),
        "attributes": {},
        "messageId": "msg-1",
        "publishTime": "2024-01-01T00:00:00Z",
        "orderingKey": "",
    }
    ps_store.enqueue(SUB, msg)

    from google.pubsub_v1.types import pubsub as t

    responses = []
    write_called = asyncio.Event()

    class MockContext:
        async def write(self, response):
            responses.append(response)
            write_called.set()

        async def abort(self, code, details):
            raise RuntimeError(f"abort: {details}")

    stop_iter = asyncio.Event()

    async def request_gen():
        yield t.StreamingPullRequest(subscription=SUB, stream_ack_deadline_seconds=10)
        # Keep the stream open until we signal stop
        await stop_iter.wait()

    task = asyncio.create_task(_streaming_pull(request_gen().__aiter__(), MockContext()))

    # Wait for at least one message to be delivered
    await asyncio.wait_for(write_called.wait(), timeout=3.0)

    stop_iter.set()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert len(responses) >= 1
    msgs = responses[0].received_messages
    assert len(msgs) == 1
    assert bytes(msgs[0].message.data) == b"hello streaming"
    assert msgs[0].ack_id != ""


async def test_streaming_pull_acks_remove_messages(reset_stores):
    """Ack IDs sent back via subsequent requests are properly acknowledged."""
    from localgcp.services.pubsub.grpc_server import _streaming_pull
    from localgcp.services.pubsub import store as ps_store
    from localgcp.services.pubsub.store import get_store

    store = get_store()
    store.set("subscriptions", SUB, {
        "name": SUB,
        "topic": TOPIC,
        "ackDeadlineSeconds": 10,
        "retainAckedMessages": False,
        "enableMessageOrdering": False,
        "labels": {},
        "pushConfig": {"pushEndpoint": "", "attributes": {}},
    })
    ps_store.ensure_queue(SUB)

    msg = {
        "data": base64.b64encode(b"ack me").decode(),
        "attributes": {},
        "messageId": "msg-2",
        "publishTime": "2024-01-01T00:00:00Z",
        "orderingKey": "",
    }
    ps_store.enqueue(SUB, msg)

    from google.pubsub_v1.types import pubsub as t

    received_ack_ids = []
    write_called = asyncio.Event()

    class MockContext:
        async def write(self, response):
            for rm in response.received_messages:
                received_ack_ids.append(rm.ack_id)
            write_called.set()

        async def abort(self, code, details):
            raise RuntimeError(f"abort: {details}")

    ack_sent = asyncio.Event()
    stop_iter = asyncio.Event()

    async def request_gen():
        yield t.StreamingPullRequest(subscription=SUB, stream_ack_deadline_seconds=10)
        await write_called.wait()  # wait until we know the ack_id
        yield t.StreamingPullRequest(ack_ids=received_ack_ids[:])
        ack_sent.set()
        await stop_iter.wait()

    task = asyncio.create_task(_streaming_pull(request_gen().__aiter__(), MockContext()))

    await asyncio.wait_for(ack_sent.wait(), timeout=3.0)
    await asyncio.sleep(0.1)  # let the ack propagate

    # After ack, the message should not be re-delivered
    results = ps_store.pull(SUB, max_messages=10)
    assert results == []

    stop_iter.set()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def test_streaming_pull_unknown_subscription(reset_stores):
    """Streaming pull on an unknown subscription aborts with NOT_FOUND."""
    from localgcp.services.pubsub.grpc_server import _streaming_pull
    from google.pubsub_v1.types import pubsub as t

    aborted = {}

    class MockContext:
        async def write(self, response):
            pass

        async def abort(self, code, details):
            aborted["code"] = code
            aborted["details"] = details
            # Simulate grpc abort stopping further execution
            raise RuntimeError("aborted")

    async def request_gen():
        yield t.StreamingPullRequest(subscription="projects/p/subscriptions/no-such-sub")

    try:
        await _streaming_pull(request_gen().__aiter__(), MockContext())
    except RuntimeError:
        pass

    import grpc
    assert aborted.get("code") == grpc.StatusCode.NOT_FOUND


async def test_streaming_pull_empty_queue_polls(reset_stores):
    """Streaming pull with no messages stays open and polls without crashing."""
    from localgcp.services.pubsub.grpc_server import _streaming_pull
    from localgcp.services.pubsub import store as ps_store
    from localgcp.services.pubsub.store import get_store
    from google.pubsub_v1.types import pubsub as t

    store = get_store()
    store.set("subscriptions", SUB, {
        "name": SUB,
        "topic": TOPIC,
        "ackDeadlineSeconds": 10,
        "retainAckedMessages": False,
        "enableMessageOrdering": False,
        "labels": {},
        "pushConfig": {"pushEndpoint": "", "attributes": {}},
    })
    ps_store.ensure_queue(SUB)

    stop_iter = asyncio.Event()

    class MockContext:
        async def write(self, response):
            pass

        async def abort(self, code, details):
            raise RuntimeError(f"abort: {details}")

    async def request_gen():
        yield t.StreamingPullRequest(subscription=SUB, stream_ack_deadline_seconds=10)
        await stop_iter.wait()

    task = asyncio.create_task(_streaming_pull(request_gen().__aiter__(), MockContext()))
    await asyncio.sleep(0.2)  # let it poll a few times

    stop_iter.set()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    # No assertion needed — just verifying no exception was raised


async def test_streaming_pull_delivers_multiple_messages(reset_stores):
    """All queued messages are delivered before waiting for more."""
    from localgcp.services.pubsub.grpc_server import _streaming_pull
    from localgcp.services.pubsub import store as ps_store
    from localgcp.services.pubsub.store import get_store
    from google.pubsub_v1.types import pubsub as t

    store = get_store()
    store.set("subscriptions", SUB, {
        "name": SUB,
        "topic": TOPIC,
        "ackDeadlineSeconds": 10,
        "retainAckedMessages": False,
        "enableMessageOrdering": False,
        "labels": {},
        "pushConfig": {"pushEndpoint": "", "attributes": {}},
    })
    ps_store.ensure_queue(SUB)

    for i in range(5):
        ps_store.enqueue(SUB, {
            "data": base64.b64encode(f"msg-{i}".encode()).decode(),
            "attributes": {},
            "messageId": f"id-{i}",
            "publishTime": "2024-01-01T00:00:00Z",
            "orderingKey": "",
        })

    all_received = []
    write_called = asyncio.Event()

    class MockContext:
        async def write(self, response):
            all_received.extend(response.received_messages)
            if len(all_received) >= 5:
                write_called.set()

        async def abort(self, code, details):
            raise RuntimeError(f"abort: {details}")

    stop_iter = asyncio.Event()

    async def request_gen():
        yield t.StreamingPullRequest(subscription=SUB, stream_ack_deadline_seconds=10)
        await stop_iter.wait()

    task = asyncio.create_task(_streaming_pull(request_gen().__aiter__(), MockContext()))
    await asyncio.wait_for(write_called.wait(), timeout=3.0)

    stop_iter.set()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert len(all_received) == 5
    payloads = {bytes(rm.message.data).decode() for rm in all_received}
    assert payloads == {f"msg-{i}" for i in range(5)}
