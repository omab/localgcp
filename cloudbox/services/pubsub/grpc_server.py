"""gRPC server for Cloud Pub/Sub.

Implements google.pubsub.v1.Publisher and google.pubsub.v1.Subscriber
using grpc.aio with a GenericRpcHandler — no generated stubs needed.

Compatible with PUBSUB_EMULATOR_HOST=<host>:<pubsub_port>.

SDK usage (default transport — no extra flags needed):
    publisher = pubsub_v1.PublisherClient()
    # set env PUBSUB_EMULATOR_HOST=localhost:8085 before importing

REST alternative (if you prefer HTTP/1.1):
    publisher = pubsub_v1.PublisherClient(
        transport="rest",
        client_options=ClientOptions(api_endpoint="http://localhost:8086"),
    )
"""

from __future__ import annotations

import asyncio
import base64
import logging
import uuid
from datetime import UTC, datetime

import grpc
import grpc.aio
from google.protobuf import empty_pb2

from cloudbox.services.pubsub import store as ps_store

logger = logging.getLogger("cloudbox.pubsub.grpc")

# ---------------------------------------------------------------------------
# Lazy proto-plus type imports — only pulled in when gRPC server starts.
# google-cloud-pubsub brings grpcio + proto-plus as transitive deps.
# ---------------------------------------------------------------------------


def _types():
    from google.pubsub_v1.types import pubsub as t

    return t


def _schema_types():
    from google.pubsub_v1.types import schema as st

    return st


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


# ---------------------------------------------------------------------------
# Serialization helpers for protobuf Empty (not proto-plus)
# ---------------------------------------------------------------------------


def _ser_empty(e) -> bytes:
    return b""


# ---------------------------------------------------------------------------
# Store ↔ proto conversion helpers
# ---------------------------------------------------------------------------


def _topic_to_proto(data: dict):
    t = _types()
    st = _schema_types()
    topic = t.Topic(name=data["name"], labels=data.get("labels", {}))
    ss = data.get("schemaSettings")
    if ss and ss.get("schema"):
        enc_str = ss.get("encoding", "ENCODING_UNSPECIFIED")
        try:
            enc = st.Encoding[enc_str]
        except KeyError:
            enc = st.Encoding.ENCODING_UNSPECIFIED
        topic.schema_settings = st.SchemaSettings(schema=ss["schema"], encoding=enc)
    return topic


def _topic_to_dict(proto) -> dict:
    d = {
        "name": proto.name,
        "labels": dict(proto.labels),
        "messageRetentionDuration": "604800s",
    }
    ss = proto.schema_settings
    if ss and ss.schema:
        enc_name = ss.encoding.name if hasattr(ss.encoding, "name") else str(ss.encoding)
        d["schemaSettings"] = {"schema": ss.schema, "encoding": enc_name}
    return d


def _sub_to_proto(data: dict):
    t = _types()
    return t.Subscription(
        name=data["name"],
        topic=data["topic"],
        ack_deadline_seconds=data.get("ackDeadlineSeconds", 10),
        retain_acked_messages=data.get("retainAckedMessages", False),
        enable_message_ordering=data.get("enableMessageOrdering", False),
        labels=data.get("labels", {}),
    )


def _sub_to_dict(proto) -> dict:
    return {
        "name": proto.name,
        "topic": proto.topic,
        "ackDeadlineSeconds": proto.ack_deadline_seconds or 10,
        "retainAckedMessages": proto.retain_acked_messages,
        "enableMessageOrdering": proto.enable_message_ordering,
        "labels": dict(proto.labels),
        "pushConfig": {"pushEndpoint": "", "attributes": {}},
        "messageRetentionDuration": "604800s",
    }


# ---------------------------------------------------------------------------
# Publisher handlers
# ---------------------------------------------------------------------------


async def _create_topic(request, context):
    store = ps_store.get_store()
    existing = store.get("topics", request.name)
    if existing:
        return _topic_to_proto(existing)
    data = _topic_to_dict(request)
    store.set("topics", request.name, data)
    logger.info("CreateTopic %s", request.name)
    return _topic_to_proto(data)


async def _get_topic(request, context):
    store = ps_store.get_store()
    data = store.get("topics", request.topic)
    if data is None:
        await context.abort(grpc.StatusCode.NOT_FOUND, f"Topic not found: {request.topic}")
        return
    return _topic_to_proto(data)


async def _update_topic(request, context):
    store = ps_store.get_store()
    data = store.get("topics", request.topic.name)
    if data is None:
        await context.abort(grpc.StatusCode.NOT_FOUND, f"Topic not found: {request.topic.name}")
        return
    paths = list(request.update_mask.paths) if request.update_mask else []
    if not paths or "labels" in paths:
        data["labels"] = dict(request.topic.labels)
    store.set("topics", request.topic.name, data)
    return _topic_to_proto(data)


async def _list_topics(request, context):
    t = _types()
    store = ps_store.get_store()
    prefix = f"{request.project}/topics/"
    items = [_topic_to_proto(v) for v in store.list("topics") if v["name"].startswith(prefix)]
    offset = int(request.page_token) if request.page_token else 0
    page_size = request.page_size or 100
    page = items[offset : offset + page_size]
    next_token = str(offset + page_size) if offset + page_size < len(items) else ""
    return t.ListTopicsResponse(topics=page, next_page_token=next_token)


async def _delete_topic(request, context):
    store = ps_store.get_store()
    if not store.delete("topics", request.topic):
        await context.abort(grpc.StatusCode.NOT_FOUND, f"Topic not found: {request.topic}")
        return
    for sub in store.list("subscriptions"):
        if sub.get("topic") == request.topic:
            ps_store.remove_queue(sub["name"])
            store.delete("subscriptions", sub["name"])
    logger.info("DeleteTopic %s", request.topic)
    return empty_pb2.Empty()


async def _publish(request, context):
    t = _types()
    store = ps_store.get_store()
    if not store.exists("topics", request.topic):
        await context.abort(grpc.StatusCode.NOT_FOUND, f"Topic not found: {request.topic}")
        return

    message_ids = []
    for raw_msg in request.messages:
        msg_id = str(uuid.uuid4())
        message_ids.append(msg_id)
        # proto-plus bytes → base64 for REST-compatible storage
        data_b64 = base64.b64encode(bytes(raw_msg.data)).decode("utf-8") if raw_msg.data else ""
        msg = {
            "data": data_b64,
            "attributes": dict(raw_msg.attributes),
            "messageId": msg_id,
            "publishTime": _now(),
            "orderingKey": raw_msg.ordering_key,
        }
        ps_store.log_to_topic(request.topic, msg)
        for sub in store.list("subscriptions"):
            if sub.get("topic") == request.topic:
                sub_name = sub["name"]
                ps_store.ensure_queue(sub_name)
                ps_store.enqueue(sub_name, msg)
    logger.info("Publish %s → %d msg(s)", request.topic, len(message_ids))
    return t.PublishResponse(message_ids=message_ids)


async def _list_topic_subscriptions(request, context):
    t = _types()
    store = ps_store.get_store()
    if not store.exists("topics", request.topic):
        await context.abort(grpc.StatusCode.NOT_FOUND, f"Topic not found: {request.topic}")
        return
    subs = [v["name"] for v in store.list("subscriptions") if v.get("topic") == request.topic]
    offset = int(request.page_token) if request.page_token else 0
    page_size = request.page_size or 100
    page = subs[offset : offset + page_size]
    next_token = str(offset + page_size) if offset + page_size < len(subs) else ""
    return t.ListTopicSubscriptionsResponse(subscriptions=page, next_page_token=next_token)


# ---------------------------------------------------------------------------
# Subscriber handlers
# ---------------------------------------------------------------------------


async def _create_subscription(request, context):
    store = ps_store.get_store()
    existing = store.get("subscriptions", request.name)
    if existing:
        return _sub_to_proto(existing)
    if not store.exists("topics", request.topic):
        await context.abort(grpc.StatusCode.NOT_FOUND, f"Topic not found: {request.topic}")
        return
    data = _sub_to_dict(request)
    store.set("subscriptions", request.name, data)
    ps_store.ensure_queue(request.name)
    logger.info("CreateSubscription %s → %s", request.name, request.topic)
    return _sub_to_proto(data)


async def _get_subscription(request, context):
    store = ps_store.get_store()
    data = store.get("subscriptions", request.subscription)
    if data is None:
        await context.abort(
            grpc.StatusCode.NOT_FOUND, f"Subscription not found: {request.subscription}"
        )
        return
    return _sub_to_proto(data)


async def _update_subscription(request, context):
    store = ps_store.get_store()
    data = store.get("subscriptions", request.subscription.name)
    if data is None:
        await context.abort(
            grpc.StatusCode.NOT_FOUND, f"Subscription not found: {request.subscription.name}"
        )
        return
    paths = list(request.update_mask.paths) if request.update_mask else []
    if not paths or "ack_deadline_seconds" in paths:
        data["ackDeadlineSeconds"] = request.subscription.ack_deadline_seconds
    if not paths or "labels" in paths:
        data["labels"] = dict(request.subscription.labels)
    store.set("subscriptions", request.subscription.name, data)
    return _sub_to_proto(data)


async def _list_subscriptions(request, context):
    t = _types()
    store = ps_store.get_store()
    prefix = f"{request.project}/subscriptions/"
    items = [_sub_to_proto(v) for v in store.list("subscriptions") if v["name"].startswith(prefix)]
    offset = int(request.page_token) if request.page_token else 0
    page_size = request.page_size or 100
    page = items[offset : offset + page_size]
    next_token = str(offset + page_size) if offset + page_size < len(items) else ""
    return t.ListSubscriptionsResponse(subscriptions=page, next_page_token=next_token)


async def _delete_subscription(request, context):
    store = ps_store.get_store()
    if not store.delete("subscriptions", request.subscription):
        await context.abort(
            grpc.StatusCode.NOT_FOUND, f"Subscription not found: {request.subscription}"
        )
        return
    ps_store.remove_queue(request.subscription)
    logger.info("DeleteSubscription %s", request.subscription)
    return empty_pb2.Empty()


async def _pull(request, context):
    t = _types()
    store = ps_store.get_store()
    if not store.exists("subscriptions", request.subscription):
        await context.abort(
            grpc.StatusCode.NOT_FOUND, f"Subscription not found: {request.subscription}"
        )
        return

    ps_store.ensure_queue(request.subscription)
    results = ps_store.pull(request.subscription, request.max_messages)

    received = []
    for ack_id, msg, attempt in results:
        data_bytes = base64.b64decode(msg["data"]) if msg.get("data") else b""
        pubsub_msg = t.PubsubMessage(
            data=data_bytes,
            attributes=msg.get("attributes", {}),
            message_id=msg.get("messageId", ""),
            ordering_key=msg.get("orderingKey", ""),
        )
        received.append(
            t.ReceivedMessage(
                ack_id=ack_id,
                message=pubsub_msg,
                delivery_attempt=attempt,
            )
        )
    return t.PullResponse(received_messages=received)


async def _acknowledge(request, context):
    store = ps_store.get_store()
    if not store.exists("subscriptions", request.subscription):
        await context.abort(
            grpc.StatusCode.NOT_FOUND, f"Subscription not found: {request.subscription}"
        )
        return
    ps_store.acknowledge(request.subscription, list(request.ack_ids))
    return empty_pb2.Empty()


async def _modify_ack_deadline(request, context):
    store = ps_store.get_store()
    if not store.exists("subscriptions", request.subscription):
        await context.abort(
            grpc.StatusCode.NOT_FOUND, f"Subscription not found: {request.subscription}"
        )
        return
    ps_store.modify_ack_deadline(
        request.subscription, list(request.ack_ids), request.ack_deadline_seconds
    )
    return empty_pb2.Empty()


async def _create_snapshot(request, context):
    store = ps_store.get_store()
    if not store.exists("subscriptions", request.subscription):
        await context.abort(
            grpc.StatusCode.NOT_FOUND, f"Subscription not found: {request.subscription}"
        )
        return
    snap = ps_store.create_snapshot(request.name, request.subscription)
    if snap is None:
        await context.abort(
            grpc.StatusCode.NOT_FOUND, f"Subscription not found: {request.subscription}"
        )
        return
    if request.labels:
        snap["labels"] = dict(request.labels)
        store.set("snapshots", request.name, snap)
    t = _types()
    return t.Snapshot(name=snap["name"], topic=snap["topic"], labels=snap.get("labels", {}))


async def _get_snapshot(request, context):
    t = _types()
    store = ps_store.get_store()
    data = store.get("snapshots", request.snapshot)
    if data is None:
        await context.abort(grpc.StatusCode.NOT_FOUND, f"Snapshot not found: {request.snapshot}")
        return
    return t.Snapshot(name=data["name"], topic=data["topic"], labels=data.get("labels", {}))


async def _list_snapshots(request, context):
    t = _types()
    store = ps_store.get_store()
    prefix = f"{request.project}/snapshots/"
    items = [
        t.Snapshot(name=v["name"], topic=v["topic"], labels=v.get("labels", {}))
        for v in store.list("snapshots")
        if v["name"].startswith(prefix)
    ]
    offset = int(request.page_token) if request.page_token else 0
    page_size = request.page_size or 100
    page = items[offset : offset + page_size]
    next_token = str(offset + page_size) if offset + page_size < len(items) else ""
    return t.ListSnapshotsResponse(snapshots=page, next_page_token=next_token)


async def _delete_snapshot(request, context):
    store = ps_store.get_store()
    if not store.delete("snapshots", request.snapshot):
        await context.abort(grpc.StatusCode.NOT_FOUND, f"Snapshot not found: {request.snapshot}")
        return
    return empty_pb2.Empty()


async def _seek(request, context):
    t = _types()
    store = ps_store.get_store()
    sub_data = store.get("subscriptions", request.subscription)
    if sub_data is None:
        await context.abort(
            grpc.StatusCode.NOT_FOUND, f"Subscription not found: {request.subscription}"
        )
        return

    topic = sub_data["topic"]

    if request.snapshot:
        snap = store.get("snapshots", request.snapshot)
        if snap is None:
            await context.abort(
                grpc.StatusCode.NOT_FOUND, f"Snapshot not found: {request.snapshot}"
            )
            return
        since_iso = snap["snapshotTime"]
    elif request.time and request.time.timestamp() > 0:
        # proto-plus maps Timestamp → datetime
        dt = request.time
        since_iso = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    else:
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "seek requires time or snapshot")
        return

    ps_store.seek_subscription(request.subscription, topic, since_iso)
    return t.SeekResponse()


# ---------------------------------------------------------------------------
# SchemaService handlers
# ---------------------------------------------------------------------------


def _schema_type_name(type_val) -> str:
    """Convert a proto-plus Schema.Type enum value to its string name."""
    if hasattr(type_val, "name"):
        return type_val.name
    return str(type_val)


def _dict_to_schema_proto(data: dict):
    st = _schema_types()
    try:
        schema_type = st.Schema.Type[data["type"]]
    except (KeyError, AttributeError):
        schema_type = st.Schema.Type.TYPE_UNSPECIFIED
    return st.Schema(name=data["name"], type_=schema_type, definition=data.get("definition", ""))


async def _create_schema(request, context):
    from cloudbox.services.pubsub.models import validate_schema_definition

    store = ps_store.get_store()
    schema_obj = request.schema  # proto-plus field is 'schema', not 'schema_'
    schema_name = schema_obj.name if schema_obj else ""
    if not schema_name and request.parent:
        schema_id = getattr(request, "schema_id", "") or "unnamed"
        schema_name = f"{request.parent}/schemas/{schema_id}"
    existing = store.get("schemas", schema_name)
    if existing:
        await context.abort(grpc.StatusCode.ALREADY_EXISTS, f"Schema already exists: {schema_name}")
        return
    schema_type = _schema_type_name(schema_obj.type_)
    definition = schema_obj.definition or ""
    err = validate_schema_definition(schema_type, definition)
    if err:
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"Invalid schema: {err}")
        return
    from cloudbox.services.pubsub.app import _now

    data = {
        "name": schema_name,
        "type": schema_type,
        "definition": definition,
        "revisionId": "1",
        "revisionCreateTime": _now(),
    }
    store.set("schemas", schema_name, data)
    return _dict_to_schema_proto(data)


async def _get_schema_grpc(request, context):
    store = ps_store.get_store()
    data = store.get("schemas", request.name)
    if data is None:
        await context.abort(grpc.StatusCode.NOT_FOUND, f"Schema not found: {request.name}")
        return
    return _dict_to_schema_proto(data)


async def _list_schemas_grpc(request, context):
    st = _schema_types()
    store = ps_store.get_store()
    prefix = f"{request.parent}/schemas/"
    items = [
        _dict_to_schema_proto(v) for v in store.list("schemas") if v["name"].startswith(prefix)
    ]
    offset = int(request.page_token) if request.page_token else 0
    page_size = request.page_size or 100
    page = items[offset : offset + page_size]
    next_token = str(offset + page_size) if offset + page_size < len(items) else ""
    return st.ListSchemasResponse(schemas=page, next_page_token=next_token)


async def _delete_schema_grpc(request, context):
    store = ps_store.get_store()
    if not store.delete("schemas", request.name):
        await context.abort(grpc.StatusCode.NOT_FOUND, f"Schema not found: {request.name}")
        return
    return empty_pb2.Empty()


async def _validate_schema_grpc(request, context):
    from cloudbox.services.pubsub.models import validate_schema_definition

    st = _schema_types()
    schema_obj = request.schema
    schema_type = _schema_type_name(schema_obj.type_)
    definition = schema_obj.definition or ""
    err = validate_schema_definition(schema_type, definition)
    if err:
        await context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"Invalid schema: {err}")
        return
    return st.ValidateSchemaResponse()


async def _validate_message_grpc(request, context):
    from cloudbox.services.pubsub.models import validate_message_against_schema

    st = _schema_types()
    store = ps_store.get_store()

    schema_obj = request.schema
    if schema_obj and (schema_obj.name or schema_obj.definition):
        schema_type = _schema_type_name(schema_obj.type_)
        definition = schema_obj.definition or ""
    elif request.name:
        data = store.get("schemas", request.name)
        if data is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, f"Schema not found: {request.name}")
            return
        schema_type = data["type"]
        definition = data.get("definition", "")
    else:
        await context.abort(
            grpc.StatusCode.INVALID_ARGUMENT, "Either schema or name must be provided"
        )
        return

    msg_bytes = bytes(request.message) if request.message else b""
    enc_name = request.encoding.name if hasattr(request.encoding, "name") else str(request.encoding)
    err = validate_message_against_schema(schema_type, definition, msg_bytes, enc_name)
    if err:
        await context.abort(
            grpc.StatusCode.INVALID_ARGUMENT, f"Message failed schema validation: {err}"
        )
        return
    return st.ValidateMessageResponse()


async def _streaming_pull(request_iterator, context):
    """Bidirectional streaming pull.

    Protocol:
    - Client opens the stream and sends an initial StreamingPullRequest with
      ``subscription`` and ``stream_ack_deadline_seconds``.
    - Server loops: pull from the in-memory queue, send StreamingPullResponse
      batches.  When the queue is empty, sleep 50 ms before re-polling.
    - Subsequent client messages carry ``ack_ids`` / ``modify_deadline_*``
      fields; a background reader task processes them concurrently.
    - The stream closes when the client disconnects (StopAsyncIteration on the
      request iterator) or when a write to the context fails.
    """
    t = _types()
    store = ps_store.get_store()

    # ── first request: subscription name + optional initial acks ──────────
    try:
        first_req = await request_iterator.__anext__()
    except (StopAsyncIteration, Exception):
        return

    sub_name = first_req.subscription
    if not store.exists("subscriptions", sub_name):
        await context.abort(grpc.StatusCode.NOT_FOUND, f"Subscription not found: {sub_name}")
        return

    ps_store.ensure_queue(sub_name)

    if first_req.ack_ids:
        ps_store.acknowledge(sub_name, list(first_req.ack_ids))

    # ── background reader: process acks from subsequent client messages ────
    stop = asyncio.Event()

    async def _reader():
        try:
            async for req in request_iterator:
                if req.ack_ids:
                    ps_store.acknowledge(sub_name, list(req.ack_ids))
                for ack_id, deadline in zip(
                    list(req.modify_deadline_ack_ids),
                    list(req.modify_deadline_seconds),
                    strict=False,
                ):
                    ps_store.modify_ack_deadline(sub_name, [ack_id], int(deadline))
        except Exception:
            pass
        finally:
            stop.set()

    reader = asyncio.create_task(_reader())
    logger.info("StreamingPull opened: %s", sub_name)

    # ── delivery loop ──────────────────────────────────────────────────────
    try:
        while not stop.is_set():
            results = ps_store.pull(sub_name, max_messages=1000)
            if results:
                received = []
                for ack_id, msg, attempt in results:
                    data_bytes = base64.b64decode(msg["data"]) if msg.get("data") else b""
                    pubsub_msg = t.PubsubMessage(
                        data=data_bytes,
                        attributes=msg.get("attributes", {}),
                        message_id=msg.get("messageId", ""),
                        ordering_key=msg.get("orderingKey", ""),
                    )
                    received.append(
                        t.ReceivedMessage(
                            ack_id=ack_id,
                            message=pubsub_msg,
                            delivery_attempt=attempt,
                        )
                    )
                await context.write(t.StreamingPullResponse(received_messages=received))
            else:
                await asyncio.sleep(0.05)
    except Exception:
        pass  # client disconnected or context closed
    finally:
        stop.set()
        reader.cancel()
        try:
            await reader
        except asyncio.CancelledError:
            pass
        logger.info("StreamingPull closed: %s", sub_name)


async def _modify_push_config(request, context):
    # Push delivery is not implemented — store the config but do nothing with it
    store = ps_store.get_store()
    data = store.get("subscriptions", request.subscription)
    if data is None:
        await context.abort(
            grpc.StatusCode.NOT_FOUND, f"Subscription not found: {request.subscription}"
        )
        return
    data["pushConfig"] = {
        "pushEndpoint": request.push_config.push_endpoint,
        "attributes": dict(request.push_config.attributes),
    }
    store.set("subscriptions", request.subscription, data)
    return empty_pb2.Empty()


# ---------------------------------------------------------------------------
# GenericRpcHandler that routes method paths to the handlers above
# ---------------------------------------------------------------------------


class _PubSubRpcHandler(grpc.GenericRpcHandler):
    """Routes all /google.pubsub.v1.{Publisher,Subscriber}/* gRPC calls."""

    def __init__(self):
        t = _types()
        st = _schema_types()
        _E = empty_pb2.Empty
        _deser_empty = lambda b: _E()  # noqa: E731

        _p = "/google.pubsub.v1.Publisher/"
        _s = "/google.pubsub.v1.Subscriber/"
        _sc = "/google.pubsub.v1.SchemaService/"

        self._map: dict[str, grpc.RpcMethodHandler] = {
            # Publisher
            _p + "CreateTopic": grpc.unary_unary_rpc_method_handler(
                _create_topic,
                request_deserializer=t.Topic.deserialize,
                response_serializer=t.Topic.serialize,
            ),
            _p + "GetTopic": grpc.unary_unary_rpc_method_handler(
                _get_topic,
                request_deserializer=t.GetTopicRequest.deserialize,
                response_serializer=t.Topic.serialize,
            ),
            _p + "UpdateTopic": grpc.unary_unary_rpc_method_handler(
                _update_topic,
                request_deserializer=t.UpdateTopicRequest.deserialize,
                response_serializer=t.Topic.serialize,
            ),
            _p + "ListTopics": grpc.unary_unary_rpc_method_handler(
                _list_topics,
                request_deserializer=t.ListTopicsRequest.deserialize,
                response_serializer=t.ListTopicsResponse.serialize,
            ),
            _p + "DeleteTopic": grpc.unary_unary_rpc_method_handler(
                _delete_topic,
                request_deserializer=t.DeleteTopicRequest.deserialize,
                response_serializer=_ser_empty,
            ),
            _p + "Publish": grpc.unary_unary_rpc_method_handler(
                _publish,
                request_deserializer=t.PublishRequest.deserialize,
                response_serializer=t.PublishResponse.serialize,
            ),
            _p + "ListTopicSubscriptions": grpc.unary_unary_rpc_method_handler(
                _list_topic_subscriptions,
                request_deserializer=t.ListTopicSubscriptionsRequest.deserialize,
                response_serializer=t.ListTopicSubscriptionsResponse.serialize,
            ),
            # Subscriber
            _s + "CreateSubscription": grpc.unary_unary_rpc_method_handler(
                _create_subscription,
                request_deserializer=t.Subscription.deserialize,
                response_serializer=t.Subscription.serialize,
            ),
            _s + "GetSubscription": grpc.unary_unary_rpc_method_handler(
                _get_subscription,
                request_deserializer=t.GetSubscriptionRequest.deserialize,
                response_serializer=t.Subscription.serialize,
            ),
            _s + "UpdateSubscription": grpc.unary_unary_rpc_method_handler(
                _update_subscription,
                request_deserializer=t.UpdateSubscriptionRequest.deserialize,
                response_serializer=t.Subscription.serialize,
            ),
            _s + "ListSubscriptions": grpc.unary_unary_rpc_method_handler(
                _list_subscriptions,
                request_deserializer=t.ListSubscriptionsRequest.deserialize,
                response_serializer=t.ListSubscriptionsResponse.serialize,
            ),
            _s + "DeleteSubscription": grpc.unary_unary_rpc_method_handler(
                _delete_subscription,
                request_deserializer=t.DeleteSubscriptionRequest.deserialize,
                response_serializer=_ser_empty,
            ),
            _s + "Pull": grpc.unary_unary_rpc_method_handler(
                _pull,
                request_deserializer=t.PullRequest.deserialize,
                response_serializer=t.PullResponse.serialize,
            ),
            _s + "Acknowledge": grpc.unary_unary_rpc_method_handler(
                _acknowledge,
                request_deserializer=t.AcknowledgeRequest.deserialize,
                response_serializer=_ser_empty,
            ),
            _s + "ModifyAckDeadline": grpc.unary_unary_rpc_method_handler(
                _modify_ack_deadline,
                request_deserializer=t.ModifyAckDeadlineRequest.deserialize,
                response_serializer=_ser_empty,
            ),
            _s + "ModifyPushConfig": grpc.unary_unary_rpc_method_handler(
                _modify_push_config,
                request_deserializer=t.ModifyPushConfigRequest.deserialize,
                response_serializer=_ser_empty,
            ),
            _s + "StreamingPull": grpc.stream_stream_rpc_method_handler(
                _streaming_pull,
                request_deserializer=t.StreamingPullRequest.deserialize,
                response_serializer=t.StreamingPullResponse.serialize,
            ),
            _s + "CreateSnapshot": grpc.unary_unary_rpc_method_handler(
                _create_snapshot,
                request_deserializer=t.CreateSnapshotRequest.deserialize,
                response_serializer=t.Snapshot.serialize,
            ),
            _s + "GetSnapshot": grpc.unary_unary_rpc_method_handler(
                _get_snapshot,
                request_deserializer=t.GetSnapshotRequest.deserialize,
                response_serializer=t.Snapshot.serialize,
            ),
            _s + "ListSnapshots": grpc.unary_unary_rpc_method_handler(
                _list_snapshots,
                request_deserializer=t.ListSnapshotsRequest.deserialize,
                response_serializer=t.ListSnapshotsResponse.serialize,
            ),
            _s + "DeleteSnapshot": grpc.unary_unary_rpc_method_handler(
                _delete_snapshot,
                request_deserializer=t.DeleteSnapshotRequest.deserialize,
                response_serializer=_ser_empty,
            ),
            _s + "Seek": grpc.unary_unary_rpc_method_handler(
                _seek,
                request_deserializer=t.SeekRequest.deserialize,
                response_serializer=t.SeekResponse.serialize,
            ),
            # SchemaService
            _sc + "CreateSchema": grpc.unary_unary_rpc_method_handler(
                _create_schema,
                request_deserializer=st.CreateSchemaRequest.deserialize,
                response_serializer=st.Schema.serialize,
            ),
            _sc + "GetSchema": grpc.unary_unary_rpc_method_handler(
                _get_schema_grpc,
                request_deserializer=st.GetSchemaRequest.deserialize,
                response_serializer=st.Schema.serialize,
            ),
            _sc + "ListSchemas": grpc.unary_unary_rpc_method_handler(
                _list_schemas_grpc,
                request_deserializer=st.ListSchemasRequest.deserialize,
                response_serializer=st.ListSchemasResponse.serialize,
            ),
            _sc + "DeleteSchema": grpc.unary_unary_rpc_method_handler(
                _delete_schema_grpc,
                request_deserializer=st.DeleteSchemaRequest.deserialize,
                response_serializer=_ser_empty,
            ),
            _sc + "ValidateSchema": grpc.unary_unary_rpc_method_handler(
                _validate_schema_grpc,
                request_deserializer=st.ValidateSchemaRequest.deserialize,
                response_serializer=st.ValidateSchemaResponse.serialize,
            ),
            _sc + "ValidateMessage": grpc.unary_unary_rpc_method_handler(
                _validate_message_grpc,
                request_deserializer=st.ValidateMessageRequest.deserialize,
                response_serializer=st.ValidateMessageResponse.serialize,
            ),
        }

    def service_name(self) -> str:
        return ""

    def service(self, handler_call_details: grpc.HandlerCallDetails):
        handler = self._map.get(handler_call_details.method)
        if handler is None:
            logger.debug("Unhandled gRPC method: %s", handler_call_details.method)
        return handler


# ---------------------------------------------------------------------------
# Server factory
# ---------------------------------------------------------------------------


async def create_server(host: str, port: int) -> grpc.aio.Server:
    """Create (but do not start) the Pub/Sub gRPC server."""
    server = grpc.aio.server()
    server.add_generic_rpc_handlers([_PubSubRpcHandler()])
    listen_addr = f"{host}:{port}"
    server.add_insecure_port(listen_addr)
    logger.info("Pub/Sub gRPC server bound to %s", listen_addr)
    return server
