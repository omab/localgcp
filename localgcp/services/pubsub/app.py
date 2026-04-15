"""Cloud Pub/Sub emulator.

Implements the Pub/Sub REST API v1 used by google-cloud-pubsub.

Route design: use concrete path patterns (topics vs subscriptions) instead
of catch-alls so FastAPI can route correctly.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

import httpx
from fastapi import BackgroundTasks, FastAPI, Response
from fastapi.responses import JSONResponse

from localgcp.core.errors import GCPError, add_gcp_exception_handler
from localgcp.core.middleware import add_request_logging
from localgcp.services.pubsub import store as ps_store
from localgcp.services.pubsub.models import (
    AcknowledgeRequest,
    ModifyAckDeadlineRequest,
    PublishRequest,
    PublishResponse,
    PullRequest,
    PullResponse,
    PubsubMessage,
    ReceivedMessage,
    SubscriptionListResponse,
    SubscriptionModel,
    TopicListResponse,
    TopicModel,
)

app = FastAPI(title="LocalGCP — Cloud Pub/Sub", version="v1")
add_gcp_exception_handler(app)
add_request_logging(app, "pubsub")

logger = logging.getLogger("localgcp.pubsub")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


async def _dispatch_push(push_endpoint: str, sub_name: str, ack_id: str, message: dict) -> None:
    """POST a message to a push subscription's endpoint.

    The payload matches the GCP Pub/Sub push message format:
        {"message": {...}, "subscription": "projects/.../subscriptions/..."}

    A 2xx response from the endpoint is treated as an acknowledgement (ack).
    Non-2xx responses and connection errors nack the message by setting its
    ack deadline to 0, making it immediately eligible for redelivery.
    """
    payload = {"message": message, "subscription": sub_name}
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(push_endpoint, json=payload, timeout=10.0)
        if resp.status_code < 300:
            ps_store.acknowledge(sub_name, [ack_id])
        else:
            logger.warning("Push delivery to %s returned HTTP %d", push_endpoint, resp.status_code)
            ps_store.modify_ack_deadline(sub_name, [ack_id], 0)
    except Exception as exc:
        logger.warning("Push delivery to %s failed: %s", push_endpoint, exc)
        ps_store.modify_ack_deadline(sub_name, [ack_id], 0)


# ---------------------------------------------------------------------------
# Topics
# ---------------------------------------------------------------------------


@app.put("/v1/projects/{project}/topics/{topic_id}")
async def create_topic(project: str, topic_id: str):
    full_name = f"projects/{project}/topics/{topic_id}"
    store = ps_store.get_store()
    existing = store.get("topics", full_name)
    if existing:
        return existing
    t = TopicModel(name=full_name)
    store.set("topics", full_name, t.model_dump())
    return t.model_dump()


@app.get("/v1/projects/{project}/topics/{topic_id}")
async def get_topic(project: str, topic_id: str):
    full_name = f"projects/{project}/topics/{topic_id}"
    store = ps_store.get_store()
    data = store.get("topics", full_name)
    if data is None:
        raise GCPError(404, f"Topic not found: {full_name}")
    return data


@app.get("/v1/projects/{project}/topics")
async def list_topics(project: str, pageSize: int = 100, pageToken: str = ""):
    store = ps_store.get_store()
    prefix = f"projects/{project}/topics/"
    items = [TopicModel(**v) for v in store.list("topics") if v["name"].startswith(prefix)]
    offset = int(pageToken) if pageToken else 0
    page = items[offset: offset + pageSize]
    next_token = str(offset + pageSize) if offset + pageSize < len(items) else None
    return TopicListResponse(topics=page, nextPageToken=next_token).model_dump(exclude_none=True)


@app.delete("/v1/projects/{project}/topics/{topic_id}", status_code=204)
async def delete_topic(project: str, topic_id: str):
    full_name = f"projects/{project}/topics/{topic_id}"
    store = ps_store.get_store()
    if not store.delete("topics", full_name):
        raise GCPError(404, f"Topic not found: {full_name}")
    # Remove all subscriptions pointing to this topic
    for sub in store.list("subscriptions"):
        if sub.get("topic") == full_name:
            ps_store.remove_queue(sub["name"])
            store.delete("subscriptions", sub["name"])
    return Response(status_code=204)


@app.post("/v1/projects/{project}/topics/{topic_id}:publish")
async def publish(
    project: str, topic_id: str, body: PublishRequest, background_tasks: BackgroundTasks
):
    full_name = f"projects/{project}/topics/{topic_id}"
    store = ps_store.get_store()
    if not store.exists("topics", full_name):
        raise GCPError(404, f"Topic not found: {full_name}")

    message_ids = []
    for raw_msg in body.messages:
        msg_id = str(uuid.uuid4())
        message_ids.append(msg_id)
        msg = {
            "data": raw_msg.get("data", ""),
            "attributes": raw_msg.get("attributes", {}),
            "messageId": msg_id,
            "publishTime": _now(),
            "orderingKey": raw_msg.get("orderingKey", ""),
        }
        for sub in store.list("subscriptions"):
            if sub.get("topic") != full_name:
                continue
            sub_name = sub["name"]
            push_endpoint = (sub.get("pushConfig") or {}).get("pushEndpoint", "")
            ps_store.ensure_queue(sub_name)
            ps_store.enqueue(sub_name, msg)
            if push_endpoint:
                # Pull immediately to get an ack_id so _dispatch_push can ack/nack
                pulled = ps_store.pull(sub_name, 1)
                if pulled:
                    ack_id, pulled_msg, _ = pulled[0]
                    background_tasks.add_task(_dispatch_push, push_endpoint, sub_name, ack_id, pulled_msg)

    return PublishResponse(messageIds=message_ids).model_dump()


# ---------------------------------------------------------------------------
# Subscriptions
# ---------------------------------------------------------------------------


@app.put("/v1/projects/{project}/subscriptions/{sub_id}")
async def create_subscription(project: str, sub_id: str, body: SubscriptionModel):
    full_name = f"projects/{project}/subscriptions/{sub_id}"
    store = ps_store.get_store()
    existing = store.get("subscriptions", full_name)
    if existing:
        return existing

    if not store.exists("topics", body.topic):
        raise GCPError(404, f"Topic not found: {body.topic}")

    sub = SubscriptionModel(name=full_name, **{k: v for k, v in body.model_dump().items() if k != "name"})
    store.set("subscriptions", full_name, sub.model_dump())
    ps_store.ensure_queue(full_name)
    return sub.model_dump()


@app.get("/v1/projects/{project}/subscriptions/{sub_id}")
async def get_subscription(project: str, sub_id: str):
    full_name = f"projects/{project}/subscriptions/{sub_id}"
    store = ps_store.get_store()
    data = store.get("subscriptions", full_name)
    if data is None:
        raise GCPError(404, f"Subscription not found: {full_name}")
    return data


@app.get("/v1/projects/{project}/subscriptions")
async def list_subscriptions(project: str, pageSize: int = 100, pageToken: str = ""):
    store = ps_store.get_store()
    prefix = f"projects/{project}/subscriptions/"
    items = [SubscriptionModel(**v) for v in store.list("subscriptions") if v["name"].startswith(prefix)]
    offset = int(pageToken) if pageToken else 0
    page = items[offset: offset + pageSize]
    next_token = str(offset + pageSize) if offset + pageSize < len(items) else None
    return SubscriptionListResponse(subscriptions=page, nextPageToken=next_token).model_dump(exclude_none=True)


@app.delete("/v1/projects/{project}/subscriptions/{sub_id}", status_code=204)
async def delete_subscription(project: str, sub_id: str):
    full_name = f"projects/{project}/subscriptions/{sub_id}"
    store = ps_store.get_store()
    if not store.delete("subscriptions", full_name):
        raise GCPError(404, f"Subscription not found: {full_name}")
    ps_store.remove_queue(full_name)
    return Response(status_code=204)


@app.post("/v1/projects/{project}/subscriptions/{sub_id}:pull")
async def pull_messages(project: str, sub_id: str, body: PullRequest):
    full_name = f"projects/{project}/subscriptions/{sub_id}"
    store = ps_store.get_store()
    sub_data = store.get("subscriptions", full_name)
    if sub_data is None:
        raise GCPError(404, f"Subscription not found: {full_name}")

    push_endpoint = (sub_data.get("pushConfig") or {}).get("pushEndpoint", "")
    if push_endpoint:
        raise GCPError(400, f"Subscription {full_name} is a push subscription and cannot be pulled from directly")

    ps_store.ensure_queue(full_name)
    results = ps_store.pull(full_name, body.maxMessages)

    received = [
        ReceivedMessage(
            ackId=ack_id,
            message=PubsubMessage(**msg),
            deliveryAttempt=attempt,
        )
        for ack_id, msg, attempt in results
    ]
    return PullResponse(receivedMessages=received).model_dump()


@app.post("/v1/projects/{project}/subscriptions/{sub_id}:acknowledge")
async def acknowledge(project: str, sub_id: str, body: AcknowledgeRequest):
    full_name = f"projects/{project}/subscriptions/{sub_id}"
    store = ps_store.get_store()
    if not store.exists("subscriptions", full_name):
        raise GCPError(404, f"Subscription not found: {full_name}")
    ps_store.acknowledge(full_name, body.ackIds)
    return {}


@app.post("/v1/projects/{project}/subscriptions/{sub_id}:modifyAckDeadline")
async def modify_ack_deadline(project: str, sub_id: str, body: ModifyAckDeadlineRequest):
    full_name = f"projects/{project}/subscriptions/{sub_id}"
    store = ps_store.get_store()
    if not store.exists("subscriptions", full_name):
        raise GCPError(404, f"Subscription not found: {full_name}")
    ps_store.modify_ack_deadline(full_name, body.ackIds, body.ackDeadlineSeconds)
    return {}
