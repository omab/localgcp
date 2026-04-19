"""Cloud Pub/Sub emulator.

Implements the Pub/Sub REST API v1 used by google-cloud-pubsub.

Route design: use concrete path patterns (topics vs subscriptions) instead
of catch-alls so FastAPI can route correctly.
"""

from __future__ import annotations

import base64
import json
import logging
import uuid
from datetime import datetime, timezone

import httpx
from fastapi import BackgroundTasks, FastAPI, Response
from fastapi.responses import JSONResponse

from cloudbox.core.errors import GCPError, add_gcp_exception_handler
from cloudbox.core.middleware import add_request_logging
from cloudbox.services.pubsub import store as ps_store
from cloudbox.services.pubsub.filter import matches as filter_matches
from cloudbox.services.pubsub.models import (
    AcknowledgeRequest,
    CreateSnapshotRequest,
    CreateTopicBody,
    ModifyAckDeadlineRequest,
    PublishRequest,
    PublishResponse,
    PullRequest,
    PullResponse,
    PubsubMessage,
    ReceivedMessage,
    SchemaListResponse,
    SchemaModel,
    SeekRequest,
    SnapshotListResponse,
    SnapshotModel,
    SubscriptionListResponse,
    SubscriptionModel,
    TopicListResponse,
    TopicModel,
    ValidateMessageRequest,
    ValidateSchemaRequest,
    validate_message_against_schema,
    validate_schema_definition,
)

app = FastAPI(title="Cloudbox — Cloud Pub/Sub", version="v1")
add_gcp_exception_handler(app)
add_request_logging(app, "pubsub")

logger = logging.getLogger("cloudbox.pubsub")


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


def _parse_bq_table_ref(table_ref: str) -> tuple[str, str, str] | None:
    """Parse 'project:dataset.table' or 'project.dataset.table' → (project, dataset, table)."""
    normalized = table_ref.replace(":", ".", 1)
    parts = normalized.split(".")
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    return None


async def _write_to_bigquery(sub_data: dict, msg: dict) -> None:
    """Insert a Pub/Sub message into a BigQuery table."""
    from cloudbox.services.bigquery.engine import get_engine

    bq_cfg = sub_data.get("bigqueryConfig") or {}
    table_ref = bq_cfg.get("table", "")
    parsed = _parse_bq_table_ref(table_ref)
    if not parsed:
        logger.warning("Invalid BigQuery table reference: %s", table_ref)
        return
    project, dataset_id, table_id = parsed

    write_metadata = bq_cfg.get("writeMetadata", False)
    use_topic_schema = bq_cfg.get("useTopicSchema", False)
    drop_unknown = bq_cfg.get("dropUnknownFields", False)

    engine = get_engine()

    row: dict = {}
    if use_topic_schema:
        raw = msg.get("data", "")
        try:
            row = json.loads(base64.b64decode(raw).decode("utf-8")) if raw else {}
        except Exception as exc:
            logger.warning("BQ subscription: failed to decode message as JSON: %s", exc)
            row = {}
    else:
        row["data"] = msg.get("data", "")  # stored as base64 string

    if write_metadata:
        row["subscription_name"] = sub_data.get("name", "")
        row["message_id"] = msg.get("messageId", "")
        row["publish_time"] = msg.get("publishTime", "")
        row["attributes"] = json.dumps(msg.get("attributes", {}))

    if drop_unknown:
        tbl = engine.get_table(project, dataset_id, table_id)
        if tbl:
            field_names = {f["name"] for f in (tbl.get("schema") or {}).get("fields", [])}
            row = {k: v for k, v in row.items() if k in field_names}

    try:
        engine.insert_rows(project, dataset_id, table_id, [{"json": row}])
        logger.debug("BQ subscription wrote row to %s", table_ref)
    except Exception as exc:
        logger.warning("BQ subscription failed to write to %s: %s", table_ref, exc)


async def _write_to_gcs(sub_data: dict, msg: dict) -> None:
    """Write a Pub/Sub message as a Cloud Storage object."""
    import hashlib
    from cloudbox.services.gcs.store import get_store as get_gcs_store
    from cloudbox.services.gcs.models import ObjectModel

    gcs_cfg = sub_data.get("cloudStorageConfig") or {}
    bucket = gcs_cfg.get("bucket", "")
    if not bucket:
        logger.warning(
            "cloudStorageConfig missing bucket for subscription %s",
            sub_data.get("name", ""),
        )
        return

    gcs_store = get_gcs_store()
    if not gcs_store.exists("buckets", bucket):
        logger.warning(
            "GCS bucket '%s' not found for subscription %s", bucket, sub_data.get("name", "")
        )
        return

    prefix = gcs_cfg.get("filenamePrefix", "")
    suffix = gcs_cfg.get("filenameSuffix", "")
    avro_cfg = gcs_cfg.get("avroConfig")

    raw_data = msg.get("data", "")
    data_bytes = base64.b64decode(raw_data) if raw_data else b""

    if avro_cfg is not None:
        write_metadata = (avro_cfg or {}).get("writeMetadata", False)
        record: dict = {"data": raw_data}
        if write_metadata:
            record.update(
                {
                    "subscription_name": sub_data.get("name", ""),
                    "message_id": msg.get("messageId", ""),
                    "publish_time": msg.get("publishTime", ""),
                    "attributes": msg.get("attributes", {}),
                }
            )
        body_bytes = json.dumps(record).encode("utf-8")
        content_type = "application/avro"
    else:
        body_bytes = data_bytes
        content_type = "text/plain"

    # Object name: {prefix}{publish_time_safe}_{messageId}{suffix}
    pub_time = msg.get("publishTime", _now()).replace(":", "").replace(".", "")
    msg_id = msg.get("messageId", str(uuid.uuid4()))
    obj_name = f"{prefix}{pub_time}_{msg_id}{suffix}"
    store_key = f"{bucket}/{obj_name}"

    md5_hash = base64.b64encode(hashlib.md5(body_bytes).digest()).decode()
    obj_meta = ObjectModel(
        name=obj_name,
        bucket=bucket,
        size=str(len(body_bytes)),
        contentType=content_type,
        md5Hash=md5_hash,
    ).model_dump()

    gcs_store.set("objects", store_key, obj_meta)
    gcs_store.set("bodies", store_key, body_bytes)
    logger.debug("GCS subscription wrote object gs://%s/%s", bucket, obj_name)


# ---------------------------------------------------------------------------
# Topics
# ---------------------------------------------------------------------------


@app.put("/v1/projects/{project}/topics/{topic_id}")
async def create_topic(project: str, topic_id: str, body: CreateTopicBody | None = None):
    full_name = f"projects/{project}/topics/{topic_id}"
    store = ps_store.get_store()
    existing = store.get("topics", full_name)
    if existing:
        return existing
    t = TopicModel(
        name=full_name,
        labels=(body.labels if body else {}),
        messageRetentionDuration=(body.messageRetentionDuration if body else "604800s"),
        schemaSettings=(body.schemaSettings if body else None),
    )
    if t.schemaSettings and t.schemaSettings.schema_:
        if not store.exists("schemas", t.schemaSettings.schema_):
            raise GCPError(404, f"Schema not found: {t.schemaSettings.schema_}")
    store.set("topics", full_name, t.model_dump(by_alias=True, exclude_none=False))
    return t.model_dump(by_alias=True, exclude_none=True)


@app.patch("/v1/projects/{project}/topics/{topic_id}")
async def update_topic(project: str, topic_id: str, body: CreateTopicBody | None = None):
    full_name = f"projects/{project}/topics/{topic_id}"
    store = ps_store.get_store()
    data = store.get("topics", full_name)
    if data is None:
        raise GCPError(404, f"Topic not found: {full_name}")
    if body:
        if body.labels is not None:
            data["labels"] = body.labels
        if body.schemaSettings is not None:
            if body.schemaSettings.schema_ and not store.exists(
                "schemas", body.schemaSettings.schema_
            ):
                raise GCPError(404, f"Schema not found: {body.schemaSettings.schema_}")
            data["schemaSettings"] = body.schemaSettings.model_dump(by_alias=True)
        data["messageRetentionDuration"] = body.messageRetentionDuration
    store.set("topics", full_name, data)
    return TopicModel.model_validate(data).model_dump(by_alias=True, exclude_none=True)


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
    items = [
        TopicModel.model_validate(v) for v in store.list("topics") if v["name"].startswith(prefix)
    ]
    offset = int(pageToken) if pageToken else 0
    page = items[offset : offset + pageSize]
    next_token = str(offset + pageSize) if offset + pageSize < len(items) else None
    return TopicListResponse(topics=page, nextPageToken=next_token).model_dump(
        by_alias=True, exclude_none=True
    )


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

    # If the topic has schema settings, validate each message before publishing
    topic_data = store.get("topics", full_name)
    schema_settings = topic_data.get("schemaSettings") if topic_data else None
    if schema_settings and schema_settings.get("schema"):
        schema_res = schema_settings["schema"]
        encoding = schema_settings.get("encoding", "ENCODING_UNSPECIFIED")
        schema_data = store.get("schemas", schema_res)
        if schema_data is not None:
            schema_type = schema_data.get("type", "TYPE_UNSPECIFIED")
            definition = schema_data.get("definition", "")
            for raw_msg in body.messages:
                raw_data = raw_msg.get("data", "")
                try:
                    msg_bytes = base64.b64decode(raw_data) if raw_data else b""
                except Exception:
                    raise GCPError(400, "Message data is not valid base64") from e
                err = validate_message_against_schema(schema_type, definition, msg_bytes, encoding)
                if err:
                    raise GCPError(400, f"Message failed schema validation: {err}")

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
        ps_store.log_to_topic(full_name, msg)
        for sub in store.list("subscriptions"):
            if sub.get("topic") != full_name:
                continue
            sub_name = sub["name"]

            # BigQuery subscription — write directly to BQ, skip queue
            if (sub.get("bigqueryConfig") or {}).get("table"):
                await _write_to_bigquery(sub, msg)
                continue

            # Cloud Storage subscription — write directly to GCS, skip queue
            if (sub.get("cloudStorageConfig") or {}).get("bucket"):
                await _write_to_gcs(sub, msg)
                continue

            # Normal subscription: apply filter, enqueue, dispatch push if configured
            if not filter_matches(sub.get("filter", ""), msg):
                continue
            push_endpoint = (sub.get("pushConfig") or {}).get("pushEndpoint", "")
            ps_store.ensure_queue(sub_name)
            ps_store.enqueue(sub_name, msg)
            if push_endpoint:
                # Pull immediately to get an ack_id so _dispatch_push can ack/nack
                pulled = ps_store.pull(sub_name, 1)
                if pulled:
                    ack_id, pulled_msg, _ = pulled[0]
                    background_tasks.add_task(
                        _dispatch_push, push_endpoint, sub_name, ack_id, pulled_msg
                    )

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

    sub = SubscriptionModel(
        name=full_name, **{k: v for k, v in body.model_dump().items() if k != "name"}
    )

    # Validate BigQuery / Cloud Storage configs
    if sub.bigqueryConfig and sub.bigqueryConfig.table:
        if not _parse_bq_table_ref(sub.bigqueryConfig.table):
            raise GCPError(400, f"Invalid BigQuery table reference: {sub.bigqueryConfig.table}")
    if sub.cloudStorageConfig and not sub.cloudStorageConfig.bucket:
        raise GCPError(400, "cloudStorageConfig.bucket is required")

    store.set("subscriptions", full_name, sub.model_dump())
    # BQ/GCS subscriptions have no pull queue — ensure_queue is still harmless
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
    items = [
        SubscriptionModel(**v) for v in store.list("subscriptions") if v["name"].startswith(prefix)
    ]
    offset = int(pageToken) if pageToken else 0
    page = items[offset : offset + pageSize]
    next_token = str(offset + pageSize) if offset + pageSize < len(items) else None
    return SubscriptionListResponse(subscriptions=page, nextPageToken=next_token).model_dump(
        exclude_none=True
    )


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
        raise GCPError(
            400,
            f"Subscription {full_name} is a push subscription and cannot be pulled from directly",
        )

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


@app.post("/v1/projects/{project}/subscriptions/{sub_id}:seek")
async def seek(project: str, sub_id: str, body: SeekRequest):
    full_name = f"projects/{project}/subscriptions/{sub_id}"
    store = ps_store.get_store()
    sub_data = store.get("subscriptions", full_name)
    if sub_data is None:
        raise GCPError(404, f"Subscription not found: {full_name}")

    topic = sub_data["topic"]

    if body.snapshot:
        snap = store.get("snapshots", body.snapshot)
        if snap is None:
            raise GCPError(404, f"Snapshot not found: {body.snapshot}")
        since_iso = snap["snapshotTime"]
    elif body.time:
        since_iso = body.time
    else:
        raise GCPError(400, "seek requires either 'time' or 'snapshot'")

    ps_store.seek_subscription(full_name, topic, since_iso)
    return {}


# ---------------------------------------------------------------------------
# Snapshots
# ---------------------------------------------------------------------------


@app.put("/v1/projects/{project}/snapshots/{snap_id}")
async def create_snapshot(project: str, snap_id: str, body: CreateSnapshotRequest):
    snap_name = f"projects/{project}/snapshots/{snap_id}"
    store = ps_store.get_store()
    if not store.exists("subscriptions", body.subscription):
        raise GCPError(404, f"Subscription not found: {body.subscription}")
    snap = ps_store.create_snapshot(snap_name, body.subscription)
    if snap is None:
        raise GCPError(404, f"Subscription not found: {body.subscription}")
    if body.labels:
        snap["labels"] = body.labels
        store.set("snapshots", snap_name, snap)
    return SnapshotModel(**snap).model_dump()


@app.get("/v1/projects/{project}/snapshots/{snap_id}")
async def get_snapshot(project: str, snap_id: str):
    snap_name = f"projects/{project}/snapshots/{snap_id}"
    store = ps_store.get_store()
    data = store.get("snapshots", snap_name)
    if data is None:
        raise GCPError(404, f"Snapshot not found: {snap_name}")
    return SnapshotModel(**data).model_dump()


@app.get("/v1/projects/{project}/snapshots")
async def list_snapshots(project: str, pageSize: int = 100, pageToken: str = ""):
    store = ps_store.get_store()
    prefix = f"projects/{project}/snapshots/"
    items = [SnapshotModel(**v) for v in store.list("snapshots") if v["name"].startswith(prefix)]
    offset = int(pageToken) if pageToken else 0
    page = items[offset : offset + pageSize]
    next_token = str(offset + pageSize) if offset + pageSize < len(items) else None
    return SnapshotListResponse(snapshots=page, nextPageToken=next_token).model_dump(
        exclude_none=True
    )


@app.patch("/v1/projects/{project}/snapshots/{snap_id}")
async def update_snapshot(project: str, snap_id: str, body: SnapshotModel):
    snap_name = f"projects/{project}/snapshots/{snap_id}"
    store = ps_store.get_store()
    data = store.get("snapshots", snap_name)
    if data is None:
        raise GCPError(404, f"Snapshot not found: {snap_name}")
    if body.labels is not None:
        data["labels"] = body.labels
    if body.expireTime:
        data["expireTime"] = body.expireTime
    store.set("snapshots", snap_name, data)
    return SnapshotModel(**data).model_dump()


@app.delete("/v1/projects/{project}/snapshots/{snap_id}", status_code=204)
async def delete_snapshot(project: str, snap_id: str):
    snap_name = f"projects/{project}/snapshots/{snap_id}"
    store = ps_store.get_store()
    if not store.delete("snapshots", snap_name):
        raise GCPError(404, f"Snapshot not found: {snap_name}")
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


@app.post("/v1/projects/{project}/schemas")
async def create_schema(project: str, body: SchemaModel, schemaId: str = ""):
    store = ps_store.get_store()
    schema_id = schemaId or body.name.split("/")[-1] if body.name else ""
    if not schema_id:
        raise GCPError(400, "schemaId query parameter or schema name is required")
    full_name = f"projects/{project}/schemas/{schema_id}"
    existing = store.get("schemas", full_name)
    if existing:
        raise GCPError(409, f"Schema already exists: {full_name}")
    err = validate_schema_definition(body.type, body.definition)
    if err:
        raise GCPError(400, f"Invalid schema: {err}")
    schema = SchemaModel(
        name=full_name,
        type=body.type,
        definition=body.definition,
        revisionId="1",
        revisionCreateTime=_now(),
    )
    store.set("schemas", full_name, schema.model_dump())
    return schema.model_dump()


@app.get("/v1/projects/{project}/schemas/{schema_id}")
async def get_schema(project: str, schema_id: str):
    full_name = f"projects/{project}/schemas/{schema_id}"
    store = ps_store.get_store()
    data = store.get("schemas", full_name)
    if data is None:
        raise GCPError(404, f"Schema not found: {full_name}")
    return data


@app.get("/v1/projects/{project}/schemas")
async def list_schemas(project: str, pageSize: int = 100, pageToken: str = ""):
    store = ps_store.get_store()
    prefix = f"projects/{project}/schemas/"
    items = [SchemaModel(**v) for v in store.list("schemas") if v["name"].startswith(prefix)]
    offset = int(pageToken) if pageToken else 0
    page = items[offset : offset + pageSize]
    next_token = str(offset + pageSize) if offset + pageSize < len(items) else None
    return SchemaListResponse(schemas=page, nextPageToken=next_token).model_dump(exclude_none=True)


@app.delete("/v1/projects/{project}/schemas/{schema_id}", status_code=204)
async def delete_schema(project: str, schema_id: str):
    full_name = f"projects/{project}/schemas/{schema_id}"
    store = ps_store.get_store()
    if not store.delete("schemas", full_name):
        raise GCPError(404, f"Schema not found: {full_name}")
    return Response(status_code=204)


@app.post("/v1/projects/{project}/schemas:validate")
async def validate_schema_endpoint(project: str, body: ValidateSchemaRequest):
    err = validate_schema_definition(body.schema_.type, body.schema_.definition)
    if err:
        raise GCPError(400, f"Invalid schema: {err}")
    return {}


@app.post("/v1/projects/{project}/schemas:validateMessage")
async def validate_message_endpoint(project: str, body: ValidateMessageRequest):
    store = ps_store.get_store()

    # Resolve schema: inline or by resource name
    if body.schema_:
        schema_type = body.schema_.type
        definition = body.schema_.definition
    elif body.name:
        schema_data = store.get("schemas", body.name)
        if schema_data is None:
            raise GCPError(404, f"Schema not found: {body.name}")
        schema_type = schema_data["type"]
        definition = schema_data["definition"]
    else:
        raise GCPError(400, "Either 'schema' or 'name' must be provided")

    try:
        msg_bytes = base64.b64decode(body.message) if body.message else b""
    except Exception:
        raise GCPError(400, "message is not valid base64") from e

    err = validate_message_against_schema(schema_type, definition, msg_bytes, body.encoding)
    if err:
        raise GCPError(400, f"Message failed schema validation: {err}")
    return {}
