"""Pydantic models for Pub/Sub REST API."""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class SchemaSettings(BaseModel):
    """Schema settings for a Pub/Sub topic."""

    model_config = ConfigDict(populate_by_name=True)

    schema_: str = Field("", alias="schema")  # resource name, e.g. projects/p/schemas/my-schema
    encoding: str = "ENCODING_UNSPECIFIED"  # JSON or BINARY


class CreateTopicBody(BaseModel):
    """Request body for PUT /v1/projects/{project}/topics/{topic_id}."""

    labels: dict[str, str] = Field(default_factory=dict)
    messageRetentionDuration: str = "604800s"
    schemaSettings: SchemaSettings | None = None


class TopicModel(BaseModel):
    """A Pub/Sub topic resource."""

    name: str
    labels: dict[str, str] = Field(default_factory=dict)
    messageRetentionDuration: str = "604800s"  # 7 days
    schemaSettings: SchemaSettings | None = None


class BigQueryConfig(BaseModel):
    """BigQuery export configuration for a subscription."""

    table: str = ""  # "project:dataset.table" or "project.dataset.table"
    useTopicSchema: bool = False  # decode message JSON and map to table columns
    writeMetadata: bool = False  # add subscription_name / message_id / publish_time / attributes
    dropUnknownFields: bool = False  # silently drop columns not in the table schema


class CloudStorageAvroConfig(BaseModel):
    """Avro-specific options for Cloud Storage export."""

    writeMetadata: bool = False  # include subscription / message metadata fields


class CloudStorageConfig(BaseModel):
    """Cloud Storage export configuration for a subscription."""

    bucket: str = ""
    filenamePrefix: str = ""
    filenameSuffix: str = ""
    maxDuration: str = "300s"
    maxBytes: int = 100 * 1024 * 1024
    avroConfig: CloudStorageAvroConfig | None = None
    # if avroConfig is None, messages are written as raw bytes (text format)


class PushConfig(BaseModel):
    """Push delivery configuration for a subscription."""

    pushEndpoint: str = ""
    attributes: dict[str, str] = Field(default_factory=dict)


class DeadLetterPolicy(BaseModel):
    """Dead-letter policy for a subscription."""

    deadLetterTopic: str = ""
    maxDeliveryAttempts: int = 5


class RetryPolicy(BaseModel):
    """Retry policy for a subscription."""

    minimumBackoff: str = "10s"  # e.g. "10s"
    maximumBackoff: str = "600s"  # e.g. "600s"


class SubscriptionModel(BaseModel):
    """A Pub/Sub subscription resource."""

    name: str
    topic: str
    pushConfig: PushConfig = Field(default_factory=PushConfig)
    ackDeadlineSeconds: int = 10
    retainAckedMessages: bool = False
    messageRetentionDuration: str = "604800s"
    labels: dict[str, str] = Field(default_factory=dict)
    enableMessageOrdering: bool = False
    deadLetterPolicy: DeadLetterPolicy | None = None
    retryPolicy: RetryPolicy | None = None
    filter: str = ""
    bigqueryConfig: BigQueryConfig | None = None
    cloudStorageConfig: CloudStorageConfig | None = None


class PubsubMessage(BaseModel):
    """A Pub/Sub message as it appears in pull responses."""

    data: str = ""  # base64-encoded
    attributes: dict[str, str] = Field(default_factory=dict)
    messageId: str = ""
    publishTime: str = ""
    orderingKey: str = ""


class PublishRequest(BaseModel):
    """Request body for publishing messages to a topic."""

    messages: list[dict[str, Any]]


class PublishResponse(BaseModel):
    """Response body for a publish request."""

    messageIds: list[str]


class PullRequest(BaseModel):
    """Request body for pulling messages from a subscription."""

    maxMessages: int = 1
    returnImmediately: bool = False  # deprecated but accepted


class ReceivedMessage(BaseModel):
    """A single message returned by a pull request."""

    ackId: str
    message: PubsubMessage
    deliveryAttempt: int = 1


class PullResponse(BaseModel):
    """Response body for a pull request."""

    receivedMessages: list[ReceivedMessage] = Field(default_factory=list)


class AcknowledgeRequest(BaseModel):
    """Request body for acknowledging messages."""

    ackIds: list[str]


class ModifyAckDeadlineRequest(BaseModel):
    """Request body for modifying ack deadlines."""

    ackIds: list[str]
    ackDeadlineSeconds: int


class TopicListResponse(BaseModel):
    """Response body for listing topics."""

    topics: list[TopicModel] = Field(default_factory=list)
    nextPageToken: str | None = None


class SubscriptionListResponse(BaseModel):
    """Response body for listing subscriptions."""

    subscriptions: list[SubscriptionModel] = Field(default_factory=list)
    nextPageToken: str | None = None


class SnapshotModel(BaseModel):
    """A Pub/Sub snapshot resource."""

    name: str = ""
    topic: str = ""
    expireTime: str = ""
    labels: dict[str, str] = Field(default_factory=dict)
    snapshotTime: str = ""  # internal: oldest unacked publishTime at creation


class SnapshotListResponse(BaseModel):
    """Response body for listing snapshots."""

    snapshots: list[SnapshotModel] = Field(default_factory=list)
    nextPageToken: str | None = None


class CreateSnapshotRequest(BaseModel):
    """Request body for creating a snapshot."""

    subscription: str
    labels: dict[str, str] = Field(default_factory=dict)


class SeekRequest(BaseModel):
    """Request body for seeking a subscription."""

    time: str = ""  # RFC3339; seek to this point in time
    snapshot: str = ""  # full snapshot resource name


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


class SchemaModel(BaseModel):
    """A Pub/Sub schema resource."""

    name: str
    type: str = "TYPE_UNSPECIFIED"  # AVRO or PROTOCOL_BUFFER
    definition: str = ""
    revisionId: str = ""
    revisionCreateTime: str = ""


class SchemaListResponse(BaseModel):
    """Response body for listing schemas."""

    schemas: list[SchemaModel] = Field(default_factory=list)
    nextPageToken: str | None = None


class ValidateSchemaRequest(BaseModel):
    """Request body for validating a schema definition."""

    model_config = ConfigDict(populate_by_name=True)

    schema_: SchemaModel = Field(alias="schema")


class ValidateMessageRequest(BaseModel):
    """Request body for validating a message against a schema."""

    model_config = ConfigDict(populate_by_name=True)

    name: str = ""  # schema resource name (alternative to inline schema)
    schema_: SchemaModel | None = Field(None, alias="schema")
    message: str = ""  # base64-encoded bytes
    encoding: str = "ENCODING_UNSPECIFIED"


def validate_schema_definition(schema_type: str, definition: str) -> str | None:
    """Validate a schema definition string for the given schema type.

    Args:
        schema_type (str): Schema type, e.g. 'AVRO' or 'PROTOCOL_BUFFER'.
        definition (str): Schema definition string to validate.

    Returns:
        str | None: An error message describing the validation failure, or None if valid.
    """
    if schema_type == "AVRO":
        try:
            json.loads(definition)
        except json.JSONDecodeError as e:
            return f"Invalid Avro schema JSON: {e}"
        try:
            import fastavro.schema  # type: ignore[import]

            fastavro.schema.parse_schema(json.loads(definition))
        except ImportError:
            pass
        except Exception as e:
            return f"Invalid Avro schema: {e}"
    # PROTOCOL_BUFFER: accept any definition (no protoc available in emulator)
    return None


def validate_message_against_schema(
    schema_type: str,
    definition: str,
    message_bytes: bytes,
    encoding: str,
) -> str | None:
    """Validate a message against a schema definition.

    Args:
        schema_type (str): Schema type, e.g. 'AVRO' or 'PROTOCOL_BUFFER'.
        definition (str): Schema definition string used for validation.
        message_bytes (bytes): Raw message bytes to validate.
        encoding (str): Message encoding, e.g. 'JSON', 'BINARY', or 'ENCODING_UNSPECIFIED'.

    Returns:
        str | None: An error message describing the validation failure, or None if valid.
    """
    if schema_type == "AVRO":
        if encoding in ("JSON", "ENCODING_UNSPECIFIED"):
            try:
                json.loads(message_bytes)
            except Exception as e:
                return f"Message is not valid JSON: {e}"
            try:
                import io

                import fastavro
                import fastavro.io.parsing  # type: ignore[import]

                parsed = fastavro.schema.parse_schema(json.loads(definition))
                reader = io.BytesIO(message_bytes)
                fastavro.schemaless_reader(reader, parsed)
            except ImportError:
                pass
            except Exception as e:
                return f"Message does not conform to Avro schema: {e}"
        # BINARY: need fastavro to decode; skip if not available
        elif encoding == "BINARY":
            try:
                import io

                import fastavro

                parsed = fastavro.schema.parse_schema(json.loads(definition))
                reader = io.BytesIO(message_bytes)
                fastavro.schemaless_reader(reader, parsed)
            except ImportError:
                pass
            except Exception as e:
                return f"Message does not conform to Avro schema (binary): {e}"
    return None
