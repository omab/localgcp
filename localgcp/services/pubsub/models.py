"""Pydantic models for Pub/Sub REST API."""
from __future__ import annotations

from typing import Any
from pydantic import BaseModel, Field


class TopicModel(BaseModel):
    name: str
    labels: dict[str, str] = Field(default_factory=dict)
    messageRetentionDuration: str = "604800s"  # 7 days


class PushConfig(BaseModel):
    pushEndpoint: str = ""
    attributes: dict[str, str] = Field(default_factory=dict)


class DeadLetterPolicy(BaseModel):
    deadLetterTopic: str = ""
    maxDeliveryAttempts: int = 5


class RetryPolicy(BaseModel):
    minimumBackoff: str = "10s"   # e.g. "10s"
    maximumBackoff: str = "600s"  # e.g. "600s"


class SubscriptionModel(BaseModel):
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


class PubsubMessage(BaseModel):
    data: str = ""          # base64-encoded
    attributes: dict[str, str] = Field(default_factory=dict)
    messageId: str = ""
    publishTime: str = ""
    orderingKey: str = ""


class PublishRequest(BaseModel):
    messages: list[dict[str, Any]]


class PublishResponse(BaseModel):
    messageIds: list[str]


class PullRequest(BaseModel):
    maxMessages: int = 1
    returnImmediately: bool = False  # deprecated but accepted


class ReceivedMessage(BaseModel):
    ackId: str
    message: PubsubMessage
    deliveryAttempt: int = 1


class PullResponse(BaseModel):
    receivedMessages: list[ReceivedMessage] = Field(default_factory=list)


class AcknowledgeRequest(BaseModel):
    ackIds: list[str]


class ModifyAckDeadlineRequest(BaseModel):
    ackIds: list[str]
    ackDeadlineSeconds: int


class TopicListResponse(BaseModel):
    topics: list[TopicModel] = Field(default_factory=list)
    nextPageToken: str | None = None


class SubscriptionListResponse(BaseModel):
    subscriptions: list[SubscriptionModel] = Field(default_factory=list)
    nextPageToken: str | None = None
