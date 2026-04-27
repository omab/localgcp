"""Pydantic models for Cloud Tasks REST API."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field


def _now() -> str:
    """Return the current UTC timestamp in ISO 8601 format with millisecond precision.

    Returns:
        str: Current UTC time formatted as 'YYYY-MM-DDTHH:MM:SS.mmmZ'.
    """
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class RateLimits(BaseModel):
    """Rate limiting configuration for a Cloud Tasks queue."""

    maxDispatchesPerSecond: float = 500.0
    maxBurstSize: int = 100
    maxConcurrentDispatches: int = 1000


class RetryConfig(BaseModel):
    """Retry configuration for a Cloud Tasks queue."""

    maxAttempts: int = 100
    maxRetryDuration: str = "0s"
    minBackoff: str = "0.100s"
    maxBackoff: str = "3600s"
    maxDoublings: int = 16


class QueueState:
    """Enumeration of Cloud Tasks queue states."""

    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    DISABLED = "DISABLED"


class QueueModel(BaseModel):
    """A Cloud Tasks queue resource."""

    name: str
    rateLimits: RateLimits = Field(default_factory=RateLimits)
    retryConfig: RetryConfig = Field(default_factory=RetryConfig)
    state: str = QueueState.RUNNING


class HttpRequest(BaseModel):
    """HTTP request configuration for a Cloud Tasks task."""

    url: str
    httpMethod: str = "POST"
    headers: dict[str, str] = Field(default_factory=dict)
    body: str = ""  # base64-encoded


class PubsubTarget(BaseModel):
    """Pub/Sub target configuration for a Cloud Tasks task."""

    topicName: str
    data: str = ""  # base64-encoded message payload
    attributes: dict[str, str] = Field(default_factory=dict)


class TaskAttempt(BaseModel):
    """Details of a single task dispatch attempt."""

    scheduleTime: str = ""
    dispatchTime: str = ""
    responseTime: str = ""
    responseStatus: dict = Field(default_factory=dict)


class TaskModel(BaseModel):
    """A Cloud Tasks task resource."""

    name: str
    httpRequest: HttpRequest | None = None
    pubsubTarget: PubsubTarget | None = None
    scheduleTime: str = Field(default_factory=_now)
    createTime: str = Field(default_factory=_now)
    dispatchDeadline: str = "1800s"
    lastAttempt: TaskAttempt | None = None
    firstAttempt: TaskAttempt | None = None
    view: str = "BASIC"
    dispatchCount: int = 0
    responseCount: int = 0


class CreateTaskRequest(BaseModel):
    """Request body for creating a new task."""

    task: dict[str, Any]
    responseView: str = "BASIC"


class ListQueuesResponse(BaseModel):
    """Response body for listing queues."""

    queues: list[QueueModel] = Field(default_factory=list)
    nextPageToken: str | None = None


class ListTasksResponse(BaseModel):
    """Response body for listing tasks in a queue."""

    tasks: list[TaskModel] = Field(default_factory=list)
    nextPageToken: str | None = None
