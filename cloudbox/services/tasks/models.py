"""Pydantic models for Cloud Tasks REST API."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class RateLimits(BaseModel):
    maxDispatchesPerSecond: float = 500.0
    maxBurstSize: int = 100
    maxConcurrentDispatches: int = 1000


class RetryConfig(BaseModel):
    maxAttempts: int = 100
    maxRetryDuration: str = "0s"
    minBackoff: str = "0.100s"
    maxBackoff: str = "3600s"
    maxDoublings: int = 16


class QueueState:
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    DISABLED = "DISABLED"


class QueueModel(BaseModel):
    name: str
    rateLimits: RateLimits = Field(default_factory=RateLimits)
    retryConfig: RetryConfig = Field(default_factory=RetryConfig)
    state: str = QueueState.RUNNING


class HttpRequest(BaseModel):
    url: str
    httpMethod: str = "POST"
    headers: dict[str, str] = Field(default_factory=dict)
    body: str = ""  # base64-encoded


class TaskAttempt(BaseModel):
    scheduleTime: str = ""
    dispatchTime: str = ""
    responseTime: str = ""
    responseStatus: dict = Field(default_factory=dict)


class TaskModel(BaseModel):
    name: str
    httpRequest: HttpRequest | None = None
    scheduleTime: str = Field(default_factory=_now)
    createTime: str = Field(default_factory=_now)
    dispatchDeadline: str = "1800s"
    lastAttempt: TaskAttempt | None = None
    firstAttempt: TaskAttempt | None = None
    view: str = "BASIC"
    dispatchCount: int = 0
    responseCount: int = 0


class CreateTaskRequest(BaseModel):
    task: dict[str, Any]
    responseView: str = "BASIC"


class ListQueuesResponse(BaseModel):
    queues: list[QueueModel] = Field(default_factory=list)
    nextPageToken: str | None = None


class ListTasksResponse(BaseModel):
    tasks: list[TaskModel] = Field(default_factory=list)
    nextPageToken: str | None = None
