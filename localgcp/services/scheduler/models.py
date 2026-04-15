"""Pydantic models for Cloud Scheduler REST API v1."""
from __future__ import annotations

from typing import Any
from pydantic import BaseModel, Field


class HttpTarget(BaseModel):
    uri: str
    httpMethod: str = "POST"
    headers: dict[str, str] = Field(default_factory=dict)
    body: str = ""  # base64-encoded


class RetryConfig(BaseModel):
    retryCount: int = 0
    maxRetryDuration: str = "0s"
    minBackoffDuration: str = "5s"
    maxBackoffDuration: str = "1h"
    maxDoublings: int = 5


class JobModel(BaseModel):
    name: str = ""
    description: str = ""
    schedule: str = ""
    timeZone: str = "UTC"
    state: str = "ENABLED"          # ENABLED, PAUSED, DISABLED
    httpTarget: HttpTarget | None = None
    retryConfig: RetryConfig = Field(default_factory=RetryConfig)
    userUpdateTime: str = ""
    scheduleTime: str = ""          # next scheduled run
    lastAttemptTime: str = ""
    status: dict[str, Any] = Field(default_factory=dict)


class JobListResponse(BaseModel):
    jobs: list[JobModel] = Field(default_factory=list)
    nextPageToken: str | None = None
