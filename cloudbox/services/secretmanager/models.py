"""Pydantic models for Secret Manager REST API."""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, Field


def _now() -> str:
    """Return the current UTC timestamp in ISO 8601 format with millisecond precision.

    Returns:
        str: Current UTC time formatted as 'YYYY-MM-DDTHH:MM:SS.mmmZ'.
    """
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class ReplicationAuto(BaseModel):
    """Automatic replication policy (no configuration required)."""


class Replication(BaseModel):
    """Replication configuration for a secret."""

    automatic: ReplicationAuto = Field(default_factory=ReplicationAuto)


class SecretModel(BaseModel):
    """A Secret Manager secret resource."""

    name: str
    replication: Replication = Field(default_factory=Replication)
    createTime: str = Field(default_factory=_now)
    labels: dict[str, str] = Field(default_factory=dict)
    etag: str = "1"
    topics: list[dict] = Field(default_factory=list)
    kmsKeyName: str = ""


class SecretVersionState:
    """Enumeration of secret version states."""

    ENABLED = "ENABLED"
    DISABLED = "DISABLED"
    DESTROYED = "DESTROYED"


class CustomerManagedEncryption(BaseModel):
    """Customer-managed encryption key reference."""

    kmsKeyVersionName: str = ""


class ReplicationStatus(BaseModel):
    """Replication status for a secret version."""

    automatic: dict = Field(default_factory=dict)


class SecretVersionModel(BaseModel):
    """A single version of a Secret Manager secret."""

    name: str
    createTime: str = Field(default_factory=_now)
    destroyTime: str | None = None
    state: str = SecretVersionState.ENABLED
    replicationStatus: ReplicationStatus = Field(default_factory=ReplicationStatus)
    etag: str = "1"


class AddVersionRequest(BaseModel):
    """Request body for adding a new secret version."""

    payload: dict  # {"data": "<base64>"}


class AccessSecretVersionResponse(BaseModel):
    """Response body for accessing a secret version payload."""

    name: str
    payload: dict  # {"data": "<base64>", "dataCrc32c": "..."}


class ListSecretsResponse(BaseModel):
    """Response body for listing secrets."""

    secrets: list[SecretModel] = Field(default_factory=list)
    nextPageToken: str | None = None
    totalSize: int = 0


class ListSecretVersionsResponse(BaseModel):
    """Response body for listing secret versions."""

    versions: list[SecretVersionModel] = Field(default_factory=list)
    nextPageToken: str | None = None
    totalSize: int = 0
