"""Pydantic models for Secret Manager REST API."""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, Field


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class ReplicationAuto(BaseModel):
    pass


class Replication(BaseModel):
    automatic: ReplicationAuto = Field(default_factory=ReplicationAuto)


class SecretModel(BaseModel):
    name: str
    replication: Replication = Field(default_factory=Replication)
    createTime: str = Field(default_factory=_now)
    labels: dict[str, str] = Field(default_factory=dict)
    etag: str = "1"


class SecretVersionState:
    ENABLED = "ENABLED"
    DISABLED = "DISABLED"
    DESTROYED = "DESTROYED"


class CustomerManagedEncryption(BaseModel):
    kmsKeyVersionName: str = ""


class ReplicationStatus(BaseModel):
    automatic: dict = Field(default_factory=dict)


class SecretVersionModel(BaseModel):
    name: str
    createTime: str = Field(default_factory=_now)
    destroyTime: str | None = None
    state: str = SecretVersionState.ENABLED
    replicationStatus: ReplicationStatus = Field(default_factory=ReplicationStatus)
    etag: str = "1"


class AddVersionRequest(BaseModel):
    payload: dict  # {"data": "<base64>"}


class AccessSecretVersionResponse(BaseModel):
    name: str
    payload: dict  # {"data": "<base64>", "dataCrc32c": "..."}


class ListSecretsResponse(BaseModel):
    secrets: list[SecretModel] = Field(default_factory=list)
    nextPageToken: str | None = None
    totalSize: int = 0


class ListSecretVersionsResponse(BaseModel):
    versions: list[SecretVersionModel] = Field(default_factory=list)
    nextPageToken: str | None = None
    totalSize: int = 0
