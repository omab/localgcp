"""Pydantic models matching GCS JSON API shapes."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field


def _now_rfc3339() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


# ---------------------------------------------------------------------------
# Lifecycle models
# ---------------------------------------------------------------------------


class LifecycleAction(BaseModel):
    type: str  # "Delete" or "SetStorageClass"
    storageClass: str = ""  # used when type == "SetStorageClass"


class LifecycleCondition(BaseModel):
    age: int | None = None  # days since object creation
    createdBefore: str = ""  # RFC3339 date; object must have been created before this
    matchesStorageClass: list[str] = Field(default_factory=list)
    numNewerVersions: int | None = None  # ignored (non-versioned emulator)


class LifecycleRule(BaseModel):
    action: LifecycleAction
    condition: LifecycleCondition = Field(default_factory=LifecycleCondition)


class Lifecycle(BaseModel):
    rule: list[LifecycleRule] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Bucket / Object models
# ---------------------------------------------------------------------------


class BucketModel(BaseModel):
    kind: str = "storage#bucket"
    id: str = ""
    name: str
    projectNumber: str = "000000000000"
    metageneration: str = "1"
    location: str = "US"
    locationType: str = "multi-region"
    storageClass: str = "STANDARD"
    selfLink: str = ""
    timeCreated: str = Field(default_factory=_now_rfc3339)
    updated: str = Field(default_factory=_now_rfc3339)
    etag: str = "CAE="
    labels: dict[str, str] = Field(default_factory=dict)
    lifecycle: Lifecycle | None = None

    def model_post_init(self, __context: Any) -> None:
        if not self.id:
            self.id = self.name
        if not self.selfLink:
            self.selfLink = f"https://www.googleapis.com/storage/v1/b/{self.name}"


class ObjectModel(BaseModel):
    kind: str = "storage#object"
    id: str = ""
    name: str
    bucket: str
    generation: str = "1"
    metageneration: str = "1"
    contentType: str = "application/octet-stream"
    storageClass: str = "STANDARD"
    size: str = "0"
    md5Hash: str = ""
    etag: str = ""
    crc32c: str = ""
    selfLink: str = ""
    mediaLink: str = ""
    timeCreated: str = Field(default_factory=_now_rfc3339)
    updated: str = Field(default_factory=_now_rfc3339)
    metadata: dict[str, str] = Field(default_factory=dict)

    def model_post_init(self, __context: Any) -> None:
        if not self.id:
            self.id = f"{self.bucket}/{self.name}/1"
        if not self.selfLink:
            self.selfLink = (
                f"https://www.googleapis.com/storage/v1/b/{self.bucket}/o/"
                + self.name.replace("/", "%2F")
            )
        if not self.mediaLink:
            self.mediaLink = (
                f"https://storage.googleapis.com/download/storage/v1/b/{self.bucket}/o/"
                + self.name.replace("/", "%2F")
                + "?alt=media"
            )


class BucketListResponse(BaseModel):
    kind: str = "storage#buckets"
    items: list[BucketModel] = Field(default_factory=list)
    nextPageToken: str | None = None


class ObjectListResponse(BaseModel):
    kind: str = "storage#objects"
    items: list[ObjectModel] = Field(default_factory=list)
    nextPageToken: str | None = None
    prefixes: list[str] = Field(default_factory=list)


# GCS notification config uses snake_case field names (unlike the rest of the GCS API).
_ALL_EVENT_TYPES = {
    "OBJECT_FINALIZE",
    "OBJECT_DELETE",
    "OBJECT_METADATA_UPDATE",
    "OBJECT_ARCHIVE",
}


class NotificationConfig(BaseModel):
    kind: str = "storage#notification"
    id: str = ""
    selfLink: str = ""
    topic: str  # "//pubsub.googleapis.com/projects/PROJECT/topics/TOPIC"
    event_types: list[str] = Field(default_factory=list)  # empty = all event types
    object_name_prefix: str = ""
    payload_format: str = "JSON_API_V1"  # "JSON_API_V1" or "NONE"
    custom_attributes: dict[str, str] = Field(default_factory=dict)
    etag: str = "CAE="

    def model_post_init(self, __context: Any) -> None:
        if not self.selfLink and self.id:
            # Derive selfLink from topic → bucket is embedded by the caller
            pass

    def matches(self, event_type: str, object_name: str) -> bool:
        """Return True if this config should fire for the given event/object."""
        types = self.event_types or list(_ALL_EVENT_TYPES)
        if event_type not in types:
            return False
        if self.object_name_prefix and not object_name.startswith(self.object_name_prefix):
            return False
        return True

    def pubsub_topic_name(self) -> str:
        """Convert the full resource name to a bare projects/.../topics/... path."""
        topic = self.topic
        # Strip "//pubsub.googleapis.com/" prefix if present
        if topic.startswith("//pubsub.googleapis.com/"):
            topic = topic[len("//pubsub.googleapis.com/"):]
        return topic


class NotificationListResponse(BaseModel):
    kind: str = "storage#notifications"
    items: list[NotificationConfig] = Field(default_factory=list)
