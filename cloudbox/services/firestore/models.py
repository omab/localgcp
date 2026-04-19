"""Pydantic models matching the Firestore REST API wire format."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class FirestoreValue(BaseModel):
    """A typed Firestore value. Only one field should be set."""

    nullValue: str | None = None
    booleanValue: bool | None = None
    integerValue: str | None = None  # int64 as string
    doubleValue: float | None = None
    timestampValue: str | None = None  # RFC3339
    stringValue: str | None = None
    bytesValue: str | None = None  # base64
    referenceValue: str | None = None
    geoPointValue: dict | None = None
    arrayValue: dict | None = None  # {"values": [...]}
    mapValue: dict | None = None  # {"fields": {...}}


class Document(BaseModel):
    name: str = ""
    fields: dict[str, Any] = Field(default_factory=dict)
    createTime: str = ""
    updateTime: str = ""


class DocumentMask(BaseModel):
    fieldPaths: list[str] = Field(default_factory=list)


class FieldTransform(BaseModel):
    fieldPath: str
    setToServerValue: str | None = None
    increment: dict | None = None  # FirestoreValue
    appendMissingElements: dict | None = None  # {"values": [...]}
    removeAllFromArray: dict | None = None  # {"values": [...]}


class Write(BaseModel):
    update: Document | None = None
    delete: str | None = None
    currentDocument: dict | None = None
    updateMask: DocumentMask | None = None
    updateTransforms: list[FieldTransform] = Field(default_factory=list)


class CommitRequest(BaseModel):
    writes: list[Write] = Field(default_factory=list)
    transaction: str | None = None


class CommitResponse(BaseModel):
    writeResults: list[dict] = Field(default_factory=list)
    commitTime: str = ""


class BatchWriteRequest(BaseModel):
    writes: list[Write] = Field(default_factory=list)
    labels: dict = Field(default_factory=dict)


class BatchWriteResponse(BaseModel):
    writeResults: list[dict] = Field(default_factory=list)
    status: list[dict] = Field(default_factory=list)


class StructuredQuery(BaseModel):
    select: dict | None = None
    from_: list[dict] | None = Field(default=None, alias="from")
    where: dict | None = None
    orderBy: list[dict] | None = None
    limit: int | None = None
    offset: int | None = None
    startAt: dict | None = None
    endAt: dict | None = None

    model_config = {"populate_by_name": True}


class RunQueryRequest(BaseModel):
    structuredQuery: StructuredQuery | None = None
    transaction: str | None = None
    newTransaction: dict | None = None
    readTime: str | None = None


class AggregationConfig(BaseModel):
    structuredQuery: StructuredQuery | None = None
    aggregations: list[dict] = Field(default_factory=list)


class RunAggregationQueryRequest(BaseModel):
    structuredAggregationQuery: AggregationConfig | None = None
    transaction: str | None = None
    newTransaction: dict | None = None
    readTime: str | None = None


class BatchGetRequest(BaseModel):
    documents: list[str]
    mask: DocumentMask | None = None
    transaction: str | None = None


class ListDocumentsResponse(BaseModel):
    documents: list[Document] = Field(default_factory=list)
    nextPageToken: str | None = None
