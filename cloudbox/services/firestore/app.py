"""Cloud Firestore emulator.

Implements the Firestore REST API v1 used by google-cloud-firestore.

Path structure after /v1/projects/{P}/databases/{D}/documents:
  - Odd number of trailing segments  → collection (list)
  - Even number of trailing segments → document (get/patch/delete)

Examples:
  .../documents/users          → collection "users"    (list)
  .../documents/users/alice    → document              (get/update/delete)
  .../documents/users/alice/posts → sub-collection      (list)
  .../documents/users/alice/posts/p1 → sub-document     (get/update/delete)
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from fastapi import FastAPI, Query, Request, Response
from fastapi.responses import JSONResponse

from cloudbox.core.errors import GCPError, add_gcp_exception_handler
from cloudbox.core.middleware import add_request_logging
from cloudbox.core.store import NamespacedStore
from cloudbox.services.firestore.models import (
    BatchGetRequest,
    BatchWriteRequest,
    BatchWriteResponse,
    CommitRequest,
    CommitResponse,
    Document,
    FieldTransform,
    ListDocumentsResponse,
    RunAggregationQueryRequest,
    RunQueryRequest,
    Write,
)
from cloudbox.services.firestore.query import _get_field as _query_get_field
from cloudbox.services.firestore.query import run_query
from cloudbox.services.firestore.store import get_store

app = FastAPI(title="Cloudbox — Cloud Firestore", version="v1")
add_gcp_exception_handler(app)
add_request_logging(app, "firestore")


def _now() -> str:
    """Return the current UTC time as a Firestore-compatible RFC3339 millisecond string.

    Returns:
        str: Current UTC timestamp, e.g. "2024-01-15T12:00:00.123Z".
    """
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _store() -> NamespacedStore:
    """Return the shared Firestore NamespacedStore instance.

    Returns:
        NamespacedStore: The store used by all route handlers.
    """
    return get_store()


def _parse_path(full_path: str) -> tuple[str, str, str, list[str]]:
    """Parse a Firestore document/collection path.

    Args:
        full_path (str): Everything after "/v1/", e.g.
            "projects/P/databases/D/documents/col/doc".

    Returns:
        tuple[str, str, str, list[str]]: A 4-tuple of
            (project, database, doc_root, trailing_segments) where doc_root is
            "projects/P/databases/D/documents" and trailing_segments are the
            path components after "documents/".
    """
    # full_path = "projects/P/databases/D/documents/col/doc/..."
    parts = full_path.split("/")
    # parts: [projects, P, databases, D, documents, col, doc, ...]
    project = parts[1] if len(parts) > 1 else ""
    database = parts[3] if len(parts) > 3 else ""
    doc_root = "/".join(parts[:5])  # projects/P/databases/D/documents
    trailing = parts[5:]  # [col, doc, ...]
    return project, database, doc_root, trailing


# ---------------------------------------------------------------------------
# Action endpoints (must come before catch-all GET/POST/PATCH/DELETE)
# ---------------------------------------------------------------------------


@app.post("/v1/projects/{project}/databases/{database}/documents:runQuery")
async def run_query_root(project: str, database: str, body: RunQueryRequest):
    """Run a structured query against the root documents collection.

    Args:
        project (str): GCP project ID.
        database (str): Firestore database ID.
        body (RunQueryRequest): The structured query request body.

    Returns:
        JSONResponse: A list of {"document": ..., "readTime": ...} result objects.
    """
    parent = f"projects/{project}/databases/{database}/documents"
    return await _run_query_impl(parent, body)


@app.post("/v1/{parent_path:path}/documents:runQuery")
async def run_query_nested(parent_path: str, body: RunQueryRequest):
    """Run a structured query against a nested collection parent.

    Args:
        parent_path (str): Path up to but not including the trailing "/documents",
            e.g. "projects/P/databases/D/documents/col/doc".
        body (RunQueryRequest): The structured query request body.

    Returns:
        JSONResponse: A list of {"document": ..., "readTime": ...} result objects.
    """
    # parent_path = "projects/P/databases/D/documents/col/doc"
    parent = f"{parent_path}/documents"
    return await _run_query_impl(parent, body)


async def _run_query_impl(parent: str, body: RunQueryRequest) -> JSONResponse:
    """Shared implementation for runQuery endpoints.

    Args:
        parent (str): Full Firestore parent path ending with "/documents".
        body (RunQueryRequest): The structured query request body.

    Returns:
        JSONResponse: A list of {"document": ..., "readTime": ...} result objects.
    """
    store = _store()
    if body.structuredQuery is None:
        return JSONResponse(content=[])

    sq = body.structuredQuery
    from_clauses = sq.from_ or []

    results = []
    for from_clause in from_clauses:
        collection_id = from_clause.get("collectionId", "")
        all_descendants = from_clause.get("allDescendants", False)

        if all_descendants:
            prefix = f"{parent}/"
            candidates = [v for v in store.list("documents") if v["name"].startswith(prefix)]
            if collection_id:
                candidates = [d for d in candidates if _in_collection(d["name"], collection_id)]
        else:
            prefix = f"{parent}/{collection_id}/"
            candidates = [
                v
                for v in store.list("documents")
                if v["name"].startswith(prefix) and "/" not in v["name"][len(prefix) :]
            ]

        sq_dict = sq.model_dump(by_alias=True, exclude_none=True)
        filtered = run_query(candidates, sq_dict)
        results.extend(filtered)

    now = _now()
    return JSONResponse(content=[{"document": doc, "readTime": now} for doc in results])


@app.post("/v1/projects/{project}/databases/{database}/documents:runAggregationQuery")
async def run_aggregation_query_root(project: str, database: str, body: RunAggregationQueryRequest):
    """Run an aggregation query (e.g. COUNT) against the root documents collection.

    Args:
        project (str): GCP project ID.
        database (str): Firestore database ID.
        body (RunAggregationQueryRequest): The aggregation query request body.

    Returns:
        JSONResponse: A list of {"result": {"aggregateFields": ...}, "readTime": ...} objects.
    """
    parent = f"projects/{project}/databases/{database}/documents"
    return await _run_aggregation_query_impl(parent, body)


@app.post("/v1/{parent_path:path}/documents:runAggregationQuery")
async def run_aggregation_query_nested(parent_path: str, body: RunAggregationQueryRequest):
    """Run an aggregation query against a nested collection parent.

    Args:
        parent_path (str): Path up to but not including the trailing "/documents".
        body (RunAggregationQueryRequest): The aggregation query request body.

    Returns:
        JSONResponse: A list of {"result": {"aggregateFields": ...}, "readTime": ...} objects.
    """
    parent = f"{parent_path}/documents"
    return await _run_aggregation_query_impl(parent, body)


async def _run_aggregation_query_impl(parent: str, body: RunAggregationQueryRequest) -> JSONResponse:
    """Shared implementation for runAggregationQuery endpoints.

    Args:
        parent (str): Full Firestore parent path ending with "/documents".
        body (RunAggregationQueryRequest): The aggregation query request body.

    Returns:
        JSONResponse: A list of {"result": {"aggregateFields": ...}, "readTime": ...} objects.
    """
    store = _store()
    saq = body.structuredAggregationQuery
    if saq is None:
        return JSONResponse(content=[])

    sq = saq.structuredQuery
    aggregations = saq.aggregations

    # Collect candidates (same logic as runQuery)
    from_clauses = (sq.from_ or []) if sq else []
    if not from_clauses:
        candidates = store.list("documents")
    else:
        candidates = []
        for from_clause in from_clauses:
            collection_id = from_clause.get("collectionId", "")
            all_descendants = from_clause.get("allDescendants", False)
            if all_descendants:
                prefix = f"{parent}/"
                coll = [v for v in store.list("documents") if v["name"].startswith(prefix)]
                if collection_id:
                    coll = [d for d in coll if _in_collection(d["name"], collection_id)]
            else:
                prefix = f"{parent}/{collection_id}/"
                coll = [
                    v
                    for v in store.list("documents")
                    if v["name"].startswith(prefix) and "/" not in v["name"][len(prefix) :]
                ]
            candidates.extend(coll)

    # Apply WHERE / ORDER BY / OFFSET / LIMIT from the nested structuredQuery
    if sq is not None:
        sq_dict = sq.model_dump(by_alias=True, exclude_none=True)
        documents = run_query(candidates, sq_dict)
    else:
        documents = candidates

    # Compute each aggregation
    now = _now()
    aggregate_fields: dict = {}

    for agg in aggregations:
        alias = agg.get("alias", "")

        if "count" in agg:
            up_to = agg["count"].get("upTo")
            count = len(documents)
            if up_to is not None:
                count = min(count, int(up_to))
            aggregate_fields[alias] = {"integerValue": str(count)}

        elif "sum" in agg:
            field_path = agg["sum"]["field"]["fieldPath"]
            total: int | float = 0
            has_double = False
            for doc in documents:
                val = _query_get_field(doc.get("fields", {}), field_path)
                if isinstance(val, bool) or not isinstance(val, (int, float)):
                    continue
                if isinstance(val, float):
                    has_double = True
                total += val
            if has_double:
                aggregate_fields[alias] = {"doubleValue": float(total)}
            else:
                aggregate_fields[alias] = {"integerValue": str(int(total))}

        elif "avg" in agg:
            field_path = agg["avg"]["field"]["fieldPath"]
            values = [
                float(_query_get_field(doc.get("fields", {}), field_path))
                for doc in documents
                if isinstance(_query_get_field(doc.get("fields", {}), field_path), (int, float))
                and not isinstance(_query_get_field(doc.get("fields", {}), field_path), bool)
            ]
            if values:
                aggregate_fields[alias] = {"doubleValue": sum(values) / len(values)}
            else:
                aggregate_fields[alias] = {"nullValue": "NULL_VALUE"}

    return JSONResponse(
        content=[
            {
                "result": {"aggregateFields": aggregate_fields},
                "readTime": now,
            }
        ]
    )


@app.post("/v1/projects/{project}/databases/{database}/documents:batchGet")
async def batch_get(project: str, database: str, body: BatchGetRequest):
    """Batch-fetch multiple documents by full resource name.

    Args:
        project (str): GCP project ID.
        database (str): Firestore database ID.
        body (BatchGetRequest): Request body containing a list of document names.

    Returns:
        JSONResponse: A list of {"found": ..., "readTime": ...} or
            {"missing": ..., "readTime": ...} result objects.
    """
    store = _store()
    now = _now()
    results = []
    for doc_name in body.documents:
        doc = store.get("documents", doc_name)
        if doc:
            results.append({"found": doc, "readTime": now})
        else:
            results.append({"missing": doc_name, "readTime": now})
    return JSONResponse(content=results)


@app.post("/v1/projects/{project}/databases/{database}:beginTransaction")
async def begin_transaction(project: str, database: str):
    """Begin a Firestore transaction and return a transaction ID.

    Args:
        project (str): GCP project ID.
        database (str): Firestore database ID.

    Returns:
        dict: A {"transaction": "<base64-encoded-id>"} response.
    """
    import base64

    txn_id = uuid.uuid4().bytes
    return {"transaction": base64.b64encode(txn_id).decode()}


def _get_field(fields: dict, parts: list[str]) -> dict | None:
    """Return the FirestoreValue at a dotted field path, or None if missing.

    Args:
        fields (dict): The "fields" dict of a Firestore document.
        parts (list[str]): Already-split path segments, e.g. ["address", "city"].

    Returns:
        dict | None: The raw Firestore typed value dict, or None if the path
            does not exist.
    """
    current = fields
    for i, part in enumerate(parts):
        if i == len(parts) - 1:
            return current.get(part)
        node = current.get(part)
        if node is None or "mapValue" not in node:
            return None
        current = node["mapValue"].get("fields", {})
    return None


def _set_field(fields: dict, parts: list[str], value: dict) -> None:
    """Set a FirestoreValue at a dotted field path, creating map nodes as needed.

    Args:
        fields (dict): The "fields" dict of a Firestore document (mutated in place).
        parts (list[str]): Already-split path segments, e.g. ["address", "city"].
        value (dict): The raw Firestore typed value dict to set at the path.
    """
    current = fields
    for i, part in enumerate(parts):
        if i == len(parts) - 1:
            current[part] = value
            return
        if part not in current or "mapValue" not in current[part]:
            current[part] = {"mapValue": {"fields": {}}}
        current = current[part]["mapValue"]["fields"]


def _apply_transforms(fields: dict, transforms: list[FieldTransform], now: str) -> dict:
    """Apply field transforms to a document fields dict and return a new fields dict.

    Args:
        fields (dict): The current "fields" dict of a Firestore document.
        transforms (list[FieldTransform]): The list of field transforms to apply.
        now (str): Current UTC timestamp string used for REQUEST_TIME transforms.

    Returns:
        dict: A new fields dict with all transforms applied.
    """
    fields = dict(fields)
    for t in transforms:
        parts = t.fieldPath.split(".")

        if t.setToServerValue == "REQUEST_TIME":
            _set_field(fields, parts, {"timestampValue": now})

        elif t.increment is not None:
            existing = _get_field(fields, parts)
            if "integerValue" in t.increment:
                delta = int(t.increment["integerValue"])
                if existing and "doubleValue" in existing:
                    _set_field(fields, parts, {"doubleValue": existing["doubleValue"] + delta})
                else:
                    base = (
                        int(existing["integerValue"])
                        if existing and "integerValue" in existing
                        else 0
                    )
                    _set_field(fields, parts, {"integerValue": str(base + delta)})
            elif "doubleValue" in t.increment:
                delta = float(t.increment["doubleValue"])
                if existing and "integerValue" in existing:
                    _set_field(
                        fields, parts, {"doubleValue": int(existing["integerValue"]) + delta}
                    )
                elif existing and "doubleValue" in existing:
                    _set_field(fields, parts, {"doubleValue": existing["doubleValue"] + delta})
                else:
                    _set_field(fields, parts, {"doubleValue": delta})

        elif t.appendMissingElements is not None:
            existing = _get_field(fields, parts)
            existing_vals = existing.get("arrayValue", {}).get("values", []) if existing else []
            result = list(existing_vals)
            for v in t.appendMissingElements.get("values", []):
                if v not in result:
                    result.append(v)
            _set_field(fields, parts, {"arrayValue": {"values": result}})

        elif t.removeAllFromArray is not None:
            existing = _get_field(fields, parts)
            existing_vals = existing.get("arrayValue", {}).get("values", []) if existing else []
            to_remove = t.removeAllFromArray.get("values", [])
            result = [v for v in existing_vals if v not in to_remove]
            _set_field(fields, parts, {"arrayValue": {"values": result}})

    return fields


def _apply_write(store: NamespacedStore, write: Write, now: str) -> dict:
    """Apply a single Write to the store and return a write-result dict.

    Args:
        store (NamespacedStore): The Firestore document store.
        write (Write): The write operation to apply (update or delete).
        now (str): Current UTC timestamp string used for createTime/updateTime fields.

    Returns:
        dict: A write-result dict, e.g. {"updateTime": "..."} for updates or {}
            for deletes.

    Raises:
        GCPError: If a currentDocument precondition fails (404 for missing
            document, 412 for exists or updateTime mismatch).
    """
    # currentDocument precondition
    if write.currentDocument:
        cd = write.currentDocument
        doc_name = (write.update.name if write.update else write.delete) or ""
        existing = store.get("documents", doc_name) if doc_name else None
        if "exists" in cd:
            if cd["exists"] and existing is None:
                raise GCPError(404, f"Document not found: {doc_name}")
            if not cd["exists"] and existing is not None:
                raise GCPError(412, f"Document already exists: {doc_name}")
        if "updateTime" in cd and (
            existing is None or existing.get("updateTime") != cd["updateTime"]
        ):
            raise GCPError(412, "Precondition Failed: updateTime mismatch")

    if write.update:
        doc = write.update
        name = doc.name
        existing = store.get("documents", name)
        if existing:
            fields = dict(existing.get("fields", {}))
            if write.updateMask:
                for fp in write.updateMask.fieldPaths:
                    if fp in doc.fields:
                        fields[fp] = doc.fields[fp]
                    else:
                        fields.pop(fp, None)
            else:
                fields = dict(doc.fields)
            updated = {**existing, "fields": fields, "updateTime": now}
        else:
            updated = {
                "name": name,
                "fields": dict(doc.fields),
                "createTime": now,
                "updateTime": now,
            }
        if write.updateTransforms:
            updated["fields"] = _apply_transforms(updated["fields"], write.updateTransforms, now)
        store.set("documents", name, updated)
        return {"updateTime": now}

    if write.delete:
        store.delete("documents", write.delete)
        return {}

    return {}


@app.post("/v1/projects/{project}/databases/{database}:commit")
async def commit(project: str, database: str, body: CommitRequest):
    """Commit a batch of writes atomically.

    Args:
        project (str): GCP project ID.
        database (str): Firestore database ID.
        body (CommitRequest): Request body containing the list of writes to commit.

    Returns:
        dict: A CommitResponse dict with writeResults and commitTime.
    """
    store = _store()
    now = _now()
    write_results = [_apply_write(store, w, now) for w in body.writes]
    return CommitResponse(writeResults=write_results, commitTime=now).model_dump()


@app.post("/v1/projects/{project}/databases/{database}:batchWrite")
async def batch_write(project: str, database: str, body: BatchWriteRequest):
    """Apply writes independently, each succeeding or failing on its own.

    Args:
        project (str): GCP project ID.
        database (str): Firestore database ID.
        body (BatchWriteRequest): Request body containing the list of writes.

    Returns:
        dict: A BatchWriteResponse dict with writeResults and per-write status codes.
    """
    store = _store()
    now = _now()
    write_results: list[dict] = []
    statuses: list[dict] = []

    for write in body.writes:
        try:
            result = _apply_write(store, write, now)
            write_results.append(result)
            statuses.append({"code": 0})
        except GCPError as exc:
            write_results.append({})
            statuses.append({"code": exc.status_code, "message": exc.message})
        except Exception as exc:
            write_results.append({})
            statuses.append({"code": 13, "message": str(exc)})

    return BatchWriteResponse(writeResults=write_results, status=statuses).model_dump()


@app.post("/v1/projects/{project}/databases/{database}:rollback")
async def rollback(project: str, database: str):
    """Roll back a transaction (no-op in emulator, transactions are not tracked).

    Args:
        project (str): GCP project ID.
        database (str): Firestore database ID.

    Returns:
        dict: An empty dict (rollback is always successful in the emulator).
    """
    return {}


# ---------------------------------------------------------------------------
# Document/collection catch-all: GET, POST, PATCH, DELETE
# ---------------------------------------------------------------------------


@app.post("/v1/projects/{project}/databases/{database}/documents/{doc_path:path}")
async def create_or_action(
    project: str,
    database: str,
    doc_path: str,
    request: Request,
    documentId: str = Query(default=""),
):
    """Handle POST to create a document in a collection or dispatch a collection action.

    When doc_path has an odd number of segments it is treated as a collection path
    and a new document is created inside it. Action suffixes such as ":runQuery" on
    a collection path are also dispatched here.

    Args:
        project (str): GCP project ID.
        database (str): Firestore database ID.
        doc_path (str): Path after "documents/", may end with an action suffix.
        request (Request): The raw FastAPI request (body is read as JSON).
        documentId (str): Optional explicit document ID; a UUID is used if omitted.

    Returns:
        dict: The newly created document resource dict.

    Raises:
        GCPError: 400 if the path has an even number of segments (not a collection),
            or 501 if an unsupported action suffix is used.
    """
    # Handle action suffixes on collections (e.g. "scores:runQuery")
    _ACTIONS = (":runQuery", ":batchGet", ":beginTransaction", ":commit", ":rollback")
    for action in _ACTIONS:
        if doc_path.endswith(action):
            collection_path = doc_path[: -len(action)]
            parent = f"projects/{project}/databases/{database}/documents/{collection_path}"
            body_data = await request.json()
            if action == ":runQuery":
                req = RunQueryRequest(**body_data)
                return await _run_query_impl(parent, req)
            raise GCPError(501, f"Action {action} not supported at collection level")

    parts = doc_path.split("/")
    doc_root = f"projects/{project}/databases/{database}/documents"

    if len(parts) % 2 == 1:
        # Odd → collection path, create document
        parent = f"{doc_root}/{'/'.join(parts[:-1])}" if len(parts) > 1 else doc_root
        collection_id = parts[-1]
        body = await request.json()
        doc_id = documentId or str(uuid.uuid4())
        name = f"{parent}/{collection_id}/{doc_id}"
        now = _now()
        doc = {
            "name": name,
            "fields": body.get("fields", {}),
            "createTime": now,
            "updateTime": now,
        }
        _store().set("documents", name, doc)
        return doc
    else:
        raise GCPError(400, f"Invalid path for POST: {doc_path}")


@app.get("/v1/projects/{project}/databases/{database}/documents/{doc_path:path}")
async def get_or_list(
    project: str,
    database: str,
    doc_path: str,
    pageSize: int = Query(default=300),
    pageToken: str = Query(default=""),
):
    """Handle GET: list a collection (odd segments) or fetch a document (even segments).

    Args:
        project (str): GCP project ID.
        database (str): Firestore database ID.
        doc_path (str): Path after "documents/".
        pageSize (int): Maximum number of documents to return per page (default 300).
        pageToken (str): Opaque page token returned by a previous list call.

    Returns:
        dict | dict: A ListDocumentsResponse dict for collections, or the raw
            document dict for a single document fetch.

    Raises:
        GCPError: 404 if a document path resolves to a non-existent document.
    """
    parts = doc_path.split("/")
    doc_root = f"projects/{project}/databases/{database}/documents"
    store = _store()

    if len(parts) % 2 == 1:
        # Odd → collection
        parent = f"{doc_root}/{'/'.join(parts[:-1])}" if len(parts) > 1 else doc_root
        collection_id = parts[-1]
        prefix = f"{parent}/{collection_id}/"
        all_docs = [
            v
            for v in store.list("documents")
            if v["name"].startswith(prefix) and "/" not in v["name"][len(prefix) :]
        ]
        all_docs.sort(key=lambda d: d["name"])
        offset = int(pageToken) if pageToken else 0
        page = all_docs[offset : offset + pageSize]
        next_token = str(offset + pageSize) if offset + pageSize < len(all_docs) else None
        return ListDocumentsResponse(
            documents=[Document(**d) for d in page],
            nextPageToken=next_token,
        ).model_dump(exclude_none=True)
    else:
        # Even → document
        name = f"{doc_root}/{doc_path}"
        doc = store.get("documents", name)
        if doc is None:
            raise GCPError(404, f"Document not found: {name}")
        return doc


@app.patch("/v1/projects/{project}/databases/{database}/documents/{doc_path:path}")
async def update_document(project: str, database: str, doc_path: str, request: Request):
    """Update or create a document at an explicit path (upsert semantics).

    Args:
        project (str): GCP project ID.
        database (str): Firestore database ID.
        doc_path (str): Path after "documents/" identifying the document.
        request (Request): The raw FastAPI request; body is read as JSON.

    Returns:
        dict: The updated or newly created document resource dict.
    """
    doc_root = f"projects/{project}/databases/{database}/documents"
    name = f"{doc_root}/{doc_path}"
    body = await request.json()
    store = _store()
    existing = store.get("documents", name)
    now = _now()

    if existing:
        fields = {**existing.get("fields", {}), **body.get("fields", {})}
        doc = {**existing, "fields": fields, "updateTime": now}
    else:
        doc = {
            "name": name,
            "fields": body.get("fields", {}),
            "createTime": now,
            "updateTime": now,
        }
    store.set("documents", name, doc)
    return doc


@app.delete(
    "/v1/projects/{project}/databases/{database}/documents/{doc_path:path}", status_code=204
)
async def delete_document(project: str, database: str, doc_path: str):
    """Delete a Firestore document by path.

    Args:
        project (str): GCP project ID.
        database (str): Firestore database ID.
        doc_path (str): Path after "documents/" identifying the document to delete.

    Returns:
        Response: HTTP 204 No Content on success.

    Raises:
        GCPError: 404 if the document does not exist.
    """
    doc_root = f"projects/{project}/databases/{database}/documents"
    name = f"{doc_root}/{doc_path}"
    store = _store()
    if not store.exists("documents", name):
        raise GCPError(404, f"Document not found: {name}")
    store.delete("documents", name)
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _in_collection(doc_name: str, collection_id: str) -> bool:
    """Return True if the document belongs to a collection with the given ID.

    Args:
        doc_name (str): Full Firestore document resource name, e.g.
            "projects/P/databases/D/documents/col/doc/sub/id".
        collection_id (str): The collection ID to check for, e.g. "col" or "sub".

    Returns:
        bool: True if any ancestor collection segment equals collection_id.
    """
    parts = doc_name.split("/")
    doc_parts = parts[5:]
    for i in range(0, len(doc_parts) - 1, 2):
        if doc_parts[i] == collection_id:
            return True
    return False
