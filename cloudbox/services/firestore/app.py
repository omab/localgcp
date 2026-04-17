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
from datetime import datetime, timezone

from fastapi import FastAPI, Query, Request, Response
from fastapi.responses import JSONResponse

from cloudbox.core.errors import GCPError, add_gcp_exception_handler
from cloudbox.core.middleware import add_request_logging
from cloudbox.services.firestore.models import (
    BatchGetRequest,
    CommitRequest,
    CommitResponse,
    Document,
    FieldTransform,
    ListDocumentsResponse,
    RunQueryRequest,
)
from cloudbox.services.firestore.query import run_query
from cloudbox.services.firestore.store import get_store

app = FastAPI(title="Cloudbox — Cloud Firestore", version="v1")
add_gcp_exception_handler(app)
add_request_logging(app, "firestore")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _store():
    return get_store()


def _parse_path(full_path: str) -> tuple[str, str, str, list[str]]:
    """Parse a Firestore document/collection path.

    Returns (project, database, doc_root, trailing_segments).
    full_path is everything after /v1/, e.g.:
      projects/P/databases/D/documents/col/doc
    """
    # full_path = "projects/P/databases/D/documents/col/doc/..."
    parts = full_path.split("/")
    # parts: [projects, P, databases, D, documents, col, doc, ...]
    project = parts[1] if len(parts) > 1 else ""
    database = parts[3] if len(parts) > 3 else ""
    doc_root = "/".join(parts[:5])  # projects/P/databases/D/documents
    trailing = parts[5:]           # [col, doc, ...]
    return project, database, doc_root, trailing


# ---------------------------------------------------------------------------
# Action endpoints (must come before catch-all GET/POST/PATCH/DELETE)
# ---------------------------------------------------------------------------


@app.post("/v1/projects/{project}/databases/{database}/documents:runQuery")
async def run_query_root(project: str, database: str, body: RunQueryRequest):
    parent = f"projects/{project}/databases/{database}/documents"
    return await _run_query_impl(parent, body)


@app.post("/v1/{parent_path:path}/documents:runQuery")
async def run_query_nested(parent_path: str, body: RunQueryRequest):
    # parent_path = "projects/P/databases/D/documents/col/doc"
    parent = f"{parent_path}/documents"
    return await _run_query_impl(parent, body)


async def _run_query_impl(parent: str, body: RunQueryRequest):
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
                v for v in store.list("documents")
                if v["name"].startswith(prefix) and "/" not in v["name"][len(prefix):]
            ]

        sq_dict = sq.model_dump(by_alias=True, exclude_none=True)
        filtered = run_query(candidates, sq_dict)
        results.extend(filtered)

    now = _now()
    return JSONResponse(content=[{"document": doc, "readTime": now} for doc in results])


@app.post("/v1/projects/{project}/databases/{database}/documents:batchGet")
async def batch_get(project: str, database: str, body: BatchGetRequest):
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
    import base64
    txn_id = uuid.uuid4().bytes
    return {"transaction": base64.b64encode(txn_id).decode()}


def _get_field(fields: dict, parts: list[str]):
    """Return the FirestoreValue at a dotted field path, or None if missing."""
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
    """Set a FirestoreValue at a dotted field path, creating map nodes as needed."""
    current = fields
    for i, part in enumerate(parts):
        if i == len(parts) - 1:
            current[part] = value
            return
        if part not in current or "mapValue" not in current[part]:
            current[part] = {"mapValue": {"fields": {}}}
        current = current[part]["mapValue"]["fields"]


def _apply_transforms(fields: dict, transforms: list[FieldTransform], now: str) -> dict:
    """Apply field transforms to a document fields dict. Returns a new fields dict."""
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
                    base = int(existing["integerValue"]) if existing and "integerValue" in existing else 0
                    _set_field(fields, parts, {"integerValue": str(base + delta)})
            elif "doubleValue" in t.increment:
                delta = float(t.increment["doubleValue"])
                if existing and "integerValue" in existing:
                    _set_field(fields, parts, {"doubleValue": int(existing["integerValue"]) + delta})
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


@app.post("/v1/projects/{project}/databases/{database}:commit")
async def commit(project: str, database: str, body: CommitRequest):
    store = _store()
    now = _now()
    write_results = []
    for write in body.writes:
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
            write_results.append({"updateTime": now})
        elif write.delete:
            store.delete("documents", write.delete)
            write_results.append({})
    return CommitResponse(writeResults=write_results, commitTime=now).model_dump()


@app.post("/v1/projects/{project}/databases/{database}:rollback")
async def rollback(project: str, database: str):
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
    """Handle POST: create document in a collection.

    doc_path segments:
      - Odd count (collection path): create doc in that collection
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
    """Handle GET: list collection (odd segments) or get document (even segments)."""
    parts = doc_path.split("/")
    doc_root = f"projects/{project}/databases/{database}/documents"
    store = _store()

    if len(parts) % 2 == 1:
        # Odd → collection
        parent = f"{doc_root}/{'/'.join(parts[:-1])}" if len(parts) > 1 else doc_root
        collection_id = parts[-1]
        prefix = f"{parent}/{collection_id}/"
        all_docs = [
            v for v in store.list("documents")
            if v["name"].startswith(prefix) and "/" not in v["name"][len(prefix):]
        ]
        all_docs.sort(key=lambda d: d["name"])
        offset = int(pageToken) if pageToken else 0
        page = all_docs[offset: offset + pageSize]
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


@app.delete("/v1/projects/{project}/databases/{database}/documents/{doc_path:path}", status_code=204)
async def delete_document(project: str, database: str, doc_path: str):
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
    parts = doc_name.split("/")
    doc_parts = parts[5:]
    for i in range(0, len(doc_parts) - 1, 2):
        if doc_parts[i] == collection_id:
            return True
    return False
