"""Cloud Spanner emulator.

Implements the Cloud Spanner REST API v1 used by google-cloud-spanner.

All routes are prefixed with /v1 — the SDK appends this path to whatever
api_endpoint is configured in ClientOptions.

Supported operations
--------------------
Instances:      create, get, list, update, delete
Databases:      create (returns LRO), get, list, delete, updateDdl, getDatabaseDdl
Sessions:       create, get, delete, batchCreate
Transactions:   beginTransaction, rollback
Data:           commit (mutations), read, streamingRead, executeSql, executeStreamingSql, executeBatchDml
Operations:     get (always returns done=true)
"""

from __future__ import annotations

from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse

from cloudbox.core.errors import GCPError, add_gcp_exception_handler
from cloudbox.core.middleware import add_request_logging
from cloudbox.services.spanner.engine import get_engine

app = FastAPI(title="Cloudbox — Cloud Spanner", version="v1")
add_gcp_exception_handler(app)
add_request_logging(app, "spanner")


def _engine():
    return get_engine()


# ---------------------------------------------------------------------------
# Instance configs (stub — SDK may query these on init)
# ---------------------------------------------------------------------------


@app.get("/v1/projects/{project}/instanceConfigs")
async def list_instance_configs(project: str):
    configs = _engine().list_instance_configs(project)
    return {"instanceConfigs": configs}


@app.get("/v1/projects/{project}/instanceConfigs/{config}")
async def get_instance_config(project: str, config: str):
    configs = _engine().list_instance_configs(project)
    return configs[0] if configs else {"name": f"projects/{project}/instanceConfigs/{config}"}


# ---------------------------------------------------------------------------
# Instances
# ---------------------------------------------------------------------------


@app.post("/v1/projects/{project}/instances", status_code=200)
async def create_instance(project: str, request: Request):
    body = await request.json()
    instance_id = body.get("instanceId", "")
    if not instance_id:
        raise GCPError(400, "instanceId is required")
    instance_body = body.get("instance", body)
    try:
        op = _engine().create_instance(project, instance_id, instance_body)
    except ValueError as e:
        raise GCPError(409, str(e)) from e
    return op


@app.get("/v1/projects/{project}/instances/{instance_id}")
async def get_instance(project: str, instance_id: str):
    meta = _engine().get_instance(project, instance_id)
    if meta is None:
        raise GCPError(404, f"Instance not found: {instance_id}")
    return meta


@app.get("/v1/projects/{project}/instances")
async def list_instances(project: str):
    items = _engine().list_instances(project)
    return {"instances": items}


@app.patch("/v1/projects/{project}/instances/{instance_id}", status_code=200)
async def update_instance(project: str, instance_id: str, request: Request):
    body = await request.json()
    try:
        meta = _engine().update_instance(project, instance_id, body)
    except ValueError as e:
        raise GCPError(404, str(e)) from e
    return meta


@app.delete("/v1/projects/{project}/instances/{instance_id}", status_code=200)
async def delete_instance(project: str, instance_id: str):
    found = _engine().delete_instance(project, instance_id)
    if not found:
        raise GCPError(404, f"Instance not found: {instance_id}")
    return {}


# ---------------------------------------------------------------------------
# Databases
# ---------------------------------------------------------------------------


@app.post("/v1/projects/{project}/instances/{instance_id}/databases", status_code=200)
async def create_database(project: str, instance_id: str, request: Request):
    body = await request.json()
    db_id = body.get("createStatement", "").replace("CREATE DATABASE ", "").strip().strip("`")
    if not db_id:
        raise GCPError(400, "createStatement is required")
    extra_statements = body.get("extraStatements", [])
    try:
        op = _engine().create_database(project, instance_id, db_id, extra_statements)
    except ValueError as e:
        msg = str(e)
        code = 409 if "already exists" in msg.lower() else 400
        raise GCPError(code, msg) from e
    return op


@app.get("/v1/projects/{project}/instances/{instance_id}/databases/{database_id}")
async def get_database(project: str, instance_id: str, database_id: str):
    meta = _engine().get_database(project, instance_id, database_id)
    if meta is None:
        raise GCPError(404, f"Database not found: {database_id}")
    return meta


@app.get("/v1/projects/{project}/instances/{instance_id}/databases")
async def list_databases(project: str, instance_id: str):
    items = _engine().list_databases(project, instance_id)
    return {"databases": items}


@app.delete(
    "/v1/projects/{project}/instances/{instance_id}/databases/{database_id}", status_code=200
)
async def delete_database(project: str, instance_id: str, database_id: str):
    found = _engine().delete_database(project, instance_id, database_id)
    if not found:
        raise GCPError(404, f"Database not found: {database_id}")
    return {}


@app.patch(
    "/v1/projects/{project}/instances/{instance_id}/databases/{database_id}/ddl", status_code=200
)
async def update_ddl(project: str, instance_id: str, database_id: str, request: Request):
    body = await request.json()
    statements = body.get("statements", [])
    if not statements:
        raise GCPError(400, "statements is required")
    try:
        op = _engine().execute_ddl(project, instance_id, database_id, statements)
    except ValueError as e:
        raise GCPError(400, str(e)) from e
    return op


@app.get(
    "/v1/projects/{project}/instances/{instance_id}/databases/{database_id}/ddl", status_code=200
)
async def get_database_ddl(project: str, instance_id: str, database_id: str):
    stmts = _engine().get_database_ddl(project, instance_id, database_id)
    return {"statements": stmts}


# ---------------------------------------------------------------------------
# Sessions
# ---------------------------------------------------------------------------


@app.post(
    "/v1/projects/{project}/instances/{instance_id}/databases/{database_id}/sessions",
    status_code=200,
)
async def create_session(project: str, instance_id: str, database_id: str, request: Request):
    body = await request.json()
    labels = body.get("session", {}).get("labels") or body.get("labels") or {}
    try:
        session = _engine().create_session(project, instance_id, database_id, labels)
    except ValueError as e:
        raise GCPError(404, str(e)) from e
    return session


@app.post(
    "/v1/projects/{project}/instances/{instance_id}/databases/{database_id}/sessions:batchCreate",
    status_code=200,
)
async def batch_create_sessions(project: str, instance_id: str, database_id: str, request: Request):
    body = await request.json()
    count = body.get("sessionCount", 1)
    labels = body.get("sessionTemplate", {}).get("labels") or {}
    try:
        sessions = _engine().batch_create_sessions(project, instance_id, database_id, count, labels)
    except ValueError as e:
        raise GCPError(404, str(e)) from e
    return {"session": sessions}


@app.get(
    "/v1/projects/{project}/instances/{instance_id}/databases/{database_id}/sessions",
    status_code=200,
)
async def list_sessions(project: str, instance_id: str, database_id: str):
    sessions = _engine().list_sessions(project, instance_id, database_id)
    return {"sessions": sessions}


@app.get(
    "/v1/projects/{project}/instances/{instance_id}/databases/{database_id}/sessions/{session_id}",
    status_code=200,
)
async def get_session(project: str, instance_id: str, database_id: str, session_id: str):
    session_name = (
        f"projects/{project}/instances/{instance_id}/databases/{database_id}/sessions/{session_id}"
    )
    meta = _engine().get_session(session_name)
    if meta is None:
        raise GCPError(404, f"Session not found: {session_id}")
    return meta


@app.delete(
    "/v1/projects/{project}/instances/{instance_id}/databases/{database_id}/sessions/{session_id}",
    status_code=200,
)
async def delete_session(project: str, instance_id: str, database_id: str, session_id: str):
    session_name = (
        f"projects/{project}/instances/{instance_id}/databases/{database_id}/sessions/{session_id}"
    )
    found = _engine().delete_session(session_name)
    if not found:
        raise GCPError(404, f"Session not found: {session_id}")
    return {}


# ---------------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------------


@app.post(
    "/v1/projects/{project}/instances/{instance_id}/databases/{database_id}/sessions/{session_id}:beginTransaction",
    status_code=200,
)
async def begin_transaction(
    project: str, instance_id: str, database_id: str, session_id: str, request: Request
):
    session_name = (
        f"projects/{project}/instances/{instance_id}/databases/{database_id}/sessions/{session_id}"
    )
    body = await request.json()
    options = body.get("options", {})
    try:
        txn = _engine().begin_transaction(session_name, options)
    except ValueError as e:
        raise GCPError(404, str(e)) from e
    return txn


@app.post(
    "/v1/projects/{project}/instances/{instance_id}/databases/{database_id}/sessions/{session_id}:rollback",
    status_code=200,
)
async def rollback(
    project: str, instance_id: str, database_id: str, session_id: str, request: Request
):
    session_name = (
        f"projects/{project}/instances/{instance_id}/databases/{database_id}/sessions/{session_id}"
    )
    body = await request.json()
    txn_id = body.get("transactionId", "")
    _engine().rollback(session_name, txn_id)
    return {}


# ---------------------------------------------------------------------------
# Mutations (commit)
# ---------------------------------------------------------------------------


@app.post(
    "/v1/projects/{project}/instances/{instance_id}/databases/{database_id}/sessions/{session_id}:commit",
    status_code=200,
)
async def commit(
    project: str, instance_id: str, database_id: str, session_id: str, request: Request
):
    session_name = (
        f"projects/{project}/instances/{instance_id}/databases/{database_id}/sessions/{session_id}"
    )
    body = await request.json()
    mutations = body.get("mutations", [])
    txn_id = body.get("transactionId")
    try:
        result = _engine().commit(session_name, mutations, txn_id)
    except ValueError as e:
        raise GCPError(404, str(e)) from e
    except Exception as e:
        raise GCPError(400, str(e)) from e
    return result


# ---------------------------------------------------------------------------
# Read
# ---------------------------------------------------------------------------


@app.post(
    "/v1/projects/{project}/instances/{instance_id}/databases/{database_id}/sessions/{session_id}:read",
    status_code=200,
)
async def read(project: str, instance_id: str, database_id: str, session_id: str, request: Request):
    session_name = (
        f"projects/{project}/instances/{instance_id}/databases/{database_id}/sessions/{session_id}"
    )
    body = await request.json()
    table = body.get("table", "")
    columns = body.get("columns", [])
    key_set = body.get("keySet", {})
    limit = body.get("limit", 0)
    index = body.get("index", "")
    if not table:
        raise GCPError(400, "table is required")
    if not columns:
        raise GCPError(400, "columns is required")
    try:
        result = _engine().read(session_name, table, columns, key_set, limit, index)
    except ValueError as e:
        raise GCPError(404, str(e)) from e
    except Exception as e:
        raise GCPError(400, str(e)) from e
    return result


@app.post(
    "/v1/projects/{project}/instances/{instance_id}/databases/{database_id}/sessions/{session_id}:streamingRead",
    status_code=200,
)
async def streaming_read(
    project: str, instance_id: str, database_id: str, session_id: str, request: Request
):
    session_name = (
        f"projects/{project}/instances/{instance_id}/databases/{database_id}/sessions/{session_id}"
    )
    body = await request.json()
    table = body.get("table", "")
    columns = body.get("columns", [])
    key_set = body.get("keySet", {})
    limit = body.get("limit", 0)
    index = body.get("index", "")
    if not table:
        raise GCPError(400, "table is required")
    if not columns:
        raise GCPError(400, "columns is required")
    try:
        result = _engine().read(session_name, table, columns, key_set, limit, index)
    except ValueError as e:
        raise GCPError(404, str(e)) from e

    import json as _json

    fields = result.get("metadata", {}).get("rowType", {}).get("fields", [])
    rows = result.get("rows", [])
    flat_values = [v for row in rows for v in row]
    chunk = {
        "metadata": {"rowType": {"fields": fields}},
        "values": flat_values,
        "chunkedValue": False,
        "resumeToken": "",
    }

    def _gen():
        yield _json.dumps(chunk) + "\n"

    return StreamingResponse(_gen(), media_type="application/json")


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------


@app.post(
    "/v1/projects/{project}/instances/{instance_id}/databases/{database_id}/sessions/{session_id}:executeSql",
    status_code=200,
)
async def execute_sql(
    project: str, instance_id: str, database_id: str, session_id: str, request: Request
):
    session_name = (
        f"projects/{project}/instances/{instance_id}/databases/{database_id}/sessions/{session_id}"
    )
    body = await request.json()
    sql = body.get("sql", "")
    if not sql:
        raise GCPError(400, "sql is required")
    params = body.get("params", {})
    param_types = body.get("paramTypes", {})
    try:
        result = _engine().execute_sql(session_name, sql, params, param_types)
    except ValueError as e:
        raise GCPError(404, str(e)) from e
    except Exception as e:
        raise GCPError(400, str(e)) from e
    return result


@app.post(
    "/v1/projects/{project}/instances/{instance_id}/databases/{database_id}/sessions/{session_id}:executeStreamingSql",
    status_code=200,
)
async def execute_streaming_sql(
    project: str, instance_id: str, database_id: str, session_id: str, request: Request
):
    session_name = (
        f"projects/{project}/instances/{instance_id}/databases/{database_id}/sessions/{session_id}"
    )
    body = await request.json()
    sql = body.get("sql", "")
    if not sql:
        raise GCPError(400, "sql is required")
    params = body.get("params", {})
    param_types = body.get("paramTypes", {})
    try:
        gen = _engine().execute_sql_streaming(session_name, sql, params, param_types)
    except ValueError as e:
        raise GCPError(404, str(e)) from e
    return StreamingResponse(gen, media_type="application/json")


@app.post(
    "/v1/projects/{project}/instances/{instance_id}/databases/{database_id}/sessions/{session_id}:executeBatchDml",
    status_code=200,
)
async def execute_batch_dml(
    project: str, instance_id: str, database_id: str, session_id: str, request: Request
):
    session_name = (
        f"projects/{project}/instances/{instance_id}/databases/{database_id}/sessions/{session_id}"
    )
    body = await request.json()
    statements = body.get("statements", [])
    try:
        result = _engine().execute_batch_dml(session_name, statements)
    except ValueError as e:
        raise GCPError(404, str(e)) from e
    return result


# ---------------------------------------------------------------------------
# Operations (always return done=true)
# ---------------------------------------------------------------------------


@app.get(
    "/v1/projects/{project}/instances/{instance_id}/databases/{database_id}/operations/{op_id}"
)
async def get_db_operation(project: str, instance_id: str, database_id: str, op_id: str):
    op_name = (
        f"projects/{project}/instances/{instance_id}/databases/{database_id}/operations/{op_id}"
    )
    op = _engine().get_operation(op_name)
    if op is not None:
        return op
    return {"name": op_name, "done": True}


@app.get("/v1/projects/{project}/instances/{instance_id}/operations/{op_id}")
async def get_instance_operation(project: str, instance_id: str, op_id: str):
    op_name = f"projects/{project}/instances/{instance_id}/operations/{op_id}"
    op = _engine().get_operation(op_name)
    if op is not None:
        return op
    return {"name": op_name, "done": True}


@app.get("/v1/projects/{project}/instances/{instance_id}/databases/{database_id}/operations")
async def list_db_operations(project: str, instance_id: str, database_id: str):
    return {"operations": []}


@app.get("/v1/projects/{project}/instances/{instance_id}/operations")
async def list_instance_operations(project: str, instance_id: str):
    return {"operations": []}
