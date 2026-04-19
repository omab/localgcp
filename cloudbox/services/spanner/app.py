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

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

from cloudbox.core.errors import GCPError, add_gcp_exception_handler
from cloudbox.core.middleware import add_request_logging
from cloudbox.services.spanner.engine import SpannerEngine, get_engine

app = FastAPI(title="Cloudbox — Cloud Spanner", version="v1")
add_gcp_exception_handler(app)
add_request_logging(app, "spanner")


def _engine() -> SpannerEngine:
    """Return the active SpannerEngine instance.

    Returns:
        SpannerEngine: The module-level engine singleton.
    """
    return get_engine()


# ---------------------------------------------------------------------------
# Instance configs (stub — SDK may query these on init)
# ---------------------------------------------------------------------------


@app.get("/v1/projects/{project}/instanceConfigs")
async def list_instance_configs(project: str):
    """List available Cloud Spanner instance configurations.

    Args:
        project (str): GCP project ID from the URL path.

    Returns:
        dict: Dict with an "instanceConfigs" list of configuration resource dicts.
    """
    configs = _engine().list_instance_configs(project)
    return {"instanceConfigs": configs}


@app.get("/v1/projects/{project}/instanceConfigs/{config}")
async def get_instance_config(project: str, config: str):
    """Get a Cloud Spanner instance configuration by name.

    Args:
        project (str): GCP project ID from the URL path.
        config (str): Instance configuration name from the URL path.

    Returns:
        dict: Instance configuration resource dict, or a stub dict if none exist.
    """
    configs = _engine().list_instance_configs(project)
    return configs[0] if configs else {"name": f"projects/{project}/instanceConfigs/{config}"}


# ---------------------------------------------------------------------------
# Instances
# ---------------------------------------------------------------------------


@app.post("/v1/projects/{project}/instances", status_code=200)
async def create_instance(project: str, request: Request):
    """Create a new Cloud Spanner instance.

    Args:
        project (str): GCP project ID from the URL path.
        request (Request): FastAPI request whose JSON body contains "instanceId" and "instance".

    Returns:
        dict: Completed long-running operation dict with the created instance in "response".

    Raises:
        GCPError: 400 if instanceId is missing; 409 if the instance already exists.
    """
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
    """Get a Cloud Spanner instance by ID.

    Args:
        project (str): GCP project ID from the URL path.
        instance_id (str): Spanner instance identifier from the URL path.

    Returns:
        dict: Instance metadata dict.

    Raises:
        GCPError: 404 if the instance does not exist.
    """
    meta = _engine().get_instance(project, instance_id)
    if meta is None:
        raise GCPError(404, f"Instance not found: {instance_id}")
    return meta


@app.get("/v1/projects/{project}/instances")
async def list_instances(project: str):
    """List all Cloud Spanner instances in a project.

    Args:
        project (str): GCP project ID from the URL path.

    Returns:
        dict: Dict with an "instances" list of instance metadata dicts.
    """
    items = _engine().list_instances(project)
    return {"instances": items}


@app.patch("/v1/projects/{project}/instances/{instance_id}", status_code=200)
async def update_instance(project: str, instance_id: str, request: Request):
    """Update a Cloud Spanner instance's display name, node count, or labels.

    Args:
        project (str): GCP project ID from the URL path.
        instance_id (str): Spanner instance identifier from the URL path.
        request (Request): FastAPI request whose JSON body contains the fields to update.

    Returns:
        dict: Updated instance metadata dict.

    Raises:
        GCPError: 404 if the instance does not exist.
    """
    body = await request.json()
    try:
        meta = _engine().update_instance(project, instance_id, body)
    except ValueError as e:
        raise GCPError(404, str(e)) from e
    return meta


@app.delete("/v1/projects/{project}/instances/{instance_id}", status_code=200)
async def delete_instance(project: str, instance_id: str):
    """Delete a Cloud Spanner instance and all its databases.

    Args:
        project (str): GCP project ID from the URL path.
        instance_id (str): Spanner instance identifier from the URL path.

    Returns:
        dict: Empty dict on success.

    Raises:
        GCPError: 404 if the instance does not exist.
    """
    found = _engine().delete_instance(project, instance_id)
    if not found:
        raise GCPError(404, f"Instance not found: {instance_id}")
    return {}


# ---------------------------------------------------------------------------
# Databases
# ---------------------------------------------------------------------------


@app.post("/v1/projects/{project}/instances/{instance_id}/databases", status_code=200)
async def create_database(project: str, instance_id: str, request: Request):
    """Create a Cloud Spanner database and apply any extra DDL statements.

    Args:
        project (str): GCP project ID from the URL path.
        instance_id (str): Spanner instance identifier from the URL path.
        request (Request): FastAPI request whose JSON body contains "createStatement" and
            optional "extraStatements".

    Returns:
        dict: Completed long-running operation dict with the created database in "response".

    Raises:
        GCPError: 400 if createStatement is missing or invalid; 409 if the database exists.
    """
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
    """Get a Cloud Spanner database by ID.

    Args:
        project (str): GCP project ID from the URL path.
        instance_id (str): Spanner instance identifier from the URL path.
        database_id (str): Spanner database identifier from the URL path.

    Returns:
        dict: Database metadata dict.

    Raises:
        GCPError: 404 if the database does not exist.
    """
    meta = _engine().get_database(project, instance_id, database_id)
    if meta is None:
        raise GCPError(404, f"Database not found: {database_id}")
    return meta


@app.get("/v1/projects/{project}/instances/{instance_id}/databases")
async def list_databases(project: str, instance_id: str):
    """List all databases in a Cloud Spanner instance.

    Args:
        project (str): GCP project ID from the URL path.
        instance_id (str): Spanner instance identifier from the URL path.

    Returns:
        dict: Dict with a "databases" list of database metadata dicts.
    """
    items = _engine().list_databases(project, instance_id)
    return {"databases": items}


@app.delete(
    "/v1/projects/{project}/instances/{instance_id}/databases/{database_id}", status_code=200
)
async def delete_database(project: str, instance_id: str, database_id: str):
    """Delete a Cloud Spanner database.

    Args:
        project (str): GCP project ID from the URL path.
        instance_id (str): Spanner instance identifier from the URL path.
        database_id (str): Spanner database identifier from the URL path.

    Returns:
        dict: Empty dict on success.

    Raises:
        GCPError: 404 if the database does not exist.
    """
    found = _engine().delete_database(project, instance_id, database_id)
    if not found:
        raise GCPError(404, f"Database not found: {database_id}")
    return {}


@app.patch(
    "/v1/projects/{project}/instances/{instance_id}/databases/{database_id}/ddl", status_code=200
)
async def update_ddl(project: str, instance_id: str, database_id: str, request: Request):
    """Execute DDL statements (CREATE/ALTER/DROP) against a Spanner database.

    Args:
        project (str): GCP project ID from the URL path.
        instance_id (str): Spanner instance identifier from the URL path.
        database_id (str): Spanner database identifier from the URL path.
        request (Request): FastAPI request whose JSON body contains a "statements" list.

    Returns:
        dict: Completed long-running operation dict with the executed statements in metadata.

    Raises:
        GCPError: 400 if statements is missing or any DDL statement fails.
    """
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
    """Return the DDL statements that define a database's schema.

    Args:
        project (str): GCP project ID from the URL path.
        instance_id (str): Spanner instance identifier from the URL path.
        database_id (str): Spanner database identifier from the URL path.

    Returns:
        dict: Dict with a "statements" list of DDL strings applied to the database.
    """
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
    """Create a Cloud Spanner session for a database.

    Args:
        project (str): GCP project ID from the URL path.
        instance_id (str): Spanner instance identifier from the URL path.
        database_id (str): Spanner database identifier from the URL path.
        request (Request): FastAPI request whose JSON body may contain a "session" with labels.

    Returns:
        dict: Session metadata dict.

    Raises:
        GCPError: 404 if the database does not exist.
    """
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
    """Batch-create multiple sessions for a database.

    Args:
        project (str): GCP project ID from the URL path.
        instance_id (str): Spanner instance identifier from the URL path.
        database_id (str): Spanner database identifier from the URL path.
        request (Request): FastAPI request whose JSON body contains "sessionCount" and
            optional "sessionTemplate" with labels.

    Returns:
        dict: Dict with a "session" list of session metadata dicts.

    Raises:
        GCPError: 404 if the database does not exist.
    """
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
    """List all active sessions for a database.

    Args:
        project (str): GCP project ID from the URL path.
        instance_id (str): Spanner instance identifier from the URL path.
        database_id (str): Spanner database identifier from the URL path.

    Returns:
        dict: Dict with a "sessions" list of session metadata dicts.
    """
    sessions = _engine().list_sessions(project, instance_id, database_id)
    return {"sessions": sessions}


@app.get(
    "/v1/projects/{project}/instances/{instance_id}/databases/{database_id}/sessions/{session_id}",
    status_code=200,
)
async def get_session(project: str, instance_id: str, database_id: str, session_id: str):
    """Get a Cloud Spanner session by ID.

    Args:
        project (str): GCP project ID from the URL path.
        instance_id (str): Spanner instance identifier from the URL path.
        database_id (str): Spanner database identifier from the URL path.
        session_id (str): Session UUID from the URL path.

    Returns:
        dict: Session metadata dict.

    Raises:
        GCPError: 404 if the session does not exist.
    """
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
    """Delete a Cloud Spanner session.

    Args:
        project (str): GCP project ID from the URL path.
        instance_id (str): Spanner instance identifier from the URL path.
        database_id (str): Spanner database identifier from the URL path.
        session_id (str): Session UUID from the URL path.

    Returns:
        dict: Empty dict on success.

    Raises:
        GCPError: 404 if the session does not exist.
    """
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
    """Begin a read-write or read-only transaction on a session.

    Args:
        project (str): GCP project ID from the URL path.
        instance_id (str): Spanner instance identifier from the URL path.
        database_id (str): Spanner database identifier from the URL path.
        session_id (str): Session UUID from the URL path.
        request (Request): FastAPI request whose JSON body contains transaction "options".

    Returns:
        dict: Transaction dict with an "id" field and optionally a "readTimestamp".

    Raises:
        GCPError: 404 if the session does not exist.
    """
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
    """Roll back an active read-write transaction.

    Args:
        project (str): GCP project ID from the URL path.
        instance_id (str): Spanner instance identifier from the URL path.
        database_id (str): Spanner database identifier from the URL path.
        session_id (str): Session UUID from the URL path.
        request (Request): FastAPI request whose JSON body contains "transactionId".

    Returns:
        dict: Empty dict on success.
    """
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
    """Apply a list of mutations and commit the transaction.

    Args:
        project (str): GCP project ID from the URL path.
        instance_id (str): Spanner instance identifier from the URL path.
        database_id (str): Spanner database identifier from the URL path.
        session_id (str): Session UUID from the URL path.
        request (Request): FastAPI request whose JSON body contains "mutations" and
            optional "transactionId".

    Returns:
        dict: Commit response dict with a "commitTimestamp" field.

    Raises:
        GCPError: 404 if the session does not exist; 400 on DuckDB execution errors.
    """
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
    """Read rows from a Spanner table by key set.

    Args:
        project (str): GCP project ID from the URL path.
        instance_id (str): Spanner instance identifier from the URL path.
        database_id (str): Spanner database identifier from the URL path.
        session_id (str): Session UUID from the URL path.
        request (Request): FastAPI request whose JSON body contains "table", "columns",
            "keySet", and optional "limit" and "index".

    Returns:
        dict: Spanner ResultSet dict with "metadata" and "rows" fields.

    Raises:
        GCPError: 400 if table or columns are missing; 404 if the session does not exist.
    """
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
    """Stream-read rows from a Spanner table, returning a chunked JSON response.

    Args:
        project (str): GCP project ID from the URL path.
        instance_id (str): Spanner instance identifier from the URL path.
        database_id (str): Spanner database identifier from the URL path.
        session_id (str): Session UUID from the URL path.
        request (Request): FastAPI request whose JSON body contains "table", "columns",
            "keySet", and optional "limit" and "index".

    Returns:
        StreamingResponse: Newline-delimited JSON stream of PartialResultSet objects.

    Raises:
        GCPError: 400 if table or columns are missing; 404 if the session does not exist.
    """
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
    """Execute a SQL query or DML statement and return all results.

    Args:
        project (str): GCP project ID from the URL path.
        instance_id (str): Spanner instance identifier from the URL path.
        database_id (str): Spanner database identifier from the URL path.
        session_id (str): Session UUID from the URL path.
        request (Request): FastAPI request whose JSON body contains "sql" and optional
            "params" and "paramTypes".

    Returns:
        dict: Spanner ResultSet dict with "metadata" and "rows" for SELECT, or "stats" for DML.

    Raises:
        GCPError: 400 if sql is missing or execution fails; 404 if session does not exist.
    """
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
    """Execute a SQL query and return results as a streaming response.

    Args:
        project (str): GCP project ID from the URL path.
        instance_id (str): Spanner instance identifier from the URL path.
        database_id (str): Spanner database identifier from the URL path.
        session_id (str): Session UUID from the URL path.
        request (Request): FastAPI request whose JSON body contains "sql" and optional
            "params" and "paramTypes".

    Returns:
        StreamingResponse: Newline-delimited JSON stream of PartialResultSet objects.

    Raises:
        GCPError: 400 if sql is missing; 404 if the session does not exist.
    """
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
    """Execute a batch of DML statements and return per-statement result sets.

    Args:
        project (str): GCP project ID from the URL path.
        instance_id (str): Spanner instance identifier from the URL path.
        database_id (str): Spanner database identifier from the URL path.
        session_id (str): Session UUID from the URL path.
        request (Request): FastAPI request whose JSON body contains a "statements" list,
            each with "sql" and optional "params" and "paramTypes".

    Returns:
        dict: Dict with "resultSets" (one per statement with row counts) and a "status" field.

    Raises:
        GCPError: 404 if the session does not exist.
    """
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
    """Get a long-running database operation; always returns done=true.

    Args:
        project (str): GCP project ID from the URL path.
        instance_id (str): Spanner instance identifier from the URL path.
        database_id (str): Spanner database identifier from the URL path.
        op_id (str): Operation UUID from the URL path.

    Returns:
        dict: Operation dict with "done" set to True.
    """
    op_name = (
        f"projects/{project}/instances/{instance_id}/databases/{database_id}/operations/{op_id}"
    )
    op = _engine().get_operation(op_name)
    if op is not None:
        return op
    return {"name": op_name, "done": True}


@app.get("/v1/projects/{project}/instances/{instance_id}/operations/{op_id}")
async def get_instance_operation(project: str, instance_id: str, op_id: str):
    """Get a long-running instance operation; always returns done=true.

    Args:
        project (str): GCP project ID from the URL path.
        instance_id (str): Spanner instance identifier from the URL path.
        op_id (str): Operation UUID from the URL path.

    Returns:
        dict: Operation dict with "done" set to True.
    """
    op_name = f"projects/{project}/instances/{instance_id}/operations/{op_id}"
    op = _engine().get_operation(op_name)
    if op is not None:
        return op
    return {"name": op_name, "done": True}


@app.get("/v1/projects/{project}/instances/{instance_id}/databases/{database_id}/operations")
async def list_db_operations(project: str, instance_id: str, database_id: str):
    """List database operations (always returns empty list).

    Args:
        project (str): GCP project ID from the URL path.
        instance_id (str): Spanner instance identifier from the URL path.
        database_id (str): Spanner database identifier from the URL path.

    Returns:
        dict: Dict with an empty "operations" list.
    """
    return {"operations": []}


@app.get("/v1/projects/{project}/instances/{instance_id}/operations")
async def list_instance_operations(project: str, instance_id: str):
    """List instance operations (always returns empty list).

    Args:
        project (str): GCP project ID from the URL path.
        instance_id (str): Spanner instance identifier from the URL path.

    Returns:
        dict: Dict with an empty "operations" list.
    """
    return {"operations": []}
