"""BigQuery emulator.

Implements the BigQuery REST API v2 used by google-cloud-bigquery.

All routes are prefixed with /bigquery/v2 — the SDK appends this path
to whatever api_endpoint is configured in ClientOptions.

Supported operations
--------------------
Datasets:   create, get, list, delete
Tables:     create, get, list, delete
Jobs:       insert (query), get
Queries:    getQueryResults (/queries/{jobId}), synchronous query (/queries)
Tabledata:  insertAll (streaming insert), list (read rows)
"""

from __future__ import annotations

import uuid

from fastapi import FastAPI, Query, Request, Response

from cloudbox.core.errors import GCPError, add_gcp_exception_handler
from cloudbox.core.middleware import add_request_logging
from cloudbox.services.bigquery.engine import get_engine

app = FastAPI(title="Cloudbox — BigQuery", version="v2")
add_gcp_exception_handler(app)
add_request_logging(app, "bigquery")


def _engine():
    return get_engine()


# ---------------------------------------------------------------------------
# Datasets
# ---------------------------------------------------------------------------


@app.post("/bigquery/v2/projects/{project}/datasets", status_code=200)
async def create_dataset(project: str, request: Request):
    body = await request.json()
    ds_ref = body.get("datasetReference", {})
    dataset_id = ds_ref.get("datasetId") or body.get("id", "")
    if not dataset_id:
        raise GCPError(400, "datasetReference.datasetId is required")
    try:
        return _engine().create_dataset(project, dataset_id, body)
    except ValueError as e:
        raise GCPError(409, str(e)) from e


@app.get("/bigquery/v2/projects/{project}/datasets/{dataset_id}")
async def get_dataset(project: str, dataset_id: str):
    meta = _engine().get_dataset(project, dataset_id)
    if meta is None:
        raise GCPError(404, f"Dataset {project}:{dataset_id} not found")
    return meta


@app.get("/bigquery/v2/projects/{project}/datasets")
async def list_datasets(project: str):
    items = _engine().list_datasets(project)
    return {
        "kind": "bigquery#datasetList",
        "datasets": [
            {
                "kind": "bigquery#dataset",
                "id": d["id"],
                "datasetReference": d["datasetReference"],
                "location": d.get("location", "US"),
            }
            for d in items
        ],
    }


@app.delete("/bigquery/v2/projects/{project}/datasets/{dataset_id}", status_code=204)
async def delete_dataset(
    project: str,
    dataset_id: str,
    deleteContents: bool = Query(default=False),
):
    try:
        found = _engine().delete_dataset(project, dataset_id, delete_contents=deleteContents)
    except ValueError as e:
        raise GCPError(400, str(e)) from e
    if not found:
        raise GCPError(404, f"Dataset {project}:{dataset_id} not found")
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------


@app.post("/bigquery/v2/projects/{project}/datasets/{dataset_id}/tables", status_code=200)
async def create_table(project: str, dataset_id: str, request: Request):
    body = await request.json()
    tbl_ref = body.get("tableReference", {})
    table_id = tbl_ref.get("tableId") or ""
    if not table_id:
        raise GCPError(400, "tableReference.tableId is required")
    try:
        if "view" in body:
            return _engine().create_view(project, dataset_id, table_id, body)
        return _engine().create_table(project, dataset_id, table_id, body)
    except ValueError as e:
        msg = str(e)
        status = 409 if "Already exists" in msg else 400
        raise GCPError(status, msg) from e


@app.patch("/bigquery/v2/projects/{project}/datasets/{dataset_id}/tables/{table_id}")
@app.put("/bigquery/v2/projects/{project}/datasets/{dataset_id}/tables/{table_id}")
async def update_table(project: str, dataset_id: str, table_id: str, request: Request):
    body = await request.json()
    try:
        if "view" in body:
            return _engine().update_view(project, dataset_id, table_id, body)
        return _engine().update_table(project, dataset_id, table_id, body)
    except ValueError as e:
        msg = str(e)
        status = 404 if "Not found" in msg else 400
        raise GCPError(status, msg) from e


@app.get("/bigquery/v2/projects/{project}/datasets/{dataset_id}/tables/{table_id}")
async def get_table(project: str, dataset_id: str, table_id: str):
    meta = _engine().get_table(project, dataset_id, table_id)
    if meta is None:
        raise GCPError(404, f"Table {project}:{dataset_id}.{table_id} not found")
    return meta


@app.get("/bigquery/v2/projects/{project}/datasets/{dataset_id}/tables")
async def list_tables(project: str, dataset_id: str):
    items = _engine().list_tables(project, dataset_id)
    return {
        "kind": "bigquery#tableList",
        "tables": [
            {
                "kind": "bigquery#table",
                "id": t["id"],
                "tableReference": t["tableReference"],
                "type": t.get("type", "TABLE"),
            }
            for t in items
        ],
        "totalItems": len(items),
    }


@app.delete(
    "/bigquery/v2/projects/{project}/datasets/{dataset_id}/tables/{table_id}",
    status_code=204,
)
async def delete_table(project: str, dataset_id: str, table_id: str):
    found = _engine().delete_table(project, dataset_id, table_id)
    if not found:
        raise GCPError(404, f"Table {project}:{dataset_id}.{table_id} not found")
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------


@app.post("/bigquery/v2/projects/{project}/jobs", status_code=200)
async def insert_job(project: str, request: Request):
    body = await request.json()
    config = body.get("configuration", {})
    query_cfg = config.get("query", {})
    sql = query_cfg.get("query", "")
    if not sql:
        raise GCPError(400, "configuration.query.query is required")

    job_ref = body.get("jobReference", {})
    job_id = job_ref.get("jobId") or str(uuid.uuid4())
    use_legacy = query_cfg.get("useLegacySql", False)
    query_parameters = query_cfg.get("queryParameters") or []
    parameter_mode = query_cfg.get("parameterMode", "NONE")

    job = _engine().run_query(
        project,
        job_id,
        sql,
        use_legacy_sql=use_legacy,
        query_parameters=query_parameters,
        parameter_mode=parameter_mode,
    )
    # Strip internal _result key before returning
    return {k: v for k, v in job.items() if not k.startswith("_")}


@app.get("/bigquery/v2/projects/{project}/jobs/{job_id}")
async def get_job(project: str, job_id: str):
    job = _engine().get_job(project, job_id)
    if job is None:
        raise GCPError(404, f"Job {project}:{job_id} not found")
    return {k: v for k, v in job.items() if not k.startswith("_")}


@app.post("/bigquery/v2/projects/{project}/jobs/{job_id}/cancel", status_code=200)
async def cancel_job(project: str, job_id: str):
    """No-op cancel — all jobs complete synchronously."""
    job = _engine().get_job(project, job_id)
    if job is None:
        raise GCPError(404, f"Job {project}:{job_id} not found")
    return {
        "kind": "bigquery#jobCancelResponse",
        "job": {k: v for k, v in job.items() if not k.startswith("_")},
    }


# ---------------------------------------------------------------------------
# Query results  (GET /queries/{jobId})
# ---------------------------------------------------------------------------


@app.get("/bigquery/v2/projects/{project}/queries/{job_id}")
async def get_query_results(
    project: str,
    job_id: str,
    maxResults: int = Query(default=1000),
    pageToken: str = Query(default=""),
    timeoutMs: int = Query(default=0),
):
    result = _engine().get_query_results(project, job_id)
    if result is None:
        raise GCPError(404, f"Job {project}:{job_id} not found")
    return result


# Synchronous query shortcut  (POST /queries)
@app.post("/bigquery/v2/projects/{project}/queries", status_code=200)
async def sync_query(project: str, request: Request):
    body = await request.json()
    sql = body.get("query", "")
    if not sql:
        raise GCPError(400, "query is required")
    job_id = str(uuid.uuid4())
    use_legacy = body.get("useLegacySql", False)
    query_parameters = body.get("queryParameters") or []
    parameter_mode = body.get("parameterMode", "NONE")
    job = _engine().run_query(
        project,
        job_id,
        sql,
        use_legacy_sql=use_legacy,
        query_parameters=query_parameters,
        parameter_mode=parameter_mode,
    )
    result = job.get("_result")
    if result is None:
        # query errored — surface as queryResponse with jobComplete=True and no rows
        return {
            "kind": "bigquery#queryResponse",
            "jobComplete": True,
            "jobReference": job["jobReference"],
            "totalRows": "0",
            "schema": {"fields": []},
            "rows": [],
            "totalBytesProcessed": "0",
        }
    return {
        "kind": "bigquery#queryResponse",
        "jobComplete": True,
        "jobReference": job["jobReference"],
        "totalRows": result["totalRows"],
        "schema": result["schema"],
        "rows": result["rows"],
        "totalBytesProcessed": "0",
    }


# ---------------------------------------------------------------------------
# Tabledata — streaming inserts and row reads
# ---------------------------------------------------------------------------


@app.post(
    "/bigquery/v2/projects/{project}/datasets/{dataset_id}/tables/{table_id}/insertAll",
    status_code=200,
)
async def insert_all(project: str, dataset_id: str, table_id: str, request: Request):
    body = await request.json()
    rows = body.get("rows", [])
    try:
        insert_errors = _engine().insert_rows(project, dataset_id, table_id, rows)
    except ValueError as e:
        raise GCPError(404, str(e)) from e
    return {
        "kind": "bigquery#tableDataInsertAllResponse",
        "insertErrors": insert_errors,
    }


@app.get("/bigquery/v2/projects/{project}/datasets/{dataset_id}/tables/{table_id}/data")
async def list_tabledata(
    project: str,
    dataset_id: str,
    table_id: str,
    maxResults: int = Query(default=1000),
    pageToken: str = Query(default=""),
):
    try:
        result = _engine().list_rows(
            project,
            dataset_id,
            table_id,
            max_results=maxResults,
            page_token=pageToken,
        )
    except ValueError as e:
        raise GCPError(404, str(e)) from e
    return result
