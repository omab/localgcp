"""Cloud Logging + Cloud Monitoring emulator.

Implements the Cloud Logging v2 REST API and a subset of Cloud Monitoring v3
used by google-cloud-logging and google-cloud-monitoring Python clients.

All Logging routes are prefixed with /v2.
All Monitoring routes are prefixed with /v3.

Supported operations (Logging)
-------------------------------
Entries:  write (batch), list
Logs:     list, delete
Sinks:    create, get, list, update, delete
Metrics:  create, get, list, update, delete

Supported operations (Monitoring)
-----------------------------------
TimeSeries: write, list (simplified)
MetricDescriptors: list
"""
from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, Request, Response

from cloudbox.core.errors import GCPError, add_gcp_exception_handler
from cloudbox.core.middleware import add_request_logging
from cloudbox.services.logging.store import get_store

app = FastAPI(title="Cloudbox — Cloud Logging", version="v2")
add_gcp_exception_handler(app)
add_request_logging(app, "logging")

_SEVERITY_ORDER = {
    "DEFAULT": 0,
    "DEBUG": 100,
    "INFO": 200,
    "NOTICE": 300,
    "WARNING": 400,
    "ERROR": 500,
    "CRITICAL": 600,
    "ALERT": 700,
    "EMERGENCY": 800,
}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _project_from_resource(resource_name: str) -> str:
    """Extract project ID from 'projects/my-project' or similar."""
    m = re.match(r"projects/([^/]+)", resource_name)
    return m.group(1) if m else resource_name


def _store():
    return get_store()


# ---------------------------------------------------------------------------
# Filter helpers
# ---------------------------------------------------------------------------

_TIMESTAMP_PATTERN = re.compile(
    r'timestamp\s*(>=|<=|>|<|=)\s*["\']?(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?)["\']?',
    re.IGNORECASE,
)
_LOGNAME_PATTERN = re.compile(
    r'logName\s*=\s*["\']([^"\']+)["\']',
    re.IGNORECASE,
)
_SEVERITY_PATTERN = re.compile(
    r'severity\s*(>=|<=|>|<|=)\s*["\']?(\w+)["\']?',
    re.IGNORECASE,
)
_RESOURCE_TYPE_PATTERN = re.compile(
    r'resource\.type\s*=\s*["\']([^"\']+)["\']',
    re.IGNORECASE,
)


def _matches_filter(entry: dict, filter_str: str) -> bool:
    """Apply basic log filter expressions to an entry."""
    if not filter_str:
        return True

    # logName filter
    for m in _LOGNAME_PATTERN.finditer(filter_str):
        if entry.get("logName", "") != m.group(1):
            return False

    # severity filter
    for m in _SEVERITY_PATTERN.finditer(filter_str):
        op = m.group(1)
        target = m.group(2).upper()
        entry_sev = entry.get("severity", "DEFAULT").upper()
        ev = _SEVERITY_ORDER.get(entry_sev, 0)
        tv = _SEVERITY_ORDER.get(target, 0)
        if op == ">=" and not (ev >= tv):
            return False
        elif op == "<=" and not (ev <= tv):
            return False
        elif op == ">" and not (ev > tv):
            return False
        elif op == "<" and not (ev < tv):
            return False
        elif op == "=" and not (ev == tv):
            return False

    # timestamp filters
    for m in _TIMESTAMP_PATTERN.finditer(filter_str):
        op = m.group(1)
        ts_str = m.group(2)
        entry_ts = entry.get("timestamp", "")
        if entry_ts and ts_str:
            if op == ">=" and not (entry_ts >= ts_str):
                return False
            elif op == "<=" and not (entry_ts <= ts_str):
                return False
            elif op == ">" and not (entry_ts > ts_str):
                return False
            elif op == "<" and not (entry_ts < ts_str):
                return False

    # resource.type filter
    for m in _RESOURCE_TYPE_PATTERN.finditer(filter_str):
        if entry.get("resource", {}).get("type", "") != m.group(1):
            return False

    return True


# ---------------------------------------------------------------------------
# Entries
# ---------------------------------------------------------------------------


@app.post("/v2/entries:write", status_code=200)
async def write_log_entries(request: Request):
    body = await request.json()
    store = _store()

    default_log_name = body.get("logName", "")
    default_resource = body.get("resource", {"type": "global", "labels": {}})
    default_labels = body.get("labels", {})
    entries = body.get("entries", [])

    # Build a per-project exclusion cache for this write batch
    _exclusion_cache: dict[str, list[dict]] = {}

    def _get_exclusions(project_id: str) -> list[dict]:
        if project_id not in _exclusion_cache:
            prefix = f"projects/{project_id}/exclusions/"
            _exclusion_cache[project_id] = [
                store.get("exclusions", k)
                for k in store.keys("exclusions")
                if k.startswith(prefix)
            ]
        return [e for e in _exclusion_cache[project_id] if e and not e.get("disabled", False)]

    for entry in entries:
        log_name = entry.get("logName") or default_log_name
        if not log_name:
            continue

        timestamp = entry.get("timestamp") or _now()
        insert_id = entry.get("insertId") or str(uuid.uuid4())

        # Merge default resource / labels
        resource = entry.get("resource") or default_resource
        labels = {**default_labels, **entry.get("labels", {})}

        stored = {
            "logName": log_name,
            "resource": resource,
            "severity": entry.get("severity", "DEFAULT"),
            "timestamp": timestamp,
            "insertId": insert_id,
            "labels": labels,
            **{
                k: entry[k]
                for k in ("textPayload", "jsonPayload", "protoPayload", "httpRequest", "operation")
                if k in entry
            },
        }

        project = _project_from_resource(log_name)

        # Drop entry if it matches any active exclusion
        if any(_matches_filter(stored, excl.get("filter", "")) for excl in _get_exclusions(project)):
            continue

        key = f"{project}/{log_name}/{insert_id}"
        store.set("entries", key, stored)

    return {}


@app.post("/v2/entries:list", status_code=200)
async def list_log_entries(request: Request):
    body = await request.json()
    store = _store()

    resource_names = body.get("resourceNames", [])
    filter_str = body.get("filter", "")
    order_by = body.get("orderBy", "timestamp desc")
    page_size = int(body.get("pageSize", 50))
    page_token = body.get("pageToken", "")

    projects = {_project_from_resource(r) for r in resource_names}

    all_entries = store.list("entries")

    # Filter by project
    if projects:
        all_entries = [
            e for e in all_entries
            if _project_from_resource(e.get("logName", "")) in projects
        ]

    # Apply log filter
    filtered = [e for e in all_entries if _matches_filter(e, filter_str)]

    # Sort
    reverse = "desc" in order_by.lower()
    filtered.sort(key=lambda e: e.get("timestamp", ""), reverse=reverse)

    # Paginate
    offset = int(page_token) if page_token else 0
    page = filtered[offset : offset + page_size]
    next_token = str(offset + page_size) if offset + page_size < len(filtered) else None

    result: dict = {"entries": page}
    if next_token:
        result["nextPageToken"] = next_token
    return result


# ---------------------------------------------------------------------------
# Logs
# ---------------------------------------------------------------------------


@app.get("/v2/projects/{project}/logs", status_code=200)
async def list_logs(project: str):
    store = _store()
    all_entries = store.list("entries")
    log_names = sorted({
        e.get("logName", "")
        for e in all_entries
        if _project_from_resource(e.get("logName", "")) == project
        and e.get("logName")
    })
    return {"logNames": log_names}


@app.delete("/v2/projects/{project}/logs/{log_id:path}", status_code=200)
async def delete_log(project: str, log_id: str):
    store = _store()
    log_name = f"projects/{project}/logs/{log_id}"
    to_delete = [k for k, v in zip(store.keys("entries"), store.list("entries")) if v.get("logName") == log_name]
    # Re-collect keys since list() and keys() may not align by index
    all_keys = store.keys("entries")
    all_vals = {k: store.get("entries", k) for k in all_keys}
    for k, v in all_vals.items():
        if v and v.get("logName") == log_name:
            store.delete("entries", k)
    return {}


# ---------------------------------------------------------------------------
# Sinks
# ---------------------------------------------------------------------------


@app.post("/v2/projects/{project}/sinks", status_code=200)
async def create_sink(project: str, request: Request):
    body = await request.json()
    sink_id = body.get("name", "")
    if not sink_id:
        raise GCPError(400, "name is required")
    store = _store()
    key = f"projects/{project}/sinks/{sink_id}"
    if store.exists("sinks", key):
        raise GCPError(409, f"Sink already exists: {sink_id}")
    now = _now()
    sink = {
        **body,
        "name": sink_id,
        "writerIdentity": f"serviceAccount:cloudbox@{project}.iam.gserviceaccount.com",
        "createTime": now,
        "updateTime": now,
    }
    store.set("sinks", key, sink)
    return sink


@app.get("/v2/projects/{project}/sinks/{sink_id}", status_code=200)
async def get_sink(project: str, sink_id: str):
    store = _store()
    key = f"projects/{project}/sinks/{sink_id}"
    sink = store.get("sinks", key)
    if sink is None:
        raise GCPError(404, f"Sink not found: {sink_id}")
    return sink


@app.get("/v2/projects/{project}/sinks", status_code=200)
async def list_sinks(project: str):
    store = _store()
    prefix = f"projects/{project}/sinks/"
    sinks = [store.get("sinks", k) for k in store.keys("sinks") if k.startswith(prefix)]
    return {"sinks": [s for s in sinks if s]}


@app.patch("/v2/projects/{project}/sinks/{sink_id}", status_code=200)
async def update_sink(project: str, sink_id: str, request: Request):
    store = _store()
    key = f"projects/{project}/sinks/{sink_id}"
    existing = store.get("sinks", key)
    if existing is None:
        raise GCPError(404, f"Sink not found: {sink_id}")
    body = await request.json()
    updated = {**existing, **body, "name": sink_id, "updateTime": _now()}
    store.set("sinks", key, updated)
    return updated


@app.delete("/v2/projects/{project}/sinks/{sink_id}", status_code=200)
async def delete_sink(project: str, sink_id: str):
    store = _store()
    key = f"projects/{project}/sinks/{sink_id}"
    found = store.delete("sinks", key)
    if not found:
        raise GCPError(404, f"Sink not found: {sink_id}")
    return {}


# ---------------------------------------------------------------------------
# Log-based metrics
# ---------------------------------------------------------------------------


@app.post("/v2/projects/{project}/metrics", status_code=200)
async def create_metric(project: str, request: Request):
    body = await request.json()
    metric_id = body.get("name", "")
    if not metric_id:
        raise GCPError(400, "name is required")
    store = _store()
    key = f"projects/{project}/metrics/{metric_id}"
    if store.exists("metrics", key):
        raise GCPError(409, f"Metric already exists: {metric_id}")
    now = _now()
    metric = {**body, "name": metric_id, "createTime": now, "updateTime": now}
    store.set("metrics", key, metric)
    return metric


@app.get("/v2/projects/{project}/metrics/{metric_id}", status_code=200)
async def get_metric(project: str, metric_id: str):
    store = _store()
    key = f"projects/{project}/metrics/{metric_id}"
    metric = store.get("metrics", key)
    if metric is None:
        raise GCPError(404, f"Metric not found: {metric_id}")
    return metric


@app.get("/v2/projects/{project}/metrics", status_code=200)
async def list_metrics(project: str):
    store = _store()
    prefix = f"projects/{project}/metrics/"
    metrics = [store.get("metrics", k) for k in store.keys("metrics") if k.startswith(prefix)]
    return {"metrics": [m for m in metrics if m]}


@app.patch("/v2/projects/{project}/metrics/{metric_id}", status_code=200)
async def update_metric(project: str, metric_id: str, request: Request):
    store = _store()
    key = f"projects/{project}/metrics/{metric_id}"
    existing = store.get("metrics", key)
    if existing is None:
        raise GCPError(404, f"Metric not found: {metric_id}")
    body = await request.json()
    updated = {**existing, **body, "name": metric_id, "updateTime": _now()}
    store.set("metrics", key, updated)
    return updated


@app.delete("/v2/projects/{project}/metrics/{metric_id}", status_code=200)
async def delete_metric(project: str, metric_id: str):
    store = _store()
    key = f"projects/{project}/metrics/{metric_id}"
    found = store.delete("metrics", key)
    if not found:
        raise GCPError(404, f"Metric not found: {metric_id}")
    return {}


# ---------------------------------------------------------------------------
# Exclusions
# ---------------------------------------------------------------------------


@app.post("/v2/projects/{project}/exclusions", status_code=200)
async def create_exclusion(project: str, request: Request):
    body = await request.json()
    name = body.get("name", "")
    if not name:
        raise GCPError(400, "name is required")
    store = _store()
    key = f"projects/{project}/exclusions/{name}"
    if store.exists("exclusions", key):
        raise GCPError(409, f"Exclusion already exists: {name}")
    now = _now()
    exclusion = {
        "name": name,
        "description": body.get("description", ""),
        "filter": body.get("filter", ""),
        "disabled": bool(body.get("disabled", False)),
        "createTime": now,
        "updateTime": now,
    }
    store.set("exclusions", key, exclusion)
    return exclusion


@app.get("/v2/projects/{project}/exclusions/{exclusion_id}", status_code=200)
async def get_exclusion(project: str, exclusion_id: str):
    store = _store()
    key = f"projects/{project}/exclusions/{exclusion_id}"
    exc = store.get("exclusions", key)
    if exc is None:
        raise GCPError(404, f"Exclusion not found: {exclusion_id}")
    return exc


@app.get("/v2/projects/{project}/exclusions", status_code=200)
async def list_exclusions(project: str):
    store = _store()
    prefix = f"projects/{project}/exclusions/"
    exclusions = [store.get("exclusions", k) for k in store.keys("exclusions") if k.startswith(prefix)]
    return {"exclusions": [e for e in exclusions if e]}


@app.patch("/v2/projects/{project}/exclusions/{exclusion_id}", status_code=200)
async def update_exclusion(project: str, exclusion_id: str, request: Request):
    store = _store()
    key = f"projects/{project}/exclusions/{exclusion_id}"
    existing = store.get("exclusions", key)
    if existing is None:
        raise GCPError(404, f"Exclusion not found: {exclusion_id}")
    body = await request.json()
    for field in ("description", "filter", "disabled"):
        if field in body:
            existing[field] = body[field]
    existing["updateTime"] = _now()
    store.set("exclusions", key, existing)
    return existing


@app.delete("/v2/projects/{project}/exclusions/{exclusion_id}", status_code=200)
async def delete_exclusion(project: str, exclusion_id: str):
    store = _store()
    key = f"projects/{project}/exclusions/{exclusion_id}"
    if not store.delete("exclusions", key):
        raise GCPError(404, f"Exclusion not found: {exclusion_id}")
    return {}


# ---------------------------------------------------------------------------
# Cloud Monitoring — Time Series
# ---------------------------------------------------------------------------


@app.post("/v3/projects/{project}/timeSeries", status_code=200)
async def write_time_series(project: str, request: Request):
    body = await request.json()
    store = _store()
    time_series_list = body.get("timeSeries", [])
    for ts in time_series_list:
        metric_type = ts.get("metric", {}).get("type", "unknown")
        points = ts.get("points", [])
        for point in points:
            ts_val = point.get("interval", {}).get("endTime") or _now()
            key = f"{project}/{metric_type}/{ts_val}/{uuid.uuid4()}"
            store.set("timeseries", key, {**ts, "_project": project})
    return {}


@app.post("/v3/projects/{project}/timeSeries:query", status_code=200)
async def query_time_series(project: str, request: Request):
    """Simplified time series query — returns stored points."""
    store = _store()
    prefix = f"{project}/"
    all_ts = [
        v for k, v in zip(store.keys("timeseries"), store.list("timeseries"))
        if k.startswith(prefix)
    ]
    # Re-collect properly
    all_keys = [k for k in store.keys("timeseries") if k.startswith(prefix)]
    all_ts = [store.get("timeseries", k) for k in all_keys]
    return {"timeSeriesData": [ts for ts in all_ts if ts]}


@app.get("/v3/projects/{project}/metricDescriptors", status_code=200)
async def list_metric_descriptors(project: str):
    return {"metricDescriptors": []}


@app.get("/v3/projects/{project}/monitoredResourceDescriptors", status_code=200)
async def list_monitored_resource_descriptors(project: str):
    return {
        "resourceDescriptors": [
            {
                "type": "global",
                "displayName": "Global",
                "description": "Global resource.",
                "labels": [],
            }
        ]
    }
