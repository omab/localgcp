"""Tests for Cloud Logging and Cloud Monitoring emulator."""

PROJECT = "local-project"
LOG_NAME = f"projects/{PROJECT}/logs/my-app"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write(client, entries, log_name=None, resource=None):
    body = {"entries": entries}
    if log_name:
        body["logName"] = log_name
    if resource:
        body["resource"] = resource
    return client.post("/v2/entries:write", json=body)


def _list(client, resource_names=None, filter_str="", page_size=100, page_token=""):
    body = {
        "resourceNames": resource_names or [f"projects/{PROJECT}"],
        "filter": filter_str,
        "pageSize": page_size,
    }
    if page_token:
        body["pageToken"] = page_token
    return client.post("/v2/entries:list", json=body)


# ---------------------------------------------------------------------------
# Write entries
# ---------------------------------------------------------------------------


def test_write_text_entry(logging_client):
    r = _write(
        logging_client,
        [{"logName": LOG_NAME, "severity": "INFO", "textPayload": "Hello"}],
    )
    assert r.status_code == 200


def test_write_json_entry(logging_client):
    r = _write(
        logging_client,
        [
            {
                "logName": LOG_NAME,
                "severity": "WARNING",
                "jsonPayload": {"key": "value"},
            }
        ],
    )
    assert r.status_code == 200


def test_write_with_default_log_name(logging_client):
    r = _write(
        logging_client,
        [{"severity": "INFO", "textPayload": "default log"}],
        log_name=LOG_NAME,
    )
    assert r.status_code == 200
    # Entry should have the top-level logName applied
    r2 = _list(logging_client)
    assert r2.status_code == 200
    entries = r2.json()["entries"]
    assert any(e.get("logName") == LOG_NAME for e in entries)


def test_write_with_default_resource(logging_client):
    r = logging_client.post(
        "/v2/entries:write",
        json={
            "logName": LOG_NAME,
            "resource": {"type": "gce_instance", "labels": {"instance_id": "123"}},
            "entries": [{"severity": "INFO", "textPayload": "vm log"}],
        },
    )
    assert r.status_code == 200
    r2 = _list(logging_client)
    assert r2.status_code == 200
    entries = r2.json()["entries"]
    vm_entries = [e for e in entries if e.get("resource", {}).get("type") == "gce_instance"]
    assert len(vm_entries) == 1


def test_write_multiple_entries(logging_client):
    r = _write(
        logging_client,
        [
            {"logName": LOG_NAME, "severity": "INFO", "textPayload": "msg1"},
            {"logName": LOG_NAME, "severity": "ERROR", "textPayload": "msg2"},
            {"logName": LOG_NAME, "severity": "DEBUG", "textPayload": "msg3"},
        ],
    )
    assert r.status_code == 200
    r2 = _list(logging_client)
    assert len(r2.json()["entries"]) == 3


# ---------------------------------------------------------------------------
# List entries
# ---------------------------------------------------------------------------


def test_list_entries_returns_all(logging_client):
    for i in range(3):
        _write(logging_client, [{"logName": LOG_NAME, "textPayload": f"msg{i}"}])
    r = _list(logging_client)
    assert r.status_code == 200
    assert len(r.json()["entries"]) == 3


def test_list_filter_by_log_name(logging_client):
    other_log = f"projects/{PROJECT}/logs/other-app"
    _write(logging_client, [{"logName": LOG_NAME, "textPayload": "app log"}])
    _write(logging_client, [{"logName": other_log, "textPayload": "other log"}])

    r = _list(logging_client, filter_str=f'logName="{LOG_NAME}"')
    assert r.status_code == 200
    entries = r.json()["entries"]
    assert all(e["logName"] == LOG_NAME for e in entries)
    assert len(entries) == 1


def test_list_filter_by_severity(logging_client):
    _write(logging_client, [{"logName": LOG_NAME, "severity": "INFO", "textPayload": "info"}])
    _write(logging_client, [{"logName": LOG_NAME, "severity": "WARNING", "textPayload": "warn"}])
    _write(logging_client, [{"logName": LOG_NAME, "severity": "ERROR", "textPayload": "error"}])
    _write(logging_client, [{"logName": LOG_NAME, "severity": "DEBUG", "textPayload": "debug"}])

    r = _list(logging_client, filter_str="severity>=WARNING")
    assert r.status_code == 200
    entries = r.json()["entries"]
    severities = {e["severity"] for e in entries}
    assert "INFO" not in severities
    assert "DEBUG" not in severities
    assert "WARNING" in severities
    assert "ERROR" in severities


def test_list_filter_by_timestamp(logging_client):
    # Write an entry with a known timestamp
    _write(
        logging_client,
        [{"logName": LOG_NAME, "severity": "INFO", "textPayload": "old", "timestamp": "2020-01-01T00:00:00Z"}],
    )
    _write(
        logging_client,
        [{"logName": LOG_NAME, "severity": "INFO", "textPayload": "new", "timestamp": "2025-01-01T00:00:00Z"}],
    )

    r = _list(logging_client, filter_str='timestamp>="2024-01-01T00:00:00Z"')
    assert r.status_code == 200
    entries = r.json()["entries"]
    texts = [e.get("textPayload", "") for e in entries]
    assert "new" in texts
    assert "old" not in texts


def test_list_pagination(logging_client):
    for i in range(10):
        _write(logging_client, [{"logName": LOG_NAME, "textPayload": f"msg{i}"}])

    r1 = _list(logging_client, page_size=4)
    assert r1.status_code == 200
    assert len(r1.json()["entries"]) == 4
    assert "nextPageToken" in r1.json()

    token = r1.json()["nextPageToken"]
    r2 = _list(logging_client, page_size=4, page_token=token)
    assert r2.status_code == 200
    assert len(r2.json()["entries"]) == 4


def test_list_no_entries(logging_client):
    r = _list(logging_client)
    assert r.status_code == 200
    assert r.json()["entries"] == []


# ---------------------------------------------------------------------------
# Logs
# ---------------------------------------------------------------------------


def test_list_logs(logging_client):
    log1 = f"projects/{PROJECT}/logs/app1"
    log2 = f"projects/{PROJECT}/logs/app2"
    _write(logging_client, [{"logName": log1, "textPayload": "a"}])
    _write(logging_client, [{"logName": log2, "textPayload": "b"}])

    r = logging_client.get(f"/v2/projects/{PROJECT}/logs")
    assert r.status_code == 200
    log_names = r.json()["logNames"]
    assert log1 in log_names
    assert log2 in log_names


def test_delete_log(logging_client):
    _write(logging_client, [{"logName": LOG_NAME, "textPayload": "to-delete"}])
    _write(
        logging_client,
        [{"logName": f"projects/{PROJECT}/logs/keeper", "textPayload": "keep"}],
    )

    r = logging_client.delete(f"/v2/projects/{PROJECT}/logs/my-app")
    assert r.status_code == 200

    r2 = _list(logging_client)
    entries = r2.json()["entries"]
    assert all(e.get("logName") != LOG_NAME for e in entries)


# ---------------------------------------------------------------------------
# Sinks
# ---------------------------------------------------------------------------


def test_create_and_get_sink(logging_client):
    r = logging_client.post(
        f"/v2/projects/{PROJECT}/sinks",
        json={
            "name": "my-sink",
            "destination": "bigquery.googleapis.com/projects/p/datasets/d",
            "filter": "severity>=ERROR",
        },
    )
    assert r.status_code == 200
    assert r.json()["name"] == "my-sink"
    assert "writerIdentity" in r.json()

    r = logging_client.get(f"/v2/projects/{PROJECT}/sinks/my-sink")
    assert r.status_code == 200
    assert r.json()["destination"].startswith("bigquery")


def test_list_sinks(logging_client):
    for s in ("sink-a", "sink-b"):
        logging_client.post(
            f"/v2/projects/{PROJECT}/sinks",
            json={"name": s, "destination": "storage.googleapis.com/my-bucket"},
        )
    r = logging_client.get(f"/v2/projects/{PROJECT}/sinks")
    assert r.status_code == 200
    names = [s["name"] for s in r.json()["sinks"]]
    assert "sink-a" in names and "sink-b" in names


def test_duplicate_sink_returns_409(logging_client):
    body = {"name": "dup-sink", "destination": "storage.googleapis.com/b"}
    logging_client.post(f"/v2/projects/{PROJECT}/sinks", json=body)
    r = logging_client.post(f"/v2/projects/{PROJECT}/sinks", json=body)
    assert r.status_code == 409


def test_update_sink(logging_client):
    logging_client.post(
        f"/v2/projects/{PROJECT}/sinks",
        json={"name": "update-me", "destination": "storage.googleapis.com/old", "filter": ""},
    )
    r = logging_client.patch(
        f"/v2/projects/{PROJECT}/sinks/update-me",
        json={"destination": "storage.googleapis.com/new"},
    )
    assert r.status_code == 200
    assert "new" in r.json()["destination"]


def test_delete_sink(logging_client):
    logging_client.post(
        f"/v2/projects/{PROJECT}/sinks",
        json={"name": "del-sink", "destination": "storage.googleapis.com/b"},
    )
    r = logging_client.delete(f"/v2/projects/{PROJECT}/sinks/del-sink")
    assert r.status_code == 200
    r = logging_client.get(f"/v2/projects/{PROJECT}/sinks/del-sink")
    assert r.status_code == 404


def test_get_missing_sink_returns_404(logging_client):
    r = logging_client.get(f"/v2/projects/{PROJECT}/sinks/nonexistent")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------


def test_create_and_get_metric(logging_client):
    r = logging_client.post(
        f"/v2/projects/{PROJECT}/metrics",
        json={"name": "error-count", "filter": "severity>=ERROR"},
    )
    assert r.status_code == 200
    assert r.json()["name"] == "error-count"

    r = logging_client.get(f"/v2/projects/{PROJECT}/metrics/error-count")
    assert r.status_code == 200
    assert r.json()["filter"] == "severity>=ERROR"


def test_list_metrics(logging_client):
    for m in ("m1", "m2"):
        logging_client.post(
            f"/v2/projects/{PROJECT}/metrics",
            json={"name": m, "filter": ""},
        )
    r = logging_client.get(f"/v2/projects/{PROJECT}/metrics")
    assert r.status_code == 200
    names = [m["name"] for m in r.json()["metrics"]]
    assert "m1" in names and "m2" in names


def test_duplicate_metric_returns_409(logging_client):
    body = {"name": "dup-metric", "filter": ""}
    logging_client.post(f"/v2/projects/{PROJECT}/metrics", json=body)
    r = logging_client.post(f"/v2/projects/{PROJECT}/metrics", json=body)
    assert r.status_code == 409


def test_update_metric(logging_client):
    logging_client.post(
        f"/v2/projects/{PROJECT}/metrics",
        json={"name": "upd-metric", "filter": "old-filter"},
    )
    r = logging_client.patch(
        f"/v2/projects/{PROJECT}/metrics/upd-metric",
        json={"filter": "new-filter"},
    )
    assert r.status_code == 200
    assert r.json()["filter"] == "new-filter"


def test_delete_metric(logging_client):
    logging_client.post(
        f"/v2/projects/{PROJECT}/metrics",
        json={"name": "del-metric", "filter": ""},
    )
    r = logging_client.delete(f"/v2/projects/{PROJECT}/metrics/del-metric")
    assert r.status_code == 200
    r = logging_client.get(f"/v2/projects/{PROJECT}/metrics/del-metric")
    assert r.status_code == 404


def test_get_missing_metric_returns_404(logging_client):
    r = logging_client.get(f"/v2/projects/{PROJECT}/metrics/nonexistent")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Cloud Monitoring — Time Series
# ---------------------------------------------------------------------------


def test_write_time_series(logging_client):
    r = logging_client.post(
        f"/v3/projects/{PROJECT}/timeSeries",
        json={
            "timeSeries": [
                {
                    "metric": {
                        "type": "custom.googleapis.com/my_metric",
                        "labels": {"env": "test"},
                    },
                    "resource": {"type": "global", "labels": {}},
                    "points": [
                        {
                            "interval": {"endTime": "2025-01-01T00:00:00Z"},
                            "value": {"doubleValue": 42.0},
                        }
                    ],
                }
            ]
        },
    )
    assert r.status_code == 200


def test_list_metric_descriptors(logging_client):
    r = logging_client.get(f"/v3/projects/{PROJECT}/metricDescriptors")
    assert r.status_code == 200
    assert "metricDescriptors" in r.json()


def test_list_monitored_resource_descriptors(logging_client):
    r = logging_client.get(f"/v3/projects/{PROJECT}/monitoredResourceDescriptors")
    assert r.status_code == 200
    assert "resourceDescriptors" in r.json()


# ---------------------------------------------------------------------------
# Entry auto-fields
# ---------------------------------------------------------------------------


def test_auto_timestamp_and_insert_id(logging_client):
    r = _write(logging_client, [{"logName": LOG_NAME, "textPayload": "auto"}])
    assert r.status_code == 200
    r2 = _list(logging_client)
    entries = r2.json()["entries"]
    assert len(entries) == 1
    e = entries[0]
    assert e.get("timestamp")     # auto-set
    assert e.get("insertId")      # auto-set


def test_explicit_insert_id_preserved(logging_client):
    _write(logging_client, [{"logName": LOG_NAME, "insertId": "my-custom-id", "textPayload": "x"}])
    r = _list(logging_client)
    entries = r.json()["entries"]
    assert any(e.get("insertId") == "my-custom-id" for e in entries)


# ---------------------------------------------------------------------------
# Write — edge cases
# ---------------------------------------------------------------------------


def test_write_entry_without_log_name_is_skipped(logging_client):
    """Entries with no logName and no default logName should be silently skipped."""
    r = logging_client.post("/v2/entries:write", json={"entries": [{"textPayload": "no name"}]})
    assert r.status_code == 200
    r2 = _list(logging_client)
    assert r2.json().get("entries", []) == []


# ---------------------------------------------------------------------------
# Filter — additional severity operators
# ---------------------------------------------------------------------------


def test_filter_severity_lte(logging_client):
    """severity <= DEBUG matches DEBUG but not INFO."""
    _write(logging_client, [
        {"logName": LOG_NAME, "severity": "DEBUG", "textPayload": "dbg"},
        {"logName": LOG_NAME, "severity": "INFO", "textPayload": "inf"},
    ])
    r = _list(logging_client, filter_str='severity <= "DEBUG"')
    entries = r.json()["entries"]
    assert all(e["severity"] == "DEBUG" for e in entries)


def test_filter_severity_gt(logging_client):
    """severity > INFO matches WARNING and above."""
    _write(logging_client, [
        {"logName": LOG_NAME, "severity": "INFO", "textPayload": "inf"},
        {"logName": LOG_NAME, "severity": "WARNING", "textPayload": "warn"},
        {"logName": LOG_NAME, "severity": "ERROR", "textPayload": "err"},
    ])
    r = _list(logging_client, filter_str='severity > "INFO"')
    entries = r.json()["entries"]
    assert len(entries) == 2
    assert all(e["severity"] in ("WARNING", "ERROR") for e in entries)


def test_filter_severity_lt(logging_client):
    """severity < WARNING matches INFO and below."""
    _write(logging_client, [
        {"logName": LOG_NAME, "severity": "DEBUG", "textPayload": "d"},
        {"logName": LOG_NAME, "severity": "INFO", "textPayload": "i"},
        {"logName": LOG_NAME, "severity": "WARNING", "textPayload": "w"},
    ])
    r = _list(logging_client, filter_str='severity < "WARNING"')
    entries = r.json()["entries"]
    assert len(entries) == 2
    assert all(e["severity"] in ("DEBUG", "INFO") for e in entries)


def test_filter_severity_eq(logging_client):
    """severity = INFO matches exactly INFO."""
    _write(logging_client, [
        {"logName": LOG_NAME, "severity": "INFO", "textPayload": "i"},
        {"logName": LOG_NAME, "severity": "WARNING", "textPayload": "w"},
    ])
    r = _list(logging_client, filter_str='severity = "INFO"')
    entries = r.json()["entries"]
    assert len(entries) == 1
    assert entries[0]["severity"] == "INFO"


def test_filter_timestamp_lte(logging_client):
    """timestamp <= filter."""
    ts = "2025-06-01T00:00:00Z"
    _write(logging_client, [
        {"logName": LOG_NAME, "timestamp": "2025-05-01T00:00:00Z", "textPayload": "before"},
        {"logName": LOG_NAME, "timestamp": "2025-07-01T00:00:00Z", "textPayload": "after"},
    ])
    r = _list(logging_client, filter_str=f'timestamp <= "{ts}"')
    entries = r.json()["entries"]
    assert len(entries) == 1
    assert entries[0]["textPayload"] == "before"


def test_filter_timestamp_gt(logging_client):
    """timestamp > filter."""
    ts = "2025-06-01T00:00:00Z"
    _write(logging_client, [
        {"logName": LOG_NAME, "timestamp": "2025-05-01T00:00:00Z", "textPayload": "before"},
        {"logName": LOG_NAME, "timestamp": "2025-07-01T00:00:00Z", "textPayload": "after"},
    ])
    r = _list(logging_client, filter_str=f'timestamp > "{ts}"')
    entries = r.json()["entries"]
    assert len(entries) == 1
    assert entries[0]["textPayload"] == "after"


def test_filter_timestamp_lt(logging_client):
    """timestamp < filter."""
    ts = "2025-07-01T00:00:00Z"
    _write(logging_client, [
        {"logName": LOG_NAME, "timestamp": "2025-05-01T00:00:00Z", "textPayload": "old"},
        {"logName": LOG_NAME, "timestamp": "2025-07-01T00:00:00Z", "textPayload": "exact"},
    ])
    r = _list(logging_client, filter_str=f'timestamp < "{ts}"')
    entries = r.json()["entries"]
    assert len(entries) == 1
    assert entries[0]["textPayload"] == "old"


def test_filter_resource_type(logging_client):
    """resource.type = filter."""
    _write(logging_client, [
        {"logName": LOG_NAME, "resource": {"type": "gce_instance"}, "textPayload": "vm"},
        {"logName": LOG_NAME, "resource": {"type": "gcs_bucket"}, "textPayload": "bucket"},
    ])
    r = _list(logging_client, filter_str='resource.type = "gce_instance"')
    entries = r.json()["entries"]
    assert len(entries) == 1
    assert entries[0]["textPayload"] == "vm"


# ---------------------------------------------------------------------------
# Sinks — error paths
# ---------------------------------------------------------------------------


def test_create_sink_empty_name_returns_400(logging_client):
    r = logging_client.post(
        f"/v2/projects/{PROJECT}/sinks",
        json={"destination": "storage.googleapis.com/my-bucket"},
    )
    assert r.status_code == 400


def test_update_missing_sink_returns_404(logging_client):
    r = logging_client.patch(
        f"/v2/projects/{PROJECT}/sinks/no-such-sink",
        json={"destination": "storage.googleapis.com/other"},
    )
    assert r.status_code == 404


def test_delete_missing_sink_returns_404(logging_client):
    r = logging_client.delete(f"/v2/projects/{PROJECT}/sinks/no-such-sink")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Metrics — error paths
# ---------------------------------------------------------------------------


def test_create_metric_empty_name_returns_400(logging_client):
    r = logging_client.post(
        f"/v2/projects/{PROJECT}/metrics",
        json={"filter": 'severity >= ERROR'},
    )
    assert r.status_code == 400


def test_update_missing_metric_returns_404(logging_client):
    r = logging_client.patch(
        f"/v2/projects/{PROJECT}/metrics/no-such-metric",
        json={"filter": 'severity >= WARNING'},
    )
    assert r.status_code == 404


def test_delete_missing_metric_returns_404(logging_client):
    r = logging_client.delete(f"/v2/projects/{PROJECT}/metrics/no-such-metric")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Time series — query
# ---------------------------------------------------------------------------


def test_query_time_series(logging_client):
    """Write a time series then query it back."""
    logging_client.post(
        f"/v3/projects/{PROJECT}/timeSeries",
        json={
            "timeSeries": [{
                "metric": {"type": "custom.googleapis.com/requests", "labels": {}},
                "resource": {"type": "global", "labels": {}},
                "points": [{"interval": {"endTime": "2025-01-01T00:00:00Z"}, "value": {"int64Value": "1"}}],
            }]
        },
    )
    r = logging_client.post(
        f"/v3/projects/{PROJECT}/timeSeries:query",
        json={"query": "fetch global"},
    )
    assert r.status_code == 200
    data = r.json()
    assert "timeSeriesData" in data
    assert len(data["timeSeriesData"]) >= 1


# ---------------------------------------------------------------------------
# Exclusions — CRUD
# ---------------------------------------------------------------------------


def test_create_and_get_exclusion(logging_client):
    r = logging_client.post(f"/v2/projects/{PROJECT}/exclusions", json={
        "name": "excl-1",
        "filter": 'severity >= "ERROR"',
        "description": "Drop errors",
    })
    assert r.status_code == 200
    body = r.json()
    assert body["name"] == "excl-1"
    assert body["disabled"] is False

    r2 = logging_client.get(f"/v2/projects/{PROJECT}/exclusions/excl-1")
    assert r2.status_code == 200
    assert r2.json()["filter"] == 'severity >= "ERROR"'


def test_list_exclusions(logging_client):
    for i in range(3):
        logging_client.post(f"/v2/projects/{PROJECT}/exclusions", json={
            "name": f"list-excl-{i}",
            "filter": f'logName = "projects/{PROJECT}/logs/log-{i}"',
        })
    r = logging_client.get(f"/v2/projects/{PROJECT}/exclusions")
    assert r.status_code == 200
    names = {e["name"] for e in r.json()["exclusions"]}
    assert "list-excl-0" in names and "list-excl-2" in names


def test_duplicate_exclusion_returns_409(logging_client):
    logging_client.post(f"/v2/projects/{PROJECT}/exclusions", json={"name": "dup-excl", "filter": ""})
    r = logging_client.post(f"/v2/projects/{PROJECT}/exclusions", json={"name": "dup-excl", "filter": ""})
    assert r.status_code == 409


def test_update_exclusion(logging_client):
    logging_client.post(f"/v2/projects/{PROJECT}/exclusions", json={
        "name": "upd-excl",
        "filter": 'severity = "DEBUG"',
    })
    r = logging_client.patch(f"/v2/projects/{PROJECT}/exclusions/upd-excl", json={
        "filter": 'severity = "INFO"',
        "disabled": True,
    })
    assert r.status_code == 200
    assert r.json()["filter"] == 'severity = "INFO"'
    assert r.json()["disabled"] is True


def test_delete_exclusion(logging_client):
    logging_client.post(f"/v2/projects/{PROJECT}/exclusions", json={"name": "del-excl", "filter": ""})
    r = logging_client.delete(f"/v2/projects/{PROJECT}/exclusions/del-excl")
    assert r.status_code == 200
    r2 = logging_client.get(f"/v2/projects/{PROJECT}/exclusions/del-excl")
    assert r2.status_code == 404


def test_get_missing_exclusion_returns_404(logging_client):
    r = logging_client.get(f"/v2/projects/{PROJECT}/exclusions/no-such")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Exclusions — filtering at write time
# ---------------------------------------------------------------------------


def _write_entry(logging_client, log_name: str, severity: str, insert_id: str):
    logging_client.post("/v2/entries:write", json={
        "entries": [{
            "logName": log_name,
            "severity": severity,
            "insertId": insert_id,
            "textPayload": f"msg-{insert_id}",
            "resource": {"type": "global", "labels": {}},
        }]
    })


def test_exclusion_filters_matching_entries(logging_client):
    """Entries matching an active exclusion filter are not stored."""
    log_name = f"projects/{PROJECT}/logs/noise"
    logging_client.post(f"/v2/projects/{PROJECT}/exclusions", json={
        "name": "drop-debug",
        "filter": 'severity = "DEBUG"',
    })

    _write_entry(logging_client, log_name, "DEBUG", "id-debug-1")
    _write_entry(logging_client, log_name, "INFO", "id-info-1")

    r = logging_client.post("/v2/entries:list", json={
        "resourceNames": [f"projects/{PROJECT}"],
        "filter": f'logName = "{log_name}"',
    })
    insert_ids = {e["insertId"] for e in r.json().get("entries", [])}
    assert "id-info-1" in insert_ids
    assert "id-debug-1" not in insert_ids


def test_disabled_exclusion_does_not_filter(logging_client):
    """A disabled exclusion must not suppress entries."""
    log_name = f"projects/{PROJECT}/logs/verbose"
    logging_client.post(f"/v2/projects/{PROJECT}/exclusions", json={
        "name": "disabled-excl",
        "filter": 'severity = "DEBUG"',
        "disabled": True,
    })

    _write_entry(logging_client, log_name, "DEBUG", "id-debug-kept")

    r = logging_client.post("/v2/entries:list", json={
        "resourceNames": [f"projects/{PROJECT}"],
        "filter": f'logName = "{log_name}"',
    })
    insert_ids = {e["insertId"] for e in r.json().get("entries", [])}
    assert "id-debug-kept" in insert_ids


def test_exclusion_by_log_name(logging_client):
    """Exclusions can target a specific log by logName."""
    noisy_log = f"projects/{PROJECT}/logs/noisy-app"
    keep_log = f"projects/{PROJECT}/logs/keep-app"

    logging_client.post(f"/v2/projects/{PROJECT}/exclusions", json={
        "name": "drop-noisy",
        "filter": f'logName = "{noisy_log}"',
    })

    _write_entry(logging_client, noisy_log, "INFO", "id-noisy")
    _write_entry(logging_client, keep_log, "INFO", "id-keep")

    r = logging_client.post("/v2/entries:list", json={
        "resourceNames": [f"projects/{PROJECT}"],
    })
    insert_ids = {e["insertId"] for e in r.json().get("entries", [])}
    assert "id-keep" in insert_ids
    assert "id-noisy" not in insert_ids


def test_deleting_exclusion_resumes_writes(logging_client):
    """After an exclusion is deleted, matching entries are stored normally."""
    log_name = f"projects/{PROJECT}/logs/temp-excl-log"
    logging_client.post(f"/v2/projects/{PROJECT}/exclusions", json={
        "name": "temp-excl",
        "filter": 'severity = "DEBUG"',
    })

    _write_entry(logging_client, log_name, "DEBUG", "id-before-delete")

    logging_client.delete(f"/v2/projects/{PROJECT}/exclusions/temp-excl")

    _write_entry(logging_client, log_name, "DEBUG", "id-after-delete")

    r = logging_client.post("/v2/entries:list", json={
        "resourceNames": [f"projects/{PROJECT}"],
        "filter": f'logName = "{log_name}"',
    })
    insert_ids = {e["insertId"] for e in r.json().get("entries", [])}
    assert "id-before-delete" not in insert_ids
    assert "id-after-delete" in insert_ids
