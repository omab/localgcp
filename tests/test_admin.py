"""Tests for the Cloudbox Admin UI API."""

import base64
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from cloudbox.admin.app import app as admin_app

PROJECT = "local-project"


@pytest.fixture
def admin_client():
    """Admin UI TestClient."""
    return TestClient(admin_app)


@pytest.fixture
def gcs(gcs_client):
    """Alias for gcs_client."""
    return gcs_client


@pytest.fixture
def pubsub(pubsub_client):
    """Alias for pubsub_client."""
    return pubsub_client


# ---------------------------------------------------------------------------
# Health and stats
# ---------------------------------------------------------------------------


def test_health(admin_client):
    r = admin_client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_stats_returns_project_and_services(admin_client):
    r = admin_client.get("/api/stats")
    assert r.status_code == 200
    body = r.json()
    assert body["project"] == PROJECT
    assert "gcs" in body["services"]
    assert "pubsub" in body["services"]


# ---------------------------------------------------------------------------
# GCS admin endpoints
# ---------------------------------------------------------------------------


def test_admin_gcs_buckets_empty(admin_client):
    r = admin_client.get("/api/gcs/buckets")
    assert r.status_code == 200
    assert r.json() == []


def test_admin_gcs_buckets_lists_created_buckets(admin_client, gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "admin-test-bucket"})

    r = admin_client.get("/api/gcs/buckets")
    assert r.status_code == 200
    names = [b["name"] for b in r.json()]
    assert "admin-test-bucket" in names


def test_admin_gcs_objects_lists_objects(admin_client, gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "bkt"})
    gcs_client.post(
        "/upload/storage/v1/b/bkt/o?uploadType=media&name=file.txt",
        content=b"hello",
        headers={"Content-Type": "text/plain"},
    )

    r = admin_client.get("/api/gcs/objects", params={"bucket": "bkt"})
    assert r.status_code == 200
    names = [o["name"] for o in r.json()]
    assert "file.txt" in names


def test_admin_gcs_delete_bucket(admin_client, gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "del-bkt"})
    gcs_client.post(
        "/upload/storage/v1/b/del-bkt/o?uploadType=media&name=obj.txt",
        content=b"x",
        headers={"Content-Type": "text/plain"},
    )

    r = admin_client.delete("/api/gcs/buckets", params={"bucket": "del-bkt"})
    assert r.status_code == 200
    assert r.json()["deleted"] == "del-bkt"

    r2 = gcs_client.get("/storage/v1/b/del-bkt")
    assert r2.status_code == 404


def test_admin_gcs_download(admin_client, gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "dl-bkt"})
    gcs_client.post(
        "/upload/storage/v1/b/dl-bkt/o?uploadType=media&name=dl.txt",
        content=b"download-me",
        headers={"Content-Type": "text/plain"},
    )

    r = admin_client.get("/api/gcs/download", params={"bucket": "dl-bkt", "name": "dl.txt"})
    assert r.status_code == 200
    assert r.content == b"download-me"


def test_admin_gcs_download_missing_returns_404(admin_client, gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "dl-bkt2"})
    r = admin_client.get("/api/gcs/download", params={"bucket": "dl-bkt2", "name": "missing.txt"})
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Pub/Sub admin endpoints
# ---------------------------------------------------------------------------


def test_admin_pubsub_topics_empty(admin_client):
    r = admin_client.get("/api/pubsub/topics")
    assert r.status_code == 200
    assert r.json() == []


def test_admin_pubsub_topics_shows_created_topics(admin_client, pubsub_client):
    topic = f"projects/{PROJECT}/topics/admin-topic"
    pubsub_client.put(f"/v1/{topic}")

    r = admin_client.get("/api/pubsub/topics")
    names = [t["name"] for t in r.json()]
    assert topic in names


def test_admin_pubsub_subscriptions_listed(admin_client, pubsub_client):
    topic = f"projects/{PROJECT}/topics/at"
    sub = f"projects/{PROJECT}/subscriptions/as"
    pubsub_client.put(f"/v1/{topic}")
    pubsub_client.put(f"/v1/{sub}", json={"name": sub, "topic": topic})

    r = admin_client.get("/api/pubsub/subscriptions")
    names = [s["name"] for s in r.json()]
    assert sub in names


def test_admin_pubsub_publish_delivers_to_subscription(admin_client, pubsub_client):
    topic = f"projects/{PROJECT}/topics/pub-topic"
    sub = f"projects/{PROJECT}/subscriptions/pub-sub"
    pubsub_client.put(f"/v1/{topic}")
    pubsub_client.put(f"/v1/{sub}", json={"name": sub, "topic": topic})

    r = admin_client.post(
        "/api/pubsub/publish",
        json={"topic": topic, "data": "hello admin"},
    )
    assert r.status_code == 200
    body = r.json()
    assert "messageId" in body
    assert body["deliveredToSubscriptions"] == 1

    r2 = pubsub_client.post(f"/v1/{sub}:pull", json={"maxMessages": 1})
    msgs = r2.json()["receivedMessages"]
    assert len(msgs) == 1
    assert base64.b64decode(msgs[0]["message"]["data"]).decode() == "hello admin"


def test_admin_pubsub_publish_missing_topic_returns_404(admin_client):
    r = admin_client.post(
        "/api/pubsub/publish",
        json={"topic": "projects/x/topics/ghost", "data": "x"},
    )
    assert r.status_code == 404


def test_admin_pubsub_delete_topic(admin_client, pubsub_client):
    topic = f"projects/{PROJECT}/topics/del-t"
    sub = f"projects/{PROJECT}/subscriptions/del-s"
    pubsub_client.put(f"/v1/{topic}")
    pubsub_client.put(f"/v1/{sub}", json={"name": sub, "topic": topic})

    r = admin_client.delete("/api/pubsub/topics", params={"topic": topic})
    assert r.status_code == 200
    assert r.json()["deleted"] == topic

    r2 = pubsub_client.get(f"/v1/{topic}")
    assert r2.status_code == 404
    r3 = pubsub_client.get(f"/v1/{sub}")
    assert r3.status_code == 404


# ---------------------------------------------------------------------------
# Firestore admin endpoints
# ---------------------------------------------------------------------------


def test_admin_firestore_collections_empty(admin_client):
    r = admin_client.get("/api/firestore/collections")
    assert r.status_code == 200
    assert r.json() == []


def test_admin_firestore_collections_lists_after_create(admin_client, firestore_client):
    db = f"projects/{PROJECT}/databases/(default)"
    docs = f"{db}/documents"
    firestore_client.post(
        f"/v1/{docs}/widgets",
        params={"documentId": "w1"},
        json={"fields": {"color": {"stringValue": "blue"}}},
    )

    r = admin_client.get("/api/firestore/collections")
    names = [c["name"] for c in r.json()]
    assert "widgets" in names


def test_admin_firestore_documents_listed(admin_client, firestore_client):
    db = f"projects/{PROJECT}/databases/(default)"
    docs = f"{db}/documents"
    firestore_client.post(
        f"/v1/{docs}/gadgets",
        params={"documentId": "g1"},
        json={"fields": {"type": {"stringValue": "phone"}}},
    )

    r = admin_client.get("/api/firestore/documents", params={"collection": "gadgets"})
    assert r.status_code == 200
    assert len(r.json()) >= 1


# ---------------------------------------------------------------------------
# Secret Manager admin endpoints
# ---------------------------------------------------------------------------


def test_admin_sm_secrets_empty(admin_client):
    r = admin_client.get("/api/secretmanager/secrets")
    assert r.status_code == 200
    assert r.json() == []


def test_admin_sm_secrets_listed(admin_client):
    from cloudbox.services.secretmanager.store import get_store

    store = get_store()
    store.set(
        "secrets",
        f"projects/{PROJECT}/secrets/my-sec",
        {"name": f"projects/{PROJECT}/secrets/my-sec"},
    )

    r = admin_client.get("/api/secretmanager/secrets")
    names = [s["name"] for s in r.json()]
    assert f"projects/{PROJECT}/secrets/my-sec" in names


# ---------------------------------------------------------------------------
# Tasks admin endpoints
# ---------------------------------------------------------------------------


def test_admin_tasks_queues_empty(admin_client):
    r = admin_client.get("/api/tasks/queues")
    assert r.status_code == 200
    assert r.json() == []


def test_admin_tasks_queues_listed(admin_client, tasks_client):
    tasks_client.post(
        f"/v2/projects/{PROJECT}/locations/us-central1/queues",
        json={"name": f"projects/{PROJECT}/locations/us-central1/queues/admin-q"},
    )

    r = admin_client.get("/api/tasks/queues")
    assert r.status_code == 200
    names = [q["name"].split("/")[-1] for q in r.json()]
    assert "admin-q" in names


# ---------------------------------------------------------------------------
# BigQuery admin endpoints
# ---------------------------------------------------------------------------


def test_admin_bq_datasets_empty(admin_client):
    r = admin_client.get("/api/bigquery/datasets")
    assert r.status_code == 200
    assert r.json() == []


def test_admin_bq_datasets_listed(admin_client, bq_client):
    bq_client.post(
        f"/bigquery/v2/projects/{PROJECT}/datasets",
        json={"datasetReference": {"projectId": PROJECT, "datasetId": "admin-ds"}},
    )

    r = admin_client.get("/api/bigquery/datasets")
    ids = [d["datasetId"] for d in r.json()]
    assert "admin-ds" in ids


# ---------------------------------------------------------------------------
# Logging admin endpoints
# ---------------------------------------------------------------------------


def test_admin_logging_entries_empty(admin_client):
    r = admin_client.get("/api/logging/entries")
    assert r.status_code == 200
    assert r.json() == []


def test_admin_logging_entries_listed(admin_client, logging_client):
    logging_client.post(
        "/v2/entries:write",
        json={"entries": [{"logName": f"projects/{PROJECT}/logs/app", "textPayload": "hi admin"}]},
    )

    r = admin_client.get("/api/logging/entries")
    assert r.status_code == 200
    payloads = [e.get("textPayload") for e in r.json()]
    assert "hi admin" in payloads


# ---------------------------------------------------------------------------
# Scheduler admin endpoints
# ---------------------------------------------------------------------------


def test_admin_scheduler_jobs_empty(admin_client):
    r = admin_client.get("/api/scheduler/jobs")
    assert r.status_code == 200
    assert r.json() == []


def test_admin_scheduler_jobs_listed(admin_client, scheduler_client):
    base = f"/v1/projects/{PROJECT}/locations/us-central1/jobs"
    scheduler_client.post(
        base,
        json={
            "name": f"projects/{PROJECT}/locations/us-central1/jobs/admin-job",
            "schedule": "* * * * *",
            "timeZone": "UTC",
            "httpTarget": {"uri": "http://localhost/noop"},
        },
    )

    r = admin_client.get("/api/scheduler/jobs")
    assert r.status_code == 200
    names = [j["name"].split("/")[-1] for j in r.json()]
    assert "admin-job" in names


# ---------------------------------------------------------------------------
# KMS admin endpoints
# ---------------------------------------------------------------------------


def test_admin_kms_keyrings_empty(admin_client):
    r = admin_client.get("/api/kms/keyrings")
    assert r.status_code == 200
    assert r.json() == []


def test_admin_kms_keyrings_listed(admin_client, kms_client):
    base = f"/v1/projects/{PROJECT}/locations/us-central1"
    kms_client.post(f"{base}/keyRings", params={"keyRingId": "admin-ring"}, json={})

    r = admin_client.get("/api/kms/keyrings")
    assert r.status_code == 200
    assert any("admin-ring" in kr["name"] for kr in r.json())


# ---------------------------------------------------------------------------
# Reset endpoints
# ---------------------------------------------------------------------------


def test_reset_all(admin_client, gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "pre-reset"})

    r = admin_client.post("/reset")
    assert r.status_code == 200
    assert r.json()["reset"] == "all"

    r2 = gcs_client.get("/storage/v1/b/pre-reset")
    assert r2.status_code == 404


def test_reset_single_service(admin_client, gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "reset-bkt"})

    r = admin_client.post("/reset/gcs")
    assert r.status_code == 200
    assert r.json()["reset"] == "gcs"

    r2 = gcs_client.get("/storage/v1/b/reset-bkt")
    assert r2.status_code == 404


# ---------------------------------------------------------------------------
# GCS notifications + delete object
# ---------------------------------------------------------------------------


def test_admin_gcs_notifications_empty(admin_client, gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "notif-bkt"})

    r = admin_client.get("/api/gcs/notifications", params={"bucket": "notif-bkt"})
    assert r.status_code == 200
    assert r.json() == []


def test_admin_gcs_notifications_listed(admin_client, gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "notif-bkt2"})
    gcs_client.post(
        "/storage/v1/b/notif-bkt2/notificationConfigs",
        json={"topic": "projects/local-project/topics/my-topic", "payload_format": "JSON_API_V1"},
    )

    r = admin_client.get("/api/gcs/notifications", params={"bucket": "notif-bkt2"})
    assert r.status_code == 200
    assert len(r.json()) == 1


def test_admin_gcs_delete_object(admin_client, gcs_client):
    gcs_client.post("/storage/v1/b", json={"name": "obj-bkt"})
    gcs_client.post(
        "/upload/storage/v1/b/obj-bkt/o?uploadType=media&name=to-del.txt",
        content=b"data",
        headers={"Content-Type": "text/plain"},
    )

    r = admin_client.delete("/api/gcs/objects", params={"bucket": "obj-bkt", "name": "to-del.txt"})
    assert r.status_code == 200
    assert r.json()["deleted"] == "obj-bkt/to-del.txt"

    r2 = admin_client.get("/api/gcs/objects", params={"bucket": "obj-bkt"})
    assert r2.json() == []


# ---------------------------------------------------------------------------
# Pub/Sub delete subscription
# ---------------------------------------------------------------------------


def test_admin_pubsub_delete_subscription(admin_client, pubsub_client):
    topic = f"projects/{PROJECT}/topics/del-sub-topic"
    sub = f"projects/{PROJECT}/subscriptions/del-sub"
    pubsub_client.put(f"/v1/{topic}")
    pubsub_client.put(f"/v1/{sub}", json={"name": sub, "topic": topic})

    r = admin_client.delete("/api/pubsub/subscriptions", params={"subscription": sub})
    assert r.status_code == 200
    assert r.json()["deleted"] == sub

    r2 = pubsub_client.get(f"/v1/{sub}")
    assert r2.status_code == 404


# ---------------------------------------------------------------------------
# Firestore delete document
# ---------------------------------------------------------------------------


def test_admin_firestore_delete_document(admin_client, firestore_client):
    db = f"projects/{PROJECT}/databases/(default)"
    docs = f"{db}/documents"
    firestore_client.post(
        f"/v1/{docs}/things",
        params={"documentId": "t1"},
        json={"fields": {"x": {"integerValue": "1"}}},
    )
    doc_key = f"{db}/documents/things/t1"

    r = admin_client.delete("/api/firestore/documents", params={"path": doc_key})
    assert r.status_code == 200
    assert r.json()["deleted"] == doc_key

    r2 = admin_client.get("/api/firestore/documents", params={"collection": "things"})
    assert r2.json() == []


# ---------------------------------------------------------------------------
# Secret Manager admin endpoints
# ---------------------------------------------------------------------------


def test_admin_sm_versions_listed(admin_client, sm_client):
    secret_name = f"projects/{PROJECT}/secrets/v-secret"
    sm_client.post(
        f"/v1/projects/{PROJECT}/secrets",
        params={"secretId": "v-secret"},
        json={"replication": {"automatic": {}}},
    )
    sm_client.post(
        f"/v1/{secret_name}:addVersion",
        json={"payload": {"data": base64.b64encode(b"hello").decode()}},
    )

    r = admin_client.get("/api/secretmanager/versions", params={"secret": "v-secret"})
    assert r.status_code == 200
    assert len(r.json()) >= 1


def test_admin_sm_versions_missing_secret_returns_404(admin_client):
    r = admin_client.get("/api/secretmanager/versions", params={"secret": "ghost-secret"})
    assert r.status_code == 404


def test_admin_sm_value_returns_decoded(admin_client, sm_client):
    secret_name = f"projects/{PROJECT}/secrets/val-secret"
    sm_client.post(
        f"/v1/projects/{PROJECT}/secrets",
        params={"secretId": "val-secret"},
        json={"replication": {"automatic": {}}},
    )
    sm_client.post(
        f"/v1/{secret_name}:addVersion",
        json={"payload": {"data": base64.b64encode(b"my-value").decode()}},
    )

    r = admin_client.get(
        "/api/secretmanager/value", params={"secret": "val-secret", "version": "1"}
    )
    assert r.status_code == 200
    assert r.json()["value"] == "my-value"


def test_admin_sm_value_missing_secret_returns_404(admin_client):
    r = admin_client.get("/api/secretmanager/value", params={"secret": "no-such", "version": "1"})
    assert r.status_code == 404


def test_admin_sm_delete_secret(admin_client, sm_client):
    sm_client.post(
        f"/v1/projects/{PROJECT}/secrets",
        params={"secretId": "del-secret"},
        json={"replication": {"automatic": {}}},
    )

    r = admin_client.delete("/api/secretmanager/secrets", params={"secret": "del-secret"})
    assert r.status_code == 200
    assert r.json()["deleted"] == "del-secret"

    r2 = admin_client.get("/api/secretmanager/secrets")
    names = [s["name"] for s in r2.json()]
    assert not any("del-secret" in n for n in names)


def test_admin_sm_delete_missing_secret_returns_404(admin_client):
    r = admin_client.delete("/api/secretmanager/secrets", params={"secret": "ghost"})
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Cloud Tasks tasks list + delete
# ---------------------------------------------------------------------------


def test_admin_tasks_tasks_listed(admin_client, tasks_client):
    queue_name = f"projects/{PROJECT}/locations/us-central1/queues/t-queue"
    tasks_client.post(
        f"/v2/projects/{PROJECT}/locations/us-central1/queues",
        json={"name": queue_name},
    )
    tasks_client.post(
        f"/v2/{queue_name}/tasks",
        json={"task": {"httpRequest": {"url": "http://localhost/noop", "httpMethod": "POST"}}},
    )

    r = admin_client.get("/api/tasks/tasks", params={"queue": queue_name})
    assert r.status_code == 200
    assert len(r.json()) == 1


def test_admin_tasks_delete_task(admin_client, tasks_client):
    queue_name = f"projects/{PROJECT}/locations/us-central1/queues/dt-queue"
    tasks_client.post(
        f"/v2/projects/{PROJECT}/locations/us-central1/queues",
        json={"name": queue_name},
    )
    resp = tasks_client.post(
        f"/v2/{queue_name}/tasks",
        json={"task": {"httpRequest": {"url": "http://localhost/noop", "httpMethod": "POST"}}},
    )
    task_name = resp.json()["name"]

    r = admin_client.delete("/api/tasks/task", params={"task": task_name})
    assert r.status_code == 200
    assert r.json()["deleted"] == task_name

    r2 = admin_client.get("/api/tasks/tasks", params={"queue": queue_name})
    assert r2.json() == []


# ---------------------------------------------------------------------------
# BigQuery tables, preview, delete dataset/table
# ---------------------------------------------------------------------------


def test_admin_bq_tables_listed(admin_client, bq_client):
    bq_client.post(
        f"/bigquery/v2/projects/{PROJECT}/datasets",
        json={"datasetReference": {"projectId": PROJECT, "datasetId": "tbl-ds"}},
    )
    bq_client.post(
        f"/bigquery/v2/projects/{PROJECT}/datasets/tbl-ds/tables",
        json={
            "tableReference": {"projectId": PROJECT, "datasetId": "tbl-ds", "tableId": "t1"},
            "schema": {"fields": [{"name": "id", "type": "INTEGER"}]},
        },
    )

    r = admin_client.get("/api/bigquery/tables", params={"dataset": "tbl-ds"})
    assert r.status_code == 200
    ids = [t["tableId"] for t in r.json()]
    assert "t1" in ids


def test_admin_bq_preview(admin_client, bq_client):
    bq_client.post(
        f"/bigquery/v2/projects/{PROJECT}/datasets",
        json={"datasetReference": {"projectId": PROJECT, "datasetId": "prev-ds"}},
    )
    bq_client.post(
        f"/bigquery/v2/projects/{PROJECT}/datasets/prev-ds/tables",
        json={
            "tableReference": {"projectId": PROJECT, "datasetId": "prev-ds", "tableId": "pt"},
            "schema": {"fields": [{"name": "val", "type": "STRING"}]},
        },
    )

    r = admin_client.get(
        "/api/bigquery/preview", params={"dataset": "prev-ds", "table": "pt", "maxResults": 10}
    )
    assert r.status_code == 200


def test_admin_bq_preview_missing_table_returns_404(admin_client, bq_client):
    bq_client.post(
        f"/bigquery/v2/projects/{PROJECT}/datasets",
        json={"datasetReference": {"projectId": PROJECT, "datasetId": "ghost-ds"}},
    )

    r = admin_client.get("/api/bigquery/preview", params={"dataset": "ghost-ds", "table": "no-tbl"})
    assert r.status_code == 404


def test_admin_bq_delete_dataset(admin_client, bq_client):
    bq_client.post(
        f"/bigquery/v2/projects/{PROJECT}/datasets",
        json={"datasetReference": {"projectId": PROJECT, "datasetId": "del-ds"}},
    )

    r = admin_client.delete("/api/bigquery/dataset", params={"dataset": "del-ds"})
    assert r.status_code == 200
    assert r.json()["deleted"] == "del-ds"

    r2 = admin_client.get("/api/bigquery/datasets")
    ids = [d["datasetId"] for d in r2.json()]
    assert "del-ds" not in ids


def test_admin_bq_delete_table(admin_client, bq_client):
    bq_client.post(
        f"/bigquery/v2/projects/{PROJECT}/datasets",
        json={"datasetReference": {"projectId": PROJECT, "datasetId": "dtbl-ds"}},
    )
    bq_client.post(
        f"/bigquery/v2/projects/{PROJECT}/datasets/dtbl-ds/tables",
        json={
            "tableReference": {"projectId": PROJECT, "datasetId": "dtbl-ds", "tableId": "dtbl"},
            "schema": {"fields": [{"name": "x", "type": "INTEGER"}]},
        },
    )

    r = admin_client.delete("/api/bigquery/table", params={"dataset": "dtbl-ds", "table": "dtbl"})
    assert r.status_code == 200
    assert r.json()["deleted"] == "dtbl-ds.dtbl"


def test_admin_bq_delete_missing_table_returns_404(admin_client, bq_client):
    bq_client.post(
        f"/bigquery/v2/projects/{PROJECT}/datasets",
        json={"datasetReference": {"projectId": PROJECT, "datasetId": "nt-ds"}},
    )

    r = admin_client.delete("/api/bigquery/table", params={"dataset": "nt-ds", "table": "ghost"})
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Logging logs list + filtered entries + delete entries
# ---------------------------------------------------------------------------


def test_admin_logging_logs_listed(admin_client, logging_client):
    logging_client.post(
        "/v2/entries:write",
        json={
            "entries": [
                {"logName": f"projects/{PROJECT}/logs/app-log", "textPayload": "msg1"},
                {"logName": f"projects/{PROJECT}/logs/sys-log", "textPayload": "msg2"},
            ]
        },
    )

    r = admin_client.get("/api/logging/logs")
    assert r.status_code == 200
    short_names = [item["shortName"] for item in r.json()]
    assert "app-log" in short_names
    assert "sys-log" in short_names


def test_admin_logging_entries_filtered_by_log(admin_client, logging_client):
    logging_client.post(
        "/v2/entries:write",
        json={
            "entries": [
                {"logName": f"projects/{PROJECT}/logs/only-log", "textPayload": "keep"},
                {"logName": f"projects/{PROJECT}/logs/other-log", "textPayload": "skip"},
            ]
        },
    )

    r = admin_client.get("/api/logging/entries", params={"log": "only-log"})
    payloads = [e.get("textPayload") for e in r.json()]
    assert "keep" in payloads
    assert "skip" not in payloads


def test_admin_logging_entries_filtered_by_severity(admin_client, logging_client):
    logging_client.post(
        "/v2/entries:write",
        json={
            "entries": [
                {
                    "logName": f"projects/{PROJECT}/logs/sev-log",
                    "textPayload": "error-msg",
                    "severity": "ERROR",
                },
                {
                    "logName": f"projects/{PROJECT}/logs/sev-log",
                    "textPayload": "debug-msg",
                    "severity": "DEBUG",
                },
            ]
        },
    )

    r = admin_client.get("/api/logging/entries", params={"severity": "ERROR"})
    payloads = [e.get("textPayload") for e in r.json()]
    assert "error-msg" in payloads
    assert "debug-msg" not in payloads


def test_admin_logging_delete_all_entries(admin_client, logging_client):
    logging_client.post(
        "/v2/entries:write",
        json={"entries": [{"logName": f"projects/{PROJECT}/logs/app", "textPayload": "x"}]},
    )

    r = admin_client.delete("/api/logging/entries")
    assert r.status_code == 200
    assert r.json()["cleared"] == "all"

    r2 = admin_client.get("/api/logging/entries")
    assert r2.json() == []


def test_admin_logging_delete_entries_by_log(admin_client, logging_client):
    logging_client.post(
        "/v2/entries:write",
        json={
            "entries": [
                {"logName": f"projects/{PROJECT}/logs/clear-me", "textPayload": "gone"},
                {"logName": f"projects/{PROJECT}/logs/keep-me", "textPayload": "stay"},
            ]
        },
    )

    r = admin_client.delete("/api/logging/entries", params={"log": "clear-me"})
    assert r.status_code == 200
    assert r.json()["cleared"] == "clear-me"

    r2 = admin_client.get("/api/logging/entries")
    payloads = [e.get("textPayload") for e in r2.json()]
    assert "gone" not in payloads
    assert "stay" in payloads


# ---------------------------------------------------------------------------
# Spanner admin endpoints
# ---------------------------------------------------------------------------


def test_admin_spanner_instances_empty(admin_client):
    r = admin_client.get("/api/spanner/instances")
    assert r.status_code == 200
    assert r.json() == []


def test_admin_spanner_instances_listed(admin_client, spanner_client):
    spanner_client.post(
        f"/v1/projects/{PROJECT}/instances",
        json={
            "instanceId": "admin-inst",
            "instance": {
                "displayName": "Admin Instance",
                "config": f"projects/{PROJECT}/instanceConfigs/regional-us-central1",
                "nodeCount": 1,
            },
        },
    )

    r = admin_client.get("/api/spanner/instances")
    assert r.status_code == 200
    ids = [i["instanceId"] for i in r.json()]
    assert "admin-inst" in ids


def test_admin_spanner_databases_listed(admin_client, spanner_client):
    spanner_client.post(
        f"/v1/projects/{PROJECT}/instances",
        json={
            "instanceId": "db-inst",
            "instance": {
                "displayName": "DB Instance",
                "config": f"projects/{PROJECT}/instanceConfigs/regional-us-central1",
                "nodeCount": 1,
            },
        },
    )
    spanner_client.post(
        f"/v1/projects/{PROJECT}/instances/db-inst/databases",
        json={"createStatement": "CREATE DATABASE `admin-db`"},
    )

    r = admin_client.get("/api/spanner/databases", params={"instance": "db-inst"})
    assert r.status_code == 200
    ids = [d["databaseId"] for d in r.json()]
    assert "admin-db" in ids


def test_admin_spanner_tables_listed(admin_client, spanner_client):
    spanner_client.post(
        f"/v1/projects/{PROJECT}/instances",
        json={
            "instanceId": "tbl-inst",
            "instance": {
                "displayName": "Tbl Instance",
                "config": f"projects/{PROJECT}/instanceConfigs/regional-us-central1",
                "nodeCount": 1,
            },
        },
    )
    spanner_client.post(
        f"/v1/projects/{PROJECT}/instances/tbl-inst/databases",
        json={
            "createStatement": "CREATE DATABASE `tbl-db`",
            "extraStatements": ["CREATE TABLE Items (id INT64) PRIMARY KEY (id)"],
        },
    )

    r = admin_client.get(
        "/api/spanner/tables", params={"instance": "tbl-inst", "database": "tbl-db"}
    )
    assert r.status_code == 200
    table_names = [t["tableName"] for t in r.json()]
    assert "Items" in table_names


def test_admin_spanner_delete_database(admin_client, spanner_client):
    spanner_client.post(
        f"/v1/projects/{PROJECT}/instances",
        json={
            "instanceId": "del-db-inst",
            "instance": {
                "displayName": "Del DB Instance",
                "config": f"projects/{PROJECT}/instanceConfigs/regional-us-central1",
                "nodeCount": 1,
            },
        },
    )
    spanner_client.post(
        f"/v1/projects/{PROJECT}/instances/del-db-inst/databases",
        json={"createStatement": "CREATE DATABASE `del-db`"},
    )

    r = admin_client.delete(
        "/api/spanner/databases", params={"instance": "del-db-inst", "database": "del-db"}
    )
    assert r.status_code == 200
    assert r.json()["deleted"] == "del-db"


def test_admin_spanner_delete_missing_database_returns_404(admin_client, spanner_client):
    spanner_client.post(
        f"/v1/projects/{PROJECT}/instances",
        json={
            "instanceId": "nd-inst",
            "instance": {
                "displayName": "ND Instance",
                "config": f"projects/{PROJECT}/instanceConfigs/regional-us-central1",
                "nodeCount": 1,
            },
        },
    )

    r = admin_client.delete(
        "/api/spanner/databases", params={"instance": "nd-inst", "database": "ghost-db"}
    )
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Scheduler admin: run, pause, resume, delete
# ---------------------------------------------------------------------------


def test_admin_scheduler_run_job(admin_client, scheduler_client):
    scheduler_client.post(
        f"/v1/projects/{PROJECT}/locations/us-central1/jobs",
        json={
            "name": f"projects/{PROJECT}/locations/us-central1/jobs/run-job",
            "schedule": "* * * * *",
            "timeZone": "UTC",
            "httpTarget": {"uri": "http://localhost/noop"},
        },
    )

    with patch("cloudbox.services.scheduler.worker._dispatch", new_callable=AsyncMock):
        r = admin_client.post("/api/scheduler/jobs/run-job:run")

    assert r.status_code == 200
    assert r.json()["lastAttemptTime"] != ""


def test_admin_scheduler_run_missing_job_returns_404(admin_client):
    with patch("cloudbox.services.scheduler.worker._dispatch", new_callable=AsyncMock):
        r = admin_client.post("/api/scheduler/jobs/ghost-job:run")
    assert r.status_code == 404


def test_admin_scheduler_pause_job(admin_client, scheduler_client):
    scheduler_client.post(
        f"/v1/projects/{PROJECT}/locations/us-central1/jobs",
        json={
            "name": f"projects/{PROJECT}/locations/us-central1/jobs/pause-job",
            "schedule": "* * * * *",
            "timeZone": "UTC",
            "httpTarget": {"uri": "http://localhost/noop"},
        },
    )

    r = admin_client.post("/api/scheduler/jobs/pause-job:pause")
    assert r.status_code == 200
    assert r.json()["state"] == "PAUSED"


def test_admin_scheduler_pause_missing_job_returns_404(admin_client):
    r = admin_client.post("/api/scheduler/jobs/ghost-job:pause")
    assert r.status_code == 404


def test_admin_scheduler_resume_job(admin_client, scheduler_client):
    scheduler_client.post(
        f"/v1/projects/{PROJECT}/locations/us-central1/jobs",
        json={
            "name": f"projects/{PROJECT}/locations/us-central1/jobs/resume-job",
            "schedule": "* * * * *",
            "timeZone": "UTC",
            "httpTarget": {"uri": "http://localhost/noop"},
        },
    )
    admin_client.post("/api/scheduler/jobs/resume-job:pause")

    r = admin_client.post("/api/scheduler/jobs/resume-job:resume")
    assert r.status_code == 200
    assert r.json()["state"] == "ENABLED"


def test_admin_scheduler_resume_missing_job_returns_404(admin_client):
    r = admin_client.post("/api/scheduler/jobs/ghost-job:resume")
    assert r.status_code == 404


def test_admin_scheduler_delete_job(admin_client, scheduler_client):
    scheduler_client.post(
        f"/v1/projects/{PROJECT}/locations/us-central1/jobs",
        json={
            "name": f"projects/{PROJECT}/locations/us-central1/jobs/del-job",
            "schedule": "* * * * *",
            "timeZone": "UTC",
            "httpTarget": {"uri": "http://localhost/noop"},
        },
    )

    r = admin_client.delete("/api/scheduler/jobs/del-job")
    assert r.status_code == 204

    r2 = admin_client.get("/api/scheduler/jobs")
    names = [j["name"].split("/")[-1] for j in r2.json()]
    assert "del-job" not in names


def test_admin_scheduler_delete_missing_job_returns_404(admin_client):
    r = admin_client.delete("/api/scheduler/jobs/ghost-job")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# KMS crypto keys
# ---------------------------------------------------------------------------


def test_admin_kms_cryptokeys_empty(admin_client):
    r = admin_client.get("/api/kms/cryptokeys")
    assert r.status_code == 200
    assert r.json() == []


def test_admin_kms_cryptokeys_listed(admin_client, kms_client):
    base = f"/v1/projects/{PROJECT}/locations/us-central1"
    kms_client.post(f"{base}/keyRings", params={"keyRingId": "ck-ring"}, json={})
    kms_client.post(
        f"{base}/keyRings/ck-ring/cryptoKeys",
        params={"cryptoKeyId": "my-key"},
        json={"purpose": "ENCRYPT_DECRYPT"},
    )

    r = admin_client.get("/api/kms/cryptokeys")
    assert r.status_code == 200
    assert any("my-key" in ck["name"] for ck in r.json())
