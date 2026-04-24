"""Tests for the Cloudbox Admin UI API."""

import base64

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
