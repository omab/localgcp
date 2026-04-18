"""Shared base URLs and helpers for all Cloudbox examples.

Cloudbox must be running before executing any example:

    uv run cloudbox
    # or: docker compose up

Override defaults via environment variables:

    CLOUDBOX_HOST=localhost
    CLOUDBOX_PROJECT=local-project
    CLOUDBOX_LOCATION=us-central1
    CLOUDBOX_GCS_PORT=4443
    CLOUDBOX_PUBSUB_REST_PORT=8086
    CLOUDBOX_FIRESTORE_PORT=8080
    CLOUDBOX_SECRETMANAGER_PORT=8090
    CLOUDBOX_TASKS_PORT=8123
    CLOUDBOX_BIGQUERY_PORT=9050
    CLOUDBOX_SCHEDULER_PORT=8091
    CLOUDBOX_LOGGING_PORT=9020
"""
import os
import httpx

HOST = os.environ.get("CLOUDBOX_HOST", "localhost")
PROJECT = os.environ.get("CLOUDBOX_PROJECT", "local-project")
LOCATION = os.environ.get("CLOUDBOX_LOCATION", "us-central1")

GCS_BASE = f"http://{HOST}:{os.environ.get('CLOUDBOX_GCS_PORT', '4443')}"
PUBSUB_BASE = f"http://{HOST}:{os.environ.get('CLOUDBOX_PUBSUB_REST_PORT', '8086')}"
FIRESTORE_BASE = f"http://{HOST}:{os.environ.get('CLOUDBOX_FIRESTORE_PORT', '8080')}"
SECRETMANAGER_BASE = f"http://{HOST}:{os.environ.get('CLOUDBOX_SECRETMANAGER_PORT', '8090')}"
TASKS_BASE = f"http://{HOST}:{os.environ.get('CLOUDBOX_TASKS_PORT', '8123')}"
BIGQUERY_BASE = f"http://{HOST}:{os.environ.get('CLOUDBOX_BIGQUERY_PORT', '9050')}"
SCHEDULER_BASE = f"http://{HOST}:{os.environ.get('CLOUDBOX_SCHEDULER_PORT', '8091')}"
LOGGING_BASE = f"http://{HOST}:{os.environ.get('CLOUDBOX_LOGGING_PORT', '9020')}"


def client() -> httpx.Client:
    return httpx.Client(timeout=10)


def ok(r: httpx.Response) -> httpx.Response:
    """Raise on non-2xx and return the response."""
    r.raise_for_status()
    return r
