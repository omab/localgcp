"""Pre-configured GCP SDK client factories pointing at LocalGCP.

Transport notes
---------------
LocalGCP runs two Pub/Sub endpoints:

  Port 8085 — gRPC (HTTP/2) — the standard emulator port.
              Set PUBSUB_EMULATOR_HOST=localhost:8085 before importing the
              SDK, or use pubsub_publisher() / pubsub_subscriber() which
              set that env-var automatically.

  Port 8086 — REST (HTTP/1.1) — for when you must use transport="rest".
              Pass transport="rest" to the helper functions below.

Firestore still defaults to gRPC and needs transport="rest" explicitly
(or use firestore_client() which handles this).

Other services (GCS, Secret Manager, Cloud Tasks) use REST by default.

Usage:
    from sdk_compat.clients import storage_client, pubsub_publisher, ...
    bucket = storage_client().bucket("my-bucket")
    publisher = pubsub_publisher()              # gRPC (default)
    publisher = pubsub_publisher(transport="rest")  # REST
"""
from __future__ import annotations

import os

_HOST = os.environ.get("LOCALGCP_HOST", "localhost")
_GCS_PORT = int(os.environ.get("LOCALGCP_GCS_PORT", "4443"))
# Pub/Sub has two ports:
#   LOCALGCP_PUBSUB_PORT     (8085) — gRPC, set PUBSUB_EMULATOR_HOST=localhost:8085
#   LOCALGCP_PUBSUB_REST_PORT (8086) — HTTP/1.1 REST, used by the helpers below
_PUBSUB_PORT = int(os.environ.get("LOCALGCP_PUBSUB_PORT", "8085"))
_PUBSUB_REST_PORT = int(os.environ.get("LOCALGCP_PUBSUB_REST_PORT", "8086"))
_FIRESTORE_PORT = int(os.environ.get("LOCALGCP_FIRESTORE_PORT", "8080"))
_SM_PORT = int(os.environ.get("LOCALGCP_SECRETMANAGER_PORT", "8090"))
_TASKS_PORT = int(os.environ.get("LOCALGCP_TASKS_PORT", "8123"))
_PROJECT = os.environ.get("LOCALGCP_PROJECT", "local-project")


def storage_client():
    """Return a google-cloud-storage Client pointed at LocalGCP.

    GCS SDK uses REST by default — no transport override needed.
    """
    from google.cloud import storage
    from google.auth.credentials import AnonymousCredentials

    return storage.Client(
        project=_PROJECT,
        credentials=AnonymousCredentials(),
        client_options={"api_endpoint": f"http://{_HOST}:{_GCS_PORT}"},
    )


def pubsub_publisher(*, transport: str = "grpc"):
    """Return a Pub/Sub PublisherClient pointed at LocalGCP.

    By default uses gRPC transport (port 8085), compatible with
    PUBSUB_EMULATOR_HOST.  Pass transport="rest" to use the HTTP/1.1
    endpoint on port 8086 instead.

    Requires google-cloud-pubsub >= 2.13.
    """
    from google.cloud import pubsub_v1
    from google.api_core import client_options as options

    if transport == "grpc":
        # Standard emulator pattern — SDK picks up the insecure channel
        import os
        os.environ.setdefault("PUBSUB_EMULATOR_HOST", f"{_HOST}:{_PUBSUB_PORT}")
        return pubsub_v1.PublisherClient()
    return pubsub_v1.PublisherClient(
        transport="rest",
        client_options=options.ClientOptions(
            api_endpoint=f"http://{_HOST}:{_PUBSUB_REST_PORT}",
        ),
    )


def pubsub_subscriber(*, transport: str = "grpc"):
    """Return a Pub/Sub SubscriberClient pointed at LocalGCP.

    By default uses gRPC transport (port 8085), compatible with
    PUBSUB_EMULATOR_HOST.  Pass transport="rest" to use the HTTP/1.1
    endpoint on port 8086 instead.

    Requires google-cloud-pubsub >= 2.13.
    """
    from google.cloud import pubsub_v1
    from google.api_core import client_options as options

    if transport == "grpc":
        import os
        os.environ.setdefault("PUBSUB_EMULATOR_HOST", f"{_HOST}:{_PUBSUB_PORT}")
        return pubsub_v1.SubscriberClient()
    return pubsub_v1.SubscriberClient(
        transport="rest",
        client_options=options.ClientOptions(
            api_endpoint=f"http://{_HOST}:{_PUBSUB_REST_PORT}",
        ),
    )


def firestore_client():
    """Return a Firestore Client pointed at LocalGCP (REST transport).

    The Firestore SDK defaults to gRPC. We force REST so it talks HTTP/1.1
    to LocalGCP's port.
    """
    from google.cloud import firestore
    from google.auth.credentials import AnonymousCredentials

    return firestore.Client(
        project=_PROJECT,
        credentials=AnonymousCredentials(),
        client_options={"api_endpoint": f"http://{_HOST}:{_FIRESTORE_PORT}"},
        # Firestore REST transport is selected automatically when api_endpoint
        # starts with "http://". If your SDK version still defaults to gRPC,
        # instantiate via firestore.Client(...) with transport explicitly:
        #   from google.cloud.firestore_v1.services.firestore.transports import rest
        #   client = firestore.Client(..., transport=rest.FirestoreRestTransport(...))
    )


def secret_manager_client():
    """Return a SecretManagerServiceClient pointed at LocalGCP.

    Secret Manager SDK uses REST by default — no transport override needed.
    """
    from google.cloud import secretmanager
    from google.api_core import client_options as options

    return secretmanager.SecretManagerServiceClient(
        client_options=options.ClientOptions(
            api_endpoint=f"http://{_HOST}:{_SM_PORT}",
        )
    )


def tasks_client():
    """Return a CloudTasksClient pointed at LocalGCP.

    Cloud Tasks SDK uses REST by default — no transport override needed.
    """
    from google.cloud import tasks_v2
    from google.api_core import client_options as options

    return tasks_v2.CloudTasksClient(
        client_options=options.ClientOptions(
            api_endpoint=f"http://{_HOST}:{_TASKS_PORT}",
        )
    )


_BQ_PORT = int(os.environ.get("LOCALGCP_BIGQUERY_PORT", "9050"))
_SCHEDULER_PORT = int(os.environ.get("LOCALGCP_SCHEDULER_PORT", "8091"))


def bigquery_client():
    """Return a BigQuery Client pointed at LocalGCP.

    Uses AnonymousCredentials so no real GCP auth is required.
    """
    from google.cloud import bigquery
    from google.auth.credentials import AnonymousCredentials
    from google.api_core import client_options as options

    return bigquery.Client(
        project=_PROJECT,
        credentials=AnonymousCredentials(),
        client_options=options.ClientOptions(
            api_endpoint=f"http://{_HOST}:{_BQ_PORT}",
        ),
    )


def scheduler_client():
    """Return a CloudSchedulerClient pointed at LocalGCP."""
    from google.cloud import scheduler_v1
    from google.api_core import client_options as options

    return scheduler_v1.CloudSchedulerClient(
        client_options=options.ClientOptions(
            api_endpoint=f"http://{_HOST}:{_SCHEDULER_PORT}",
        )
    )
