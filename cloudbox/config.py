import os
from dataclasses import dataclass, field


@dataclass
class Settings:
    default_project: str = field(
        default_factory=lambda: os.environ.get("CLOUDBOX_PROJECT", "local-project")
    )
    default_location: str = field(
        default_factory=lambda: os.environ.get("CLOUDBOX_LOCATION", "us-central1")
    )

    # Persistence: set to a directory path to enable JSON file persistence
    data_dir: str | None = field(default_factory=lambda: os.environ.get("CLOUDBOX_DATA_DIR"))

    # Service ports
    gcs_port: int = field(default_factory=lambda: int(os.environ.get("CLOUDBOX_GCS_PORT", "4443")))
    # pubsub_port is the gRPC port — compatible with PUBSUB_EMULATOR_HOST
    pubsub_port: int = field(
        default_factory=lambda: int(os.environ.get("CLOUDBOX_PUBSUB_PORT", "8085"))
    )
    # pubsub_rest_port is a secondary HTTP/1.1 REST endpoint (for transport="rest")
    pubsub_rest_port: int = field(
        default_factory=lambda: int(os.environ.get("CLOUDBOX_PUBSUB_REST_PORT", "8086"))
    )
    firestore_port: int = field(
        default_factory=lambda: int(os.environ.get("CLOUDBOX_FIRESTORE_PORT", "8080"))
    )
    secretmanager_port: int = field(
        default_factory=lambda: int(os.environ.get("CLOUDBOX_SECRETMANAGER_PORT", "8090"))
    )
    tasks_port: int = field(
        default_factory=lambda: int(os.environ.get("CLOUDBOX_TASKS_PORT", "8123"))
    )
    bigquery_port: int = field(
        default_factory=lambda: int(os.environ.get("CLOUDBOX_BIGQUERY_PORT", "9050"))
    )
    spanner_port: int = field(
        default_factory=lambda: int(os.environ.get("CLOUDBOX_SPANNER_PORT", "9010"))
    )
    logging_port: int = field(
        default_factory=lambda: int(os.environ.get("CLOUDBOX_LOGGING_PORT", "9020"))
    )
    scheduler_port: int = field(
        default_factory=lambda: int(os.environ.get("CLOUDBOX_SCHEDULER_PORT", "8091"))
    )
    admin_port: int = field(
        default_factory=lambda: int(os.environ.get("CLOUDBOX_ADMIN_PORT", "8888"))
    )

    host: str = field(default_factory=lambda: os.environ.get("CLOUDBOX_HOST", "0.0.0.0"))


settings = Settings()
