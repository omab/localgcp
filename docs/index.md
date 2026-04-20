# Cloudbox Documentation

Cloudbox is a local emulator for Google Cloud Platform services. Run Cloud Storage, Pub/Sub,
Firestore, Secret Manager, Cloud Tasks, BigQuery, Cloud Spanner, Cloud Logging, Cloud
Scheduler, and Cloud KMS entirely on your machine — no real GCP credentials required.

## Service reference

Each service has a dedicated reference page covering connection setup, all supported API
endpoints, field tables, and known limitations.

| Service | Port | Reference |
|---|---|---|
| Cloud Storage | 4443 | [Cloud Storage](services/gcs.md) |
| Cloud Pub/Sub (gRPC) | 8085 | [Cloud Pub/Sub](services/pubsub.md) |
| Cloud Pub/Sub (REST) | 8086 | [Cloud Pub/Sub](services/pubsub.md) |
| Cloud Firestore | 8080 | [Cloud Firestore](services/firestore.md) |
| Secret Manager | 8090 | [Secret Manager](services/secretmanager.md) |
| Cloud Tasks | 8123 | [Cloud Tasks](services/tasks.md) |
| Cloud Scheduler | 8091 | [Cloud Scheduler](services/scheduler.md) |
| Cloud KMS | 8092 | [Cloud KMS](services/kms.md) |
| BigQuery | 9050 | [BigQuery](services/bigquery.md) |
| Cloud Spanner | 9010 | [Cloud Spanner](services/spanner.md) |
| Cloud Logging / Monitoring | 9020 | [Cloud Logging](services/logging.md) |

## Quick start

```bash
# Install and run
uv sync
uv run cloudbox

# Or with Docker
docker compose up
```

All services start concurrently. Override any port with the corresponding
`CLOUDBOX_*_PORT` environment variable (see each service's reference page).

## Key environment variables

| Variable | Default | Purpose |
|---|---|---|
| `CLOUDBOX_PROJECT` | `local-project` | Default GCP project ID |
| `CLOUDBOX_LOCATION` | `us-central1` | Default region |
| `CLOUDBOX_DATA_DIR` | *(unset)* | Enable file-backed persistence |
| `CLOUDBOX_LOG_LEVEL` | `info` | Log verbosity |

## Further reading

- [GitHub repository](https://github.com/omab/cloudbox) — source code, issue tracker, and contributions
- [ROADMAP](https://github.com/omab/cloudbox/blob/main/ROADMAP.md) — planned features and effort estimates
- [Examples](https://github.com/omab/cloudbox/tree/main/examples) — runnable end-to-end scripts for every service
