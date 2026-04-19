# Cloudbox Roadmap

Ordered by impact. Effort is estimated relative to the existing codebase:
**S** = days · **M** = 1–2 weeks · **L** = 2–4 weeks · **XL** = month+

---

## New services

These add coverage for GCP services not yet emulated. Ordered by how often
real-world applications depend on them.

| Service | Impact | Effort | Notes |
|---|:---:|:---:|---|
| Cloud Memorystore (Redis) | High | M | Use `fakeredis` as the backend; expose a standard Redis TCP port. Most apps that use GCP also use Redis for caching or session storage. |
| Eventarc | High | M | Event routing to Cloud Run / HTTP endpoints. Builds on the existing Pub/Sub infrastructure; needs a trigger model and a dispatch loop similar to Cloud Scheduler. |
| Cloud Run | High | L | Invoke containerised or in-process HTTP handlers locally. Could be modelled as a registry of named services with a routing proxy — no Docker required for simple cases. |
| Cloud SQL | High | XL | Relational database (Postgres / MySQL mode). Most realistic approach: embed a real Postgres process or use DuckDB as a Postgres wire-protocol server. Large effort but very high payoff since almost every GCP app uses a relational DB. |
| Cloud Datastore | Medium | M | Firestore in Datastore mode uses a different API surface (`datastore.googleapis.com`). Many legacy Python apps use `google-cloud-datastore` directly. The storage model maps cleanly onto the existing Firestore store. |
| Cloud Bigtable | Medium | L | Wide-column store backed by DuckDB. Row key ranges and column family filtering are the core operations; the gRPC client is the primary SDK target. |
| Cloud Functions | Medium | L | Execute Python / Node.js function handlers locally in response to HTTP or Pub/Sub triggers. Needs a function registry, a trigger wiring layer, and a sandboxed invocation model. |
| Vertex AI | Medium | XL | See detailed breakdown below. |
| Cloud Endpoints / API Gateway | Low | M | Request validation and routing proxy. Useful for teams building APIs on top of Cloudbox-backed services. |

### Vertex AI — detailed plan

Vertex AI is several distinct sub-products. Each phase is independently shippable.

**Backend strategy:** the emulator supports two selectable backends via `CLOUDBOX_VERTEX_BACKEND`:

- `mock` (default) — return static configurable responses; no external dependency.
- `ollama` — proxy requests to a local [Ollama](https://ollama.com) instance, providing real LLM inference without GCP credentials or billing. Since Ollama, LM Studio, llama.cpp server, and vLLM all expose an OpenAI-compatible API, the proxy is implemented as a generic `OpenAICompatibleBackend`.

Model name mapping is configurable because local model names differ from GCP model names:

```
CLOUDBOX_OLLAMA_HOST=localhost:11434
CLOUDBOX_VERTEX_MODEL_MAP=gemini-2.0-flash:llama3.2,text-embedding-004:nomic-embed-text
```

If Ollama is not reachable, the emulator falls back to mock mode with a warning.

**Phase 1 — Generative AI stub (port 9060) · Effort: M**

| Endpoint | Notes |
|---|---|
| `POST .../publishers/google/models/{model}:generateContent` | Map `contents[].parts[].text` → Ollama `messages[].content`; translate response back to `candidates[]` shape. |
| `POST .../publishers/google/models/{model}:streamGenerateContent` | Re-encode Ollama's newline-delimited chunks into Vertex's streaming schema. |
| `POST .../publishers/google/models/{model}:embedContent` | Ollama `/api/embed` → `{"embedding": {"values": [...]}}`. Works with `nomic-embed-text`, `mxbai-embed-large`. |
| `POST .../publishers/google/models/{model}:countTokens` | Mock mode: configurable count. Ollama mode: approximate with character-based heuristic (Ollama has no tokenizer endpoint). |

Ollama coverage: text generation ✅ · streaming ✅ · multi-turn ✅ · system instructions ✅ · temperature/topP/maxTokens ✅ · safety ratings ❌ (always return empty) · `finishReason: SAFETY` ❌ (always `STOP`).

**Phase 2 — Online prediction endpoints · Effort: M**

| Endpoint | Notes |
|---|---|
| `POST/GET/LIST/DELETE .../endpoints` | Standard NamespacedStore CRUD. Introduces the shared **Long-Running Operation (LRO)** pattern: create/deploy operations return `{ name: ".../operations/{id}", done: true }` immediately. A single `GET .../operations/{id}` route covers all services. |
| `POST .../endpoints/{endpoint}:deployModel` / `:undeployModel` | Update traffic split in store; return LRO. |
| `POST .../endpoints/{endpoint}:predict` | Return configurable mock; in Ollama mode, proxy to the model mapped to this endpoint. |
| `POST .../endpoints/{endpoint}:rawPredict` / `:streamRawPredict` | Pass request body through to Ollama as-is; return response body unchanged. |
| `POST/GET/LIST/DELETE .../models` | Model Registry CRUD via NamespacedStore. |

**Phase 3 — Vector Search · Effort: L**

| Endpoint | Notes |
|---|---|
| `POST/GET/LIST/DELETE .../indexes` | Index CRUD; store dimension count, distance metric, and datapoints in NamespacedStore. |
| `POST .../indexes/{index}:upsertDatapoints` | Store `{datapointId → featureVector}` entries. |
| `POST .../indexes/{index}:removeDatapoints` | Delete by ID. |
| `POST/GET/LIST/DELETE .../indexEndpoints` | Index endpoint CRUD + deployIndex/undeployIndex via LRO. |
| `POST .../indexEndpoints/{ep}:findNeighbors` | Brute-force similarity search over stored vectors. Support cosine, dot-product, and L2 distance. Apply `restricts` / `numericRestricts` filters before ranking. Fast enough for local test datasets (thousands of vectors). |

**Phase 4 — Everything else · Effort: XL · Low priority**

Training pipelines, AutoML, Feature Store, Model Monitoring, Workbench — these involve long-running distributed compute and GCS-backed artifacts. Not practically emulatable; stub endpoints that accept calls and return plausible-looking responses are the ceiling here.
| Artifact Registry | Low | M | Basic OCI image push/pull (registry API). Enables local container build pipelines without a real registry. |

---

## Improvements to existing services

Grouped by service, ordered within each group by impact.

### Cloud Storage

| Feature | Impact | Effort | Notes |
|---|:---:|:---:|---|
| Signed URLs (V2 / V4) | High | M | Very common — apps generate signed URLs for client-side uploads and downloads. Can be emulated by issuing time-limited tokens backed by a local HMAC secret. |
| Object versioning | Medium | M | Retain superseded object bodies under their original generation number; expose `?generation=` reads. Generation is already tracked — the main work is storing multiple payloads per object name. |
| XML API compatibility | Medium | M | Some SDKs and tools (e.g. `boto3` with GCS backend) use the S3-compatible XML API. Adds a second surface on the same port. |
| IAM policies (`getIamPolicy` / `setIamPolicy`) | Low | S | Return a plausible no-op response. Unblocks SDK calls that assert on the response structure even in tests. |

### Cloud Pub/Sub

| Feature | Impact | Effort | Notes |
|---|:---:|:---:|---|
| Exactly-once delivery | Medium | M | Track ack IDs server-side to prevent duplicate redelivery within a configurable window. |
| Seek to timestamp (full fidelity) | Low | S | Currently implemented but only approximate. Improve to replay messages from the topic log whose `publishTime` is after the target timestamp. |

### Cloud Firestore

| Feature | Impact | Effort | Notes |
|---|:---:|:---:|---|
| Real-time listeners (`listen` / `on_snapshot`) | High | L | Apps that use `DocumentReference.on_snapshot()` or collection listeners cannot run without this. Requires a long-lived SSE or WebSocket endpoint and a change-notification fan-out layer. |
| Composite index enforcement | Low | S | Reject queries that would require a composite index in production. Helps surface missing index errors locally before deployment. |

### Secret Manager

| Feature | Impact | Effort | Notes |
|---|:---:|:---:|---|
| CMEK via Cloud KMS integration | Medium | S | KMS is already implemented. Wire Secret Manager's `kmsKeyName` field so that secret payloads are encrypted / decrypted via the KMS emulator. Unblocks apps that enforce CMEK in all environments. |
| Rotation notifications (Pub/Sub) | Medium | S | Publish a message to a configured Pub/Sub topic when a new version is added. Unblocks apps that drive key rotation via Pub/Sub push subscriptions. |

### Cloud Tasks

| Feature | Impact | Effort | Notes |
|---|:---:|:---:|---|
| Content-based task deduplication | Medium | S | Hash the task body and reject duplicates within the deduplication window. Currently only exact name collisions are rejected. |
| OIDC / OAuth tokens in dispatch headers | Low | S | Attach a locally-signed JWT to outbound task requests. Unblocks handlers that validate the `Authorization` header. |
| App Engine HTTP tasks | Low | S | Translate App Engine routing config to a local `localhost` URL. |

### Cloud Scheduler

| Feature | Impact | Effort | Notes |
|---|:---:|:---:|---|
| Pub/Sub target | Medium | S | Publish a message to a Pub/Sub topic instead of making an HTTP call. The Pub/Sub emulator is already running; this is wiring only. |
| App Engine target | Low | S | Same as Cloud Tasks — translate App Engine routing to a local URL. |

### Cloud Logging

| Feature | Impact | Effort | Notes |
|---|:---:|:---:|---|
| Complex filter expressions (`AND`, `OR`, `NOT`, nested fields) | High | M | The current filter only handles simple equality / comparison. Many production logging queries use compound expressions. Implementing a small filter parser would unlock most real-world queries. |
| Log views and log buckets | Low | M | Needed for apps that scope log reads to a specific bucket / view. Currently all entries are returned from a single namespace. |

### BigQuery

| Feature | Impact | Effort | Notes |
|---|:---:|:---:|---|
| Multi-statement scripts | Medium | M | Split on `;` and execute statements sequentially, accumulating results. DuckDB handles most individual statements already. |
| Partitioned tables (query pruning) | Medium | M | Store partition metadata and rewrite queries to add `WHERE` clauses that skip irrelevant partitions. The data can remain flat in DuckDB. |
| `MERGE` statement | Medium | M | Common in ETL pipelines. DuckDB supports `INSERT OR REPLACE`; map BigQuery `MERGE` syntax to the appropriate DuckDB form. |
| BigQuery ML stubs (`CREATE MODEL`, `ML.PREDICT`) | Low | L | Return configurable mock predictions. Unblocks apps that call BQML without requiring a real model. |

### Cloud Spanner

| Feature | Impact | Effort | Notes |
|---|:---:|:---:|---|
| Partitioned reads and DML | Medium | M | Used by bulk-read and batch-update patterns. Requires splitting key ranges and returning a partition token that clients exchange for actual reads. |
| Stale reads (`readTimestamp`, `exactStaleness`) | Low | M | Return current data regardless of the requested staleness. Unblocks SDK calls that specify a bounded-staleness read without requiring true MVCC. |

### Cloud KMS

| Feature | Impact | Effort | Notes |
|---|:---:|:---:|---|
| Asymmetric sign and verify (RSA / EC) | Medium | M | Use the `cryptography` library (already a dependency) to generate key pairs per version and implement `asymmetricSign` / `getPublicKey`. Unblocks JWT-signing workflows and apps that use Cloud KMS for code signing. |
| MAC keys (HMAC-SHA256) | Low | S | Straightforward once the asymmetric key generation pattern is established. |

---

## Developer experience

| Feature | Impact | Effort | Notes |
|---|:---:|:---:|---|
| Admin UI: live log tail | High | M | Stream log entries to the browser via Server-Sent Events. The most common debugging action is watching logs in real time. |
| Admin UI: data import / export | High | M | Download a snapshot of all service state as JSON; re-upload it to seed a fresh instance. Dramatically speeds up reproducible bug reports. |
| Admin UI: per-service reset button in each panel | Medium | S | Currently only available from the overview tab. Adding it to each individual panel reduces context-switching. |
| `gcloudlocal` CLI: KMS, Logging, Spanner, Bigtable commands | Medium | M | Expand `gcloudlocal` to cover the services added after the initial release. |
| Docker Compose profiles | Medium | S | Allow starting only a subset of services via `--profile storage`, `--profile messaging`, etc. Reduces resource usage in projects that only need a few services. |
| SDK compat smoke tests for all services | Medium | M | `sdk_compat/test_with_sdk.py` currently covers only the original set. Extend it to cover Spanner, Logging, Scheduler, and KMS. |
| OpenTelemetry metrics endpoint | Low | M | Expose a `/metrics` Prometheus scrape endpoint and instrument request counts, latency, and store sizes per service. |
| Config file support (`.cloudbox.toml`) | Low | S | Allow setting project, location, ports, and data dir in a config file instead of environment variables. |

---

## Infrastructure and quality

| Feature | Impact | Effort | Notes |
|---|:---:|:---:|---|
| Restore 80%+ test coverage | High | M | Coverage dropped when new services were added. Audit uncovered lines in `bigquery`, `spanner`, `pubsub`, and `logging` and add targeted tests. |
| Property-based testing (`hypothesis`) | Medium | M | Generate random payloads and query parameters to surface edge cases in filters, SQL rewriting, and store operations. |
| Helm chart / Kubernetes manifest | Medium | M | A single-pod deployment manifest makes Cloudbox easy to include in CI clusters. |
| Benchmark suite | Low | M | Measure throughput (messages/s, queries/s, objects/s) per service. Establishes a baseline and makes regressions visible. |
| GitHub Actions: per-service test matrix | Low | S | Run each service's tests in a separate job so failures are immediately pinpointed without reading through a combined log. |
