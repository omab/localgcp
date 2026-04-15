#!/usr/bin/env python3
"""
gcloudlocal — gcloud-compatible CLI targeting the LocalGCP emulator.

Mirrors gcloud's command structure:

    gcloudlocal [--project PROJECT] [--location LOCATION] [--format json]
                SERVICE RESOURCE VERB [ARGS] [FLAGS]

Services supported:
    storage     buckets list/create/describe/delete
                objects list/describe/delete
                cp SRC DST          (upload or download based on gs:// prefix)

    pubsub      topics list/create/describe/delete/publish
                subscriptions list/create/describe/delete/pull

    secrets     list/create/describe/delete
                versions add/list/access/enable/disable/destroy

    firestore   documents list/get/delete

    tasks       queues list/create/describe/delete/pause/resume/purge
                tasks  list/create/describe/delete/run

Environment variables:
    LOCALGCP_PROJECT         default project  (default: local-project)
    LOCALGCP_LOCATION        default location (default: us-central1)
    LOCALGCP_HOST            emulator host    (default: localhost)
    LOCALGCP_GCS_PORT        GCS port         (default: 4443)
    LOCALGCP_PUBSUB_REST_PORT  Pub/Sub REST   (default: 8086)
    LOCALGCP_FIRESTORE_PORT  Firestore port   (default: 8080)
    LOCALGCP_SECRETMANAGER_PORT  Secret Mgr   (default: 8090)
    LOCALGCP_TASKS_PORT      Cloud Tasks port (default: 8123)
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import httpx

# ---------------------------------------------------------------------------
# Defaults from environment
# ---------------------------------------------------------------------------

_HOST = os.environ.get("LOCALGCP_HOST", "localhost")

_BASES: dict[str, str] = {
    "gcs": f"http://{_HOST}:{os.environ.get('LOCALGCP_GCS_PORT', '4443')}",
    "pubsub": f"http://{_HOST}:{os.environ.get('LOCALGCP_PUBSUB_REST_PORT', '8086')}",
    "firestore": f"http://{_HOST}:{os.environ.get('LOCALGCP_FIRESTORE_PORT', '8080')}",
    "secrets": f"http://{_HOST}:{os.environ.get('LOCALGCP_SECRETMANAGER_PORT', '8090')}",
    "tasks": f"http://{_HOST}:{os.environ.get('LOCALGCP_TASKS_PORT', '8123')}",
}

DEFAULT_PROJECT = os.environ.get("LOCALGCP_PROJECT", "local-project")
DEFAULT_LOCATION = os.environ.get("LOCALGCP_LOCATION", "us-central1")

# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

_output_format = "default"  # overridden by --format


def _print(data: Any) -> None:
    if _output_format == "json":
        print(json.dumps(data, indent=2))
        return
    # default: simple pretty-print
    if isinstance(data, list):
        for item in data:
            _print_item(item)
    elif isinstance(data, dict):
        _print_item(data)
    else:
        print(data)


def _print_item(item: dict) -> None:
    for k, v in item.items():
        if isinstance(v, dict):
            print(f"{k}:")
            for kk, vv in v.items():
                print(f"  {kk}: {vv}")
        else:
            print(f"{k}: {v}")
    print()


def _table(rows: list[dict], columns: list[str]) -> None:
    if _output_format == "json":
        print(json.dumps(rows, indent=2))
        return
    if not rows:
        print("Listed 0 items.")
        return
    widths = {c: len(c) for c in columns}
    for row in rows:
        for c in columns:
            widths[c] = max(widths[c], len(str(row.get(c, ""))))
    header = "  ".join(c.upper().ljust(widths[c]) for c in columns)
    print(header)
    print("  ".join("-" * widths[c] for c in columns))
    for row in rows:
        print("  ".join(str(row.get(c, "")).ljust(widths[c]) for c in columns))


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _client(service: str) -> httpx.Client:
    return httpx.Client(base_url=_BASES[service], timeout=15.0)


def _check(resp: httpx.Response) -> dict:
    if resp.status_code >= 400:
        try:
            err = resp.json()
        except Exception:
            err = {"error": {"message": resp.text}}
        msg = err.get("error", {}).get("message", resp.text)
        print(f"ERROR {resp.status_code}: {msg}", file=sys.stderr)
        sys.exit(1)
    if resp.status_code == 204 or not resp.content:
        return {}
    return resp.json()


def _gs_parse(uri: str) -> tuple[str, str]:
    """Parse gs://bucket/object → (bucket, object_name). Object may be empty."""
    if not uri.startswith("gs://"):
        raise SystemExit(f"Expected a gs:// URI, got: {uri}")
    rest = uri[5:]
    if "/" in rest:
        bucket, obj = rest.split("/", 1)
        return bucket, obj
    return rest, ""


# ---------------------------------------------------------------------------
# storage
# ---------------------------------------------------------------------------

def _storage_buckets_list(project: str, **_) -> None:
    with _client("gcs") as c:
        data = _check(c.get("/storage/v1/b", params={"project": project}))
    items = data.get("items", [])
    _table([{"name": b["name"], "location": b.get("location", ""), "created": b.get("timeCreated", "")} for b in items],
           ["name", "location", "created"])


def _storage_buckets_create(project: str, bucket: str, region: str, **_) -> None:
    with _client("gcs") as c:
        data = _check(c.post("/storage/v1/b", params={"project": project},
                             json={"name": bucket, "location": region}))
    print(f"Created bucket gs://{data.get('name', bucket)}.")


def _storage_buckets_describe(bucket: str, **_) -> None:
    with _client("gcs") as c:
        data = _check(c.get(f"/storage/v1/b/{bucket}"))
    _print(data)


def _storage_buckets_delete(bucket: str, **_) -> None:
    with _client("gcs") as c:
        _check(c.delete(f"/storage/v1/b/{bucket}"))
    print(f"Deleted bucket gs://{bucket}.")


def _storage_objects_list(uri: str, **_) -> None:
    bucket, prefix = _gs_parse(uri)
    params = {}
    if prefix:
        params["prefix"] = prefix
    with _client("gcs") as c:
        data = _check(c.get(f"/storage/v1/b/{bucket}/o", params=params))
    items = data.get("items", [])
    _table([{"name": o["name"], "size": o.get("size", ""), "updated": o.get("updated", "")} for o in items],
           ["name", "size", "updated"])


def _storage_objects_describe(uri: str, **_) -> None:
    bucket, obj = _gs_parse(uri)
    with _client("gcs") as c:
        data = _check(c.get(f"/storage/v1/b/{bucket}/o/{obj}"))
    _print(data)


def _storage_objects_delete(uri: str, **_) -> None:
    bucket, obj = _gs_parse(uri)
    with _client("gcs") as c:
        _check(c.delete(f"/storage/v1/b/{bucket}/o/{obj}"))
    print(f"Deleted {uri}.")


def _storage_notifications_list(bucket: str, **_) -> None:
    bucket_name, _ = _gs_parse(bucket)
    with _client("gcs") as c:
        data = _check(c.get(f"/storage/v1/b/{bucket_name}/notificationConfigs"))
    items = data.get("items", [])
    _table(
        [{"id": n.get("id", ""), "topic": n.get("topic", ""),
          "event_types": ",".join(n.get("event_types") or ["ALL"]),
          "payload_format": n.get("payload_format", "")} for n in items],
        ["id", "topic", "event_types", "payload_format"],
    )


def _storage_notifications_create(
    project: str, bucket: str, topic: str,
    event_types: list[str], payload_format: str, **_
) -> None:
    bucket_name, _ = _gs_parse(bucket)
    topic_path = topic if "/" in topic else f"projects/{project}/topics/{topic}"
    body: dict[str, Any] = {"topic": topic_path, "payload_format": payload_format}
    if event_types:
        body["event_types"] = event_types
    with _client("gcs") as c:
        data = _check(c.post(f"/storage/v1/b/{bucket_name}/notificationConfigs", json=body))
    print(f"Created notification id={data.get('id', '?')} on gs://{bucket_name}.")


def _storage_notifications_delete(bucket: str, notification_id: str, **_) -> None:
    bucket_name, _ = _gs_parse(bucket)
    with _client("gcs") as c:
        _check(c.delete(f"/storage/v1/b/{bucket_name}/notificationConfigs/{notification_id}"))
    print(f"Deleted notification {notification_id} from gs://{bucket_name}.")


def _storage_cp(src: str, dst: str, **_) -> None:
    src_is_gcs = src.startswith("gs://")
    dst_is_gcs = dst.startswith("gs://")

    if src_is_gcs and not dst_is_gcs:
        # download
        bucket, obj = _gs_parse(src)
        dst_path = Path(dst)
        with _client("gcs") as c:
            resp = c.get(f"/storage/v1/b/{bucket}/o/{obj}", params={"alt": "media"})
            if resp.status_code >= 400:
                _check(resp)
        dst_path.write_bytes(resp.content)
        print(f"Downloaded {src} → {dst_path}")

    elif not src_is_gcs and dst_is_gcs:
        # upload
        src_path = Path(src)
        if not src_path.exists():
            raise SystemExit(f"File not found: {src_path}")
        bucket, blob = _gs_parse(dst)
        if not blob:
            blob = src_path.name
        with _client("gcs") as c:
            _check(c.post(
                f"/upload/storage/v1/b/{bucket}/o",
                params={"name": blob, "uploadType": "media"},
                content=src_path.read_bytes(),
                headers={"Content-Type": "application/octet-stream"},
            ))
        print(f"Uploaded {src_path} → gs://{bucket}/{blob}")

    elif not src_is_gcs and not dst_is_gcs:
        raise SystemExit("At least one of SRC or DST must be a gs:// URI.")
    else:
        # gcs → gcs copy
        src_bucket, src_obj = _gs_parse(src)
        dst_bucket, dst_obj = _gs_parse(dst)
        if not dst_obj:
            dst_obj = src_obj
        with _client("gcs") as c:
            _check(c.post(f"/storage/v1/b/{src_bucket}/o/{src_obj}/copyTo/b/{dst_bucket}/o/{dst_obj}"))
        print(f"Copied {src} → {dst}")


# ---------------------------------------------------------------------------
# pubsub topics
# ---------------------------------------------------------------------------

def _pubsub_topics_list(project: str, **_) -> None:
    with _client("pubsub") as c:
        data = _check(c.get(f"/v1/projects/{project}/topics"))
    topics = data.get("topics", [])
    _table([{"name": t["name"]} for t in topics], ["name"])


def _pubsub_topics_create(project: str, topic: str, **_) -> None:
    with _client("pubsub") as c:
        data = _check(c.put(f"/v1/projects/{project}/topics/{topic}"))
    print(f"Created topic {data.get('name', topic)}.")


def _pubsub_topics_describe(project: str, topic: str, **_) -> None:
    with _client("pubsub") as c:
        data = _check(c.get(f"/v1/projects/{project}/topics/{topic}"))
    _print(data)


def _pubsub_topics_delete(project: str, topic: str, **_) -> None:
    with _client("pubsub") as c:
        _check(c.delete(f"/v1/projects/{project}/topics/{topic}"))
    print(f"Deleted topic projects/{project}/topics/{topic}.")


def _pubsub_topics_publish(project: str, topic: str, message: str, attributes: list[str], **_) -> None:
    encoded = base64.b64encode(message.encode()).decode()
    msg: dict[str, Any] = {"data": encoded}
    if attributes:
        attrs = {}
        for attr in attributes:
            k, _, v = attr.partition("=")
            attrs[k.strip()] = v.strip()
        msg["attributes"] = attrs
    with _client("pubsub") as c:
        data = _check(c.post(f"/v1/projects/{project}/topics/{topic}:publish",
                             json={"messages": [msg]}))
    ids = data.get("messageIds", [])
    print(f"Published message ID: {ids[0] if ids else '?'}")


# ---------------------------------------------------------------------------
# pubsub subscriptions
# ---------------------------------------------------------------------------

def _pubsub_subs_list(project: str, **_) -> None:
    with _client("pubsub") as c:
        data = _check(c.get(f"/v1/projects/{project}/subscriptions"))
    subs = data.get("subscriptions", [])
    _table([{"name": s["name"], "topic": s.get("topic", "")} for s in subs],
           ["name", "topic"])


def _pubsub_subs_create(project: str, subscription: str, topic: str, **_) -> None:
    topic_path = topic if "/" in topic else f"projects/{project}/topics/{topic}"
    sub_path = f"projects/{project}/subscriptions/{subscription}"
    with _client("pubsub") as c:
        data = _check(c.put(f"/v1/projects/{project}/subscriptions/{subscription}",
                             json={"name": sub_path, "topic": topic_path}))
    print(f"Created subscription {data.get('name', subscription)}.")


def _pubsub_subs_describe(project: str, subscription: str, **_) -> None:
    with _client("pubsub") as c:
        data = _check(c.get(f"/v1/projects/{project}/subscriptions/{subscription}"))
    _print(data)


def _pubsub_subs_delete(project: str, subscription: str, **_) -> None:
    with _client("pubsub") as c:
        _check(c.delete(f"/v1/projects/{project}/subscriptions/{subscription}"))
    print(f"Deleted subscription projects/{project}/subscriptions/{subscription}.")


def _pubsub_subs_pull(project: str, subscription: str, max_messages: int, follow: bool, auto_ack: bool, **_) -> None:
    sub_path = f"/v1/projects/{project}/subscriptions/{subscription}"

    def _pull_once() -> int:
        with _client("pubsub") as c:
            data = _check(c.post(f"{sub_path}:pull",
                                 json={"maxMessages": max_messages}))
        received = data.get("receivedMessages", [])
        if not received:
            return 0
        for rm in received:
            msg = rm["message"]
            raw = base64.b64decode(msg.get("data", ""))
            try:
                body = json.loads(raw.decode("utf-8"))
                body_str = json.dumps(body, indent=2)
            except (json.JSONDecodeError, UnicodeDecodeError):
                try:
                    body_str = raw.decode("utf-8")
                except UnicodeDecodeError:
                    body_str = base64.b64encode(raw).decode()
            attrs = msg.get("attributes", {})
            print(f"--- id={msg['messageId']}  publishTime={msg.get('publishTime', '?')} ---")
            if attrs:
                print(f"attributes: {json.dumps(attrs)}")
            print(body_str)
            print()
        if auto_ack:
            ack_ids = [rm["ackId"] for rm in received]
            with _client("pubsub") as c:
                _check(c.post(f"{sub_path}:acknowledge", json={"ackIds": ack_ids}))
        return len(received)

    if follow:
        print(f"Pulling from projects/{project}/subscriptions/{subscription} (Ctrl-C to stop)…\n")
        try:
            while True:
                count = _pull_once()
                if count == 0:
                    time.sleep(1)
        except KeyboardInterrupt:
            print("\nStopped.")
    else:
        count = _pull_once()
        if count == 0:
            print("No messages available.")
        else:
            print(f"Received {count} message(s).")


# ---------------------------------------------------------------------------
# secrets
# ---------------------------------------------------------------------------

def _secrets_list(project: str, **_) -> None:
    with _client("secrets") as c:
        data = _check(c.get(f"/v1/projects/{project}/secrets"))
    secrets = data.get("secrets", [])
    _table([{"name": s["name"], "createTime": s.get("createTime", "")} for s in secrets],
           ["name", "createTime"])


def _secrets_create(project: str, secret: str, **_) -> None:
    with _client("secrets") as c:
        data = _check(c.post(f"/v1/projects/{project}/secrets",
                             params={"secretId": secret},
                             json={"replication": {"automatic": {}}}))
    print(f"Created secret {data.get('name', secret)}.")


def _secrets_describe(project: str, secret: str, **_) -> None:
    with _client("secrets") as c:
        data = _check(c.get(f"/v1/projects/{project}/secrets/{secret}"))
    _print(data)


def _secrets_delete(project: str, secret: str, **_) -> None:
    with _client("secrets") as c:
        _check(c.delete(f"/v1/projects/{project}/secrets/{secret}"))
    print(f"Deleted secret projects/{project}/secrets/{secret}.")


def _secrets_versions_add(project: str, secret: str, data_value: str | None,
                          data_file: str | None, **_) -> None:
    if data_file:
        p = Path(data_file)
        if data_file == "-":
            raw = sys.stdin.buffer.read()
        elif not p.exists():
            raise SystemExit(f"File not found: {data_file}")
        else:
            raw = p.read_bytes()
    elif data_value is not None:
        raw = data_value.encode()
    else:
        raise SystemExit("Provide --data or --data-file.")
    encoded = base64.b64encode(raw).decode()
    with _client("secrets") as c:
        data = _check(c.post(f"/v1/projects/{project}/secrets/{secret}:addVersion",
                             json={"payload": {"data": encoded}}))
    print(f"Created version {data.get('name', '?')}.")


def _secrets_versions_list(project: str, secret: str, **_) -> None:
    with _client("secrets") as c:
        data = _check(c.get(f"/v1/projects/{project}/secrets/{secret}/versions"))
    versions = data.get("versions", [])
    _table([{"name": v["name"], "state": v.get("state", ""), "createTime": v.get("createTime", "")}
            for v in versions], ["name", "state", "createTime"])


def _secrets_versions_access(project: str, secret: str, version: str, **_) -> None:
    with _client("secrets") as c:
        data = _check(c.post(
            f"/v1/projects/{project}/secrets/{secret}/versions/{version}:access"))
    payload_b64 = data.get("payload", {}).get("data", "")
    try:
        print(base64.b64decode(payload_b64).decode("utf-8"))
    except UnicodeDecodeError:
        print(payload_b64)  # raw base64 if binary


def _secrets_versions_action(project: str, secret: str, version: str, action: str, **_) -> None:
    with _client("secrets") as c:
        data = _check(c.post(
            f"/v1/projects/{project}/secrets/{secret}/versions/{version}:{action}"))
    print(f"{action.capitalize()}d version {data.get('name', version)}.")


# ---------------------------------------------------------------------------
# firestore
# ---------------------------------------------------------------------------

def _firestore_documents_list(project: str, collection: str, database: str, **_) -> None:
    with _client("firestore") as c:
        data = _check(c.get(
            f"/v1/projects/{project}/databases/{database}/documents/{collection}"))
    docs = data.get("documents", [])
    _table([{"name": d["name"], "updateTime": d.get("updateTime", "")} for d in docs],
           ["name", "updateTime"])


def _firestore_documents_get(project: str, path: str, database: str, **_) -> None:
    with _client("firestore") as c:
        data = _check(c.get(
            f"/v1/projects/{project}/databases/{database}/documents/{path}"))
    _print(data)


def _firestore_documents_delete(project: str, path: str, database: str, **_) -> None:
    with _client("firestore") as c:
        _check(c.delete(
            f"/v1/projects/{project}/databases/{database}/documents/{path}"))
    print(f"Deleted document {path}.")


# ---------------------------------------------------------------------------
# tasks queues
# ---------------------------------------------------------------------------

def _tasks_queues_list(project: str, location: str, **_) -> None:
    with _client("tasks") as c:
        data = _check(c.get(f"/v2/projects/{project}/locations/{location}/queues"))
    queues = data.get("queues", [])
    _table([{"name": q["name"], "state": q.get("state", "")} for q in queues],
           ["name", "state"])


def _tasks_queues_create(project: str, location: str, queue: str, **_) -> None:
    with _client("tasks") as c:
        data = _check(c.post(f"/v2/projects/{project}/locations/{location}/queues",
                             json={"name": f"projects/{project}/locations/{location}/queues/{queue}"}))
    print(f"Created queue {data.get('name', queue)}.")


def _tasks_queues_describe(project: str, location: str, queue: str, **_) -> None:
    with _client("tasks") as c:
        data = _check(c.get(f"/v2/projects/{project}/locations/{location}/queues/{queue}"))
    _print(data)


def _tasks_queues_delete(project: str, location: str, queue: str, **_) -> None:
    with _client("tasks") as c:
        _check(c.delete(f"/v2/projects/{project}/locations/{location}/queues/{queue}"))
    print(f"Deleted queue {queue}.")


def _tasks_queues_action(project: str, location: str, queue: str, action: str, **_) -> None:
    with _client("tasks") as c:
        data = _check(c.post(
            f"/v2/projects/{project}/locations/{location}/queues/{queue}:{action}"))
    print(f"{action.capitalize()}d queue {data.get('name', queue)}.")


# ---------------------------------------------------------------------------
# tasks tasks
# ---------------------------------------------------------------------------

def _tasks_tasks_list(project: str, location: str, queue: str, **_) -> None:
    with _client("tasks") as c:
        data = _check(c.get(
            f"/v2/projects/{project}/locations/{location}/queues/{queue}/tasks"))
    tasks = data.get("tasks", [])
    _table([{"name": t["name"], "scheduleTime": t.get("scheduleTime", ""),
             "createTime": t.get("createTime", "")} for t in tasks],
           ["name", "scheduleTime", "createTime"])


def _tasks_tasks_create(project: str, location: str, queue: str,
                        url: str, method: str, body: str | None, **_) -> None:
    http_req: dict[str, Any] = {"url": url, "httpMethod": method.upper()}
    if body:
        http_req["body"] = base64.b64encode(body.encode()).decode()
    with _client("tasks") as c:
        data = _check(c.post(
            f"/v2/projects/{project}/locations/{location}/queues/{queue}/tasks",
            json={"task": {"httpRequest": http_req}}))
    print(f"Created task {data.get('name', '?')}.")


def _tasks_tasks_describe(project: str, location: str, queue: str, task: str, **_) -> None:
    with _client("tasks") as c:
        data = _check(c.get(
            f"/v2/projects/{project}/locations/{location}/queues/{queue}/tasks/{task}"))
    _print(data)


def _tasks_tasks_delete(project: str, location: str, queue: str, task: str, **_) -> None:
    with _client("tasks") as c:
        _check(c.delete(
            f"/v2/projects/{project}/locations/{location}/queues/{queue}/tasks/{task}"))
    print(f"Deleted task {task}.")


def _tasks_tasks_run(project: str, location: str, queue: str, task: str, **_) -> None:
    with _client("tasks") as c:
        data = _check(c.post(
            f"/v2/projects/{project}/locations/{location}/queues/{queue}/tasks/{task}:run"))
    print(f"Dispatched task {data.get('name', task)}.")


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        prog="gcloudlocal",
        description="gcloud-compatible CLI for the LocalGCP emulator.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    root.add_argument("--project", default=DEFAULT_PROJECT, help="GCP project ID")
    root.add_argument("--location", default=DEFAULT_LOCATION, help="GCP location/region")
    root.add_argument("--format", choices=["default", "json"], default="default",
                      dest="output_format", help="Output format")

    svc = root.add_subparsers(dest="service", metavar="SERVICE")
    svc.required = True

    # --- storage ---
    p_storage = svc.add_parser("storage", help="Cloud Storage")
    p_storage_sub = p_storage.add_subparsers(dest="resource", metavar="RESOURCE")
    p_storage_sub.required = True

    # storage buckets
    p_buckets = p_storage_sub.add_parser("buckets", help="Manage buckets")
    p_buckets_sub = p_buckets.add_subparsers(dest="verb", metavar="VERB")
    p_buckets_sub.required = True
    p_buckets_sub.add_parser("list", help="List buckets")
    p_bc = p_buckets_sub.add_parser("create", help="Create a bucket")
    p_bc.add_argument("bucket", help="Bucket name")
    p_bc.add_argument("--region", default=DEFAULT_LOCATION, help="Bucket region")
    p_bd = p_buckets_sub.add_parser("describe", help="Describe a bucket")
    p_bd.add_argument("bucket", help="Bucket name")
    p_bdel = p_buckets_sub.add_parser("delete", help="Delete a bucket")
    p_bdel.add_argument("bucket", help="Bucket name")

    # storage objects
    p_objects = p_storage_sub.add_parser("objects", help="Manage objects")
    p_objects_sub = p_objects.add_subparsers(dest="verb", metavar="VERB")
    p_objects_sub.required = True
    p_ol = p_objects_sub.add_parser("list", help="List objects in a bucket")
    p_ol.add_argument("uri", metavar="gs://BUCKET[/PREFIX]")
    p_od = p_objects_sub.add_parser("describe", help="Describe an object")
    p_od.add_argument("uri", metavar="gs://BUCKET/OBJECT")
    p_odel = p_objects_sub.add_parser("delete", help="Delete an object")
    p_odel.add_argument("uri", metavar="gs://BUCKET/OBJECT")

    # storage notifications
    p_notifs = p_storage_sub.add_parser("notifications", help="Manage bucket notification configs")
    p_notifs_sub = p_notifs.add_subparsers(dest="verb", metavar="VERB")
    p_notifs_sub.required = True
    p_nl = p_notifs_sub.add_parser("list", help="List notification configs on a bucket")
    p_nl.add_argument("bucket", metavar="gs://BUCKET")
    p_nc = p_notifs_sub.add_parser("create", help="Create a notification config")
    p_nc.add_argument("bucket", metavar="gs://BUCKET")
    p_nc.add_argument("--topic", required=True, help="Pub/Sub topic name or full path")
    p_nc.add_argument("--event-types", dest="event_types", nargs="+", default=[],
                      metavar="TYPE",
                      help="Event types to notify on (default: all). "
                           "E.g. OBJECT_FINALIZE OBJECT_DELETE")
    p_nc.add_argument("--payload-format", dest="payload_format", default="JSON_API_V1",
                      choices=["JSON_API_V1", "NONE"], help="Message payload format")
    p_nd = p_notifs_sub.add_parser("delete", help="Delete a notification config")
    p_nd.add_argument("bucket", metavar="gs://BUCKET")
    p_nd.add_argument("notification_id", help="Notification config ID")

    # storage cp
    p_cp = p_storage_sub.add_parser("cp", help="Copy files (upload/download/gcs-to-gcs)")
    p_cp.add_argument("src", metavar="SRC")
    p_cp.add_argument("dst", metavar="DST")

    # --- pubsub ---
    p_pubsub = svc.add_parser("pubsub", help="Cloud Pub/Sub")
    p_pubsub_sub = p_pubsub.add_subparsers(dest="resource", metavar="RESOURCE")
    p_pubsub_sub.required = True

    # pubsub topics
    p_topics = p_pubsub_sub.add_parser("topics", help="Manage topics")
    p_topics_sub = p_topics.add_subparsers(dest="verb", metavar="VERB")
    p_topics_sub.required = True
    p_topics_sub.add_parser("list", help="List topics")
    p_tc = p_topics_sub.add_parser("create", help="Create a topic")
    p_tc.add_argument("topic", help="Topic name (short or full path)")
    p_tdes = p_topics_sub.add_parser("describe", help="Describe a topic")
    p_tdes.add_argument("topic")
    p_tdel = p_topics_sub.add_parser("delete", help="Delete a topic")
    p_tdel.add_argument("topic")
    p_tpub = p_topics_sub.add_parser("publish", help="Publish a message to a topic")
    p_tpub.add_argument("topic")
    p_tpub.add_argument("--message", required=True, help="Message payload (UTF-8 text)")
    p_tpub.add_argument("--attribute", dest="attributes", action="append", default=[],
                        metavar="KEY=VALUE", help="Message attribute (repeatable)")

    # pubsub subscriptions
    p_subs = p_pubsub_sub.add_parser("subscriptions", help="Manage subscriptions")
    p_subs_sub = p_subs.add_subparsers(dest="verb", metavar="VERB")
    p_subs_sub.required = True
    p_subs_sub.add_parser("list", help="List subscriptions")
    p_sc = p_subs_sub.add_parser("create", help="Create a subscription")
    p_sc.add_argument("subscription", help="Subscription name")
    p_sc.add_argument("--topic", required=True, help="Topic name or full path")
    p_sdes = p_subs_sub.add_parser("describe", help="Describe a subscription")
    p_sdes.add_argument("subscription")
    p_sdel = p_subs_sub.add_parser("delete", help="Delete a subscription")
    p_sdel.add_argument("subscription")
    p_spull = p_subs_sub.add_parser("pull", help="Pull messages from a subscription")
    p_spull.add_argument("subscription")
    p_spull.add_argument("--max-messages", type=int, default=10)
    p_spull.add_argument("--follow", action="store_true", default=False,
                         help="Keep pulling until Ctrl-C")
    p_spull.add_argument("--no-auto-ack", dest="auto_ack", action="store_false", default=True,
                         help="Do not acknowledge pulled messages")

    # --- secrets ---
    p_secrets = svc.add_parser("secrets", help="Secret Manager")
    p_secrets_sub = p_secrets.add_subparsers(dest="resource", metavar="RESOURCE")
    p_secrets_sub.required = True

    # secrets (top-level verbs)
    for verb, help_text in [("list", "List secrets"), ("create", "Create a secret"),
                             ("describe", "Describe a secret"), ("delete", "Delete a secret")]:
        ps = p_secrets_sub.add_parser(verb, help=help_text)
        if verb in ("create", "describe", "delete"):
            ps.add_argument("secret", help="Secret ID")

    # secrets versions
    p_sv = p_secrets_sub.add_parser("versions", help="Manage secret versions")
    p_sv_sub = p_sv.add_subparsers(dest="verb", metavar="VERB")
    p_sv_sub.required = True

    p_svadd = p_sv_sub.add_parser("add", help="Add a new version")
    p_svadd.add_argument("secret", help="Secret ID")
    p_svadd.add_argument("--data", dest="data_value", default=None, help="Secret value as string")
    p_svadd.add_argument("--data-file", default=None, help="File to read secret value from (- for stdin)")

    p_svlist = p_sv_sub.add_parser("list", help="List versions of a secret")
    p_svlist.add_argument("secret")

    p_svaccess = p_sv_sub.add_parser("access", help="Access (read) a secret version value")
    p_svaccess.add_argument("version", help="Version ID or 'latest'")
    p_svaccess.add_argument("--secret", required=True, dest="secret", help="Secret ID")

    for verb in ("enable", "disable", "destroy"):
        pv = p_sv_sub.add_parser(verb, help=f"{verb.capitalize()} a secret version")
        pv.add_argument("version", help="Version ID")
        pv.add_argument("--secret", required=True, dest="secret")

    # --- firestore ---
    p_fs = svc.add_parser("firestore", help="Cloud Firestore")
    p_fs_sub = p_fs.add_subparsers(dest="resource", metavar="RESOURCE")
    p_fs_sub.required = True
    p_fs.set_defaults(database="(default)")

    p_fsdocs = p_fs_sub.add_parser("documents", help="Manage documents")
    p_fsdocs_sub = p_fsdocs.add_subparsers(dest="verb", metavar="VERB")
    p_fsdocs_sub.required = True
    p_fsdocs.add_argument("--database", default="(default)", help="Firestore database ID")

    p_fslist = p_fsdocs_sub.add_parser("list", help="List documents in a collection")
    p_fslist.add_argument("collection", help="Collection path")
    p_fsget = p_fsdocs_sub.add_parser("get", help="Get a document")
    p_fsget.add_argument("path", help="Document path (collection/doc_id)")
    p_fsdel = p_fsdocs_sub.add_parser("delete", help="Delete a document")
    p_fsdel.add_argument("path", help="Document path (collection/doc_id)")

    # --- tasks ---
    p_tasks = svc.add_parser("tasks", help="Cloud Tasks")
    p_tasks_sub = p_tasks.add_subparsers(dest="resource", metavar="RESOURCE")
    p_tasks_sub.required = True

    # tasks queues
    p_tq = p_tasks_sub.add_parser("queues", help="Manage queues")
    p_tq_sub = p_tq.add_subparsers(dest="verb", metavar="VERB")
    p_tq_sub.required = True
    p_tq_sub.add_parser("list", help="List queues")
    p_tqc = p_tq_sub.add_parser("create", help="Create a queue")
    p_tqc.add_argument("queue", help="Queue ID")
    p_tqd = p_tq_sub.add_parser("describe", help="Describe a queue")
    p_tqd.add_argument("queue")
    p_tqdel = p_tq_sub.add_parser("delete", help="Delete a queue")
    p_tqdel.add_argument("queue")
    for verb in ("pause", "resume", "purge"):
        pv = p_tq_sub.add_parser(verb, help=f"{verb.capitalize()} a queue")
        pv.add_argument("queue")

    # tasks tasks
    p_tt = p_tasks_sub.add_parser("tasks", help="Manage tasks")
    p_tt_sub = p_tt.add_subparsers(dest="verb", metavar="VERB")
    p_tt_sub.required = True
    p_ttlist = p_tt_sub.add_parser("list", help="List tasks in a queue")
    p_ttlist.add_argument("queue")
    p_ttc = p_tt_sub.add_parser("create", help="Create a task")
    p_ttc.add_argument("queue")
    p_ttc.add_argument("--url", required=True, help="HTTP target URL")
    p_ttc.add_argument("--method", default="POST", help="HTTP method")
    p_ttc.add_argument("--body", default=None, help="Request body")
    p_ttdes = p_tt_sub.add_parser("describe", help="Describe a task")
    p_ttdes.add_argument("queue")
    p_ttdes.add_argument("task")
    p_ttdel = p_tt_sub.add_parser("delete", help="Delete a task")
    p_ttdel.add_argument("queue")
    p_ttdel.add_argument("task")
    p_ttrun = p_tt_sub.add_parser("run", help="Force-run a task")
    p_ttrun.add_argument("queue")
    p_ttrun.add_argument("task")

    return root


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

_DISPATCH = {
    # storage
    ("storage", "buckets", "list"):     _storage_buckets_list,
    ("storage", "buckets", "create"):   _storage_buckets_create,
    ("storage", "buckets", "describe"): _storage_buckets_describe,
    ("storage", "buckets", "delete"):   _storage_buckets_delete,
    ("storage", "objects", "list"):     _storage_objects_list,
    ("storage", "objects", "describe"): _storage_objects_describe,
    ("storage", "objects", "delete"):   _storage_objects_delete,
    ("storage", "notifications", "list"):   _storage_notifications_list,
    ("storage", "notifications", "create"): _storage_notifications_create,
    ("storage", "notifications", "delete"): _storage_notifications_delete,
    ("storage", "cp", None):            _storage_cp,
    # pubsub topics
    ("pubsub", "topics", "list"):       _pubsub_topics_list,
    ("pubsub", "topics", "create"):     _pubsub_topics_create,
    ("pubsub", "topics", "describe"):   _pubsub_topics_describe,
    ("pubsub", "topics", "delete"):     _pubsub_topics_delete,
    ("pubsub", "topics", "publish"):    _pubsub_topics_publish,
    # pubsub subscriptions
    ("pubsub", "subscriptions", "list"):     _pubsub_subs_list,
    ("pubsub", "subscriptions", "create"):   _pubsub_subs_create,
    ("pubsub", "subscriptions", "describe"): _pubsub_subs_describe,
    ("pubsub", "subscriptions", "delete"):   _pubsub_subs_delete,
    ("pubsub", "subscriptions", "pull"):     _pubsub_subs_pull,
    # secrets top-level
    ("secrets", "list", None):     _secrets_list,
    ("secrets", "create", None):   _secrets_create,
    ("secrets", "describe", None): _secrets_describe,
    ("secrets", "delete", None):   _secrets_delete,
    # secrets versions
    ("secrets", "versions", "add"):     _secrets_versions_add,
    ("secrets", "versions", "list"):    _secrets_versions_list,
    ("secrets", "versions", "access"):  _secrets_versions_access,
    ("secrets", "versions", "enable"):  lambda **kw: _secrets_versions_action(action="enable", **kw),
    ("secrets", "versions", "disable"): lambda **kw: _secrets_versions_action(action="disable", **kw),
    ("secrets", "versions", "destroy"): lambda **kw: _secrets_versions_action(action="destroy", **kw),
    # firestore
    ("firestore", "documents", "list"):   _firestore_documents_list,
    ("firestore", "documents", "get"):    _firestore_documents_get,
    ("firestore", "documents", "delete"): _firestore_documents_delete,
    # tasks queues
    ("tasks", "queues", "list"):     _tasks_queues_list,
    ("tasks", "queues", "create"):   _tasks_queues_create,
    ("tasks", "queues", "describe"): _tasks_queues_describe,
    ("tasks", "queues", "delete"):   _tasks_queues_delete,
    ("tasks", "queues", "pause"):    lambda **kw: _tasks_queues_action(action="pause", **kw),
    ("tasks", "queues", "resume"):   lambda **kw: _tasks_queues_action(action="resume", **kw),
    ("tasks", "queues", "purge"):    lambda **kw: _tasks_queues_action(action="purge", **kw),
    # tasks tasks
    ("tasks", "tasks", "list"):     _tasks_tasks_list,
    ("tasks", "tasks", "create"):   _tasks_tasks_create,
    ("tasks", "tasks", "describe"): _tasks_tasks_describe,
    ("tasks", "tasks", "delete"):   _tasks_tasks_delete,
    ("tasks", "tasks", "run"):      _tasks_tasks_run,
}


def main() -> None:
    global _output_format

    parser = _build_parser()
    args = parser.parse_args()
    _output_format = args.output_format

    kwargs = vars(args)

    service = kwargs.get("service")
    resource = kwargs.get("resource")
    verb = kwargs.get("verb")

    # secrets top-level verbs (list/create/describe/delete) have no nested verb
    if service == "secrets" and resource in ("list", "create", "describe", "delete"):
        resource, verb = resource, None

    # storage cp has no verb
    if service == "storage" and resource == "cp":
        verb = None

    key = (service, resource, verb)
    fn = _DISPATCH.get(key)
    if fn is None:
        parser.print_help()
        sys.exit(1)

    fn(**kwargs)


if __name__ == "__main__":
    main()
