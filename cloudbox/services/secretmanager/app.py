"""Secret Manager emulator.

Implements the Secret Manager REST API v1 used by google-cloud-secret-manager.
"""

from __future__ import annotations

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse

from cloudbox.core.errors import GCPError, add_gcp_exception_handler
from cloudbox.core.middleware import add_request_logging
from cloudbox.services.secretmanager.models import (
    AccessSecretVersionResponse,
    AddVersionRequest,
    ListSecretsResponse,
    ListSecretVersionsResponse,
    SecretModel,
    SecretVersionModel,
    SecretVersionState,
    _now,
)
from cloudbox.services.secretmanager.store import get_store

app = FastAPI(title="Cloudbox — Secret Manager", version="v1")
add_gcp_exception_handler(app)
add_request_logging(app, "secretmanager")


def _store():
    return get_store()


def _version_number(secret_name: str) -> int:
    """Return the next version number for a secret."""
    store = _store()
    existing = [k for k in store.keys("versions") if k.startswith(f"{secret_name}/versions/")]
    nums = []
    for k in existing:
        try:
            nums.append(int(k.split("/versions/")[1]))
        except (ValueError, IndexError):
            pass
    return max(nums, default=0) + 1


def _resolve_version(secret_name: str, version_id: str) -> str | None:
    """Resolve 'latest' or a numeric version to the canonical version key."""
    store = _store()
    if version_id == "latest":
        candidates = [k for k in store.keys("versions") if k.startswith(f"{secret_name}/versions/")]
        enabled = []
        for k in candidates:
            v = store.get("versions", k)
            if v and v.get("state") == SecretVersionState.ENABLED:
                try:
                    enabled.append((int(k.split("/versions/")[1]), k))
                except (ValueError, IndexError):
                    pass
        if not enabled:
            return None
        return max(enabled)[1]
    else:
        key = f"{secret_name}/versions/{version_id}"
        return key if store.exists("versions", key) else None


# ---------------------------------------------------------------------------
# Secrets
# ---------------------------------------------------------------------------


@app.post("/v1/projects/{project}/secrets")
async def create_secret(project: str, request: Request):
    body = await request.json()
    secret_id = request.query_params.get("secretId", body.get("secretId", ""))
    if not secret_id:
        raise GCPError(400, "secretId is required")

    name = f"projects/{project}/secrets/{secret_id}"
    store = _store()
    if store.exists("secrets", name):
        raise GCPError(409, f"Secret {name} already exists.")

    secret = SecretModel(
        name=name,
        labels=body.get("labels", {}),
    )
    store.set("secrets", name, secret.model_dump())
    return JSONResponse(status_code=200, content=secret.model_dump())


@app.get("/v1/projects/{project}/secrets")
async def list_secrets(
    project: str,
    pageSize: int = Query(default=25),
    pageToken: str = Query(default=""),
):
    store = _store()
    prefix = f"projects/{project}/secrets/"
    all_secrets = [SecretModel(**v) for v in store.list("secrets") if v["name"].startswith(prefix)]
    all_secrets.sort(key=lambda s: s.name)
    offset = int(pageToken) if pageToken else 0
    page = all_secrets[offset : offset + pageSize]
    next_token = str(offset + pageSize) if offset + pageSize < len(all_secrets) else None
    return ListSecretsResponse(
        secrets=page, nextPageToken=next_token, totalSize=len(all_secrets)
    ).model_dump(exclude_none=True)


@app.get("/v1/projects/{project}/secrets/{secret_id}")
async def get_secret(project: str, secret_id: str):
    name = f"projects/{project}/secrets/{secret_id}"
    store = _store()
    data = store.get("secrets", name)
    if data is None:
        raise GCPError(404, f"Secret {name} not found.")
    return data


@app.patch("/v1/projects/{project}/secrets/{secret_id}")
async def update_secret(project: str, secret_id: str, request: Request):
    name = f"projects/{project}/secrets/{secret_id}"
    store = _store()
    data = store.get("secrets", name)
    if data is None:
        raise GCPError(404, f"Secret {name} not found.")
    body = await request.json()
    if "labels" in body:
        data["labels"] = body["labels"]
    store.set("secrets", name, data)
    return data


@app.delete("/v1/projects/{project}/secrets/{secret_id}", status_code=200)
async def delete_secret(project: str, secret_id: str):
    name = f"projects/{project}/secrets/{secret_id}"
    store = _store()
    if not store.exists("secrets", name):
        raise GCPError(404, f"Secret {name} not found.")
    store.delete("secrets", name)
    # Delete all versions and payloads
    for k in list(store.keys("versions")):
        if k.startswith(f"{name}/versions/"):
            store.delete("versions", k)
            store.delete("payloads", k)
    return {}


# ---------------------------------------------------------------------------
# Secret Versions
# ---------------------------------------------------------------------------


@app.post("/v1/projects/{project}/secrets/{secret_id}:addVersion")
async def add_version(project: str, secret_id: str, body: AddVersionRequest):
    secret_name = f"projects/{project}/secrets/{secret_id}"
    store = _store()
    if not store.exists("secrets", secret_name):
        raise GCPError(404, f"Secret {secret_name} not found.")

    n = _version_number(secret_name)
    version_name = f"{secret_name}/versions/{n}"

    version = SecretVersionModel(name=version_name)
    store.set("versions", version_name, version.model_dump())
    store.set("payloads", version_name, body.payload.get("data", ""))

    return version.model_dump()


@app.get("/v1/projects/{project}/secrets/{secret_id}/versions")
async def list_versions(
    project: str,
    secret_id: str,
    pageSize: int = Query(default=25),
    pageToken: str = Query(default=""),
    filter: str = Query(default=""),
):
    secret_name = f"projects/{project}/secrets/{secret_id}"
    store = _store()
    if not store.exists("secrets", secret_name):
        raise GCPError(404, f"Secret {secret_name} not found.")

    prefix = f"{secret_name}/versions/"
    all_versions = [
        SecretVersionModel(**v)
        for k, v in [(k, store.get("versions", k)) for k in store.keys("versions")]
        if k.startswith(prefix) and v
    ]
    all_versions.sort(key=lambda v: v.name)

    if filter:
        state_filter = filter.upper().replace("STATE=", "").strip()
        all_versions = [v for v in all_versions if v.state == state_filter]

    offset = int(pageToken) if pageToken else 0
    page = all_versions[offset : offset + pageSize]
    next_token = str(offset + pageSize) if offset + pageSize < len(all_versions) else None

    return ListSecretVersionsResponse(
        versions=page, nextPageToken=next_token, totalSize=len(all_versions)
    ).model_dump(exclude_none=True)


@app.get("/v1/projects/{project}/secrets/{secret_id}/versions/{version_id}")
async def get_version(project: str, secret_id: str, version_id: str):
    secret_name = f"projects/{project}/secrets/{secret_id}"
    store = _store()
    version_key = _resolve_version(secret_name, version_id)
    if version_key is None:
        raise GCPError(404, f"Version {version_id} not found for secret {secret_name}.")
    return store.get("versions", version_key)


@app.post("/v1/projects/{project}/secrets/{secret_id}/versions/{version_id}:access")
async def access_version(project: str, secret_id: str, version_id: str):
    secret_name = f"projects/{project}/secrets/{secret_id}"
    store = _store()
    version_key = _resolve_version(secret_name, version_id)
    if version_key is None:
        raise GCPError(404, f"Version {version_id} not found for secret {secret_name}.")

    version_data = store.get("versions", version_key)
    if version_data and version_data.get("state") != SecretVersionState.ENABLED:
        raise GCPError(403, f"Secret version {version_key} is not enabled.")

    data = store.get("payloads", version_key) or ""
    return AccessSecretVersionResponse(
        name=version_key,
        payload={"data": data},
    ).model_dump()


@app.post("/v1/projects/{project}/secrets/{secret_id}/versions/{version_id}:disable")
async def disable_version(project: str, secret_id: str, version_id: str):
    return _set_version_state(project, secret_id, version_id, SecretVersionState.DISABLED)


@app.post("/v1/projects/{project}/secrets/{secret_id}/versions/{version_id}:enable")
async def enable_version(project: str, secret_id: str, version_id: str):
    return _set_version_state(project, secret_id, version_id, SecretVersionState.ENABLED)


@app.post("/v1/projects/{project}/secrets/{secret_id}/versions/{version_id}:destroy")
async def destroy_version(project: str, secret_id: str, version_id: str):
    result = _set_version_state(project, secret_id, version_id, SecretVersionState.DESTROYED)
    # Wipe payload
    secret_name = f"projects/{project}/secrets/{secret_id}"
    store = _store()
    version_key = f"{secret_name}/versions/{version_id}"
    store.delete("payloads", version_key)
    return result


def _set_version_state(project: str, secret_id: str, version_id: str, state: str):
    secret_name = f"projects/{project}/secrets/{secret_id}"
    store = _store()
    version_key = _resolve_version(secret_name, version_id)
    if version_key is None:
        raise GCPError(404, f"Version {version_id} not found for secret {secret_name}.")
    data = store.get("versions", version_key)
    data["state"] = state
    if state == SecretVersionState.DESTROYED:
        data["destroyTime"] = _now()
    store.set("versions", version_key, data)
    return data
