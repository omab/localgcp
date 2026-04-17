"""Secret Manager — create a secret, add versions, access and disable them.

    uv run python examples/secretmanager/secrets.py
"""
import sys
import os
import base64
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from examples.shared import SECRETMANAGER_BASE, PROJECT, client, ok

SECRET_ID = "my-api-key"


def main():
    http = client()

    parent = f"projects/{PROJECT}"
    secret_name = f"{parent}/secrets/{SECRET_ID}"

    # Create secret
    ok(http.post(
        f"{SECRETMANAGER_BASE}/v1/{parent}/secrets",
        params={"secretId": SECRET_ID},
        json={"replication": {"automatic": {}}},
    ))
    print(f"Created secret: {secret_name}")

    # Add version 1
    r = ok(http.post(
        f"{SECRETMANAGER_BASE}/v1/{secret_name}:addVersion",
        json={"payload": {"data": base64.b64encode(b"super-secret-v1").decode()}},
    ))
    v1_name = r.json()["name"]
    print(f"Added version: {v1_name}")

    # Add version 2
    r = ok(http.post(
        f"{SECRETMANAGER_BASE}/v1/{secret_name}:addVersion",
        json={"payload": {"data": base64.b64encode(b"super-secret-v2").decode()}},
    ))
    v2_name = r.json()["name"]
    print(f"Added version: {v2_name}")

    # Access latest version
    r = ok(http.post(f"{SECRETMANAGER_BASE}/v1/{secret_name}/versions/latest:access"))
    value = base64.b64decode(r.json()["payload"]["data"]).decode()
    print(f"Latest value: {value!r}")
    assert value == "super-secret-v2"

    # Access version 1 explicitly
    r = ok(http.post(f"{SECRETMANAGER_BASE}/v1/{v1_name}:access"))
    value = base64.b64decode(r.json()["payload"]["data"]).decode()
    print(f"Version 1 value: {value!r}")
    assert value == "super-secret-v1"

    # List versions
    r = ok(http.get(f"{SECRETMANAGER_BASE}/v1/{secret_name}/versions"))
    versions = r.json().get("versions", [])
    print(f"Versions: {[v['name'].split('/')[-1] for v in versions]}")

    # Disable version 1
    ok(http.post(f"{SECRETMANAGER_BASE}/v1/{v1_name}:disable"))
    r = http.post(f"{SECRETMANAGER_BASE}/v1/{v1_name}:access")
    print(f"Access disabled version: status={r.status_code}")  # 400/404

    # Cleanup
    http.delete(f"{SECRETMANAGER_BASE}/v1/{secret_name}")
    print("Deleted secret")


if __name__ == "__main__":
    main()
