"""Cloud KMS — key rotation and version lifecycle.

Demonstrates:
  - Adding a new key version (rotation)
  - Verifying the new version becomes the primary used for encryption
  - Decrypting old ciphertext after rotation (cross-version decryption)
  - Disabling and re-enabling a key version
  - Scheduling a version for destruction

    uv run python examples/kms/key_rotation.py
"""

from __future__ import annotations

import base64
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from examples.shared import KMS_BASE, LOCATION, PROJECT, client, ok

RING_ID = "rotation-ring"
KEY_ID = "rotation-key"
PARENT = f"projects/{PROJECT}/locations/{LOCATION}"
RING_NAME = f"{PARENT}/keyRings/{RING_ID}"
KEY_NAME = f"{RING_NAME}/cryptoKeys/{KEY_ID}"


def main() -> None:
    """Run the key rotation and version lifecycle demonstration."""
    http = client()

    # ── 1. Setup ──────────────────────────────────────────────────────────────
    ok(http.post(f"{KMS_BASE}/v1/{PARENT}/keyRings", params={"keyRingId": RING_ID}, json={}))
    ok(
        http.post(
            f"{KMS_BASE}/v1/{RING_NAME}/cryptoKeys",
            params={"cryptoKeyId": KEY_ID},
            json={"purpose": "ENCRYPT_DECRYPT"},
        )
    )
    print(f"Key created with version 1 as primary: {KEY_NAME}")

    # ── 2. Encrypt with version 1 ─────────────────────────────────────────────
    plaintext = b"sensitive payload - encrypted before rotation"
    r = ok(
        http.post(
            f"{KMS_BASE}/v1/{KEY_NAME}:encrypt",
            json={"plaintext": base64.b64encode(plaintext).decode()},
        )
    )
    old_ciphertext = r.json()["ciphertext"]
    encrypted_by = r.json()["name"]
    print(f"\nEncrypted with: {encrypted_by.split('/')[-1]}")

    # ── 3. Rotate — add version 2 ─────────────────────────────────────────────
    r = ok(http.post(f"{KMS_BASE}/v1/{KEY_NAME}/cryptoKeyVersions", json={}))
    v2_name = r.json()["name"]
    print(f"Added key version: {v2_name.split('/')[-1]}")

    # Confirm version 2 is now primary
    r = ok(http.get(f"{KMS_BASE}/v1/{KEY_NAME}"))
    primary = r.json()["primary"]["name"]
    assert primary == v2_name
    print(f"New primary: {primary.split('/')[-1]}")

    # ── 4. New encryption uses version 2 ──────────────────────────────────────
    r = ok(
        http.post(
            f"{KMS_BASE}/v1/{KEY_NAME}:encrypt",
            json={"plaintext": base64.b64encode(b"new payload").decode()},
        )
    )
    assert r.json()["name"] == v2_name
    print(f"New encryption uses: {r.json()['name'].split('/')[-1]}  OK")

    # ── 5. Old ciphertext still decrypts via version 1 ───────────────────────
    r = ok(
        http.post(
            f"{KMS_BASE}/v1/{KEY_NAME}:decrypt",
            json={"ciphertext": old_ciphertext},
        )
    )
    recovered = base64.b64decode(r.json()["plaintext"])
    assert recovered == plaintext
    print("Old ciphertext decrypted after rotation: OK")

    # ── 6. List all versions ──────────────────────────────────────────────────
    r = ok(http.get(f"{KMS_BASE}/v1/{KEY_NAME}/cryptoKeyVersions"))
    versions = r.json().get("cryptoKeyVersions", [])
    print(f"\nKey versions ({len(versions)} total):")
    for v in versions:
        vid = v["name"].split("/")[-1]
        print(f"  {vid}  state={v['state']}")

    # ── 7. Disable version 1 ─────────────────────────────────────────────────
    v1_name = f"{KEY_NAME}/cryptoKeyVersions/1"
    ok(http.patch(f"{KMS_BASE}/v1/{v1_name}", json={"state": "DISABLED"}))
    print("\nDisabled version 1")

    # Old ciphertext can no longer be decrypted
    r = http.post(f"{KMS_BASE}/v1/{KEY_NAME}:decrypt", json={"ciphertext": old_ciphertext})
    assert r.status_code == 400
    print("Decrypt with disabled version rejected: OK (status 400)")

    # Re-enable version 1
    ok(http.patch(f"{KMS_BASE}/v1/{v1_name}", json={"state": "ENABLED"}))
    r = ok(http.post(f"{KMS_BASE}/v1/{KEY_NAME}:decrypt", json={"ciphertext": old_ciphertext}))
    assert base64.b64decode(r.json()["plaintext"]) == plaintext
    print("Re-enabled version 1 — old ciphertext decrypts again: OK")

    # ── 8. Schedule version 1 for destruction ─────────────────────────────────
    r = ok(http.post(f"{KMS_BASE}/v1/{v1_name}:destroy"))
    assert r.json()["state"] == "DESTROY_SCHEDULED"
    print(f"\nVersion 1 scheduled for destruction (state={r.json()['state']})")

    # Restore it (before the scheduled time elapses)
    r = ok(http.post(f"{KMS_BASE}/v1/{v1_name}:restore"))
    assert r.json()["state"] == "DISABLED"
    print(f"Restored version 1 (state={r.json()['state']})")

    print("\nDone.")


if __name__ == "__main__":
    main()
