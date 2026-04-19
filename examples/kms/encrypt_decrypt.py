"""Cloud KMS — encrypt and decrypt data using a symmetric key.

Demonstrates:
  - Creating a key ring and crypto key
  - Encrypting plaintext with the primary key version
  - Decrypting ciphertext back to plaintext
  - Verifying that ciphertext produced by one key cannot be decrypted by another
  - Using additional authenticated data (AAD) for context binding

    uv run python examples/kms/encrypt_decrypt.py
"""

from __future__ import annotations

import base64
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from examples.shared import KMS_BASE, LOCATION, PROJECT, client, ok

RING_ID = "example-ring"
KEY_ID = "example-key"
PARENT = f"projects/{PROJECT}/locations/{LOCATION}"
RING_NAME = f"{PARENT}/keyRings/{RING_ID}"
KEY_NAME = f"{RING_NAME}/cryptoKeys/{KEY_ID}"


def main() -> None:
    """Run the encrypt/decrypt demonstration."""
    http = client()

    # ── 1. Create key ring ────────────────────────────────────────────────────
    ok(http.post(f"{KMS_BASE}/v1/{PARENT}/keyRings", params={"keyRingId": RING_ID}, json={}))
    print(f"Created key ring: {RING_NAME}")

    # ── 2. Create symmetric ENCRYPT_DECRYPT key ───────────────────────────────
    r = ok(
        http.post(
            f"{KMS_BASE}/v1/{RING_NAME}/cryptoKeys",
            params={"cryptoKeyId": KEY_ID},
            json={"purpose": "ENCRYPT_DECRYPT"},
        )
    )
    primary = r.json()["primary"]["name"]
    print(f"Created crypto key: {KEY_NAME}")
    print(f"  Primary version: {primary}")

    # ── 3. Encrypt plaintext ──────────────────────────────────────────────────
    plaintext = b"Hello, Cloud KMS!"
    plaintext_b64 = base64.b64encode(plaintext).decode()

    r = ok(
        http.post(
            f"{KMS_BASE}/v1/{KEY_NAME}:encrypt",
            json={"plaintext": plaintext_b64},
        )
    )
    ciphertext = r.json()["ciphertext"]
    print(f"\nPlaintext : {plaintext!r}")
    print(f"Ciphertext: {ciphertext[:40]}… ({len(ciphertext)} chars)")

    # ── 4. Decrypt ciphertext ─────────────────────────────────────────────────
    r = ok(
        http.post(
            f"{KMS_BASE}/v1/{KEY_NAME}:decrypt",
            json={"ciphertext": ciphertext},
        )
    )
    recovered = base64.b64decode(r.json()["plaintext"])
    print(f"Decrypted : {recovered!r}")
    assert recovered == plaintext, "Decrypted value does not match original"
    print("  OK: round-trip successful")

    # ── 5. Additional Authenticated Data (AAD) ────────────────────────────────
    aad = base64.b64encode(b"user-id:42").decode()

    r = ok(
        http.post(
            f"{KMS_BASE}/v1/{KEY_NAME}:encrypt",
            json={"plaintext": plaintext_b64, "additionalAuthenticatedData": aad},
        )
    )
    ct_with_aad = r.json()["ciphertext"]

    # Decrypt with correct AAD
    r = ok(
        http.post(
            f"{KMS_BASE}/v1/{KEY_NAME}:decrypt",
            json={"ciphertext": ct_with_aad, "additionalAuthenticatedData": aad},
        )
    )
    assert base64.b64decode(r.json()["plaintext"]) == plaintext
    print("\nAAD round-trip: OK")

    # Decrypt with wrong AAD must fail
    wrong_aad = base64.b64encode(b"user-id:99").decode()
    r = http.post(
        f"{KMS_BASE}/v1/{KEY_NAME}:decrypt",
        json={"ciphertext": ct_with_aad, "additionalAuthenticatedData": wrong_aad},
    )
    assert r.status_code == 400
    print("AAD mismatch rejected: OK (status 400)")

    # ── 6. Ciphertext is key-specific ─────────────────────────────────────────
    other_key_id = "other-key"
    other_key_name = f"{RING_NAME}/cryptoKeys/{other_key_id}"
    ok(
        http.post(
            f"{KMS_BASE}/v1/{RING_NAME}/cryptoKeys",
            params={"cryptoKeyId": other_key_id},
            json={"purpose": "ENCRYPT_DECRYPT"},
        )
    )
    r = http.post(
        f"{KMS_BASE}/v1/{other_key_name}:decrypt",
        json={"ciphertext": ciphertext},
    )
    assert r.status_code == 400
    print("\nCross-key decrypt rejected: OK (status 400)")

    # ── 7. Cleanup ────────────────────────────────────────────────────────────
    print("\nDone. Key ring and keys remain (no delete API in KMS).")


if __name__ == "__main__":
    main()
