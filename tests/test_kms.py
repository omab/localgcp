"""Tests for Cloud KMS emulator."""

import base64

PROJECT = "local-project"
LOCATION = "us-central1"
BASE = f"/v1/projects/{PROJECT}/locations/{LOCATION}"
RBASE = f"projects/{PROJECT}/locations/{LOCATION}"


def _ring(client, ring_id="my-ring"):
    return client.post(f"{BASE}/keyRings", params={"keyRingId": ring_id}, json={})


def _key(client, ring_id="my-ring", key_id="my-key", purpose="ENCRYPT_DECRYPT"):
    return client.post(
        f"{BASE}/keyRings/{ring_id}/cryptoKeys",
        params={"cryptoKeyId": key_id},
        json={"purpose": purpose},
    )


# ---------------------------------------------------------------------------
# KeyRing tests
# ---------------------------------------------------------------------------


def test_create_and_get_key_ring(kms_client):
    r = _ring(kms_client)
    assert r.status_code == 200
    assert r.json()["name"] == f"{RBASE}/keyRings/my-ring"

    r = kms_client.get(f"{BASE}/keyRings/my-ring")
    assert r.status_code == 200
    assert "createTime" in r.json()


def test_duplicate_key_ring_returns_409(kms_client):
    _ring(kms_client)
    r = _ring(kms_client)
    assert r.status_code == 409


def test_list_key_rings(kms_client):
    _ring(kms_client, "ring-a")
    _ring(kms_client, "ring-b")
    r = kms_client.get(f"{BASE}/keyRings")
    assert r.status_code == 200
    names = [x["name"] for x in r.json()["keyRings"]]
    assert f"{RBASE}/keyRings/ring-a" in names
    assert f"{RBASE}/keyRings/ring-b" in names


def test_get_missing_key_ring_returns_404(kms_client):
    r = kms_client.get(f"{BASE}/keyRings/no-such-ring")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# CryptoKey tests
# ---------------------------------------------------------------------------


def test_create_and_get_crypto_key(kms_client):
    _ring(kms_client)
    r = _key(kms_client)
    assert r.status_code == 200
    body = r.json()
    assert body["name"] == f"{RBASE}/keyRings/my-ring/cryptoKeys/my-key"
    assert body["purpose"] == "ENCRYPT_DECRYPT"
    assert body["primary"]["name"].endswith("/cryptoKeyVersions/1")
    assert body["primary"]["state"] == "ENABLED"


def test_duplicate_crypto_key_returns_409(kms_client):
    _ring(kms_client)
    _key(kms_client)
    r = _key(kms_client)
    assert r.status_code == 409


def test_list_crypto_keys(kms_client):
    _ring(kms_client)
    _key(kms_client, key_id="key-a")
    _key(kms_client, key_id="key-b")
    r = kms_client.get(f"{BASE}/keyRings/my-ring/cryptoKeys")
    assert r.status_code == 200
    names = [x["name"] for x in r.json()["cryptoKeys"]]
    assert f"{RBASE}/keyRings/my-ring/cryptoKeys/key-a" in names
    assert f"{RBASE}/keyRings/my-ring/cryptoKeys/key-b" in names


def test_update_crypto_key_labels(kms_client):
    _ring(kms_client)
    _key(kms_client)
    r = kms_client.patch(
        f"{BASE}/keyRings/my-ring/cryptoKeys/my-key",
        json={"labels": {"env": "test"}},
    )
    assert r.status_code == 200
    assert r.json()["labels"] == {"env": "test"}


def test_create_key_on_missing_ring_returns_404(kms_client):
    r = _key(kms_client, ring_id="ghost-ring")
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# Encrypt / Decrypt tests
# ---------------------------------------------------------------------------


def test_encrypt_and_decrypt(kms_client):
    _ring(kms_client)
    _key(kms_client)

    plaintext = base64.b64encode(b"hello cloudbox kms").decode()
    r = kms_client.post(
        f"{BASE}/keyRings/my-ring/cryptoKeys/my-key:encrypt",
        json={"plaintext": plaintext},
    )
    assert r.status_code == 200
    ciphertext = r.json()["ciphertext"]
    assert ciphertext != plaintext

    r = kms_client.post(
        f"{BASE}/keyRings/my-ring/cryptoKeys/my-key:decrypt",
        json={"ciphertext": ciphertext},
    )
    assert r.status_code == 200
    assert r.json()["plaintext"] == plaintext


def test_encrypt_decrypt_with_aad(kms_client):
    _ring(kms_client)
    _key(kms_client)

    plaintext = base64.b64encode(b"secret with context").decode()
    aad = base64.b64encode(b"my-context").decode()

    r = kms_client.post(
        f"{BASE}/keyRings/my-ring/cryptoKeys/my-key:encrypt",
        json={"plaintext": plaintext, "additionalAuthenticatedData": aad},
    )
    assert r.status_code == 200
    ciphertext = r.json()["ciphertext"]

    # Correct AAD decrypts successfully
    r = kms_client.post(
        f"{BASE}/keyRings/my-ring/cryptoKeys/my-key:decrypt",
        json={"ciphertext": ciphertext, "additionalAuthenticatedData": aad},
    )
    assert r.status_code == 200
    assert r.json()["plaintext"] == plaintext

    # Wrong AAD fails
    wrong_aad = base64.b64encode(b"wrong-context").decode()
    r = kms_client.post(
        f"{BASE}/keyRings/my-ring/cryptoKeys/my-key:decrypt",
        json={"ciphertext": ciphertext, "additionalAuthenticatedData": wrong_aad},
    )
    assert r.status_code == 400


def test_decrypt_wrong_key_returns_400(kms_client):
    _ring(kms_client)
    _key(kms_client, key_id="key-a")
    _key(kms_client, key_id="key-b")

    plaintext = base64.b64encode(b"data").decode()
    r = kms_client.post(
        f"{BASE}/keyRings/my-ring/cryptoKeys/key-a:encrypt",
        json={"plaintext": plaintext},
    )
    ciphertext = r.json()["ciphertext"]

    r = kms_client.post(
        f"{BASE}/keyRings/my-ring/cryptoKeys/key-b:decrypt",
        json={"ciphertext": ciphertext},
    )
    assert r.status_code == 400


def test_encrypt_missing_key_returns_404(kms_client):
    _ring(kms_client)
    r = kms_client.post(
        f"{BASE}/keyRings/my-ring/cryptoKeys/ghost:encrypt",
        json={"plaintext": base64.b64encode(b"x").decode()},
    )
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# CryptoKeyVersion tests
# ---------------------------------------------------------------------------


def test_create_new_version(kms_client):
    _ring(kms_client)
    _key(kms_client)
    r = kms_client.post(
        f"{BASE}/keyRings/my-ring/cryptoKeys/my-key/cryptoKeyVersions",
        json={},
    )
    assert r.status_code == 200
    assert r.json()["name"].endswith("/cryptoKeyVersions/2")
    assert r.json()["state"] == "ENABLED"


def test_list_versions(kms_client):
    _ring(kms_client)
    _key(kms_client)
    kms_client.post(f"{BASE}/keyRings/my-ring/cryptoKeys/my-key/cryptoKeyVersions", json={})
    r = kms_client.get(f"{BASE}/keyRings/my-ring/cryptoKeys/my-key/cryptoKeyVersions")
    assert r.status_code == 200
    assert r.json()["totalSize"] == 2


def test_disable_and_encrypt_fails(kms_client):
    _ring(kms_client)
    _key(kms_client)

    # Disable version 1
    kms_client.patch(
        f"{BASE}/keyRings/my-ring/cryptoKeys/my-key/cryptoKeyVersions/1",
        json={"state": "DISABLED"},
    )

    # primary is now None → encrypt should fail
    r = kms_client.post(
        f"{BASE}/keyRings/my-ring/cryptoKeys/my-key:encrypt",
        json={"plaintext": base64.b64encode(b"data").decode()},
    )
    assert r.status_code == 400


def test_new_version_becomes_primary(kms_client):
    _ring(kms_client)
    _key(kms_client)
    kms_client.post(f"{BASE}/keyRings/my-ring/cryptoKeys/my-key/cryptoKeyVersions", json={})

    r = kms_client.get(f"{BASE}/keyRings/my-ring/cryptoKeys/my-key")
    assert r.json()["primary"]["name"].endswith("/cryptoKeyVersions/2")


def test_destroy_and_restore_version(kms_client):
    _ring(kms_client)
    _key(kms_client)

    r = kms_client.post(f"{BASE}/keyRings/my-ring/cryptoKeys/my-key/cryptoKeyVersions/1:destroy")
    assert r.status_code == 200
    assert r.json()["state"] == "DESTROY_SCHEDULED"

    r = kms_client.post(f"{BASE}/keyRings/my-ring/cryptoKeys/my-key/cryptoKeyVersions/1:restore")
    assert r.status_code == 200
    assert r.json()["state"] == "DISABLED"


def test_decrypt_with_rotated_key(kms_client):
    """Ciphertext encrypted by old primary can still be decrypted after rotation."""
    _ring(kms_client)
    _key(kms_client)

    plaintext = base64.b64encode(b"rotate me").decode()
    r = kms_client.post(
        f"{BASE}/keyRings/my-ring/cryptoKeys/my-key:encrypt",
        json={"plaintext": plaintext},
    )
    ciphertext = r.json()["ciphertext"]

    # Add version 2 — becomes new primary
    kms_client.post(f"{BASE}/keyRings/my-ring/cryptoKeys/my-key/cryptoKeyVersions", json={})

    # Old ciphertext (encrypted with v1 key) should still decrypt
    r = kms_client.post(
        f"{BASE}/keyRings/my-ring/cryptoKeys/my-key:decrypt",
        json={"ciphertext": ciphertext},
    )
    assert r.status_code == 200
    assert r.json()["plaintext"] == plaintext


def test_asymmetric_get_public_key_basic(kms_client):
    _ring(kms_client)
    _key(kms_client, purpose="ASYMMETRIC_SIGN")
    r = kms_client.get(f"{BASE}/keyRings/my-ring/cryptoKeys/my-key/cryptoKeyVersions/1/publicKey")
    assert r.status_code == 200
    assert "BEGIN PUBLIC KEY" in r.json()["pem"]


def test_asymmetric_sign_basic(kms_client):
    _ring(kms_client)
    _key(kms_client, purpose="ASYMMETRIC_SIGN")
    r = kms_client.post(
        f"{BASE}/keyRings/my-ring/cryptoKeys/my-key/cryptoKeyVersions/1:asymmetricSign",
        json={"digest": {"sha256": base64.b64encode(b"x" * 32).decode()}},
    )
    assert r.status_code == 200
    assert len(base64.b64decode(r.json()["signature"])) > 0


def test_asymmetric_decrypt_invalid_returns_400(kms_client):
    _ring(kms_client)
    _key(kms_client, purpose="ASYMMETRIC_DECRYPT")
    r = kms_client.post(
        f"{BASE}/keyRings/my-ring/cryptoKeys/my-key/cryptoKeyVersions/1:asymmetricDecrypt",
        json={"ciphertext": base64.b64encode(b"data").decode()},
    )
    assert r.status_code == 400


def test_get_missing_crypto_key_returns_404(kms_client):
    _ring(kms_client)
    r = kms_client.get(f"{BASE}/keyRings/my-ring/cryptoKeys/no-such-key")
    assert r.status_code == 404


def test_get_crypto_key_version_by_id(kms_client):
    _ring(kms_client)
    _key(kms_client)
    r = kms_client.get(f"{BASE}/keyRings/my-ring/cryptoKeys/my-key/cryptoKeyVersions/1")
    assert r.status_code == 200
    assert r.json()["state"] == "ENABLED"
    assert r.json()["name"].endswith("/cryptoKeyVersions/1")


def test_get_missing_version_returns_404(kms_client):
    _ring(kms_client)
    _key(kms_client)
    r = kms_client.get(f"{BASE}/keyRings/my-ring/cryptoKeys/my-key/cryptoKeyVersions/99")
    assert r.status_code == 404


def test_reenable_disabled_version(kms_client):
    """PATCH state=ENABLED re-enables a disabled version so encrypt works again."""
    _ring(kms_client)
    _key(kms_client)

    # Disable version 1
    kms_client.patch(
        f"{BASE}/keyRings/my-ring/cryptoKeys/my-key/cryptoKeyVersions/1",
        json={"state": "DISABLED"},
    )
    # Re-enable it
    r = kms_client.patch(
        f"{BASE}/keyRings/my-ring/cryptoKeys/my-key/cryptoKeyVersions/1",
        json={"state": "ENABLED"},
    )
    assert r.status_code == 200
    assert r.json()["state"] == "ENABLED"

    # Encrypt should now succeed again
    r2 = kms_client.post(
        f"{BASE}/keyRings/my-ring/cryptoKeys/my-key:encrypt",
        json={"plaintext": base64.b64encode(b"data").decode()},
    )
    assert r2.status_code == 200


def test_list_versions_filter_by_state(kms_client):
    _ring(kms_client)
    _key(kms_client)
    # Create a second version, then disable version 1
    kms_client.post(f"{BASE}/keyRings/my-ring/cryptoKeys/my-key/cryptoKeyVersions", json={})
    kms_client.patch(
        f"{BASE}/keyRings/my-ring/cryptoKeys/my-key/cryptoKeyVersions/1",
        json={"state": "DISABLED"},
    )

    r_enabled = kms_client.get(
        f"{BASE}/keyRings/my-ring/cryptoKeys/my-key/cryptoKeyVersions?filter=state=ENABLED"
    )
    assert r_enabled.status_code == 200
    assert all(v["state"] == "ENABLED" for v in r_enabled.json()["cryptoKeyVersions"])
    assert len(r_enabled.json()["cryptoKeyVersions"]) == 1

    r_disabled = kms_client.get(
        f"{BASE}/keyRings/my-ring/cryptoKeys/my-key/cryptoKeyVersions?filter=state=DISABLED"
    )
    assert len(r_disabled.json()["cryptoKeyVersions"]) == 1


def test_decrypt_with_destroyed_version_returns_400(kms_client):
    """Decrypting with a ciphertext from a destroyed version raises 400."""
    _ring(kms_client)
    _key(kms_client)

    plaintext = base64.b64encode(b"secret data").decode()
    enc = kms_client.post(
        f"{BASE}/keyRings/my-ring/cryptoKeys/my-key:encrypt",
        json={"plaintext": plaintext},
    )
    ciphertext = enc.json()["ciphertext"]

    # Destroy the version that encrypted it
    kms_client.post(f"{BASE}/keyRings/my-ring/cryptoKeys/my-key/cryptoKeyVersions/1:destroy")

    r = kms_client.post(
        f"{BASE}/keyRings/my-ring/cryptoKeys/my-key:decrypt",
        json={"ciphertext": ciphertext},
    )
    assert r.status_code == 400


def test_create_version_on_missing_key_returns_404(kms_client):
    _ring(kms_client)
    r = kms_client.post(
        f"{BASE}/keyRings/my-ring/cryptoKeys/ghost-key/cryptoKeyVersions",
        json={},
    )
    assert r.status_code == 404


def test_destroy_missing_version_returns_404(kms_client):
    _ring(kms_client)
    _key(kms_client)
    r = kms_client.post(f"{BASE}/keyRings/my-ring/cryptoKeys/my-key/cryptoKeyVersions/99:destroy")
    assert r.status_code == 404


def test_restore_non_destroy_scheduled_returns_400(kms_client):
    """Restoring an ENABLED version (not DESTROY_SCHEDULED) returns 400."""
    _ring(kms_client)
    _key(kms_client)
    r = kms_client.post(f"{BASE}/keyRings/my-ring/cryptoKeys/my-key/cryptoKeyVersions/1:restore")
    assert r.status_code == 400


# ---------------------------------------------------------------------------
# Asymmetric sign / verify / decrypt tests
# ---------------------------------------------------------------------------


def _asym_key(client, ring_id="asym-ring", key_id="ec-key", algorithm="EC_SIGN_P256_SHA256"):
    client.post(f"{BASE}/keyRings", params={"keyRingId": ring_id}, json={})
    return client.post(
        f"{BASE}/keyRings/{ring_id}/cryptoKeys",
        params={"cryptoKeyId": key_id},
        json={
            "purpose": "ASYMMETRIC_SIGN",
            "versionTemplate": {"algorithm": algorithm},
        },
    )


def test_get_public_key_ec(kms_client):
    _asym_key(kms_client)
    r = kms_client.get(f"{BASE}/keyRings/asym-ring/cryptoKeys/ec-key/cryptoKeyVersions/1/publicKey")
    assert r.status_code == 200
    data = r.json()
    assert "BEGIN PUBLIC KEY" in data["pem"]
    assert data["algorithm"] == "EC_SIGN_P256_SHA256"


def test_get_public_key_p384(kms_client):
    _asym_key(kms_client, key_id="ec384-key", algorithm="EC_SIGN_P384_SHA384")
    r = kms_client.get(
        f"{BASE}/keyRings/asym-ring/cryptoKeys/ec384-key/cryptoKeyVersions/1/publicKey"
    )
    assert r.status_code == 200
    assert "BEGIN PUBLIC KEY" in r.json()["pem"]


def test_asymmetric_sign_ec(kms_client):
    import hashlib

    _asym_key(kms_client)
    message = b"hello asymmetric world"
    digest = base64.b64encode(hashlib.sha256(message).digest()).decode()

    r = kms_client.post(
        f"{BASE}/keyRings/asym-ring/cryptoKeys/ec-key/cryptoKeyVersions/1:asymmetricSign",
        json={"digest": {"sha256": digest}},
    )
    assert r.status_code == 200
    sig_b64 = r.json()["signature"]
    assert len(base64.b64decode(sig_b64)) > 0


def test_asymmetric_sign_verify_roundtrip(kms_client):
    """Sign a digest and verify the signature using the returned public key."""
    import hashlib

    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec, utils

    _asym_key(kms_client)
    message = b"roundtrip verification"
    digest_bytes = hashlib.sha256(message).digest()
    digest_b64 = base64.b64encode(digest_bytes).decode()

    # Sign
    sign_r = kms_client.post(
        f"{BASE}/keyRings/asym-ring/cryptoKeys/ec-key/cryptoKeyVersions/1:asymmetricSign",
        json={"digest": {"sha256": digest_b64}},
    )
    assert sign_r.status_code == 200
    signature = base64.b64decode(sign_r.json()["signature"])

    # Get public key
    pub_r = kms_client.get(
        f"{BASE}/keyRings/asym-ring/cryptoKeys/ec-key/cryptoKeyVersions/1/publicKey"
    )
    pub_pem = pub_r.json()["pem"].encode()
    public_key = serialization.load_pem_public_key(pub_pem)

    # Verify using Prehashed since we signed a pre-computed digest, not the raw message
    public_key.verify(signature, digest_bytes, ec.ECDSA(utils.Prehashed(hashes.SHA256())))


def test_asymmetric_sign_missing_digest_returns_400(kms_client):
    _asym_key(kms_client)
    r = kms_client.post(
        f"{BASE}/keyRings/asym-ring/cryptoKeys/ec-key/cryptoKeyVersions/1:asymmetricSign",
        json={"digest": {}},
    )
    assert r.status_code == 400


def test_get_public_key_not_found_returns_404(kms_client):
    _asym_key(kms_client)
    r = kms_client.get(
        f"{BASE}/keyRings/asym-ring/cryptoKeys/ec-key/cryptoKeyVersions/99/publicKey"
    )
    assert r.status_code == 404


def test_asymmetric_decrypt_rsa(kms_client):
    """Create an RSA ASYMMETRIC_DECRYPT key and round-trip encrypt/decrypt."""
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding as asym_padding

    kms_client.post(f"{BASE}/keyRings", params={"keyRingId": "rsa-ring"}, json={})
    kms_client.post(
        f"{BASE}/keyRings/rsa-ring/cryptoKeys",
        params={"cryptoKeyId": "rsa-key"},
        json={
            "purpose": "ASYMMETRIC_DECRYPT",
            "versionTemplate": {"algorithm": "RSA_DECRYPT_OAEP_2048_SHA256"},
        },
    )

    # Get public key
    pub_r = kms_client.get(
        f"{BASE}/keyRings/rsa-ring/cryptoKeys/rsa-key/cryptoKeyVersions/1/publicKey"
    )
    assert pub_r.status_code == 200
    pub_pem = pub_r.json()["pem"].encode()
    public_key = serialization.load_pem_public_key(pub_pem)

    # Encrypt with the public key
    plaintext = b"secret rsa message"
    ciphertext = public_key.encrypt(
        plaintext,
        asym_padding.OAEP(
            mgf=asym_padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None,
        ),
    )

    # Decrypt via KMS
    r = kms_client.post(
        f"{BASE}/keyRings/rsa-ring/cryptoKeys/rsa-key/cryptoKeyVersions/1:asymmetricDecrypt",
        json={"ciphertext": base64.b64encode(ciphertext).decode()},
    )
    assert r.status_code == 200
    recovered = base64.b64decode(r.json()["plaintext"])
    assert recovered == plaintext


def test_asymmetric_decrypt_invalid_ciphertext_returns_400(kms_client):
    kms_client.post(f"{BASE}/keyRings", params={"keyRingId": "rsa-ring2"}, json={})
    kms_client.post(
        f"{BASE}/keyRings/rsa-ring2/cryptoKeys",
        params={"cryptoKeyId": "rsa-key2"},
        json={
            "purpose": "ASYMMETRIC_DECRYPT",
            "versionTemplate": {"algorithm": "RSA_DECRYPT_OAEP_2048_SHA256"},
        },
    )
    r = kms_client.post(
        f"{BASE}/keyRings/rsa-ring2/cryptoKeys/rsa-key2/cryptoKeyVersions/1:asymmetricDecrypt",
        json={"ciphertext": base64.b64encode(b"not-valid-ciphertext").decode()},
    )
    assert r.status_code == 400


def test_asymmetric_sign_rsa_pss(kms_client):
    import hashlib

    kms_client.post(f"{BASE}/keyRings", params={"keyRingId": "rsa-sign-ring"}, json={})
    kms_client.post(
        f"{BASE}/keyRings/rsa-sign-ring/cryptoKeys",
        params={"cryptoKeyId": "rsa-sign-key"},
        json={
            "purpose": "ASYMMETRIC_SIGN",
            "versionTemplate": {"algorithm": "RSA_SIGN_PSS_2048_SHA256"},
        },
    )
    digest = base64.b64encode(hashlib.sha256(b"rsa pss message").digest()).decode()
    r = kms_client.post(
        f"{BASE}/keyRings/rsa-sign-ring/cryptoKeys/rsa-sign-key/cryptoKeyVersions/1:asymmetricSign",
        json={"digest": {"sha256": digest}},
    )
    assert r.status_code == 200
    assert len(base64.b64decode(r.json()["signature"])) > 0
