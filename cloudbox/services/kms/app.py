"""Cloud KMS emulator.

Implements the Cloud KMS REST API v1 used by google-cloud-kms.

Supports:
  - KeyRing CRUD
  - CryptoKey CRUD (ENCRYPT_DECRYPT, ASYMMETRIC_SIGN, ASYMMETRIC_DECRYPT, MAC purposes)
  - CryptoKeyVersion lifecycle (create, enable, disable, destroy, restore)
  - Symmetric encrypt/decrypt (AES-256-GCM)
  - Asymmetric sign/verify (EC P-256/P-384, RSA-PSS 2048/3072/4096)
  - Asymmetric decrypt (RSA-OAEP)
  - getPublicKey for asymmetric key versions
  - MAC sign/verify (HMAC-SHA256)
"""

from __future__ import annotations

import base64
import hashlib
import hmac as _hmac
import os
import struct

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse

from cloudbox.core.errors import GCPError, add_gcp_exception_handler
from cloudbox.core.middleware import add_request_logging
from cloudbox.services.kms.models import (
    AsymmetricDecryptRequest,
    AsymmetricDecryptResponse,
    AsymmetricSignRequest,
    AsymmetricSignResponse,
    CryptoKeyModel,
    CryptoKeyPurpose,
    CryptoKeyVersionAlgorithm,
    CryptoKeyVersionModel,
    CryptoKeyVersionState,
    CryptoKeyVersionTemplate,
    DecryptRequest,
    DecryptResponse,
    EncryptRequest,
    EncryptResponse,
    KeyRingModel,
    ListCryptoKeysResponse,
    ListCryptoKeyVersionsResponse,
    ListKeyRingsResponse,
    MacSignRequest,
    MacSignResponse,
    MacVerifyRequest,
    MacVerifyResponse,
    PublicKeyResponse,
    _now,
)
from cloudbox.services.kms.store import get_store

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    _AESGCM_AVAILABLE = True
except ImportError:
    _AESGCM_AVAILABLE = False

try:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec, padding, rsa, utils

    _ASYMMETRIC_AVAILABLE = True
except ImportError:
    _ASYMMETRIC_AVAILABLE = False

app = FastAPI(title="Cloudbox — Cloud KMS", version="v1")
add_gcp_exception_handler(app)
add_request_logging(app, "kms")

_NONCE_SIZE = 12  # AES-GCM nonce bytes


def _store():
    return get_store()


# ---------------------------------------------------------------------------
# Crypto helpers
# ---------------------------------------------------------------------------


def _new_aes_key() -> bytes:
    return os.urandom(32)


def _encrypt_payload(version_name: str, plaintext: bytes, aad: bytes | None) -> bytes:
    """Encrypt plaintext with the key for version_name using AES-256-GCM.

    Returns raw bytes: 2-byte version-name length + version name + nonce + GCM output.
    """
    if not _AESGCM_AVAILABLE:
        raise GCPError(503, "cryptography package not installed; cannot encrypt")
    store = _store()
    raw_key_b64 = store.get("keys", version_name)
    if raw_key_b64 is None:
        raise GCPError(404, f"Key material for {version_name} not found")
    key = base64.b64decode(raw_key_b64)
    aesgcm = AESGCM(key)
    nonce = os.urandom(_NONCE_SIZE)
    ct = aesgcm.encrypt(nonce, plaintext, aad)
    name_bytes = version_name.encode()
    return struct.pack(">H", len(name_bytes)) + name_bytes + nonce + ct


def _decrypt_payload(blob: bytes, aad: bytes | None) -> tuple[str, bytes]:
    """Decrypt a blob produced by _encrypt_payload.

    Returns (version_name, plaintext).
    """
    if not _AESGCM_AVAILABLE:
        raise GCPError(503, "cryptography package not installed; cannot decrypt")
    if len(blob) < 2:
        raise GCPError(400, "Invalid ciphertext")
    name_len = struct.unpack(">H", blob[:2])[0]
    offset = 2 + name_len
    if len(blob) < offset + _NONCE_SIZE:
        raise GCPError(400, "Invalid ciphertext")
    version_name = blob[2:offset].decode()
    nonce = blob[offset : offset + _NONCE_SIZE]
    ct = blob[offset + _NONCE_SIZE :]
    store = _store()
    raw_key_b64 = store.get("keys", version_name)
    if raw_key_b64 is None:
        raise GCPError(404, f"Key material for {version_name} not found")
    key = base64.b64decode(raw_key_b64)
    aesgcm = AESGCM(key)
    try:
        plaintext = aesgcm.decrypt(nonce, ct, aad)
    except Exception as exc:
        raise GCPError(400, "Decryption failed: invalid ciphertext or key") from exc
    return version_name, plaintext


def _rsa_key_size(algorithm: str) -> int:
    """Return the RSA key size in bits for the given algorithm.

    Args:
        algorithm: A CryptoKeyVersionAlgorithm constant string.

    Returns:
        Key size in bits (2048, 3072, or 4096). Defaults to 2048.
    """
    if "3072" in algorithm:
        return 3072
    if "4096" in algorithm:
        return 4096
    return 2048


def _generate_asymmetric_key(algorithm: str) -> bytes:
    """Generate an asymmetric private key and return it as PEM-encoded bytes.

    Args:
        algorithm: A CryptoKeyVersionAlgorithm constant indicating the key type.

    Returns:
        PEM-encoded private key bytes.

    Raises:
        GCPError: 503 if the cryptography package is not installed.
        GCPError: 400 if the algorithm is not recognized.
    """
    if not _ASYMMETRIC_AVAILABLE:
        raise GCPError(503, "cryptography package not installed; cannot generate asymmetric key")
    if algorithm.startswith("EC_SIGN_P256"):
        private_key = ec.generate_private_key(ec.SECP256R1())
    elif algorithm.startswith("EC_SIGN_P384"):
        private_key = ec.generate_private_key(ec.SECP384R1())
    elif algorithm.startswith("RSA_"):
        key_size = _rsa_key_size(algorithm)
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=key_size)
    else:
        raise GCPError(400, f"Unsupported algorithm for asymmetric key generation: {algorithm}")
    return private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _load_private_key(version_name: str):
    """Load and return the private key object for an asymmetric key version.

    Args:
        version_name: Full resource name of the CryptoKeyVersion.

    Returns:
        A private key object (EC or RSA) from the cryptography library.

    Raises:
        GCPError: 404 if key material is not found, 503 if cryptography unavailable.
    """
    if not _ASYMMETRIC_AVAILABLE:
        raise GCPError(503, "cryptography package not installed")
    pem_b64 = _store().get("keys", version_name)
    if pem_b64 is None:
        raise GCPError(404, f"Key material for {version_name} not found")
    pem = base64.b64decode(pem_b64)
    return serialization.load_pem_private_key(pem, password=None)


def _compute_hmac(key_b64: str, data: bytes) -> bytes:
    """Compute HMAC-SHA256 over data using a base64-encoded key.

    Args:
        key_b64: Base64-encoded raw HMAC key bytes.
        data: Raw bytes to authenticate.

    Returns:
        Raw 32-byte HMAC-SHA256 digest.
    """
    key = base64.b64decode(key_b64)
    return _hmac.new(key, data, hashlib.sha256).digest()


def _provision_version(version_name: str, purpose: str) -> None:
    """Generate and store key material for a new CryptoKeyVersion.

    Args:
        version_name: Full resource name of the CryptoKeyVersion.
        purpose: CryptoKey purpose constant (ENCRYPT_DECRYPT, ASYMMETRIC_SIGN, etc.).
    """
    if purpose == CryptoKeyPurpose.ENCRYPT_DECRYPT:
        key = _new_aes_key()
        _store().set("keys", version_name, base64.b64encode(key).decode())
    elif purpose in (CryptoKeyPurpose.ASYMMETRIC_SIGN, CryptoKeyPurpose.ASYMMETRIC_DECRYPT):
        # Look up the algorithm from the version record to pick the right key type
        version_data = _store().get("versions", version_name) or {}
        algorithm = version_data.get("algorithm", "")
        pem = _generate_asymmetric_key(algorithm)
        _store().set("keys", version_name, base64.b64encode(pem).decode())
    elif purpose == CryptoKeyPurpose.MAC:
        # 32-byte random key for HMAC-SHA256
        key = os.urandom(32)
        _store().set("keys", version_name, base64.b64encode(key).decode())


def _next_version_number(key_name: str) -> int:
    store = _store()
    prefix = f"{key_name}/cryptoKeyVersions/"
    nums = []
    for k in store.keys("versions"):
        if k.startswith(prefix):
            try:
                nums.append(int(k[len(prefix) :]))
            except ValueError:
                pass
    return max(nums, default=0) + 1


def _get_algorithm(purpose: str) -> str:
    if purpose == CryptoKeyPurpose.ENCRYPT_DECRYPT:
        return CryptoKeyVersionAlgorithm.GOOGLE_SYMMETRIC_ENCRYPTION
    if purpose == CryptoKeyPurpose.ASYMMETRIC_SIGN:
        return CryptoKeyVersionAlgorithm.EC_SIGN_P256_SHA256
    if purpose == CryptoKeyPurpose.ASYMMETRIC_DECRYPT:
        return CryptoKeyVersionAlgorithm.RSA_DECRYPT_OAEP_2048_SHA256
    return CryptoKeyVersionAlgorithm.GOOGLE_SYMMETRIC_ENCRYPTION


# ---------------------------------------------------------------------------
# KeyRings
# ---------------------------------------------------------------------------


@app.post("/v1/projects/{project}/locations/{location}/keyRings")
async def create_key_ring(project: str, location: str, request: Request):
    """Create a new Cloud KMS KeyRing.

    Args:
        project (str): GCP project ID.
        location (str): GCP region.
        request (Request): HTTP request; ``keyRingId`` is read from query params or body.

    Returns:
        dict: The newly created KeyRingModel dict.

    Raises:
        GCPError: 400 if keyRingId is missing, 409 if the key ring already exists.
    """
    key_ring_id = request.query_params.get("keyRingId")
    if not key_ring_id:
        body = await request.json()
        key_ring_id = body.get("keyRingId", "")
    if not key_ring_id:
        raise GCPError(400, "keyRingId is required")
    name = f"projects/{project}/locations/{location}/keyRings/{key_ring_id}"
    store = _store()
    if store.exists("keyrings", name):
        raise GCPError(409, f"KeyRing {name} already exists")
    ring = KeyRingModel(name=name)
    store.set("keyrings", name, ring.model_dump())
    return JSONResponse(status_code=200, content=ring.model_dump())


@app.get("/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}")
async def get_key_ring(project: str, location: str, key_ring_id: str):
    """Get a Cloud KMS KeyRing by ID.

    Args:
        project (str): GCP project ID.
        location (str): GCP region.
        key_ring_id (str): Key ring ID.

    Returns:
        dict: The KeyRingModel dict.

    Raises:
        GCPError: 404 if not found.
    """
    name = f"projects/{project}/locations/{location}/keyRings/{key_ring_id}"
    data = _store().get("keyrings", name)
    if data is None:
        raise GCPError(404, f"KeyRing {name} not found")
    return data


@app.get("/v1/projects/{project}/locations/{location}/keyRings")
async def list_key_rings(
    project: str,
    location: str,
    pageSize: int = Query(default=25),
    pageToken: str = Query(default=""),
):
    """List Cloud KMS KeyRings for a project and location.

    Args:
        project (str): GCP project ID.
        location (str): GCP region.
        pageSize (int): Maximum number of results per page.
        pageToken (str): Pagination token from a previous response.

    Returns:
        dict: ListKeyRingsResponse with keyRings and optional nextPageToken.
    """
    prefix = f"projects/{project}/locations/{location}/keyRings/"
    store = _store()
    all_rings = [KeyRingModel(**v) for v in store.list("keyrings") if v["name"].startswith(prefix)]
    all_rings.sort(key=lambda r: r.name)
    offset = int(pageToken) if pageToken else 0
    page = all_rings[offset : offset + pageSize]
    next_token = str(offset + pageSize) if offset + pageSize < len(all_rings) else None
    return ListKeyRingsResponse(
        keyRings=page, nextPageToken=next_token, totalSize=len(all_rings)
    ).model_dump(exclude_none=True)


# ---------------------------------------------------------------------------
# CryptoKeys
# ---------------------------------------------------------------------------


@app.post("/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys")
async def create_crypto_key(project: str, location: str, key_ring_id: str, request: Request):
    """Create a new CryptoKey in a KeyRing.

    Args:
        project (str): GCP project ID.
        location (str): GCP region.
        key_ring_id (str): Key ring ID.
        request (Request): HTTP request; ``cryptoKeyId`` from query params or body.

    Returns:
        dict: The newly created CryptoKeyModel dict with the initial version.

    Raises:
        GCPError: 400 if cryptoKeyId is missing, 404 if the key ring does not exist,
            409 if the key already exists.
    """
    crypto_key_id = request.query_params.get("cryptoKeyId")
    body = await request.json()
    if not crypto_key_id:
        crypto_key_id = body.get("cryptoKeyId", "")
    if not crypto_key_id:
        raise GCPError(400, "cryptoKeyId is required")

    ring_name = f"projects/{project}/locations/{location}/keyRings/{key_ring_id}"
    store = _store()
    if not store.exists("keyrings", ring_name):
        raise GCPError(404, f"KeyRing {ring_name} not found")

    key_name = f"{ring_name}/cryptoKeys/{crypto_key_id}"
    if store.exists("cryptokeys", key_name):
        raise GCPError(409, f"CryptoKey {key_name} already exists")

    purpose = body.get("purpose", CryptoKeyPurpose.ENCRYPT_DECRYPT)
    # Allow overriding the algorithm via versionTemplate (common for asymmetric keys)
    template_body = body.get("versionTemplate", {})
    algorithm = template_body.get("algorithm") or _get_algorithm(purpose)
    template = CryptoKeyVersionTemplate(algorithm=algorithm)

    ck = CryptoKeyModel(
        name=key_name,
        purpose=purpose,
        versionTemplate=template,
        labels=body.get("labels", {}),
        nextRotationTime=body.get("nextRotationTime"),
        rotationPeriod=body.get("rotationPeriod"),
    )

    # Create initial version 1 as primary
    v1_name = f"{key_name}/cryptoKeyVersions/1"
    v1 = CryptoKeyVersionModel(name=v1_name, algorithm=algorithm)
    store.set("versions", v1_name, v1.model_dump())
    _provision_version(v1_name, purpose)

    ck.primary = v1
    store.set("cryptokeys", key_name, ck.model_dump())
    return JSONResponse(status_code=200, content=ck.model_dump(exclude_none=True))


@app.get(
    "/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}"
)
async def get_crypto_key(project: str, location: str, key_ring_id: str, crypto_key_id: str):
    """Get a CryptoKey by ID.

    Args:
        project (str): GCP project ID.
        location (str): GCP region.
        key_ring_id (str): Key ring ID.
        crypto_key_id (str): CryptoKey ID.

    Returns:
        dict: The CryptoKeyModel dict.

    Raises:
        GCPError: 404 if not found.
    """
    name = (
        f"projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}"
    )
    data = _store().get("cryptokeys", name)
    if data is None:
        raise GCPError(404, f"CryptoKey {name} not found")
    return data


@app.get("/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys")
async def list_crypto_keys(
    project: str,
    location: str,
    key_ring_id: str,
    pageSize: int = Query(default=25),
    pageToken: str = Query(default=""),
):
    """List CryptoKeys for a KeyRing.

    Args:
        project (str): GCP project ID.
        location (str): GCP region.
        key_ring_id (str): Key ring ID.
        pageSize (int): Maximum number of results per page.
        pageToken (str): Pagination token from a previous response.

    Returns:
        dict: ListCryptoKeysResponse with cryptoKeys and optional nextPageToken.
    """
    prefix = f"projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/"
    store = _store()
    all_keys = [
        CryptoKeyModel(**v)
        for v in store.list("cryptokeys")
        if v["name"].startswith(prefix) and "/cryptoKeyVersions/" not in v["name"]
    ]
    all_keys.sort(key=lambda k: k.name)
    offset = int(pageToken) if pageToken else 0
    page = all_keys[offset : offset + pageSize]
    next_token = str(offset + pageSize) if offset + pageSize < len(all_keys) else None
    return ListCryptoKeysResponse(
        cryptoKeys=page, nextPageToken=next_token, totalSize=len(all_keys)
    ).model_dump(exclude_none=True)


@app.patch(
    "/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}"
)
async def update_crypto_key(
    project: str, location: str, key_ring_id: str, crypto_key_id: str, request: Request
):
    """Update mutable fields of a CryptoKey.

    Args:
        project (str): GCP project ID.
        location (str): GCP region.
        key_ring_id (str): Key ring ID.
        crypto_key_id (str): CryptoKey ID.
        request (Request): HTTP request body with labels, nextRotationTime, rotationPeriod.

    Returns:
        dict: The updated CryptoKeyModel dict.

    Raises:
        GCPError: 404 if not found.
    """
    name = (
        f"projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}"
    )
    store = _store()
    data = store.get("cryptokeys", name)
    if data is None:
        raise GCPError(404, f"CryptoKey {name} not found")
    body = await request.json()
    for field in ("labels", "nextRotationTime", "rotationPeriod"):
        if field in body:
            data[field] = body[field]
    store.set("cryptokeys", name, data)
    return data


# ---------------------------------------------------------------------------
# Encrypt / Decrypt
# ---------------------------------------------------------------------------


@app.post(
    "/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}:encrypt"
)
async def encrypt(
    project: str, location: str, key_ring_id: str, crypto_key_id: str, body: EncryptRequest
):
    """Encrypt plaintext using the primary CryptoKeyVersion (AES-256-GCM).

    Args:
        project (str): GCP project ID.
        location (str): GCP region.
        key_ring_id (str): Key ring ID.
        crypto_key_id (str): CryptoKey ID (must have ENCRYPT_DECRYPT purpose).
        body (EncryptRequest): Base64-encoded plaintext and optional AAD.

    Returns:
        dict: EncryptResponse with base64-encoded ciphertext.

    Raises:
        GCPError: 404 if the key does not exist, 400 if the key is not ENCRYPT_DECRYPT
            or has no enabled primary version.
    """
    key_name = (
        f"projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}"
    )
    store = _store()
    ck_data = store.get("cryptokeys", key_name)
    if ck_data is None:
        raise GCPError(404, f"CryptoKey {key_name} not found")
    if ck_data.get("purpose") != CryptoKeyPurpose.ENCRYPT_DECRYPT:
        raise GCPError(400, f"CryptoKey {key_name} does not support ENCRYPT_DECRYPT")
    primary = ck_data.get("primary")
    if not primary or primary.get("state") != CryptoKeyVersionState.ENABLED:
        raise GCPError(400, f"CryptoKey {key_name} has no enabled primary version")

    version_name = primary["name"]
    plaintext = base64.b64decode(body.plaintext)
    aad = (
        base64.b64decode(body.additionalAuthenticatedData)
        if body.additionalAuthenticatedData
        else None
    )
    blob = _encrypt_payload(version_name, plaintext, aad)
    ciphertext_b64 = base64.b64encode(blob).decode()
    return EncryptResponse(name=version_name, ciphertext=ciphertext_b64).model_dump(
        exclude_none=True
    )


@app.post(
    "/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}:decrypt"
)
async def decrypt(
    project: str, location: str, key_ring_id: str, crypto_key_id: str, body: DecryptRequest
):
    """Decrypt ciphertext produced by the encrypt endpoint.

    Args:
        project (str): GCP project ID.
        location (str): GCP region.
        key_ring_id (str): Key ring ID.
        crypto_key_id (str): CryptoKey ID (must have ENCRYPT_DECRYPT purpose).
        body (DecryptRequest): Base64-encoded ciphertext and optional AAD.

    Returns:
        dict: DecryptResponse with base64-encoded plaintext.

    Raises:
        GCPError: 404 if the key does not exist, 400 if the purpose is wrong,
            the ciphertext belongs to a different key, or decryption fails.
    """
    key_name = (
        f"projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}"
    )
    store = _store()
    ck_data = store.get("cryptokeys", key_name)
    if ck_data is None:
        raise GCPError(404, f"CryptoKey {key_name} not found")
    if ck_data.get("purpose") != CryptoKeyPurpose.ENCRYPT_DECRYPT:
        raise GCPError(400, f"CryptoKey {key_name} does not support ENCRYPT_DECRYPT")

    blob = base64.b64decode(body.ciphertext)
    aad = (
        base64.b64decode(body.additionalAuthenticatedData)
        if body.additionalAuthenticatedData
        else None
    )
    _version_name, plaintext = _decrypt_payload(blob, aad)

    # Verify the version belongs to this key
    if not _version_name.startswith(key_name + "/cryptoKeyVersions/"):
        raise GCPError(400, "Ciphertext was not encrypted by this key")

    version_data = store.get("versions", _version_name)
    if version_data and version_data.get("state") not in (
        CryptoKeyVersionState.ENABLED,
        CryptoKeyVersionState.DISABLED,
    ):
        raise GCPError(400, f"CryptoKeyVersion {_version_name} is destroyed and cannot decrypt")

    return DecryptResponse(
        plaintext=base64.b64encode(plaintext).decode(), usedPrimary=True
    ).model_dump(exclude_none=True)


# ---------------------------------------------------------------------------
# CryptoKeyVersions
# ---------------------------------------------------------------------------


@app.post(
    "/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}/cryptoKeyVersions"
)
async def create_crypto_key_version(
    project: str, location: str, key_ring_id: str, crypto_key_id: str
):
    """Create a new CryptoKeyVersion for a CryptoKey.

    Args:
        project (str): GCP project ID.
        location (str): GCP region.
        key_ring_id (str): Key ring ID.
        crypto_key_id (str): CryptoKey ID.

    Returns:
        dict: The newly created CryptoKeyVersionModel dict.

    Raises:
        GCPError: 404 if the CryptoKey does not exist.
    """
    key_name = (
        f"projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}"
    )
    store = _store()
    ck_data = store.get("cryptokeys", key_name)
    if ck_data is None:
        raise GCPError(404, f"CryptoKey {key_name} not found")

    n = _next_version_number(key_name)
    version_name = f"{key_name}/cryptoKeyVersions/{n}"
    purpose = ck_data.get("purpose", CryptoKeyPurpose.ENCRYPT_DECRYPT)
    algorithm = _get_algorithm(purpose)
    v = CryptoKeyVersionModel(name=version_name, algorithm=algorithm)
    store.set("versions", version_name, v.model_dump())
    _provision_version(version_name, purpose)
    _sync_primary(store, key_name)
    return JSONResponse(status_code=200, content=v.model_dump(exclude_none=True))


@app.get(
    "/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}/cryptoKeyVersions/{version_id}"
)
async def get_crypto_key_version(
    project: str, location: str, key_ring_id: str, crypto_key_id: str, version_id: str
):
    """Get a CryptoKeyVersion by ID.

    Args:
        project (str): GCP project ID.
        location (str): GCP region.
        key_ring_id (str): Key ring ID.
        crypto_key_id (str): CryptoKey ID.
        version_id (str): Version ID.

    Returns:
        dict: The CryptoKeyVersionModel dict.

    Raises:
        GCPError: 404 if not found.
    """
    key_name = (
        f"projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}"
    )
    version_name = f"{key_name}/cryptoKeyVersions/{version_id}"
    data = _store().get("versions", version_name)
    if data is None:
        raise GCPError(404, f"CryptoKeyVersion {version_name} not found")
    return data


@app.get(
    "/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}/cryptoKeyVersions"
)
async def list_crypto_key_versions(
    project: str,
    location: str,
    key_ring_id: str,
    crypto_key_id: str,
    pageSize: int = Query(default=25),
    pageToken: str = Query(default=""),
    filter: str = Query(default=""),
):
    """List CryptoKeyVersions for a CryptoKey.

    Args:
        project (str): GCP project ID.
        location (str): GCP region.
        key_ring_id (str): Key ring ID.
        crypto_key_id (str): CryptoKey ID.
        pageSize (int): Maximum number of results per page.
        pageToken (str): Pagination token from a previous response.
        filter (str): Optional state filter, e.g. ``state=ENABLED``.

    Returns:
        dict: ListCryptoKeyVersionsResponse with versions and optional nextPageToken.
    """
    key_name = (
        f"projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}"
    )
    prefix = f"{key_name}/cryptoKeyVersions/"
    store = _store()
    all_versions = [
        CryptoKeyVersionModel(**v)
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
    return ListCryptoKeyVersionsResponse(
        cryptoKeyVersions=page, nextPageToken=next_token, totalSize=len(all_versions)
    ).model_dump(exclude_none=True)


@app.patch(
    "/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}/cryptoKeyVersions/{version_id}"
)
async def update_crypto_key_version(
    project: str,
    location: str,
    key_ring_id: str,
    crypto_key_id: str,
    version_id: str,
    request: Request,
):
    """Update the state of a CryptoKeyVersion.

    Args:
        project (str): GCP project ID.
        location (str): GCP region.
        key_ring_id (str): Key ring ID.
        crypto_key_id (str): CryptoKey ID.
        version_id (str): Version ID.
        request (Request): HTTP request body with the new ``state`` value.

    Returns:
        dict: The updated CryptoKeyVersionModel dict.

    Raises:
        GCPError: 404 if not found.
    """
    key_name = (
        f"projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}"
    )
    version_name = f"{key_name}/cryptoKeyVersions/{version_id}"
    store = _store()
    data = store.get("versions", version_name)
    if data is None:
        raise GCPError(404, f"CryptoKeyVersion {version_name} not found")
    body = await request.json()
    if "state" in body:
        data["state"] = body["state"]
    store.set("versions", version_name, data)
    # Sync primary on the CryptoKey if needed
    _sync_primary(store, key_name)
    return data


@app.post(
    "/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}/cryptoKeyVersions/{version_id}:destroy"
)
async def destroy_crypto_key_version(
    project: str, location: str, key_ring_id: str, crypto_key_id: str, version_id: str
):
    """Schedule a CryptoKeyVersion for destruction.

    Args:
        project (str): GCP project ID.
        location (str): GCP region.
        key_ring_id (str): Key ring ID.
        crypto_key_id (str): CryptoKey ID.
        version_id (str): Version ID.

    Returns:
        dict: The updated CryptoKeyVersionModel dict with state DESTROY_SCHEDULED.

    Raises:
        GCPError: 404 if not found.
    """
    return _set_version_state(
        project,
        location,
        key_ring_id,
        crypto_key_id,
        version_id,
        CryptoKeyVersionState.DESTROY_SCHEDULED,
        wipe_key=False,
    )


@app.post(
    "/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}/cryptoKeyVersions/{version_id}:restore"
)
async def restore_crypto_key_version(
    project: str, location: str, key_ring_id: str, crypto_key_id: str, version_id: str
):
    """Restore a CryptoKeyVersion from DESTROY_SCHEDULED back to DISABLED.

    Args:
        project (str): GCP project ID.
        location (str): GCP region.
        key_ring_id (str): Key ring ID.
        crypto_key_id (str): CryptoKey ID.
        version_id (str): Version ID.

    Returns:
        dict: The updated CryptoKeyVersionModel dict with state DISABLED.

    Raises:
        GCPError: 404 if not found, 400 if the version is not in DESTROY_SCHEDULED state.
    """
    key_name = (
        f"projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}"
    )
    version_name = f"{key_name}/cryptoKeyVersions/{version_id}"
    store = _store()
    data = store.get("versions", version_name)
    if data is None:
        raise GCPError(404, f"CryptoKeyVersion {version_name} not found")
    if data.get("state") not in (CryptoKeyVersionState.DESTROY_SCHEDULED,):
        raise GCPError(
            400,
            f"CryptoKeyVersion {version_name} cannot be restored from state {data.get('state')}",
        )
    data["state"] = CryptoKeyVersionState.DISABLED
    data["destroyTime"] = None
    store.set("versions", version_name, data)
    return data


# ---------------------------------------------------------------------------
# Asymmetric operations
# ---------------------------------------------------------------------------


@app.get(
    "/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}/cryptoKeyVersions/{version_id}/publicKey"
)
async def get_public_key(
    project: str, location: str, key_ring_id: str, crypto_key_id: str, version_id: str
):
    """Return the PEM-encoded public key for an asymmetric CryptoKeyVersion.

    Args:
        project (str): GCP project ID.
        location (str): GCP region.
        key_ring_id (str): Key ring ID.
        crypto_key_id (str): CryptoKey ID.
        version_id (str): CryptoKeyVersion ID.

    Returns:
        PublicKeyResponse: PEM-encoded public key and algorithm.

    Raises:
        GCPError: 404 if version or key material not found, 400 if not an asymmetric key,
            503 if cryptography library unavailable.
    """
    key_name = (
        f"projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}"
    )
    version_name = f"{key_name}/cryptoKeyVersions/{version_id}"
    store = _store()
    version_data = store.get("versions", version_name)
    if version_data is None:
        raise GCPError(404, f"CryptoKeyVersion {version_name} not found")
    if version_data.get("state") != CryptoKeyVersionState.ENABLED:
        raise GCPError(400, f"CryptoKeyVersion {version_name} is not ENABLED")

    private_key = _load_private_key(version_name)
    pub_pem = (
        private_key.public_key()
        .public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode()
    )
    return PublicKeyResponse(
        pem=pub_pem,
        algorithm=version_data.get("algorithm", ""),
        name=version_name,
    ).model_dump(exclude_none=True)


@app.post(
    "/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}/cryptoKeyVersions/{version_id}:asymmetricSign"
)
async def asymmetric_sign(
    project: str,
    location: str,
    key_ring_id: str,
    crypto_key_id: str,
    version_id: str,
    body: AsymmetricSignRequest,
):
    """Sign a pre-computed digest using an asymmetric CryptoKeyVersion.

    The ``digest`` field must contain exactly one of ``sha256``, ``sha384``,
    or ``sha512``, with the base64-encoded hash value as the field value.

    Args:
        project (str): GCP project ID.
        location (str): GCP region.
        key_ring_id (str): Key ring ID.
        crypto_key_id (str): CryptoKey ID.
        version_id (str): CryptoKeyVersion ID.
        body (AsymmetricSignRequest): Request body with the pre-computed digest.

    Returns:
        AsymmetricSignResponse: Base64-encoded DER signature.

    Raises:
        GCPError: 404 if version or key material not found, 400 if digest is missing
            or invalid, 503 if cryptography library unavailable.
    """
    if not _ASYMMETRIC_AVAILABLE:
        raise GCPError(503, "cryptography package not installed; cannot sign")

    key_name = (
        f"projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}"
    )
    version_name = f"{key_name}/cryptoKeyVersions/{version_id}"
    store = _store()
    version_data = store.get("versions", version_name)
    if version_data is None:
        raise GCPError(404, f"CryptoKeyVersion {version_name} not found")
    if version_data.get("state") != CryptoKeyVersionState.ENABLED:
        raise GCPError(400, f"CryptoKeyVersion {version_name} is not ENABLED")

    # Extract the digest value and hash algorithm
    digest_map = body.digest or {}
    if "sha256" in digest_map:
        digest_bytes = base64.b64decode(digest_map["sha256"])
        hash_alg = hashes.SHA256()
    elif "sha384" in digest_map:
        digest_bytes = base64.b64decode(digest_map["sha384"])
        hash_alg = hashes.SHA384()
    elif "sha512" in digest_map:
        digest_bytes = base64.b64decode(digest_map["sha512"])
        hash_alg = hashes.SHA512()
    else:
        raise GCPError(400, "digest must contain one of: sha256, sha384, sha512")

    private_key = _load_private_key(version_name)
    algorithm = version_data.get("algorithm", "")

    try:
        if isinstance(private_key, ec.EllipticCurvePrivateKey):
            sig = private_key.sign(digest_bytes, ec.ECDSA(utils.Prehashed(hash_alg)))
        elif isinstance(private_key, rsa.RSAPrivateKey):
            sig = private_key.sign(
                digest_bytes,
                padding.PSS(
                    mgf=padding.MGF1(hash_alg),
                    salt_length=padding.PSS.MAX_LENGTH,
                ),
                utils.Prehashed(hash_alg),
            )
        else:
            raise GCPError(400, f"Unsupported key type for algorithm: {algorithm}")
    except Exception as exc:
        if isinstance(exc, GCPError):
            raise
        raise GCPError(400, f"Signing failed: {exc}") from exc

    return AsymmetricSignResponse(
        signature=base64.b64encode(sig).decode(),
        name=version_name,
    ).model_dump(exclude_none=True)


@app.post(
    "/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}/cryptoKeyVersions/{version_id}:asymmetricDecrypt"
)
async def asymmetric_decrypt(
    project: str,
    location: str,
    key_ring_id: str,
    crypto_key_id: str,
    version_id: str,
    body: AsymmetricDecryptRequest,
):
    """Decrypt data encrypted with the public key of an RSA CryptoKeyVersion.

    Uses RSA-OAEP with the digest algorithm matching the key's algorithm field.

    Args:
        project (str): GCP project ID.
        location (str): GCP region.
        key_ring_id (str): Key ring ID.
        crypto_key_id (str): CryptoKey ID.
        version_id (str): CryptoKeyVersion ID.
        body (AsymmetricDecryptRequest): Request body with the base64-encoded ciphertext.

    Returns:
        AsymmetricDecryptResponse: Base64-encoded plaintext.

    Raises:
        GCPError: 404 if version or key material not found, 400 if decryption fails,
            503 if cryptography library unavailable.
    """
    if not _ASYMMETRIC_AVAILABLE:
        raise GCPError(503, "cryptography package not installed; cannot decrypt")

    key_name = (
        f"projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}"
    )
    version_name = f"{key_name}/cryptoKeyVersions/{version_id}"
    store = _store()
    version_data = store.get("versions", version_name)
    if version_data is None:
        raise GCPError(404, f"CryptoKeyVersion {version_name} not found")
    if version_data.get("state") != CryptoKeyVersionState.ENABLED:
        raise GCPError(400, f"CryptoKeyVersion {version_name} is not ENABLED")

    private_key = _load_private_key(version_name)
    if not isinstance(private_key, rsa.RSAPrivateKey):
        raise GCPError(400, "asymmetricDecrypt requires an RSA key")

    algorithm = version_data.get("algorithm", "")
    hash_alg = hashes.SHA256() if "SHA256" in algorithm else hashes.SHA384()

    ciphertext = base64.b64decode(body.ciphertext)
    try:
        plaintext = private_key.decrypt(
            ciphertext,
            padding.OAEP(mgf=padding.MGF1(hash_alg), algorithm=hash_alg, label=None),
        )
    except Exception as exc:
        raise GCPError(400, f"Decryption failed: {exc}") from exc

    return AsymmetricDecryptResponse(
        plaintext=base64.b64encode(plaintext).decode(),
    ).model_dump(exclude_none=True)


@app.post(
    "/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}/cryptoKeyVersions/{version_id}:macSign"
)
async def mac_sign(
    project: str,
    location: str,
    key_ring_id: str,
    crypto_key_id: str,
    version_id: str,
    body: MacSignRequest,
) -> dict:
    """Compute an HMAC-SHA256 tag over the supplied data.

    Args:
        project: GCP project ID.
        location: GCP region.
        key_ring_id: Key ring identifier.
        crypto_key_id: CryptoKey identifier.
        version_id: CryptoKeyVersion identifier.
        body: MacSignRequest with base64-encoded ``data``.

    Returns:
        MacSignResponse with base64-encoded ``mac`` tag.

    Raises:
        GCPError: 404 if version not found; 400 if not a MAC key or not ENABLED.
    """
    version_name = (
        f"projects/{project}/locations/{location}/keyRings/{key_ring_id}"
        f"/cryptoKeys/{crypto_key_id}/cryptoKeyVersions/{version_id}"
    )
    version_data = _store().get("versions", version_name)
    if version_data is None:
        raise GCPError(404, f"CryptoKeyVersion {version_name} not found")
    if version_data.get("state") != CryptoKeyVersionState.ENABLED:
        raise GCPError(400, f"CryptoKeyVersion {version_name} is not ENABLED")

    key_name = (
        f"projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}"
    )
    ck_data = _store().get("cryptokeys", key_name)
    if not ck_data or ck_data.get("purpose") != CryptoKeyPurpose.MAC:
        raise GCPError(400, "macSign requires a MAC key")

    key_b64 = _store().get("keys", version_name)
    if not key_b64:
        raise GCPError(404, f"Key material for {version_name} not found")

    data = base64.b64decode(body.data) if body.data else b""
    mac = _compute_hmac(key_b64, data)
    return MacSignResponse(
        name=version_name,
        mac=base64.b64encode(mac).decode(),
    ).model_dump(exclude_none=True)


@app.post(
    "/v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}/cryptoKeyVersions/{version_id}:macVerify"
)
async def mac_verify(
    project: str,
    location: str,
    key_ring_id: str,
    crypto_key_id: str,
    version_id: str,
    body: MacVerifyRequest,
) -> dict:
    """Verify an HMAC-SHA256 tag against the supplied data.

    Args:
        project: GCP project ID.
        location: GCP region.
        key_ring_id: Key ring identifier.
        crypto_key_id: CryptoKey identifier.
        version_id: CryptoKeyVersion identifier.
        body: MacVerifyRequest with base64-encoded ``data`` and ``mac``.

    Returns:
        MacVerifyResponse with ``success=True`` if the tag is valid.

    Raises:
        GCPError: 404 if version not found; 400 if not a MAC key or not ENABLED.
    """
    version_name = (
        f"projects/{project}/locations/{location}/keyRings/{key_ring_id}"
        f"/cryptoKeys/{crypto_key_id}/cryptoKeyVersions/{version_id}"
    )
    version_data = _store().get("versions", version_name)
    if version_data is None:
        raise GCPError(404, f"CryptoKeyVersion {version_name} not found")
    if version_data.get("state") != CryptoKeyVersionState.ENABLED:
        raise GCPError(400, f"CryptoKeyVersion {version_name} is not ENABLED")

    key_name = (
        f"projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}"
    )
    ck_data = _store().get("cryptokeys", key_name)
    if not ck_data or ck_data.get("purpose") != CryptoKeyPurpose.MAC:
        raise GCPError(400, "macVerify requires a MAC key")

    key_b64 = _store().get("keys", version_name)
    if not key_b64:
        raise GCPError(404, f"Key material for {version_name} not found")

    data = base64.b64decode(body.data) if body.data else b""
    expected = _compute_hmac(key_b64, data)
    provided = base64.b64decode(body.mac)
    success = _hmac.compare_digest(expected, provided)
    return MacVerifyResponse(
        name=version_name,
        success=success,
    ).model_dump(exclude_none=True)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _set_version_state(
    project: str,
    location: str,
    key_ring_id: str,
    crypto_key_id: str,
    version_id: str,
    state: str,
    wipe_key: bool = False,
) -> dict:
    key_name = (
        f"projects/{project}/locations/{location}/keyRings/{key_ring_id}/cryptoKeys/{crypto_key_id}"
    )
    version_name = f"{key_name}/cryptoKeyVersions/{version_id}"
    store = _store()
    data = store.get("versions", version_name)
    if data is None:
        raise GCPError(404, f"CryptoKeyVersion {version_name} not found")
    data["state"] = state
    if state in (CryptoKeyVersionState.DESTROY_SCHEDULED,):
        data["destroyTime"] = _now()
    if wipe_key:
        store.delete("keys", version_name)
    store.set("versions", version_name, data)
    _sync_primary(store, key_name)
    return data


def _sync_primary(store, key_name: str) -> None:
    """Update the primary pointer on a CryptoKey to the highest-numbered enabled version."""
    ck_data = store.get("cryptokeys", key_name)
    if ck_data is None:
        return
    prefix = f"{key_name}/cryptoKeyVersions/"
    enabled = []
    for k in store.keys("versions"):
        if k.startswith(prefix):
            v = store.get("versions", k)
            if v and v.get("state") == CryptoKeyVersionState.ENABLED:
                try:
                    enabled.append((int(k[len(prefix) :]), v))
                except ValueError:
                    pass
    if enabled:
        ck_data["primary"] = max(enabled, key=lambda x: x[0])[1]
    else:
        ck_data["primary"] = None
    store.set("cryptokeys", key_name, ck_data)
