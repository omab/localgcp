"""Pydantic models for Cloud KMS REST API v1."""

from __future__ import annotations

from datetime import UTC, datetime

from pydantic import BaseModel, Field


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class KeyRingModel(BaseModel):
    """A Cloud KMS KeyRing resource."""

    name: str
    createTime: str = Field(default_factory=_now)


class CryptoKeyPurpose:
    """Enumeration of CryptoKey purpose values."""

    ENCRYPT_DECRYPT = "ENCRYPT_DECRYPT"
    ASYMMETRIC_SIGN = "ASYMMETRIC_SIGN"
    ASYMMETRIC_DECRYPT = "ASYMMETRIC_DECRYPT"
    MAC = "MAC"


class CryptoKeyVersionAlgorithm:
    """Enumeration of CryptoKeyVersion algorithm constants."""

    GOOGLE_SYMMETRIC_ENCRYPTION = "GOOGLE_SYMMETRIC_ENCRYPTION"
    RSA_SIGN_PSS_2048_SHA256 = "RSA_SIGN_PSS_2048_SHA256"
    RSA_SIGN_PSS_3072_SHA256 = "RSA_SIGN_PSS_3072_SHA256"
    RSA_SIGN_PSS_4096_SHA256 = "RSA_SIGN_PSS_4096_SHA256"
    RSA_DECRYPT_OAEP_2048_SHA256 = "RSA_DECRYPT_OAEP_2048_SHA256"
    EC_SIGN_P256_SHA256 = "EC_SIGN_P256_SHA256"
    EC_SIGN_P384_SHA384 = "EC_SIGN_P384_SHA384"


class CryptoKeyVersionState:
    """Enumeration of CryptoKeyVersion lifecycle states."""

    PENDING_GENERATION = "PENDING_GENERATION"
    ENABLED = "ENABLED"
    DISABLED = "DISABLED"
    DESTROY_SCHEDULED = "DESTROY_SCHEDULED"
    DESTROYED = "DESTROYED"


class CryptoKeyVersionModel(BaseModel):
    """A single version of a Cloud KMS CryptoKey."""

    name: str
    state: str = CryptoKeyVersionState.ENABLED
    createTime: str = Field(default_factory=_now)
    generateTime: str = Field(default_factory=_now)
    destroyTime: str | None = None
    destroyEventTime: str | None = None
    algorithm: str = CryptoKeyVersionAlgorithm.GOOGLE_SYMMETRIC_ENCRYPTION
    protectionLevel: str = "SOFTWARE"


class CryptoKeyVersionTemplate(BaseModel):
    """Template specifying the algorithm and protection level for new key versions."""

    algorithm: str = CryptoKeyVersionAlgorithm.GOOGLE_SYMMETRIC_ENCRYPTION
    protectionLevel: str = "SOFTWARE"


class CryptoKeyModel(BaseModel):
    """A Cloud KMS CryptoKey resource."""

    name: str
    purpose: str = CryptoKeyPurpose.ENCRYPT_DECRYPT
    createTime: str = Field(default_factory=_now)
    nextRotationTime: str | None = None
    rotationPeriod: str | None = None
    primary: CryptoKeyVersionModel | None = None
    versionTemplate: CryptoKeyVersionTemplate = Field(default_factory=CryptoKeyVersionTemplate)
    labels: dict[str, str] = Field(default_factory=dict)


class ListKeyRingsResponse(BaseModel):
    """Response body for listing KeyRings."""

    keyRings: list[KeyRingModel] = Field(default_factory=list)
    nextPageToken: str | None = None
    totalSize: int = 0


class ListCryptoKeysResponse(BaseModel):
    """Response body for listing CryptoKeys."""

    cryptoKeys: list[CryptoKeyModel] = Field(default_factory=list)
    nextPageToken: str | None = None
    totalSize: int = 0


class ListCryptoKeyVersionsResponse(BaseModel):
    """Response body for listing CryptoKeyVersions."""

    cryptoKeyVersions: list[CryptoKeyVersionModel] = Field(default_factory=list)
    nextPageToken: str | None = None
    totalSize: int = 0


class EncryptRequest(BaseModel):
    """Request body for the encrypt endpoint."""

    plaintext: str  # base64-encoded
    additionalAuthenticatedData: str | None = None


class EncryptResponse(BaseModel):
    """Response body for the encrypt endpoint."""

    name: str
    ciphertext: str  # base64-encoded
    ciphertextCrc32c: str | None = None


class DecryptRequest(BaseModel):
    """Request body for the decrypt endpoint."""

    ciphertext: str  # base64-encoded
    additionalAuthenticatedData: str | None = None


class DecryptResponse(BaseModel):
    """Response body for the decrypt endpoint."""

    plaintext: str  # base64-encoded
    plaintextCrc32c: str | None = None
    usedPrimary: bool = True


class AsymmetricSignRequest(BaseModel):
    """Request body for asymmetricSign."""

    digest: dict  # {"sha256": "<base64>", "sha384": "<base64>", ...}
    digestCrc32c: str | None = None


class AsymmetricSignResponse(BaseModel):
    """Response body for asymmetricSign."""

    signature: str  # base64-encoded DER signature
    signatureCrc32c: str | None = None
    verifiedDigestCrc32c: bool = False
    name: str = ""
    protectionLevel: str = "SOFTWARE"


class PublicKeyResponse(BaseModel):
    """Response body for getPublicKey."""

    pem: str
    algorithm: str
    pemCrc32c: str | None = None
    name: str = ""
    protectionLevel: str = "SOFTWARE"


class AsymmetricDecryptRequest(BaseModel):
    """Request body for asymmetricDecrypt."""

    ciphertext: str  # base64-encoded
    ciphertextCrc32c: str | None = None


class AsymmetricDecryptResponse(BaseModel):
    """Response body for asymmetricDecrypt."""

    plaintext: str  # base64-encoded
    plaintextCrc32c: str | None = None
    verifiedCiphertextCrc32c: bool = False
    protectionLevel: str = "SOFTWARE"


class MacSignRequest(BaseModel):
    """Request body for macSign."""

    data: str  # base64-encoded plaintext to authenticate
    dataCrc32c: str | None = None


class MacSignResponse(BaseModel):
    """Response body for macSign."""

    name: str = ""
    mac: str = ""  # base64-encoded HMAC-SHA256 tag
    macCrc32c: str | None = None
    verifiedDataCrc32c: bool = False
    protectionLevel: str = "SOFTWARE"


class MacVerifyRequest(BaseModel):
    """Request body for macVerify."""

    data: str  # base64-encoded plaintext
    mac: str  # base64-encoded HMAC-SHA256 tag to verify
    dataCrc32c: str | None = None
    macCrc32c: str | None = None


class MacVerifyResponse(BaseModel):
    """Response body for macVerify."""

    name: str = ""
    success: bool = False
    verifiedDataCrc32c: bool = False
    verifiedMacCrc32c: bool = False
    protectionLevel: str = "SOFTWARE"
