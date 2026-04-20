# Cloud KMS

Cloudbox emulates the Cloud KMS REST API (v1). The `google-cloud-kms` Python SDK works
against it without modification. Cryptographic operations use real AES-256-GCM encryption
via the `cryptography` library.

## Connection

**Port:** `8092` (override with `CLOUDBOX_KMS_PORT`)

```python
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import kms

client = kms.KeyManagementServiceClient(
    credentials=AnonymousCredentials(),
    client_options=ClientOptions(api_endpoint="http://localhost:8092"),
)
```

---

## Resource hierarchy

```
projects/{project}/locations/{location}
  └── keyRings/{key_ring_id}
        └── cryptoKeys/{crypto_key_id}
              └── cryptoKeyVersions/{version_id}
```

---

## Key rings

### Create key ring

```
POST /v1/projects/{project}/locations/{location}/keyRings?keyRingId={id}
```

Body may be `{}`. Returns the key ring resource. `409` if it already exists.

### Get key ring

```
GET /v1/projects/{project}/locations/{location}/keyRings/{key_ring_id}
```

### List key rings

```
GET /v1/projects/{project}/locations/{location}/keyRings
```

Returns `{ "keyRings": [...] }`.

---

## Crypto keys

### Create crypto key

```
POST /v1/projects/{project}/locations/{location}/keyRings/{ring_id}/cryptoKeys?cryptoKeyId={id}
```

```json
{
  "purpose": "ENCRYPT_DECRYPT",
  "versionTemplate": {
    "algorithm": "GOOGLE_SYMMETRIC_ENCRYPTION",
    "protectionLevel": "SOFTWARE"
  }
}
```

Supported purposes: `ENCRYPT_DECRYPT`. Asymmetric purposes (`ASYMMETRIC_SIGN`,
`ASYMMETRIC_DECRYPT`) are accepted and stored but the cryptographic operations return `501`.

Creating a key automatically creates version `1` as the primary version. Returns the key
resource including the `primary` version pointer.

### Get crypto key

```
GET /v1/projects/{project}/locations/{location}/keyRings/{ring_id}/cryptoKeys/{key_id}
```

Returns the key resource with its current `primary` version.

### List crypto keys

```
GET /v1/projects/{project}/locations/{location}/keyRings/{ring_id}/cryptoKeys
```

Returns `{ "cryptoKeys": [...] }`.

### Patch crypto key

```
PATCH /v1/projects/{project}/locations/{location}/keyRings/{ring_id}/cryptoKeys/{key_id}
```

```json
{ "labels": { "team": "backend" } }
```

Updates `labels`. Returns the updated key resource.

---

## Encrypt / decrypt

### Encrypt

```
POST /v1/.../cryptoKeys/{key_id}:encrypt
```

```json
{
  "plaintext": "SGVsbG8sIFdvcmxkIQ==",
  "additionalAuthenticatedData": "dXNlci1pZDo0Mg=="
}
```

Both `plaintext` and `additionalAuthenticatedData` must be base64-encoded.
`additionalAuthenticatedData` (AAD) is optional — when supplied it is bound to the
ciphertext and must be provided identically at decrypt time.

Returns:

```json
{
  "name": "projects/.../cryptoKeyVersions/1",
  "ciphertext": "<base64-encoded-ciphertext>",
  "ciphertextCrc32c": "<crc32c>"
}
```

Encryption always uses the current primary version. The version name is embedded in the
ciphertext, so the correct version key is automatically selected at decrypt time even after
key rotation.

### Decrypt

```
POST /v1/.../cryptoKeys/{key_id}:decrypt
```

```json
{
  "ciphertext": "<base64-encoded-ciphertext>",
  "additionalAuthenticatedData": "dXNlci1pZDo0Mg=="
}
```

Returns:

```json
{
  "plaintext": "SGVsbG8sIFdvcmxkIQ==",
  "plaintextCrc32c": "<crc32c>"
}
```

Failure cases:

| Condition | Status |
|---|---|
| Wrong key (ciphertext from a different key) | `400` |
| AAD mismatch | `400` |
| Version disabled | `400` |
| Version destroyed | `400` |

---

## Key versions

### Add version (rotation)

```
POST /v1/.../cryptoKeys/{key_id}/cryptoKeyVersions
```

Body: `{}`. Creates the next numbered version and immediately promotes it to primary.
The old version remains active for decrypting existing ciphertexts.

### Get version

```
GET /v1/.../cryptoKeys/{key_id}/cryptoKeyVersions/{version_id}
```

### List versions

```
GET /v1/.../cryptoKeys/{key_id}/cryptoKeyVersions
```

Returns `{ "cryptoKeyVersions": [...] }`.

### Patch version state

```
PATCH /v1/.../cryptoKeyVersions/{version_id}
```

```json
{ "state": "DISABLED" }
```

Sets the version state. Supported values: `ENABLED`, `DISABLED`. After a state change,
the primary pointer on the parent key is updated to the highest-numbered enabled version.

### Schedule destruction

```
POST /v1/.../cryptoKeyVersions/{version_id}:destroy
```

Sets state to `DESTROY_SCHEDULED`. Returns the updated version resource.

### Restore version

```
POST /v1/.../cryptoKeyVersions/{version_id}:restore
```

Restores a `DESTROY_SCHEDULED` version to `DISABLED`. Returns the updated version
resource.

---

## Key version states

| State | Can encrypt? | Can decrypt? |
|---|:---:|:---:|
| `ENABLED` | Yes (if primary) | Yes |
| `DISABLED` | No | No |
| `DESTROY_SCHEDULED` | No | No |
| `DESTROYED` | No | No |

Primary selection: when the primary version is disabled or scheduled for destruction,
the key's `primary` pointer automatically advances to the next-highest enabled version.
If no enabled versions remain, `primary` is cleared.

---

## Key rotation flow

1. Encrypt data — uses primary version `1`.
2. Call `POST .../cryptoKeyVersions` — creates version `2`, version `2` becomes primary.
3. Encrypt new data — uses version `2`.
4. Old ciphertext — still decrypts via version `1` (version name embedded in ciphertext).
5. Disable version `1` — old ciphertexts can no longer be decrypted.
6. Schedule version `1` for destruction.

---

## Ciphertext format

Ciphertexts are opaque base64-encoded blobs. Internally they encode:

```
[2-byte version name length] [version name bytes] [12-byte AES-GCM nonce] [AES-GCM ciphertext]
```

The version name is embedded so that decryption automatically selects the correct key
material, enabling transparent cross-version decryption after rotation.

---

## Asymmetric operations (stubs)

The following endpoints are accepted but return `501 Not Implemented`:

- `GET .../cryptoKeyVersions/{version_id}/publicKey`
- `POST .../cryptoKeyVersions/{version_id}:asymmetricSign`
- `POST .../cryptoKeyVersions/{version_id}:asymmetricDecrypt`

---

## Known limitations

| Feature | Notes |
|---|---|
| Asymmetric sign / verify | Returns `501` — RSA and EC key operations not yet implemented |
| MAC keys (HMAC) | Not implemented |
| Key import | `ImportJob` endpoints not implemented |
| Key deletion | GCP does not allow key ring or key deletion; `DELETE` is not implemented |
| IAM | Not enforced |

---

## Examples

```bash
uv run python examples/kms/encrypt_decrypt.py
uv run python examples/kms/key_rotation.py
```

| Example | What it demonstrates |
|---|---|
| `encrypt_decrypt.py` | Key ring and key creation; encrypt/decrypt round-trip; AAD binding; cross-key rejection |
| `key_rotation.py` | Adding a version; version promotion; decrypting old ciphertexts after rotation; disable/enable/destroy/restore lifecycle |
