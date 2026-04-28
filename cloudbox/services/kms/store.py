"""Cloud KMS in-memory store.

Namespaces:
  keyrings     → keyring_name → KeyRingModel dict
  cryptokeys   → cryptokey_name → CryptoKeyModel dict
  versions     → version_name → CryptoKeyVersionModel dict
  keys         → version_name → base64-encoded raw 32-byte AES key
"""

from cloudbox.config import settings
from cloudbox.core.store import NamespacedStore

_store = NamespacedStore("kms", settings.data_dir)


def get_store() -> NamespacedStore:
    """Return the shared KMS NamespacedStore instance."""
    return _store
