"""Secret Manager in-memory store.

Namespaces:
  secrets   → secret_name → SecretModel dict
  versions  → "{secret_name}/versions/{n}" → SecretVersionModel dict
  payloads  → "{secret_name}/versions/{n}" → base64-encoded data string
"""

from cloudbox.config import settings
from cloudbox.core.store import NamespacedStore

_store = NamespacedStore("secretmanager", settings.data_dir)


def get_store() -> NamespacedStore:
    """Return the shared Secret Manager store instance.

    Returns:
        NamespacedStore: The module-level store used by all Secret Manager route handlers.
    """
    return _store
