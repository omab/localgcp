"""Cloud Logging in-memory store."""

from cloudbox.config import settings
from cloudbox.core.store import NamespacedStore

_store = NamespacedStore("logging", data_dir=settings.data_dir)


def get_store() -> NamespacedStore:
    """Return the shared Cloud Logging store instance.

    Returns:
        NamespacedStore: The module-level store used by all Logging route handlers.
    """
    return _store
