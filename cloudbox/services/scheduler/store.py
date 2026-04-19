"""Scheduler in-memory state."""

from cloudbox.config import settings
from cloudbox.core.store import NamespacedStore

_store = NamespacedStore("scheduler", settings.data_dir)


def get_store() -> NamespacedStore:
    """Return the shared Cloud Scheduler store instance.

    Returns:
        NamespacedStore: The module-level store used by all Scheduler route handlers.
    """
    return _store
