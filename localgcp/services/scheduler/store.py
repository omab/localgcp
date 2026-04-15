"""Scheduler in-memory state."""
from localgcp.config import settings
from localgcp.core.store import NamespacedStore

_store = NamespacedStore("scheduler", settings.data_dir)


def get_store() -> NamespacedStore:
    return _store
