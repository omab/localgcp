"""Scheduler in-memory state."""

from cloudbox.config import settings
from cloudbox.core.store import NamespacedStore

_store = NamespacedStore("scheduler", settings.data_dir)


def get_store() -> NamespacedStore:
    return _store
